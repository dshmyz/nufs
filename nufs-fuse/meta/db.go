// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// Package meta maintains the caching of all meta data of the files and directories.
package meta

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	minio "github.com/minio/minio-go/v7"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// RegisterExt registers a msgpack extension type.
func RegisterExt(id int8, value interface{}) interface{} {
	msgpack.RegisterExt(id, value)
	return value
}

// Open opens or creates a BadgerDB database at the given path.
// The options parameter is kept for API compatibility but ignored.
func Open(path string, mode os.FileMode, options interface{}) (*DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions(filepath.Join(dir, "badger")).
		WithLogger(nil) // Silence badger's internal logger.

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:   db,
		path: path,
	}, nil
}

// DB wraps a BadgerDB instance and provides a BoltDB-compatible API.
type DB struct {
	db   *badger.DB
	path string
	mu   sync.Mutex // serialises writable transactions
}

// Begin starts a new transaction.
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		db.mu.Lock()
	}
	txn := db.db.NewTransaction(writable)
	return &Tx{
		txn:      txn,
		db:       db,
		writable: writable,
	}, nil
}

// Update executes a function within a writable transaction.
func (db *DB) Update(fn func(*Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	tx := &Tx{txn: txn, db: db, writable: true}
	if err := fn(tx); err != nil {
		return err
	}
	return txn.Commit()
}

// View executes a function within a read-only transaction.
func (db *DB) View(fn func(*Tx) error) error {
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	tx := &Tx{txn: txn, db: db, writable: false}
	return fn(tx)
}

// Close closes the database.
func (db *DB) Close() error {
	return db.db.Close()
}

// ---------------------------------------------------------------------------
// Tx — transaction wrapper providing BoltDB-compatible API.
// ---------------------------------------------------------------------------

// Tx wraps a BadgerDB transaction.
type Tx struct {
	txn      *badger.Txn
	db       *DB
	writable bool
}

// Bucket returns a root-level bucket by name.
func (tx *Tx) Bucket(name string) *Bucket {
	return &Bucket{prefix: name, txn: tx.txn}
}

// CreateBucketIfNotExists creates a root-level bucket marker.
// In BadgerDB, buckets are implicit (prefix-based), but we store a marker
// so that Bucket() can verify existence if needed.
func (tx *Tx) CreateBucketIfNotExists(name string) (*Bucket, error) {
	key := []byte(name + "__bucket__")
	if _, err := tx.txn.Get(key); err == badger.ErrKeyNotFound {
		if err := tx.txn.Set(key, nil); err != nil {
			return nil, err
		}
	}
	return &Bucket{prefix: name, txn: tx.txn}, nil
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	if tx.writable {
		tx.db.mu.Unlock()
	}
	return tx.txn.Commit()
}

// Rollback discards the transaction.
func (tx *Tx) Rollback() error {
	if tx.writable {
		tx.db.mu.Unlock()
	}
	tx.txn.Discard()
	return nil
}

// ---------------------------------------------------------------------------
// Bucket — prefix-based key namespace emulating BoltDB nested buckets.
// ---------------------------------------------------------------------------

// Bucket represents a key namespace backed by a BadgerDB key prefix.
type Bucket struct {
	prefix string
	txn    *badger.Txn
}

// fullKey returns the full BadgerDB key for a given logical key.
func (b *Bucket) fullKey(key string) []byte {
	return []byte(b.prefix + key)
}

// Get retrieves and deserializes a value by key.
func (b *Bucket) Get(key string, v ...interface{}) error {
	item, err := b.txn.Get(b.fullKey(key))
	if err == badger.ErrKeyNotFound {
		return ErrNoSuchObject
	}
	if err != nil {
		return err
	}
	return item.Value(func(data []byte) error {
		return msgpack.Unmarshal(data, v...)
	})
}

// Put serializes and stores a value at the given key.
func (b *Bucket) Put(key string, v interface{}) error {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return err
	}
	return b.txn.Set(b.fullKey(key), data)
}

// Delete removes a key from the bucket.
func (b *Bucket) Delete(key string) error {
	return b.txn.Delete(b.fullKey(key))
}

// ForEach iterates over direct children of the bucket (single level),
// matching BoltDB's bucket.ForEach semantics. Sub-bucket marker keys
// and nested entries (containing '/' after the prefix) are skipped.
func (b *Bucket) ForEach(fn func(string, interface{}) error) error {
	prefix := []byte(b.prefix)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix

	it := b.txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		relKey := string(k[len(prefix):])

		// Skip empty, sub-bucket markers, and nested entries (single-level only).
		if relKey == "" || strings.Contains(relKey, "/") || strings.HasSuffix(relKey, "__bucket__") {
			continue
		}

		var valCopy []byte
		if err := item.Value(func(v []byte) error {
			valCopy = make([]byte, len(v))
			copy(valCopy, v)
			return nil
		}); err != nil {
			return err
		}

		var o interface{}
		if err := msgpack.Unmarshal(valCopy, &o); err != nil {
			return err
		}

		if err := fn(relKey, o); err != nil {
			return err
		}
	}
	return nil
}

// Bucket returns a nested sub-bucket by name.
func (b *Bucket) Bucket(name string) *Bucket {
	return &Bucket{
		prefix: b.prefix + name,
		txn:    b.txn,
	}
}

// CreateBucketIfNotExists is a no-op — BadgerDB buckets are implicit.
func (b *Bucket) CreateBucketIfNotExists(key string) (*Bucket, error) {
	return &Bucket{prefix: b.prefix + key, txn: b.txn}, nil
}

// DeleteBucket removes all keys under the sub-bucket prefix.
func (b *Bucket) DeleteBucket(key string) error {
	subPrefix := []byte(b.prefix + key)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = subPrefix
	opts.PrefetchValues = false

	it := b.txn.NewIterator(opts)
	defer it.Close()

	var keys [][]byte
	for it.Seek(subPrefix); it.ValidForPrefix(subPrefix); it.Next() {
		k := make([]byte, len(it.Item().Key()))
		copy(k, it.Item().Key())
		keys = append(keys, k)
	}
	// Also delete the marker key.
	keys = append(keys, []byte(b.prefix+key+"__bucket__"))

	for _, k := range keys {
		if err := b.txn.Delete(k); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
	}
	return nil
}

// seqKey is the reserved suffix for sequence counters within a bucket.
const seqKey = "__seq__"

// NextSequence returns the next auto-incrementing sequence number for this bucket.
// It is safe to call multiple times within the same transaction.
func (b *Bucket) NextSequence() (uint64, error) {
	key := b.fullKey(seqKey)

	var seq uint64
	item, err := b.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		seq = 0
	} else if err != nil {
		return 0, err
	} else {
		if verr := item.Value(func(v []byte) error {
			if len(v) == 8 {
				seq = binary.BigEndian.Uint64(v)
			}
			return nil
		}); verr != nil {
			return 0, verr
		}
	}

	seq++
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)
	if err := b.txn.Set(key, buf); err != nil {
		return 0, err
	}
	return seq, nil
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

// ErrNoSuchObject is returned when a key is not found in the store.
var ErrNoSuchObject = errors.New("No such object")

// IsNoSuchObject checks whether err indicates a missing key.
func IsNoSuchObject(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrNoSuchObject {
		return true
	}
	if err.Error() == ErrNoSuchObject.Error() {
		return true
	}
	errorResponse := minio.ToErrorResponse(err)
	return errorResponse.Code == "NoSuchKey"
}
