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

// Package minfs contains the MinFS core package
package minfs

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minfs/meta"
	"github.com/minio/minio-go/v7"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var (
	_ = meta.RegisterExt(1, File{})
	_ = meta.RegisterExt(2, Dir{})
	_ = meta.RegisterExt(3, Symlink{})
)

const pendingBucket = "pending/"

// pendingUpload tracks a cache file that needs to be uploaded to S3.
// Persisted in BadgerDB so crash recovery can replay failed uploads.
type pendingUpload struct {
	CachePath  string `json:"cache_path"`
	RemotePath string `json:"remote_path"`
	Size       int64  `json:"size"`
}

// MinFS contains the meta data for the MinFS client
type MinFS struct {
	config *Config
	api    *minio.Client

	db *meta.DB

	// Logger instance.
	log       *log.Logger
	logWriter *os.File

	// Audit logger for write operations.
	auditLog    *log.Logger
	auditWriter *os.File

	// contains all open handles, protected by m.
	handles         map[uint64]*FileHandle
	handleIDCounter uint64 // accessed via sync/atomic

	locks map[string]*pathLock

	// LRU cache tracking: cache file path -> last access time.
	cacheAccess   map[string]time.Time
	cacheAccessMu sync.Mutex

	m sync.Mutex

	syncChan     chan interface{}
	workersWg    sync.WaitGroup
	shutdownOnce sync.Once
	metricsSrv   *http.Server

	// S3 circuit breaker — fast-fail when backend is unreachable.
	breaker *circuitBreaker
}

// New will return a new MinFS client
func New(options ...func(*Config)) (*MinFS, error) {
	// Initialize config.
	ac, err := InitMinFSConfig()
	if err != nil {
		return nil, err
	}

	// Set defaults
	cfg := &Config{
		cache:       globalDBDir,
		basePath:    "",
		accountID:   fmt.Sprintf("%d", time.Now().UTC().Unix()),
		gid:         0,
		uid:         0,
		accessKey:   ac.AccessKey,
		secretKey:   ac.SecretKey,
		mode:        os.FileMode(0660),
		scanTTL:     60 * time.Second,
		metricsAddr: ":9900",
	}

	for _, optionFn := range options {
		optionFn(cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// Ensure the cache directory exists (each instance needs its own).
	if err := os.MkdirAll(cfg.cache, 0700); err != nil {
		return nil, fmt.Errorf("failed to create cache directory %s: %w", cfg.cache, err)
	}

	// Open log file in the instance's cache directory for isolation.
	logPath := path.Join(cfg.cache, "minfs.log")
	logW, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	// Initialize MinFS.
	mfs := &MinFS{
		config:    cfg,
		syncChan:  make(chan interface{}, 100),
		locks:     map[string]*pathLock{},
		log:       log.New(logW, "MinFS ", log.Ldate|log.Ltime|log.Lshortfile),
		logWriter: logW,
		handles:   map[uint64]*FileHandle{},
		breaker:   newCircuitBreaker(5, 30*time.Second),
		cacheAccess: map[string]time.Time{},
	}

	// Initialize audit logger in the same cache directory.
	auditW, err := os.OpenFile(path.Join(cfg.cache, "audit.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		logW.Close()
		return nil, err
	}
	mfs.auditWriter = auditW
	mfs.auditLog = log.New(auditW, "", 0)

	// Success..
	return mfs, nil
}

func (mfs *MinFS) mount() (*fuse.Conn, error) {
	return fuse.Mount(
		mfs.config.mountpoint,
		fuse.FSName("MinFS"),
		fuse.Subtype("MinFS"),
		fuse.LocalVolume(),
		fuse.VolumeName(mfs.config.bucket),
		fuse.AllowOther(),
		fuse.DefaultPermissions(),
	)
}

// Serve starts the MinFS client
func (mfs *MinFS) Serve() (err error) {
	if mfs.config.debug {
		fuse.Debug = func(msg interface{}) {
			mfs.log.Printf("%#v\n", msg)
		}
	}

	defer mfs.shutdown()

	mfs.log.Println("Mounting target....")
	// mount the drive
	var c *fuse.Conn
	c, err = mfs.mount()
	if err != nil {
		return err
	}
	defer c.Close()

	// Trap signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		mfs.shutdown()
	}()

	// Initialize database.
	mfs.log.Println("Opening cache database...")
	mfs.db, err = meta.Open(path.Join(mfs.config.cache, "cache.db"), 0600, nil)
	if err != nil {
		return err
	}
	defer mfs.db.Close()

	mfs.log.Println("Initializing cache database...")
	if err = mfs.db.Update(func(tx *meta.Tx) error {
		if _, berr := tx.CreateBucketIfNotExists("minio/"); berr != nil {
			return berr
		}
		_, berr := tx.CreateBucketIfNotExists(pendingBucket)
		return berr
	}); err != nil {
		return err
	}

	mfs.log.Println("Initializing minio client...")

	var (
		host   = mfs.config.target.Host
		access = mfs.config.accessKey
		secret = mfs.config.secretKey
		token  = mfs.config.secretToken
		secure = mfs.config.target.Scheme == "https"
	)

	var transport http.RoundTripper = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: mfs.config.insecure,
		},
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
	}

	creds := newRefreshableCreds(access, secret, token, path.Join(mfs.config.cache, "config.json"))
	options := &minio.Options{
		Creds:     creds,
		Secure:    secure,
		Transport: transport,
	}

	mfs.api, err = minio.New(host, options)
	if err != nil {
		return err
	}

	// Validate if the bucket is valid and accessible.
	exists, err := mfs.api.BucketExists(context.Background(), mfs.config.bucket)
	if err != nil {
		return err
	}
	if !exists {
		mfs.log.Println("Bucket doesn't exist... aborting")
		return os.ErrNotExist
	}

	// Start sync worker pool (4 concurrent upload workers).
	// Start metrics/health HTTP server (non-blocking).
	if mfs.config.metricsAddr != "" {
		mfs.metricsSrv = startMetricsServer(mfs.config.metricsAddr)
		mfs.log.Printf("Metrics server listening on %s\n", mfs.config.metricsAddr)
		// Keep circuit breaker state in sync with metrics.
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				globalMetrics.CircuitBreakerState = mfs.breaker.State()
			}
		}()
	}

	if err = mfs.startSync(4); err != nil {
		return err
	}

	// Recover any pending uploads from previous crash in background.
	go mfs.recoverPending()

	mfs.log.Println("Serving... Have fun!")
	// Serve the filesystem
	if err = fs.Serve(c, mfs); err != nil {
		mfs.log.Println("Error while serving the file system.", err)
		return err
	}

	<-c.Ready
	return c.MountError
}

func (mfs *MinFS) shutdown() {
	mfs.shutdownOnce.Do(func() {
		// Close sync channel and wait for all upload workers to finish.
		close(mfs.syncChan)
		mfs.workersWg.Wait()

		// Shut down metrics server.
		if mfs.metricsSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			mfs.metricsSrv.Shutdown(ctx)
			cancel()
		}

		// Unmount the filesystem.
		fuse.Unmount(mfs.config.mountpoint)

		// Close the log file.
		if mfs.logWriter != nil {
			mfs.logWriter.Close()
		}

		// Close the audit log file.
		if mfs.auditWriter != nil {
			mfs.auditWriter.Close()
		}

		mfs.log.Println("MinFS stopped cleanly.")
	})
}

func (mfs *MinFS) sync(req interface{}) error {
	mfs.syncChan <- req
	return nil
}

func (mfs *MinFS) moveOp(req *MoveOperation) {
	dst := minio.CopyDestOptions{
		Bucket: mfs.config.bucket,
		Object: req.Target,
	}
	src := minio.CopySrcOptions{
		Bucket: mfs.config.bucket,
		Object: req.Source,
	}
	metricsIncS3Copy()

	// Step 1: Copy to new location.
	if err := mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			start := time.Now()
			_, err := mfs.api.CopyObject(context.Background(), dst, src)
			metricsObserveS3Get(time.Since(start).Seconds())
			return err
		})
	}); err != nil {
		metricsIncS3Error()
		req.Error <- err
		return
	}

	// Step 2: Delete the old location.
	// If this fails, the object exists in BOTH locations (safe — no data loss).
	// A subsequent scan/rename will eventually clean up the stale copy.
	metricsIncS3Remove()
	delErr := mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			return mfs.api.RemoveObject(context.Background(), mfs.config.bucket, req.Source, minio.RemoveObjectOptions{})
		})
	})
	if delErr != nil {
		mfs.log.Printf("WARNING: rename: copy succeeded but delete of %s failed (stale copy remains): %s\n",
			req.Source, delErr)
		// Return nil — the rename logically succeeded (data is at the new location).
	}
	req.Error <- nil
}

func (mfs *MinFS) copyOp(req *CopyOperation) {
	dst := minio.CopyDestOptions{
		Bucket: mfs.config.bucket,
		Object: req.Target,
	}
	src := minio.CopySrcOptions{
		Bucket: mfs.config.bucket,
		Object: req.Source,
	}
	metricsIncS3Copy()
	req.Error <- mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			start := time.Now()
			_, err := mfs.api.CopyObject(context.Background(), dst, src)
			metricsObserveS3Get(time.Since(start).Seconds())
			return err
		})
	})
}

// multipartThreshold is the file size above which multipart upload is used.
// S3 single PUT limit is 5GB.
const multipartThreshold int64 = 5 * 1024 * 1024 * 1024

// multipartPartSize is the size of each part in a multipart upload (100MB).
const multipartPartSize int64 = 100 * 1024 * 1024

func (mfs *MinFS) putOp(req *PutOperation) {
	r, err := os.Open(req.Source)
	if err != nil {
		req.Error <- err
		return
	}
	defer r.Close()

	ops := minio.PutObjectOptions{
		ContentType: mime.TypeByExtension(filepath.Ext(req.Target)),
	}

	// Use multipart upload for files exceeding the threshold.
	if req.Length > multipartThreshold {
		ops.PartSize = uint64(multipartPartSize)
	}

	err = mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			if _, seekErr := r.Seek(0, 0); seekErr != nil {
				return seekErr
			}
			metricsIncS3Put()
			start := time.Now()
			_, err := mfs.api.PutObject(context.Background(), mfs.config.bucket, req.Target, r, req.Length, ops)
			metricsObserveS3Put(time.Since(start).Seconds())
			return err
		})
	})
	if err != nil {
		metricsIncS3Error()
		req.Error <- err
		return
	}
	mfs.log.Printf("Upload finished: %s -> %s.\n", req.Source, req.Target)
	req.Error <- nil
}

// syncWorker is a single worker goroutine that processes sync operations from syncChan.
func (mfs *MinFS) syncWorker() {
	defer mfs.workersWg.Done()
	for req := range mfs.syncChan {
		switch req := req.(type) {
		case *MoveOperation:
			mfs.moveOp(req)
		case *CopyOperation:
			mfs.copyOp(req)
		case *PutOperation:
			mfs.putOp(req)
		default:
			mfs.log.Printf("ERROR: unknown sync operation type: %T\n", req)
		}
	}
}

func (mfs *MinFS) startSync(numWorkers int) error {
	for i := 0; i < numWorkers; i++ {
		mfs.workersWg.Add(1)
		go mfs.syncWorker()
	}
	return nil
}

// Statfs will return meta information on the minio filesystem
func (mfs *MinFS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	resp.Blocks = 0x1000000000
	resp.Bfree = 0x1000000000
	resp.Bavail = 0x1000000000
	resp.Namelen = 32768
	resp.Bsize = 1024
	return nil
}

// Acquire will return a new FileHandle
func (mfs *MinFS) Acquire(f *File) (*FileHandle, error) {
	if err := mfs.Lock(f.FullPath()); err != nil {
		return nil, err
	}

	id := atomic.AddUint64(&mfs.handleIDCounter, 1)
	h := &FileHandle{
		f:      f,
		handle: id,
	}

	mfs.m.Lock()
	mfs.handles[id] = h
	mfs.m.Unlock()
	metricsIncActiveHandles()

	return h, nil
}

// Release release the filehandle
func (mfs *MinFS) Release(fh *FileHandle) error {
	mfs.m.Lock()
	delete(mfs.handles, fh.handle)
	mfs.m.Unlock()
	metricsDecActiveHandles()

	return mfs.Unlock(fh.f.FullPath())
}

// cacheEntry is used internally by evictCache for sorting by modification time.
type cacheEntry struct {
	path    string
	size    int64
	modTime time.Time
}

// evictCache enforces the cache disk quota by deleting the least-recently-used
// cache files until total size is within the configured limit. Uses in-memory
// LRU tracking for O(n) eviction instead of filesystem-based time sorting.
func (mfs *MinFS) evictCache() {
	quota := mfs.config.cacheQuota
	if quota <= 0 {
		return
	}

	entries, err := os.ReadDir(mfs.config.cache)
	if err != nil {
		mfs.log.Printf("WARNING: evictCache: ReadDir failed: %s\n", err)
		return
	}

	mfs.cacheAccessMu.Lock()
	defer mfs.cacheAccessMu.Unlock()

	var totalSize int64
	files := make([]cacheEntry, 0, len(entries))

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		fp := path.Join(mfs.config.cache, e.Name())
		totalSize += info.Size()

		// Use tracked access time if available, fall back to mod time.
		at := info.ModTime()
		if tracked, ok := mfs.cacheAccess[fp]; ok {
			at = tracked
		}

		files = append(files, cacheEntry{
			path:    fp,
			size:    info.Size(),
			modTime: at,
		})
	}

	if totalSize <= quota {
		return
	}

	// Sort oldest access time first (LRU eviction).
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})

	for _, f := range files {
		if totalSize <= quota {
			break
		}
		if err := os.Remove(f.path); err != nil {
			mfs.log.Printf("WARNING: evictCache: failed to remove %s: %s\n", f.path, err)
			continue
		}
		delete(mfs.cacheAccess, f.path)
		totalSize -= f.size
		mfs.log.Printf("INFO: evictCache: removed %s (%d bytes) to free quota\n", f.path, f.size)
	}
}

// touchCache records an access time for a cache file (used by LRU eviction).
func (mfs *MinFS) touchCache(cachePath string) {
	mfs.cacheAccessMu.Lock()
	mfs.cacheAccess[cachePath] = time.Now()
	mfs.cacheAccessMu.Unlock()
}

// NextSequence will return the next free iNode
func (mfs *MinFS) NextSequence(tx *meta.Tx) (sequence uint64, err error) {
	bucket := tx.Bucket("minio/")
	return bucket.NextSequence()
}

// Root is the root folder of the MinFS mountpoint
func (mfs *MinFS) Root() (fs.Node, error) {
	return &Dir{
		dir:  nil,
		mfs:  mfs,
		Path: "",

		UID:  mfs.config.uid,
		GID:  mfs.config.gid,
		Mode: os.ModeDir | 0750,
	}, nil
}

// Storer -
type Storer interface {
	store(tx *meta.Tx)
}

// NewCachePath generates a unique cache file path that does not collide
// with any existing file.
func (mfs *MinFS) NewCachePath() (string, error) {
	for {
		cachePath := path.Join(mfs.config.cache, nextSuffix())
		_, err := os.Stat(cachePath)
		if err != nil {
			if os.IsNotExist(err) {
				return cachePath, nil
			}
			return "", err
		}
		// Path already exists, try next one.
	}
}

// audit writes a structured audit log entry for write operations.
func (mfs *MinFS) audit(op, path, status string) {
	if mfs.auditLog == nil {
		return
	}
	mfs.auditLog.Printf("%s op=%s path=%q status=%s",
		time.Now().UTC().Format(time.RFC3339Nano), op, path, status)
}

// recordPending persists a pending upload entry so it survives process restarts.
// Uses the cache file basename as key (no '/' characters) to comply with ForEach.
func (mfs *MinFS) recordPending(remotePath, cachePath string, size int64) {
	p := pendingUpload{CachePath: cachePath, RemotePath: remotePath, Size: size}
	data, err := json.Marshal(p)
	if err != nil {
		mfs.log.Printf("WARNING: recordPending marshal: %s\n", err)
		return
	}
	if err := mfs.db.Update(func(tx *meta.Tx) error {
		b := tx.Bucket(pendingBucket)
		return b.Put(path.Base(cachePath), data)
	}); err != nil {
		mfs.log.Printf("WARNING: recordPending: %s\n", err)
	}
}

// clearPending removes a pending upload entry after successful upload.
func (mfs *MinFS) clearPending(cachePath string) {
	_ = mfs.db.Update(func(tx *meta.Tx) error {
		b := tx.Bucket(pendingBucket)
		return b.Delete(path.Base(cachePath))
	})
}

// recoverPending replays any incomplete uploads from a previous crash.
func (mfs *MinFS) recoverPending() {
	type entry struct {
		pu pendingUpload
	}
	var entries []entry

	_ = mfs.db.View(func(tx *meta.Tx) error {
		b := tx.Bucket(pendingBucket)
		return b.ForEach(func(k string, v interface{}) error {
			if data, ok := v.([]byte); ok {
				var pu pendingUpload
				if json.Unmarshal(data, &pu) == nil {
					entries = append(entries, entry{pu: pu})
				}
			}
			return nil
		})
	})

	if len(entries) == 0 {
		return
	}

	mfs.log.Printf("Recovering %d pending upload(s) from previous session...\n", len(entries))
	for _, e := range entries {
		f, err := os.Open(e.pu.CachePath)
		if err != nil {
			if os.IsNotExist(err) {
				mfs.log.Printf("WARNING: cache file %s missing, skipping %s\n", e.pu.CachePath, e.pu.RemotePath)
				mfs.clearPending(e.pu.CachePath)
			}
			continue
		}

		err = mfs.breaker.Execute(func() error {
			return retryWithBackoff(func() error {
				if _, seekErr := f.Seek(0, 0); seekErr != nil {
					return seekErr
				}
				_, putErr := mfs.api.PutObject(context.Background(), mfs.config.bucket,
					e.pu.RemotePath, f, e.pu.Size, minio.PutObjectOptions{})
				return putErr
			})
		})
		f.Close()

		if err != nil {
			mfs.log.Printf("WARNING: recovery upload %s failed: %s\n", e.pu.RemotePath, err)
			continue
		}

		os.Remove(e.pu.CachePath)
		mfs.clearPending(e.pu.CachePath)
		mfs.log.Printf("Recovered: %s\n", e.pu.RemotePath)
	}
}
