package metadata

import (
	"encoding/binary"
	"fmt"
)

// Cross-shard rename (V2.1 §11.6): same-shard rename uses one Raft
// batch. Cross-shard namespace rename uses a dedicated coordinator
// record, source and target prepare records, one durable commit
// decision, idempotent application, and a recovery worker. Prepare lease
// expiry never independently decides rollback; the durable coordinator
// decision is authoritative.

// TxnState is the coordinator lifecycle.
type TxnState uint8

const (
	TxnPreparing TxnState = iota
	TxnCommitted
	TxnApplied
)

func (s TxnState) String() string {
	switch s {
	case TxnPreparing:
		return "preparing"
	case TxnCommitted:
		return "committed"
	case TxnApplied:
		return "applied"
	default:
		return "unknown"
	}
}

// CrossShardRename is the coordinator record for one cross-shard rename
// transaction (§11.6).
type CrossShardRename struct {
	// TxnID uniquely identifies the transaction.
	TxnID uint64 `json:"txn_id"`
	// State is Preparing → Committed → Applied.
	State TxnState `json:"state"`
	// SourceShard/TargetShard are the physical Raft groups.
	SourceShard uint32 `json:"source_shard"`
	TargetShard uint32 `json:"target_shard"`
	// SourceDir/SourceName locate the entry being moved.
	SourceDir  InodeID `json:"source_dir"`
	SourceName string  `json:"source_name"`
	// TargetDir/TargetName locate the destination.
	TargetDir  InodeID `json:"target_dir"`
	TargetName string  `json:"target_name"`
	// ChildInode is the inode being moved (unchanged by the rename).
	ChildInode InodeID `json:"child_inode"`
	// CommitDecision is the durable commit decision. It is written once
	// and is authoritative; prepare-lease expiry never rolls it back.
	CommitDecision bool `json:"commit_decision"`
	// Prepared indicates both prepare records are durable.
	Prepared bool `json:"prepared"`
}

// CrossShardTxnStore manages cross-shard rename transactions.
type CrossShardTxnStore struct {
	store *PebbleStore
}

// NewCrossShardTxnStore creates the cross-shard txn store.
func NewCrossShardTxnStore(store *PebbleStore) *CrossShardTxnStore {
	return &CrossShardTxnStore{store: store}
}

// txnKey formats a transaction key.
func txnKey(txnID uint64) string {
	return fmt.Sprintf("%s%d", prefixCrossShardTxn, txnID)
}

// Get reads a transaction record.
func (s *CrossShardTxnStore) Get(txnID uint64) (*CrossShardRename, error) {
	var txn CrossShardRename
	exists, err := s.store.getValue(txnKey(txnID), &txn)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &txn, nil
}

// Put writes a transaction record (through Raft).
func (s *CrossShardTxnStore) Put(txn *CrossShardRename) error {
	return s.store.putMsgpack(txnKey(txn.TxnID), txn)
}

// Begin creates the coordinator record in Preparing state. It persists
// source/target identity; nothing is moved yet.
func (s *CrossShardTxnStore) Begin(txnID uint64, srcShard, tgtShard uint32, srcDir InodeID, srcName string, tgtDir InodeID, tgtName string, child InodeID) (*CrossShardRename, error) {
	txn := &CrossShardRename{
		TxnID:        txnID,
		State:        TxnPreparing,
		SourceShard:  srcShard,
		TargetShard:  tgtShard,
		SourceDir:    srcDir,
		SourceName:   srcName,
		TargetDir:    tgtDir,
		TargetName:   tgtName,
		ChildInode:   child,
		Prepared:     false,
		CommitDecision: false,
	}
	if err := s.Put(txn); err != nil {
		return nil, err
	}
	return txn, nil
}

// MarkPrepared records that both source and target prepare records are
// durable (§11.6: source and target prepare records).
func (s *CrossShardTxnStore) MarkPrepared(txnID uint64) (*CrossShardRename, error) {
	txn, err := s.Get(txnID)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		return nil, fmt.Errorf("metadata: cross-shard txn %d not found", txnID)
	}
	txn.Prepared = true
	if err := s.Put(txn); err != nil {
		return nil, err
	}
	return txn, nil
}

// Commit writes the durable commit decision (§11.6: "one durable commit
// decision"). After this, recovery applies the decision idempotently; a
// prepare-lease expiry never independently decides rollback.
func (s *CrossShardTxnStore) Commit(txnID uint64) (*CrossShardRename, error) {
	txn, err := s.Get(txnID)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		return nil, fmt.Errorf("metadata: cross-shard txn %d not found", txnID)
	}
	if !txn.Prepared {
		return nil, fmt.Errorf("metadata: cross-shard txn %d not prepared", txnID)
	}
	txn.CommitDecision = true
	txn.State = TxnCommitted
	if err := s.Put(txn); err != nil {
		return nil, err
	}
	return txn, nil
}

// MarkApplied transitions the transaction to Applied once the source
// and target entries are durably moved.
func (s *CrossShardTxnStore) MarkApplied(txnID uint64) (*CrossShardRename, error) {
	txn, err := s.Get(txnID)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		return nil, fmt.Errorf("metadata: cross-shard txn %d not found", txnID)
	}
	txn.State = TxnApplied
	if err := s.Put(txn); err != nil {
		return nil, err
	}
	return txn, nil
}

// ResolveDecision returns the authoritative outcome for a transaction,
// used by the recovery worker (§11.6): if a durable commit decision
// exists it must be applied; otherwise the transaction is still
// preparing and can be abandoned (never rolled back independently).
func (s *CrossShardTxnStore) ResolveDecision(txnID uint64) (*CrossShardRename, error) {
	txn, err := s.Get(txnID)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		return nil, nil
	}
	return txn, nil
}

var _ = binary.BigEndian
