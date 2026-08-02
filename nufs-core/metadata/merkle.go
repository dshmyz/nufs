package metadata

import (
	"fmt"
)

// Merkle reconciliation (V2.1 §12): when two nodes' partition summaries
// differ for a partition, they build a temporary Merkle subtree from a
// stable index snapshot, narrow the differing ranges, and exchange pages
// of at most 4096 entries. Foreground writes never update a full Merkle
// path; the Merkle tree is built on demand for reconciliation only.

// MerkleNode is one node of the temporary Merkle subtree.
type MerkleNode struct {
	// Range is the extent-ID range covered by this node.
	StartExtent uint64 `json:"start_extent"`
	EndExtent   uint64 `json:"end_extent"` // exclusive
	// Hash is the XOR of extent hashes in the range (commutative).
	Hash uint64 `json:"hash"`
	// Children are non-nil for internal nodes.
	Children []*MerkleNode `json:"children,omitempty"`
	// Leaf lists the extents in this node's range (only for leaves).
	Leaf []ExtentKey `json:"leaf,omitempty"`
}

// ExtentKey identifies one extent (ID + generation) for exchange.
type ExtentKey struct {
	ExtentID   uint64 `json:"extent_id"`
	Generation uint64 `json:"generation"`
}

// MerklePageSize is the exchange page bound (§12: pages of at most 4096
// entries).
const MerklePageSize = 4096

// MerkleTree is a temporary subtree built from a stable index snapshot.
type MerkleTree struct {
	root *MerkleNode
}

// hashExtentRange hashes an extent range (reuses hashExtent).
func hashExtentRange(keys []ExtentKey) uint64 {
	var h uint64
	for _, k := range keys {
		h ^= hashExtent(k.ExtentID, k.Generation)
	}
	return h
}

// BuildMerkle builds a balanced binary Merkle tree over a sorted extent
// key list. The tree is built on demand (reconciliation only) and its
// internal nodes carry commutative hashes so narrowing can skip
// unchanged subtrees.
func BuildMerkle(keys []ExtentKey) *MerkleTree {
	root := buildNode(keys, 0, uint64(len(keys)))
	return &MerkleTree{root: root}
}

func buildNode(keys []ExtentKey, start, end uint64) *MerkleNode {
	if end-start <= MerklePageSize {
		// Leaf: up to one page of extents.
		leaf := append([]ExtentKey(nil), keys[start:end]...)
		var startID, endID uint64
		if len(leaf) > 0 {
			startID = leaf[0].ExtentID
			endID = leaf[len(leaf)-1].ExtentID + 1
		}
		return &MerkleNode{
			StartExtent: startID,
			EndExtent:   endID,
			Hash:        hashExtentRange(leaf),
			Leaf:        leaf,
		}
	}
	mid := (start + end) / 2
	left := buildNode(keys, start, mid)
	right := buildNode(keys, mid, end)
	return &MerkleNode{
		StartExtent: left.StartExtent,
		EndExtent:   right.EndExtent,
		Hash:        left.Hash ^ right.Hash,
		Children:    []*MerkleNode{left, right},
	}
}

// DiffNodes returns the leaf nodes whose hashes differ from another
// tree (after summary comparison flagged the partition). The caller
// exchanges only these leaves' pages (§12).
func (m *MerkleTree) DiffNodes(other *MerkleTree) []*MerkleNode {
	var out []*MerkleNode
	collectDiff(m.root, other.root, &out)
	return out
}

func collectDiff(a, b *MerkleNode, out *[]*MerkleNode) {
	if a == nil || b == nil {
		return
	}
	if a.Hash == b.Hash {
		return // unchanged subtree
	}
	if a.Children == nil && b.Children == nil {
		*out = append(*out, a)
		return
	}
	// Recurse into matching children (trees are balanced identically).
	if len(a.Children) == 2 && len(b.Children) == 2 {
		collectDiff(a.Children[0], b.Children[0], out)
		collectDiff(a.Children[1], b.Children[1], out)
	}
}

var _ = fmt.Sprintf
