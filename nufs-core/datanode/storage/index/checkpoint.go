package index

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"

	"github.com/example/dfs/datanode/storage"
)

// Checkpoint is a point-in-time, durable snapshot of the extent index
// plus the metadata needed to resume recovery (§7.3). It is published
// atomically: built in a temp directory, synced, then renamed into
// place. The latest retainCheckpoints are kept.
type Checkpoint struct {
	// Sequence is the last index WAL sequence fully included.
	Sequence uint64
	// Dir is the published checkpoint directory.
	Dir string
}

// CheckpointOpts configures checkpoint publication.
type CheckpointOpts struct {
	// Dir is the checkpoints/ root.
	Dir string
	// Retain is how many checkpoints to keep (default 3).
	Retain int
	// UseInMemory uses an in-memory source (tests).
	Source *pebble.DB
}

// Create builds and atomically publishes a checkpoint of the index.
// It returns the checkpoint's sequence.
func Create(ix *Index, seq uint64, opts CheckpointOpts) (*Checkpoint, error) {
	retain := opts.Retain
	if retain <= 0 {
		retain = storage.DefaultRetainCheckpoints
	}
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	// Build into a temp directory.
	tmp := filepath.Join(opts.Dir, fmt.Sprintf("checkpoint-%d.tmp", seq))
	_ = os.RemoveAll(tmp)
	if err := ix.DB().Checkpoint(tmp); err != nil {
		return nil, fmt.Errorf("storage: checkpoint: %w", err)
	}
	// Persist the sequence in a sidecar so recovery knows where to
	// resume WAL replay.
	seqFile := filepath.Join(tmp, "sequence")
	if err := os.WriteFile(seqFile, []byte(fmt.Sprintf("%d", seq)), 0644); err != nil {
		return nil, err
	}
	// Sync the checkpoint dir before publishing.
	if err := syncDir(tmp); err != nil {
		return nil, err
	}

	final := filepath.Join(opts.Dir, fmt.Sprintf("checkpoint-%d", seq))
	_ = os.RemoveAll(final)
	if err := os.Rename(tmp, final); err != nil {
		return nil, err
	}
	if err := syncDir(opts.Dir); err != nil {
		return nil, err
	}

	// Prune old checkpoints, keeping the newest `retain`.
	if err := pruneCheckpoints(opts.Dir, retain); err != nil {
		return nil, err
	}

	return &Checkpoint{Sequence: seq, Dir: final}, nil
}

// Load opens the newest valid checkpoint. Returns (nil, nil) if no
// checkpoint exists (fresh disk — recovery replays from scratch).
func Load(opts CheckpointOpts) (*Checkpoint, *Index, error) {
	dirs, err := listCheckpoints(opts.Dir)
	if err != nil {
		return nil, nil, err
	}
	if len(dirs) == 0 {
		return nil, nil, nil
	}
	// Newest first.
	newest := dirs[len(dirs)-1]
	seq, err := readSequence(newest)
	if err != nil {
		return nil, nil, err
	}
	// Reopen the Pebble checkpoint as the index.
	ix, err := Open(Options{Dir: newest, UseInMemory: false})
	if err != nil {
		return nil, nil, fmt.Errorf("storage: open checkpoint %s: %w", newest, err)
	}
	return &Checkpoint{Sequence: seq, Dir: newest}, ix, nil
}

// readSequence reads the sequence sidecar from a checkpoint dir.
func readSequence(dir string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(dir, "sequence"))
	if err != nil {
		return 0, err
	}
	var seq uint64
	if _, err := fmt.Sscanf(string(data), "%d", &seq); err != nil {
		return 0, err
	}
	return seq, nil
}

// listCheckpoints returns existing checkpoint dirs sorted by sequence.
func listCheckpoints(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var dirs []string
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) < 11 || e.Name()[:11] != "checkpoint-" {
			continue
		}
		if isTmp(e.Name()) {
			continue
		}
		dirs = append(dirs, filepath.Join(dir, e.Name()))
	}
	sortStrings(dirs)
	return dirs, nil
}

// isTmp reports whether a name is a temp checkpoint dir.
func isTmp(name string) bool {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return i+4 <= len(name) && name[i:i+4] == ".tmp"
		}
	}
	return false
}

// sortStrings sorts a string slice (checkpoint dirs by lexical order,
// which equals sequence order given zero-padded names).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// pruneCheckpoints removes all but the newest `retain` checkpoints.
func pruneCheckpoints(dir string, retain int) error {
	dirs, err := listCheckpoints(dir)
	if err != nil {
		return err
	}
	for i := 0; i < len(dirs)-retain; i++ {
		if err := os.RemoveAll(dirs[i]); err != nil {
			return err
		}
	}
	return nil
}

// syncDir fsyncs a directory.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
