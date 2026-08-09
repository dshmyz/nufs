package main

// ============================================================
// stat / ns — query a file or directory's metadata by path.
//
//   nufs-cli stat <bucket>/<dir>/<file>   # inode metadata + xattrs + chunks
//   nufs-cli ns   <bucket>/<dir>          # list directory entries (like ls)
//
// Both work in local mode (direct Pebble access) and remote mode
// (metad ops HTTP API). A bucket path is resolved from the bucket's
// root inode by walking each path segment via Lookup(parent, name),
// mirroring how the S3 gateway resolves an object key.
// ============================================================

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/example/dfs/metadata"
)

// nsPort abstracts namespace reads so the same stat/ns logic can run
// against a local PebbleStore or the remote metad ops API.
type nsPort interface {
	GetBucket(name string) (*metadata.BucketInfo, error)
	Lookup(parent metadata.InodeID, name string) (*metadata.InodeMeta, error)
	GetInode(id metadata.InodeID) (*metadata.InodeMeta, error)
	ReadDir(parent metadata.InodeID) ([]metadata.DirEntry, error)
}

// localNS implements nsPort over the embedded PebbleStore.
type localNS struct {
	ctx   context.Context
	store *metadata.PebbleStore
}

func (l *localNS) GetBucket(name string) (*metadata.BucketInfo, error) {
	return l.store.GetBucket(l.ctx, name)
}
func (l *localNS) Lookup(parent metadata.InodeID, name string) (*metadata.InodeMeta, error) {
	return l.store.Lookup(l.ctx, parent, name)
}
func (l *localNS) GetInode(id metadata.InodeID) (*metadata.InodeMeta, error) {
	return l.store.GetInode(l.ctx, id)
}
func (l *localNS) ReadDir(parent metadata.InodeID) ([]metadata.DirEntry, error) {
	return l.store.ReadDir(l.ctx, parent, 0, 10000)
}

// remoteNS implements nsPort over the metad ops HTTP API.
type remoteNS struct {
	api *remoteAPI
}

func (r *remoteNS) GetBucket(name string) (*metadata.BucketInfo, error) {
	resp := r.api.get("/api/v1/buckets/" + url.PathEscape(name))
	var b metadata.BucketInfo
	if err := json.Unmarshal(resp, &b); err != nil {
		return nil, fmt.Errorf("decode bucket: %w", err)
	}
	return &b, nil
}

func (r *remoteNS) Lookup(parent metadata.InodeID, name string) (*metadata.InodeMeta, error) {
	q := url.Values{}
	q.Set("parent", fmt.Sprintf("%d", parent))
	q.Set("name", name)
	resp := r.api.get("/api/v1/namespace/lookup?" + q.Encode())
	var m metadata.InodeMeta
	if err := json.Unmarshal(resp, &m); err != nil {
		return nil, fmt.Errorf("decode inode: %w", err)
	}
	return &m, nil
}

func (r *remoteNS) GetInode(id metadata.InodeID) (*metadata.InodeMeta, error) {
	resp := r.api.get(fmt.Sprintf("/api/v1/inodes/%d", id))
	var m metadata.InodeMeta
	if err := json.Unmarshal(resp, &m); err != nil {
		return nil, fmt.Errorf("decode inode: %w", err)
	}
	return &m, nil
}

func (r *remoteNS) ReadDir(parent metadata.InodeID) ([]metadata.DirEntry, error) {
	resp := r.api.get(fmt.Sprintf("/api/v1/namespace/readdir?parent=%d&limit=10000", parent))
	var entries []metadata.DirEntry
	if err := json.Unmarshal(resp, &entries); err != nil {
		return nil, fmt.Errorf("decode readdir: %w", err)
	}
	return entries, nil
}

// splitBucketPath splits "<bucket>/<dir>/<file>" into the bucket name and
// the remaining slash-separated path segments (empty if the bucket root
// itself is the target).
func splitBucketPath(arg string) (bucket string, segments []string) {
	arg = strings.TrimPrefix(arg, "/")
	out, rest, _ := strings.Cut(arg, "/")
	bucket = out
	if rest == "" {
		return bucket, nil
	}
	for _, seg := range strings.Split(rest, "/") {
		if seg != "" {
			segments = append(segments, seg)
		}
	}
	return bucket, segments
}

// resolvePath walks from a bucket's root inode through each path segment,
// returning the final inode. If no segments are given, returns the bucket
// root inode.
func resolvePath(p nsPort, bucket string, segments []string) (*metadata.InodeMeta, error) {
	b, err := p.GetBucket(bucket)
	if err != nil {
		return nil, fmt.Errorf("bucket %q: %w", bucket, err)
	}
	cur := b.RootInode
	for _, seg := range segments {
		m, err := p.Lookup(cur, seg)
		if err != nil {
			return nil, fmt.Errorf("path %q in bucket %q: %w", seg, bucket, err)
		}
		cur = m.ID
	}
	return p.GetInode(cur)
}

// cmdStat prints a single inode's full metadata: type/size/times, xattrs
// and its chunk map.
func cmdStat(p nsPort, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: nufs-cli stat <bucket>[/dir/.../file]")
		os.Exit(1)
	}
	bucket, segments := splitBucketPath(args[0])
	inode, err := resolvePath(p, bucket, segments)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Path:\t%s\n", args[0])
	w.Flush()
	printInodeMeta(inode)
}

// cmdNS lists the directory entries under a bucket path, like `ls`.
func cmdNS(p nsPort, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: nufs-cli ns <bucket>[/dir/...]")
		os.Exit(1)
	}
	bucket, segments := splitBucketPath(args[0])
	dir, err := resolvePath(p, bucket, segments)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ns: %v\n", err)
		os.Exit(1)
	}
	if dir.Type != metadata.FileDirectory {
		fmt.Fprintf(os.Stderr, "ns: %s is not a directory\n", args[0])
		os.Exit(1)
	}
	entries, err := p.ReadDir(dir.ID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ns: readdir: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tTYPE\tINODE")
	for _, e := range entries {
		marker := ""
		if e.Type == metadata.FileDirectory {
			marker = "/"
		}
		fmt.Fprintf(w, "%s%s\t%s\t%d\n", e.Name, marker, fileTypeString(e.Type), e.InodeID)
	}
	w.Flush()
	fmt.Printf("\nTotal: %d entries\n", len(entries))
}

func printInodeMeta(inode *metadata.InodeMeta) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Inode ID:\t%d\n", inode.ID)
	fmt.Fprintf(w, "Type:\t%s\n", fileTypeString(inode.Type))
	fmt.Fprintf(w, "Mode:\t%04o\n", inode.Mode)
	fmt.Fprintf(w, "Size:\t%d\n", inode.Size)
	fmt.Fprintf(w, "Links:\t%d\n", inode.NLink)
	fmt.Fprintf(w, "UID/GID:\t%d/%d\n", inode.UID, inode.GID)
	fmt.Fprintf(w, "Created:\t%s\n", ts(inode.CTime))
	fmt.Fprintf(w, "Modified:\t%s\n", ts(inode.MTime))
	fmt.Fprintf(w, "Accessed:\t%s\n", ts(inode.ATime))
	switch inode.Type {
	case metadata.FileSymlink:
		fmt.Fprintf(w, "Symlink:\t%s\n", inode.Symlink)
	}
	w.Flush()

	if len(inode.XAttrs) > 0 {
		fmt.Println("\nXAttrs:")
		aw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		for k, v := range inode.XAttrs {
			fmt.Fprintf(aw, "  %s\t%s\n", k, printableBytes(v))
		}
		aw.Flush()
	}

	if len(inode.ChunkMap) > 0 {
		fmt.Println("\nChunks:")
		cw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(cw, "  CHUNK_ID\tOFFSET\tLENGTH\tVERSION")
		for _, c := range inode.ChunkMap {
			fmt.Fprintf(cw, "  %d\t%d\t%d\t%d\n", c.ID, c.Offset, c.Length, c.Version)
		}
		cw.Flush()
	} else if inode.Type == metadata.FileRegular {
		fmt.Println("\nChunks: (none)")
	}
}

func fileTypeString(t metadata.FileType) string {
	switch t {
	case metadata.FileRegular:
		return "regular"
	case metadata.FileDirectory:
		return "directory"
	case metadata.FileSymlink:
		return "symlink"
	case metadata.FileFIFO:
		return "fifo"
	case metadata.FileCharDevice:
		return "char-device"
	case metadata.FileBlockDevice:
		return "block-device"
	case metadata.FileSocket:
		return "socket"
	default:
		return fmt.Sprintf("type(%d)", t)
	}
}

func ts(unixNano int64) string {
	if unixNano == 0 {
		return "-"
	}
	return time.Unix(0, unixNano).Format(time.RFC3339)
}

func printableBytes(b []byte) string {
	// Render byte slices that look printable as text, otherwise base64.
	printable := true
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			printable = false
			break
		}
	}
	if !printable {
		return fmt.Sprintf("<%d bytes: %x>", len(b), b)
	}
	return string(b)
}
