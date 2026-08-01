package s3fs

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var errTimeout = errors.New("s3fs: lock timeout")

// S3FileSystem is the root node of the S3-backed FUSE filesystem.
type S3FileSystem struct {
	fs.Inode

	config *Config
	api    *minio.Client
	cache  *PebbleCache

	mu       sync.RWMutex // 使用 RWMutex 优化读多写少场景
	handles  map[uint64]*S3FileHandle
	handleID uint64
	locks    map[string]*pathLock

	lockWait time.Duration // Wait timeout; 0 = defaultLockWaitTimeout

	syncChan  chan interface{}
	workersWg sync.WaitGroup
	breaker   *circuitBreaker

	metricsSrv *http.Server
	shutdownCh chan struct{}
}

// New creates a new S3FileSystem.
func New(cfg *Config, options ...Option) (*S3FileSystem, error) {
	for _, opt := range options {
		opt(cfg)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Ensure cache directory exists.
	if err := os.MkdirAll(cfg.CacheDir, 0700); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	// Open metadata cache.
	cache, err := OpenCache(path.Join(cfg.CacheDir, "cache.db"))
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}

	// Load credentials.
	ac := LoadCredentials(cfg.CacheDir)

	// Create S3 client.
	var transport http.RoundTripper = &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableCompression:  true,
	}
	if cfg.Insecure {
		transport = &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        100,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableCompression:  true,
			TLSClientConfig:     insecureTLSConfig(),
		}
	}

	creds := credentials.NewStaticV4(ac.AccessKey, ac.SecretKey, ac.SecretToken)
	api, err := minio.New(cfg.Target.Host, &minio.Options{
		Creds:     creds,
		Secure:    cfg.Target.Scheme == "https",
		Transport: transport,
	})
	if err != nil {
		cache.Close()
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	// Validate bucket exists.
	exists, err := api.BucketExists(context.Background(), cfg.Bucket)
	if err != nil {
		cache.Close()
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		cache.Close()
		return nil, fmt.Errorf("bucket %q does not exist", cfg.Bucket)
	}

	mfs := &S3FileSystem{
		config:    cfg,
		api:       api,
		cache:     cache,
		handles:   make(map[uint64]*S3FileHandle),
		locks:     newLockMap(),
		syncChan:  make(chan interface{}, 100),
		breaker:   newCircuitBreaker(5, 30*time.Second),
		shutdownCh: make(chan struct{}),
	}

	// Start sync workers.
	mfs.startSync(4)

	// Start metrics server.
	if cfg.MetricsAddr != "" {
		mfs.metricsSrv = StartMetricsServer(cfg.MetricsAddr)
		slog.Info("s3fs: metrics server listening", "addr", cfg.MetricsAddr)
	}

	// Recover pending uploads from previous crash.
	go mfs.recoverPending()

	return mfs, nil
}

// Root returns the root directory node.
func (fsys *S3FileSystem) Root() (fs.InodeEmbedder, error) {
	rootID := fsys.cache.NextID()
	root := &S3Dir{
		mfs:     fsys,
		dir:     nil,
		Path:    "",
		InodeID: rootID,
		Mode:    os.ModeDir | 0750,
		UID:     fsys.config.UID,
		GID:     fsys.config.GID,
		Chgtime: time.Now(),
		Crtime:  time.Now(),
		Mtime:   time.Now(),
		Atime:   time.Now(),
	}
	if err := fsys.cache.PutInode(root.toCacheInode()); err != nil {
		return nil, err
	}
	return root, nil
}

// Serve mounts the filesystem and starts serving FUSE requests.
func (fsys *S3FileSystem) Serve(mountpoint string) error {
	defer fsys.shutdown()

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:        "s3fs",
			FsName:      "s3fs",
			AllowOther:  true,
			SyncRead:    true, // Disable async read for remote storage
		},
		EntryTimeout: &fsys.config.ScanTTL,
		AttrTimeout:  &fsys.config.ScanTTL,
	}

	server, err := fs.Mount(mountpoint, fsys, opts)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}

	// Trap signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fsys.shutdown()
		server.Unmount()
	}()

	server.Wait()
	return nil
}

func (fsys *S3FileSystem) shutdown() {
	select {
	case <-fsys.shutdownCh:
		return // already shutdown
	default:
		close(fsys.shutdownCh)
	}

	// Stop sync workers.
	close(fsys.syncChan)
	fsys.workersWg.Wait()

	// Close metrics server.
	if fsys.metricsSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fsys.metricsSrv.Shutdown(ctx)
		cancel()
	}

	// Close cache.
	fsys.cache.Close()
}

// Acquire returns a new FileHandle for the given file.
func (fsys *S3FileSystem) Acquire(f *S3File) (*S3FileHandle, error) {
	if err := fsys.Lock(f.FullPath()); err != nil {
		return nil, err
	}

	id := atomic.AddUint64(&fsys.handleID, 1)
	h := &S3FileHandle{
		f:      f,
		handle: id,
	}
	fsys.mu.Lock()
	fsys.handles[id] = h
	fsys.mu.Unlock()
	metricsIncActiveHandles()
	return h, nil
}

// Release releases a file handle.
func (fsys *S3FileSystem) Release(fh *S3FileHandle) {
	fsys.mu.Lock()
	delete(fsys.handles, fh.handle)
	fsys.mu.Unlock()
	metricsDecActiveHandles()
	fsys.Unlock(fh.f.FullPath())
}

// sync submits an operation to the async upload queue.
func (fsys *S3FileSystem) sync(op interface{}) error {
	fsys.syncChan <- op
	return nil
}

func (fsys *S3FileSystem) startSync(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		fsys.workersWg.Add(1)
		go fsys.syncWorker()
	}
}

func (fsys *S3FileSystem) syncWorker() {
	defer fsys.workersWg.Done()
	for op := range fsys.syncChan {
		switch op := op.(type) {
		case *MoveOperation:
			fsys.moveOp(op)
		case *CopyOperation:
			fsys.copyOp(op)
		case *PutOperation:
			fsys.putOp(op)
		}
	}
}

func (fsys *S3FileSystem) moveOp(op *MoveOperation) {
	dst := minio.CopyDestOptions{Bucket: fsys.config.Bucket, Object: op.Target}
	src := minio.CopySrcOptions{Bucket: fsys.config.Bucket, Object: op.Source}
	metricsIncS3Copy()

	err := fsys.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			start := time.Now()
			_, err := fsys.api.CopyObject(context.Background(), dst, src)
			metricsObserveS3Get(time.Since(start).Seconds())
			return err
		})
	})
	if err != nil {
		metricsIncS3Error()
		op.Error <- err
		return
	}

	metricsIncS3Remove()
	delErr := fsys.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			return fsys.api.RemoveObject(context.Background(), fsys.config.Bucket, op.Source, minio.RemoveObjectOptions{})
		})
	})
	if delErr != nil {
		slog.Warn("s3fs: rename ok but delete failed", "source", op.Source, "error", delErr)
	}
	op.Error <- nil
}

func (fsys *S3FileSystem) copyOp(op *CopyOperation) {
	dst := minio.CopyDestOptions{Bucket: fsys.config.Bucket, Object: op.Target}
	src := minio.CopySrcOptions{Bucket: fsys.config.Bucket, Object: op.Source}
	metricsIncS3Copy()
	op.Error <- fsys.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			start := time.Now()
			_, err := fsys.api.CopyObject(context.Background(), dst, src)
			metricsObserveS3Get(time.Since(start).Seconds())
			return err
		})
	})
}

const multipartThreshold int64 = 5 * 1024 * 1024 * 1024
const multipartPartSize int64 = 100 * 1024 * 1024

func (fsys *S3FileSystem) putOp(op *PutOperation) {
	r, err := os.Open(op.Source)
	if err != nil {
		op.Error <- err
		return
	}
	defer r.Close()

	ops := minio.PutObjectOptions{}
	if op.Length > multipartThreshold {
		ops.PartSize = uint64(multipartPartSize)
	}

	err = fsys.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			if _, seekErr := r.Seek(0, 0); seekErr != nil {
				return seekErr
			}
			metricsIncS3Put()
			start := time.Now()
			_, err := fsys.api.PutObject(context.Background(), fsys.config.Bucket, op.Target, r, op.Length, ops)
			metricsObserveS3Put(time.Since(start).Seconds())
			return err
		})
	})
	if err != nil {
		metricsIncS3Error()
	}
	op.Error <- err
}

func (fsys *S3FileSystem) recoverPending() {
	entries, err := fsys.cache.ListPending()
	if err != nil || len(entries) == 0 {
		return
	}
	slog.Info("s3fs: recovering pending uploads", "count", len(entries))
	for _, pu := range entries {
		f, err := os.Open(pu.CachePath)
		if err != nil {
			if os.IsNotExist(err) {
				fsys.cache.ClearPending(pu.CachePath)
			}
			continue
		}
		err = fsys.breaker.Execute(func() error {
			return retryWithBackoff(func() error {
				if _, seekErr := f.Seek(0, 0); seekErr != nil {
					return seekErr
				}
				_, putErr := fsys.api.PutObject(context.Background(), fsys.config.Bucket,
					pu.RemotePath, f, pu.Size, minio.PutObjectOptions{})
				return putErr
			})
		})
		f.Close()
		if err != nil {
			slog.Warn("s3fs: recovery upload failed", "path", pu.RemotePath, "error", err)
			continue
		}
		os.Remove(pu.CachePath)
		fsys.cache.ClearPending(pu.CachePath)
		slog.Info("s3fs: recovered upload", "path", pu.RemotePath)
	}
}

// NewCachePath generates a unique cache file path.
func (fsys *S3FileSystem) NewCachePath() (string, error) {
	for {
		cachePath := path.Join(fsys.config.CacheDir, fmt.Sprintf("cache-%d-%d", time.Now().UnixNano(), os.Getpid()))
		_, err := os.Stat(cachePath)
		if os.IsNotExist(err) {
			return cachePath, nil
		}
		if err != nil {
			return "", err
		}
	}
}

func insecureTLSConfig() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true} //nolint:gosec
}
