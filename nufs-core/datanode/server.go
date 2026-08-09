package datanode

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// LocalChunkStore is the interface the TCP server requires from a
// storage backend. Both the legacy ChunkStore and the V2.1 adapter
// implement it, so the server can serve either engine.
type LocalChunkStore interface {
	Write(chunkID metadata.ChunkID, data []byte) error
	// WriteGen writes chunk data under a metadata-issued generation
	// (Metadata V2 fencing). generation==0 means "unspecified": the backend
	// uses its own local generation (legacy V1 behavior). Backends without a
	// generation concept may implement it as a plain Write.
	WriteGen(chunkID metadata.ChunkID, generation uint64, data []byte) error
	Read(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, uint32, error)
	Delete(chunkID metadata.ChunkID) error
	Seal(chunkID metadata.ChunkID) (uint32, error)
	Info(chunkID metadata.ChunkID) (*LocalChunkInfo, bool)
	ListChunks() []LocalChunkInfo
	Stats() (totalBytes int64, chunkCount int64)
}

// Server is the data node TCP server that handles chunk read/write/replicate requests.
// It supports connection limiting, request-level timeouts, and backpressure
// to protect against connection storms and slow clients.
type Server struct {
	cfg        Config
	store      LocalChunkStore
	listener   net.Listener
	wg         sync.WaitGroup
	running    atomic.Bool
	requestSeq atomic.Uint64

	// Connection management
	connSem          chan struct{} // Semaphore limiting concurrent connections
	activeConn       atomic.Int64  // Current active connection count
	reqTimeout       time.Duration // Per-request timeout (0 = no timeout)
	slowReqThreshold time.Duration // Log warnings for requests exceeding this duration

	// Active connection tracking for graceful shutdown. Stop() closes
	// every live connection so blocked handleConn goroutines exit
	// immediately instead of waiting for reqTimeout.
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
}

// NewServer creates a new data node server.
func NewServer(cfg Config, store LocalChunkStore) *Server {
	maxConns := cfg.MaxConnections
	if maxConns <= 0 {
		maxConns = 256 // Default: 256 concurrent connections
	}
	reqTimeout := cfg.RequestTimeout
	if reqTimeout <= 0 {
		reqTimeout = 30 * time.Second
	}
	return &Server{
		cfg:              cfg,
		store:            store,
		connSem:          make(chan struct{}, maxConns),
		reqTimeout:       reqTimeout,
		slowReqThreshold: 500 * time.Millisecond, // Log slow requests > 500ms
		conns:            make(map[net.Conn]struct{}),
	}
}

// Start begins listening for incoming connections.
// When Config.TLS is enabled, the listener wraps connections with TLS.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("datanode: listen %s: %w", s.cfg.ListenAddr, err)
	}

	// Wrap with TLS if configured
	if s.cfg.TLS.Enabled() {
		tlsCfg, err := tlsutil.ServerConfig(s.cfg.TLS)
		if err != nil {
			ln.Close()
			return fmt.Errorf("datanode: tls config: %w", err)
		}
		ln = tls.NewListener(ln, tlsCfg)
	}

	s.listener = ln
	s.running.Store(true)

	scheme := "tcp"
	if s.cfg.TLS.Enabled() {
		scheme = "tls"
	}
	slog.Info("datanode: server listening", "addr", s.cfg.ListenAddr, "scheme", scheme, "nodeID", s.cfg.NodeID)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop gracefully shuts down the server. It stops accepting new
// connections, then actively closes every live connection so that
// handleConn goroutines blocked reading the next request exit
// immediately instead of waiting for reqTimeout.
func (s *Server) Stop() {
	if !s.running.Swap(false) {
		return
	}
	if s.listener != nil {
		s.listener.Close()
	}
	// Close all active connections to unblock handleConn goroutines.
	s.connMu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.connMu.Unlock()
	s.wg.Wait()
	slog.Info("datanode: server stopped")
}

// registerConn tracks a live connection so Stop() can close it.
func (s *Server) registerConn(conn net.Conn) {
	s.connMu.Lock()
	s.conns[conn] = struct{}{}
	s.connMu.Unlock()
}

// unregisterConn removes a connection that has finished serving.
func (s *Server) unregisterConn(conn net.Conn) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

// Addr returns the address the server is listening on. It is only valid
// after Start() returns successfully; before that it returns "".
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for s.running.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running.Load() {
				slog.Error("datanode: accept error", "error", err)
			}
			continue
		}

		// Connection limiting: reject if at capacity
		select {
		case s.connSem <- struct{}{}:
			// Got a slot
		default:
			slog.Warn("datanode: rejecting connection, max connections reached",
				"remote", conn.RemoteAddr(), "max", cap(s.connSem))
			conn.Close()
			continue
		}

		s.activeConn.Add(1)
		s.registerConn(conn)
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer s.unregisterConn(conn)
	defer func() {
		<-s.connSem // Release connection slot
		s.activeConn.Add(-1)
	}()

	// Set initial deadline for the first request
	if s.reqTimeout > 0 {
		conn.SetDeadline(time.Now().Add(s.reqTimeout))
	}

	for s.running.Load() {
		header, body, err := readMessage(conn)
		if err != nil {
			if err != io.EOF {
				slog.Error("datanode: read message", "error", err)
			}
			return
		}

		// Reset deadline for each request (backpressure: slow clients get disconnected)
		if s.reqTimeout > 0 {
			conn.SetDeadline(time.Now().Add(s.reqTimeout))
		}

		start := time.Now()
		resp := s.dispatch(header, body)
		elapsed := time.Since(start)

		if elapsed > s.slowReqThreshold {
			slog.Warn("datanode: slow request",
				"type", header.Type,
				"chunkID", header.ChunkID,
				"requestID", header.RequestID,
				"duration", elapsed,
				"remote", conn.RemoteAddr().String(),
			)
		}

		if err := writeResponse(conn, resp); err != nil {
			slog.Error("datanode: write response", "error", err)
			return
		}
	}
}

// dispatch routes a request to the appropriate handler.
func (s *Server) dispatch(header *Header, body []byte) *Response {
	switch header.Type {
	case ReqWriteChunk:
		return s.handleWrite(header, body)
	case ReqReadChunk:
		return s.handleRead(header)
	case ReqDeleteChunk:
		return s.handleDelete(header)
	case ReqReplicateChunk:
		return s.handleReplicate(header, body)
	case ReqReplicateECShard:
		return s.handleReplicateECShard(header, body)
	case ReqReadECShard:
		return s.handleReadECShard(header)
	case ReqChunkInfo:
		return s.handleChunkInfo(header)
	case ReqListChunks:
		return s.handleListChunks(header)
	case ReqHealth:
		return s.handleHealth(header)
	default:
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     "unknown request type",
		}
	}
}

// ========== Request Handlers ==========

func (s *Server) handleWrite(header *Header, data []byte) *Response {
	// Verify checksum if provided
	if header.Checksum != 0 {
		actual := crc32.ChecksumIEEE(data)
		if actual != header.Checksum {
			return &Response{
				RequestID: header.RequestID,
				Status:    StatusError,
				Error:     "checksum mismatch",
			}
		}
	}

	if err := s.writeWithGen(header, data); err != nil {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     err.Error(),
		}
	}

	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Length:    int32(len(data)),
	}
}

// writeWithGen routes a write to the store, honoring the metadata-issued
// generation when the request carries one; otherwise it falls back to the
// store's plain Write (legacy local-generation behavior).
func (s *Server) writeWithGen(header *Header, data []byte) error {
	if header.Generation != 0 {
		return s.store.WriteGen(header.ChunkID, header.Generation, data)
	}
	return s.store.Write(header.ChunkID, data)
}

func (s *Server) handleRead(header *Header) *Response {
	data, checksum, err := s.store.Read(header.ChunkID, header.Offset, header.Length)
	if err != nil {
		status := StatusError
		if isChunkNotFound(err) {
			status = StatusNotFound
		}
		return &Response{
			RequestID: header.RequestID,
			Status:    status,
			Error:     err.Error(),
		}
	}

	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Data:      data,
		Length:    int32(len(data)),
		Checksum:  checksum,
	}
}

func (s *Server) handleDelete(header *Header) *Response {
	if err := s.store.Delete(header.ChunkID); err != nil {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     err.Error(),
		}
	}
	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
	}
}

func (s *Server) handleReplicate(header *Header, data []byte) *Response {
	// Replication write: same as regular write but marks chunk as replica
	if err := s.writeWithGen(header, data); err != nil {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     err.Error(),
		}
	}

	// Seal the chunk so CRC is set and integrity checks work on subsequent reads.
	if _, err := s.store.Seal(header.ChunkID); err != nil {
		slog.Warn("datanode: seal after replicate failed", "chunkID", header.ChunkID, "error", err)
	}

	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Length:    int32(len(data)),
	}
}

// handleReplicateECShard writes a single EC shard onto this datanode's shard
// store. It is the server side of the coordinator push in a cross-node EC
// conversion (S3): a coordinating datanode that does NOT own a shard pushes the
// shard bytes here. The store must expose WriteShard (V2Store with attached
// shard stores); otherwise the request fails cleanly.
func (s *Server) handleReplicateECShard(header *Header, data []byte) *Response {
	if header.Checksum != 0 {
		actual := crc32.ChecksumIEEE(data)
		if actual != header.Checksum {
			return &Response{
				RequestID: header.RequestID,
				Status:    StatusError,
				Error:     "checksum mismatch",
			}
		}
	}
	ws, ok := s.store.(interface {
		WriteShard(metadata.ChunkID, int, []byte) error
	})
	if !ok {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     "store does not support EC shard writes",
		}
	}
	// The coordinator may name the exact target disk (planned §14 placement). An
	// absent/zero disk falls back to the store's own shard routing (WriteShard).
	// When the store exposes the landing-preference write, a tombstoned landing
	// disk (a self-heal/repair push re-writing a deleted shard's generation)
	// falls back to a healthy shard disk instead of failing the §14 fence.
	if d, err := strconv.Atoi(header.Extra["disk"]); err == nil && d > 0 {
		if wsd, ok2 := s.store.(interface {
			WriteShardAtDiskPref(metadata.ChunkID, int, int, []byte) error
		}); ok2 {
			if err := wsd.WriteShardAtDiskPref(header.ChunkID, header.ShardIndex, d, data); err != nil {
				return &Response{
					RequestID: header.RequestID,
					Status:    StatusError,
					Error:     err.Error(),
				}
			}
			return &Response{RequestID: header.RequestID, Status: StatusOK, Length: int32(len(data))}
		}
		if wsd, ok2 := s.store.(interface {
			WriteShardAtDisk(metadata.ChunkID, int, int, []byte) error
		}); ok2 {
			if err := wsd.WriteShardAtDisk(header.ChunkID, header.ShardIndex, d, data); err != nil {
				return &Response{
					RequestID: header.RequestID,
					Status:    StatusError,
					Error:     err.Error(),
				}
			}
			return &Response{RequestID: header.RequestID, Status: StatusOK, Length: int32(len(data))}
		}
	}
	if err := ws.WriteShard(header.ChunkID, header.ShardIndex, data); err != nil {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     err.Error(),
		}
	}
	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Length:    int32(len(data)),
	}
}

// handleReadECShard reads one EC shard extent and returns its bytes. It is
// the server side of the coordinator's cross-node verify (S3): the coordinator
// fetches every shard from the node that owns it and decodes the aggregate.
func (s *Server) handleReadECShard(header *Header) *Response {
	rs, ok := s.store.(interface {
		ReadShard(metadata.ChunkID, int) ([]byte, uint32, error)
	})
	if !ok {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     "store does not support EC shard reads",
		}
	}
	data, sum, err := rs.ReadShard(header.ChunkID, header.ShardIndex)
	if err != nil {
		status := StatusError
		if err == storage.ErrExtentNotFound {
			status = StatusNotFound
		}
		return &Response{
			RequestID: header.RequestID,
			Status:    status,
			Error:     err.Error(),
		}
	}
	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Data:      data,
		Length:    int32(len(data)),
		Checksum:  sum,
	}
}

func (s *Server) handleChunkInfo(header *Header) *Response {
	info, ok := s.store.Info(header.ChunkID)
	if !ok {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusNotFound,
			Error:     "chunk not found",
		}
	}

	data, err := json.Marshal(info)
	if err != nil {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     err.Error(),
		}
	}

	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Data:      data,
		Length:    int32(len(data)),
	}
}

func (s *Server) handleListChunks(header *Header) *Response {
	chunks := s.store.ListChunks()
	data, err := json.Marshal(chunks)
	if err != nil {
		return &Response{
			RequestID: header.RequestID,
			Status:    StatusError,
			Error:     err.Error(),
		}
	}
	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Data:      data,
		Length:    int32(len(data)),
	}
}

func (s *Server) handleHealth(header *Header) *Response {
	totalBytes, chunkCount := s.store.Stats()
	health := map[string]interface{}{
		"node_id":     s.cfg.NodeID,
		"total_bytes": totalBytes,
		"chunk_count": chunkCount,
		"capacity_gb": s.cfg.CapacityGB,
	}
	data, _ := json.Marshal(health)
	return &Response{
		RequestID: header.RequestID,
		Status:    StatusOK,
		Data:      data,
		Length:    int32(len(data)),
	}
}

// ========== Wire Protocol ==========
//
// Message format:
//   [4 bytes: header_len (big-endian uint32)]
//   [header_len bytes: Header JSON]
//   [4 bytes: body_len (big-endian uint32)]
//   [body_len bytes: body data]

func readMessage(r io.Reader) (*Header, []byte, error) {
	// Read header length
	var headerLen uint32
	if err := binary.Read(r, binary.BigEndian, &headerLen); err != nil {
		return nil, nil, err
	}
	if headerLen > 64*1024 { // sanity check: max 64KB header
		return nil, nil, fmt.Errorf("datanode: header too large: %d", headerLen)
	}

	// Read header JSON
	headerData := make([]byte, headerLen)
	if _, err := io.ReadFull(r, headerData); err != nil {
		return nil, nil, fmt.Errorf("datanode: read header: %w", err)
	}

	var header Header
	if err := json.Unmarshal(headerData, &header); err != nil {
		return nil, nil, fmt.Errorf("datanode: unmarshal header: %w", err)
	}

	// Read body length
	var bodyLen uint32
	if err := binary.Read(r, binary.BigEndian, &bodyLen); err != nil {
		return nil, nil, fmt.Errorf("datanode: read body length: %w", err)
	}
	if bodyLen > 128*1024*1024 { // sanity check: max 128MB body
		return nil, nil, fmt.Errorf("datanode: body too large: %d", bodyLen)
	}

	// Read body
	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, nil, fmt.Errorf("datanode: read body: %w", err)
		}
	}

	return &header, body, nil
}

// responseHeader is the JSON-serialized portion of a Response.
// The Data field is excluded so that binary chunk data is written
// verbatim after the header, avoiding the ~33% base64 expansion
// that json.Marshal would apply to []byte fields.
type responseHeader struct {
	RequestID uint64         `json:"request_id"`
	Status    ResponseStatus `json:"status"`
	Error     string         `json:"error,omitempty"`
	Length    int32          `json:"length"`
	Checksum  uint32         `json:"checksum"`
}

const (
	maxResponseHeaderLen = 64 * 1024         // 64KB
	maxResponseDataLen   = 128 * 1024 * 1024 // 128MB
)

// writeResponse serializes a Response using a binary framing that
// avoids base64-encoding the Data field. Wire format:
//
//	[4-byte header_len][header JSON][4-byte data_len][raw data bytes]
//
// The header JSON contains all metadata (status, checksum, etc.)
// without the Data payload, so it stays small. The Data bytes are
// written directly after the data length prefix.
func writeResponse(w io.Writer, resp *Response) error {
	hdr := responseHeader{
		RequestID: resp.RequestID,
		Status:    resp.Status,
		Error:     resp.Error,
		Length:    resp.Length,
		Checksum:  resp.Checksum,
	}
	hdrData, err := json.Marshal(&hdr)
	if err != nil {
		return fmt.Errorf("datanode: marshal response header: %w", err)
	}

	// Write header length + header JSON
	if err := binary.Write(w, binary.BigEndian, uint32(len(hdrData))); err != nil {
		return fmt.Errorf("datanode: write response header length: %w", err)
	}
	if _, err := w.Write(hdrData); err != nil {
		return fmt.Errorf("datanode: write response header: %w", err)
	}

	// Write data length + raw data bytes
	dataLen := uint32(len(resp.Data))
	if err := binary.Write(w, binary.BigEndian, dataLen); err != nil {
		return fmt.Errorf("datanode: write response data length: %w", err)
	}
	if dataLen > 0 {
		if _, err := w.Write(resp.Data); err != nil {
			return fmt.Errorf("datanode: write response data: %w", err)
		}
	}
	return nil
}

// readResponse deserializes a Response from the binary framing
// produced by writeResponse. It is the counterpart used by clients
// to parse server replies without base64 overhead.
func readResponse(r io.Reader) (*Response, error) {
	var hdrLen uint32
	if err := binary.Read(r, binary.BigEndian, &hdrLen); err != nil {
		return nil, fmt.Errorf("datanode: read response header length: %w", err)
	}
	if hdrLen > maxResponseHeaderLen {
		return nil, fmt.Errorf("datanode: response header too large: %d", hdrLen)
	}

	hdrData := make([]byte, hdrLen)
	if _, err := io.ReadFull(r, hdrData); err != nil {
		return nil, fmt.Errorf("datanode: read response header: %w", err)
	}

	var hdr responseHeader
	if err := json.Unmarshal(hdrData, &hdr); err != nil {
		return nil, fmt.Errorf("datanode: unmarshal response header: %w", err)
	}

	var dataLen uint32
	if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
		return nil, fmt.Errorf("datanode: read response data length: %w", err)
	}
	if dataLen > maxResponseDataLen {
		return nil, fmt.Errorf("datanode: response data too large: %d", dataLen)
	}

	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("datanode: read response data: %w", err)
		}
	}

	return &Response{
		RequestID: hdr.RequestID,
		Status:    hdr.Status,
		Error:     hdr.Error,
		Data:      data,
		Length:    hdr.Length,
		Checksum:  hdr.Checksum,
	}, nil
}

func isChunkNotFound(err error) bool {
	return err != nil && errors.Is(err, metadata.ErrChunkNotFound)
}
