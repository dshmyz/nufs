package datanode

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"time"

	"github.com/example/dfs/metadata"
)

// Server is the data node TCP server that handles chunk read/write/replicate requests.
type Server struct {
	cfg        Config
	store      *ChunkStore
	listener   net.Listener
	wg         sync.WaitGroup
	running    atomic.Bool
	requestSeq atomic.Uint64
}

// NewServer creates a new data node server.
func NewServer(cfg Config, store *ChunkStore) *Server {
	return &Server{
		cfg:   cfg,
		store: store,
	}
}

// Start begins listening for incoming connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("datanode: listen %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = ln
	s.running.Store(true)

	log.Printf("datanode: server listening on %s (node_id=%d)", s.cfg.ListenAddr, s.cfg.NodeID)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	if !s.running.Swap(false) {
		return
	}
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	log.Printf("datanode: server stopped")
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for s.running.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running.Load() {
				log.Printf("datanode: accept error: %v", err)
			}
			continue
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for s.running.Load() {
		header, body, err := readMessage(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("datanode: read message: %v", err)
			}
			return
		}

		resp := s.dispatch(header, body)
		if err := writeResponse(conn, resp); err != nil {
			log.Printf("datanode: write response: %v", err)
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

	if err := s.store.Write(header.ChunkID, data); err != nil {
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
	if err := s.store.Write(header.ChunkID, data); err != nil {
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

func writeResponse(w io.Writer, resp *Response) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("datanode: marshal response: %w", err)
	}

	// Write length-prefixed response
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("datanode: write response length: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("datanode: write response: %w", err)
	}
	return nil
}

// ========== Client (for inter-node communication) ==========

// Client is a data node client for inter-node chunk replication.
type Client struct {
	addr string
	conn net.Conn
	mu   sync.Mutex
	seq  atomic.Uint64
}

// NewClient creates a new data node client.
func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

// Connect establishes a TCP connection to the data node.
func (c *Client) Connect() error {
	// Use net.DialTimeout to avoid hanging on unreachable nodes
	conn, err := net.DialTimeout("tcp", c.addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("datanode: connect to %s: %w", c.addr, err)
	}
	c.conn = conn
	return nil
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// WriteChunk sends a chunk write request.
func (c *Client) WriteChunk(chunkID metadata.ChunkID, data []byte) (*Response, error) {
	header := &Header{
		Type:      ReqWriteChunk,
		ChunkID:   chunkID,
		Length:    int32(len(data)),
		Checksum:  crc32.ChecksumIEEE(data),
		RequestID: c.seq.Add(1),
	}
	return c.sendRequest(header, data)
}

// ReadChunk sends a chunk read request.
func (c *Client) ReadChunk(chunkID metadata.ChunkID, offset int64, length int32) (*Response, error) {
	header := &Header{
		Type:      ReqReadChunk,
		ChunkID:   chunkID,
		Offset:    offset,
		Length:    length,
		RequestID: c.seq.Add(1),
	}
	return c.sendRequest(header, nil)
}

// ReplicateChunk sends a chunk replication request.
func (c *Client) ReplicateChunk(chunkID metadata.ChunkID, data []byte) (*Response, error) {
	header := &Header{
		Type:      ReqReplicateChunk,
		ChunkID:   chunkID,
		Length:    int32(len(data)),
		Checksum:  crc32.ChecksumIEEE(data),
		RequestID: c.seq.Add(1),
	}
	return c.sendRequest(header, data)
}

func (c *Client) sendRequest(header *Header, body []byte) (*Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, fmt.Errorf("datanode: not connected")
	}

	// Write header
	headerData, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint32(len(headerData))); err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(headerData); err != nil {
		return nil, err
	}

	// Write body
	bodyLen := uint32(0)
	if body != nil {
		bodyLen = uint32(len(body))
	}
	if err := binary.Write(c.conn, binary.BigEndian, bodyLen); err != nil {
		return nil, err
	}
	if bodyLen > 0 {
		if _, err := c.conn.Write(body); err != nil {
			return nil, err
		}
	}

	// Read response
	var respLen uint32
	if err := binary.Read(c.conn, binary.BigEndian, &respLen); err != nil {
		return nil, fmt.Errorf("datanode: read response length: %w", err)
	}
	respData := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respData); err != nil {
		return nil, fmt.Errorf("datanode: read response: %w", err)
	}

	var resp Response
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("datanode: unmarshal response: %w", err)
	}
	return &resp, nil
}

func isChunkNotFound(err error) bool {
	return err != nil && errors.Is(err, metadata.ErrChunkNotFound)
}
