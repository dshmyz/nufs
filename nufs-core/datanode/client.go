package datanode

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/metadata"
)

// Client is a data node client for inter-node chunk replication.
type Client struct {
	addr    string
	conn    net.Conn
	mu      sync.Mutex
	seq     atomic.Uint64
	timeout time.Duration
	tlsCfg  *tls.Config // nil = plain TCP
	closed  atomic.Bool
}

// NewClient creates a new data node client.
func NewClient(addr string) *Client {
	return &Client{addr: addr, timeout: 30 * time.Second}
}

// NewTLSClient creates a data node client that connects over TLS.
func NewTLSClient(addr string, cfg tlsutil.Config) (*Client, error) {
	tlsCfg, err := tlsutil.ClientConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("datanode: tls client config: %w", err)
	}
	return &Client{addr: addr, timeout: 30 * time.Second, tlsCfg: tlsCfg}, nil
}

// SetTimeout sets the operation timeout for read/write operations.
func (c *Client) SetTimeout(d time.Duration) {
	c.timeout = d
}

// Connect establishes a TCP connection to the data node.
// When the client was created with NewTLSClient, the connection
// is upgraded to TLS automatically.
func (c *Client) Connect() error {
	return c.connectLocked()
}

// connectLocked establishes the underlying TCP/TLS connection. The
// caller is responsible for synchronization (c.mu, or the single-
// threaded construction path in the pool's dial).
func (c *Client) connectLocked() error {
	if c.tlsCfg != nil {
		conn, err := tls.DialWithDialer(
			&net.Dialer{Timeout: 10 * time.Second},
			"tcp", c.addr, c.tlsCfg,
		)
		if err != nil {
			return fmt.Errorf("datanode: tls connect to %s: %w", c.addr, err)
		}
		c.conn = conn
		return nil
	}
	// Use net.DialTimeout to avoid hanging on unreachable nodes
	conn, err := net.DialTimeout("tcp", c.addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("datanode: connect to %s: %w", c.addr, err)
	}
	c.conn = conn
	return nil
}

// closeConnLocked closes the underlying connection and clears it so a
// subsequent connectLocked can establish a fresh one. Caller holds c.mu.
func (c *Client) closeConnLocked() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// Close closes the connection.
func (c *Client) Close() error {
	c.closed.Store(true)
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// IsClosed reports whether the client has been closed.
func (c *Client) IsClosed() bool {
	return c.closed.Load()
}

// WriteChunk sends a chunk write request.
func (c *Client) WriteChunk(chunkID metadata.ChunkID, data []byte) (*Response, error) {
	return c.WriteChunkGen(chunkID, 0, data)
}

// WriteChunkGen sends a chunk write request carrying a metadata-issued
// generation (Metadata V2 fencing). A generation of 0 leaves the receiving
// datanode to its local generation (legacy behavior).
func (c *Client) WriteChunkGen(chunkID metadata.ChunkID, generation uint64, data []byte) (*Response, error) {
	header := &Header{
		Type:       ReqWriteChunk,
		ChunkID:    chunkID,
		Generation: generation,
		Length:     int32(len(data)),
		Checksum:   crc32.ChecksumIEEE(data),
		RequestID:  c.seq.Add(1),
	}
	return c.sendRequest(header, data)
}

// ReadECShard fetches one EC shard extent from a peer datanode (the read side
// of the S3 cross-node coordination — the coordinator assembles shards from the
// nodes that own them to verify an aggregate). Returns the shard bytes and the
// peer's recorded checksum.
func (c *Client) ReadECShard(chunkID metadata.ChunkID, shardIndex int) (*Response, error) {
	header := &Header{
		Type:       ReqReadECShard,
		ChunkID:    chunkID,
		ShardIndex: shardIndex,
		RequestID:  c.seq.Add(1),
	}
	return c.sendRequest(header, nil)
}
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
	return c.ReplicateChunkGen(chunkID, 0, data)
}

// ReplicateChunkGen sends a chunk replication request carrying a
// metadata-issued generation (Metadata V2 fencing). A generation of 0 means
// "unspecified" and the receiving datanode keeps its own local generation
// (legacy behavior).
func (c *Client) ReplicateChunkGen(chunkID metadata.ChunkID, generation uint64, data []byte) (*Response, error) {
	header := &Header{
		Type:       ReqReplicateChunk,
		ChunkID:    chunkID,
		Generation: generation,
		Length:     int32(len(data)),
		Checksum:   crc32.ChecksumIEEE(data),
		RequestID:  c.seq.Add(1),
	}
	return c.sendRequest(header, data)
}

// ReplicateECShard sends a single EC shard to a peer datanode for placement
// on its shard store. It is the coordinator->peer push in a cross-node EC
// conversion (S3): the coordinating datanode encodes the shard and pushes the
// bytes to the peer that owns that shard index. disk (when >=0) names the exact
// target shard-store index on the peer per the planned §14 placement; -1 lets
// the peer route via its own shard selection. The peer's Server routes it to
// its shard store (ReqReplicateECShard).
func (c *Client) ReplicateECShard(chunkID metadata.ChunkID, shardIndex, disk int, data []byte) (*Response, error) {
	extra := map[string]string{}
	if disk >= 0 {
		extra["disk"] = strconv.Itoa(disk)
	}
	header := &Header{
		Type:       ReqReplicateECShard,
		ChunkID:    chunkID,
		ShardIndex: shardIndex,
		Length:     int32(len(data)),
		Checksum:   crc32.ChecksumIEEE(data),
		RequestID:  c.seq.Add(1),
		Extra:      extra,
	}
	return c.sendRequest(header, data)
}

// sendRequest sends a request and reads the response. If the underlying
// connection is dead (server closed it, TCP reset, idle reaped, etc.) the
// first attempt fails with a network error; sendRequest then reconnects
// once and retries. This makes connection-pool reuse transparent to
// callers: a stale pooled connection does not surface as a failed
// operation when the peer is reachable.
//
// All datanode RPCs are idempotent under retry (WriteChunk/ReplicateChunk
// overwrite the same chunkID with the same data; ReadChunk is a pure
// read), so a single retry is always safe.
func (c *Client) sendRequest(header *Header, body []byte) (*Response, error) {
	headerData, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	bodyLen := uint32(0)
	if body != nil {
		bodyLen = uint32(len(body))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		if err := c.connectLocked(); err != nil {
			return nil, err
		}
	}

	resp, err := c.attemptRequest(headerData, body, bodyLen)
	if err == nil {
		return resp, nil
	}

	// The connection is broken. Discard it, reconnect, and retry once.
	// If the peer is genuinely unreachable the redial fails fast and we
	// surface that error.
	lastErr := err
	c.closeConnLocked()
	if err := c.connectLocked(); err != nil {
		return nil, fmt.Errorf("datanode: reconnect to %s after failure: %w (original: %v)", c.addr, err, lastErr)
	}
	return c.attemptRequest(headerData, body, bodyLen)
}

// attemptRequest performs a single request/response exchange on c.conn.
// Caller must hold c.mu and ensure c.conn != nil. Any error returned
// indicates the connection is unusable.
func (c *Client) attemptRequest(headerData, body []byte, bodyLen uint32) (*Response, error) {
	// Apply deadline for the full operation
	if c.timeout > 0 {
		if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
			return nil, fmt.Errorf("datanode: set deadline: %w", err)
		}
		defer c.conn.SetDeadline(time.Time{})
	}

	// Write header
	if err := binary.Write(c.conn, binary.BigEndian, uint32(len(headerData))); err != nil {
		return nil, fmt.Errorf("datanode: write header length: %w", err)
	}
	if _, err := c.conn.Write(headerData); err != nil {
		return nil, fmt.Errorf("datanode: write header: %w", err)
	}

	// Write body
	if err := binary.Write(c.conn, binary.BigEndian, bodyLen); err != nil {
		return nil, fmt.Errorf("datanode: write body length: %w", err)
	}
	if bodyLen > 0 {
		if _, err := c.conn.Write(body); err != nil {
			return nil, fmt.Errorf("datanode: write body: %w", err)
		}
	}

	// Read response using binary framing (no base64 overhead)
	resp, err := readResponse(c.conn)
	if err != nil {
		return nil, fmt.Errorf("datanode: read response: %w", err)
	}
	return resp, nil
}
