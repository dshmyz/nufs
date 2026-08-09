package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============================================================
// Fuzz tests — protocol parsing and boundary inputs
// ============================================================
//
// Run with: go test -fuzz=Fuzz -fuzztime=30s ./datanode/
//

// FuzzReadMessage fuzzes the wire protocol parser (readMessage).
// It exercises header length limits, malformed JSON, oversized bodies,
// and boundary values.
func FuzzReadMessage(f *testing.F) {
	// Seed corpus: valid message
	header := Header{
		Type:      ReqWriteChunk,
		ChunkID:   1,
		Offset:    0,
		Length:    4,
		Checksum:  0,
		RequestID: 1,
	}
	headerJSON, _ := json.Marshal(header)
	body := []byte("test")

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(len(headerJSON)))
	buf.Write(headerJSON)
	binary.Write(&buf, binary.BigEndian, uint32(len(body)))
	buf.Write(body)
	f.Add(buf.Bytes())

	// Seed: empty input
	f.Add([]byte{})

	// Seed: just header length = 0
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})

	// Seed: header length exceeds limit
	f.Add([]byte{0, 1, 0, 0}) // 65536 > 64KB limit

	// Seed: valid header length but truncated JSON
	f.Add([]byte{0, 0, 0, 5, '{', '"', 't'})

	// Seed: body length exceeds limit
	validHeader := append([]byte{0, 0, 0, 2}, '{', '}')
	f.Add(append(validHeader, 0x08, 0, 0, 0)) // 128MB+ body length

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bytes.NewReader(data)
		_, _, _ = readMessage(reader)
		// readMessage should never panic; it returns errors for bad input
	})
}

// FuzzReadMessageHeaderJSON fuzzes the JSON unmarshalling of the Header.
// This catches issues with unexpected JSON fields, types, and edge values.
func FuzzReadMessageHeaderJSON(f *testing.F) {
	// Valid header JSON
	f.Add(`{"type":1,"chunk_id":1,"offset":0,"length":0,"checksum":0,"request_id":1}`)
	// Empty object
	f.Add(`{}`)
	// Unexpected types
	f.Add(`{"type":"invalid","chunk_id":"bad"}`)
	// Very large numbers
	f.Add(`{"chunk_id":9223372036854775807,"offset":-1,"length":2147483647}`)
	// Null fields
	f.Add(`{"type":null,"chunk_id":null}`)
	// Extra unknown fields
	f.Add(`{"type":1,"unknown_field":"x","chunk_id":1}`)
	// Nested objects
	f.Add(`{"type":1,"extra":{"key":"value"}}`)
	// Array instead of object
	f.Add(`[1,2,3]`)
	// String instead of object
	f.Add(`"not an object"`)

	f.Fuzz(func(t *testing.T, headerStr string) {
		var header Header
		_ = json.Unmarshal([]byte(headerStr), &header)
		// Should not panic regardless of input
	})
}

// FuzzDispatch fuzzes the request dispatcher with various request types
// and body payloads. This verifies that dispatch handles all request
// types gracefully without panicking.
func FuzzDispatch(f *testing.F) {
	// Seed: write request with body
	f.Add(int(ReqWriteChunk), int64(1), int32(4), uint32(0), []byte("data"))
	// Seed: read request, no body
	f.Add(int(ReqReadChunk), int64(1), int32(0), uint32(0), []byte{})
	// Seed: delete request
	f.Add(int(ReqDeleteChunk), int64(1), int32(0), uint32(0), []byte{})
	// Seed: info request
	f.Add(int(ReqChunkInfo), int64(1), int32(0), uint32(0), []byte{})
	// Seed: health request
	f.Add(int(ReqHealth), int64(0), int32(0), uint32(0), []byte{})
	// Seed: unknown request type
	f.Add(int(255), int64(0), int32(0), uint32(0), []byte{})
	// Seed: negative chunk ID
	f.Add(int(ReqReadChunk), int64(-1), int32(-1), uint32(0), []byte("x"))
	// Seed: very large body
	largeBody := make([]byte, 1024*1024) // 1MB
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}
	f.Add(int(ReqWriteChunk), int64(1), int32(int32(len(largeBody))), uint32(0), largeBody)

	f.Fuzz(func(t *testing.T, reqType int, chunkID int64, length int32, checksum uint32, body []byte) {
		// Limit body size to avoid OOM in fuzz
		if len(body) > 2*1024*1024 {
			t.Skip()
		}

		dir := t.TempDir()
		store, err := NewChunkStore(dir, 4, 4, nil)
		if err != nil {
			t.Fatal(err)
		}

		cfg := DefaultConfig()
		srv := NewServer(cfg, store)

		header := &Header{
			Type:      RequestType(reqType),
			ChunkID:   metadata.ChunkID(chunkID),
			Offset:    0,
			Length:    length,
			Checksum:  checksum,
			RequestID: 1,
		}

		// dispatch should never panic
		_ = srv.dispatch(header, body)
	})
}

// FuzzWriteResponse fuzzes the response serialization.
func FuzzWriteResponse(f *testing.F) {
	// Valid response
	f.Add(uint64(1), int(0), "", []byte("ok"))
	// Empty data
	f.Add(uint64(0), int(1), "error", []byte{})
	// Large data
	f.Add(uint64(999), int(0), "", make([]byte, 65536))
	// Error with special characters
	f.Add(uint64(1), int(2), "error\x00with\x01binary", []byte(""))

	f.Fuzz(func(t *testing.T, requestID uint64, status int, errMsg string, data []byte) {
		if len(data) > 1*1024*1024 {
			t.Skip()
		}
		resp := &Response{
			RequestID: requestID,
			Status:    ResponseStatus(status & 0xFF),
			Error:     errMsg,
			Data:      data,
			Length:    int32(len(data)),
		}

		var buf bytes.Buffer
		_ = writeResponse(&buf, resp)
		// Should not panic
	})
}

// FuzzRoundTrip fuzzes the write-then-read round trip of the wire protocol.
// Ensures that a valid message can always be read back correctly.
func FuzzRoundTrip(f *testing.F) {
	f.Add(int(ReqWriteChunk), int64(42), int32(10), uint32(12345), []byte("hello world"))
	f.Add(int(ReqReadChunk), int64(1), int32(0), uint32(0), []byte{})
	f.Add(int(ReqDeleteChunk), int64(999), int32(0), uint32(0), []byte{})

	f.Fuzz(func(t *testing.T, reqType int, chunkID int64, length int32, checksum uint32, body []byte) {
		if len(body) > 64*1024 {
			t.Skip()
		}

		header := &Header{
			Type:      RequestType(reqType),
			ChunkID:   metadata.ChunkID(chunkID),
			Offset:    0,
			Length:    length,
			Checksum:  checksum,
			RequestID: 1,
		}

		// Serialize
		headerJSON, err := json.Marshal(header)
		if err != nil {
			return // invalid input, skip
		}

		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint32(len(headerJSON)))
		buf.Write(headerJSON)
		binary.Write(&buf, binary.BigEndian, uint32(len(body)))
		if len(body) > 0 {
			buf.Write(body)
		}

		// Deserialize
		parsedHeader, parsedBody, err := readMessage(&buf)
		if err != nil {
			return // expected for some fuzz inputs
		}

		// Verify round-trip consistency
		if parsedHeader.Type != header.Type {
			t.Errorf("type mismatch: got %d, want %d", parsedHeader.Type, header.Type)
		}
		if parsedHeader.ChunkID != header.ChunkID {
			t.Errorf("chunkID mismatch: got %d, want %d", parsedHeader.ChunkID, header.ChunkID)
		}
		if len(parsedBody) != len(body) {
			t.Errorf("body length mismatch: got %d, want %d", len(parsedBody), len(body))
		}
	})
}
