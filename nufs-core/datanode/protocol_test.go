package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"
)

// TestWriteReadResponse_RoundTrip verifies that writeResponse and
// readResponse correctly serialize and deserialize a Response with
// arbitrary binary data, including empty and large payloads.
func TestWriteReadResponse_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		resp *Response
	}{
		{
			name: "simple ok with data",
			resp: &Response{
				RequestID: 42,
				Status:    StatusOK,
				Data:      []byte("hello world"),
				Length:    11,
				Checksum:  0x12345678,
			},
		},
		{
			name: "error response no data",
			resp: &Response{
				RequestID: 99,
				Status:    StatusError,
				Error:     "chunk not found",
				Length:    0,
			},
		},
		{
			name: "empty data slice",
			resp: &Response{
				RequestID: 7,
				Status:    StatusOK,
				Data:      []byte{},
				Length:    0,
			},
		},
		{
			name: "nil data",
			resp: &Response{
				RequestID: 3,
				Status:    StatusOK,
				Data:      nil,
				Length:    0,
			},
		},
		{
			name: "binary data with null bytes",
			resp: &Response{
				RequestID: 100,
				Status:    StatusOK,
				Data:      []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x7F},
				Length:    7,
				Checksum:  0xDEADBEEF,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeResponse(&buf, tc.resp); err != nil {
				t.Fatalf("writeResponse: %v", err)
			}

			got, err := readResponse(&buf)
			if err != nil {
				t.Fatalf("readResponse: %v", err)
			}

			if got.RequestID != tc.resp.RequestID {
				t.Errorf("RequestID: got %d, want %d", got.RequestID, tc.resp.RequestID)
			}
			if got.Status != tc.resp.Status {
				t.Errorf("Status: got %d, want %d", got.Status, tc.resp.Status)
			}
			if got.Error != tc.resp.Error {
				t.Errorf("Error: got %q, want %q", got.Error, tc.resp.Error)
			}
			if got.Length != tc.resp.Length {
				t.Errorf("Length: got %d, want %d", got.Length, tc.resp.Length)
			}
			if got.Checksum != tc.resp.Checksum {
				t.Errorf("Checksum: got %d, want %d", got.Checksum, tc.resp.Checksum)
			}
			if !bytes.Equal(got.Data, tc.resp.Data) {
				t.Errorf("Data: got %v, want %v", got.Data, tc.resp.Data)
			}
		})
	}
}

// TestWriteResponse_NoBase64Overhead verifies that the wire format
// does NOT base64-encode the Data field. With the old JSON+base64
// format, 1000 bytes of data would serialize to ~1336 bytes of JSON
// (plus framing). The binary format should be close to
// header_len + header_json + 4 + data_len.
func TestWriteResponse_NoBase64Overhead(t *testing.T) {
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i)
	}
	resp := &Response{
		RequestID: 1,
		Status:    StatusOK,
		Data:      data,
		Length:    1000,
	}

	var buf bytes.Buffer
	if err := writeResponse(&buf, resp); err != nil {
		t.Fatalf("writeResponse: %v", err)
	}

	// With binary format: 4 (header_len) + header_json + 4 (data_len) + 1000 (data)
	// header_json is small (~60 bytes). Total should be well under 1100.
	// Old JSON+base64 would produce ~1336+ bytes for the data alone.
	totalSize := buf.Len()
	if totalSize >= 1100 {
		t.Errorf("response too large (%d bytes), likely still using base64 encoding", totalSize)
	}

	// Verify the raw data bytes appear verbatim in the output (no base64).
	// The data bytes 0x00..0xFF should appear as-is in the last 1000 bytes.
	tail := buf.Bytes()[buf.Len()-1000:]
	if !bytes.Equal(tail, data) {
		t.Errorf("raw data not found verbatim in wire format (base64 still in use?)")
	}
}

// TestWriteResponse_LargeData verifies that large payloads (1MB+)
// serialize efficiently without base64 expansion.
func TestWriteResponse_LargeData(t *testing.T) {
	dataSize := 1 * 1024 * 1024 // 1MB
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	resp := &Response{
		RequestID: 2,
		Status:    StatusOK,
		Data:      data,
		Length:    int32(dataSize),
	}

	var buf bytes.Buffer
	if err := writeResponse(&buf, resp); err != nil {
		t.Fatalf("writeResponse: %v", err)
	}

	// Binary format overhead: ~4 + ~60 + 4 = ~68 bytes
	// base64 would add ~333KB overhead
	overhead := buf.Len() - dataSize
	if overhead > 200 {
		t.Errorf("overhead too large: %d bytes (expected <200, base64 would add ~333KB)", overhead)
	}

	// Verify round-trip
	got, err := readResponse(&buf)
	if err != nil {
		t.Fatalf("readResponse: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Errorf("large data round-trip mismatch")
	}
}

// TestReadResponse_RejectsOversizedHeader verifies that readResponse
// rejects headers exceeding the sanity limit.
func TestReadResponse_RejectsOversizedHeader(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(100*1024)) // 100KB header, > 64KB limit
	buf.Write(make([]byte, 100*1024))

	_, err := readResponse(&buf)
	if err == nil {
		t.Fatal("expected error for oversized header, got nil")
	}
}

// TestReadResponse_RejectsOversizedData verifies that readResponse
// rejects data payloads exceeding the sanity limit.
func TestReadResponse_RejectsOversizedData(t *testing.T) {
	// Build a valid header
	header := responseHeader{
		RequestID: 1,
		Status:    StatusOK,
		Length:    0,
	}
	headerJSON, _ := json.Marshal(header)

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(len(headerJSON)))
	buf.Write(headerJSON)
	binary.Write(&buf, binary.BigEndian, uint32(200*1024*1024)) // 200MB, > 128MB limit

	_, err := readResponse(&buf)
	if err == nil {
		t.Fatal("expected error for oversized data, got nil")
	}
}
