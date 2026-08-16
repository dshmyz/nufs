package metadata

import (
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Codec encoding format for PebbleStore values.
// New writes use msgpack for performance; reads auto-detect format
// for backward compatibility with existing JSON-encoded data.
//
// Format detection:
//   - If the first byte is '{' (0x7B) or '[' (0x5B), treat as JSON
//   - Otherwise, treat as msgpack

// codecFormat indicates the serialization format.
type codecFormat uint8

const (
	codecAuto    codecFormat = iota // Auto-detect on read
	codecMsgpack                    // msgpack (hot path)
)

// marshalValue serializes v using the specified codec format.
func marshalValue(v interface{}, format codecFormat) ([]byte, error) {
	switch format {
	case codecMsgpack:
		data, err := msgpack.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("msgpack marshal: %w", err)
		}
		return data, nil
	default:
		// Default to msgpack for hot path
		data, err := msgpack.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("msgpack marshal: %w", err)
		}
		return data, nil
	}
}

// unmarshalValue deserializes data into v, auto-detecting the format.
// JSON data (starting with '{' or '[') is decoded with json.Unmarshal;
// all other data is decoded with msgpack.Unmarshal.
func unmarshalValue(data []byte, v interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("pebble store: unmarshal: empty data")
	}

	// Auto-detect: JSON objects start with '{', arrays with '['
	if data[0] == '{' || data[0] == '[' {
		if err := json.Unmarshal(data, v); err != nil {
			return fmt.Errorf("json unmarshal: %w", err)
		}
		return nil
	}

	// Try msgpack first (hot path)
	if err := msgpack.Unmarshal(data, v); err != nil {
		// Fallback to JSON if msgpack fails (might be legacy data)
		if jsonErr := json.Unmarshal(data, v); jsonErr != nil {
			return fmt.Errorf("msgpack unmarshal: %w (json fallback also failed: %v)", err, jsonErr)
		}
	}
	return nil
}
