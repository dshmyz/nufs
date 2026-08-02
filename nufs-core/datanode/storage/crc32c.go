package storage

import (
	"hash"
	"hash/crc32"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// CRC32C returns the Castagnoli CRC used by V3 durable storage formats.
func CRC32C(data []byte) uint32 { return crc32.Checksum(data, crc32cTable) }

// NewCRC32C returns a Castagnoli hash for streaming V3 checksum validation.
func NewCRC32C() hash.Hash32 { return crc32.New(crc32cTable) }
