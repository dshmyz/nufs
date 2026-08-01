package segment

import (
	"hash/crc32"
	"time"
)

func nowUnixNano() int64 { return time.Now().UnixNano() }

func crc32ChecksumIEEE(data []byte) uint32 { return crc32.ChecksumIEEE(data) }
