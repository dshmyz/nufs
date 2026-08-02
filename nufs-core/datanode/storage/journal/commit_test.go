package journal

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/example/dfs/datanode/storage"
)

func TestV3BatchCommitChecksumRejectsIEEE(t *testing.T) {
	descs := []BatchDescriptor{{ExtentID: 1, Generation: 2, SegmentID: 3, Offset: 4, StoredLen: 5, LogicalLen: 6, Checksum: 7, Op: 3}}
	descriptorBuf := make([]byte, BatchDescriptorSize)
	descriptorsCRC, err := EncodeDescriptors(descriptorBuf, descs)
	if err != nil {
		t.Fatal(err)
	}
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	if want := crc32.Checksum(descriptorBuf, castagnoli); descriptorsCRC != want {
		t.Fatalf("descriptor CRC = %08x, want Castagnoli %08x", descriptorsCRC, want)
	}

	commit := BatchCommit{Magic: BatchCommitMagic, Version: storage.FormatVersion, Seq: 1, RecordCount: 1, FirstOffset: 4, LastOffset: 59, DescriptorsCRC: descriptorsCRC}
	buf := make([]byte, BatchCommitSize)
	if err := commit.Encode(buf); err != nil {
		t.Fatal(err)
	}
	if got, want := binary.BigEndian.Uint32(buf[38:42]), crc32.Checksum(buf[0:38], castagnoli); got != want {
		t.Fatalf("BatchCommit CRC = %08x, want Castagnoli %08x", got, want)
	}
	var decoded BatchCommit
	if err := decoded.Decode(buf); err != nil {
		t.Fatalf("Castagnoli-encoded BatchCommit rejected: %v", err)
	}

	binary.BigEndian.PutUint32(buf[38:42], crc32.ChecksumIEEE(buf[0:38]))
	if err := decoded.Decode(buf); err == nil {
		t.Fatal("IEEE-encoded BatchCommit accepted")
	}
}
