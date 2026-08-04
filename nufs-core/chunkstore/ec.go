package chunkstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/metadata"
)

// writeECChunk encodes data into K+M shards and writes each shard
// to its assigned datanode concurrently. Returns success once K
// shards (the write quorum) are acknowledged.
func (s *DatanodeChunkStore) writeECChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	ec := chunk.ECGroup
	encoder := GetECEncoder(ec.DataShards, ec.ParityShards)

	result, err := encoder.Encode(data)
	if err != nil {
		return fmt.Errorf("chunkstore: ec encode chunk %d: %w", chunk.ID, err)
	}

	// Collect all shards (data + parity) indexed by ShardIndex
	allShards := make([][]byte, ec.DataShards+ec.ParityShards)
	copy(allShards[:ec.DataShards], result.DataShards)
	copy(allShards[ec.DataShards:], result.ParityShards)

	type shardResult struct {
		nodeID metadata.NodeID
		err    error
	}
	ch := make(chan shardResult, len(chunk.Replicas))

	for _, rep := range chunk.Replicas {
		go func(r metadata.ReplicaInfo) {
			if r.ShardIndex < 0 || r.ShardIndex >= len(allShards) {
				ch <- shardResult{r.NodeID, fmt.Errorf("invalid shard index %d", r.ShardIndex)}
				return
			}
			if r.Addr == "" {
				ch <- shardResult{r.NodeID, fmt.Errorf("empty addr for node %d", r.NodeID)}
				return
			}

			client, err := s.pool.Get(r.Addr)
			if err != nil {
				ch <- shardResult{r.NodeID, fmt.Errorf("connect %s: %w", r.Addr, err)}
				return
			}
			resp, err := client.WriteChunk(chunk.ID, allShards[r.ShardIndex])
			s.pool.Put(r.Addr, client)

			if err != nil {
				ch <- shardResult{r.NodeID, fmt.Errorf("write shard %d to %s: %w", r.ShardIndex, r.Addr, err)}
				return
			}
			if resp.Status != datanode.StatusOK {
				ch <- shardResult{r.NodeID, fmt.Errorf("node %d status=%d: %s", r.NodeID, resp.Status, resp.Error)}
				return
			}
			ch <- shardResult{r.NodeID, nil}
		}(rep)
	}

	quorum := ec.DataShards // K shards must succeed
	successes := 0
	var lastErr error
	for i := 0; i < len(chunk.Replicas); i++ {
		select {
		case r := <-ch:
			if r.err != nil {
				lastErr = r.err
				slog.Warn("chunkstore: ec shard write failed", "chunkID", chunk.ID, "nodeID", r.nodeID, "error", r.err)
			} else {
				successes++
			}
		case <-ctx.Done():
			return fmt.Errorf("chunkstore: ec write context cancelled: %w", ctx.Err())
		}
	}

	if successes < quorum {
		if lastErr != nil {
			return fmt.Errorf("chunkstore: ec write only %d/%d shards succeeded for chunk %d: %w",
				successes, quorum, chunk.ID, lastErr)
		}
		return fmt.Errorf("chunkstore: ec write only %d/%d shards succeeded for chunk %d",
			successes, quorum, chunk.ID)
	}

	return nil
}

// readECChunk reads shards from K+M datanodes in parallel, then
// decodes the original data using erasure coding.
func (s *DatanodeChunkStore) readECChunk(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	ec := chunk.ECGroup
	totalShards := ec.DataShards + ec.ParityShards
	encoder := GetECEncoder(ec.DataShards, ec.ParityShards)

	type shardData struct {
		index int
		data  []byte
		err   error
	}
	ch := make(chan shardData, len(chunk.Replicas))

	// Read all shards in parallel
	for _, rep := range chunk.Replicas {
		go func(r metadata.ReplicaInfo) {
			if r.Addr == "" {
				ch <- shardData{r.ShardIndex, nil, fmt.Errorf("empty addr for node %d", r.NodeID)}
				return
			}

			client, err := s.pool.Get(r.Addr)
			if err != nil {
				ch <- shardData{r.ShardIndex, nil, fmt.Errorf("connect %s: %w", r.Addr, err)}
				return
			}
			// V2.1 converted chunks store each EC shard as an independent extent in a
			// dedicated shard store keyed (chunkID, gen=shardIndex+1), readable only via
			// ReadECShard — generic ReadChunk(chunk.ID) looks in the data store and finds
			// nothing. The ECStripeID discriminator (Program 5) marks a V2.1-converted
			// 6+3 chunk. Legacy V1 gateway EC (ECStripeID == "") keeps the original
			// ReadChunk path: there each Replica is a whole shard file keyed by chunk.ID.
			var resp *datanode.Response
			if chunk.ECStripeID != "" {
				resp, err = client.ReadECShard(chunk.ID, r.ShardIndex)
			} else {
				resp, err = client.ReadChunk(chunk.ID, 0, 0)
			}
			s.pool.Put(r.Addr, client)

			if err != nil {
				ch <- shardData{r.ShardIndex, nil, fmt.Errorf("read shard from %s: %w", r.Addr, err)}
				return
			}
			if resp.Status != datanode.StatusOK {
				ch <- shardData{r.ShardIndex, nil, fmt.Errorf("node %d status=%d: %s", r.NodeID, resp.Status, resp.Error)}
				return
			}
			ch <- shardData{r.ShardIndex, resp.Data, nil}
		}(rep)
	}

	// Collect shards
	shards := make([][]byte, totalShards)
	present := make([]bool, totalShards)
	for i := 0; i < len(chunk.Replicas); i++ {
		select {
		case sd := <-ch:
			if sd.err == nil && sd.index >= 0 && sd.index < totalShards {
				shards[sd.index] = sd.data
				present[sd.index] = true
			} else if sd.err != nil {
				slog.Warn("chunkstore: ec shard read failed", "chunkID", chunk.ID, "shard", sd.index, "error", sd.err)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("chunkstore: ec read context cancelled: %w", ctx.Err())
		}
	}

	// EC decode
	// chunk.Size is set to MaxChunkSize at allocation time and never updated
	// by the committer. For chunks allocated via S3 gateway, Size is wrong.
	// For chunks created directly (tests), Size is the actual data length.
	// Use shard sizes to compute the padded length, then trim to chunk.Size
	// if it's smaller (actual data) or use padded length if chunk.Size is
	// MaxChunkSize (allocation placeholder).
	paddedLen := 0
	for i, s := range shards {
		if present[i] && len(s) > paddedLen {
			paddedLen = len(s)
		}
	}
	paddedLen *= ec.DataShards // K * shardSize = padded original length
	decodeLen := paddedLen
	if chunk.Size > 0 && int64(chunk.Size) < int64(paddedLen) {
		decodeLen = int(chunk.Size) // actual data length (tests set this correctly)
	}
	fullData, err := encoder.Decode(shards, present, decodeLen)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: ec decode chunk %d: %w", chunk.ID, err)
	}

	// Apply range slicing
	dataLen := int64(len(fullData))
	if offset < 0 {
		offset = 0
	}
	if offset >= dataLen {
		return []byte{}, nil
	}
	end := dataLen
	if length > 0 {
		end = offset + int64(length)
	}
	if end > dataLen {
		end = dataLen
	}
	return fullData[offset:end], nil
}
