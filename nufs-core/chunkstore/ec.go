package chunkstore

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ECWriteAuthority is the write-path direct-EC authority seam (Program 10,
// §14): it answers "where does each shard of a direct EC write land" and later
// records the landed write as a durable EC stripe. On the production path it is
// satisfied structurally by *metadata.HTTPClient (PlanECWrite / RecordDirectEC
// RPCs to the metadata service); a test stub satisfies it in unit tests. It is
// the mirror of the ECAuthority (Program 3) and ECLandingResolver /
// ECOrphanResolver (Program 7) structural-interface seams. When it is nil the
// write path falls back to V1 semantics (writeECChunk), so unwired stores keep
// today's behavior.
type ECWriteAuthority interface {
	PlanECWrite(ctx context.Context, chunkID metadata.ChunkID, dataShards, parityShards int) ([]metadata.ECShard, error)
	RecordDirectEC(ctx context.Context, chunkID metadata.ChunkID, dataShards, parityShards int, shards []metadata.ECShard, checksum uint32) error
}

// writeECShardDirect is the V2.1 write-path direct-EC write (Program 10, §14,
// aligning with V1 writeECChunk): it encodes data into K+M shards and pushes
// each shard *directly* to its owning node's shard store via ReplicateECShard —
// no intermediate replica, exactly like V1 but landing in the shard store. The
// §14 per-shard (NodeID, DiskID) placement is decided by the metadata authority
// (PlanECWrite), not the gateway. Once ≥K shards (the write quorum) land, the
// landed write is recorded as a durable Complete stripe + ChunkMeta.ECStripeID
// (RecordDirectEC) so the serving read (ReadECShard), self-heal (landing
// resolve) and orphan-GC paths recognize it exactly as they do a converted
// chunk.
//
// If the authority is unavailable or the plan cannot meet §14 (e.g. a V1-only
// cluster with no V2.1 shard disks), it falls back to V1 writeECChunk *before
// writing anything* so the write still lands via legacy whole-shard replication.
func (s *DatanodeChunkStore) writeECShardDirect(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	ec := chunk.ECGroup
	encoder := GetECEncoder(ec.DataShards, ec.ParityShards)

	result, err := encoder.Encode(data)
	if err != nil {
		return fmt.Errorf("chunkstore: ec encode chunk %d: %w", chunk.ID, err)
	}
	totalShards := ec.DataShards + ec.ParityShards
	allShards := make([][]byte, totalShards)
	copy(allShards[:ec.DataShards], result.DataShards)
	copy(allShards[ec.DataShards:], result.ParityShards)

	// The metadata authority (not the gateway) decides the §14 placement. If it
	// is unreachable or cannot place all shards across a V2.1 topology, fall
	// back to V1 whole-shard replication before touching any shard store.
	plan, err := s.ecWrite.PlanECWrite(ctx, chunk.ID, ec.DataShards, ec.ParityShards)
	if err != nil || len(plan) != totalShards {
		slog.Warn("chunkstore: ec direct plan unavailable, falling back to V1 writeECChunk",
			"chunkID", chunk.ID, "planned", len(plan), "want", totalShards, "error", err)
		return s.writeECChunk(ctx, chunk, data)
	}

	type shardResult struct {
		nodeID metadata.NodeID
		err    error
	}
	ch := make(chan shardResult, len(chunk.Replicas))
	for i, rep := range chunk.Replicas {
		// plan[i].NodeID is aligned with chunk.Replicas[i].NodeID (the authority
		// resolved it from the chunk), so we push shard i to the owning node's
		// address — replicas[i].Addr — with the planned disk on that node
		// (DiskID%1000 = node-local shard-store index, §14).
		disk := int(plan[i].DiskID % 1000)
		go func(r metadata.ReplicaInfo, idx, disk int) {
			if r.Addr == "" {
				ch <- shardResult{r.NodeID, fmt.Errorf("empty addr for node %d", r.NodeID)}
				return
			}
			client, err := s.pool.Get(r.Addr)
			if err != nil {
				ch <- shardResult{r.NodeID, fmt.Errorf("connect %s: %w", r.Addr, err)}
				return
			}
			resp, err := client.ReplicateECShard(chunk.ID, idx, disk, allShards[idx])
			s.pool.Put(r.Addr, client)
			if err != nil {
				ch <- shardResult{r.NodeID, fmt.Errorf("write shard %d to %s: %w", idx, r.Addr, err)}
				return
			}
			if resp.Status != datanode.StatusOK {
				ch <- shardResult{r.NodeID, fmt.Errorf("node %d status=%d: %s", r.NodeID, resp.Status, resp.Error)}
				return
			}
			ch <- shardResult{r.NodeID, nil}
		}(rep, i, disk)
	}

	quorum := ec.DataShards // K shards must land
	successes := 0
	var lastErr error
	for i := 0; i < len(chunk.Replicas); i++ {
		select {
		case r := <-ch:
			if r.err != nil {
				lastErr = r.err
				slog.Warn("chunkstore: ec direct shard write failed",
					"chunkID", chunk.ID, "nodeID", r.nodeID, "error", r.err)
			} else {
				successes++
			}
		case <-ctx.Done():
			return fmt.Errorf("chunkstore: ec write context cancelled: %w", ctx.Err())
		}
	}
	if successes < quorum {
		if lastErr != nil {
			return fmt.Errorf("chunkstore: ec direct write only %d/%d shards for chunk %d: %w",
				successes, quorum, chunk.ID, lastErr)
		}
		return fmt.Errorf("chunkstore: ec direct write only %d/%d shards for chunk %d",
			successes, quorum, chunk.ID)
	}

	// Quorum landed. Lift the chunk into durable EC state (Complete stripe +
	// ECStripeID) so serving read / self-heal / orphan-GC recognize it.
	checksum := crc32.ChecksumIEEE(data)
	if err := s.ecWrite.RecordDirectEC(ctx, chunk.ID, ec.DataShards, ec.ParityShards, plan, checksum); err != nil {
		return fmt.Errorf("chunkstore: ec record-direct chunk %d: %w", chunk.ID, err)
	}
	return nil
}

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
//
// When offset and length are provided (V2.1 ECStripeID path), only the data
// shards whose byte ranges overlap [offset, offset+length) are read; the
// remaining slots are marked absent and filled by parity during decode.  This
// reduces both network I/O (fewer shards fetched) and disk I/O (the server
// applies the range to each fetched shard via ReadRangeFrames).  The EC
// decoder still reconstructs the full original payload, so memory
// amplification remains; the win is on the transport layer.
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

	// Determine which data shards overlap the requested window so we can
	// skip fetching shards that carry no bytes in the window.  This is
	// only meaningful for the V2.1 ECStripeID path; V1 legacy still reads
	// full chunks (ReadChunk(id,0,0)) regardless.
	shardSize := int64(0) // bytes per data shard (including padding)
	if ec.DataShards > 0 {
		shardSize = (int64(metadata.MaxChunkSize) + int64(ec.DataShards) - 1) / int64(ec.DataShards)
	}
	dataLen := shardSize * int64(ec.DataShards) // padded original length

	wantWindow := chunk.ECStripeID != "" && length > 0 && offset >= 0 && offset < dataLen
	needDataShards := make(map[int]struct{})   // data shard indices we need
	needParityShards := make(map[int]struct{}) // parity shard indices we need

	if wantWindow {
		windowEnd := offset + int64(length)
		if windowEnd > dataLen {
			windowEnd = dataLen
		}
		for i := 0; i < ec.DataShards; i++ {
			shardStart := shardSize * int64(i)
			shardEnd := shardStart + shardSize
			if shardEnd <= offset || shardStart >= windowEnd {
				continue // no overlap — skip
			}
			needDataShards[i] = struct{}{}
		}
		// Decoder needs K = DataShards shards total.  We have len(needDataShards)
		// data shards.  Read parity shards to fill the gap, prioritising the
		// first parity shards for consistency.
		parityNeed := ec.DataShards - len(needDataShards)
		if parityNeed > ec.ParityShards {
			parityNeed = ec.ParityShards
		}
		for p := 0; p < parityNeed; p++ {
			needParityShards[ec.DataShards+p] = struct{}{}
		}
		slog.Debug("chunkstore: ec range read", "chunkID", chunk.ID, "window",
			fmt.Sprintf("[%d,%d)", offset, offset+int64(length)),
			"dataShards", len(needDataShards), "parityShards", len(needParityShards))
	}

	// Read shards in parallel.  Non-overlapping shards are skipped (not
	// sent to the channel); the collector marks them absent.
	launched := 0
	for _, rep := range chunk.Replicas {
		idx := rep.ShardIndex

		// Decide whether to fetch this shard.
		fetch := true
		if wantWindow {
			fetch = false
			if _, ok := needDataShards[idx]; ok {
				fetch = true
			} else if _, ok := needParityShards[idx]; ok {
				fetch = true
			}
		}
		if !fetch {
			continue // skip — not in window
		}
		launched++

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
			// V2.1 converted chunks store each EC shard as an independent
			// extent.  When wantWindow, pass the shard-relative range so the
			// server only reads intersecting frames via ReadRangeFrames.
			var resp *datanode.Response
			if chunk.ECStripeID != "" {
				var shOffset, shLen int32
				if wantWindow && r.ShardIndex < ec.DataShards {
					shardStart := shardSize * int64(r.ShardIndex)
					overlapStart := offset - shardStart
					if overlapStart < 0 {
						overlapStart = 0
					}
					overlapEnd := offset + int64(length) - shardStart
					if overlapEnd > shardSize {
						overlapEnd = shardSize
					}
					shOffset = int32(overlapStart)
					shLen = int32(overlapEnd - overlapStart)
				}
				resp, err = client.ReadECShard(chunk.ID, r.ShardIndex, int64(shOffset), shLen)
			} else {
				// Legacy V1 EC: whole-shard files, no range support.
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

	// Collect shards — exactly as many as goroutines were launched.
	shards := make([][]byte, totalShards)
	present := make([]bool, totalShards)
	for i := 0; i < launched; i++ {
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

	// Pad partial data shards to shardSize so all present shards have the
	// same length — reedsolomon.Reconstruct requires uniform shard sizes.
	if wantWindow && shardSize > 0 {
		for i := 0; i < ec.DataShards; i++ {
			if present[i] && int64(len(shards[i])) < shardSize {
				padded := make([]byte, shardSize)
				copy(padded, shards[i])
				shards[i] = padded
			}
		}
	}
	fullData, err := encoder.Decode(shards, present, decodeLen)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: ec decode chunk %d: %w", chunk.ID, err)
	}

	// Apply range slicing
	dataLen2 := int64(len(fullData))
	if offset < 0 {
		offset = 0
	}
	if offset >= dataLen2 {
		return []byte{}, nil
	}
	end := dataLen2
	if length > 0 {
		end = offset + int64(length)
	}
	if end > dataLen2 {
		end = dataLen2
	}
	return fullData[offset:end], nil
}
