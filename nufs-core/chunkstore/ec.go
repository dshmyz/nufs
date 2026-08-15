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
// Two paths:
//
//   - V2.1 window read: when the chunk carries an ECStripeID and chunk.Size
//     is a trustworthy literal length (converted chunks — ConvertToEC keeps
//     the literal size), readECWindow serves exactly [offset, offset+length)
//     from the data shard(s) that own its bytes: ~1× transport amplification
//     on the healthy path, and a window-sized reconstruction from peers when
//     an owner is unreachable.  The MaxChunkSize allocation cap is NOT a
//     length, so a cap-sized metadata record falls back to the full read.
//   - Full read (V1 legacy EC, plain chunks, or unknown literal length):
//     every shard is fetched in full and the whole stripe is decoded, then
//     the requested range is trimmed.
func (s *DatanodeChunkStore) readECChunk(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	ec := chunk.ECGroup
	totalShards := ec.DataShards + ec.ParityShards
	encoder := GetECEncoder(ec.DataShards, ec.ParityShards)

	// shardSize below is the maximum possible per-shard size (allocation
	// cap); it only bounds the window guard.  The true per-shard size is
	// derived from the chunk's literal length in readECWindow.
	shardSize := int64(0)
	if ec.DataShards > 0 {
		shardSize = (int64(metadata.MaxChunkSize) + int64(ec.DataShards) - 1) / int64(ec.DataShards)
	}
	dataLen := shardSize * int64(ec.DataShards) // padded original length at the cap

	// Window path: the codec is contiguous (byte p lives in data shard
	// p/shardSize), so a range read only needs the shard(s) that own its
	// bytes.  It requires a trustworthy literal length.  Today only
	// ConvertToEC produces ECStripeID chunks and it keeps the literal size;
	// the direct-EC write path's metadata record is not yet served, so a
	// cap-sized Size with ECStripeID cannot occur in production.  Anything
	// ambiguous falls through to the full read below, which is always
	// correct regardless of Size.
	literalSize := int64(chunk.Size)
	if chunk.ECStripeID != "" && length > 0 && offset >= 0 && offset < dataLen &&
		literalSize > 0 && literalSize <= dataLen {
		return s.readECWindow(ctx, chunk, offset, length, literalSize)
	}

	// ---- Full-read path: fetch all K+M shards in parallel, decode, trim. ----
	type shardData struct {
		index int
		data  []byte
		err   error
	}
	ch := make(chan shardData, len(chunk.Replicas))
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
			var resp *datanode.Response
			if chunk.ECStripeID != "" {
				// V2.1 converted chunks store each EC shard as an independent
				// extent keyed (chunkID, gen=shardIndex+1), readable only via
				// ReadECShard.
				resp, err = client.ReadECShard(chunk.ID, r.ShardIndex, 0, 0)
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

	// Collect shards.
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

	// EC decode.  The padded length implied by the shard sizes is
	// authoritative (chunk.Size may be the MaxChunkSize allocation cap, which
	// exceeds the true padded length); chunk.Size trims it when it is the
	// smaller literal length.
	paddedLen := 0
	for i, sh := range shards {
		if present[i] && len(sh) > paddedLen {
			paddedLen = len(sh)
		}
	}
	paddedLen *= ec.DataShards // K * shardSize = padded original length
	decodeLen := paddedLen
	if chunk.Size > 0 && int64(chunk.Size) < int64(paddedLen) {
		decodeLen = int(chunk.Size) // actual data length
	}
	fullData, err := encoder.Decode(shards, present, decodeLen)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: ec decode chunk %d: %w", chunk.ID, err)
	}

	// Apply range slicing.
	if offset < 0 {
		offset = 0
	}
	if offset >= int64(decodeLen) {
		return []byte{}, nil
	}
	end := int64(decodeLen)
	if length > 0 {
		end = offset + int64(length)
	}
	if end > int64(decodeLen) {
		end = int64(decodeLen)
	}
	return fullData[offset:end], nil
}

// readECWindow serves the sub-range [offset, offset+length) of a V2.1 EC
// chunk directly from the data shard(s) that own its bytes.
//
// The codec is systematic and contiguous (ec_encoder.Encode places original
// byte p in data shard p/shardSize at in-shard offset p%shardSize, padded so
// every shard is exactly shardSize bytes).  A window therefore overlaps only
// the shards whose byte ranges intersect it — one shard for any window up to
// shardSize.  When those shards are reachable, their window bytes ARE the
// requested data: no decode, no parity, ~1× transport amplification.
//
// When an owning shard is unreachable, its window is reconstructed from
// peers: Reed-Solomon is linear per in-shard byte position, so the missing
// shard's window can be rebuilt from the same in-shard range fetched from any
// K other shards (data or parity, all sliced to the same window length).  The
// reconstruction is window-sized (K × window bytes), never shard-sized.
func (s *DatanodeChunkStore) readECWindow(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32, literalSize int64) ([]byte, error) {
	ec := chunk.ECGroup
	k := ec.DataShards
	totalShards := k + ec.ParityShards
	encoder := GetECEncoder(k, ec.ParityShards)

	// True per-shard size implied by the literal data length (the encoder
	// padded to ceil(literal/K) per shard).
	shardSize := (literalSize + int64(k) - 1) / int64(k)

	end := offset + int64(length)
	if end > literalSize {
		end = literalSize
	}
	if end <= offset {
		return []byte{}, nil
	}

	// Overlapping data shards and the local [start, end) window in each.
	type window struct {
		shard      int
		start, end int32
	}
	var wins []window
	for i := 0; i < k; i++ {
		shardStart := shardSize * int64(i)
		shardEnd := shardStart + shardSize
		if shardEnd <= offset || shardStart >= end {
			continue
		}
		ws := offset - shardStart
		if ws < 0 {
			ws = 0
		}
		we := end - shardStart
		if we > shardSize {
			we = shardSize
		}
		wins = append(wins, window{i, int32(ws), int32(we)})
	}

	byIndex := make(map[int]metadata.ReplicaInfo, len(chunk.Replicas))
	for _, rep := range chunk.Replicas {
		byIndex[rep.ShardIndex] = rep
	}

	// Phase 1: fetch each owning shard's window directly, in parallel.
	got := make([][]byte, len(wins))
	type windowResult struct {
		idx  int
		data []byte
		err  error
	}
	resCh := make(chan windowResult, len(wins))
	launched := 0
	for wi, w := range wins {
		rep, ok := byIndex[w.shard]
		if !ok {
			continue // no replica recorded — reconstructed in phase 2
		}
		launched++
		go func(wi int, w window, r metadata.ReplicaInfo) {
			data, err := s.readECShardRange(ctx, chunk, r, w.shard, w.start, w.end)
			resCh <- windowResult{wi, data, err}
		}(wi, w, rep)
	}
	missing := false
	for i := 0; i < launched; i++ {
		select {
		case res := <-resCh:
			if res.err == nil {
				got[res.idx] = res.data
			} else {
				slog.Warn("chunkstore: ec window read failed", "chunkID", chunk.ID,
					"shard", wins[res.idx].shard, "error", res.err)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("chunkstore: ec window read context cancelled: %w", ctx.Err())
		}
	}
	for _, d := range got {
		if d == nil {
			missing = true
			break
		}
	}
	if !missing {
		var out []byte
		for _, d := range got {
			out = append(out, d...)
		}
		return out, nil
	}

	// Phase 2: reconstruct each missing owning shard's window from peers.
	for wi, w := range wins {
		if got[wi] != nil {
			continue
		}
		winLen := w.end - w.start
		if winLen <= 0 {
			continue
		}
		var others []int
		for i := 0; i < totalShards; i++ {
			if i != w.shard {
				others = append(others, i)
			}
		}
		// Fetch the same local window from every other shard (data and
		// parity) in parallel; the collector takes what arrives.
		slices := make([][]byte, totalShards)
		valid := 0
		type peerResult struct {
			idx  int
			data []byte
			err  error
		}
		peerCh := make(chan peerResult, len(others))
		pl := 0
		for _, oi := range others {
			rep, ok := byIndex[oi]
			if !ok {
				continue
			}
			pl++
			go func(oi int, r metadata.ReplicaInfo) {
				data, err := s.readECShardRange(ctx, chunk, r, oi, w.start, w.end)
				peerCh <- peerResult{oi, data, err}
			}(oi, rep)
		}
		for i := 0; i < pl; i++ {
			select {
			case res := <-peerCh:
				if res.err == nil && len(res.data) == int(winLen) {
					slices[res.idx] = res.data
					valid++
				} else if res.err != nil {
					slog.Warn("chunkstore: ec window peer read failed", "chunkID", chunk.ID,
						"shard", res.idx, "error", res.err)
				}
			case <-ctx.Done():
				return nil, fmt.Errorf("chunkstore: ec window reconstruct context cancelled: %w", ctx.Err())
			}
		}
		if valid < k {
			return nil, fmt.Errorf("chunkstore: ec window read: shard %d unavailable, only %d/%d peer windows available",
				w.shard, valid, k)
		}
		// Reconstruct just the window: equal-length slices decode exactly
		// like a full stripe of that size (no padding is fabricated — every
		// slice is the shard's own bytes at that in-shard range).
		if err := encoder.enc.Reconstruct(slices); err != nil {
			return nil, fmt.Errorf("chunkstore: ec window reconstruct shard %d: %w", w.shard, err)
		}
		if len(slices[w.shard]) != int(winLen) {
			return nil, fmt.Errorf("chunkstore: ec window reconstruct shard %d: bad length %d", w.shard, len(slices[w.shard]))
		}
		got[wi] = slices[w.shard]
	}

	var out []byte
	for _, d := range got {
		out = append(out, d...)
	}
	return out, nil
}

// readECShardRange fetches [start, end) of one EC shard extent from its owner.
func (s *DatanodeChunkStore) readECShardRange(ctx context.Context, chunk *metadata.ChunkMeta, rep metadata.ReplicaInfo, shardIndex int, start, end int32) ([]byte, error) {
	if rep.Addr == "" {
		return nil, fmt.Errorf("empty addr for node %d", rep.NodeID)
	}
	client, err := s.pool.Get(rep.Addr)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", rep.Addr, err)
	}
	resp, err := client.ReadECShard(chunk.ID, shardIndex, int64(start), end-start)
	s.pool.Put(rep.Addr, client)
	if err != nil {
		return nil, fmt.Errorf("read shard %d from %s: %w", shardIndex, rep.Addr, err)
	}
	if resp.Status != datanode.StatusOK {
		return nil, fmt.Errorf("node %d status=%d: %s", rep.NodeID, resp.Status, resp.Error)
	}
	return resp.Data, nil
}
