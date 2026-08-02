# Task 3 Fix Round 2 Report

## Scope

- Kept the pre-existing Task 3 report change intact and appended this round.
- Preserved the V3 format layout: `SegmentCRC` is the final four-byte footer
  field at byte range `[61,65)`, excluded from its own checksum.

## Changes

- `RecoverFromSegmentLog` propagates callback/application errors rather than
  treating them as recoverable tails. The result is not DataReady and no file
  truncation occurs for the valid committed batch.
- Store startup therefore fails closed when recovery encounters committed
  relocation records, which are recognized on disk but unsupported by the
  current storage application path.
- `Writer.WriteFooter` computes the V3 SegmentCRC by streaming the file with a
  bounded 64 KiB buffer, adding the footer prefix `[0,61)`, then writing and
  syncing the populated footer.
- `ValidateSealedSegment` validates the parsed header/footer and the full
  file-context CRC. Readers invoke it for any encoded footer at EOF, and for
  every path in a `sealed` directory so corrupted footer prefixes also fail.

## Tests

Focused regression tests cover application failure preservation, fail-closed
relocation startup, correct sealing/validation, covered-byte corruption,
excluded-field checksum mismatch, zero/unset CRC rejection, and literal
operation wire values.

## Verification

Run from `nufs-core`:

```text
go test -race ./datanode/storage/segment -run 'Relocate|Recover|Footer|Seal|CRC|Delete|Crash' -count=20 -timeout 240s
# PASS (exit 0)

go test -race ./datanode/storage/segment ./datanode/storage/journal ./datanode/storage/index -count=1 -timeout 300s
# PASS (exit 0)

git diff --check
# PASS (exit 0; no output)
```
