# NUFS V2.1 P0 Task 2B Fix Round 4 Report

## Scope

Focused recovery-byte-limit hardening from clean HEAD `366cc30` on
`codex/v21-p0-hardening`. No architectural or exported-bypass changes.

## RED evidence

The focused regression tests were added before the production edit and failed
against the prior zero-only normalization:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run 'ByteLimitsClamp|NegativeTrailingLimitCannotDisable|SparseTailUsesBoundedMemoryAndCanonicalBudget' -count=1 -timeout 60s
# FAIL: negative limits remained -1, over-cap limits remained over cap, and a negative trailing limit returned nil instead of ErrRecoveryBudgetExceeded
```

## Implementation

- `recoveryByteLimits` now maps `<= 0` and over-cap replay values to
  `storage.MaxRecoveryReplayBytes` (256 MiB), and applies the corresponding
  128 MiB trailing cap.
- In-range positive values are unchanged, preserving tighter test and policy
  limits.
- Table-driven tests cover zero, negative, exact-cap, below-cap, and
  above-cap inputs for both options.
- A sparse tail larger than the canonical trailing cap proves a negative
  exported trailing value cannot disable the budget.
- The sparse 4 GiB test no longer supplies an over-cap allowance; it verifies
  bounded allocation, canonical budget failure, and no truncation on that
  failure.

## Final verification

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run 'ByteLimitsClamp|NegativeTrailingLimitCannotDisable|SparseTailUsesBoundedMemoryAndCanonicalBudget' -count=1 -timeout 60s
# PASS (exit 0)

go test -race ./datanode/storage/segment ./datanode/storage/recovery -run 'Recover|Budget|DataReady|Checkpoint' -count=10 -timeout 180s
# PASS (exit 0)

go test -race ./datanode/storage/segment ./datanode/storage/recovery ./datanode/storage/index -count=1 -timeout 240s
# PASS (exit 0)

cd /Users/gracegaoya/work/project/nufs
git diff --check
# PASS
```

## Files

- `nufs-core/datanode/storage/segment/recover.go`
- `nufs-core/datanode/storage/segment/recovery_budget_test.go`
- `nufs-core/datanode/storage/segment/recover_stream_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-2b-report.md`
- `.superpowers/sdd/nufs-v21-p0-task-2b-fix4-report.md`
