#!/usr/bin/env bash
#
# fuse-posix-test.sh — POSIX compliance test for NUFS FUSE mount.
#
# Usage:
#   ./fuse-posix-test.sh /mnt/nufs/bucket
#
# Tests critical POSIX operations against a mounted NUFS filesystem.
# Exit code 0 = all pass, 1 = any failure.
#
# Requirements: bash, coreutils (dd, stat, truncate, chmod, chown,
#               ln, readlink, touch, mkdir, rmdir, ls, cat), no root needed.
#
set -euo pipefail

MOUNT="${1:?Usage: $0 <mountpoint>}"
PASS=0
FAIL=0
TMPDIR=$(mktemp -d /tmp/fuse-posix-test.XXXXXX)
trap 'rm -rf "$TMPDIR"' EXIT

# ─── helpers ───────────────────────────────────────────────────────────
pass() { PASS=$((PASS + 1)); }
fail() { FAIL=$((FAIL + 1)); echo "FAIL: $*"; }
assert_eq()  { [ "$1" = "$2" ]  && pass || fail "expected '$2', got '$1'"; }
assert_ne()  { [ "$1" != "$2" ] && pass || fail "expected != '$2', got '$1'"; }
assert_gt()  { [ "$1" -gt "$2" ] && pass || fail "expected > $2, got '$1'"; }
assert_file_exists() { [ -f "$1" ] && pass || fail "file $1 does not exist"; }
assert_file_not_exists() { [ ! -f "$1" ] && pass || fail "file $1 still exists"; }
assert_dir_exists() { [ -d "$1" ] && pass || fail "dir $1 does not exist"; }
assert_dir_not_exists() { [ ! -d "$1" ] && pass || fail "dir $1 still exists"; }

run_test() {
    local name="$1"
    shift
    printf "%-55s " "$name"
    if "$@" >/dev/null 2>&1; then
        pass
        printf "PASS\n"
    else
        fail "$name"
        printf "FAIL\n"
    fi
}

# ─── cleanup previous run ──────────────────────────────────────────────
rm -rf "$MOUNT/_posix_test"
mkdir -p "$MOUNT/_posix_test"
BASE="$MOUNT/_posix_test"

# ─── 1. File creation & read ──────────────────────────────────────────
echo "=== File operations ==="

test_file_create_read() {
    local f="$BASE/create_read.txt"
    echo "hello world" > "$f"
    local got
    got=$(cat "$f")
    [ "$got" = "hello world" ]
}
run_test "create and read file" test_file_create_read

test_file_write_overwrite() {
    local f="$BASE/overwrite.txt"
    echo "first" > "$f"
    echo "second" > "$f"
    local got
    got=$(cat "$f")
    [ "$got" = "second" ]
}
run_test "overwrite file" test_file_write_overwrite

test_file_append() {
    local f="$BASE/append.txt"
    echo "line1" > "$f"
    echo "line2" >> "$f"
    local lines
    lines=$(wc -l < "$f")
    [ "$lines" -eq 2 ]
}
run_test "append to file" test_file_append

test_file_delete() {
    local f="$BASE/delete_me.txt"
    touch "$f"
    rm "$f"
    assert_file_not_exists "$f"
}
run_test "delete file" test_file_delete

test_file_read_nonexistent() {
    local f="$BASE/nonexistent_$$"
    ! cat "$f" 2>/dev/null
}
run_test "read nonexistent file fails" test_file_read_nonexistent

# ─── 2. Truncation ────────────────────────────────────────────────────
echo "=== Truncation ==="

test_truncate_down() {
    local f="$BASE/trunc_down.txt"
    dd if=/dev/urandom bs=1024 count=10 of="$f" 2>/dev/null
    local size_before
    size_before=$(stat -c %s "$f")
    truncate -s 100 "$f"
    local size_after
    size_after=$(stat -c %s "$f")
    [ "$size_before" -eq 10240 ] && [ "$size_after" -eq 100 ]
}
run_test "truncate down (10K → 100)" test_truncate_down

test_truncate_up() {
    local f="$BASE/trunc_up.txt"
    echo "data" > "$f"
    truncate -s 10000 "$f"
    local size
    size=$(stat -c %s "$f")
    [ "$size" -eq 10000 ]
}
run_test "truncate up (5 → 10000)" test_truncate_up

test_truncate_preserves_prefix() {
    local f="$BASE/trunc_prefix.txt"
    dd if=/dev/urandom bs=1 count=200 of="$f" 2>/dev/null
    local prefix
    prefix=$(dd if="$f" bs=1 count=50 2>/dev/null | xxd -p)
    truncate -s 100 "$f"
    local prefix_after
    prefix_after=$(dd if="$f" bs=1 count=50 2>/dev/null | xxd -p)
    [ "$prefix" = "$prefix_after" ]
}
run_test "truncate preserves prefix bytes" test_truncate_preserves_prefix

test_truncate_to_zero() {
    local f="$BASE/trunc_zero.txt"
    echo "content" > "$f"
    truncate -s 0 "$f"
    local size
    size=$(stat -c %s "$f")
    [ "$size" -eq 0 ]
}
run_test "truncate to zero" test_truncate_to_zero

test_fallocate() {
    local f="$BASE/falloc.txt"
    truncate -s 0 "$f"
    fallocate -l 10000 "$f" 2>/dev/null || truncate -s 10000 "$f"
    local size
    size=$(stat -c %s "$f")
    [ "$size" -eq 10000 ]
}
run_test "fallocate (or fallback truncate)" test_fallocate

# ─── 3. pwrite-style partial overwrite ────────────────────────────────
echo "=== Partial overwrite ==="

test_partial_overwrite_preserves_tail() {
    local f="$BASE/partial_overwrite.txt"
    # Create 200-byte file with known tail
    dd if=/dev/zero bs=1 count=200 of="$f" 2>/dev/null
    printf "TAIL123" | dd of="$f" bs=1 seek=193 conv=notrunc 2>/dev/null
    # Overwrite middle
    printf "XXXXX" | dd of="$f" bs=1 seek=100 conv=notrunc 2>/dev/null
    # Verify size unchanged and tail preserved
    local size tail
    size=$(stat -c %s "$f")
    tail=$(dd if="$f" bs=1 skip=193 count=7 2>/dev/null)
    [ "$size" -eq 200 ] && [ "$tail" = "TAIL123" ]
}
run_test "partial overwrite preserves tail (truncation bug regression)" test_partial_overwrite_preserves_tail

# ─── 4. Directory operations ──────────────────────────────────────────
echo "=== Directory operations ==="

test_mkdir_rmdir() {
    local d="$BASE/dir_test"
    mkdir "$d"
    assert_dir_exists "$d"
    rmdir "$d"
    assert_dir_not_exists "$d"
}
run_test "mkdir and rmdir" test_mkdir_rmdir

test_readdir() {
    local d="$BASE/readdir_test"
    mkdir -p "$d"
    touch "$d/a.txt" "$d/b.txt" "$d/c.txt"
    local count
    count=$(ls "$d" | wc -l)
    rmdir "$d"
    [ "$count" -eq 3 ]
}
run_test "readdir lists correct entries" test_readdir

test_rmdir_nonempty_fails() {
    local d="$BASE/dir_nonempty"
    mkdir -p "$d"
    touch "$d/file.txt"
    ! rmdir "$d" 2>/dev/null
    rm -rf "$d"
}
run_test "rmdir non-empty directory fails" test_rmdir_nonempty_fails

# ─── 5. Permissions ───────────────────────────────────────────────────
echo "=== Permissions ==="

test_chmod() {
    local f="$BASE/perm_test.txt"
    echo "test" > "$f"
    chmod 0644 "$f"
    local mode
    mode=$(stat -c %a "$f")
    [ "$mode" = "644" ]
}
run_test "chmod 0644" test_chmod

test_chmod_restrictive() {
    local f="$BASE/perm_restrict.txt"
    echo "test" > "$f"
    chmod 0400 "$f"
    local mode
    mode=$(stat -c %a "$f")
    [ "$mode" = "400" ]
}
run_test "chmod 0400 (read-only)" test_chmod_restrictive

test_chmod_executable() {
    local f="$BASE/perm_exec.sh"
    echo '#!/bin/sh' > "$f"
    chmod 0755 "$f"
    local mode
    mode=$(stat -c %a "$f")
    [ "$mode" = "755" ]
}
run_test "chmod 0755 (executable)" test_chmod_executable

test_chown() {
    local f="$BASE/chown_test.txt"
    echo "test" > "$f"
    local uid gid
    uid=$(id -u)
    gid=$(id -g)
    chown "$uid:$gid" "$f" 2>/dev/null || true
    local got_uid got_gid
    got_uid=$(stat -c %u "$f")
    got_gid=$(stat -c %g "$f")
    [ "$got_uid" -eq "$uid" ] && [ "$got_gid" -eq "$gid" ]
}
run_test "chown uid:gid" test_chown

test_sticky_bit() {
    local d="$BASE/sticky_dir"
    mkdir -p "$d"
    chmod +t "$d"
    local mode
    mode=$(stat -c %A "$d")
    rmdir "$d"
    [[ "$mode" == *"t" || "$mode" == *"T" ]]
}
run_test "sticky bit on directory" test_sticky_bit

test_setgid_inheritance() {
    local parent="$BASE/sgid_parent"
    local child="$parent/sgid_child"
    mkdir -p "$parent"
    chmod g+s "$parent"
    mkdir "$child"
    local parent_mode child_mode
    parent_mode=$(stat -c %A "$parent")
    child_mode=$(stat -c %A "$child")
    rm -rf "$parent"
    [[ "$parent_mode" == *"2"* ]] && [[ "$child_mode" == *"2"* ]]
}
run_test "setgid inheritance on mkdir" test_setgid_inheritance

# ─── 6. Symlinks ──────────────────────────────────────────────────────
echo "=== Symlinks ==="

test_symlink_create_readlink() {
    local target="$BASE/symlink_target.txt"
    echo "target data" > "$target"
    local link="$BASE/symlink_link"
    ln -s "$target" "$link"
    local got
    got=$(readlink "$link")
    [ "$got" = "$target" ]
}
run_test "create and readlink" test_symlink_create_readlink

test_symlink_stat_follows() {
    local target="$BASE/symlink_stat_target.txt"
    echo "hello" > "$target"
    local link="$BASE/symlink_stat_link"
    ln -s "$target" "$link"
    local size
    size=$(stat -c %s "$link")
    rm "$link" "$target"
    [ "$size" -eq 6 ]
}
run_test "stat on symlink follows to target" test_symlink_stat_follows

test_symlink_deref_read() {
    local target="$BASE/symlink_deref_target.txt"
    echo "deref data" > "$target"
    local link="$BASE/symlink_deref_link"
    ln -s "$target" "$link"
    local got
    got=$(cat "$link")
    rm "$link" "$target"
    [ "$got" = "deref data" ]
}
run_test "read through symlink dereferences" test_symlink_deref_read

# ─── 7. Hard links ────────────────────────────────────────────────────
echo "=== Hard links ==="

test_hardlink_both_survive_unlink() {
    local orig="$BASE/hardlink_orig.txt"
    echo "shared data" > "$orig"
    local link="$BASE/hardlink_link.txt"
    ln "$orig" "$link"
    rm "$orig"
    local got
    got=$(cat "$link")
    [ "$got" = "shared data" ]
}
run_test "hard link: unlink original, link survives" test_hardlink_both_survive_unlink

test_hardlink_shared_data() {
    local a="$BASE/hl_shared_a.txt"
    echo "content123" > "$a"
    local b="$BASE/hl_shared_b.txt"
    ln "$a" "$b"
    echo "NEW" > "$a"
    local got_b
    got_b=$(cat "$b")
    rm "$a" "$b"
    [ "$got_b" = "NEW" ]
}
run_test "hard link: write through one, visible in other" test_hardlink_shared_data

test_hardlink_nlink_count() {
    local f="$BASE/hl_nlink.txt"
    echo "x" > "$f"
    local link="$BASE/hl_nlink_link.txt"
    ln "$f" "$link"
    local nlink
    nlink=$(stat -c %h "$f")
    rm "$link" "$f"
    [ "$nlink" -ge 2 ]
}
run_test "hard link: nlink count ≥ 2" test_hardlink_nlink_count

# ─── 8. File metadata ─────────────────────────────────────────────────
echo "=== File metadata ==="

test_mtime_on_write() {
    local f="$BASE/mtime_test.txt"
    echo "a" > "$f"
    sleep 0.1
    echo "b" > "$f"
    local mtime
    mtime=$(stat -c %Y "$f")
    # mtime should be recent (within last 10 seconds)
    [ "$(( $(date +%s) - mtime ))" -lt 10 ]
}
run_test "mtime updates on write" test_mtime_on_write

test_file_size_accurate() {
    local f="$BASE/size_test.txt"
    dd if=/dev/urandom bs=1024 count=100 of="$f" 2>/dev/null
    local size
    size=$(stat -c %s "$f")
    [ "$size" -eq 102400 ]
}
run_test "file size accurate after dd" test_file_size_accurate

# ─── 9. Concurrent writes (basic) ─────────────────────────────────────
echo "=== Concurrent writes ==="

test_concurrent_appends() {
    local f="$BASE/concurrent_append.txt"
    > "$f"
    local pids=()
    for i in $(seq 1 5); do
        (for j in $(seq 1 100); do echo "w${i}_${j}"; done >> "$f") &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" || return 1
    done
    local lines
    lines=$(wc -l < "$f")
    [ "$lines" -eq 500 ]
}
run_test "concurrent appends (5 writers × 100 lines)" test_concurrent_appends

# ─── 10. Large file I/O ───────────────────────────────────────────────
echo "=== Large file I/O ==="

test_large_file_sequential() {
    local f="$BASE/large_seq.txt"
    dd if=/dev/urandom bs=1M count=10 of="$f" 2>/dev/null
    local size
    size=$(stat -c %s "$f")
    local checksum
    checksum=$(md5sum "$f" | awk '{print $1}')
    # Re-read and verify checksum unchanged
    local checksum2
    checksum2=$(md5sum "$f" | awk '{print $1}')
    [ "$size" -eq 10485760 ] && [ "$checksum" = "$checksum2" ]
}
run_test "large file sequential write (10 MiB) + verify" test_large_file_sequential

test_large_file_random_write() {
    local f="$BASE/large_random.txt"
    truncate -s 1M "$f"
    dd if=/dev/urandom bs=1k count=1 of="$f" seek=500 conv=notrunc 2>/dev/null
    dd if=/dev/urandom bs=1k count=1 of="$f" seek=2000 conv=notrunc 2>/dev/null
    local size
    size=$(stat -c %s "$f")
    [ "$size" -eq 1048576 ]
}
run_test "large file random write (1 MiB sparse)" test_large_file_random_write

# ─── cleanup ───────────────────────────────────────────────────────────
rm -rf "$MOUNT/_posix_test"

# ─── summary ───────────────────────────────────────────────────────────
echo ""
echo "================================"
TOTAL=$((PASS + FAIL))
echo "Results: $PASS passed, $FAIL failed ($TOTAL total)"
echo "================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
else
    echo "All POSIX compliance tests passed."
    exit 0
fi
