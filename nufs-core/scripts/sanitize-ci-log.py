#!/usr/bin/env python3
"""Rewrite CI diagnostics so credential-like values cannot be uploaded."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


PATTERNS = [
    (re.compile(r"(?i)(authorization\s*:\s*(?:bearer|basic|aws4-hmac-sha256)\s+)[^\s]+"), r"\1[REDACTED]"),
    (re.compile(r'''(?i)(["']?(?:secret|token|password|passwd|api[-_]?key|access[-_]?key|secret[-_]?key)["']?\s*[:=]\s*["']?)[^"'\s,;}]+'''), r"\1[REDACTED]"),
    (re.compile(r'''(?i)(["']?(?:AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|GITHUB_TOKEN)["']?\s*[:=]\s*["']?)[^"'\s,;}]+'''), r"\1[REDACTED]"),
    (re.compile(r"(?i)(ghp_|gho_|ghs_|github_pat_)[A-Za-z0-9_:-]+"), "[REDACTED]"),
    (re.compile(r"AKIA[0-9A-Z]{12,}"), "[REDACTED]"),
    (re.compile(r"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY[0-9]*"), "[REDACTED]"),
]


def sanitize(value: str) -> str:
    for pattern, replacement in PATTERNS:
        value = pattern.sub(replacement, value)
    return value.replace(r"\1=[REDACTED]", "[REDACTED]=[REDACTED]")


def sanitize_file(path: Path) -> None:
    path.write_text(sanitize(path.read_text(errors="replace")), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    if not args.root.is_dir():
        return 0
    for path in args.root.rglob("*"):
        if path.is_file():
            sanitize_file(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
