#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
from pathlib import Path
from typing import List, Tuple

START = 198
END   = 5197
MONTH_PREFIX = "2404"  # e.g., 2404-00198 .. 2404-05197

def expected_names() -> List[str]:
    return [f"{MONTH_PREFIX}-{i:05d}" for i in range(START, END + 1)]

def scan_root(root: Path):
    want = expected_names()
    want_set = set(want)

    actual = {p.name for p in root.iterdir() if p.is_dir()}
    missing = sorted(n for n in want if n not in actual)
    extras  = sorted(n for n in actual if n.startswith(f"{MONTH_PREFIX}-") and n not in want_set)

    has_tmp = []
    missing_meta = []
    missing_refs = []

    for name in want:
        sub = root / name
        if not sub.is_dir():
            continue

        # ---- Check tmp / *.tmp ----
        found_tmp = False
        for dp, dirnames, filenames in os.walk(sub):
            # tmp or _tmp folders
            for d in dirnames:
                if d.lower() in ("tmp", "_tmp"):
                    found_tmp = True
                    break
            # *.tmp files
            if any(fn.lower().endswith(".tmp") for fn in filenames):
                found_tmp = True
            if found_tmp:
                break
        if found_tmp:
            has_tmp.append(name)

        # ---- Check metadata.json and references.json ----
        meta = sub / "metadata.json"
        refs = sub / "references.json"
        if not meta.is_file():
            missing_meta.append(name)
        if not refs.is_file():
            missing_refs.append(name)

    return missing, has_tmp, missing_meta, missing_refs, extras


def main():
    ap = argparse.ArgumentParser(description="Verify integrity of 5000 arXiv folders for Lab01.")
    ap.add_argument("--root", default="22127227",
                    help="Root directory containing 2404-xxxxx folders (default: ./22127227)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"[ERR] Invalid root: {root}")

    missing, has_tmp, missing_meta, missing_refs, extras = scan_root(root)

    total_expected = END - START + 1
    print("========== INTEGRITY REPORT ==========")
    print(f"Root: {root}")
    print(f"Expected total: {total_expected} (from {MONTH_PREFIX}-{START:05d} to {MONTH_PREFIX}-{END:05d})")
    existing_count = sum(1 for d in root.iterdir() if d.is_dir() and d.name.startswith(MONTH_PREFIX + '-'))
    print(f"Existing folders (prefix {MONTH_PREFIX}-): {existing_count}")
    print()

    print(f"[Missing folders] {len(missing)}:")
    if missing:
        print("\n".join(missing))
    else:
        print("(None)")

    print("\n--------------------------------\n")

    print(f"[Folders containing tmp/_tmp or *.tmp] {len(has_tmp)}:")
    if has_tmp:
        print("\n".join(has_tmp))
    else:
        print("(None)")

    print("\n--------------------------------\n")

    print(f"[Missing metadata.json] {len(missing_meta)}:")
    if missing_meta:
        print("\n".join(missing_meta))
    else:
        print("(All present)")

    print("\n--------------------------------\n")

    print(f"[Missing references.json] {len(missing_refs)}:")
    if missing_refs:
        print("\n".join(missing_refs))
    else:
        print("(All present)")

    if extras:
        print("\n--------------------------------\n")
        print(f"[Out-of-range] {len(extras)} folders named 2404-* but not in {START}..{END}:")
        print("\n".join(extras))

    print("\n========== END ==========")


if __name__ == "__main__":
    main()
