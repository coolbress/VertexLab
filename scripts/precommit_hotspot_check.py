#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys

HOTSPOTS = {
    "packages/vertex-forager/src/vertex_forager/core/contracts.py",
    "packages/vertex-forager/src/vertex_forager/api.py",
}

def run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True).strip()

def main() -> int:
    try:
        staged = set(run(["git", "diff", "--cached", "--name-only"]).splitlines())
        targets = HOTSPOTS.intersection(staged)
        if not targets:
            return 0
        base = run(["git", "merge-base", "origin/main", "HEAD"])
        changed_on_main = set(run(["git", "diff", "--name-only", f"{base}..origin/main"]).splitlines())
        conflicts = targets.intersection(changed_on_main)
        if conflicts:
            print("Hotspot files also changed on origin/main:")
            for f in sorted(conflicts):
                print(f" - {f}")
            print("Run: git fetch origin && git diff origin/main -- <파일경로>")
            print("Or rehearsal: git fetch origin && git merge --no-commit --no-ff origin/main && git diff --name-only --diff-filter=U && git merge --abort")
            return 1
        return 0
    except subprocess.CalledProcessError:
        return 0

if __name__ == "__main__":
    sys.exit(main())
