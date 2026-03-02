#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys

HOTSPOTS: set[str] | None = None  # None => check all staged files

def run(cmd: list[str]) -> str:
    """Run a shell command and capture stdout.

    Args:
        cmd: Command and arguments as a list of strings.

    Returns:
        The standard output of the command, decoded as a string.

    Raises:
        subprocess.CalledProcessError: If the command exits with a non-zero status.
    """
    return subprocess.check_output(cmd, text=True).strip()

def main() -> int:
    """Pre-commit hotspot diff check against origin/main.

    Returns:
        int: 0 if no hotspot conflicts detected, 1 if conflicts found.

    Notes:
        - Checks staged files for hotspot paths and compares with origin/main changes.
        - Any subprocess errors are treated as pass-through (returns 0).
    """
    try:
        staged = set(run(["git", "diff", "--cached", "--name-only"]).splitlines())
        targets = staged if not HOTSPOTS else HOTSPOTS.intersection(staged)
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
    except subprocess.CalledProcessError as e:
        print(f"Error running git command: {e}")
        out = getattr(e, "output", "")
        if out:
            print(out)
        return 1

if __name__ == "__main__":
    sys.exit(main())
