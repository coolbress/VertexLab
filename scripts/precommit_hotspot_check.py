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
    proc = subprocess.run(cmd, text=True, capture_output=True, check=True)
    return proc.stdout.strip()

def main() -> int:
    """Pre-commit hotspot diff check against origin/main.

    Returns:
        int: 0 if no hotspot conflicts detected, 1 if conflicts found.

    Notes:
        - Checks staged files for hotspot paths and compares with origin/main changes.
        - Subprocess errors result in a non-zero exit code (returns 1).
    """
    try:
        staged = set(run(["git", "diff", "--cached", "--name-only"]).splitlines())
        targets = staged if HOTSPOTS is None else HOTSPOTS.intersection(staged)
        if not targets:
            return 0
        base = run(["git", "merge-base", "origin/main", "HEAD"])
        changed_on_main = set(run(["git", "diff", "--name-only", f"{base}..origin/main"]).splitlines())
        conflicts = targets.intersection(changed_on_main)
        if conflicts:
            print("Hotspot files also changed on origin/main:", file=sys.stderr)
            for f in sorted(conflicts):
                print(f" - {f}", file=sys.stderr)
            print("Run: git fetch origin && git diff origin/main -- <파일경로>", file=sys.stderr)
            print("Or rehearsal: git fetch origin && git merge --no-commit --no-ff origin/main && git diff --name-only --diff-filter=U && git merge --abort", file=sys.stderr)
            return 1
        return 0
    except subprocess.CalledProcessError as e:
        print(f"Error running git command: {e}", file=sys.stderr)
        if e.stdout:
            print(e.stdout, file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
