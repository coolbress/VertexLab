#!/usr/bin/env python3
"""
Simple import cycle checker for the vertex_forager package.

Builds a directed import graph using modulefinder and detects cycles via DFS.
"""
from __future__ import annotations

import sys
import os
from modulefinder import ModuleFinder
from collections import defaultdict
from pathlib import Path
from fnmatch import fnmatch

PKG = "vertex_forager"
ROOT = Path(__file__).resolve().parent.parent / "packages" / "vertex-forager" / "src"
SYS_PATH = [str(ROOT)] + sys.path

def build_graph() -> tuple[dict[str, set[str]], list[tuple[str, str]]]:
    """Build dependency graph for the package.

    Args:
        None

    Returns:
        tuple[dict[str, set[str]], list[tuple[str, str]]]:
            - Map of module name -> set of dependent module names
            - List of (path, error) for files that failed to load

    Notes:
        - Uses ModuleFinder across all .py files under ROOT/PKG
        - Only collects dependencies that resolve to the package namespace
        - Resolves relative imports against each module's name
    """
    finder = ModuleFinder(path=SYS_PATH)
    pkg_dir = ROOT / PKG.replace(".", os.sep)
    failures: list[tuple[str, str]] = []
    for path in pkg_dir.rglob("*.py"):
        try:
            finder.run_script(str(path))
        except (ImportError, SyntaxError, ModuleNotFoundError, AttributeError) as e:
            failures.append((str(path), repr(e)))
        except Exception:
            raise

    def _resolve(name: str, base: str) -> str:
        if not name:
            return name
        if name.startswith("."):
            dots = len(name) - len(name.lstrip("."))
            base_parts = base.split(".")
            parent = ".".join(base_parts[:-dots]) if dots else base
            rel = name.lstrip(".")
            return parent if not rel else f"{parent}.{rel}"
        return name

    graph: dict[str, set[str]] = defaultdict(set)
    for name, mod in finder.modules.items():
        if not name.startswith(PKG):
            continue
        base = getattr(mod, "__name__", name)
        _ = graph[name]  # ensure node exists even if no deps
        globalnames = getattr(mod, "globalnames", {})
        for dep in globalnames.keys():
            dep_abs = _resolve(dep, base)
            if dep_abs.startswith(PKG):
                graph[name].add(dep_abs)
        starimports = getattr(mod, "starimports", [])
        for dep in starimports:
            dep_abs = _resolve(dep, base)
            if dep_abs.startswith(PKG):
                graph[name].add(dep_abs)
        code_obj = getattr(mod, "code", None)
        if code_obj is not None:
            for dep in getattr(code_obj, "co_names", ()):
                if isinstance(dep, str):
                    dep_abs = _resolve(dep, base)
                    if dep_abs.startswith(PKG):
                        graph[name].add(dep_abs)
    return graph, failures

def find_cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    """Detect import cycles in the provided graph.

    Args:
        graph: Dependency graph (module -> set of module dependencies)

    Returns:
        list[list[str]]: List of cycles found; each cycle repeats the start at the end

    Notes:
        - Uses DFS with visitation states (0/1/2) to detect back-edges
    """
    visited: dict[str, int] = {}  # 0=unvisited, 1=visiting, 2=done
    stack: list[str] = []
    cycles: list[list[str]] = []

    def dfs(node: str) -> None:
        state = visited.get(node, 0)
        if state == 1:
            # Found a cycle
            if node in stack:
                idx = stack.index(node)
                cycles.append(stack[idx:] + [node])
            return
        if state == 2:
            return
        visited[node] = 1
        stack.append(node)
        for neigh in graph.get(node, []):
            dfs(neigh)
        stack.pop()
        visited[node] = 2

    for node in list(graph.keys()):
        if visited.get(node, 0) == 0:
            dfs(node)
    return cycles

def main() -> int:
    """Entry point: build graph, find cycles, print summary, return exit code.

    Returns:
        int: 0 if no cycles, 1 if cycles detected

    Notes:
        - Relies on module-scope SYS_PATH/ROOT/PKG assumptions
    """
    graph, failures = build_graph()
    allow_globs = [
        "*/*/__init__.py",
        "*/api.py",
        "*/cli.py",
        "*/constants.py",
    ]
    if failures:
        print("Failed to analyze the following modules:")
        for path, err in failures:
            print(f" - {path}: {err}")
        # Optionally warn allowed patterns
        allowed = [(p, e) for (p, e) in failures if any(fnmatch(p, g) for g in allow_globs)]
        if allowed:
            print("Note: the following match allowlist patterns (still failing):")
            for path, err in allowed:
                print(f" - {path}")
        return 1
    cycles = find_cycles(graph)
    if cycles:
        print("Detected import cycles:")
        for cyc in cycles:
            print(" -> ".join(cyc))
        return 1
    print("No import cycles detected.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
