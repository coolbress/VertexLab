#!/usr/bin/env python3
"""
Simple import cycle checker for the vertex_forager package.

Builds a directed import graph using modulefinder and detects cycles via DFS.
"""
from __future__ import annotations

import sys
import os
import ast
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
        - Parses imports via AST across all .py files under ROOT/PKG
        - Only collects dependencies that resolve to the package namespace
        - Resolves relative imports against each module's name
    """
    pkg_dir = ROOT / PKG.replace(".", os.sep)
    failures: list[tuple[str, str]] = []
    graph: dict[str, set[str]] = defaultdict(set)

    def module_name_for(path: Path) -> str:
        rel = path.relative_to(ROOT)
        parts = list(rel.parts)
        if not parts or parts[0] != PKG:
            return ""
        if parts[-1] == "__init__.py":
            return ".".join(parts[:-1])
        return ".".join(parts[:-1] + [parts[-1].removesuffix(".py")])

    def resolve_from(base: str, module: str | None, level: int, names: list[ast.alias]) -> list[str]:
        if level > 0:
            base_parts = base.split(".")
            parent = ".".join(base_parts[:-level]) if level <= len(base_parts) else ""
            target_base = parent if not module else f"{parent}.{module}"
            results: list[str] = []
            for n in names:
                name = n.name
                target = target_base if not name else (f"{target_base}.{name}" if target_base else name)
                results.append(target)
            return results
        else:
            mod = module or ""
            if not mod:
                return [alias.name for alias in names]
            return [mod]

    for path in pkg_dir.rglob("*.py"):
        modname = module_name_for(path)
        if not modname:
            continue
        _ = graph[modname]  # ensure node exists
        try:
            src = path.read_text(encoding="utf-8")
            tree = ast.parse(src, filename=str(path))
        except (UnicodeDecodeError, SyntaxError) as e:
            failures.append((str(path), repr(e)))
            continue
        def walk_no_type_checking(n: ast.AST):
            yield n
            for ch in ast.iter_child_nodes(n):
                if isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == "TYPE_CHECKING":
                    continue
                yield from walk_no_type_checking(ch)

        for node in walk_no_type_checking(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    dep = alias.name
                    if dep.startswith(PKG):
                        graph[modname].add(dep)
            elif isinstance(node, ast.ImportFrom):
                targets = resolve_from(modname, node.module, node.level, node.names)
                for dep in targets:
                    if dep.startswith(PKG):
                        graph[modname].add(dep)
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
