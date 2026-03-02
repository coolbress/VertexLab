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

PKG = "vertex_forager"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "packages", "vertex-forager", "src"))
SYS_PATH = [ROOT] + sys.path

def build_graph() -> dict[str, set[str]]:
    """Build dependency graph for the package.

    Args:
        None

    Returns:
        dict[str, set[str]]: Map of module name -> set of dependent module names

    Notes:
        - Uses ModuleFinder across all .py files under ROOT/PKG
        - Only collects dependencies that resolve to the package namespace
        - Resolves relative imports against each module's name
    """
    finder = ModuleFinder(path=SYS_PATH)
    pkg_dir = os.path.join(ROOT, PKG.replace(".", os.sep))
    for root, _, files in os.walk(pkg_dir):
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                try:
                    finder.run_script(path)
                except Exception:
                    # Skip files that fail to load under ModuleFinder
                    pass

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
        for dep in mod.globalnames.keys():
            dep_abs = _resolve(dep, base)
            if dep_abs.startswith(PKG):
                graph[name].add(dep_abs)
        for dep in mod.starimports:
            dep_abs = _resolve(dep, base)
            if dep_abs.startswith(PKG):
                graph[name].add(dep_abs)
        for dep in mod.code.co_names if mod.code else []:
            if isinstance(dep, str):
                dep_abs = _resolve(dep, base)
                if dep_abs.startswith(PKG):
                    graph[name].add(dep_abs)
    return graph

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
    graph = build_graph()
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
