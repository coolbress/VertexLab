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
    finder = ModuleFinder(path=SYS_PATH)
    # Seed with the package root
    finder.run_script(os.path.join(ROOT, PKG, "__init__.py"))
    graph: dict[str, set[str]] = defaultdict(set)
    for name, mod in finder.modules.items():
        if not name.startswith(PKG):
            continue
        for dep in mod.globalnames.keys():
            # Skip non-modules
            if dep.startswith(PKG):
                graph[name].add(dep)
        for dep in mod.starimports:
            if dep.startswith(PKG):
                graph[name].add(dep)
        for dep in mod.code.co_names if mod.code else []:
            if isinstance(dep, str) and dep.startswith(PKG):
                graph[name].add(dep)
    return graph

def find_cycles(graph: dict[str, set[str]]) -> list[list[str]]:
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
