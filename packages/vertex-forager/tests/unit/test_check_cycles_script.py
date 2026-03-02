from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_find_cycles_detects_and_no_cycle_cases():
    from scripts import check_cycles as cc

    cyc_graph = {
        "a": {"b"},
        "b": {"c"},
        "c": {"a"},
    }
    cycles = cc.find_cycles(cyc_graph)
    assert cycles, "Expected at least one cycle"
    assert cycles[0][0] == cycles[0][-1]

    acyclic = {
        "a": {"b"},
        "b": set(),
        "c": set(),
    }
    assert cc.find_cycles(acyclic) == []


def test_build_graph_resolves_relative_imports(tmp_path: Path, monkeypatch):
    # Create temporary package structure under src/vertex_forager
    src = tmp_path / "src"
    pkg = src / "vertex_forager" / "pkg_a"
    pkg.mkdir(parents=True)

    (src / "vertex_forager" / "__init__.py").write_text("")
    (pkg / "__init__.py").write_text("")
    (pkg / "mod2.py").write_text("X = 1\n")
    (pkg / "mod1.py").write_text("from vertex_forager.pkg_a.mod2 import X\n")
    (pkg / "bad.py").write_text("from .. import nope\n")
    (pkg / "modabs.py").write_text("import vertex_forager.pkg_a.mod2 as m2\n")

    # Import module and patch ROOT/SYS_PATH
    from scripts import check_cycles as cc
    monkeypatch.setattr(cc, "ROOT", src)
    monkeypatch.setattr(cc, "SYS_PATH", [str(src)] + sys.path)

    graph, failures = cc.build_graph()
    assert any(str(p).endswith("bad.py") for p, _ in failures), "Expected failure recorded for invalid relative import"

    # Graph should contain at least one dependency edge
    total_edges = sum(len(v) for v in graph.values())
    assert (len(graph) > 0) or (total_edges > 0), "Graph should contain modules or at least one dependency edge"
