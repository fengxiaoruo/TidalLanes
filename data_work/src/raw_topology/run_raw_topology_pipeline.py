"""
Run the standalone raw-road topology prototype end to end.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_OUTPUT = ROOT / "outputs" / "raw_topology_mvp"


def parse_args():
    parser = argparse.ArgumentParser(description="Run the standalone raw-road topology prototype")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT), help="Prototype output directory")
    parser.add_argument("--snap-tol", type=float, default=15.0, help="Endpoint snap tolerance in meters")
    parser.add_argument("--node-tol", type=float, default=0.5, help="Final node clustering tolerance in meters")
    parser.add_argument("--min-edge-len", type=float, default=3.0, help="Drop edges shorter than this threshold")
    return parser.parse_args()


def run_module(module: str, *args: str):
    cmd = [sys.executable, "-m", module, *args]
    print(f"[raw_topology] running {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def main():
    args = parse_args()
    out = str(Path(args.output_root))
    run_module("src.raw_topology.clean_raw_roads", "--output-root", out)
    run_module(
        "src.raw_topology.build_topology_graph",
        "--output-root",
        out,
        "--snap-tol",
        str(args.snap_tol),
        "--node-tol",
        str(args.node_tol),
        "--min-edge-len",
        str(args.min_edge_len),
    )
    run_module("src.raw_topology.diagnose_topology_graph", "--output-root", out)
    run_module("src.raw_topology.refine_conservative_topology", "--output-root", out)
    run_module("src.raw_topology.export_review_artifacts", "--output-root", out)
    print(f"[raw_topology] complete output_root={out}")


if __name__ == "__main__":
    main()
