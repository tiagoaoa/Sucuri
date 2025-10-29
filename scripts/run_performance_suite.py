#!/usr/bin/env python3
"""Run the full Sucuri performance comparison (threads vs processes).

This helper wraps ``examples/perf_benchmark.py`` so you can kick off the
complete experiment suite with a single command.  By default it benchmarks
both the thread-enabled scheduler (Python 3.14 no-GIL) and the legacy
multiprocessing scheduler across worker counts 1..12, writes all raw samples
to a timestamped results directory, and asks ``perf_benchmark.py`` to emit the
speed-up graphs.
"""

from __future__ import annotations

import argparse
import importlib
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, List


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter used to run the benchmarks (default: current)",
    )
    parser.add_argument(
        "--scenarios",
        nargs="*",
        default=None,
        help="Subset of scenarios to execute (default: all)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=list(range(1, 13)),
        help="Worker counts to benchmark (default: 1..12)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of repeated runs per worker count (default: 5)",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Scale factor applied to each scenario workload (default: 1.0)",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Optional label recorded alongside every sample (default: none)",
    )
    parser.add_argument(
        "--engines",
        nargs="+",
        default=["threads", "processes"],
        help="Engines to benchmark (default: threads processes)",
    )
    parser.add_argument(
        "--plot-format",
        choices=["png", "pdf", "svg"],
        default="png",
        help="Image format for generated graphs (default: png)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("perf_results"),
        help="Directory where run artefacts are stored (default: ./perf_results)",
    )
    parser.add_argument(
        "--skip-whatsapp",
        action="store_true",
        help="Disable creation of WhatsApp-friendly JPEG copies",
    )
    return parser.parse_args()


def ensure_matplotlib_available(python_exe: str) -> None:
    """Emit a warning if matplotlib is unavailable for the chosen Python."""

    if python_exe != sys.executable:
        # We cannot easily introspect another interpreter; rely on benchmark script.
        return

    try:
        importlib.import_module("matplotlib")
    except ImportError:
        print(
            "Warning: matplotlib is not available in this environment."
            " Plots will be skipped unless you install it.",
            file=sys.stderr,
        )


def format_workers(values: Iterable[int]) -> List[str]:
    return [str(v) for v in values]


def main() -> None:
    args = parse_args()

    ensure_matplotlib_available(args.python)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_root = args.output_root
    if not output_root.is_absolute():
        output_root = REPO_ROOT / output_root
    run_dir = output_root / timestamp
    graph_dir = run_dir / "graphs"
    whatsapp_dir: Path | None = None
    if not args.skip_whatsapp:
        whatsapp_dir = run_dir / "whatsapp"
    results_path = run_dir / "samples.json"

    run_dir.mkdir(parents=True, exist_ok=True)

    benchmark_script = REPO_ROOT / "examples" / "perf_benchmark.py"
    cmd: List[str] = [args.python, str(benchmark_script)]

    if args.scenarios:
        cmd.append("--scenarios")
        cmd.extend(args.scenarios)

    cmd.append("--engines")
    cmd.extend(args.engines)

    cmd.append("--workers")
    cmd.extend(format_workers(args.workers))

    cmd.extend(["--runs", str(args.runs)])
    cmd.extend(["--scale", str(args.scale)])
    cmd.extend(["--plot-format", args.plot_format])
    cmd.extend(["--graph-dir", str(graph_dir)])
    if whatsapp_dir is not None:
        cmd.extend(["--whatsapp-dir", str(whatsapp_dir)])
    cmd.extend(["--output", str(results_path)])

    if args.label is not None:
        cmd.extend(["--label", args.label])

    print("Running:")
    print(" ".join(cmd))
    sys.stdout.flush()

    subprocess.run(cmd, check=True)

    print("\nArtifacts written to:")
    print(f"  Results JSON: {results_path}")
    print(f"  Graphs dir:   {graph_dir}")
    if whatsapp_dir is not None:
        print(f"  WhatsApp dir: {whatsapp_dir}")


if __name__ == "__main__":
    main()
