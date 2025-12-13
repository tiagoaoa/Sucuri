"""Benchmark Python vs Rust LCS backends and plot speedups across workers."""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
from pathlib import Path
from statistics import mean
from time import perf_counter
from typing import Iterable, List, Tuple

import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.LCS import lcs_parallel


def build_rust_extension(crate_dir: Path, target_dir: Path) -> None:
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "never")
    env.setdefault("PYO3_USE_ABI3_FORWARD_COMPATIBILITY", "1")
    subprocess.run(
        ["cargo", "build", "--release"],
        check=True,
        cwd=str(crate_dir),
        env=env,
    )
    if not target_dir.exists():
        raise RuntimeError(f"Rust build did not produce artifacts at {target_dir}")

    os.environ.setdefault("SUCURI_RUST_PATH", str(target_dir))
    if str(target_dir) not in sys.path:
        sys.path.insert(0, str(target_dir))


def generate_sequences(length: int, seed: int = 42, alphabet: str = "ACGT") -> Tuple[str, str]:
    rng = random.Random(seed)
    seq_a = "".join(rng.choice(alphabet) for _ in range(length))
    seq_b = "".join(rng.choice(alphabet) for _ in range(length))
    return seq_a, seq_b


def best_runtime(
    context: lcs_parallel.LCSContext,
    workers: int,
    compute_fn,
    *,
    repeats: int = 3,
) -> float:
    durations: List[float] = []
    for _ in range(repeats):
        start = perf_counter()
        lcs_parallel.compute_lcs(context, workers, compute_fn)
        durations.append(perf_counter() - start)
    return min(durations)


def measure_speedups(
    worker_counts: Iterable[int],
    context: lcs_parallel.LCSContext,
    *,
    repeats: int = 3,
) -> Tuple[List[float], List[float]]:
    python_baseline = best_runtime(context, 1, lcs_parallel.lcs_block_python, repeats=repeats)
    rust_baseline = best_runtime(context, 1, lcs_parallel.lcs_block_rust, repeats=repeats)

    python_speedups = []
    rust_speedups = []

    for workers in worker_counts:
        py_time = best_runtime(context, workers, lcs_parallel.lcs_block_python, repeats=repeats)
        rs_time = best_runtime(context, workers, lcs_parallel.lcs_block_rust, repeats=repeats)
        python_speedups.append(python_baseline / py_time)
        rust_speedups.append(rust_baseline / rs_time)

    return python_speedups, rust_speedups


def plot_results(
    worker_counts: List[int],
    python_speedups: List[float],
    rust_speedups: List[float],
    output_path: Path,
) -> None:
    plt.figure(figsize=(8, 5))
    plt.plot(worker_counts, python_speedups, marker="o", label="Python only")
    plt.plot(worker_counts, rust_speedups, marker="s", label="Python + Rust")
    plt.xlabel("Workers")
    plt.ylabel("Speedup vs 1 worker (best of runs)")
    plt.title("LCS Block Scheduler Speedups")
    plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.6)
    plt.legend()
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=160)
    plt.close()


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--length", type=int, default=4096, help="Sequence length for both inputs")
    parser.add_argument("--block", type=int, default=256, help="Block size used in the grid")
    parser.add_argument("--repeats", type=int, default=3, help="Runs per configuration (best is kept)")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("examples/LCS/lcs_speedup.png"),
        help="Path to store the generated plot",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    crate_dir = repo_root / "rust" / "sucuri_lcs"
    target_dir = crate_dir / "target" / "release"
    build_rust_extension(crate_dir, target_dir)

    seq_a, seq_b = generate_sequences(args.length)
    context = lcs_parallel.LCSContext.from_strings(seq_a, seq_b, args.block)
    worker_counts = list(range(1, 13))
    python_speedups, rust_speedups = measure_speedups(worker_counts, context, repeats=args.repeats)

    plot_results(worker_counts, python_speedups, rust_speedups, args.output)

    summary = {
        "length": args.length,
        "block": args.block,
        "worker_counts": worker_counts,
        "python_speedups": python_speedups,
        "rust_speedups": rust_speedups,
        "mean_python_speedup": mean(python_speedups),
        "mean_rust_speedup": mean(rust_speedups),
        "output": str(args.output),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
