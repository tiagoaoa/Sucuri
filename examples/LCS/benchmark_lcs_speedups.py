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
    import numpy as np
    from matplotlib.patches import FancyBboxPatch, Rectangle
    import matplotlib.patheffects as path_effects

    # Enable xkcd-style hand-drawn look
    with plt.xkcd(scale=1.2, length=120, randomness=3):
        fig, ax = plt.subplots(figsize=(12, 7), facecolor="white")
        ax.set_facecolor("white")

        # Colors: coral/orange-red and teal/cyan
        coral = "#FF6B6B"
        coral_hatch = "#FF8C42"
        teal = "#4ECDC4"
        teal_hatch = "#7FDBDA"
        gray_line = "#9E9E9E"

        x = np.array(worker_counts)
        bar_width = 0.35

        # Draw bars with cross-hatching
        bars_python = ax.bar(
            x - bar_width / 2,
            python_speedups,
            bar_width,
            label="Python only",
            color=coral,
            edgecolor="#333",
            linewidth=2,
            hatch="//",
            zorder=3,
        )
        bars_rust = ax.bar(
            x + bar_width / 2,
            rust_speedups,
            bar_width,
            label="Python + Rust",
            color=teal,
            edgecolor="#333",
            linewidth=2,
            hatch="\\\\",
            zorder=3,
        )

        # Add the line overlay with big gray circles
        line_y = [(p + r) / 2 for p, r in zip(python_speedups, rust_speedups)]
        ax.plot(
            x,
            line_y,
            color=gray_line,
            linewidth=2,
            linestyle="-",
            zorder=2,
            alpha=0.7,
        )
        ax.scatter(
            x,
            line_y,
            s=200,
            color="#E0E0E0",
            edgecolor=gray_line,
            linewidth=2,
            zorder=4,
        )

        # Faint grid
        ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.3, color="#CCCCCC", zorder=0)
        ax.set_axisbelow(True)

        # Red dashed vertical line at peak performance
        peak_idx = np.argmax(python_speedups)
        peak_x = worker_counts[peak_idx]
        ax.axvline(
            x=peak_x,
            color="#FF0000",
            linestyle="--",
            linewidth=2.5,
            zorder=5,
        )
        ax.text(
            peak_x + 0.15,
            max(max(python_speedups), max(rust_speedups)) * 0.95,
            f"PEAK!\n{peak_x} workers",
            fontsize=11,
            color="#FF0000",
            fontweight="bold",
            ha="left",
            va="top",
        )

        # Title with yellow highlighter effect
        title_text = "LCS Block Scheduler Speedups"
        title = ax.set_title(
            title_text,
            fontsize=22,
            fontweight="bold",
            color="#222",
            pad=20,
        )
        title.set_path_effects([
            path_effects.withStroke(linewidth=12, foreground="#FFEB3B", alpha=0.7),
            path_effects.Normal(),
        ])

        # Axis labels - casual style
        ax.set_xlabel("Workers", fontsize=14, fontweight="bold")
        ax.set_ylabel("Speedup vs 1 worker", fontsize=14, fontweight="bold")

        # X-axis ticks rotated 45 degrees
        ax.set_xticks(worker_counts)
        ax.set_xticklabels(
            [str(w) for w in worker_counts],
            rotation=45,
            ha="right",
            fontsize=11,
            fontweight="bold",
        )
        ax.tick_params(axis="y", labelsize=11)

        # Legend at bottom center with rounded rectangle
        legend = ax.legend(
            loc="upper center",
            bbox_to_anchor=(0.5, -0.15),
            ncol=2,
            fontsize=12,
            frameon=True,
            fancybox=True,
            shadow=True,
            edgecolor="#666",
        )
        legend.get_frame().set_facecolor("#FAFAFA")
        legend.get_frame().set_linewidth(2)

        # Add smug annotation
        max_speedup = max(max(python_speedups), max(rust_speedups))
        ax.annotate(
            "no GIL go brrr",
            xy=(worker_counts[-1], max_speedup * 0.6),
            fontsize=10,
            fontstyle="italic",
            color="#666",
            ha="right",
        )

        ax.set_xlim(0.2, max(worker_counts) + 0.8)
        ax.set_ylim(0, max_speedup * 1.15)

        plt.tight_layout()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=160, facecolor="white", edgecolor="none")
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
