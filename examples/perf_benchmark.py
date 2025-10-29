"""Performance benchmark suite derived from the classic Sucuri examples.

Run this script across different Sucuri execution engines (e.g., the legacy
multiprocessing scheduler versus the Python 3.14 no-GIL thread scheduler) to
compare throughput of the runtime.

Example usage::

    python examples/perf_benchmark.py --workers 1 4 8 --runs 5 --output perf.json
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Callable, Dict, Iterable, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pyDF.nodes as thread_nodes
import pyDF.pydf as thread_runtime
import pyDF.nodes_multiprocess as process_nodes
import pyDF.pydf_multiprocess as process_runtime


Runner = Callable[["Engine", int, float], Dict[str, float]]


@dataclass(frozen=True)
class Engine:
    """Execution engine (runtime + helper nodes) used by the benchmarks."""

    name: str
    description: str
    runtime: ModuleType
    nodes: ModuleType


ENGINES: Dict[str, Engine] = {
    "threads": Engine(
        name="threads",
        description="Thread-based scheduler (Python 3.14 no-GIL)",
        runtime=thread_runtime,
        nodes=thread_nodes,
    ),
    "processes": Engine(
        name="processes",
        description="Multiprocessing scheduler (legacy)",
        runtime=process_runtime,
        nodes=process_nodes,
    ),
}


@dataclass(frozen=True)
class Scenario:
    """Benchmark scenario metadata."""

    name: str
    description: str
    runner: Runner


# ---------------------------------------------------------------------------
# Scenario implementations


def calc_pi_runner(engine: Engine, workers: int, scale: float) -> Dict[str, float]:
    """Adapted from examples/calc_pi.py. Approximates pi via Riemann sums."""

    total_steps = max(int(2_000_000 * scale), workers)
    stride = 1.0 / total_steps
    result: Dict[str, float] = {}

    Graph = engine.runtime.DFGraph
    Node = engine.runtime.Node
    Feeder = engine.nodes.Feeder

    def partial(values: Sequence):
        worker_id, step_stride, steps, worker_count = values[0]
        acc = 0.0
        for step in range(worker_id, steps, worker_count):
            x = (step + 0.5) * step_stride
            acc += 4.0 / (1.0 + x * x)
        return acc * step_stride

    def reduce_pi(values: Sequence[float]):
        pi_estimate = sum(values)
        result["pi"] = pi_estimate
        return None

    graph = Graph()
    reducer = Node(reduce_pi, workers)
    reducer.pin([0])
    graph.add(reducer)

    for wid in range(workers):
        config = Feeder((wid, stride, total_steps, workers))
        graph.add(config)

        partial_node = Node(partial, 1)
        graph.add(partial_node)

        config.add_edge(partial_node, 0)
        partial_node.add_edge(reducer, wid)

    scheduler = engine.runtime.Scheduler(graph, workers, mpi_enabled=False)

    start = time.perf_counter()
    scheduler.start()
    elapsed = time.perf_counter() - start

    return {"elapsed": elapsed, **result}


def text_pipeline_runner(engine: Engine, workers: int, scale: float) -> Dict[str, float]:
    """Loosely follows examples/pipeline.py using in-memory text payloads."""

    base_lines = [
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.\n",
        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.\n",
        "Nisi ut aliquip ex ea commodo consequat.\n",
    ]
    repeat = max(int(50_000 * scale / max(len(base_lines), 1)), 1)
    lines = base_lines * repeat
    metrics = {"lines": float(len(lines)), "checksum": 0.0}
    TaggedValue = engine.nodes.TaggedValue
    Node = engine.runtime.Node
    Source = engine.nodes.Source

    def normalize(values: Sequence):
        value, tag = unwrap_tagged(values[0], TaggedValue)
        cleaned = value.strip().lower()
        return wrap_tagged(cleaned, tag, TaggedValue)

    def embellish(values: Sequence):
        value, tag = unwrap_tagged(values[0], TaggedValue)
        transformed = value.replace(" ", "-") * 2
        return wrap_tagged(transformed, tag, TaggedValue)

    def sink(values: Sequence):
        value, _ = unwrap_tagged(values[0], TaggedValue)
        metrics["checksum"] += len(value)
        return None

    graph = engine.runtime.DFGraph()
    source = Source(iter(lines))
    norm = Node(normalize, 1)
    decorate = Node(embellish, 1)
    drain = Node(sink, 1)

    graph.add(source)
    graph.add(norm)
    graph.add(decorate)
    graph.add(drain)

    source.add_edge(norm, 0)
    norm.add_edge(decorate, 0)
    decorate.add_edge(drain, 0)

    scheduler = engine.runtime.Scheduler(graph, workers, mpi_enabled=False)

    start = time.perf_counter()
    scheduler.start()
    elapsed = time.perf_counter() - start

    return {"elapsed": elapsed, **metrics}


SCENARIOS: Dict[str, Scenario] = {
    "calc_pi": Scenario(
        name="calc_pi",
        description="Numerical integration of 1/(1+x^2) to approximate pi",
        runner=calc_pi_runner,
    ),
    "text_pipeline": Scenario(
        name="text_pipeline",
        description="Three-stage text processing pipeline",
        runner=text_pipeline_runner,
    ),
}


# ---------------------------------------------------------------------------
# CLI driver


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenarios",
        nargs="*",
        choices=sorted(SCENARIOS.keys()),
        help="Subset of scenarios to execute (default: all)",
    )
    parser.add_argument(
        "--engines",
        nargs="+",
        choices=sorted(ENGINES.keys()),
        default=["threads", "processes"],
        help="Execution engines to benchmark (default: threads processes)",
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
        default=3,
        help="Repeat each scenario this many times",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Scale factor applied to the workload size",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Optional label (e.g., interpreter build) recorded with results",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="If provided, write JSON results to this path",
    )
    parser.add_argument(
        "--graph-dir",
        type=Path,
        default=Path("perf_graphs"),
        help="Directory where speed-up plots will be written (default: perf_graphs)",
    )
    parser.add_argument(
        "--plot-format",
        choices=["png", "pdf", "svg"],
        default="png",
        help="Image format for generated plots (default: png)",
    )
    parser.add_argument(
        "--whatsapp-dir",
        type=Path,
        help=(
            "If provided, also create downscaled JPEG copies suitable for "
            "messaging apps in this directory"
        ),
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    selected = args.scenarios or list(SCENARIOS.keys())
    engines = [ENGINES[name] for name in args.engines]
    baseline_workers = min(args.workers)
    records: List[Dict[str, float]] = []
    aggregates: Dict[str, Dict[str, Dict[int, List[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )

    for scenario_name in selected:
        scenario = SCENARIOS[scenario_name]
        print(f"\n==> Scenario: {scenario.name} — {scenario.description}")
        for engine in engines:
            print(f"  Engine: {engine.name} — {engine.description}")
            engine_baseline: float | None = None
            for workers in args.workers:
                timings: List[float] = []
                for run in range(1, args.runs + 1):
                    metrics = scenario.runner(engine, workers, args.scale)
                    elapsed = metrics.pop("elapsed")
                    timings.append(elapsed)
                    aggregates[scenario.name][engine.name][workers].append(elapsed)

                    record = {
                        "scenario": scenario.name,
                        "engine": engine.name,
                        "workers": workers,
                        "run": run,
                        "elapsed": elapsed,
                    }
                    if args.label is not None:
                        record["label"] = args.label
                    record.update(metrics)
                    records.append(record)

                    metric_info = f" | metrics: {metrics}" if metrics else ""
                    print(
                        f"    Run {run} @ {workers} workers: {elapsed:.3f}s{metric_info}"
                    )

                mean = statistics.mean(timings)
                stdev = statistics.pstdev(timings) if len(timings) > 1 else 0.0
                if workers == baseline_workers:
                    engine_baseline = mean
                speedup = (
                    engine_baseline / mean
                    if engine_baseline is not None and mean > 0
                    else float("nan")
                )
                print(
                    f"    Summary {workers} workers: mean={mean:.3f}s "
                    f"σ={stdev:.3f}s speed-up={speedup:.2f}x vs {baseline_workers}"
                )

    if args.output:
        args.output.write_text(json.dumps(records, indent=2))
        print(f"\nWrote {len(records)} samples to {args.output}")

    if args.graph_dir:
        generate_speedup_plots(
            aggregates,
            engines,
            args.workers,
            Path(args.graph_dir),
            args.plot_format,
            baseline_workers,
            Path(args.whatsapp_dir) if args.whatsapp_dir else None,
        )


def unwrap_tagged(value, tagged_cls):
    if isinstance(value, tagged_cls):
        return value.value, value.tag
    return value, None


def wrap_tagged(value, tag, tagged_cls):
    if tag is None:
        return value
    return tagged_cls(value, tag)


def generate_speedup_plots(
    aggregates: Dict[str, Dict[str, Dict[int, List[float]]]],
    engines: Sequence[Engine],
    workers: Sequence[int],
    graph_dir: Path,
    fmt: str,
    baseline_workers: int,
    whatsapp_dir: Optional[Path] = None,
) -> None:
    if not aggregates:
        return

    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        print(
            "matplotlib is not available; skipping speed-up plot generation. "
            "Install matplotlib to enable graphs."
        )
        return

    graph_dir.mkdir(parents=True, exist_ok=True)

    for scenario_name, engine_data in aggregates.items():
        fig, ax = plt.subplots()
        plotted = False

        for engine in engines:
            worker_map = engine_data.get(engine.name)
            if not worker_map:
                continue
            worker_counts = sorted(worker_map.keys())
            if baseline_workers not in worker_map:
                continue

            baseline = statistics.mean(worker_map[baseline_workers])
            if baseline <= 0:
                continue

            speedups = []
            for wid in worker_counts:
                mean = statistics.mean(worker_map[wid])
                speedups.append(baseline / mean if mean > 0 else float("nan"))

            ax.plot(
                worker_counts,
                speedups,
                marker="o",
                label=f"{engine.name} ({engine.description})",
            )
            plotted = True

        if not plotted:
            plt.close(fig)
            continue

        ax.set_xlabel("Workers")
        ax.set_ylabel("Speed-up vs baseline")
        ax.set_title(f"{scenario_name} speed-up by engine")
        ax.set_xticks(sorted(set(workers)))
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.legend()
        fig.tight_layout()

        output_path = graph_dir / f"{scenario_name}_speedup.{fmt}"
        fig.savefig(output_path)
        print(f"Saved speed-up plot to {output_path}")
        if whatsapp_dir is not None:
            export_whatsapp_variant(output_path, whatsapp_dir)
        plt.close(fig)


def export_whatsapp_variant(image_path: Path, target_dir: Path) -> None:
    try:
        from PIL import Image
    except ImportError:
        print(
            "  Pillow not available; skipping WhatsApp-friendly export. "
            "Install Pillow to enable this step."
        )
        return

    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        with Image.open(image_path) as img:
            img = img.convert("RGB")
            width, height = img.size
            max_edge = max(width, height)
            if max_edge > 1080:
                if width >= height:
                    new_width = 1080
                    new_height = max(1, int(height * 1080 / width))
                else:
                    new_height = 1080
                    new_width = max(1, int(width * 1080 / height))
                resample_attr = getattr(Image, "Resampling", None)
                if resample_attr is not None:
                    resample_filter = resample_attr.LANCZOS
                else:
                    resample_filter = Image.LANCZOS
                img = img.resize((new_width, new_height), resample=resample_filter)

            output_path = target_dir / f"{image_path.stem}_whatsapp.jpg"
            img.save(output_path, "JPEG", quality=85, optimize=True, progressive=True)
            print(f"  WhatsApp export saved to {output_path}")
    except Exception as exc:
        print(f"  Failed to create WhatsApp export for {image_path}: {exc}")


if __name__ == "__main__":
    main()
