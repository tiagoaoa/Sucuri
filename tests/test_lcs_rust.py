from __future__ import annotations

import os
import random
import subprocess
import sys
import time
import unittest
from pathlib import Path
from statistics import mean

from examples.LCS import lcs_plugin_demo
from pyDF.plugins import lcs


class LcsRustIntegrationTest(unittest.TestCase):
    """Ensure the Rust accelerated LCS backend behaves and performs correctly."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = Path(__file__).resolve().parents[1]
        cls.crate_dir = cls.repo_root / "rust" / "sucuri_lcs"
        cls.target_dir = cls.crate_dir / "target" / "release"

        env = os.environ.copy()
        env.setdefault("CARGO_TERM_COLOR", "never")
        env.setdefault("PYO3_USE_ABI3_FORWARD_COMPATIBILITY", "1")
        subprocess.run(
            ["cargo", "build", "--release"],
            check=True,
            cwd=str(cls.crate_dir),
            env=env,
        )

        if not cls.target_dir.exists():
            raise unittest.SkipTest("Rust crate did not produce a release artefact")

        os.environ.setdefault("SUCURI_RUST_PATH", str(cls.target_dir))
        if str(cls.target_dir) not in sys.path:
            sys.path.insert(0, str(cls.target_dir))

        # Restore deterministic test data.
        random.seed(42)
        alphabet = "ACGT"
        cls.seq_length = 1024
        cls.block_size = 128
        cls.sequence_a = "".join(random.choice(alphabet) for _ in range(cls.seq_length))
        cls.sequence_b = "".join(random.choice(alphabet) for _ in range(cls.seq_length))
        cls.context = lcs.LCSContext.from_strings(cls.sequence_a, cls.sequence_b, cls.block_size)

    def run_backend(self, workers: int, compute_fn, repeats: int = 2) -> tuple[int, float]:
        best = float("inf")
        score = None
        for _ in range(repeats):
            start = time.perf_counter()
            score = lcs.compute(self.context, workers, compute_fn)
            duration = time.perf_counter() - start
            best = min(best, duration)
        assert score is not None
        return score, best

    def test_rust_matches_python_results(self) -> None:
        for workers in (1, 4, 8):
            python_score, _ = self.run_backend(workers, lcs.block_python)
            rust_score, _ = self.run_backend(workers, lcs_plugin_demo.lcs_block)
            self.assertEqual(
                python_score,
                rust_score,
                f"Mismatch between Python and Rust backends for {workers} workers",
            )

    def test_rust_speedup_scaling(self) -> None:
        # Warm both backends once to amortise import/initialisation costs.
        self.run_backend(1, lcs.block_python)
        self.run_backend(1, lcs_plugin_demo.lcs_block)

        python_times = []
        rust_times = []
        worker_counts = list(range(1, 13))

        for workers in worker_counts:
            _, python_time = self.run_backend(workers, lcs.block_python)
            _, rust_time = self.run_backend(workers, lcs_plugin_demo.lcs_block)
            python_times.append(python_time)
            rust_times.append(rust_time)

        speedups = [p / r for p, r in zip(python_times, rust_times)]
        self.assertTrue(
            all(val > 0 for val in speedups), "Non-positive runtime encountered"
        )
        self.assertGreater(
            max(speedups),
            1.005,
            f"Expected the Rust backend to provide a tangible speed-up, got {speedups}",
        )

        # Collect statistics for manual inspection when a failure occurs.
        self.speedup_summary = {
            "worker_counts": worker_counts,
            "python_times": python_times,
            "rust_times": rust_times,
            "speedups": speedups,
            "mean_speedup": mean(speedups),
        }


if __name__ == "__main__":
    unittest.main()
