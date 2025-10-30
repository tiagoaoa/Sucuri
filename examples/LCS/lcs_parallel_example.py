"""CLI helper to execute the LCS dataflow using either backend."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from examples.LCS.lcs_parallel import (
    LCSContext,
    compute_lcs,
    lcs_block_python,
    lcs_block_rust,
    read_sequence,
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("seq_a", type=Path, help="Path to the first sequence file")
    parser.add_argument("seq_b", type=Path, help="Path to the second sequence file")
    parser.add_argument("--block", type=int, default=128, help="Square block size to use")
    parser.add_argument("--workers", type=int, default=4, help="Number of scheduler workers")
    parser.add_argument(
        "--backend",
        choices=["python", "rust"],
        default="rust",
        help="Select the accelerated Rust backend or the pure Python implementation",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    seq_a = read_sequence(args.seq_a)
    seq_b = read_sequence(args.seq_b)
    context = LCSContext(seq_a=seq_a, seq_b=seq_b, block=args.block)

    compute_fn = lcs_block_rust if args.backend == "rust" else lcs_block_python
    score = compute_lcs(context, args.workers, compute_fn)
    print(f"Backend: {args.backend}, score: {score}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
