"""LCS utilities and Rust integration plugin."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from pyDF import DFGraph, Node, Oper, Scheduler
from pyDF.integrations.rust import RustConfig, RustPlugin, register_plugin


@dataclass(frozen=True)
class LCSContext:
    """Immutable configuration shared by all blocks in the grid."""

    seq_a: bytes
    seq_b: bytes
    block: int

    @classmethod
    def from_strings(cls, seq_a: str, seq_b: str, block: int) -> "LCSContext":
        return cls(_ensure_bytes(seq_a), _ensure_bytes(seq_b), block)

    @classmethod
    def from_bytes(cls, seq_a: bytes, seq_b: bytes, block: int) -> "LCSContext":
        return cls(seq_a, seq_b, block)

    @property
    def size_a(self) -> int:
        return len(self.seq_a)

    @property
    def size_b(self) -> int:
        return len(self.seq_b)

    @property
    def grid_width(self) -> int:
        if self.size_a == 0:
            return 1
        return (self.size_a + self.block - 1) // self.block

    @property
    def grid_height(self) -> int:
        if self.size_b == 0:
            return 1
        return (self.size_b + self.block - 1) // self.block

    def bounds_a(self, j: int) -> Tuple[int, int]:
        start = j * self.block
        end = min(start + self.block, self.size_a)
        return start, end

    def bounds_b(self, i: int) -> Tuple[int, int]:
        start = i * self.block
        end = min(start + self.block, self.size_b)
        return start, end


def _ensure_bytes(data: Iterable[str] | bytes | str) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.rstrip("\n").encode("utf-8")
    return "".join(data).encode("utf-8")


def read_sequence(path: Path) -> bytes:
    return path.read_text().rstrip("\n").encode("utf-8")


def copy_vector(vec: Optional[Iterable[int]]) -> Optional[List[int]]:
    if vec is None:
        return None
    return list(vec)


def unpack_inputs(i: int, j: int, inputs: List[List[int]]) -> Tuple[Optional[List[int]], Optional[List[int]]]:
    if i == 0 and j == 0:
        return None, None
    if i == 0:
        return None, copy_vector(inputs[0])
    if j == 0:
        return copy_vector(inputs[0]), None
    return copy_vector(inputs[0]), copy_vector(inputs[1])


class LCSBlock(Node):
    """Specialised node that routes horizontal/vertical block borders."""

    def __init__(
        self,
        context: LCSContext,
        compute_fn,
        i: int,
        j: int,
        input_count: int,
    ) -> None:
        super().__init__(compute_fn, input_count)
        self.context = context
        self.compute_fn = compute_fn
        self.i = i
        self.j = j
        self.dsts: List[Tuple[int, int, int]] = []

    def add_edge(self, dst: Node, dstport: int, srcport: int = 0) -> None:
        self.dsts.append((dst.id, dstport, srcport))

    def run(self, args, workerid, operq) -> None:
        if len(self.inport) == 0:
            inputs: List[List[int]] = []
        else:
            inputs = [operand.val for operand in args] if args else []

        borders = self.compute_fn(self.context, self.i, self.j, inputs)
        opers = self.create_oper(borders, workerid, operq)
        self.sendops(opers, operq)

    def create_oper(self, value, workerid, operq):
        opers = []
        if not self.dsts:
            opers.append(Oper(workerid, None, None, None))
        else:
            for (dstid, dstport, srcport) in self.dsts:
                opers.append(Oper(workerid, dstid, dstport, value[srcport]))
        return opers


class ResultNode(Node):
    """Terminal node that captures the final DP row produced by the grid."""

    def __init__(self) -> None:
        super().__init__(self._store, 1)
        self.result: Optional[List[int]] = None

    def _store(self, values: List[List[int]]) -> None:
        self.result = copy_vector(values[0])
        return None


def _input_count(i: int, j: int) -> int:
    if i == 0 and j == 0:
        return 0
    if i == 0 or j == 0:
        return 1
    return 2


def build_graph(
    context: LCSContext,
    compute_fn,
) -> Tuple[DFGraph, ResultNode]:
    graph = DFGraph()
    g_h, g_w = context.grid_height, context.grid_width

    blocks: List[List[LCSBlock]] = []
    for i in range(g_h):
        row = []
        for j in range(g_w):
            node = LCSBlock(context, compute_fn, i, j, _input_count(i, j))
            graph.add(node)
            row.append(node)
        blocks.append(row)

    for i in range(g_h):
        for j in range(g_w):
            if i > 0:
                blocks[i - 1][j].add_edge(blocks[i][j], 0, 0)
            if j > 0:
                dstport = 1 if i > 0 else 0
                blocks[i][j - 1].add_edge(blocks[i][j], dstport, 1)

    sink = ResultNode()
    graph.add(sink)
    blocks[-1][-1].add_edge(sink, 0, 0)
    return graph, sink


def compute(context: LCSContext, workers: int, compute_fn) -> int:
    graph, sink = build_graph(context, compute_fn)
    scheduler = Scheduler(graph, n_workers=workers, mpi_enabled=False)
    scheduler.start()
    if sink.result is None:
        raise RuntimeError("LCS computation finished without producing a result")
    return sink.result[-1]


def block_python(
    context: LCSContext,
    i: int,
    j: int,
    inputs: List[List[int]],
) -> Tuple[List[int], List[int]]:
    north, west = unpack_inputs(i, j, inputs)
    start_a, end_a = context.bounds_a(j)
    start_b, end_b = context.bounds_b(i)
    width = end_a - start_a
    height = end_b - start_b

    matrix = [[0] * (width + 1) for _ in range(height + 1)]

    if north is not None:
        if len(north) != width + 1:
            raise ValueError("north border has invalid length")
        matrix[0] = copy_vector(north)
    if west is not None:
        if len(west) != height + 1:
            raise ValueError("west border has invalid length")
        for idx, value in enumerate(west):
            matrix[idx][0] = value

    seq_a = context.seq_a
    seq_b = context.seq_b

    for row in range(1, height + 1):
        ch_b = seq_b[start_b + row - 1]
        for col in range(1, width + 1):
            ch_a = seq_a[start_a + col - 1]
            if ch_a == ch_b:
                matrix[row][col] = matrix[row - 1][col - 1] + 1
            else:
                matrix[row][col] = max(matrix[row][col - 1], matrix[row - 1][col])

    bottom_row = matrix[-1]
    right_column = [matrix[row][width] for row in range(height + 1)]
    return bottom_row, right_column


def _rust_arg_adapter(
    context: LCSContext,
    i: int,
    j: int,
    inputs: List[List[int]],
) -> Tuple[Tuple, dict]:
    north, west = unpack_inputs(i, j, inputs)
    start_a, end_a = context.bounds_a(j)
    start_b, end_b = context.bounds_b(i)
    return (
        (
            context.seq_a,
            context.seq_b,
            start_a,
            end_a,
            start_b,
            end_b,
            north,
            west,
        ),
        {},
    )


def _rust_search_path() -> Optional[Path]:
    repo_root = Path(__file__).resolve().parents[2]
    release = repo_root / "rust" / "sucuri_lcs" / "target" / "release"
    if release.exists():
        return release
    debug = release.parent / "debug"
    if debug.exists():
        return debug
    return None


class _LCSRustPlugin(RustPlugin):
    """Configure @rust automatically for the minimal LCS example."""

    def configure(self, python_impl):
        if python_impl.__module__ == "examples.LCS.lcs_plugin_demo" and python_impl.__name__ == "lcs_block":
            return RustConfig(
                module="sucuri_lcs",
                func="lcs_block",
                paths=(_rust_search_path,),
                arg_adapter=_rust_arg_adapter,
            )
        return None


register_plugin(_LCSRustPlugin())


def cli_main(block_fn, argv: Optional[List[str]] = None) -> int:
    """Entry point for the minimal example."""

    parser = argparse.ArgumentParser(description="LCS dataflow benchmark with optional Rust acceleration.")
    parser.add_argument("seq_a", type=Path, help="Path to the first sequence")
    parser.add_argument("seq_b", type=Path, help="Path to the second sequence")
    parser.add_argument("--block", type=int, default=256, help="Block size for the DP matrix decomposition")
    parser.add_argument("--workers", type=int, default=4, help="Number of scheduler workers")
    args = parser.parse_args(argv)

    seq_a = read_sequence(args.seq_a)
    seq_b = read_sequence(args.seq_b)
    context = LCSContext.from_bytes(seq_a, seq_b, args.block)
    score = compute(context, args.workers, block_fn)
    print(f"Score = {score}")
    return 0


__all__ = [
    "LCSContext",
    "copy_vector",
    "unpack_inputs",
    "block_python",
    "build_graph",
    "cli_main",
    "compute",
    "read_sequence",
]
