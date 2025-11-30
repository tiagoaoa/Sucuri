# Sucuri Tutorial (Workspace Edition)

This tutorial distills the core ideas from *"A Minimalistic Dataflow Programming Library for Python"* and the live code you have in this repository.

---

## Why the paper still matters

The SBAC-PADW 2014 paper argued for a minimal set of abstractions that keeps dataflow programs ergonomic in Python while still running efficiently on shared-memory, distributed-memory, and hybrid clusters. Everything you see in `pyDF/` stays faithful to that thesis:

- **Deterministic parallelism.** Workers pull ready tasks from the scheduler, but determinism is preserved through tagging and the `Serializer`.
- **Affinity & MPI awareness.** The `Scheduler` can pin nodes to processes and cooperate with MPI workers (see `Scheduler.mpi_handle` in `pyDF/pydf.py`).
- **Minimal primitives.** `DFGraph`, `Node`, `Feeder`, `Source`, `Serializer`, and `FilterTagged` cover most pipelines.

---

## Core building blocks from the repo

| Concept | Where to read | What to notice |
|---------|---------------|----------------|
| `DFGraph` | `pyDF/pydf.py` | Stores nodes, assigns ids, the scheduler reads from it for every task. |
| `Node` | `pyDF/pydf.py` | Implements `run`, `create_oper`, and affinity. |
| `TaggedValue` | `pyDF/nodes.py` | Ordering primitive for streaming workloads. |
| `Serializer` | `pyDF/nodes.py` | Pins to worker 0 by default to emit ordered results. |
| `Scheduler` | `pyDF/pydf.py` | Launches workers, manages idle queues, integrates with MPI. |

---

## Smallest useful graph

`examples/addition.py` is the literal "hello world" for Sucuri and maps one-to-one with the abstractions:

```python
from pyDF import *

def soma(args):
    a, b = args
    print("Adding %d + %d" % (a, b))
    return a + b

graph = DFGraph()
sched = Scheduler(graph, mpi_enabled=False)
A = Feeder(1)
B = Feeder(2)
adder = Node(soma, 2)

graph.add(A); graph.add(B); graph.add(adder)
A.add_edge(adder, 0)
B.add_edge(adder, 1)
sched.start()
```

Every example follows the same recipe: wire sources/feeders to computational nodes, optionally serialize, then start the scheduler.

---

## Streaming and tags

The `Source` node in `pyDF/nodes.py` emits `TaggedValue` objects so down-stream nodes can match their inputs by tag even when multiple workers produce them out of order. The `Serializer` keeps the sequence intact by inserting into sorted buffers.

The `examples/pipeline.py` script combines a `Source` reading `text.txt` with a `Serializer` that prints each line again, proving that backpressure and tag alignment work without extra glue code.

---

## Scheduler & execution lifecycle

`pyDF/pydf.py` shows that each `Worker` process requests a task by sending an empty `Oper`, executes the bound `Node`, and sends produced operands back through a queue. The master scheduler keeps a pending task counter per worker (to honor affinity) and can delegate tasks via MPI when `mpi_enabled=True`.

```python
# distilled from calc_pi.py
nprocs = int(sys.argv[1])
graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled=False)

reducer = Node(sum_total, nprocs)
graph.add(reducer)

for i in range(nprocs):
    stride_feed = Feeder([stride, i, nprocs])
    worker = Node(psum, 1)
    graph.add(stride_feed); graph.add(worker)
    stride_feed.add_edge(worker, 0)
    worker.add_edge(reducer, i)

sched.start()
```

The `calc_pi` example under `examples/calc_pi.py` doubles as an integration test for affinity and fan-in: each partial sum node feeds a unique reducer port so the scheduler can fire the reduction task when all operands arrive.

---

## Examples doubling as tests

- **`examples/numerical_integration/`**: contains pure PyDF, hybrid, and pure C orchestrations plus `run.sh`/`test.py` harnesses to profile tags under stress.
- **`examples/TSPSucuri`**: demonstrates graph-template reuse for combinatorial search.
- **`examples/videoStreamProcessing`**: dedicates pipelines to decode, filter, and encode frames, which stresses affinity pinning and serialization.
- **`examples/matchtag.py`**: shows how `FilterTagged` matches operands by tag without draining the queues manually.

Because the repo lacks a formal pytest suite, these scripts are the de facto regression tests. Run them often when modifying scheduler logic.

---

## Working plan for a new pipeline

1. Sketch the dataflow graph (use the Graph Lab dock icon).
2. Map each block to `Node`, `Source`, or `Serializer` instances. Keep deterministic ordering by tagging streams that might interleave.
3. Decide worker counts and MPI usage early; call `Scheduler(graph, nprocs, mpi_enabled=True)` only when `mpi4py` is configured on the target environment.
4. Instrument long-running nodes with logging: the `Worker` class prints its id, so you can correlate messages per worker.

---

## Next steps

Use the Graph Lab to draft your next Sucuri pipeline, keep implementation snippets in the per-node editors, and spin up the provided examples for inspiration. This workspace remembers nothing by design—commit your artifacts into `docs/` or `examples/` once the prototype looks good.

---

## Repo navigator

A rapid guide to the assets referenced by the tutorial:

- `pyDF/pydf.py` — Worker, Scheduler, DFGraph, plain Nodes.
- `pyDF/nodes.py` — Higher-level nodes: `Source`, `Feeder`, `FilterTagged`, `Serializer`, `FlipFlop`.
- `examples/pipeline.py` — streaming text example with `Source` and `Serializer`.
- `examples/calc_pi.py` — reduction tree over partial integrals.
- `examples/mine/` & `examples/TSPSucuri/` — show custom schedulers for search/mining workloads.
- `examples/numerical_integration/` — stress test plus SWIG bindings.
- `wservice/` — demonstration of exposing a Sucuri graph over a web interface.
- `docs/sucuri-tutorial/index.html` — the static tutorial that complements this windowed version.

Bookmark these paths while iterating on the graph. Drag this window where you need it and leave it open as a cheat sheet.
