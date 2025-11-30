# Sucuri Tutorial & Graph Builder

*A minimalistic dataflow journey*

Inspired by "A Minimalistic Dataflow Programming Library for Python" and grounded in the concrete examples/tests inside this repository, this tutorial distills how Sucuri composes graphs, schedules work, and allows you to experiment with node-level behaviors.

[Read the SBAC-PADW'14 paper](https://doi.org/10.1109/SBAC-PADW.2014.20)

## Key Highlights

- **DFGraph** - Explicit nodes + edges; see `pyDF/pydf.py`.
- **Schedulers** - Multiprocess + MPI aware worker pool.
- **Operands** - Tagged payloads orchestrate determinism.

---

## Paper-informed primer

The SBAC-PADW paper frames Sucuri as a pragmatic dataflow runtime. These cards summarize the main takeaways as they appear in the codebase.

### Composable operators

The paper stresses that a dataflow graph is fully described by nodes and edges. In pyDF/pydf.py we literally see that `DFGraph.add` only assigns integer ids and stores Node objects—no magic scheduler hints required.

### Deterministic tokens

Tagged operands (see `TaggedValue` in pyDF/nodes.py) echo the paper's focus on determinism: tags become implicit timestamps that the Serializer relies on to re-establish ordering.

### Scheduling scope

Workers talk to the Scheduler through multiprocessing pipes exactly as described in the SBAC-PADW runtime sketch. The optional MPI layer mirrors the multi-node deployment path.

### Minimal API surface

Every example in this repo uses the same trio—`DFGraph`, `Node`/`Source`/`Feeder`, and `Scheduler`. This keeps the learning curve aligned with the minimalist goal of the paper.

---

## Runtime components in practice

Each component below references files from this repository so you can open the source and compare with the explanations from the paper.

### DFGraph

`DFGraph` (pyDF/pydf.py) stores nodes in insertion order and assigns numeric ids. Edges are attached by calling `node.add_edge(dst, dstport)`, which simply caches tuples. The simplicity mirrors the paper's insistence that topology is just metadata.

- Nodes are plain Python callables wrapped by `Node` or its subclasses.
- Affinity isn't part of the graph; it lives on each node via `Node.pin`.
- Graphs stay mutable until you pass them to `Scheduler`.

### Nodes & Sources

`Node`, `Source`, `Feeder`, `FilterTagged`, and `Serializer` (pyDF/nodes.py) bring the paper's operator taxonomy to life. Each overrides `run()` and optionally `match()` to control how operands are consumed.

- `Source` iterates over any iterable and emits `TaggedValue` objects while piggybacking task requests.
- `FilterTagged` buffers operands by tag to synchronize multi-input joins.
- `Serializer` enforces in-order delivery for tagged streams and usually pins to worker 0.

### Scheduler

Located in pyDF/pydf.py, the scheduler coordinates worker processes, handles task queues, and can fan out through MPI. Pending-task counters and optional affinities implement the locality hints mentioned in the paper.

- Workers are subclasses of `multiprocessing.Process` that pull tasks over pipes.
- Each worker emits `Oper` messages containing produced values and implicit task requests.
- MPI support duplicates the worker pool per rank for distributed execution.

### Tagged operands

`TaggedValue` keeps a numeric tag alongside every payload. Examples such as `examples/matchtag.py` and the Serializer implementation demonstrate how tags safeguard determinism and ordering.

- Comparators are implemented so tags can be sorted and matched via `bisect` and dictionaries.
- Nodes may forward `None` or send an empty task request when filtering out data.
- Tagged operands make it trivial to describe iterative algorithms or zipping sources.

---

## Case studies from the repo

Explore real scripts located under `examples/`. Each tab mirrors how the paper's abstractions show up in runnable code.

### Feeders and a binary operator

**Path:** `examples/addition.py`

Two `Feeder` nodes emit constants, and a single `Node(soma, 2)` sums the pair. It's the smallest possible DAG mirroring the paper's figure where tokens move between nodes.

```python
from pyDF import *


def soma(args):
    a, b = args
    print(f"Adding {a} + {b}")
    return a + b


graph = DFGraph()
sched = Scheduler(graph, mpi_enabled=False)

A = Feeder(1)
B = Feeder(2)
C = Node(soma, 2)

graph.add(A)
graph.add(B)
graph.add(C)

A.add_edge(C, 0)
B.add_edge(C, 1)

sched.start()
```

### Streaming text pipeline

**Path:** `examples/pipeline.py`

A `Source` feeds lines from `text.txt` into a pinned `Serializer` node that prints them. This shows how I/O-bound sources coexist with strictly ordered sinks.

```python
import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *


def print_line(args):
    line = args[0]
    print(f"-- {line.rstrip()} --")

nprocs = int(sys.argv[1])

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled=False)
fp = open("text.txt", "r")

src = Source(fp)
printer = Serializer(print_line, 1)
printer.pin(0)

graph.add(src)
graph.add(printer)

src.add_edge(printer, 0)

sched.start()
```

### Tagged joins

**Path:** `examples/matchtag.py`

Five `Source(range(n))` producers emit tagged integers that converge into `FilterTagged`. The node waits for matching tags across all ports before calling `imprime`. It's the tagged-join story from the paper.

```python
import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

def imprime(args):
    print(args)
    return None

nworkers = int(sys.argv[1])
n = int(sys.argv[2])

graph = DFGraph()
sched = Scheduler(graph, nworkers, mpi_enabled=False)

sources = [Source(range(n)) for _ in range(5)]
joiner = FilterTagged(imprime, 5)

for src in sources:
    graph.add(src)
graph.add(joiner)

for port, src in enumerate(sources):
    src.add_edge(joiner, port)

sched.start()
```

---

## Runtime flow checklist

The sequence below traces how Sucuri executes a graph when you call `Scheduler.start()`. It is pieced together from `pyDF/pydf.py`, `pyDF/nodes.py`, and the sample graphs.

1. **Compose the graph** - Instantiate `DFGraph`, add `Node`/`Source`/`Feeder` instances, and wire edges. Optional pinning happens before the scheduler touches the graph.

2. **Start the scheduler** - `Scheduler.start()` (pyDF/pydf.py) launches worker processes and, when MPI is enabled, spawns communication threads to relay tasks across ranks.

3. **Workers request tasks** - Each `Worker` pushes a dummy `Oper` into the shared queue to request work. The scheduler responds by sending a `Task` describing which node to run and what operands to consume.

4. **Nodes execute** - Nodes pull values from their input ports via `match()`. Once `run()` returns a value, `Node.create_oper` fans out `Oper` messages to destination ports.

5. **Operands propagate** - Targets receive operands, append them to their `inport` buffers, and call `match()` again. Tagged nodes such as `FilterTagged` or `Serializer` group by tag before emitting.

6. **Shutdown** - When a terminal node has no destinations it still emits an `Oper` with `dstid=None` to signal the scheduler. Once every worker drains its queue, the scheduler sends termination messages.

---

## Graph builder

Draft a graph that mirrors your pipeline and annotate each node with Python code. The tool keeps the representation human-readable so you can copy-paste it back into a script.

### Controls

- Double-click on the canvas to create nodes.
- Click a node in *Edit* mode to open its code editor.
- Switch to *Connect* to draw directed edges.

---

*Crafted for the Sucuri repository. See `pyDF/pydf.py`, `pyDF/nodes.py`, and the scripts in `examples/` for the source material.*
