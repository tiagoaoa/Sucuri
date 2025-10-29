# Python Dataflow Library (multiprocessing scheduler)
# Tiago Alves <tiago@ime.uerj.br>

import multiprocessing as mp
import threading

try:
    _CTX = mp.get_context("fork")
except ValueError:
    _CTX = mp.get_context()


DEBUG = False


def _debug(message):
    if DEBUG:
        print(message)


class Worker(_CTX.Process):
    """Process-based worker used by the legacy multiprocessing scheduler."""

    def __init__(self, graph, operand_queue, conn, workerid):
        super().__init__(name=f"Worker-{workerid}")
        self.graph = graph
        self.operq = operand_queue
        self.conn = conn
        self.wid = workerid

    def run(self):
        _debug(f"I am worker {self.wid}")
        # Request a first task
        self.operq.put([Oper(self.wid, None, None, None)])

        while True:
            task = self.conn.recv()
            if task is None:
                break

            node = self.graph.nodes[task.nodeid]
            node.run(task.args, self.wid, self.operq)


class Task:
    def __init__(self, f, nodeid, args=None):
        self.nodeid = nodeid
        self.args = args


class DFGraph:
    def __init__(self):
        self.nodes = []
        self.node_count = 0

    def add(self, node):
        node.id = self.node_count
        self.node_count += 1
        self.nodes.append(node)


class Node:
    def __init__(self, f, inputn):
        self.f = f
        self.inport = [[] for _ in range(inputn)]
        self.dsts = []
        self.affinity = None

    def add_edge(self, dst, dstport):
        self.dsts.append((dst.id, dstport))

    def pin(self, workerid):
        self.affinity = workerid

    def run(self, args, workerid, operq):
        if len(self.inport) == 0:
            opers = self.create_oper(self.f(), workerid, operq)
        else:
            opers = self.create_oper(self.f([a.val for a in args]), workerid, operq)
        self.sendops(opers, operq)

    def sendops(self, opers, operq):
        operq.put(opers)

    def create_oper(self, value, workerid, operq):
        opers = []
        if not self.dsts:
            opers.append(Oper(workerid, None, None, None))
        else:
            for (dstid, dstport) in self.dsts:
                opers.append(Oper(workerid, dstid, dstport, value))
        return opers

    def insert_op(self, dstport, oper):
        _debug(f"Received Oper {oper.val}")
        self.inport[dstport].append(oper)

    def match(self):
        args = []
        for port in self.inport:
            if port:
                args.append(port[0])
        if len(args) == len(self.inport):
            for inport in self.inport:
                arg = inport[0]
                inport.remove(arg)
            _debug(f"Received args {args[0].val}")
            return args
        return None


class Oper:
    def __init__(self, prodid, dstid, dstport, val):
        self.wid = prodid
        self.dstid = dstid
        self.dstport = dstport
        self.val = val
        self.request_task = True


class Scheduler:
    TASK_TAG = 0
    TERMINATE_TAG = 1

    def __init__(self, graph, n_workers=1, mpi_enabled=True):
        self.operq = _CTX.Queue()
        self.graph = graph
        self.tasks = []
        self.conn = []
        self.waiting = []
        self.n_workers = n_workers
        self.pending_tasks = [0] * n_workers
        self.keep_working = True

        worker_conns = []
        for _ in range(n_workers):
            sched_conn, worker_conn = _CTX.Pipe()
            worker_conns.append(worker_conn)
            self.conn.append(sched_conn)

        self.workers = [
            Worker(self.graph, self.operq, worker_conns[i], i) for i in range(n_workers)
        ]

        if mpi_enabled:
            self.mpi_handle()
        else:
            self.mpi_rank = None

    def mpi_handle(self):
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        self.mpi_size = comm.Get_size()
        self.mpi_rank = rank
        self.n_slaves = self.mpi_size - 1
        self.keep_working = True

        if rank == 0:
            _debug(
                f"I am the master. There are {self.mpi_size} mpi processes. "
                f"(hostname = {MPI.Get_processor_name()})"
            )
            self.pending_tasks = [0] * self.n_workers * self.mpi_size
            self.outqueue = _CTX.Queue()

            def mpi_input(inqueue):
                while self.keep_working:
                    msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
                    inqueue.put(msg)

            def mpi_output(outqueue):
                while self.keep_working:
                    task = outqueue.get()
                    if task is not None:
                        dest = task.workerid // self.n_workers
                        comm.send(task, dest=dest, tag=Scheduler.TASK_TAG)
                    else:
                        self.keep_working = False
                        mpi_terminate()

            def mpi_terminate():
                _debug("MPI TERMINATING")
                for i in range(0, self.mpi_size):
                    comm.send(None, dest=i, tag=Scheduler.TERMINATE_TAG)

            t_in = threading.Thread(target=mpi_input, args=(self.operq,))
            t_out = threading.Thread(target=mpi_output, args=(self.outqueue,))
        else:
            _debug(f"I am a slave. (hostname = {MPI.Get_processor_name()})")
            self.inqueue = _CTX.Queue()
            for worker in self.workers:
                worker.wid += rank * self.n_workers

            status = MPI.Status()

            def mpi_input(inqueue):
                while self.keep_working:
                    task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag() == Scheduler.TERMINATE_TAG:
                        self.keep_working = False
                        _debug("MPI received termination.")
                        self.terminate_workers(self.workers)
                    else:
                        workerid = task.workerid
                        connid = workerid % self.n_workers
                        self.conn[connid].send(task)
                self.operq.put(None)

            def mpi_output(outqueue):
                while self.keep_working:
                    msg = outqueue.get()
                    if msg is not None:
                        comm.send(msg, dest=0, tag=0)

            t_in = threading.Thread(target=mpi_input, args=(self.inqueue,))
            t_out = threading.Thread(target=mpi_output, args=(self.operq,))

        threads = [t_in, t_out]
        self.threads = threads
        for t in threads:
            t.start()

    def propagate_op(self, oper):
        dst = self.graph.nodes[oper.dstid]
        dst.insert_op(oper.dstport, oper)
        args = dst.match()
        if args is not None:
            self.issue(dst, args)

    def check_affinity(self, task):
        node = self.graph.nodes[task.nodeid]
        if node.affinity is None:
            return None

        affinity = node.affinity[0]
        if len(node.affinity) > 1:
            node.affinity = node.affinity[1:] + [node.affinity[0]]
        return affinity

    def issue(self, node, args):
        task = Task(node.f, node.id, args)
        self.tasks.append(task)

    def all_idle(self, workers):
        if self.mpi_rank == 0:
            return len(self.waiting) == self.n_workers * self.mpi_size
        return len(self.waiting) == self.n_workers

    def terminate_workers(self, workers):
        _debug(
            f"Terminating workers {self.all_idle(self.workers)} "
            f"{self.operq.qsize()} {len(self.tasks)}"
        )
        if getattr(self, "mpi_rank", None) == 0:
            self.outqueue.put(None)
            for t in self.threads:
                t.join()
        for conn in self.conn:
            conn.send(None)
        for worker in workers:
            worker.join()

    def start(self):
        _debug(f"Roots {[r for r in self.graph.nodes if len(r.inport) == 0]}")
        for root in [r for r in self.graph.nodes if len(r.inport) == 0]:
            task = Task(root.f, root.id)
            self.tasks.append(task)

        for worker in self.workers:
            _debug(f"Starting {worker.wid}")
            worker.start()

        if self.mpi_rank == 0 or self.mpi_rank is None:
            _debug("Main loop")
            self.main_loop()

    def main_loop(self):
        tasks = self.tasks
        operq = self.operq
        workers = self.workers
        while len(tasks) > 0 or not self.all_idle(workers) or operq.qsize() > 0:
            opersmsg = operq.get()
            for oper in opersmsg:
                if oper.val is not None:
                    self.propagate_op(oper)

            wid = opersmsg[0].wid
            if wid not in self.waiting and opersmsg[0].request_task:
                if self.pending_tasks[wid] > 0:
                    self.pending_tasks[wid] -= 1
                else:
                    self.waiting.append(wid)

            while len(tasks) > 0 and len(self.waiting) > 0:
                task = tasks.pop(0)
                wid = self.check_affinity(task)
                if wid is not None:
                    if wid in self.waiting:
                        self.waiting.remove(wid)
                    else:
                        self.pending_tasks[wid] += 1
                else:
                    wid = self.waiting.pop(0)

                if wid < self.n_workers:
                    worker = workers[wid]
                    self.conn[worker.wid].send(task)
                else:
                    task.workerid = wid
                    self.outqueue.put(task)

        _debug(f"Waiting {self.waiting}")
        self.terminate_workers(workers)
