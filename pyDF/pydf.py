#Python Dataflow Library
#Tiago Alves <tiago@ime.uerj.br>

import queue
import threading


DEBUG = False


def _debug(message):
    if DEBUG:
        print(message)


class Worker(threading.Thread):
    def __init__(self, graph, operand_queue, task_queue, workerid):
        super().__init__(name=f"Worker-{workerid}", daemon=True)
        self.operq = operand_queue
        self.graph = graph
        self.wid = workerid
        self.task_queue = task_queue
        self._stop_event = threading.Event()

    def stop(self):
        if not self._stop_event.is_set():
            self._stop_event.set()
            self.task_queue.put(None)

    def run(self):
        _debug(f"I am worker {self.wid}")
        self.operq.put([Oper(self.wid, None, None, None)])  # Request a task to start

        while not self._stop_event.is_set():
            task = self.task_queue.get()
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

        self.nodes += [node]


class Node:
    def __init__(self, f, inputn):
        self.f = f
        self.inport = [[] for _ in range(inputn)]
        self.dsts = []
        self.affinity = None

    def add_edge(self, dst, dstport):
        self.dsts += [(dst.id, dstport)]

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
        if self.dsts == []:
            opers.append(Oper(workerid, None, None, None))
        else:
            for (dstid, dstport) in self.dsts:
                oper = Oper(workerid, dstid, dstport, value)
                opers.append(oper)
        return opers

    def insert_op(self, dstport, oper):
        _debug(f"Received Oper {oper.val}")
        self.inport[dstport].append(oper)

    def match(self):
        args = []
        for port in self.inport:
            if len(port) > 0:
                args += [port[0]]
        if len(args) == len(self.inport):
            for inport in self.inport:
                arg = inport[0]
                inport.remove(arg)

            _debug(f"Received args {args[0].val}")

            return args
        return None


class Oper:
    def __init__(self, prodid, dstid, dstport, val, request_task=True):
        self.wid, self.dstid, self.dstport, self.val = prodid, dstid, dstport, val
        # wid -> id of the worker that produced the oper
        # dstid -> id of the target task
        # dstport -> input port of the target task
        # val -> actual value of the operand

        self.request_task = request_task


class Scheduler:
    TASK_TAG = 0
    TERMINATE_TAG = 1

    def __init__(self, graph, n_workers=1, mpi_enabled=True):
        self.operq = queue.Queue()

        self.graph = graph
        self.tasks = []
        self.conn = []
        self.waiting = []
        self.n_workers = n_workers
        self.pending_tasks = [0] * n_workers
        self.keep_working = threading.Event()
        self.keep_working.set()

        for wid in range(n_workers):
            task_queue = queue.Queue()
            self.conn += [task_queue]
        self.workers = [Worker(self.graph, self.operq, self.conn[i], i) for i in range(n_workers)]

        self.mpi_rank = None
        if mpi_enabled:
            self.mpi_handle()

    def mpi_handle(self):
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        self.mpi_size = comm.Get_size()
        self.mpi_rank = rank
        self.n_slaves = self.mpi_size - 1
        thread_level = MPI.Query_thread()
        if thread_level < MPI.THREAD_SERIALIZED:
            raise RuntimeError(
                "MPI runtime must provide at least MPI_THREAD_SERIALIZED for threaded Sucuri workers"
            )

        if rank == 0:
            _debug(
                f"I am the master. There are {self.mpi_size} mpi processes. (hostname = {MPI.Get_processor_name()})"
            )
            self.pending_tasks = [0] * (self.n_workers * self.mpi_size)
            self.outqueue = queue.Queue()

            def mpi_terminate():
                _debug("MPI TERMINATING")
                for i in range(0, self.mpi_size):
                    comm.send(None, dest=i, tag=Scheduler.TERMINATE_TAG)

            def mpi_input(inqueue):
                while self.keep_working.is_set():
                    msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
                    inqueue.put(msg)

            def mpi_output(outqueue):
                while True:
                    task = outqueue.get()
                    if task is not None:
                        dest = task.workerid // self.n_workers
                        comm.send(task, dest=dest, tag=Scheduler.TASK_TAG)
                    else:
                        self.keep_working.clear()
                        mpi_terminate()
                        break

            t_in = threading.Thread(target=mpi_input, args=(self.operq,), daemon=True)
            t_out = threading.Thread(target=mpi_output, args=(self.outqueue,), daemon=True)
        else:
            _debug(f"I am a slave. (hostname = {MPI.Get_processor_name()})")
            self.inqueue = queue.Queue()
            for worker in self.workers:
                worker.wid += rank * self.n_workers

            self.pending_tasks = [0] * (self.n_workers * self.mpi_size)
            status = MPI.Status()

            def mpi_input(inqueue):
                while True:
                    task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag() == Scheduler.TERMINATE_TAG:
                        self.keep_working.clear()
                        _debug("MPI received termination.")
                        self.terminate_workers(self.workers)
                        break
                    else:
                        workerid = task.workerid
                        connid = workerid % self.n_workers
                        self.conn[connid].put(task)
                self.operq.put(None)

            def mpi_output(outqueue):
                while True:
                    msg = outqueue.get()
                    if msg is not None:
                        comm.send(msg, dest=0, tag=0)
                    else:
                        break

            t_in = threading.Thread(target=mpi_input, args=(self.inqueue,), daemon=True)
            t_out = threading.Thread(target=mpi_output, args=(self.operq,), daemon=True)

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
            node.affinity = node.affinity[1:] + [node.affinity[0]]  # shift the affinity list
        return affinity

    def issue(self, node, args):
        task = Task(node.f, node.id, args)
        self.tasks += [task]

    def _ensure_pending_slot(self, wid):
        if wid >= len(self.pending_tasks):
            self.pending_tasks.extend([0] * (wid + 1 - len(self.pending_tasks)))

    def all_idle(self, workers):
        if self.mpi_rank == 0:
            return len(self.waiting) == self.n_workers * self.mpi_size
        return len(self.waiting) == self.n_workers

    def terminate_workers(self, workers):
        _debug(
            f"Terminating workers {self.all_idle(self.workers)} {self.operq.qsize()} {len(self.tasks)}"
        )
        if self.mpi_rank == 0:
            if hasattr(self, "outqueue"):
                self.outqueue.put(None)
            for t in getattr(self, "threads", []):
                t.join()
        for worker in workers:
            worker.stop()
        for worker in workers:
            worker.join()

    def start(self):
        _debug(f"Roots {[r for r in self.graph.nodes if len(r.inport) == 0]}")
        for root in [r for r in self.graph.nodes if len(r.inport) == 0]:
            task = Task(root.f, root.id)
            self.tasks += [task]


        for worker in self.workers:
            _debug(f"Starting {worker.wid}")
            worker.start()

        if self.mpi_rank == 0 or self.mpi_rank is None:
            # if this is the leader process or if mpi is not being used
            _debug("Main loop")
            self.main_loop()

    def main_loop(self):
        tasks = self.tasks
        operq = self.operq
        workers = self.workers
        while len(tasks) > 0 or not self.all_idle(self.workers) or operq.qsize() > 0:
            opersmsg = operq.get()
            if opersmsg is None:
                continue
            for oper in opersmsg:
                if oper.val is not None:
                    self.propagate_op(oper)

            wid = opersmsg[0].wid
            self._ensure_pending_slot(wid)
            if wid not in self.waiting and opersmsg[0].request_task:
                if self.pending_tasks[wid] > 0:
                    self.pending_tasks[wid] -= 1
                else:
                    self.waiting += [wid]  # indicate that the worker is idle, waiting for a task

            while len(tasks) > 0 and len(self.waiting) > 0:
                task = tasks.pop(0)
                wid = self.check_affinity(task)
                if wid is not None:
                    if wid in self.waiting:
                        self.waiting.remove(wid)
                    else:
                        self._ensure_pending_slot(wid)
                        self.pending_tasks[wid] += 1
                else:
                    wid = self.waiting.pop(0)
                if wid < self.n_workers:  # local worker
                    worker = workers[wid]

                    self.conn[worker.wid].put(task)
                else:
                    task.workerid = wid
                    self.outqueue.put(task)

        _debug(f"Waiting {self.waiting}")
        self.terminate_workers(self.workers)
