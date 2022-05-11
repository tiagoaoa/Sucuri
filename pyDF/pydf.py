# Python Dataflow Library
# Tiago A.O.Alfes <tiago@ime.uerj.br>

from multiprocessing import Process, Queue, Value, Pipe
import threading


class Worker(Process):
	"""
	Worker, an abstraction for a processing node.
	Usually one-to-one for the number of processors.
	"""
	def __init__(self, graph, operand_queue, conn, workerid):
		# since we are overriding the superclass's init method
		super(Worker, self).__init__()
		# self.taskq = task_queue
		self.operq = operand_queue
		self.idle = False
		self.graph = graph
		self.wid = workerid
		self.conn = conn  # connection with the scheduler to receive tasks

	# def sendops(self, opers):
	# print "%s sending oper" %self.name
	#	self.operq.put(opers)

	def run(self):
		print "I am worker %s" % self.wid
		self.operq.put([Oper(self.wid, None, None, None)])  # Request a task to start

		while True:
			# print "Waiting for task %s" %self.name
			task = self.conn.recv()

			# print "Start working %s" %(self.name)
			node = self.graph.nodes[task.nodeid]
			node.run(task.args, self.wid, self.operq)
		# self.sendops(opermsg)


class Task(object):
	def __init__(self, f, nodeid, args=None):
		self.nodeid = nodeid
		self.args = args


class DFGraph(object):
	"""
	Dataflow graph.
	"""
	def __init__(self):
		self.nodes = []
		self.node_count = 0

	def add(self, node):
		"""
		Adds a node to the dataflow graph.
		Needs to be called to every node used before building the dependency graph.
		:param node: Node
			Node to be added to the graph.
		"""
		node.id = self.node_count
		self.node_count += 1

		self.nodes += [node]


class Node(object):
	"""
	Graph node
	"""
	def __init__(self, f, inputn):
		self.f = f
		self.inport = [[] for i in range(inputn)]
		self.dsts = []
		self.affinity = None

	def add_edge(self, dst, dstport):
		"""
		Adds an edge to the node
		:param dst: Node
			The destination node.
		:param dstport: int
			The destination port on dst node.
		"""
		self.dsts += [(dst.id, dstport)]

	def pin(self, workerid):
		"""
		Attachs this node to a specific worker.
		:param workerid: int
			Worker id to be attached for.
		"""
		self.affinity = workerid

	def run(self, args, workerid, operq):
		"""
		Calls the node operation.
		:param args: List
			Expected operands.
		:param workerid: int
			Worker id running the node.
		:param operq:
			Operation queue.
		"""
		# print "Running %s" %self
		if len(self.inport) == 0:
			opers = self.create_oper(self.f(), workerid, operq)
		else:
			opers = self.create_oper(self.f([a.val for a in args]), workerid, operq)
		self.sendops(opers, operq)

	def sendops(self, opers, operq):
		operq.put(opers)

	def create_oper(self, value, workerid, operq):
		"""
		Create operand message
		:param value:
			Message content.
		:param workerid:
			Worker id running the node.
		:param operq:
			Operation queue.
		"""
		opers = []
		if not self.dsts:
			# if no output is produced by the node, we still have to send a msg to the scheduler.
			opers.append(Oper(workerid, None, None, None))
		else:
			for (dstid, dstport) in self.dsts:
				oper = Oper(workerid, dstid, dstport, value)
				opers.append(oper)
			# print "Result produced %s (worker: %d)" %(oper.val, workerid)
		return opers

	def match(self):
		args = []
		for port in self.inport:
			if len(port) > 0:
				arg = port[0]
				args += [port[0]]
		if len(args) == len(self.inport):
			for inport in self.inport:
				arg = inport[0]
				inport.remove(arg)
			return args
		else:
			return None


class Oper(object):
	"""
	Operand.
	"""
	def __init__(self, prodid, dstid, dstport, val):
		"""
		Operand
		:param prodid:
			Id of the worker that produced the oper
		:param dstid:
			Id of the target task
		:param dstport:
			Input port of the target task
		:param val:
			actual value of the operand
		"""
		self.wid, self.dstid, self.dstport, self.val = prodid, dstid, dstport, val
		self.request_task = True #if true, piggybacks a request for a task to the worker where the opers were produced.


class Scheduler(object):
	"""
	Sucuri scheduler.
	"""
	TASK_TAG = 0
	TERMINATE_TAG = 1

	def __init__(self, graph, n_workers=1, mpi_enabled=True):
		"""
		:param graph: DFGraph
			The dataflow graph.
		:param n_workers: int
			Number of workers used.
		:param mpi_enabled:
			Indicates if uses MPI or not.
		"""
		# self.taskq = Queue()  #queue where the ready tasks are inserted
		self.operq = Queue()

		self.graph = graph
		self.tasks = []
		worker_conns = []
		self.conn = []
		self.waiting = []  # queue containing idle workers
		self.n_workers = n_workers  # number of workers
		# keeps track of the number of tasks sent to each worker without a request from the worker (due to affinity)
		self.pending_tasks = [0] * n_workers
		for i in range(n_workers):
			sched_conn, worker_conn = Pipe()
			worker_conns += [worker_conn]
			self.conn += [sched_conn]
		self.workers = [Worker(self.graph, self.operq, worker_conns[i], i) for i in range(n_workers)]

		if mpi_enabled:
			self.mpi_handle()
		else:
			self.mpi_rank = None

	def mpi_handle(self):
		"""
		MPI implementation for the dataflow.
		"""
		from mpi4py import MPI
		comm = MPI.COMM_WORLD
		rank = comm.Get_rank()
		self.mpi_size = comm.Get_size()
		self.mpi_rank = rank
		self.n_slaves = self.mpi_size - 1
		self.keep_working = True

		if rank == 0:
			print "I am the master. There are %s mpi processes. (hostname = %s)" % (self.mpi_size, MPI.Get_processor_name())
			self.pending_tasks = [0] * self.n_workers * self.mpi_size
			self.outqueue = Queue()

			def mpi_input(inqueue):
				while self.keep_working:
					msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
					# print "MPI Received opermsg from slave."
					inqueue.put(msg)

			def mpi_output(outqueue):
				while self.keep_working:
					task = outqueue.get()
					if task is not None:  # task == None means termination
						# print "MPI Sending task to slave node."
						dest = task.workerid / self.n_workers  # destination mpi process
						comm.send(task, dest=dest, tag=Scheduler.TASK_TAG)
					else:
						self.keep_working = False
						mpi_terminate()

			def mpi_terminate():
				print "MPI TERMINATING"
				for i in xrange(0, self.mpi_size):
					comm.send(None, dest=i, tag=Scheduler.TERMINATE_TAG)

			t_in = threading.Thread(target=mpi_input, args=(self.operq,))
			t_out = threading.Thread(target=mpi_output, args=(self.outqueue,))
		else:
			print "I am a slave. (hostname = %s)" % MPI.Get_processor_name()
			# slave
			self.inqueue = Queue()
			for worker in self.workers:
				worker.wid += rank * self.n_workers

			status = MPI.Status()

			def mpi_input(inqueue):
				while self.keep_working:
					task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
					if status.Get_tag() == Scheduler.TERMINATE_TAG:
						self.keep_working = False
						print "MPI received termination."
						self.terminate_workers(self.workers)
					else:
						# print "MPI Sending task to worker in slave."
						workerid = task.workerid
						connid = workerid % self.n_workers
						self.conn[connid].send(task)
				self.operq.put(None)

			def mpi_output(outqueue):
				while self.keep_working:
					msg = outqueue.get()
					if msg is not None:
						# print "MPI send opermsg to master."
						comm.send(msg, dest=0, tag=0)

			t_in = threading.Thread(target=mpi_input, args=(self.inqueue,))
			t_out = threading.Thread(target=mpi_output, args=(self.operq,))

		threads = [t_in, t_out]
		self.threads = threads
		for t in threads:
			t.start()

	def propagate_op(self, oper):
		dst = self.graph.nodes[oper.dstid]

		dst.inport[oper.dstport] += [oper]
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
		#	print "Args %s " %args
		task = Task(node.f, node.id, args)
		self.tasks += [task]

	def all_idle(self, workers):
		# print [(w.idle, w.name) for w in workers]
		# print "All idle? %s" %reduce(lambda a, b: a and b, [w.idle for w in workers])
		if self.mpi_rank == 0:
			return len(self.waiting) == self.n_workers * self.mpi_size
		else:
			return len(self.waiting) == self.n_workers

	def terminate_workers(self, workers):
		print "Terminating workers %s %d %d" % (self.all_idle(self.workers), self.operq.qsize(), len(self.tasks))
		if self.mpi_rank == 0:
			self.outqueue.put(None)
			for t in self.threads:
				t.join()
		for worker in workers:
			worker.terminate()

	def start(self):
		"""
		Starts the processing dataflow environment.
		"""
		operq = self.operq

		print "Roots %s" % [r for r in self.graph.nodes if len(r.inport) == 0]
		for root in [r for r in self.graph.nodes if len(r.inport) == 0]:
			task = Task(root.f, root.id)
			self.tasks += [task]

		for worker in self.workers:
			print "Starting %s" % worker.wid
			worker.start()

		if self.mpi_rank == 0 or self.mpi_rank is None:
			# it this is the leader process or if mpi is not being used
			print "Main loop"
			self.main_loop()

	def main_loop(self):
		tasks = self.tasks
		operq = self.operq
		workers = self.workers
		while len(tasks) > 0 or not self.all_idle(self.workers) or operq.qsize() > 0:
			opersmsg = operq.get()
			for oper in opersmsg:
				if oper.val is not None:
					self.propagate_op(oper)

			wid = opersmsg[0].wid
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
						self.pending_tasks[wid] += 1
				else:
					wid = self.waiting.pop(0)
				# print "Got opermsg from worker %d" %wid
				if wid < self.n_workers:  # local worker
					worker = workers[wid]

					self.conn[worker.wid].send(task)
				else:
					task.workerid = wid
					self.outqueue.put(task)

		print "Waiting %s" % self.waiting
		self.terminate_workers(self.workers)
