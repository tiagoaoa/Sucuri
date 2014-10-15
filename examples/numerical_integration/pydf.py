#Python Dataflow Library
#Tiago A.O.A. <tiagoaoa@cos.ufrj.br>

from multiprocessing import Process, Queue, Value, Pipe


class Worker(Process):
	def __init__(self, graph, operand_queue, conn, workerid):
		Process.__init__(self)		#since we are overriding the superclass's init method
		#self.taskq = task_queue
		self.operq = operand_queue
		self.idle = False
		self.graph = graph
		self.wid = workerid
		self.conn = conn #connection with the scheduler to receive tasks

	#def sendops(self, opers):
		#print "%s sending oper" %self.name
	#	self.operq.put(opers)	
	
	def isidle(self):
		return self.idle



	def run(self):
		print "I am worker %s" %self.name
		self.operq.put([Oper(self.wid, None, None, None)]) #Request a task to start

		while True:
			#print "Waiting for task %s" %self.name
			task = self.conn.recv()

			#print "Start working %s" %(self.name)
			node = self.graph.nodes[task.nodeid]
			opermsg = node.run(task.args, self.wid, self.operq)
			#self.sendops(opermsg)
			
		



class Task:

	def __init__(self, f, nodeid, args=None):
		self.nodeid = nodeid
		self.args = args

	


class DFGraph:
	def __init__(self):
		self.nodes = {}

	def add(self, node):
		self.nodes[id(node)] = node
	
class Node:
	def __init__(self, f, inputn):
		self.f = f
		self.inport = [[] for i in range(inputn)]
		self.dsts = []


	def add_edge(self, dst, dstport):
		self.dsts += [(id(dst), dstport)]


	def run(self, args, workerid, operq):
		#print "Running %s" %self
		if len(self.inport) == 0:
			opers = self.create_oper(self.f(), workerid, operq)
		else:
			opers = self.create_oper(self.f(args), workerid, operq)
		self.sendops(opers, operq)


	def sendops(self, opers, operq):
		operq.put(opers)

	def create_oper(self, value, workerid, operq, tag = 0): #create operand message
		opers = []
		if self.dsts == []:
			opers.append(Oper(workerid, None, None, None)) #if no output is produced by the node, we still have to send a msg to the scheduler.
		else:
			for (dstid, dstport) in self.dsts:
				oper = Oper(workerid, dstid, dstport, value)
				oper.tag = tag
				opers.append(oper)
				print "Result produced %s" %oper.val
		return opers





class Oper:
	def __init__(self, prodid, dstid, dstport, val):
		self.wid, self.dstid, self.dstport, self.val = prodid, dstid, dstport, val
		#wid -> id of the worker that produced the oper
		#dstid -> id of the target task
		#dstport -> input port of the target task
		#val -> actual value of the operand

		self.tag = 0 #default tag
		self.request_task = True #if true, piggybacks a request for a task to the worker where the opers were produced.


class Scheduler:
	def __init__(self, graph, n=1):
		#self.taskq = Queue()  #queue where the ready tasks are inserted
		self.operq = Queue()

		self.graph = graph
		self.tasks = []
		worker_conns = []
		self.conn = []
		self.waiting = [] #queue containing idle workers
		for i in range(n):
			sched_conn, worker_conn = Pipe()
			worker_conns += [worker_conn]
			self.conn += [sched_conn]
		self.workers = [Worker(self.graph, self.operq, worker_conns[i], i) for i in range(n)]
			

			#for worker in self.workers:
			#	worker.start()


	def isready(self, node):
		return reduce(lambda a, b: a and b, [len(port) > 0 for port in node.inport])



	def propagate_op(self, oper):
		dst = self.graph.nodes[oper.dstid]
		dst.inport[oper.dstport] += [oper.val]

		if self.isready(dst):
			self.issue(dst)




	def issue(self, node):
		args = [port.pop() for port in node.inport]
		task = Task(node.f, id(node), args)
		self.tasks += [task]

	def all_idle(self, workers):
		#print [(w.idle, w.name) for w in workers]	
		#print "All idle? %s" %reduce(lambda a, b: a and b, [w.idle for w in workers])
		return len(self.waiting) == len(self.workers)


	def terminate_workers(self, workers):
		print "Terminating workers %s %d %d" %(self.all_idle(self.workers), self.operq.qsize(), len(self.tasks))
		for worker in workers:
			worker.terminate()
	def start(self):
		operq = self.operq
		#taskq = self.taskq
		#print [r for r in self.graph.nodes.values()]

		#print "Roots %s" %[r for r in self.graph.nodes.values() if len(r.inport) == 0]
		for root in [r for r in self.graph.nodes.values() if len(r.inport) == 0]:
			task = Task(root.f, id(root))
			self.tasks += [task]
			
		print self.workers
	
		for worker in self.workers:
			print "Starting %s" %worker.name
			worker.start()

		self.main_loop()

	def main_loop(self):
		tasks = self.tasks
		operq = self.operq
		workers = self.workers
		while len(tasks) > 0 or not self.all_idle(self.workers) or operq.qsize() > 0:
			#print "idle? %d sizes %d %d" %(self.all_idle(self.workers), operq.qsize(), len(tasks))
			opersmsg = operq.get()
			for oper in opersmsg:
				if oper.val != None:
					self.propagate_op(oper)

			wid = opersmsg[0].wid
			#print "Oper came from %s" %self.workers[wid].name
			if workers[wid] not in self.waiting and opersmsg[0].request_task: 
				self.waiting += [wid] #indicate that the worker is idle, waiting for a task




			while len(tasks) > 0 and len(self.waiting) > 0:
				task = tasks.pop(0)
				worker = workers[self.waiting.pop(0)]
				print "Sending %s to %s" %(task.nodeid, worker.name)
				self.conn[worker.wid].send(task)
				

			
		self.terminate_workers(self.workers)
		
		
		
