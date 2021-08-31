#Python Dataflow Library
#Tiago Alves <tiago@ime.uerj.br>

from multiprocessing import Process, Queue, Value, Pipe
import threading

import sys

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




    def run(self):
        print("I am worker {}".format(self.wid))
        self.operq.put([Oper(self.wid, None, None, None)]) #Request a task to start

        while True:
            #print "Waiting for task %s" %self.name
            task = self.conn.recv()

            #print "Start working %s" %(self.name)
            node = self.graph.nodes[task.nodeid]
            node.run(task.args, self.wid, self.operq)
            #self.sendops(opermsg)





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
        self.inport = [[] for i in range(inputn)]
        self.dsts = []
        self.affinity = None


    def add_edge(self, dst, dstport):
        self.dsts += [(dst.id, dstport)]


    def pin(self, workerid):
        self.affinity = workerid


    def run(self, args, workerid, operq):
        #print "Running %s" %self
        if len(self.inport) == 0:
            opers = self.create_oper(self.f(), workerid, operq)
        else:
            opers = self.create_oper(self.f([a.val for a in args]), workerid, operq)
        self.sendops(opers, operq)


    def sendops(self, opers, operq):
        operq.put(opers)

    def create_oper(self, value, workerid, operq): #create operand message
        opers = []
        if self.dsts == []:
            opers.append(Oper(workerid, None, None, None)) #if no output is produced by the node, we still have to send a msg to the scheduler.
        else:
            for (dstid, dstport) in self.dsts:
                oper = Oper(workerid, dstid, dstport, value)
                opers.append(oper)
                #print "Result produced %s (worker: %d)" %(oper.val, workerid)
        return opers

    def insert_op(self, dstport, oper):
        print("Received Oper {}".format(oper.val))
        self.inport[dstport].append(oper)
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
            
            print("Received args {}".format(args[0].val))

            return args
        else:
            return None



class Oper:
    def __init__(self, prodid, dstid, dstport, val):
        self.wid, self.dstid, self.dstport, self.val = prodid, dstid, dstport, val
        #wid -> id of the worker that produced the oper
        #dstid -> id of the target task
        #dstport -> input port of the target task
        #val -> actual value of the operand

        self.request_task = True #if true, piggybacks a request for a task to the worker where the opers were produced.





class Scheduler:
    TASK_TAG = 0
    TERMINATE_TAG = 1
    def __init__(self, graph, n_workers=1, mpi_enabled = True):
        #self.taskq = Queue()  #queue where the ready tasks are inserted
        self.operq = Queue()

        self.graph = graph
        self.tasks = []
        worker_conns = []
        self.conn = []
        self.waiting = [] #queue containing idle workers
        self.n_workers = n_workers #number of workers
        self.pending_tasks = [0] * n_workers #keeps track of the number of tasks sent to each worker without a request from the worker (due to affinity)
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
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        self.mpi_size = comm.Get_size()
        self.mpi_rank = rank
        self.n_slaves = self.mpi_size - 1
        self.keep_working = True

        if rank == 0:
            print("I am the master. There are %s mpi processes. (hostname = %s)" %(self.mpi_size, MPI.Get_processor_name()))
            self.pending_tasks = [0] * self.n_workers * self.mpi_size
            self.outqueue = Queue()
            def mpi_input(inqueue):
                while self.keep_working:
                    msg = comm.recv(source = MPI.ANY_SOURCE, tag = MPI.ANY_TAG)
                    #print "MPI Received opermsg from slave."
                    inqueue.put(msg)

            def mpi_output(outqueue):
                while self.keep_working:
                    task = outqueue.get()
                    if task != None: #task == None means termination
                        #print "MPI Sending task to slave node."
                        dest = task.workerid // self.n_workers #destination mpi process
                        comm.send(task, dest = dest, tag = Scheduler.TASK_TAG)
                    else:
                        self.keep_working = False
                        mpi_terminate()
            def mpi_terminate():
                print("MPI TERMINATING")
                for i in range(0, self.mpi_size):
                    comm.send(None, dest = i, tag = Scheduler.TERMINATE_TAG)

            t_in = threading.Thread(target = mpi_input, args = (self.operq,))
            t_out = threading.Thread(target = mpi_output, args = (self.outqueue,))
        else:
            print("I am a slave. (hostname = {})".format(MPI.Get_processor_name()))
            #slave
            self.inqueue = Queue()
            for worker in self.workers:
                worker.wid += rank * self.n_workers

            status = MPI.Status()
            def mpi_input(inqueue):
                while self.keep_working:
                    task = comm.recv(source = 0, tag = MPI.ANY_TAG, status = status)
                    if status.Get_tag() == Scheduler.TERMINATE_TAG:
                        self.keep_working = False
                        print("MPI received termination.")
                        self.terminate_workers(self.workers)	
                    else:
                        #print "MPI Sending task to worker in slave."
                        workerid = task.workerid
                        connid = workerid % self.n_workers
                        self.conn[connid].send(task)
                self.operq.put(None)
            def mpi_output(outqueue):
                while self.keep_working:
                    msg = outqueue.get()
                    if msg != None:
                        #print "MPI send opermsg to master."
                        comm.send(msg, dest = 0, tag = 0)

            t_in = threading.Thread(target = mpi_input, args = (self.inqueue,))
            t_out = threading.Thread(target = mpi_output, args = (self.operq,))

        threads = [t_in, t_out]
        self.threads = threads
        for t in threads:
            t.start()






    def propagate_op(self, oper):
        dst = self.graph.nodes[oper.dstid]

        dst.insert_op(oper.dstport, oper)
        args = dst.match()
        if args != None:
            self.issue(dst, args)
    def check_affinity(self, task):

        node = self.graph.nodes[task.nodeid]
        if node.affinity == None:
            return None

        affinity = node.affinity[0]
        if len(node.affinity) > 1:
            node.affinity = node.affinity[1:] + [node.affinity[0]] #shift the affinity list
        return affinity


    def issue(self, node, args):
        task = Task(node.f, node.id, args)
        self.tasks += [task]
       # print("Creating Task {} Tasks: {}".format(node, self.tasks))


    def all_idle(self, workers):
        #return 0
        if self.mpi_rank == 0:
            return len(self.waiting) == self.n_workers * self.mpi_size
        else:
            return len(self.waiting) == self.n_workers


    def terminate_workers(self, workers):
        print("Terminating workers {} {} {}".format(self.all_idle(self.workers), self.operq.qsize(), len(self.tasks)))
        if self.mpi_rank == 0:
            self.outqueue.put(None)
            for t in self.threads:
                t.join()
        for worker in workers:
            worker.terminate()

    def start(self):
        operq = self.operq

        print("Roots {}".format([r for r in self.graph.nodes if len(r.inport) == 0]))
        for root in [r for r in self.graph.nodes if len(r.inport) == 0]:
            task = Task(root.f, root.id)
            self.tasks += [task]


        for worker in self.workers:
            print("Starting {}".format(worker.wid))
            worker.start()

        if self.mpi_rank == 0 or self.mpi_rank == None:
            #it this is the leader process or if mpi is not being used 
            print("Main loop")
            self.main_loop()


    def main_loop(self):
        tasks = self.tasks
        operq = self.operq
        workers = self.workers
        while len(tasks) > 0 or not self.all_idle(self.workers) or operq.qsize() > 0:
            opersmsg = operq.get()
            for oper in opersmsg:
                if oper.val != None:
                    self.propagate_op(oper)

            wid = opersmsg[0].wid
            if wid not in self.waiting and opersmsg[0].request_task:
                if self.pending_tasks[wid] > 0:
                    self.pending_tasks[wid] -= 1
                else:
                    self.waiting += [wid] #indicate that the worker is idle, waiting for a task

            while len(tasks) > 0 and len(self.waiting) > 0:
                task = tasks.pop(0)
                wid = self.check_affinity(task)
                #print("Affinity is {} {}".format(task, wid))
                if wid != None:
                    if wid in self.waiting:
                        self.waiting.remove(wid)
                    else:
                        self.pending_tasks[wid] += 1
                else:
                    wid = self.waiting.pop(0)
                #print "Got opermsg from worker %d" %wid
                if wid < self.n_workers: #local worker
                    worker = workers[wid]

                    self.conn[worker.wid].send(task)
                else:
                    task.workerid = wid
                    self.outqueue.put(task)

        print("Waiting {}".format(self.waiting))		
        self.terminate_workers(self.workers)


