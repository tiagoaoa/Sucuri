from pydf import *
import sys

def psum(args):

	n, nprocs = args[0][0], args[0][1]
	n /= nprocs

	sump = 0.0
	print "Doing a summation"
	for i in range(n):
		sump += 0.1

	print "Finished summation"
	return sump

def sum_total(args):
	total = 0.0
	for partial in args:
		total += partial
	print "Reduction: %f" %total

class Feeder(Node):

	def __init__(self, value):
		self.value = value
		self.dsts = []
		self.inport = []

	def f(self):
		print "Feedind %s" %self.value
		return self.value


nprocs = int(sys.argv[1])
n = int(sys.argv[2])

graph = DFGraph()
sched = Scheduler(graph, nprocs)



A = Feeder([n, nprocs])
graph.add(A)
R = Node(sum_total, nprocs)
graph.add(R)
S = []
for i in range(nprocs):
	Spartial = Node(psum, 1)
	graph.add(Spartial)
	A.add_edge(Spartial, 0)
	Spartial.add_edge(R, i)



sched.start()




