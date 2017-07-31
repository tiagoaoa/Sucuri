import sys, os

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *


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


nprocs = int(sys.argv[1])
n = int(sys.argv[2])

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)



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




