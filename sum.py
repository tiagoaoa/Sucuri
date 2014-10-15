from pyDF import *
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


nworkers = int(sys.argv[1])
nprocs = int(sys.argv[2])
n = int(sys.argv[3])

graph = DFGraph()
sched = Scheduler(graph, nworkers, mpi_enabled = False)



A = Feeder([n, nprocs*nworkers])
graph.add(A)
R = Node(sum_total, nprocs*nworkers)
graph.add(R)
S = []
for i in range(nprocs*nworkers):
	Spartial = Node(psum, 1)
	graph.add(Spartial)
	A.add_edge(Spartial, 0)
	Spartial.add_edge(R, i)



sched.start()




