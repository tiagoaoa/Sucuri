from pyDF import *

def soma(args):
	a, b = args
	print "Adding %d + %d" %(a, b)
	return a+b



graph = DFGraph()
sched = Scheduler(graph, mpi_enabled = False)

A = Feeder(1)
B = Feeder(2)
C = Node(soma, 2)

graph.add(A)
graph.add(B)
graph.add(C)


A.add_edge(C, 0)
B.add_edge(C, 1)

sched.start()




