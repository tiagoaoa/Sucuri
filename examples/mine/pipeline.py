import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *





def print_line(args):
	line = args[0]
	print "-- " + line[:-1] + " --"

nprocs = int(sys.argv[1])

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)
fp = open("text.txt", "r")

src = Source(fp)
printer = Serializer(print_line, 1)

printer.pin(0)
graph.add(src)
graph.add(printer)

src.add_edge(printer, 0)

sched.start()
