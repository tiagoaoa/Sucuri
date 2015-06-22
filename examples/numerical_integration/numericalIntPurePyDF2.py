import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *
import math

def intNum(args):
	peso=args[0]
	start=args[1]
	end=args[2]
	s = 0
	for x in xrange(start, end+1):
		total = 0
	        itTotal = (((x % 10) * peso) + 1)
       		for y in xrange(1, itTotal):
                	total += math.sqrt(y)
		s+= total
	return s

def intNumTotal(args):
	total = 0.0
	for partial in args:
		total+= partial
	print 'Result: %f' %total


nprocs = int(sys.argv[1])
nworkers = int(sys.argv[2])
n = int(sys.argv[3])
peso = int(sys.argv[4])

graph = DFGraph()
sched = Scheduler(graph, nworkers, mpi_enabled = False)

R = Node(intNumTotal, nprocs)
graph.add(R)

fpeso=Feeder(peso)
graph.add(fpeso)

size = n / nprocs

for i in range(nprocs):

	start = (i * size) + 1
	if(i == nprocs - 1):
                end = n
        else:
		end = (i + 1) * size

	fstart=Feeder(start)
	fend=Feeder(end)	
	graph.add(fstart)
	graph.add(fend)
	
	#print 'nprocs %d, i %d, start %d, end %d, peso %d' %(nprocs, i, start, end, peso)
	intPartial = Node(intNum, 3)
	graph.add(intPartial)

	fpeso.add_edge(intPartial,0)
	fstart.add_edge(intPartial,1)
	fend.add_edge(intPartial,2)
	
	intPartial.add_edge(R, i)

sched.start()



