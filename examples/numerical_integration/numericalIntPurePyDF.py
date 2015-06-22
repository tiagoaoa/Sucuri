import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *
import math

class ND(Node):
	
	def __init__(self, f, inputn, start, end, peso):
		self.f = f
		self.inport = [[] for i in range(inputn)]
		self.dsts = []
		self.affinity = None
		self.start = start
		self.end = end
		self.peso = peso
	
	def run(self, args, workerid, operq):
                if len(self.inport) == 0:
                        opers = self.create_oper(self.f(self), workerid, operq)
                else:
                        opers = self.create_oper(self.f(self,args), workerid, operq)
                self.sendops(opers, operq)



def intNum(self):
	s = 0
	for x in xrange(self.start, self.end+1):
		total = 0
	        itTotal = (((x % 10) * self.peso) + 1)
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
n = int(sys.argv[2])
peso = int(sys.argv[3])

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

R = Node(intNumTotal, nprocs)
graph.add(R)

size = n / nprocs

for i in range(nprocs):
	
	start = (i * size) + 1
	if(i == nprocs - 1):
                end = n
        else:
		end = (i + 1) * size
	print 'nprocs %d, i %d, start %d, end %d, peso %d' %(nprocs, i, start, end, peso)
	intPartial = ND(intNum, 0, start, end, peso)
	graph.add(intPartial)
	intPartial.add_edge(R, i)

sched.start()



