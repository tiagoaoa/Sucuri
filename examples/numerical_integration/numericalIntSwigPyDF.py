from pyDF import *
import sys, math, numericalInt

class ND(Node):
	
	def __init__(self, f, inputn, start, end):
		self.f = f
		self.inport = [[] for i in range(inputn)]
		self.dsts = []
		self.start = start
		self.end = end
	
	def run(self, args, workerid, operq):
                if len(self.inport) == 0:
                        opers = self.create_oper(self.f(self), workerid, operq)
                else:
                        opers = self.create_oper(self.f(self,args), workerid, operq)
                self.sendops(opers, operq)



def intNum(self):
	s = 0
	for x in xrange(self.start, self.end+1):
		s += numericalInt.f(x % 10)
	return s

def intNumTotal(args):
	total = 0.0
	for partial in args:
		total+= partial
	print 'Result: %f' %total


nprocs = int(sys.argv[1])
n = int(sys.argv[2])

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
	print 'nprocs %d, i %d, start %d, end %d' %(nprocs, i, start, end)
	intPartial = ND(intNum, 0, start, end)
	graph.add(intPartial)
	intPartial.add_edge(R, i)

sched.start()



