from pyDF import *
import sys, math

class ND(Node):
	
	def __init__(self, inputn, start, end):
		Node.__init__(self, self.intNum, inputn)
		self.start = start
		self.end = end
	
	def intNum(self):
		s = 0
		print "Running"
		for x in xrange(self.start, self.end+1):
			total = 0
		        itTotal = (((x % 10) * 100) + 1)
       			for y in xrange(1, itTotal):
                		total += math.sqrt(y)
			s+= total
		print "Partial %f" %s
		return s

def intNumTotal(args):
	total = 0.0
	for partial in args:
		total+= partial
	
	print "Adding %f + %f = %f" %(args[0], args[1], total)
#	print 'Reduction: %f' %total
	return total


nprocs = int(sys.argv[1])
n = int(sys.argv[2])

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

R = Node(intNumTotal, nprocs)
graph.add(R)

size = n / nprocs
partial_integrations = []

for i in xrange(nprocs):	
	start = (i * size) + 1
	end = (i + 1) * size
	print 'nprocs %d, i %d, start %d, end %d' %(nprocs, i, start, end)
	intPartial = ND(0, start, end)
	graph.add(intPartial)
	partial_integrations.append(intPartial)

total = nprocs
prev_level = partial_integrations

while total > 1:
	next_level = []
	print [i for i in range(len(prev_level))]
	for i in xrange(total/2):
		partial = Node(intNumTotal, 2)
		graph.add(partial)
		prev_level[i].add_edge(partial, 0)
		prev_level[i + total/2].add_edge(partial, 1)
		next_level += [partial]
	if total % 2:
		next_level += [prev_level[total - 1]]
	total = total/2 + (total % 2)
	prev_level = next_level

sched.start()



