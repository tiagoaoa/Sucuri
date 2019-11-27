import sys, os

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *


f = lambda x: 1/(1+x**2)

def psum(args):
	stride, my_id, nprocs = args[0]
        print (stride, my_id, nprocs)
	

	sump = 0.0 
	print "Doing partial summation"

        x = stride * my_id
        while x < 1.0:
            x += stride * nprocs 
            sump += f(x) * stride


	print "Finished partial summation %f"  %sump
	return sump

def sum_total(args):
	total = 0.0
        print "Partials %s" %args
	for partial in args:
		total += partial

        pi = total * 4
	print "Reduction: %f" %pi


nprocs = int(sys.argv[1])
stride = float(sys.argv[2])

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)



R = Node(sum_total, nprocs)
graph.add(R)


for i in range(nprocs):
        Id = Feeder([stride, i, nprocs])
        graph.add(Id)

        Spartial = Node(psum, 1)
	graph.add(Spartial)

	Id.add_edge(Spartial, 0)
	Spartial.add_edge(R, i)



sched.start()




