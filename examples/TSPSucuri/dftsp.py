import sys, os

sys.path.append("/home/imcoelho/Sucuri")
from pyDF import *

hostname = os.uname()[1]



import numpy
def alloc_cpu():
	print "primeira %s" %hostname 
	a = numpy.random.randn(4,4)
	a = a.astype(numpy.float32)
	
	return a

def run_kernel(args):
	a = args[0]
	import pycuda.driver as cuda
	import pycuda.autoinit
	from pycuda.compiler import SourceModule

	print "Calling kernel %s" %hostname
	a_gpu = cuda.mem_alloc(a.nbytes)

	cuda.memcpy_htod(a_gpu, a)


	mod = SourceModule("""
	  __global__ void doublify(float *a)
	  {
	    int idx = threadIdx.x + threadIdx.y*4;
	    a[idx] *= 2;
	  }
	  """)


	func = mod.get_function("doublify")
	func(a_gpu, block=(4,4,1))



	a_doubled = numpy.empty_like(a)
	cuda.memcpy_dtoh(a_doubled, a_gpu)


	return a_doubled


def print_a(args):
		print "Output %s: %s" %(hostname, args)

ini = Node(alloc_cpu, 0)




num_workers = int(sys.argv[1])
graph = DFGraph()
sched = Scheduler(graph, num_workers, mpi_enabled = True)




graph.add(ini)
run_gpus = []

output = Node(print_a, num_workers)
graph.add(output)


for i in range(num_workers):
	gpunode = Node(run_kernel, 1)
	gpunode.pin(i*num_workers)


	run_gpus.append(gpunode)
	graph.add(gpunode)
	ini.add_edge(gpunode, 0)
	
	gpunode.add_edge(output, i)






sched.start()
