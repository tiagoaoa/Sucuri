#Example of a simple filter function applied to multiple text files.
import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

import re



def filterprices(args):
	prices = []
	filename = args[0]
	base = open("inputs/" + filename, "r")

	regexp = "[0-9]+\.[0-9]+" #"R\$ [0-9]+\,[0-9]+"
	for line in base:
		for price in re.findall(regexp, line):
			fprice = float(price)#price.replace("R$ ", "").replace(",","."))
			if fprice > 20.0:
				#print "%s > 5" %fprice
				prices+=[fprice]
	print "%s %s" %(filename, len(prices))
	return (filename, len(prices))

def outprices(args):
	print "Output Prices: %s %d" %(args[0][0], args[0][1])

graph = DFGraph()
nprocs = int(sys.argv[1])
sched = Scheduler(graph, nprocs, mpi_enabled = False)

src = Source(sorted(os.listdir("inputs")))

fltr = FilterTagged(filterprices, 1)

out = Serializer(outprices, 1)

graph.add(src)
graph.add(fltr)
graph.add(out)

src.add_edge(fltr, 0)
fltr.add_edge(out, 0)
fltr.pin([2,3])

sched.start()
