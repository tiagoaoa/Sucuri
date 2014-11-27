import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

import re



def filterprices(args):
	prices = []
	filename = args[0]
	base = open("inputs/" + filename, "r")

	regexp = "R\$ [0-9]+\,[0-9]+"
	for line in base:
		for price in re.findall(regexp, line):
			fprice = float(price.replace("R$ ", "").replace(",","."))
			if fprice > 5:
				#print "%s > 5" %fprice
				prices+=[fprice]
	print "%s %s" %(filename, len(prices))
	return len(prices)

def outprices(args):
	print "Prices: %s" %args[0]

graph = DFGraph()
nprocs = int(sys.argv[1])
sched = Scheduler(graph, nprocs, mpi_enabled = False)

src = Source(sorted(os.listdir("inputs")))

filter = FilterTagged(filterprices, 1)

out = Serializer(outprices, 1)

graph.add(src)
graph.add(filter)
graph.add(out)

src.add_edge(filter, 0)
filter.add_edge(out, 0)


sched.start()
