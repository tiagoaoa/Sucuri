import math, sys

def f(numInt,peso):
	total = 0
	itTotal = (((numInt % 10) * peso) +1)
	for y in xrange(1, itTotal):
		total += math.sqrt(y)
	return total


s = 0
n = int(sys.argv[1])
peso = int(sys.argv[2])

for x in xrange(1, n+1):
	s += f(x, peso)
print 'Result: %f' %s
