import math, sys, numericalInt

s = 0
n = int(sys.argv[1])

for x in xrange(1, n+1):
	s += numericalInt.f(x)
print s	
