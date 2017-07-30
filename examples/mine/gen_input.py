import sys
import random
import string
for i in range(int(sys.argv[1])):
	linha = ""
	for j in range(10):
		linha += random.choice(string.letters + " ")

	linha += " "
	linha += "%s" %(random.uniform(0.10, 50.0))
	print linha

