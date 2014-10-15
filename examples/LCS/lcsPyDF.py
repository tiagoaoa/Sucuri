import sys
sys.path.append("/cluster_sala/pydf")
from pyDF import *
import math

def printMatrix(M):
    for i in xrange(len(M)):
        print M[i]

class N2D(Node):
    
    def __init__(self, f, inputn, i, j):
	        self.f = f
        	self.inport = [[] for n in range(inputn)]
        	self.dsts = []
        	self.i = i
        	self.j = j

    def add_edge(self, dst, dstport, srcport=0):
                self.dsts += [(dst.id, dstport, srcport)]
    
    def run(self, args, workerid, operq):
        #print "Running %s" %self
        
        if len(self.inport) == 0:
            output = self.f(self)
        else:
            output = self.f(self, args)
        opers = self.create_oper(output, workerid, operq)
        self.sendops(opers, operq)
    
    def create_oper(self, value, workerid, operq, tag = 0): #create operand message
        opers = []
        if self.dsts == []:
            opers.append(Oper(workerid, None, None, None)) #if no output is produced by the node, we still have to send a msg to the scheduler.
        else:
            for (dstid, dstport,srcport) in self.dsts:
                oper = Oper(workerid, dstid, dstport, value[srcport])
                oper.tag = tag
                opers.append(oper)
                #print "Result produced %s (worker: %d)" %(oper.val, workerid)
        return opers

def LCS(i,j,oper):
    startA = j*block;
    if ((j+1) == gW):
        endA = sizeA
    else:
        endA = startA+block
    startB = i*block;
    if ((i+1) == gH):
        endB = sizeB
    else:
        endB = startB+block
    lsizeA = endA - startA
    lsizeB = endB - startB
    #print 'Node (%d,%d) calculates (%d,%d) - (%d,%d)' % (i,j, startB,startA,endB,endA)

    SM = [[0 for x in xrange(lsizeA+1)] for y in xrange(lsizeB+1)]
    port=0
    if i>0:
        for x in xrange(len(oper[0])):
            SM[0][x]=oper[0][x]
        port=port+1
    if j>0:
        for x in xrange(len(oper[port])):
            SM[x][0] = oper[port][x]

    
    #print 'Antes: Matrix (%d, %d)' %(i,j)
    #printMatrix(SM)
    for ii in xrange(1, lsizeB+1):
        for jj in xrange(1, lsizeA+1):
            if sB[startB+ii-1] == sA[startA+jj-1]:
                SM[ii][jj] = SM[ii-1][jj-1] + 1
            else:
                SM[ii][jj] = max(SM[ii][jj-1], SM[ii-1][jj])

    #print 'Matrix (%d, %d)' %(i,j)
    #printMatrix(SM)
    ##if (i+1) == gH:
##	print 'Somente direita'
  ##  	return ([m[lsizeA] for m in SM],[])
    ##else:
    return (SM[lsizeB],[m[lsizeA] for m in SM])
def inputs(i,j):
    if i==0 and j==0:
        return 0
    if i==0 or j ==0:
        return 1
    return 2

def compute(self,args=[]):
    #encaixar substituir essa chamada ao LCS pela chamada SWIG
    #print 'Entrei no compute (%d, %d)' % (self.i, self.j)
    return LCS(self.i, self.j,args)

def printLCS(args):
    print 'Score = %d' % args[0][-1]
    #printMatrix(SM)

nameA = sys.argv[1]
nameB = sys.argv[2]
fA = open(nameA,'r')
fB = open(nameB,'r')
sA = fA.read()
sB = fB.read()

sizeA = len(sA) - int(sA[-1] == '\n')
sizeB = len(sB) - int(sB[-1] == '\n')
print "Sizes %d x %d" % (sizeB, sizeA)
nprocs = int(sys.argv[3])
block = int(sys.argv[4])
gH = int(math.ceil(float(sizeB)/block))
gW = int(math.ceil(float(sizeA)/block))
print "Grid %d x %d" % (gH, gW)
lcsGraph = DFGraph()
sched = Scheduler(lcsGraph, nprocs)

#SM = [[0 for j in xrange(sizeA+1)] for i in xrange(sizeB+1)]

G = [[N2D(compute,inputs(i,j),i,j) for j in xrange(gW)] for i in xrange(gH)]


for i in xrange(gH):
    for j in xrange(gW):
	#print 'Node (%d,%d) %d' % (i,j,inputs(i,j))
        lcsGraph.add(G[i][j])

for i in xrange(gH):
    for j in xrange(gW):
        if i > 0:
              #create edge from  upper neighbor
              #print 'Edge (%d,%d) -> (%d,%d)[%d]' % (i-1,j,i,j,0)
              G[i-1][j].add_edge(G[i][j],0,0)
        if j > 0:
              #create edge from left neighor
              #print 'Edge (%d,%d) -> (%d,%d)[%d]' % (i,j-1,i,j, int(i>0))
              G[i][j-1].add_edge(G[i][j],int(i>0),1)

R = Node(printLCS, 1)
lcsGraph.add(R)
G[gH-1][gW-1].add_edge(R,0)
sched.start()

