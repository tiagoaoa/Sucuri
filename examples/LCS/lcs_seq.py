import sys

def LCS(X, Y):
    m = len(X)
    n = len(Y)
    # An (m+1) times (n+1) matrix
    C = [[0 for j in xrange(n+1)] for i in xrange(m+1)]
    for i in xrange(1, m+1):
        for j in xrange(1, n+1):
            if X[i-1] == Y[j-1]:
                C[i][j] = C[i-1][j-1] + 1
            else:
                C[i][j] = max(C[i][j-1], C[i-1][j])
    return C

def printMatrix(M):
    for i in xrange(len(M)):
        print M[i]


nameA = sys.argv[1]
nameB = sys.argv[2]
fA = open(nameA,'r')
fB = open(nameB,'r')
sA = fA.read()
sB = fB.read()

dA = int(sA[len(sA)-1] == '\n')
dB = int(sB[len(sB)-1] == '\n')

SM = LCS(sB[0:len(sB)-dB],sA[0:len(sA)-dA])
#printMatrix(SM)
print SM[len(sB)-dB][len(sA)-dA]

