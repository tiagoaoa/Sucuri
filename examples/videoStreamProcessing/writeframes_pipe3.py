#import os
import sys
sys.path.append("/home/lmarzulo/Documents/Dataflow/pydf")
from pyDF import *
import numpy as np
import cv2


class VideoStreamer(Node): #source class

        def __init__(self,fname):
                self.inport = []
                self.dsts = []
                self.f = teste
                self.tagcounter = 0
                self.fname = fname

        def run(self, args, workerid, operq):
                cap = cv2.VideoCapture(self.fname)
                print "Abrindo" 
                while(cap.isOpened()):
					ret, frame = cap.read()
					if ret==True:
						self.tagcounter += 1
						#print "Reading frame %d" % self.tagcounter
						opers = self.create_oper((frame,self.tagcounter), workerid, operq, self.tagcounter)
						for oper in opers:
							oper.request_task = False
						self.sendops(opers, operq)
						#print "Enviado %d" % opers[0].tag
					else:
						break
                opers = [Oper(workerid, None, None, None)] #sinalize eof
                self.sendops(opers, operq)
                print "saindo" 
                cap.release()

def teste(args):
	print "test" 
	return 0
	
def iFilter(args):
	#print "Filter %d" % args[0][1]
	
	frame = args[0][0]
	
	kernel = np.ones((5,5),np.float32)/25
	f2d = cv2.filter2D(frame,-1,kernel)
	
	gblur =cv2.GaussianBlur(f2d,(5,5),0)
	
	bfilt = cv2.bilateralFilter(gblur,9,75,75)
	
	flip = cv2.flip(bfilt,0)

	edge = cv2.Canny(flip,100,200)
	
	blur = cv2.blur(edge,(10,10))
        
	mblur = cv2.medianBlur(blur,5)	
	
	return (mblur,args[0][1])
        
def imgWrite(args):
	#print "Write %d" % args[0][1]
	cv2.imwrite("%s/frame%09d.jpg" % (path, args[0][1]), args[0][0])



nprocs = int(sys.argv[1])
path=sys.argv[3]
fname=sys.argv[2]
#os.makedirs(path)



graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)


streamer = VideoStreamer(fname)
imgFilter = Node(iFilter,1)
imgWriter = Node(imgWrite,1)

graph.add(streamer)
graph.add(imgFilter)
graph.add(imgWriter)

streamer.add_edge(imgFilter, 0)
imgFilter.add_edge(imgWriter, 0)

sched.start()

# Release everything if job is finished

#cv2.destroyAllWindows()



