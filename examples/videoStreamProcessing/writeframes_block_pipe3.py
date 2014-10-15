#import os
import sys
sys.path.append("/home/lmarzulo/Documents/Dataflow/pydf")
from pyDF import *
import numpy as np
import cv2
import time




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
						print "Reading frame %d" % self.tagcounter
						
						frameList=[]
						frameList.append(frame)
						for b in xrange(blockSize-1):
							if (cap.isOpened()):
								ret, frame = cap.read()
								if ret==True:
										frameList.append(frame)
								else:
										break;
							else:
								break
								
						opers = self.create_oper((frameList,self.tagcounter), workerid, operq, self.tagcounter)
						for oper in opers:
							oper.request_task = False
						self.sendops(opers, operq)
						print "Enviado %d" % opers[0].tag
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
	frameID = (blockSize*(args[0][1]-1)) + 1
	results = []
	for frame in args[0][0]:
		print "Filtering Frame %d" % frameID
		flip = cv2.flip(frame,0)

		edge = cv2.Canny(flip,100,200)
	
		blur = cv2.blur(edge,(10,10))
        
		mBlur = cv2.medianBlur(blur,5)
		
		results.append(mBlur)
		frameID+=1
		
	return (results,args[0][1])
        
def imgWrite(args):
	frameID = (blockSize*(args[0][1]-1)) + 1
	for frame in args[0][0]:
		print "Writing Frame %d" % frameID
		cv2.imwrite("%s/frame%09d.jpg" % (path, frameID), frame)
		frameID+=1



nprocs = int(sys.argv[1])
path=sys.argv[3]
fname=sys.argv[2]
blockSize = int(sys.argv[4])
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



