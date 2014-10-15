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
						print "Reading frame %d" % self.tagcounter
						opers = self.create_oper((frame,self.tagcounter), workerid, operq, self.tagcounter)
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
	
def flip(args):
	print "Flip %d" % args[0][1]
	return (cv2.flip(args[0][0],0),args[0][1])

def edge(args):
	print "Edge %d" % args[0][1]
	return (cv2.Canny(args[0][0],100,200),args[0][1])
        
def blur(args):
	print "Blur %d" % args[0][1]
	return (cv2.blur(args[0][0],(10,10)),args[0][1]) 
        
def mblur(args):
	print "mBlur %d" % args[0][1]
	return (cv2.medianBlur(args[0][0],5),args[0][1])
        
def imgWrite(args):
	print "Write %d" % args[0][1]
	cv2.imwrite("%s/frame%09d.jpg" % (path, args[0][1]), args[0][0])



nprocs = int(sys.argv[1])
path=sys.argv[3]
fname=sys.argv[2]
#os.makedirs(path)



graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)


streamer = VideoStreamer(fname)
flipFilter = Node(flip,1)
edgeFilter = Node(edge,1)
blurFilter = Node(blur,1)
mblurFilter = Node(mblur,1)
imgWriter = Node(imgWrite,1)

graph.add(streamer)
graph.add(flipFilter)
graph.add(edgeFilter)
graph.add(blurFilter)
graph.add(mblurFilter)
graph.add(imgWriter)

streamer.add_edge(flipFilter, 0)
flipFilter.add_edge(edgeFilter, 0)
edgeFilter.add_edge(blurFilter, 0)
blurFilter.add_edge(mblurFilter, 0)
mblurFilter.add_edge(imgWriter, 0)

sched.start()

# Release everything if job is finished

#cv2.destroyAllWindows()



