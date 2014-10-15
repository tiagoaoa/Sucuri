import os
import numpy as np
import cv2
import sys
path=sys.argv[2]
fname=sys.argv[1]
#os.makedirs(path)
cap = cv2.VideoCapture(fname)

# Define the codec and create VideoWriter object
count=0
while(cap.isOpened()):
    ret, frame = cap.read()
    if ret==True:
		kernel = np.ones((5,5),np.float32)/25
		f2d = cv2.filter2D(frame,-1,kernel)
		
		gblur =cv2.GaussianBlur(f2d,(5,5),0)
	
		bfilt = cv2.bilateralFilter(gblur,9,75,75)
	
		flip = cv2.flip(bfilt,0)

		edge = cv2.Canny(flip,100,200)
	
		blur = cv2.blur(edge,(10,10))
        
		mblur = cv2.medianBlur(blur,5)	
		
		count+=1
		cv2.imwrite("%s/frame%09d.jpg" % (path, count), mblur)
	
    else:
        break

# Release everything if job is finished
cap.release()
cv2.destroyAllWindows()
