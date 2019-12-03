import os
import sys

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *


from PIL import Image

save_dir = 'grayimgs'

def list_imgs(rootdir):
    fnames = []

    for current, directories, files in os.walk(rootdir):
        for f in files:
            fnames.append(current + '/' + f)
 
    fnames.sort()
    return fnames



def rgb2gray(args):
    fname = args[0]
    grayname = fname.split('/')[-1]
    img = Image.open(fname).convert('LA')

    grayname = save_dir + '/' +  grayname.split('.')[0]  + '.png'

    img.save(grayname)



    return grayname


def print_name(args):
    fname = args[0]

    print "Converted %s" %fname




nprocs = int(sys.argv[1])
file_list = list_imgs(sys.argv[2])[:1000]

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)



feed_files = Source(file_list)

convert_file = FilterTagged(rgb2gray, 1)  

pname = Serializer(print_name, 1)


graph.add(feed_files)
graph.add(convert_file)
graph.add(pname)


feed_files.add_edge(convert_file, 0)
convert_file.add_edge(pname, 0)


sched.start()








