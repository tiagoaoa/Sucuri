import sys, os
sys.path.append(os.environ['PYDFHOME'])
from pyDF import *
from PIL import Image


def render_partial(args):
    task_id = int(args[0])
    n_splits = int(args[1])

    stride = 1 / n_splits

    os.system("sh ${PYDFHOME}/exemples/render_blend.sh %f %f" %(task_id * stride, (task_id + 1)* stride))
    


def join_partial_images(filenames):
    imgs = []

    for f in filenames:
        imgs.append(Image.open(f))

    width = img[0].width
    height = img[0].height

    dst = Image.new('RGB', (width * len(filenames), height))

    for img, index in zip(imgs,range(len(imgs))):
        dst.paste(img, (width * index, 0))

    dst.save('rendered_output.png')

n_splits = int(sys.argv[1]) #how many times the scene should be splitted  (higher n_splits => lower total rendering time)
graph = DFGraph()
sched = Scheduler(graph, mpi_enabled = True)

#Instatiate graph nodes and edges below.

JoinImages = Node(join_partial_images, n_splits)

for i in range(n_splits):
    Id = Feeder([i, n_splits, blend_file])
    graph.add(Id)
    
    RenderPartial = Node(render_partial, 1)
    graph.add(RenderPartial)

    Id.add_edge(RenderPartial, 0)
    RenderPartial.add_edge(JoinImages, i)
    

sched.start()

