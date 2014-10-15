#!/bin/bash
P=$1
swig -python numericalInt.i 
gcc -fPIC -c numericalInt.c numericalInt_wrap.c -DPESO=${P} -I/usr/include/python2.7 -I.
ld -shared numericalInt.o numericalInt_wrap.o -o _numericalInt.so
#time python numericalIntHybridPython.py $1
