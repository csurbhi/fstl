Copyright (c) 2016 Northeastern University

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


------------------------------------------------------------------------------------------------------------------------------------

       FSTL -- A Framework for Design and Exploration of Shingled Magnetic Recording Translation Layers 

           Peter Desnoyers, Mohammad Hossein Hajkazemmi, Mania Abdi, Mansour Shafaei 
			pjd@ccs.neu.edu, hajkazemi@ece.neu.edu
              			Solid-State Storage Lab
        		Northeastern University Computer Science



An example  showing how to run dm-nstl and an E-region STL on top:

1- build the stllib shared liberary:
        $ gcc -c -fPIC -lz libstl.c  -o libstl.o
        $ gcc libstl.o -shared -o libstl.so
2- build the device-mapper target
        $ make
3- insert teh kernel module
        $ insmod dm-nstl.ko

4- run the STL with
        $ python2.7 format-stl.py /dev/sdb 524288 2 118 28000
        $ python2.7 seagate.py /dev/sdb TL1 FIFO
