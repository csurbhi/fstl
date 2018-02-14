mple showing how to run dm-nstl and an E-region STL on top:

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
