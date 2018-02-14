#!/usr/bin/python

import random

extents = [(random.randint(0,2000), random.randint(1,40)) for i in range(30)]
extents.sort()

i=0
while i+1 < len(extents):
    if extents[i][0] + extents[i][1] > extents[i+1][0]:
        del extents[i]
    else:
        i += 1

_map2 = [None] * 2041
for a,n in extents:
    for i in range(a,a+n):
        _map2[i] = "%d,+%d" % (a,n)

last = _map2[-1]
for i in range(len(_map2)-1,-1,-1):
    if not _map2[i]:
        _map2[i] = last
    last = _map2[i]
for i in range(len(_map2)):
    if not _map2[i]:
        _map2[i] = 'NULL'


        # for a,n in extents:
        #     print "%d,+%d" % (a, n)
random.shuffle(extents)
for a,n in extents:
    print "%d,+%d" % (a, n)

import os
fd = os.open("/proc/rbtest", os.O_RDWR)
for a,n in extents:
    s = "i %d %d %d\n" % (a, n, 10000)
    os.write(fd, s)

val = None
os.write(fd, b'f')
while val != b'NULL\n':
    val = os.read(fd, 128)
    print val,
    os.write(fd, b'n')


for i in range(500):
    a = random.randint(0,2000)
    os.write(fd, 'q %d' % a)
    val = os.read(fd, 128)
    ext = str(val).split()[0]
    if ext != _map2[a]:
        print 'ERROR: %d %s [%s]' % (a, ext, _map2[a]),
    else:
        print 'OK: ', ext, _map2[a]

val = None
os.write(fd, b'f')
while val != b'NULL\n':
    os.write(fd, b'r')
    os.write(fd, b'f')
    val = os.read(fd, 128)

os.close(fd)
