#!/usr/bin/python

import random
import os
import sys

fd = os.open("/proc/rbtest", os.O_RDWR)

random.seed(17)

verbose = len(sys.argv) > 1 and sys.argv[1] == '-v'

_pba = 1000
_map = [-1] * 2050
    
for i in range(2000):
    a = random.randint(0,1999)
    l = random.randint(1,49)
    val = _pba
    _pba += l
    #if random.randint(0,100) < 4:
    #    val = -1
    for j in range(a,a+l):
        _map[j] = val + j - a
    os.write(fd, "u %d %d %d" % (a, l, val))
    if verbose:
        print "u %d,+%d -> %d" % (a, l, val)
        os.write(fd, b'f')
        val = os.read(fd, 128)
        while val != b'NULL\n':
            print "  ", val,
            os.write(fd, b'n')
            val = os.read(fd, 128)
    
    for j in range(30):
        a = random.randint(0,2049)

        os.write(fd, "q %d" % a)
        strval = str(os.read(fd, 128))
        rsp = pba = -1
        lba = a
        if strval != "NULL\n":
            if verbose:
                print strval,
            _tmp,_arrow,pba = strval.split()
            lba,n = _tmp.split(',+')
            (lba,pba,n) = map(int, (lba, pba, n))
            if lba <= a:
                rsp = pba + a - lba
        
        if verbose:
            print '- q %d = "%s" = %d' % (a, strval[0:-1], rsp)
        if rsp != _map[a]:
            print "ERROR: %d -> %d (%d)" % (a, rsp, _map[a])
            break
        elif verbose:
            print "ok: %d -> %d" % (a, rsp)

    if rsp != _map[a]:
        break
    
val = None
os.write(fd, b'f')
while val != b'NULL\n':
    os.write(fd, b'r')
    os.write(fd, b'f')
    val = os.read(fd, 128)

os.close(fd)
