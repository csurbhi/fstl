#!/usr/bin/env python

import matplotlib.pyplot as plt
import csv
import numpy

freq = [0] * 29808

zonenr = 0
i=0;
with open('writeiolog', 'r') as csvfile:
        bwreader = csv.reader(csvfile, delimiter=' ')
        for row in bwreader:
            if (i >= 3):
                if (row[1] == 'write'):
                    bytenr = int(row[2])
                    blknr = bytenr/4096
                    zonenr = int(blknr/65536)
                    if (zonenr > 29808):
                        print("zonenr: " + str(zonenr) + " blknr: " + str(blknr))
                    else:
                        freq[zonenr] = freq[zonenr] + 1
            i = i + 1

i = 0;
for x in freq:
    if x > 0:
        print ("zonenr: " + str(i) + " : freq: " + str(x))
        i = i + 1


