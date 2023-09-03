#!/usr/bin/env python

import matplotlib.pyplot as plt
import csv
import numpy

i = 0
bw = []
time = []
maxbw=0
minbw=10000000000
with open('bw_log_bw.1.log', 'r') as csvfile:
        bwreader = csv.reader(csvfile, delimiter=',')
        for row in bwreader:
            time.append(int(row[0])/1000)
            temp = int(row[1])
            bw.append(temp/1000)
            if temp > maxbw:
                maxbw = temp
            if temp < minbw:
                minbw = temp


print ("Max-bw: " + str(maxbw));
print("Min-bw: " + str(minbw));
print("Len: " + str(len(bw)));
nr=int (len(bw)/2)
sortedbw = sorted(bw)
meanbw = sortedbw[nr];
count = 0

for temp in bw:
    #print(" * " + str(temp))
    if (temp < 1):
        count = count + 1
print("Number of times bw <= 1000 is: " + str(count));
print("Mean-bw: " + str(meanbw)); 
table = []
meanbw = numpy.mean(bw)
print("*Average-bw:", "{:1.2f}".format(meanbw))
medianbw = numpy.median(bw)
print("median-bw: " + "{:1.2f}".format(medianbw));
stddev = numpy.std(bw);
print("Standard deviation: ", "{:1.2f}".format(stddev));
variance = numpy.var(bw)
print("variance: " + "{:1.2f}".format(variance));
tenper = numpy.percentile(bw, 90, 0)
print("Top 10 percentile: ", "{:1.2f}".format(tenper));
ninetyper = numpy.percentile(bw, 10, 0)
print("Bottom 10 percentile: ", "{:1.2f}".format(ninetyper));
#The first coordinate is a shift on the x-axis, second coordinate is a gap between plot and text box (table in your case), third coordinate is a width of the text box, fourth coordinate is a height of text box.
plt.plot(time, bw)
plt.xlabel("Time in seconds")
plt.ylabel("Write bandwidth");
plt.savefig("STL_unifrom-write.jpg");
plt.show()
plt.box(on=None)
