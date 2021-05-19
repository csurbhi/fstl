import sys
import time
import subprocess

# create the device mapper target and open the control file
#  
zone_lbas = (256 * 1024 * 1024 / 512)
print "zone_lbas: " + str(zone_lbas)
#data_start = 1048576
data_start = 0
volume_size = 75 * zone_lbas
print "volume_size: " + str(volume_size)
blkdev = "/dev/vdb"
print "blkddev: " + str(blkdev)
tgtname = "TL0"
print "tgtname: " + str(tgtname)
data_end = volume_size
print "data_end: " + str(data_end)
print '0 %d nstl %s %s %d %d' % (volume_size, blkdev, tgtname, zone_lbas, data_end)
subprocess.call(['/sbin/dmsetup', 'create', tgtname, '--table',
                 '0 %d lsdm %s %s %d %d' % (volume_size, blkdev, tgtname, 
                                            zone_lbas, data_end)])
