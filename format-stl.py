#/usr/bin/python
#
# file:        format-stl.py
# description:
#

import stllib
import sys
import os
import time

if len(sys.argv) != 6:
    print "usage: format <device> <zone_lbas> <ckpt_zones> <cache_zones> <data_zones>"
    sys.exit()

#homa
conv_zones = 64 #based on 8TB host mangaed drive
stllib.disk_open(sys.argv[1])

##### write the superblock

sb = stllib.stl_sb(magic=stllib.SB_MAGIC,
                   zone_lbas = int(sys.argv[2]),
                   ckpt_zones = int(sys.argv[3]),
                   cache_zones = int(sys.argv[4]),
                   temp_zones = 2,
                   data_zones = int(sys.argv[5]),
                   last_modified = int(time.time()))
stllib.write_superblock(sb, 0+conv_zones)
sb.last_modified = 0
stllib.write_superblock(sb, 1+conv_zones)


# write a checkpoint with:
#   WF = first media cache zone
#   free zones = remaining media cache zones
#   map = one extent per data zone
# looks like this:
# [0: sb] [1: hdr] [2: wf+freelist] [3: data extents...] [x: next]

exts_per_pg = 4096 / 16
n_extent_pgs = (sb.data_zones + exts_per_pg - 1) / exts_per_pg
next_page = 1 + 1 + 1 + n_extent_pgs
this_page = 1

# header
h = stllib.stl_hdr(next_pba=(conv_zones*sb.zone_lbas + next_page*8), seq=1)
stllib.write_hdr(conv_zones*sb.zone_lbas +  this_page*8, h)
this_page += 1

# wf+freelist
WFs = [(conv_zones+ sb.ckpt_zones) * sb.zone_lbas]
FZs = [((conv_zones+sb.ckpt_zones+i) * sb.zone_lbas, (conv_zones+sb.ckpt_zones+i+1) * sb.zone_lbas)
       for i in range(1, sb.cache_zones)]
stllib.write_ckpt(conv_zones*sb.zone_lbas + this_page*8, WFs, FZs)
this_page += 1

# map
data_start = (conv_zones + sb.ckpt_zones + sb.cache_zones + sb.temp_zones) * sb.zone_lbas

_map = [ (i, data_start + i, sb.zone_lbas) for i in range(0, sb.data_zones*sb.zone_lbas, sb.zone_lbas)]
stllib.write_map(conv_zones*sb.zone_lbas + this_page*8, _map)
this_page += n_extent_pgs

# for CMR we overwrite the next checkpoint location and the current write frontier so
# startup doesn't recover old data.

#stllib.spoil(this_page*8) ##homma
#stllib.spoil(WFs[0])

