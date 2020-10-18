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
    print ("usage: format <device> <zone_lbas> <ckpt_zones> <cache_zones> <data_zones>")
    sys.exit()

#homa
conv_zones = 0 #based on 8TB host mangaed drive
print("argv[1]: " + sys.argv[1])
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
n_extent_pgs = int((sb.data_zones + exts_per_pg - 1) / exts_per_pg)
if ((sb.data_zones + exts_per_pg - 1) % exts_per_pg) > 0:
    n_extent_pgs = n_extent_pgs + 1
next_page = 1 + 1 + 1 + n_extent_pgs
this_page = 1

# header
pba = conv_zones*sb.zone_lbas + next_page*8;
print("pba: " + str(pba))
h = stllib.stl_hdr(next_pba=pba, seq=1)
stllib.write_hdr(conv_zones*sb.zone_lbas +  this_page*8, h)
this_page += 1

# wf+freelist
print("sb.ckpt_zones: " + str(sb.ckpt_zones))
WFs = [(conv_zones+ sb.ckpt_zones) * sb.zone_lbas]
print("WFs: " + str(*WFs))
print("sb.cache_zones: " + str(sb.cache_zones))
FZs = [((conv_zones+sb.ckpt_zones+i) * sb.zone_lbas, (conv_zones+sb.ckpt_zones+i+1) * sb.zone_lbas)
       for i in range(1, sb.data_zones)]
print("FZs: " + str(FZs))
print("")

print("sb.zone_lbas: " + str(sb.zone_lbas) + " conv_zones: " + str(conv_zones) + " this_page: " + str(this_page))
print("**writing checkpoint at: " + str(conv_zones*sb.zone_lbas + this_page*8))
ret=stllib.write_ckpt(conv_zones*sb.zone_lbas + this_page*8, WFs, FZs)
print("-------------------->written checkpoint sectors: " + str(ret))
this_page += 1

# map
data_start = (conv_zones + sb.ckpt_zones + sb.temp_zones) * sb.zone_lbas

_map = [ (i, data_start + i, sb.zone_lbas) for i in range(0, sb.data_zones*sb.zone_lbas, sb.zone_lbas)]
stllib.write_map(conv_zones*sb.zone_lbas + this_page*8, _map)
this_page += n_extent_pgs

# for CMR we overwrite the next checkpoint location and the current write frontier so
# startup doesn't recover old data.

#stllib.spoil(this_page*8) ##homma
#stllib.spoil(WFs[0])

