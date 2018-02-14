#
# file:        seagate.py
# description: semi-emulation of Seagate drive-managed shingling translation layer
#

import sys
import time
import subprocess
import stllib


if len(sys.argv) != 4:
    print 'usage: seagate <name> <device> <cleaning method>'
    exit()

#homa, the offset (number of conventional zones) could be written in the superblock
conv_zones = 64 #based on 8TB host mangaed drive
ZONE_LBAs = 524288

hdparm_path = "/home/goedel/projects/dm-nstl/hdparm-smr"

blkdev, tgtname, gc_type = sys.argv[2], sys.argv[1],  sys.argv[3]

if gc_type != 'min_sec' and gc_type != 'min_bnd' and gc_type != 'fifo':
    print 'wrong  garbage collection type selected'
    exit()

# read superblocks, keep most recent
stllib.disk_open(blkdev)
sb1 = stllib.read_superblock(lba=conv_zones*ZONE_LBAs) ## homa
if not sb1:
    print 'bad disk format (superblock1)'
    exit()

sb2 = stllib.read_superblock(lba=(conv_zones+1)*ZONE_LBAs) ## homa
if not sb2:
    print 'bad disk format (superblock2) - continuing'
if sb1.last_modified > sb2.last_modified:
    sb = sb1
    next_ckpt = stllib.PAGE_SECTORS + (conv_zones*ZONE_LBAs) ##homa
else:
    sb = sb2
    next_ckpt = stllib.PAGE_SECTORS + ((conv_zones+1)*ZONE_LBAs)  ##homa

zone_lbas = sb.zone_lbas

#homa
cache_pba = (conv_zones+sb.ckpt_zones) * zone_lbas 
cache_end_pba = cache_pba + sb.cache_zones * zone_lbas
#cache_pba = (conv_zonez) * zone_lbas

# this works for non-power-of-2 zone sizes...
def zone_start(lba):
    return lba - (lba % zone_lbas)
def zone_end(lba):
    return zone_start(lba) + zone_lbas

end_ckpt = zone_end(next_ckpt)
last_valid = -1
last_seq = 0
_last_gc_pba = [cache_pba + i*zone_lbas for i in range(sb.cache_zones)]

# used to keep track of cleaning
last_gc_pba = [i for i in range(cache_pba, 
                                cache_pba+sb.cache_zones*zone_lbas, zone_lbas)]
def check_gc_pba():
    for a,b in zip(last_gc_pba, _last_gc_pba):
        if a < b or a >= zone_end(b):
            print 'ERROR'

# chase down the last checkpoint. 
#start_space = stllib.get_space()
while next_ckpt < end_ckpt:
    h = stllib.read_hdr(next_ckpt)
    if not h or h.seq < last_seq:
        break
    last_valid, seq = next_ckpt, h.seq
#    print 'checkpoint at %d, free room in LBAs: %d' % (next_ckpt), start_space
    print 'checkpoint at %d, next checkpoint at %d' % (next_ckpt, h.next_pba)
    next_ckpt = h.next_pba

if last_valid == -1:
    print 'disk corrupt - no checkpoint'
    exit()

# create the device mapper target and open the control file
# 
data_start = (conv_zones + sb.ckpt_zones + sb.cache_zones + sb.temp_zones) * sb.zone_lbas ## homa
volume_size = sb.data_zones * zone_lbas
data_end = data_start + volume_size
subprocess.call(['/sbin/dmsetup', 'create', tgtname, '--table',
                 '0 %d nstl %s %s %d %d' % (volume_size, blkdev, tgtname, 
                                            zone_lbas, data_end)])
time.sleep(0.5)
ctlfile = '/dev/stl/' + tgtname
stllib.ctl_open(ctlfile)

# read the write frontier and free zones from the checkpoint,
# but don't do anything with them until we've replayed the journal
#
def cache_band(lba):
    return lba/sb.zone_lbas - (sb.ckpt_zones + conv_zones)

prev_ckpt = last_valid
last_valid += 8
wf, freezones = stllib.read_ckpt(last_valid)
freezone_map = [False] * sb.cache_zones
for start,end in freezones:
    freezone_map[cache_band(start)] = True

# read the map and feed it to the device
#
last_valid += 8
_map = stllib.read_map(last_valid, (next_ckpt - last_valid) / stllib.PAGE_SECTORS)
stllib.put_map(_map)

# chase down checkpoint, removing free zones from the list if we wrap
# around to a new one
#
while True:
    h = stllib.read_hdr(wf)
    if not h:
        break
    if h.len > 0:
        stllib.put_map([(h.lba, h.pba, h.len)])
    wf = h.next_pba
    freezone_map[cache_band(wf)] = False

# feed the free zone list and write frontier to the target, and 
# start handling user I/Os
#
free_bands = [tuple[0] for tuple in filter(lambda x: x[1], enumerate(freezone_map))]
freezones = [(x + conv_zones + sb.ckpt_zones) * sb.zone_lbas for x in free_bands] ##homa
for lba in freezones:
    stllib.put_freezone(lba, lba+sb.zone_lbas)
    stllib.put_space(sb.zone_lbas)
    
stllib.put_write_frontier(wf)
stllib.put_space(zone_end(wf) - wf)

stllib.start_ios()

msgs_per_pg = 4096 / 16

#----------------------------------------------------------------------
def write_header(lba, _prev, _next):
    h = stllib.stl_hdr(prev_pba=_prev, next_pba=_next)
    stllib.write_hdr(lba, h)

def checkpoint():
    global next_ckpt, prev_ckpt
    wf = stllib.get_write_frontier()
    seq = stllib.get_sequence()
    zones = stllib.get_freezones()
    space = stllib.get_space()
    map = stllib.get_map()
    
    print 'checkpoint: %d wf: %d freezones: %d records: %d, free space %d' % (next_ckpt, wf, 
                                                               len(zones), len(map), space)

    n_zone_pages = 1                      # cache is <= 256 bands
    n_map_pages = (len(map) + msgs_per_pg - 1) / msgs_per_pg
    n_pages = 1 + n_zone_pages + n_map_pages


    if next_ckpt + (n_pages+2)*stllib.PAGE_SECTORS > zone_end(next_ckpt):
        new_sb = sb1 #homa
        new_sb.last_modified = int(time.time()) #homa
        this_band = next_ckpt / zone_lbas #homa
        new_band = conv_zones + ((this_band + 1) % sb.ckpt_zones)
        #new_band = (this_band + 1) % sb.ckpt_zones
        new_next_ckpt = new_band * zone_lbas + stllib.PAGE_SECTORS # we waana reserve the first page for writing the superblock
        write_header(next_ckpt, prev_ckpt, new_next_ckpt) # Since next time we will start from the new band we may not need to write this header
        prev_ckpt,next_ckpt = next_ckpt,new_next_ckpt
        zloc = new_band * zone_lbas
        print "Reseting ckpt zone----------------------- at ", zloc, "\n"
        rstZPtr = subprocess.call(["sudo %s/hdparm --please-destroy-my-drive  --reset-one-write-pointer %s %s" %(hdparm_path, zloc, blkdev)], shell=True)
        stllib.write_superblock(new_sb, new_band)

#    if next_ckpt + (n_pages+2)*stllib.PAGE_SECTORS > zone_end(next_ckpt):
#        this_band = next_ckpt / zone_lbas
#        new_band = (this_band + 1) % sb.ckpt_zones
#        new_next_ckpt = new_band * zone_lbas + stllib.PAGE_SECTORS
#        write_header(next_ckpt, prev_ckpt, new_next_ckpt)
#        prev_ckpt,next_ckpt = next_ckpt,new_next_ckpt

    new_next_ckpt = next_ckpt + (1 + n_pages) * stllib.PAGE_SECTORS
    write_header(next_ckpt, prev_ckpt, new_next_ckpt)
    prev_ckpt = next_ckpt
    next_ckpt += stllib.PAGE_SECTORS

    stllib.write_ckpt(next_ckpt, [wf], zones)
    next_ckpt += stllib.PAGE_SECTORS

    stllib.write_map(next_ckpt, map)
    next_ckpt += n_map_pages * stllib.PAGE_SECTORS

    
def trim_lbas(extents, lo, hi):
    for entry in extents:
        if entry[0] < lo:
            n = lo - entry[0]
            entry[0] = lo
            entry[1] += n
            entry[2] -= n
        if entry[0] + entry[2] > hi:
            entry[2] = hi - entry[0]

def same_zone(a, b):
    return zone_start(a) == zone_start(b)

"""
   pacing: the put_space mechanism allows us to inform the kernel
   module of cleaning progress so that it can release I/Os gradually
   as cleaning progresses.
   need to fix this so that we have more control over blocking
"""
#gc_type = 'min_sec'
#gc_type = 'min_bnd'
#gc_type = 'fifo'

gc_num = 1
fifo_bnd_to_clean = 0;
valid_bnds = {}
def garbage_collect():
    global gc_num
    global fifo_bnd_to_clean
    # collect one zone - first get map, etc.
    #
#    fp = open("/home/goedel/projects/dm-nstl/experiment/compilebench/tmp/gc-%d.log" % gc_num, "w")
    fp = open("/tmp/gc-%d.log" % gc_num, "w")
    gc_num += 1
    seq, wf = stllib.get_sequence(), stllib.get_write_frontier()
    freezones = stllib.get_freezones()
    full_map = stllib.get_map()
    cache_map = filter(lambda x: x[1] < cache_end_pba, full_map)
    if not cache_map:
        return
#homa
#    cache_map.sort(lambda a,b: int(a[1] - b[1]))

    # count valid sectors in each zone and find the min
    valid_sectors = [0] * sb.cache_zones
    for lba,pba,n in cache_map:
        z = (pba - cache_pba) / zone_lbas
        valid_sectors[z] += n
    for start,end in freezones:           # free zones don't count
        z = (start - cache_pba) / zone_lbas
        valid_sectors[z] = zone_lbas + 1
    z = (wf - cache_pba) / zone_lbas      # nor does the write frontier
    valid_sectors[z] = zone_lbas + 1    
    if gc_type == 'min_sec':
        i = valid_sectors.index(min(valid_sectors))
    elif gc_type == "fifo":
        i = fifo_bnd_to_clean
    elif gc_type == "min_bnd":
        valid_bnds.clear()
        for z in range(sb.cache_zones):
            valid_bnds[z] = []
        for lba,pba,n in cache_map:
            z = (pba - cache_pba) / zone_lbas
            lg_z = lba / zone_lbas
            if lg_z not in valid_bnds[z]:
                valid_bnds[z].append(lg_z)
        for start,end in freezones:           # free zones don't count
            z = (start - cache_pba) / zone_lbas
            del valid_bnds[z][:]
            for i in range(sb.data_zones+1):
                valid_bnds[z].append(i)
        z = (wf - cache_pba) / zone_lbas      # nor does the write frontier
        del valid_bnds[z][:]
        for i in range(sb.data_zones+1):
            valid_bnds[z].append(i)
        ######################3          #nor does the mkfs info
        for i in range(9):
            del valid_bnds[i][:]
            for j in range(sb.data_zones+1):
                valid_bnds[i].append(j)
        ######################
        max_z_len = sb.data_zones
        for z in valid_bnds: #range(len(valid_bnds)):
            print "----------------- zone, # lg zones covered and # of valid_sec: ", z+2, len(valid_bnds[z]), valid_sectors[z]
            if len(valid_bnds[z]) < max_z_len: 
                max_z_len = len(valid_bnds[z])
                i = z

    lo_pba = (i+sb.ckpt_zones+conv_zones)*zone_lbas ##homa
    hi_pba = lo_pba + zone_lbas
    print 'cleaning band %d (%d-%d), wf is in band %d' % (i+2, lo_pba, hi_pba, ((wf - cache_pba) / zone_lbas) + 2)

    # if zone is completely empty, we're done. (TODO - reset write pointer)
    #
    zloc = lo_pba
    print "Reseting cache zone with no valid data-----------------------", zloc, "\n"
    if valid_sectors[i] == 0:
        print 'YAY! empty zone in cache'
        rstZPtr = subprocess.call(["sudo %s/hdparm --please-destroy-my-drive  --reset-one-write-pointer %s %s" %(hdparm_path, zloc, blkdev)], shell=True)
        stllib.put_freezone(lo_pba, hi_pba)
        stllib.put_space(zone_lbas)
        #homa
        if gc_type == "fifo":
            if fifo_bnd_to_clean == sb.cache_zones-1:
                fifo_bnd_to_clean = 0
            else:
                fifo_bnd_to_clean += 1
#       stllib.put_space(hi_pba - last_gc_pba[i]) #homa: put free spac based on free zones
#homa        print >> fp, 'in YAY1 report last_gc_pba[%d]: %d, after: %d'% (i, last_gc_pba[i], lo_pba)
        last_gc_pba[i] = lo_pba
        check_gc_pba()
        return

    # find the extent in that zone with smallest PBA, and corresponding LBA
    #
    cache_map = filter(lambda x: x[1] >= lo_pba and x[1] < hi_pba, cache_map)
    #homa
    if not cache_map:
        print "----------------------------Some thing is really WRONG---------------"
        print lo_pba, hi_pba, valid_sectors[i]
        freezones2 = stllib.get_freezones()
        for start,end in freezones2:
            print "zone at ERROR:", start, end

    min_extent = min(cache_map, key=lambda x: x[1])
    tail_lba,tail_pba = min_extent[0],min_extent[1]

    print >> fp, 'tail_lba %d pba %d' % (tail_lba, tail_pba)
    for l,p,n in cache_map:
        print >> fp, '  %d +%d %d' % (l,n,p)

    # find all extents in the same logical zone. complicated by the fact that 
    # an extent can overlap two logical zones
    #
    start,end = zone_start(tail_lba), zone_end(tail_lba)
    logical_zone = filter(lambda x: x[0] < end and (x[0]+x[2]) > start, full_map)
    trim_lbas(logical_zone, start, end)

    #homa
#    logical_zone_debug = logical_zone
#    logical_zone_debug.sort(lambda a,b: int(a[0] - b[0]))
#    print >> fp, 'log zone bef cleaning:'
#    for ext in logical_zone_debug:
#        print >> fp, '  %d +%d %d' % (ext[0],ext[2],ext[1])
#    print >> fp, '-----End for log zone bef cleaning'

    # now copy anything in the data zone to the temp zone
    #
    in_data_zone = filter(lambda x: x[1] >= cache_end_pba, logical_zone)
    n_copied = 0
    in_data_zone.sort(lambda a,b: int(a[1] - b[1]))   # order by PBA
    temp_start = cache_end_pba
   
    tmp_need_reset_flg = 0
    while in_data_zone: 
        tmp_need_reset_flg = 1
        n,reqs = 0,[]
        while in_data_zone and n < 32*1024:
            tmp = in_data_zone.pop(0)
            n += tmp[2]
            reqs.append(tmp)
        print >> fp, 'data target:', temp_start
        _t = temp_start
        for l,p,n in reqs:
            print >> fp, '  - %d +%d %d -> %d +%d %d' % (l,n,p, l,n,_t)
            _t += n
        stllib.put_target(seq, temp_start)
        stllib.put_copy(reqs)
        temp_start += stllib.doit()

    # homa 
    if tmp_need_reset_flg ==1:
        zloc = cache_end_pba    
        print "Reseting the WP of the tmp zone----------------------", zloc, "\n"
        stZPtr = subprocess.call(["sudo %s/hdparm --please-destroy-my-drive  --reset-one-write-pointer %s %s" %(hdparm_path, zloc, blkdev)], shell=True)

    print 'gather:', n_copied, 'sectors copied, LBA zone', start

    # problem - we need to sort by LBA for correctness, but PBA for efficiency
    #
    logical_zone = filter(lambda x: x[0] < end and (x[0]+x[2]) > start, stllib.get_map())
    trim_lbas(logical_zone, start, end)
    logical_zone.sort(lambda a,b: int(a[0] - b[0]))

    print 'logical_zone: %d extents %d sectors' % (len(logical_zone), 
                                                   sum([x[2] for x in logical_zone]))

    start_pba = zone_start(tail_pba)
    end_pba = zone_end(tail_pba)
    
    # FIXME - reset write pointer
    target = start + data_start
    while logical_zone:
        n,reqs = 0,[]
        while logical_zone and n < 32*1024:  #32*1024: homa
            tmp = logical_zone.pop(0)
            n += tmp[2]
            reqs.append(tmp)
        print 'writing %d = %d' % (len(reqs), n)
        print >> fp, 'target', target
        _t = target
        for l,p,n in reqs:
            print >> fp, '  %d +%d %d -> %d +%d %d' % (l,n,p,l,n,_t)
            _t += n
        stllib.put_target(seq, target)
        stllib.put_copy(reqs)
        target += stllib.doit()

        physical_zone_copied = filter(lambda x: x[1] >= start_pba and x[1] < tail_pba, reqs) 
        if physical_zone_copied:
            pba = max(map(lambda x: x[1] + x[2], physical_zone_copied))
            space = pba - last_gc_pba[i]
#homa            print 'in physical zone: last_gc_pba[%d]: %d, after: %d'% (i, last_gc_pba[i], pba)
#homa            print >> fp, 'in physical zone report last_gc_pba[%d]: %d, after: %d'% (i, last_gc_pba[i], pba)
            print " WARNING"
            last_gc_pba[i] = pba
            check_gc_pba()
            stllib.put_space(space)

#homa
#    logical_zone_debug = filter(lambda x: x[0] < end and (x[0]+x[2]) > start, stllib.get_map())
#    logical_zone_debug.sort(lambda a,b: int(a[0] - b[0]))
#    print >> fp, 'log zone aft cleaning:'
#    for ext in logical_zone_debug:
#        print >> fp, '  %d +%d %d' % (ext[0],ext[2],ext[1])
#    print >> fp, '-----End for log zone aft cleaning'

    print >> fp, 'full map:'
    for l,p,n in stllib.get_map():
        print >> fp, '  %d +%d %d' % (l,n,p)

    # todo - make this safe against skipped copies
    # now merge all the extents we copied:
    stllib.put_map([(start, start+data_start, zone_lbas)])

    print >> fp, 'trimmed map:'
    for l,p,n in stllib.get_map():
        print >> fp, '  %d +%d %d' % (l,n,p)

    # finally see how much we collected
    start,end = zone_start(tail_pba), zone_end(tail_pba)
    z = (start - cache_pba) / zone_lbas
    assert z == i
    zone_in_cache = filter(lambda x: x[1] >= start and x[1] < end, stllib.get_map())
    if not zone_in_cache:
        print 'YAY2! got one'
#        print >> fp,"in YAY2 recliam, line 356", pba, last_gc_pba[i], start, end
        space = end - last_gc_pba[i]
#        print 'in YAY2-1 last_gc_pba[%d]: %d, after: %d'% (i, last_gc_pba[i], start)
#        print >> fp, 'in YAY2-1 last_gc_pba[%d]: %d, after: %d'% (i, last_gc_pba[i], start)
        last_gc_pba[i] = start
        check_gc_pba()
        zloc = start
        print "Reseting a zone that was recently cleaned-----------------------", zloc, "\n"
        rstZPtr = subprocess.call(["sudo %s/hdparm --please-destroy-my-drive  --reset-one-write-pointer %s %s" %(hdparm_path, zloc, blkdev)], shell=True)
        stllib.put_freezone(start, end)
#        stllib.put_space(space)
        stllib.put_space(zone_lbas) #homa: put free spac based on free zones
        if gc_type == "fifo":
            if fifo_bnd_to_clean == sb.cache_zones-1:
                fifo_bnd_to_clean = 0
            else:
                fifo_bnd_to_clean += 1
    else:
        min_pba = min(map(lambda x: x[1], zone_in_cache))
#        print 'in YAY2-2 last_gc_pba[%d]: %d, after: %d'% (i, last_gc_pba[i], min_pba)
#        print >> fp, 'in YAY2-2 last_gc_pba[%d]: %d, after: %d'% (i, last_gc_pba[i], min_pba)
        if min_pba < last_gc_pba[i]:
            print >> fp, "negative"
            print 'negative', min_pba, last_gc_pba[i], i
#        print >> fp, "in YAY2-2 reclaim, line 362", min_pba, last_gc_pba[i]
        space = min_pba - last_gc_pba[i]
        last_gc_pba[i] = min_pba
        check_gc_pba()
#        stllib.put_space(space) # homa: put free spac based on free zones

    fp.close()
    
#----------------------------------------------------------------------


# now we enter our checkpoint / garbage collection loop
#
writes = stllib.get_sequence()
reads = stllib.get_reads()
total_io = writes + reads
idle_ticks = 0
write_ticks = 0

idle_free = (sb.cache_zones - 10) * sb.zone_lbas
busy_free = sb.zone_lbas

while True:
    _writes = stllib.get_sequence()
    _total_io = _writes + stllib.get_reads()
    if _total_io != total_io:
        idle_ticks = 0
        total_io = _total_io

    room = stllib.get_space()
    if 0:#room < busy_free or (idle_ticks > 15 and room < idle_free):
        print "empty space in # of LBAs", room 
        garbage_collect()
        checkpoint()
        continue

    # finally checkpoint if we need to
    n = _writes - writes
#    if n > 500 or (write_ticks > 10 and n > 100) or (write_ticks > 100 and n > 0):
    if n > 5000 or (write_ticks > 10 and n > 100) or (write_ticks > 100 and n > 0):
        checkpoint()
        write_ticks = 0
        writes = _writes
        
    time.sleep(0.5)
    idle_ticks += 1
    write_ticks += 1

exit()

try:
    time.sleep(100)
except:
    # close everything down
    subprocess.call(['/sbin/dmsetup', 'remove', tgtname])
