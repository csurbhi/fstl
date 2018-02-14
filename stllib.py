#
# file:        stllib.py - python interface to dm-nstl
#
from ctypes import *
from enum import IntEnum
import os
import random

class stl_msg(Structure):
    _fields_ = [("cmd",   c_ulonglong, 8),
                ("flags", c_ulonglong, 8),
                ("lba",   c_ulonglong, 48),
                ("len",   c_ulonglong, 24),
                ("pba",   c_ulonglong, 40)]

# has to match nstl-u.h
#
class stl_cmd(IntEnum):
    NO_OP         = 0
    GET_EXT       = 1       # W: list extents
    PUT_EXT       = 2       # W: set, R: value
    GET_WF        = 3       # W: get write frontier
    PUT_WF        = 4       # W: set, R: value (pba)
    GET_SEQ       = 5       # W: write sequence # (lba)
    PUT_SEQ       = 6       # W: set, R: value (lba)
    GET_READS     = 7       # W: count of reads (lba)
    VAL_READS     = 8       # R: value (lba)
    CMD_START     = 9       # W: start processing user I/Os
    PUT_TGT       = 10
    PUT_COPY      = 11
    CMD_DOIT      = 12
    VAL_COPIED    = 13
    GET_FREEZONE  = 14      # W, return is list of PUT_FREEZONE
    PUT_FREEZONE  = 15      # lba = zone start, pba = zone_end
    GET_SPACE     = 16
    PUT_SPACE     = 17      # lba = free sectors, pba = free zones
    KILL          = 18
    END           = 19      # R: sentinel for end of list


HDR_MAGIC     = 0x4c545353 # 'SSTL'

"""
    stllib functions:
    ctl_open(/dev/...)
    ctl_close()
    ctl_rw(buf, n_out, n_in)
"""

for dir in (os.getcwd(), '/mnt'):
    try:
        stllib = CDLL(dir + "/libstl.so")
        break
    except:
        pass

assert stllib

def ctl_open(dev):
    val = stllib.ctl_open(dev)
    assert val > -1

def ctl_close():
    stllib.ctl_close()
    
def get_map():
    val = []
    n = 128 * 1024
    buf = (stl_msg * n)()
    buf[0].cmd = stl_cmd.GET_EXT
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(n))
    val = []
    for i in range(n):
        if buf[i].cmd != stl_cmd.PUT_EXT:
            break
        val.append([buf[i].lba, buf[i].pba, buf[i].len])
    return val

def put_map(map):
    n = len(map)
    buf = (stl_msg * n)()
    for i in range(n):
        buf[i].lba, buf[i].pba, buf[i].len = map[i]
        buf[i].cmd = stl_cmd.PUT_EXT
    stllib.ctl_rw(byref(buf), c_int(n), c_int(0))
    
def get_write_frontier():
    buf = stl_msg(cmd=stl_cmd.GET_WF)
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(1))
    assert n == 1 and buf.cmd == stl_cmd.PUT_WF
    return buf.pba

def put_write_frontier(wf):
    buf = stl_msg(cmd=stl_cmd.PUT_WF, pba=wf)
    stllib.ctl_rw(byref(buf), c_int(1), c_int(0))

def get_sequence():
    buf = stl_msg(cmd=stl_cmd.GET_SEQ)
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(1))
    assert n == 1 and buf.cmd == stl_cmd.PUT_SEQ
    return buf.lba

#def put_sequence(seq):
#    buf = stl_msg(cmd=stl_cmd.PUT_SEQ, seq, 0, 0)
#    stllib.ctl_rw(byref(buf), c_int(1), c_int(0))

def get_reads():
    buf = stl_msg(cmd=stl_cmd.GET_READS)
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(1))
    assert n == 1 and buf.cmd == stl_cmd.VAL_READS
    return buf.lba

def put_target(_seq, _pba):
    buf = stl_msg(cmd=stl_cmd.PUT_TGT, lba=_seq, pba=_pba)
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(0))

def put_copy(extents):
    buf = (stl_msg * len(extents))()
    i = 0
    for lba,pba,n in extents:
        buf[i].cmd = stl_cmd.PUT_COPY
        buf[i].lba,buf[i].pba,buf[i].len = lba,pba,n
        i += 1
    stllib.ctl_rw(byref(buf), c_int(len(extents)), c_int(0))

def doit():
    buf = stl_msg(cmd=stl_cmd.CMD_DOIT)
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(1))
    assert n == 1 and buf.cmd == stl_cmd.VAL_COPIED
    return buf.lba

def start_ios():
    buf = stl_msg(cmd=stl_cmd.CMD_START)
    stllib.ctl_rw(byref(buf), c_int(1), c_int(0))

def get_freezones():
    buf = (stl_msg * 200)()
    buf[0].cmd = stl_cmd.GET_FREEZONE
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(200))
    val = []
    for i in range(n):
        if buf[i].cmd != stl_cmd.PUT_FREEZONE:
            break
        val.append((buf[i].lba, buf[i].pba))
    return val

def put_freezone(_start, _end):
    buf = stl_msg(cmd=stl_cmd.PUT_FREEZONE, lba=_start, pba=_end)
    stllib.ctl_rw(byref(buf), c_int(1), c_int(0))

def get_space():
    buf = stl_msg(cmd=stl_cmd.GET_SPACE)
    n = stllib.ctl_rw(byref(buf), c_int(1), c_int(1))
    assert n == 1 and buf.cmd == stl_cmd.PUT_SPACE
    return buf.lba

def put_space(sectors):
    buf = stl_msg(stl_cmd.PUT_SPACE, lba=sectors)
    stllib.ctl_rw(byref(buf), c_int(1), c_int(0))


class stl_hdr(Structure):
    _pack_ = 1
    _fields_ = [("magic", c_uint),
                ("nonce", c_uint),
                ("crc32", c_uint),
                ("flags", c_ushort),
                ("len", c_ushort),
                ("prev_pba", c_ulonglong),
                ("next_pba", c_ulonglong),
                ("lba", c_ulonglong),
                ("pba", c_ulonglong),
                ("seq", c_uint),
                ("pad", c_char * (2048 - 2*2 - 4*4 - 4*8))]


class stl_sb(Structure):
    _fields_ = [("magic", c_uint),
                ("version", c_uint),
                ("features", c_uint),
                ("zone_lbas", c_uint),
                ("ckpt_zones", c_uint),
                ("cache_zones", c_uint),
                ("temp_zones", c_uint),
                ("data_zones", c_uint),
                ("last_modified", c_uint),
                ("_pad", c_char * (4096 - 36))]
        
"""
    dsk_open()
    dsk_close()
    dsk_read()
    dsk_write()
"""

def disk_open(name):
    val = stllib.dsk_open(name)
    assert val > -1

def disk_close():
    stllib.dsk_close()

# note that callers can access ctype fields without needing
# access to the above definitions

_version = 1
_features = 0
SB_MAGIC = 0x4c545378
PAGE_SECTORS = 8

def read_superblock(lba=0):
    sb = stl_sb()
    n = stllib.dsk_read(byref(sb), c_int(lba), c_int(1))
    assert n == 1
    if sb.magic != SB_MAGIC or sb.version > _version or sb.features != 0:
        return None
    return sb

def write_superblock(sb, band):
    stllib.dsk_write(byref(sb), band * sb.zone_lbas, 1)
    stllib.dsk_sync()

def read_hdr(lba):
    hdr = stl_hdr()
    n = stllib.dsk_read(byref(hdr), lba, 4)
    if n < 0 or not stllib.hdr_check(byref(hdr)):
        return None
    return hdr

def write_hdr(lba, hdr):
    if type(hdr) is not stl_hdr:
        hdr = stl_hdr(HDR_MAGIC, 0, 0, 0, hdr.len, hdr.prev_pba, hdr.next_pba,
                      hdr.lba, hdr.pba, hdr.seq)
    hdr.magic = HDR_MAGIC
    hdr.nonce = random.randint(0, 0xFFFFFFFF)
    stllib.hdr_sign(byref(hdr))
    n = stllib.dsk_write(byref(hdr), c_int(lba), c_int(8))
    assert n >= 0
    stllib.dsk_sync()

msgs_per_pg = 4096/16

def read_map(lba, pages):
    _map = []
    buf = (stl_msg * msgs_per_pg)()
    for i in range(pages):
        assert stllib.dsk_read(byref(buf), c_uint(lba), c_uint(8)) == 8
        for j in range(msgs_per_pg):
            if buf[j].cmd != stl_cmd.PUT_EXT:
                break
            _map.append((buf[j].lba, buf[j].pba, buf[j].len))
            lba += 8
    return _map

def write_map(lba, _map):
    buf = (stl_msg * msgs_per_pg)()
    while _map:
        __map = _map[0:msgs_per_pg]
        _map = _map[msgs_per_pg:]
        i = 0
        for tuple in __map:
            buf[i].cmd = stl_cmd.PUT_EXT
            buf[i].lba, buf[i].pba, buf[i].len = tuple
            i += 1
        while i < msgs_per_pg:
            buf[i].cmd = stl_cmd.NO_OP
            i += 1
        stllib.dsk_write(byref(buf), c_uint(lba), c_uint(8))
        lba += 8

# returns (write_frontier, (freelist))
def read_ckpt(lba):
    buf = (stl_msg * msgs_per_pg)()
    n = stllib.dsk_read(byref(buf), c_uint(lba), c_uint(8))
    assert n == 8
    msgs = [buf[i] for i in range(msgs_per_pg)]
    WFs = filter(lambda m: m.cmd == stl_cmd.PUT_WF, msgs)
    FZs = filter(lambda m: m.cmd == stl_cmd.PUT_FREEZONE, msgs)
    if not WFs or not FZs:
        return (None, [])
    return (WFs[0].pba, [(m.lba, m.pba) for m in FZs])

def write_ckpt(lba, wfs, fzs):
    buf = (stl_msg * msgs_per_pg)()
    i = 0
    for wf in wfs:
        buf[i].cmd = stl_cmd.PUT_WF
        buf[i].pba = wf
        i += 1
    for fz in fzs:
        buf[i].cmd = stl_cmd.PUT_FREEZONE
        buf[i].lba, buf[i].pba = fz
        i += 1
    stllib.dsk_write(byref(buf), c_uint(lba), c_uint(8))
    
def spoil(lba):
    buf = (c_char * 4096)()
    stllib.dsk_write(byref(buf), c_uint(lba), c_uint(8))
    
