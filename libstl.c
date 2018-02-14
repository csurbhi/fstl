/*
 * file:        libstl.c
 * description: library interface to dm-nstl
 *
 * Copyright (C) 2016 Peter Desnoyers. All rights reserved.
 *
 * This file is released under the GPL.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <zlib.h>

#include "nstl-u.h"

int ctl_fd = -1;

int ctl_open(char *name)
{
    return ctl_fd = open(name, O_RDWR);
}

int ctl_close(void)
{
    close(ctl_fd);
    ctl_fd = -1;
    return 0;
}

int ctl_rw(struct stl_msg *buf, int n_out, int n_in)
{
    int n;
    if (n_out > 0 && write(ctl_fd, buf, n_out * sizeof(*buf)) < 0)
        perror("control write error"), exit(1);
    if (n_in > 0 && (n = read(ctl_fd, buf, n_in * sizeof(*buf))) < 0)
        perror("control read error"), exit(1);
    return n / sizeof(*buf);
}

int dsk_fd = -1;

int dsk_open(char *name)
{
    return dsk_fd = open(name, O_RDWR);
}

int dsk_close(void)
{
    close(dsk_fd);
    dsk_fd = -1;
    return 0;
}
       
int dsk_read(void *buf, int lba, int nsectors)
{
    int val = lseek(dsk_fd, (off_t)lba * 512, SEEK_SET);
    if (val > -1)
        val = read(dsk_fd, buf, (size_t)nsectors * 512);
    if (val < 0)
        perror("disk read error"), exit(1);
    return val / 512;
}

int dsk_write(void *buf, int lba, int nsectors)
{
    int val = lseek(dsk_fd, (off_t)lba * 512, SEEK_SET);
    if (val > -1)
        val = write(dsk_fd, buf, (size_t)nsectors * 512);
    if (val < 0)
        perror("disk write error"), exit(1);
    return val / 512;
}

int dsk_sync(void)
{
    return fsync(dsk_fd);
}

int hdr_check(struct stl_header *h)
{
    if (h->magic != STL_HDR_MAGIC)
        return 0;
    
    uint32_t crc = h->crc32;
    h->crc32 = 0;
    uint32_t crc2 = ~crc32(-1, (unsigned char*)h, STL_HDR_SIZE);
    h->crc32 = crc;
    return crc == crc2;
}

int hdr_sign(struct stl_header *h)
{
    h->crc32 = 0;
    h->crc32 = ~crc32(-1, (unsigned char*)h, STL_HDR_SIZE);
    return 0;
}

