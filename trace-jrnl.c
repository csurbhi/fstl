#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <zlib.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>

#include "nstl-u.h"

/* test <device> <startpba> */
int main(int argc, char **argv)
{
    int fd = open(argv[1], O_RDONLY|O_DIRECT);
    char *buf = valloc(4096);
    struct stl_header *p = (void*)buf;
    uint64_t pba = atoll(argv[2]);
    int64_t last_seq = -1;

    while (1) {
        if (lseek(fd, pba*512, SEEK_SET) < 0)
            perror("lseek"), exit(1);
        if (read(fd, buf, STL_HDR_SIZE) < 0)
            perror("read"), exit(1);
        uint32_t crc = p->crc32;
        p->crc32 = 0;
        uint32_t mycrc = crc32(-1, buf, STL_HDR_SIZE);
        int ok = ~mycrc == crc;
        char mg[32];
        sprintf(mg, "0x%08x", p->magic);
        char *magic = (p->magic == STL_HDR_MAGIC ?
                       "HDR" : (p->magic == 0x4c545378 ? "SB" : mg));
        printf("%lu: magic %s crc %s seq %u prev %lu next %lu len %u lba %lu pba %lu\n",
               pba, magic, ok ? "OK" : "BAD", p->seq, p->prev_pba, p->next_pba, 
               p->len, p->lba, p->pba);
        if (p->magic != STL_HDR_MAGIC || !ok)
            break;
        if (last_seq >= 0 && p->seq < last_seq)
            break;
        last_seq = p->seq;
        pba = p->next_pba;
    }
    close(fd);

    return 0;
}

    
    
