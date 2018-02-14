/*
 * test a block device
 */

/*
  idea is to randomly write 4k blocks with a string "seq %d block %d"
  at the top and then go back and (a) look for them in the right
  places, and (b) look for any mis-placed ones. Repeat with
  random-sized writes as well.
*/

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include "stl-u.h"

/* usage: block-tester <device> <#lbas> <count> */

int main(int argc, char **argv)
{
    int _short = 0, verbose = 0;
    if (!strcmp(argv[1], "--short")) {
        _short = 1;
        argv++;
    }
    if (!strcmp(argv[1], "--verbose")) {
        verbose = 1;
        argv++;
    }

    int fd = open(argv[1], O_RDWR | O_DIRECT);
    long n_lbas = atoll(argv[2]);
    int count = atoi(argv[3]);
    int n_pages = n_lbas / 8;
    int *sequences = calloc(n_pages*sizeof(int), 1);
    int *seq2lba = calloc(count*sizeof(int), 1);
    int *seq2len = calloc(count * sizeof(int), 1);
    unsigned i, val, sequence = 1;
    void *pages = valloc(4*4096);

    memset(pages, 0, 4*4096);
    
    srandom(1);
    for (i = 0; i < count; i++) {
        long j, pagenum = random() % n_pages;
        int len = (random() % 4) + 1;
        if (pagenum + len > n_pages)
            continue;
        for (j = pagenum; j < pagenum + len; j++) {
            int *p = pages + (j-pagenum)*4096;
            sequences[j] = sequence;
            p[0] = sequence;
            p[1] = j*8;
        }
        if (verbose)
            printf("w %d -> %ld + %d\n", sequence, pagenum*8, len*8);
        pwrite(fd, pages, len*4096, pagenum * 4096);
        seq2lba[sequence] = pagenum;
        seq2len[sequence] = len;
        sequence++;
    }

    fflush(stdout);
    if (verbose) 
        system("/mnt/get-extents /proc/stl0");
            
    printf("sleeping...\n");
    fflush(stdout);
    sleep(30);
    printf("lba seq [lba] [seq]\n");
    memset(pages, 0, 4*4096);
    for (i = 0; i < n_pages; i++) {
        if (_short && !sequences[i])
            continue;
        if (verbose)
            printf("r %ld\n", i*8L);
        int *p = pages;
        pread(fd, pages, 4096, i*4096L);
        long seq = p[0], lba = p[1];
        if (sequences[i] != seq || seq && i*8L != lba)
            printf("r %d\t%d\t%ld\t%ld\n", i*8, sequences[i], lba, seq);
    }
    close(fd);
}
