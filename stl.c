#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <zlib.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <setjmp.h>
#include <assert.h>
#include <signal.h>

#include "stl-u.h"

#define SB_MAGIC 0x7853544c     /* 'xSTL' */
/*               0x4c545378*/

#define SB_VERSION 1
#define PAGE_SIZE 4096

struct superblock {
    uint32_t magic;
    uint32_t version;
    uint32_t features;
    uint32_t zone_size;
    uint32_t ckpt_zones;
    uint32_t cache_zones;
    uint32_t data_zones;
    uint32_t last_modified;
};

struct _sb {
    struct superblock sb;
    char pad[1024 - sizeof(struct superblock)];
};

//static jmp_buf fail_buf; /* error handling */

int verbose;

struct smr {
    char *  devname;
    char *  ctlname;
    int64_t zone_size;          /* 64bit so we don't overflow */
    int64_t cache_start;        /* LBA */
    int64_t data_start;         /* LBA */
    int disk_fd;                /* note - O_DIRECT */
    int dm_fd;                  /* control fd */
    int64_t write_frontier;     /* LBA */
    int64_t start_ckpt;         /* LBA */
    struct superblock sb;       /* most recent copy */
};

#define SECTOR_SIZE 512
#define PAGE_SECTORS 8

int64_t zone_start(struct smr *smr, int64_t lba) {
    return lba - (lba % smr->zone_size);
}
int64_t zone_end(struct smr *smr, int64_t lba) {
    return zone_start(smr, lba) + smr->zone_size;
}
    
int64_t get_generic(struct smr *smr, enum stl_cmd cmd, enum stl_cmd rsp, uint64_t *pba)
{
    struct stl_msg m = {.cmd = cmd, .flags = 0, .lba = 0, .len = 0, .pba = 0};
    if (write(smr->dm_fd, &m, sizeof(m)) < 0)
        perror("error sending GET to kernel"), exit(1);
    if (read(smr->dm_fd, &m, sizeof(m)) < 0)
        perror("error getting response from kernel"), exit(1);
    if (m.cmd != rsp)
        printf("invalid response to %d: %d (%d)\n", cmd, m.cmd, rsp), exit(1);
    if (pba != NULL)
        *pba = m.pba;
    return m.lba;
}

int64_t get_generic_pba(struct smr *smr, enum stl_cmd cmd, enum stl_cmd rsp) {
    uint64_t pba;
    get_generic(smr, cmd, rsp, &pba);
    return pba;
}

int64_t get_write_count(struct smr *smr) { 
    return get_generic(smr, STL_GET_WC, STL_VAL_WC, NULL);
}
int64_t get_read_count(struct smr *smr) {
    return get_generic(smr, STL_GET_RC, STL_VAL_RC, NULL);
}
int64_t get_write_frontier(struct smr *smr) {
    return get_generic_pba(smr, STL_GET_WF, STL_PUT_WF);
}
void    get_tail_pointer(struct smr *smr, uint64_t *plba, uint64_t *ppba) {
    *plba = get_generic(smr, STL_GET_TAIL, STL_VAL_TAIL, ppba);
}

void generic_write(struct smr *smr, enum stl_cmd cmd, uint64_t lba, uint64_t pba, uint32_t len)
{
    struct stl_msg m = {.cmd = cmd, .flags = 0, .lba = lba, .len = len, .pba = pba};
    if (write(smr->dm_fd, &m, sizeof(m)) < 0)
        perror("error sending PUT to kernel"), exit(1);
}

#define set_write_frontier(smr, wf) generic_write(smr, STL_PUT_WF, 0, wf, 0)
#define start_ios(smr)              generic_write(smr, STL_CMD_START, 0, 0, 0)


void checkpoint_map(struct smr *smr)
{
    void *page = valloc(PAGE_SIZE);
    int bufsiz = 128*1024*sizeof(struct stl_msg); /* max 128K extents!!! */
    void *buf = calloc(bufsiz, 1);
    struct stl_msg *p = buf;

    uint64_t wf = get_write_frontier(smr);
    generic_write(smr, STL_GET_EXT, 0, 0, 0);

    int n = read(smr->dm_fd, buf, bufsiz);
    if (n < 0)
        perror("GET_EXT read error"), exit(1);
    
    printf("checkpoint at %lu: wf %lu, %lu records (inc. END)\n", smr->start_ckpt, 
           wf, n / sizeof(struct stl_msg));

    int i = n / sizeof(struct stl_msg) - 1;
    assert(p[i].cmd == STL_END);
    p[i++] = (struct stl_msg){.cmd = STL_PUT_WF, .lba = 0, .pba = wf, .len = 0};
    p[i++] = (struct stl_msg){.cmd = STL_END, .lba = 0, .pba = 0, .len = 0};

    /* If we overflow the current checkpoint zone we go to the other
     * one and write a superblock. FIXME - race condition if we crash
     * before writing the checkpoint.
     */
    int npages = (i*sizeof(struct stl_msg) + PAGE_SIZE - 1) / PAGE_SIZE;
    if ((npages+2) * PAGE_SECTORS + smr->start_ckpt > zone_end(smr, smr->start_ckpt)) {
        int64_t new_start_ckpt = (smr->start_ckpt > smr->zone_size) ? 0 : smr->zone_size;
        struct superblock *_sb = page;
        memset(page, 0, PAGE_SIZE);
        *_sb = smr->sb;
        _sb->last_modified = time(NULL);
        pwrite(smr->disk_fd, page, PAGE_SIZE, new_start_ckpt);
        smr->start_ckpt = new_start_ckpt + PAGE_SECTORS;
        printf("wrapped ckpt to %ld\n", smr->start_ckpt);
    }
    
    struct stl_header *h = page;
    memset(h, 0, PAGE_SIZE);
    h->magic = STL_HDR_MAGIC;
    h->nonce = rand();
    h->next_pba = smr->start_ckpt + (npages+1) * PAGE_SECTORS;
    h->crc32 = ~crc32(-1, (unsigned char*)h, STL_HDR_SIZE);

    lseek(smr->disk_fd, smr->start_ckpt * SECTOR_SIZE, SEEK_SET);
    write(smr->disk_fd, h, PAGE_SIZE);
    for (i = 0; i < npages; i++) {
        memcpy(page, buf + i*PAGE_SIZE, PAGE_SIZE);
        write(smr->disk_fd, page, PAGE_SIZE);
    }

    free(page);
    free(buf);

    smr->start_ckpt += (npages+1) * PAGE_SECTORS;
}


void garbage_collect(struct smr *smr, int64_t tail_lba)
{
    /* 16MB ~= 8-12 tracks = 32K LBAs */
//    int chunksz = 32768;
    int chunksz = 16 * 1024;
    int64_t zone_start = tail_lba - (tail_lba % smr->zone_size);
    int64_t lba, zone_end = zone_start + smr->zone_size;

    for (lba = zone_start; lba < zone_end; lba += chunksz) {
        printf("gather %ld\n", lba);
        generic_write(smr, STL_CMD_GATHER, lba, lba + chunksz, 0);
    }

    for (lba = zone_start; lba < zone_end; lba += chunksz) {
        printf("rewrite %ld\n", lba);
        generic_write(smr, STL_CMD_REWRITE, lba, lba + chunksz, 0);
    }

    uint64_t _lba, pba, wf;
    get_tail_pointer(smr, &_lba, &pba);
    wf = get_write_frontier(smr);
    printf("GC: tail %ld/%ld, wf %ld\n", _lba, pba, wf);
}


int _do_shutdown;

void shutdown(int sig)
{
    printf("shutting down...\n");
    _do_shutdown = 1;
}

void start_stl(int argc, char **argv)
{
    void *buf = valloc(PAGE_SIZE);
    struct _sb sb[2];
    struct smr smr;

    if (argc != 1)
        printf("usage: stl start <device>\n"), exit(1);

    memset(&smr, 0, sizeof(smr));
    smr.devname = argv[0];
    smr.ctlname = "/proc/stl0"; /* FIXME */
    
    /* read the super
     */
    if ((smr.disk_fd = open(smr.devname, O_RDWR|O_DIRECT)) < 0)
        perror("can't open device"), exit(1);

    read(smr.disk_fd, buf, PAGE_SIZE);
    memcpy(&sb[0], buf, sizeof(sb[0]));
    if (sb[0].sb.magic != SB_MAGIC || sb[0].sb.version > SB_VERSION)
        printf("bad disk format\n"), exit(1);
    
    lseek(smr.disk_fd, sb[0].sb.zone_size * 512, SEEK_SET);
    read(smr.disk_fd, buf, PAGE_SIZE);
    memcpy(&sb[1], buf, sizeof(sb[1]));
    if (sb[1].sb.magic != SB_MAGIC || sb[1].sb.version > SB_VERSION)
        printf("superblock copy is bad\n"), exit(1);

    int i =  (sb[0].sb.last_modified > sb[1].sb.last_modified) ? 0 : 1;
    smr.sb = sb[i].sb;
    
    smr.zone_size = smr.sb.zone_size;
    smr.cache_start = (int64_t)smr.zone_size * smr.sb.ckpt_zones;
    smr.data_start = smr.cache_start + smr.sb.cache_zones * smr.zone_size;
    
    smr.start_ckpt = PAGE_SECTORS + (i * smr.zone_size);
    int64_t end_ckpt = zone_end(&smr, smr.start_ckpt);
    int64_t last_valid = -1;

    /* chase down the last checkpoint. Note that checkpoints only have
     * headers. Check both magic and CRC, as CRC will match on all
     * zeros
     */
    struct stl_header *h = buf;
    for (; smr.start_ckpt < end_ckpt; smr.start_ckpt = h->next_pba) {
        pread(smr.disk_fd, buf, STL_HDR_SIZE, smr.start_ckpt * SECTOR_SIZE);
        if (h->magic != STL_HDR_MAGIC)
            break;
        uint32_t crc = h->crc32;
        h->crc32 = 0;
        uint32_t crc2 = ~crc32(-1, buf, STL_HDR_SIZE);
        if (crc != crc2)
            break;
        printf("found checkpoint at %ld\n", smr.start_ckpt);
        last_valid = smr.start_ckpt;
    }

    int64_t volume_size = smr.zone_size * sb[i].sb.data_zones;
    int64_t data_start_zone = smr.sb.ckpt_zones + smr.sb.cache_zones;
    int64_t total_zones = data_start_zone + smr.sb.data_zones;
    char cmd[256];
    sprintf(cmd, "/sbin/dmsetup create stl0 --table '0 %ld stl %s %d %d %d %d'",
            volume_size, smr.devname, (int)smr.zone_size,
            (int)smr.sb.ckpt_zones, (int)data_start_zone, (int)total_zones);
    system(cmd);

    /* read the checkpoint and feed it to the device. Save the write
     * frontier until we've chased the journal.
     */
    usleep(10000);
    smr.dm_fd = open(smr.ctlname, O_RDWR);
    uint64_t write_frontier = smr.cache_start;
    
    if (last_valid > -1) {
        struct stl_msg *p = buf;
        int j;
        
        last_valid += PAGE_SECTORS;
        while (last_valid < smr.start_ckpt) {
            pread(smr.disk_fd, buf, PAGE_SIZE, last_valid*512);
            for (j = 0; j < PAGE_SIZE / sizeof(*p); j++) {
                if (p[j].cmd == STL_PUT_WF) {
                    write_frontier = p[j].pba;
                    printf("ckpt WF: %d\n", (int)p[j].pba);
                }
                if (p[j].cmd == STL_END)
                    break;
                if (p[j].cmd == STL_PUT_EXT && verbose)
                    printf("ckpt %ld +%d -> %ld\n", (long)p[j].lba, (int)p[j].len, (long)p[j].pba);
            }
            if (write(smr.dm_fd, buf, j*sizeof(*p)) < j*sizeof(*p))
                perror("write ckpt"), exit(1);
            last_valid += 8;
        }
    }

    printf("write frontier: %lu\n", write_frontier); 
    /* now chase the journaled writes
     */
    while (1) {
        printf("."); fflush(stdout);
        if (lseek(smr.disk_fd, write_frontier*512, SEEK_SET) < 0)
            perror("lseek"), exit(1);
        if (read(smr.disk_fd, buf, STL_HDR_SIZE) < 0)
            perror("read hdr"), exit(1);
        uint32_t crc = h->crc32;
        h->crc32 = 0;
        uint32_t mycrc = crc32(-1, buf, STL_HDR_SIZE);
        int ok = ~mycrc == crc;
        if (h->magic != STL_HDR_MAGIC || !ok)
            break;
        if (h->len > 0) {
            generic_write(&smr, STL_PUT_WF, h->lba, h->pba, h->len);
            if (verbose)
                printf("jrnl: %d +%d -> %d\n", (int)h->lba, (int)h->len, (int)h->pba);
        }
        write_frontier = h->next_pba;
    }
    printf("\nwrite frontier: %lu\n", write_frontier); 
    
    /* set the write frontier and start it off.
     */
    set_write_frontier(&smr, write_frontier);
    start_ios(&smr);
    
    uint64_t writes = get_write_count(&smr);
    uint64_t reads = get_read_count(&smr);

    uint64_t total_io = writes + reads;
    int idle_tics = 0;
    int write_tics = 0;
    
    signal(SIGINT, shutdown);
    
    while (!_do_shutdown) {
        usleep(100000);
        idle_tics++;
        write_tics++;

        /* first calculate idleness - we'll use this for cleaning
         */
        uint64_t _writes = get_write_count(&smr);
        uint64_t _total_io = _writes + get_read_count(&smr);
        if (_total_io != total_io) {
            idle_tics = 0;
            total_io = _total_io;
        }

        /* now clean if we've been idle for >1.5s or not enough room
         */
        uint64_t tail_lba, tail_pba, wf;
        get_tail_pointer(&smr, &tail_lba, &tail_pba);
        wf = get_write_frontier(&smr);

        /* [....WF     T......]  or
           [   T.........WF   ] 
           ^                  ^
           cache_start        cache_end
           but note that if cache_size is empty, WF=tail
        */
        int64_t cache_size = smr.data_start - smr.cache_start;
        int64_t room = (tail_pba - wf + cache_size) % cache_size;

        /* threshold for blocking in mapper is 1.25 zones; here it's 1/2 cache
         */
        while (room != 0 && (room <= cache_size / 2 || (idle_tics > 15))) {
            printf("GC: tail_pba %ld wf %ld room %ld\n", tail_pba, wf, room);
            garbage_collect(&smr, tail_lba);
            checkpoint_map(&smr);
            get_tail_pointer(&smr, &tail_lba, &tail_pba);
            wf = get_write_frontier(&smr);
            room = (tail_pba - wf + cache_size) % cache_size;            
            idle_tics = 0;
            _writes = writes;   /* we've been checkpointed... */
        }

        /* finally, checkpoint if we've got a bunch of uncheckpointed
           extents, or if we've been idle for 10s
        */
        uint64_t diff = _writes - writes;
        if (diff > 500 || (diff > 100 && write_tics > 10) || (diff > 0 && write_tics > 100)) {
            checkpoint_map(&smr);
            fflush(stdout);
            write_tics = 0;
            writes = _writes;
        }
    }
    printf("should clean up here...\n");
    exit(0);
}

void format_cmr(int argc, char **argv)
{
    if (argc != 5) {
        printf("usage: stl formatcmr <device> <zone_lbas> <ckpt_zones>"
               " <mcache_zones> <data zones>\n");
        exit(1);
    }
    char *dev = argv[0];
    int zone_lbas = atoi(argv[1]);
    int ckpt_zones = atoi(argv[2]);
    int mcache_zones = atoi(argv[3]);
    int data_zones = atoi(argv[4]);

    void *buf = valloc(PAGE_SIZE);
    struct _sb *ptr = buf;
    int i, now = time(NULL);

    memset(buf, 0, PAGE_SIZE);

    for (i = 0; i < 4; i++) {
        ptr[i].sb.magic = SB_MAGIC;
        ptr[i].sb.version = 1;
        ptr[i].sb.zone_size = zone_lbas;
        ptr[i].sb.ckpt_zones = ckpt_zones; /* better be 2? */
        ptr[i].sb.cache_zones = mcache_zones;
        ptr[i].sb.data_zones = data_zones;
        ptr[i].sb.last_modified = now;
    }

    /* TODO: validate against disk parameters */

    /* big checkpoint for SSTL = 128K extents * 16B = 2MB = 1 track */

    int fd = open(dev, O_RDWR | O_DIRECT);
    if (fd < 0)
        perror("can't write to device"), exit(1);
    if (lseek(fd, 0, SEEK_SET) < 0)
        perror("can't seek"), exit(1);
    if (write(fd, buf, PAGE_SIZE) < sizeof(buf))
        perror("can't write"), exit(1);

    /* write 2nd superblock */
    lseek(fd, zone_lbas*512, SEEK_SET);
    ptr[0].sb.last_modified = 0;
    write(fd, buf, PAGE_SIZE);

    memset(buf, 0, PAGE_SIZE);

    /* overwrite the beginning of the checkpoint zone */
    lseek(fd, PAGE_SIZE, SEEK_SET);
    write(fd, buf, PAGE_SIZE);
    
    /* overwrite the beginning of the media cache */
    lseek(fd, zone_lbas * ckpt_zones * 512, SEEK_SET);
    write(fd, buf, PAGE_SIZE);
    
    close(fd);
}


/* 
 * stl formatcmr device zone_size ckpt_size mcache_size
 *         zone_size in LBAs, cpkt_size and mcache_size in zones
 * stl start device
 */
int main(int argc, char **argv)
{
    if (argc > 1 && !strcmp(argv[1], "formatcmr"))
        format_cmr(argc-2, argv+2);
    else if (argc > 1 && !strcmp(argv[1], "start"))
        start_stl(argc-2, argv+2);
    else
        printf("unrecognized command\n");
    return 0;
}
