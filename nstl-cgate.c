/*
 * file:        nstl-cgate.c
 * description: Seagate-like media cache-based STL
 *
 * Copyright (C) 2016 Peter Desnoyers. All rights reserved.
 *
 * This file is released under the GPL.
 */

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

#include "nstl-u.h"

#define SB_MAGIC 0x4c545378     /* 'xSTL' */

/* note that these work for non-power-of-2 values */
#define round_down(x, y) ((x) - (x)%(y))
#define round_up(x, y) round_down((x)+(y)-1, (y))


#define SB_VERSION 1
#define PAGE_SIZE 4096

struct superblock {
    uint32_t magic;
    uint32_t version;
    uint32_t features;
    uint32_t zone_lbas;
    uint32_t ckpt_zones;
    uint32_t cache_zones;
    uint32_t temp_zones;        /* copy of data zone during cleaning */
    uint32_t data_zones;
    uint32_t last_modified;
};

struct _sb {
    struct superblock sb;
    char pad[1024 - sizeof(struct superblock)];
};

int log_fd = -1;
void log_(int code, void *data, int len)
{
    int x[] = {code, len};
    if (log_fd < 0)
        log_fd = open("/home/pjd/data.log", O_WRONLY | O_CREAT, 0777);
    write(log_fd, x, sizeof(x));
    write(log_fd, data, len);
}

//static jmp_buf fail_buf; /* error handling */

int verbose;

struct smr {
    char *  devname;
    char    ctlname[32];
    int64_t zone_lbas;          /* 64bit so we don't overflow */
    int64_t cache_start;        /* LBA */
    int64_t temp_start;         /* LBA, staging for data zones */
    int64_t data_start;         /* LBA */
    int64_t data_end;
    int disk_fd;                /* note - O_DIRECT */
    int dm_fd;                  /* control fd */
    int64_t write_frontier;     /* LBA */
    int64_t start_ckpt;         /* LBA */
    struct superblock sb;       /* most recent copy */
    int64_t *last_gc_pba;       /* per cache zone */
};

#define SECTOR_SIZE 512
#define PAGE_SECTORS 8

int64_t zone_start(struct smr *smr, int64_t lba) {
    return lba - (lba % smr->zone_lbas);
}
int64_t zone_end(struct smr *smr, int64_t lba) {
    return zone_start(smr, lba) + smr->zone_lbas;
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

int64_t get_sequence(struct smr *smr) { 
    return get_generic(smr, STL_GET_SEQ, STL_PUT_SEQ, NULL);
}
int64_t get_read_count(struct smr *smr) {
    return get_generic(smr, STL_GET_READS, STL_VAL_READS, NULL);
}
int64_t get_write_frontier(struct smr *smr) {
    return get_generic_pba(smr, STL_GET_WF, STL_PUT_WF);
}
int64_t get_free_sectors(struct smr *smr) {
    return get_generic(smr, STL_GET_SPACE, STL_PUT_SPACE, NULL);
}
int64_t get_free_zones(struct smr *smr) {
    return get_generic_pba(smr, STL_GET_SPACE, STL_PUT_SPACE);
}

void generic_write(struct smr *smr, enum stl_cmd cmd, uint64_t lba, uint64_t pba, uint32_t len)
{
    struct stl_msg m = {.cmd = cmd, .flags = 0, .lba = lba, .len = len, .pba = pba};
    if (write(smr->dm_fd, &m, sizeof(m)) < 0)
        perror("error sending PUT to kernel"), exit(1);
}

#define set_write_frontier(smr, wf) generic_write(smr, STL_PUT_WF, 0, wf, 0)
#define start_ios(smr)              generic_write(smr, STL_CMD_START, 0, 0, 0)

/* helper routines for sorting lists of extents
 */
int cmp_lba(const void *p1, const void *p2)
{
    const struct stl_msg *m1 = p1, *m2 = p2;
    return (m1->lba < m2->lba) ? -1 : 1;
}
int cmp_pba(const void *p1, const void *p2)
{
    const struct stl_msg *m1 = p1, *m2 = p2;
    return (m1->pba < m2->pba) ? -1 : 1;
}

/* Get the entire map. 
 * allocates aligned buffer, caller must free
 */
struct stl_msg *get_map(struct smr *smr, int *p_nmsgs)
{
    int i, bufsiz = 128*1024*sizeof(struct stl_msg); /* max 128K extents!!! */
    void *buf = valloc(bufsiz);
    memset(buf, 0, bufsiz);

    generic_write(smr, STL_GET_EXT, 0, 0, 0);
    int n = read(smr->dm_fd, buf, bufsiz);
    if (n < 0)
        perror("GET_EXT read error"), exit(1);

    struct stl_msg *m = buf;
    for (i = 0; i < n / sizeof(struct stl_msg); i++)
        if (m[i].cmd != STL_PUT_EXT)
            break;
    *p_nmsgs = i;
    return buf;
}

/* 10TB / 256MB ~= 40,000 zones = 640K
 * allocates aligned buffer, caller must free
 */
struct stl_msg *get_freelist(struct smr *smr, int *p_nmsgs)
{
    int i, bufsiz = 64 * 1024 * sizeof(struct stl_msg);
    void *buf = valloc(bufsiz);
    memset(buf, 0, bufsiz);

    generic_write(smr, STL_GET_FREEZONE, 0, 0, 0);
    int n = read(smr->dm_fd, buf, bufsiz);
    if (n < 0)
        perror("GET_FREE read error"), exit(1);
    struct stl_msg *m = buf;
    for (i = 0; i < n / sizeof(struct stl_msg); i++)
        if (m[i].cmd != STL_PUT_FREEZONE)
            break;
    *p_nmsgs = i;
    return buf;
}

void checkpoint_map(struct smr *smr)
{
    void *page = valloc(PAGE_SIZE);
    uint64_t wf = get_write_frontier(smr);
    uint64_t seq = get_sequence(smr);
    int n_map, n_zone;
    struct stl_msg *map = get_map(smr, &n_map);
    struct stl_msg *zones = get_freelist(smr, &n_zone);
    
    printf("checkpoint at %lu: wf %lu, %d free zones, %d records (inc. END)\n",
           smr->start_ckpt, wf, n_zone, n_map);

    /* note that proc_write ignores STL_END so we can leave it in. One
     * page for the write frontier + frees sectors, N for the map, M
     * for the freelist. 
     *
     * If we overflow the current checkpoint zone we go to the other
     * one and write a superblock.
     * FIXME - race condition if we crash before writing the
     * checkpoint, need (1) a trailer to indicate done-ness, and (2)
     * backtrack when locating checkpoint
     */
    int n_zone_pages = round_up(n_zone * sizeof(struct stl_msg), PAGE_SIZE) / PAGE_SIZE;
    int n_map_pages = round_up(n_map * sizeof(struct stl_msg), PAGE_SIZE) / PAGE_SIZE;
    int npages = 1 + n_zone_pages + n_map_pages;

    if ((npages+2) * PAGE_SECTORS + smr->start_ckpt > zone_end(smr, smr->start_ckpt)) {
        int64_t new_start_ckpt = (smr->start_ckpt > smr->zone_lbas) ? 0 : smr->zone_lbas;
        struct superblock *_sb = page;
        memset(page, 0, PAGE_SIZE);
        *_sb = smr->sb;
        _sb->last_modified = time(NULL);
        pwrite(smr->disk_fd, page, PAGE_SIZE, new_start_ckpt);
        smr->start_ckpt = new_start_ckpt + PAGE_SECTORS;
        printf("wrapped ckpt to %ld\n", smr->start_ckpt);
    }

    /* write the header
     */
    struct stl_header *h = page;
    memset(h, 0, PAGE_SIZE);
    h->magic = STL_HDR_MAGIC;
    h->nonce = rand();
    h->next_pba = smr->start_ckpt + (npages+1) * PAGE_SECTORS;
    h->seq = seq;
    h->crc32 = ~crc32(-1, (unsigned char*)h, STL_HDR_SIZE);
    lseek(smr->disk_fd, smr->start_ckpt * SECTOR_SIZE, SEEK_SET);
    write(smr->disk_fd, h, PAGE_SIZE);

    /* then the map and free zones
     */
    write(smr->disk_fd, (void*)map, n_map_pages * PAGE_SIZE);
    write(smr->disk_fd, (void*)zones, n_zone_pages * PAGE_SIZE);

    /* and finally the write frontier and free sectors
     */
    memset(page, 0, PAGE_SIZE);
    struct stl_msg *m = page;
    m[0] = (struct stl_msg){.cmd = STL_PUT_WF, .lba = 0, .pba = wf, .len = 0};
    m[1] = (struct stl_msg){.cmd = STL_PUT_SPACE, .lba = get_free_sectors(smr),
                            .pba = 0, .len = 0};
    write(smr->disk_fd, page, PAGE_SIZE);

    free(page);
    free(map);
    free(zones);

    smr->start_ckpt += (npages+1) * PAGE_SECTORS;
}


struct stl_msg *copy_map(struct stl_msg *map, int mapsz)
{
    struct stl_msg *copy = malloc(mapsz * sizeof(*copy));
    memcpy(copy, map, mapsz * sizeof(*copy));
    return copy;
}

/* remove any entries with PBAs in range [zone_start..zone_end)
 */
int remove_pbas(struct smr *smr, int64_t zone_start, int64_t zone_end,
               struct stl_msg *map, int mapsz)
{
    int i, j;
    for (i = j = 0; i < mapsz; i++)
        if (map[i].pba + map[i].len <= zone_start || map[i].pba >= zone_end)
            map[j++] = map[i];
    return j;
}
int keep_pbas(struct smr *smr, int64_t zone_start, int64_t zone_end,
               struct stl_msg *map, int mapsz)
{
    mapsz = remove_pbas(smr, 0, zone_start, map, mapsz);
    return remove_pbas(smr, zone_end, smr->data_end, map, mapsz);
}

/* remove any entries with LBAs in range [zone_start..zone_end)
 */
int remove_lbas(struct smr *smr, int64_t zone_start, int64_t zone_end,
               struct stl_msg *map, int mapsz)
{
    int i, j;
    for (i = j = 0; i < mapsz; i++)
        if (map[i].lba + map[i].len <= zone_start || map[i].lba >= zone_end)
            map[j++] = map[i];
    return j;
}

/* note that this keeps extents that cross the zone boundary.
 */
int keep_lbas(struct smr *smr, int64_t zone_start, int64_t zone_end,
               struct stl_msg *map, int mapsz)
{
    mapsz = remove_lbas(smr, 0, zone_start, map, mapsz);
    return remove_lbas(smr, zone_end, smr->data_end, map, mapsz);
}

/* this fixes that.
 */
void trim_lbas(int64_t start, int64_t end, struct stl_msg *map, int mapsz)
{
    int i;
    for (i = 0; i < mapsz; i++) {
        int64_t offset = start - map[i].lba;
        if (offset > 0) {
            map[i].lba += offset;
            map[i].pba += offset;
            map[i].len -= offset;
        }
        int64_t trim = (map[i].lba + map[i].len) - end;
        if (trim > 0) {
            map[i].len -= trim;
        }
    }
}

void put_free_zone(struct smr *smr, int64_t pba)
{
    int i, n, free_zones[100];
    struct stl_msg *zones = get_freelist(smr, &n);
    memset(free_zones, 0, sizeof(free_zones));
    for (i = 0; i < n; i++)
        free_zones[zones[i].lba / smr->zone_lbas] = 1;
    assert(free_zones[pba/smr->zone_lbas] == 0);
    generic_write(smr, STL_PUT_FREEZONE, pba, pba + smr->zone_lbas, 0);
    free(zones);
}

/* find the tail LBA and copy any of its data extents to temp
   space. Returns the LBA for the logical zone which has been copied.
 */
int64_t collect_one_zone(struct smr *smr)
{
    int64_t seq = get_sequence(smr);

    /* get the full map, then make a copy with only those entries in
     * cache, not counting the write frontier.
     */
    int i, o_mapsz;
    struct stl_msg *orig_map = get_map(smr, &o_mapsz);
    log_(__LINE__, orig_map, o_mapsz*sizeof(*orig_map));
    struct stl_msg *cache_map = copy_map(orig_map, o_mapsz);
    int c_mapsz = keep_pbas(smr, smr->cache_start, smr->temp_start, cache_map, o_mapsz);
    log_(__LINE__, cache_map, c_mapsz*sizeof(*cache_map));
    
    /* if cache is empty, don't do any cleaning.
     */
    if (c_mapsz == 0) {
        free(orig_map);
        free(cache_map);
        return -1;
    }

    int64_t wf = get_write_frontier(smr);
// irrelevant    c_mapsz = remove_pbas(smr, zone_start(smr, wf), zone_end(smr, wf), cache_map, c_mapsz);

    /* how many valid sectors in each zone?
     */
    int n, nzones = (smr->temp_start - smr->cache_start) / smr->zone_lbas;
    int *valid_sectors = calloc(nzones * sizeof(int), 1);

    for (i = 0; i < c_mapsz; i++) {
        int z = (cache_map[i].pba - smr->cache_start) / smr->zone_lbas;
        valid_sectors[z] += (cache_map[i].len + 8);
    }

    /* if it's on the free list, take it out of the running so we
     * don't try to clean it 
     */
    struct stl_msg *zones = get_freelist(smr, &n);
    log_(__LINE__, zones, n*sizeof(*zones));
    for (i = 0; i < n && zones[i].cmd == STL_PUT_FREEZONE; i++) {
        int z = (zones[i].lba - smr->cache_start) / smr->zone_lbas;
        valid_sectors[z] += smr->zone_lbas;
    }
    int wf_zone = (wf - smr->cache_start) / smr->zone_lbas;
    valid_sectors[wf_zone] += smr->zone_lbas;
    free(zones);
    log_(__LINE__, valid_sectors, nzones * sizeof(int));

    /* now find the greedy choice
     */
    int min_i = 0, min_valid = smr->zone_lbas;
    for (i = 0; i < nzones; i++)
        if (valid_sectors[i] < min_valid) {
            min_i = i;
            min_valid = valid_sectors[i];
        }
    int64_t min_p_zone = smr->cache_start + min_i * smr->zone_lbas;
    log_(__LINE__, &min_p_zone, sizeof(min_p_zone));

    /* if there's an empty physical zone in cache, just feed it back
     * and return -1 to indicate no re-write needed.
     * to temp.
     */
    if (min_valid == 0) {
        printf("YAY! empty zone %ld in cache\n", min_p_zone);
        free(cache_map);
        free(orig_map);
        /* reset write pointer */
        //generic_write(smr, STL_PUT_FREEZONE, min_p_zone, min_p_zone + smr->zone_lbas, 0);
        put_free_zone(smr, min_p_zone);
        generic_write(smr, STL_PUT_SPACE, smr->zone_lbas, 0, 0);
        int cache_zone = (min_p_zone - smr->cache_start) / smr->zone_lbas;
        smr->last_gc_pba[cache_zone] = smr->cache_start + cache_zone * smr->zone_lbas;
        return -1;
    }
    
    /* find the first map entry in that zone. We don't need the cache
     * map after this.
     */
    c_mapsz = keep_pbas(smr, min_p_zone, min_p_zone + smr->zone_lbas, cache_map, c_mapsz);
    qsort(cache_map, c_mapsz, sizeof(struct stl_msg), cmp_pba);
    int64_t tail_lba = cache_map[0].lba, tail_pba = cache_map[0].pba;
    log_(__LINE__, cache_map, c_mapsz*sizeof(*cache_map));

    /* now get the list of extents that have to be copied, keep only
     * the data zone ones, and trim them to fit the zone.
     */
    int64_t start = zone_start(smr, tail_lba), end = zone_end(smr, tail_lba);
    o_mapsz = keep_lbas(smr, start, end, orig_map, o_mapsz);
    o_mapsz = keep_pbas(smr, smr->data_start, smr->data_end, orig_map, o_mapsz);
    trim_lbas(start, end, orig_map, o_mapsz);
    log_(__LINE__, orig_map, o_mapsz*sizeof(*orig_map));

    /* and let's just copy them all at once.
     */
    generic_write(smr, STL_PUT_TGT, seq, smr->temp_start, 0);
    for (i = 0; i < o_mapsz; i++) {
        orig_map[i].cmd = STL_PUT_COPY;
    }
    write(smr->dm_fd, orig_map, o_mapsz * sizeof(struct stl_msg));
    int64_t sectors_copied = get_generic(smr, STL_CMD_DOIT, STL_VAL_COPIED, NULL);

    printf("gather: %ld sectors copied, LBA zone %ld\n", sectors_copied, start);

    /* how many sectors did we free up? Tell the device mapper.
     */
    int nf, cache_zone = (zone_start(smr, tail_pba) - smr->cache_start) / smr->zone_lbas;
    int64_t freed_sectors = zone_end(smr, tail_pba) - smr->last_gc_pba[cache_zone];
    struct stl_msg *final_map = get_map(smr, &nf);
    log_(__LINE__, final_map, nf*sizeof(*final_map));
    nf = keep_lbas(smr, zone_start(smr, tail_lba), zone_end(smr, tail_lba), final_map, nf);
    nf = keep_pbas(smr, zone_start(smr, tail_pba), zone_end(smr, tail_pba), final_map, nf);
    qsort(cache_map, c_mapsz, sizeof(struct stl_msg), cmp_pba);
    if (c_mapsz > 0) {
        int64_t phys_end = cache_map[0].pba + cache_map[0].len + 4;
        freed_sectors = phys_end - smr->last_gc_pba[cache_zone];
        if (freed_sectors > 10000)
            printf("freed_sectors: %d\n", (int) freed_sectors);
        assert(freed_sectors > 0);
        smr->last_gc_pba[cache_zone] = phys_end;
    }
    free(cache_map);
    free(orig_map);
    free(final_map);
    
    generic_write(smr, STL_PUT_SPACE, freed_sectors, 0, 0);
    printf("put space: %ld\n", freed_sectors);
    
    return start;
}


void rewrite_one_zone(struct smr *smr, int64_t start, int64_t end)
{
    int64_t seq = get_sequence(smr);

    /* get all the extents in this zone
     */
    int i, mapsz;
    struct stl_msg *map = get_map(smr, &mapsz);
    mapsz = keep_lbas(smr, start, end, map, mapsz);
    trim_lbas(start, end, map, mapsz);

    /* and now we copy them
     */
    int64_t data_zone = smr->data_start + start;
    generic_write(smr, STL_PUT_TGT, seq, data_zone, 0);
    for (i = 0; i < mapsz; i++) {
        map[i].cmd = STL_PUT_COPY;
    }
    write(smr->dm_fd, map, mapsz * sizeof(struct stl_msg));
    int64_t sectors_copied = get_generic(smr, STL_CMD_DOIT, STL_VAL_COPIED, NULL);

    printf("rewrite: %ld sectors copied, LBA zone %ld\n", sectors_copied, start);

    /* what cache zone were we cleaning? (do this just before throwing
     * away the original map)
     */
    mapsz = keep_pbas(smr, smr->cache_start, smr->temp_start, map, mapsz);
    assert(mapsz > 0);
    int64_t cache_pba = zone_start(smr, map[0].pba);
    int cache_zone = (cache_pba - smr->cache_start) / smr->zone_lbas;
    
    free(map);

    /* Get the new map, check if we freed the entire zone. If so, give
     * it back immediately. 
     */
    map = get_map(smr, &mapsz);
    mapsz = keep_lbas(smr, start, end, map, mapsz);
    mapsz = keep_pbas(smr, smr->cache_start, smr->temp_start, map, mapsz);
    if (mapsz == 0) {
        //generic_write(smr, STL_PUT_FREEZONE, cache_pba, zone_end(smr, cache_pba), 0);
        put_free_zone(smr, cache_pba);
        smr->last_gc_pba[cache_zone] = smr->cache_start + cache_zone * smr->zone_lbas;
    }
    free(map);
    
    /* now merge all those extents that we copied.
     */
    generic_write(smr, STL_PUT_EXT, start, smr->data_start + start, smr->zone_lbas);
}

void garbage_collect(struct smr *smr)
{
    int64_t tail_lba = collect_one_zone(smr);
    if (tail_lba != -1)
        rewrite_one_zone(smr, zone_start(smr, tail_lba), zone_end(smr, tail_lba));
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

    if (argc != 2)
        printf("usage: stl start <name> <device>\n"), exit(1);

    memset(&smr, 0, sizeof(smr));
    sprintf(smr.ctlname, "/dev/stl/%s", argv[0]);
    smr.devname = argv[1];
    
    /* read the super
     */
    if ((smr.disk_fd = open(smr.devname, O_RDWR|O_DIRECT)) < 0)
        perror("can't open device"), exit(1);

    read(smr.disk_fd, buf, PAGE_SIZE);
    memcpy(&sb[0], buf, sizeof(sb[0]));
    if (sb[0].sb.magic != SB_MAGIC || sb[0].sb.version > SB_VERSION)
        printf("bad disk format\n"), exit(1);
    
    lseek(smr.disk_fd, sb[0].sb.zone_lbas * 512, SEEK_SET);
    read(smr.disk_fd, buf, PAGE_SIZE);
    memcpy(&sb[1], buf, sizeof(sb[1]));
    if (sb[1].sb.magic != SB_MAGIC || sb[1].sb.version > SB_VERSION)
        printf("superblock copy is bad\n"), exit(1);

    int j, i =  (sb[0].sb.last_modified > sb[1].sb.last_modified) ? 0 : 1;
    smr.sb = sb[i].sb;
    
    smr.zone_lbas = smr.sb.zone_lbas;
    smr.cache_start = smr.zone_lbas * smr.sb.ckpt_zones;
    smr.temp_start = smr.cache_start + smr.zone_lbas * smr.sb.cache_zones;
    smr.data_start = smr.temp_start + smr.zone_lbas * smr.sb.temp_zones;
    smr.data_end = smr.data_start + smr.zone_lbas * smr.sb.data_zones;
    
    smr.start_ckpt = PAGE_SECTORS + (i * smr.zone_lbas);
    int64_t end_ckpt = zone_end(&smr, smr.start_ckpt);
    int64_t last_valid = -1;

    smr.last_gc_pba = malloc(smr.sb.cache_zones * sizeof(int64_t));
    for (j = 0; j < smr.sb.cache_zones; j++)
        smr.last_gc_pba[j] = smr.cache_start + j * smr.zone_lbas;
    
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

    if (last_valid == -1)
        printf("disk corrupted - no checkpoint\n"), exit(1);
    
    int64_t volume_size = smr.zone_lbas * sb[i].sb.data_zones;
    char cmd[256];
    sprintf(cmd, "/sbin/dmsetup create %s --table '0 %ld nstl %s %s %d %ld'",
            argv[0], volume_size, smr.devname, argv[0], (int)smr.zone_lbas, smr.data_end);
    system(cmd);

    /* read the checkpoint and feed it to the device. Save the write
     * frontier until we've chased the journal.
     */
    usleep(10000);
    smr.dm_fd = open(smr.ctlname, O_RDWR);
    uint64_t write_frontier = smr.cache_start;
    
    struct stl_msg *p = buf;
        
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
            generic_write(&smr, STL_PUT_EXT, h->lba, h->pba, h->len);
            if (verbose)
                printf("jrnl: %d +%d -> %d\n", (int)h->lba, (int)h->len, (int)h->pba);
        }
        /* if we wrap around a zone, make sure we pull it from the
         * free zone list. 
         */
//        if (zone_start(&smr, write_frontier) != zone_start(&smr, h->next_pba)) {
//            generic_write(&smr, STL_PULL_FREEZONE, zone_start(&smr, write_frontier),
//                          zone_end(&smr, write_frontier), 0);
//        }
        write_frontier = h->next_pba;
    }
    printf("\nwrite frontier: %lu\n", write_frontier); 
    
    /* set the write frontier and start it off.
     */
    set_write_frontier(&smr, write_frontier);
    start_ios(&smr);
    
    uint64_t writes = get_sequence(&smr);
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
        uint64_t _writes = get_sequence(&smr);
        uint64_t _total_io = _writes + get_read_count(&smr);
        if (_total_io != total_io) {
            idle_tics = 0;
            total_io = _total_io;
        }

        /* now clean if we've been idle for >1.5s or not enough room
         */
        int64_t room, idle_free = (smr.temp_start - smr.cache_start - 2*smr.zone_lbas);
        while (((room = get_free_sectors(&smr)) < smr.zone_lbas) ||
               (idle_tics > 15 && room < idle_free)) {
            printf("GC: free sectors %ld (tics %d)\n", room, idle_tics);
            garbage_collect(&smr);
            checkpoint_map(&smr);
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

struct stl_msg stl_msg(int cmd, int64_t lba, int64_t pba, int len)
{
    struct stl_msg m = {.cmd = cmd, .lba = lba, .pba = pba, .len = len, .flags = 0};
    return m;
}

/*
 */
void format_cmr(int argc, char **argv)
{
    if (argc != 5) {
        printf("usage: stl formatcmr <device> <zone_lbas> <ckpt_zones>"
               " <mcache_zones> <data zones>\n");
        exit(1);
    }
    char *dev = argv[0];
    long zone_lbas = atoi(argv[1]);
    long ckpt_zones = atoi(argv[2]);
    long mcache_zones = atoi(argv[3]);
    long data_zones = atoi(argv[4]);
    
    void *buf = valloc(PAGE_SIZE);
    struct _sb *ptr = buf;
    int i, j, now = time(NULL);

    memset(buf, 0, PAGE_SIZE);

    for (i = 0; i < 4; i++) {
        ptr[i].sb.magic = SB_MAGIC;
        ptr[i].sb.version = 1;
        ptr[i].sb.zone_lbas = zone_lbas;
        ptr[i].sb.ckpt_zones = ckpt_zones; /* better be 2? */
        ptr[i].sb.cache_zones = mcache_zones;
        ptr[i].sb.temp_zones = 2;
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


    /* write a checkpoint with free zones = (media cache - first MC
     * zone), data zone mapping with one extent per zone, and a
     * write pointer at the beginning of the first media cache zone.
     * [0: sb] [1: hdr] [2: wf+freelist] [3: data extents...] [x: next]
     */
    int exts_per_pg = PAGE_SIZE / sizeof(struct stl_msg);
    int n_extent_pgs = round_up(data_zones, exts_per_pg) / exts_per_pg;
    int next_page = 1 + 1 + 1 + n_extent_pgs;
    
    struct stl_header *h = buf;
    memset(h, 0, PAGE_SIZE);
    h->magic = STL_HDR_MAGIC;
    h->nonce = rand();
    h->next_pba = next_page * PAGE_SECTORS;
    h->crc32 = ~crc32(-1, (unsigned char*)h, STL_HDR_SIZE);

    lseek(fd, PAGE_SIZE, SEEK_SET);
    write(fd, buf, PAGE_SIZE);

    memset(buf, 0, PAGE_SIZE);
    struct stl_msg *m = buf;
    for (i = 0; i < mcache_zones; i++) {
        if (i == 0) {
            m[i] = stl_msg(STL_PUT_WF, 0, zone_lbas * ckpt_zones, 0);
        } else {
            m[i] = stl_msg(STL_PUT_FREEZONE, (ckpt_zones + i) * zone_lbas,
                           (ckpt_zones + i+1) * zone_lbas, 0);
        }
    }
    m[i++] = stl_msg(STL_PUT_SPACE, zone_lbas * mcache_zones, 0, 0);
    
    write(fd, buf, PAGE_SIZE);
    
    /* write out one extent per data zone. 
     */
    for (i = 0; i < n_extent_pgs; i++) {
        memset(buf, 0, PAGE_SIZE);
        for (j = 0; j < exts_per_pg && (i*n_extent_pgs)+j < data_zones; j++) {
            int k = i*n_extent_pgs + j;
            m[j].cmd = STL_PUT_EXT;
            m[j].lba = k * zone_lbas;
            m[j].pba = (k + ckpt_zones + mcache_zones + 2) * zone_lbas;
            m[j].len = zone_lbas;
        }
        write(fd, buf, PAGE_SIZE);
    }
    
    /* overwrite the beginning of the media cache so we don't chase
     * false journal entries
     */
    memset(buf, 0, PAGE_SIZE);
    lseek(fd, zone_lbas * (ckpt_zones + 4) * 512, SEEK_SET);
    write(fd, buf, PAGE_SIZE);
    
    close(fd);
}


/* 
 * stl formatcmr device zone_lbas ckpt_size mcache_size
 *         zone_lbas in LBAs, cpkt_size and mcache_size in zones
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
