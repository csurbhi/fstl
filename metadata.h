#include <linux/types.h>
#include <linux/refcount.h>
#include <linux/wait.h>
#include "nstl-u.h"
#include "format_metadata.h"

/*
 *
 *
 *
 * SB1 , SB2, CKPT1, CKPT2, Map, Seg Info Table, Data
 *
 *
 *
 *
 */

#define STL_SB_MAGIC 0x7853544c
#define STL_HDR_MAGIC 0x4c545353
#define NR_SECTORS_IN_BLK 8
#define BITS_IN_BYTE 8
#define LOG_SECTOR_SIZE 9


#define REVMAP_PRIV_MAGIC 0x5
#define MAX_ZONE_REVMAP 2
#define BLOCKS_IN_ZONE 65536
#define SUBBIOCTX_MAGIC 0x2
#define MAX_TM_PAGES 100
#define MAX_SIT_PAGES 100

struct read_ctx {
	struct ctx *ctx;
	refcount_t *ref;
};


struct tm_page_write_ctx {
	struct ctx *ctx;
	refcount_t ref;
	struct tm_page *tm_page;
};

struct ref_list
{
	refcount_t ref;
	struct list_head list;
};

struct tm_page {
	struct rb_node *rb;
	sector_t blknr;
	struct page *page;
	struct list_head reflist;
};

struct sit_page_write_ctx {
	struct ctx *ctx;
	struct page *page;
};

struct sit_page {
	struct rb_node *rb;
	sector_t blknr;
	struct page *page;
};

struct revmap_meta_inmem {
	u64 revmap_pba;			/* PBA where the revmap entries should be written. Reset when the refcount is 0
					 */
	struct ctx * ctx;
	struct page *page;
	refcount_t ref;
	u8 magic;
};

struct nstl_bioctx {
	struct bio * clone;
	refcount_t ref;
	struct bio * orig;
	struct ctx *ctx;
};

struct extent_entry {
	sector_t lba;
	sector_t pba;
	size_t   len;
}__packed;



struct nstl_sub_bioctx {
	struct extent_entry extent;
	struct nstl_bioctx * bioctx;
	u8 magic;
	u8 retry;
};


struct free_zone_info {
	unsigned int nr_free_zones;
	char * free_segmap;
};

struct victim_selection {
	int (*select_victim)(struct stl_sb_info *);
};

struct cur_zone_info {
	struct mutex cur_zone_mutex;
	unsigned int zone_nr;
	unsigned int next_blk_nr;
};

/* Each zone is 256MB in size.
 * there are 65536 blocks in a zone
 * there are 524288 sectors in a zone
 * if 1 bit per block, then we need
 * 8192 bytes for the block bitmap in a zone.
 * We are right now using a block bitmap
 * rather than a sector bitmap as FS writes
 * in terms of blocks.
 * 
 * A 8TB disk has 32768 zones. Thus we need
 * 65536 blocks to maintain the bitmaps alone.
 * and 642 blocks to maintain the other information
 * such as vblocks and mtime.
 * Thus total of 66178 blocks are needed 
 * that comes out to be 258MB of metadata
 * information for GC.
 */

/* The same structure is used for writing the header/trailer.
 * header->len is always 0. If you crash after writing the header
 * and some data but not the trailer, at remount time, you read
 * the blocks after the header till the end of the zone. If you
 * don't find the trailer, then you do not trust the trailing
 * data. When found, the trailer->len should
 * indicate the data that it covers. Every zone must have atleast
 * one header and trailer, but it could be multiple as well.
 */

/* Type of segments could be
 * Read Hot, Read Warm
 * Write Hot, Write Warm.
 * Read-Write Cold
 */

#define MAX_PATH_LEN 256

struct stl_dev_info {
        struct block_device *bdev;
        char path[MAX_PATH_LEN];
        unsigned int total_zones;
        sector_t start_blk;
        sector_t end_blk;
        unsigned long *zone_bitmap;        /* Bitmap indicating sequential zones */
};


/* TODO: clean up struct sb_info. ctx
 * is doing that job
 */
#define MAX_TIME 5


struct stl_gc_thread;

/* this has grown kind of organically, and needs to be cleaned up.
*/
struct ctx {
	sector_t          nr_lbas_in_zone;	/* in 512B LBAs */
	int               max_pba;

	spinlock_t        lock;
	sector_t          write_frontier; /* LBA, protected by lock */
	sector_t          wf_end;
	sector_t          free_sectors_in_wf;  /* Indicates the free sectors in the current write frontier */
	int		  n_gc_candidates;

	struct rb_root	  extent_tbl_root; /* in memory extent map */
	struct rb_root    tm_rb_root;	          /* map RB tree */
	struct rb_root	  sit_rb_root;	  /* SIT RB tree */
	rwlock_t          extent_tbl_lock;
	rwlock_t	  sit_rb_lock;
	int               n_extents;      /* map size */

	mempool_t        *extent_pool;
	mempool_t        *page_pool;
	struct bio_set   * bs;

	struct dm_dev    *dev;

	atomic_t          io_count;

	atomic_t          n_reads;
	sector_t          target;	/* in our case now points to the segment getting GCed */
	unsigned          sectors_copied;
	atomic_t          pages_alloced;

	char              nodename[32];
  
	struct stl_gc_thread *gc_th;
	struct page 	*sb_page;
	struct stl_sb 	*sb;
	struct page 	*ckpt_page;
	struct stl_ckpt *ckpt;
	char *freezone_bitmap;
	int 	nr_freezones;
	char *gc_zone_bitmap;
	int nr_gc_zones;
	int	bitmap_bytes;
	time64_t mounted_time;
	time64_t elapsed_time;
	unsigned int flag_ckpt;
	atomic_t nr_writes;
       	atomic_t nr_failed_writes;
       	atomic_t revmap_sector_count;
       	atomic_t revmap_blk_count;
	struct kmem_cache * bioctx_cache;
	struct kmem_cache * revmap_bioctx_cache;
	struct kmem_cache * sit_page_cache;
	struct kmem_cache *read_ctx_cache;
	struct kmem_cache *reflist_cache;
	struct kmem_cache *tm_page_write_cache;
	wait_queue_head_t tm_blk_flushq;
	spinlock_t tm_flush_lock;
	spinlock_t sit_flush_lock;
	wait_queue_head_t zone_entry_flushq;
	spinlock_t flush_zone_lock;
	wait_queue_head_t revmap_blks_flushq;
	spinlock_t revmap_blk_lock;
	atomic_t zone_revmap_count;	/* This should always be less than 3, incremented on get_new_zone and decremented
					 * when one zone worth of entries are written to the disk
					 */
	sector_t revmap_pba;
	struct page * revmap_page;
	spinlock_t flush_lock;
	spinlock_t sit_kv_store_lock;
	struct page *revmap_bm;		/* Stores the bitmap for the reverse map blocks flush status (65536 * 2) */
	u8	revmap_bm_order;
	atomic_t tm_flush_count;
	atomic_t sit_flush_count;
	sector_t ckpt_pba;
	atomic_t nr_pending_writes;
	atomic_t nr_sit_pages;
	atomic_t nr_tm_pages;
};

/* total size = xx bytes (64b). fits in 1 cache line 
   for 32b is xx bytes, fits in ARM cache line */
struct extent {
	struct rb_node rb;	/* 20 bytes */
	sector_t lba;		/* 512B LBA */
	sector_t pba;		
	u32      len;
	atomic_t refs[3];
	atomic_t total_refs;
	unsigned seq;
}; /* xx bytes including padding after 'rb', xx on 32-bit */

struct sit_page * search_sit_kv_store(struct ctx *ctx, sector_t pba, struct rb_node **);
struct sit_page * add_sit_entry_kv_store(struct ctx *ctx, sector_t pba);


