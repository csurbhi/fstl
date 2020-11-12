#include <linux/types.h>
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

struct zone_summary_info {
	sector_t table_lba;	/* start block address of SIT area */
	unsigned int nr_table_blocks;
	unsigned int nr_valid_blocks;
	char *seg_bitmap;
	char *invalid_seg_map;
	unsigned int bitmap_size;
	unsigned int seg_entries_per_blk;
	struct rw_semaphore seg_entry_lock;
	struct stl_seg_entry *seg_entry_cache;
	unsigned long elapsed_time;
	unsigned long mounted_time;
	unsigned long min_mtime;
	unsigned long max_mtime;
	unsigned int last_victim[2];
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


struct stl_sb_info {
	struct proc_dir_entry *s_proc;		/* proc entry */
	struct stl_sb *raw_super;		/* raw super block pointer */
	struct rw_semaphore sb_lock;		/* lock for raw super block */
	int valid_super_block;			/* valid super block no */
	unsigned long s_flag;				/* flags for sbi */
	struct mutex writepages;		/* mutex for writepages() */
	unsigned int blocks_per_zone;		/* blocks per zone */
	unsigned int log_blocks_per_blkz;	/* log2 blocks per zone */

	/* for segment-related operations */
	struct stl_sm_info *sm_info;		/* segment manager */

	/* keep migration IO order for LFS mode */
	struct rw_semaphore io_order_lock;

	/* for checkpoint */
	struct stl_ckpt *ckpt;			/* raw checkpoint pointer */
	int cur_cp_pack;			/* remain current cp pack */
	spinlock_t cp_lock;			/* for flag in ckpt */
	struct mutex cp_mutex;			/* checkpoint procedure lock */
	struct rw_semaphore cp_rwsem;		/* blocking FS operations */
	struct rw_semaphore node_write;		/* locking node writes */
	struct rw_semaphore node_change;	/* locking node change */
	wait_queue_head_t cp_wait;
	unsigned long last_time[MAX_TIME];	/* to store time in jiffies */
	long interval_time[MAX_TIME];		/* to store thresholds */


	spinlock_t fsync_node_lock;		/* for node entry lock */


	/* for extent tree cache */
	struct radix_tree_root extent_tree_root;/* cache extent cache entries */
	struct mutex extent_tree_lock;	/* locking extent radix tree */
	atomic_t total_ext_tree;		/* extent tree count */

	/* basic device mapper units */
	unsigned int log_sectors_per_block;	/* log2 sectors per block */
	unsigned int log_blocksize;		/* log2 block size */
	unsigned int blocksize;			/* block size */
	unsigned int log_blocks_per_seg;	/* log2 blocks per segment */
	unsigned int blocks_per_seg;		/* blocks per segment */

	sector_t user_block_count;		/* # of user blocks */
	sector_t total_valid_block_count;	/* # of valid blocks */
	sector_t discard_blks;			/* discard command candidats */
	sector_t last_valid_block_count;	/* for recovery */
	sector_t reserved_blocks;		/* configurable reserved blocks */

	/* # of allocated blocks */
	struct percpu_counter alloc_valid_block_count;

	/* for cleaning operations */
	struct mutex gc_mutex;			/* mutex for GC */
	struct stl_gc_kthread	*gc_thread;	/* GC thread */
	unsigned int cur_victim_zone;		/* current victim section num */
	unsigned int gc_mode;			/* current GC state */
	unsigned int next_victim_zone[2];	/* next segment in victim section */
	/* maximum # of trials to find a victim segment for SSR and GC */
	unsigned int max_victim_search;
};

struct extent_entry {
	sector_t lba;
	sector_t pba;
	size_t   len;
}__packed;


/* used to hold a range to be copied.
*/
struct copy_req {
	struct list_head list;
	sector_t lba;
	sector_t pba;
	int len;
	int flags;
	struct bio *bio;
	struct extent *e;
};
static struct kmem_cache *_copyreq_cache;

/* zone free list entry
*/
struct free_zone {
	struct list_head list;
	sector_t start;
	sector_t end;
};


/* TODO: we keep this in a linked list,
 * but ideally we should keep this in 
 * two trees: one for cost benefit 
 * and one for greedy
 */
struct gc_candidate {
	struct list_head list;
	unsigned int segment_nr;
	unsigned int valid_blocks;
	unsigned long mtime;
};

struct stl_gc_thread;

/* this has grown kind of organically, and needs to be cleaned up.
*/
struct ctx {
	sector_t          nr_lbas_in_zone;	/* in 512B LBAs */
	int               max_pba;

	spinlock_t        lock;
	sector_t          write_frontier; /* LBA, protected by lock */
	sector_t          wf_end;
	sector_t          previous;	  /* protected by lock */
	unsigned          sequence;
	struct list_head  free_zones;
	int               n_free_zones;
	sector_t          free_sectors_in_wf;  /* Indicates the free sectors in the current write frontier */
	struct list_head  gc_candidates;
	int		  n_gc_candidates;

	struct rb_root    rb;	          /* map RB tree */
	struct rb_root	  sit_rb;	  /* SIT RB tree */
	rwlock_t          rb_lock;
	rwlock_t	  sit_rb_lock;
	int               n_extents;      /* map size */

	mempool_t        *extent_pool;
	mempool_t        *copyreq_pool;
	mempool_t        *page_pool;
	struct bio_set   * bs;

	struct dm_dev    *dev;

	struct stl_msg    msg;
	sector_t          list_lba;	/* needed for external gc */

	wait_queue_head_t cleaning_wait; /* now we do not need this as we will use a lock to coordinate GC and writes */
	wait_queue_head_t space_wait; /* same comment as above */
	atomic_t          io_count;
	struct completion move_done;

	atomic_t          n_reads;
	sector_t          target;	/* in our case now points to the segment getting GCed */
	unsigned long     target_seq;   /* Figure out what this is */
	unsigned          sectors_copied;
	struct list_head  copyreqs;
	atomic_t          pages_alloced;

	char              nodename[32];
	struct miscdevice misc;
  
	struct stl_gc_thread *gc_th;
	struct page 	*sb_page;
	struct stl_sb 	*sb;
	struct page 	*ckpt_page;
	struct stl_ckpt *ckpt;
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

