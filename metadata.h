#include<linux/types.h>

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
	int (*select_victim)(struct stl_sb_info);
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
#define VBLK_MAP_SIZE 8192
__u8 valid_map[VBLK_MAP_SIZE];
 */

struct stl_seg_entry {
	__le16 vblocks;
	__le64 mtime;
	/* We do not store any valid map here
	 * as an extent map is stored separately
	 * as a part of the translation map
	 */
    	/* can also add Type of segment */
}__packed;


/* The same structure is used for writing the header/trailer.
 * header->len is always 0. If you crash after writing the header
 * and some data but not the trailer, at remount time, you read
 * the blocks after the header till the end of the zone. If you
 * don't find the trailer, then you do not trust the trailing
 * data. When found, the trailer->len should
 * indicate the data that it covers. Every zone must have atleast
 * one header and trailer, but it could be multiple as well.
 */
struct stl_header {
	uint32_t magic;
	uint32_t nonce;
	uint32_t crc32;
	uint16_t flags;
	uint16_t len;
	uint64_t prev_pba;
	uint64_t next_pba;
	uint64_t lba;
	uint64_t pba;
	uint64_t seq;
} __attribute__((packed));

/* Type of segments could be
 * Read Hot, Read Warm
 * Write Hot, Write Warm.
 * Read-Write Cold
 */

struct stl_ckpt {
	__le64 checkpoint_ver;
	__le64 user_block_count;
	__le64 valid_block_count;
	__le32 rsvd segment_count;
	__le32 free_segment_count;
	__le32 blk_nr; 		/* write at this blk nr */
	__le32 cur_frontier;
	__le32 blk_pba;
	__le64 elapsed_time;
	struct stl_seg_entry cur_seg_entry;
	struct stl_header header;
	char reserved[0];
}__packed;


struct stl_dev_info {
        struct block_device *bdev;
        char path[MAX_PATH_LEN];
        unsigned int total_zones;
        sector_t start_blk;
        sector_t end_blk;
        unsigned long *zone_bitmap;        /* Bitmap indicating sequential zones */
};

#define STL_SB_SIZE 4096

struct stl_sb {
	__le32 magic;			/* Magic Number */
	__le32 log_sector_size;		/* log2 sector size in bytes */
	__le32 log_block_size;		/* log2 block size in bytes */
	__le32 log_zone_size;	/	/* log2 zone size in bytes */
	__le32 checksum_offset;		/* checksum offset inside super block */
	__le32 zone_count;		/* total # of segments */
	__le32 blk_count_ckpt;		/* # of blocks for checkpoint */
	__le32 blk_count_map;		/* # of segments for extent map*/
	__le32 blk_count_sit;		/* # of segments for SIT */
	__le32 zone_count_reserved;	/* # CMR zones that are reserved */
	__le32 zone_count_main;		/* # of segments for main area */
	__le32 cp_pba;			/* start block address of checkpoint */
	__le32 map_pba;			/* start block address of NAT */
	__le32 sit_pba;			/* start block address of SIT */
	__le32 zone0_pba;		/* start block address of segment 0 */
	__le32 nr_invalid_zones;	/* zones that have errors in them */
	//__u8 uuid[16];			/* 128-bit uuid for volume */
	//__le16 volume_name[MAX_VOLUME_NAME];	/* volume name */
	__le32 crc;			/* checksum of superblock */
	__u8 reserved[0];		/* valid reserved region. Rest of the block space */
} __packed;


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

	/* for bio operations */
	struct f2fs_bio_info *write_io[NR_PAGE_TYPE];	/* for write bios */
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





