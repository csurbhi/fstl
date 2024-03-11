#include <linux/types.h>

/*
 * Design:
 * SB1, SB2, Revmap Bitmap, Revmap, CKPT1, CKPT2, SIT, Translation Map, Data
 *
 * We dont want to keep two revmap entries:
 * 1. We can at max loose 1 sector of entries + 1 page of in memory entries. 
 * We are okay with that? This will happen, in spite of keeping two entries. 
 * It will happen inspite of keeping a journal.
 *
 * The translation entries:
 * 1. We don’t have to keep two copies. Because we have the entries in revmap. 
 *    So we already do have two copies.
 * 2. If we crash, then we can copy the contents of the revmap back to the 
 *    translation table.
 *
 *
 * Revmap bitmap: Only 1 copy
 * 1. We keep only one copy. On a crash, when we reboot, we just ignore this 
 *    revmap bit map.
 * 2. We read the revmap entries and write them out to the translation map.
 * 3. Then we start with a new bitmap.
 *
 *  SIT:
 * 1. We keep only one copy.
 * 2. If one sector gets destroyed while writing, then we do the following:
 *       i) We create the RB tree by reading the translation map.
 *       ii) While doing so, we keep a count of the valid blocks in every 
 *	    sector and then rebuild the SIT.
 *       iii) This way, we can ignore what is on disk.
 *       iv) We use the mtime that is stored in the last ckpt for entries where
 *	    the valid blocks dont match with our calculation.
 *
 *
 * Checkpoint:
 *        We keep two copies of this. We need the mtime for further SIT
 *        calculation. Also this is just one sector of information. We keep one
 *        checkpoint in one block. We write alternately to the checkpoints. The
 *        checkpoint with the higher elapsed time is the right checkpoint.
 * 
 *
 * So our order of writing is as follows:
 * 1. Write the data
 * 2. Write revmap entries.
 * 3. Data is now secured on the disk
 * 4. Write trans map entries
 * 5. Write revmap bitmap
 * 6. Write checkpoint
 * 7. Write SIT
 * 
 * do_ckpt() Performs 5, 6, 7 in order. We wait for 5, 6, and 7 to complete before proceeding by putting a barrier after 7.
 *
 * Revmap bitmap can be updated only after the translation blocks have been
 * written on disk.
 * SIT can only be updated after revmap entries are on disk.
 * Checkpoint variables can be updated only after revmap entries are on disk.
 *
 * So, we need some mechanism to identify that there was a crash.
 * We can keep this information in the checkpoint.
 * ckpt->clean can be 0 in ctr() and 1 in dtr().
 * On the next mount if ckpt->clean is 0, then we know we had a crash!
 *
 */

typedef __le64 u64;
typedef __le32 u32;
typedef __le16 u16;
typedef u64 uint64_t;
typedef u32 uint32_t;
typedef u16 uint16_t;
typedef u64 sector_t;

#ifndef SECTOR_SIZE
	#define SECTOR_SIZE 512
#endif
#ifndef BLOCK_SIZE
	#define BLOCK_SIZE 4096
#endif
#define BLK_SZ 4096
#define STL_SB_MAGIC 0x7853544c
#define STL_CKPT_MAGIC 0x1A2B3C4D

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

#define SIT_ENTRIES_BLK 	(BLK_SIZE/sizeof(struct lsdm_seg_entry))
#define TM_ENTRIES_BLK 		(BLK_SIZE/sizeof(struct tm_entry))

struct lsdm_seg_entry {
	unsigned int vblocks;  /* maximum vblocks currently are 65536 */
	unsigned long mtime;
	/* We do not store any valid map here
	 * as an extent map is stored separately
	 * as a part of the translation map
	 */
    	/* can also add Type of segment */
} __attribute__((packed));


/* In the worst case, we spend 20 bytes per block. There are 65536
 * such blocks. So we need 65536 such entries */
struct lsdm_revmap_extent {
	__le64 lba;
	__le64 pba;
	__le32 len; /* At a maximum there are 512 pages in a bio */
}__attribute__((packed));



/* We flush after every 655536 block writes or when the timer goes
 * off. prev_zonenr may not be recorded, in case we are recording the
 * mapping for the current zone alone. In that case prev_count will be
 * 0 as there are 0 entries recorded for previous zone
 */

#define NR_SECTORS_PER_BLK		8	/* 4096 / 512 */
#define BLK_SIZE			4096
#define NR_EXT_ENTRIES_PER_SEC		((SECTOR_SIZE - 4)/(sizeof(struct lsdm_revmap_extent)))
//#define NR_EXT_ENTRIES_PER_BLK 		(NR_EXT_ENTRIES_PER_SEC * NR_SECTORS_IN_BLK)
#define NR_EXT_ENTRIES_PER_BLK 		48
#define MAX_EXTENTS_PER_ZONE		65536
struct lsdm_revmap_entry_sector{
	struct lsdm_revmap_extent extents[NR_EXT_ENTRIES_PER_SEC];
	__le32 crc;	/* We use 31 bits to indicate the crc and LSB bit maintains 0/1 for
			   identifying if the sector belongs to this iteration or next
			  */
 }__attribute__((packed));

/* first sector */
struct lsdm_revmap_metadata {
	__le32 zone_nr_0;
	__le32 zone_nr_1;
	unsigned char version:1;  /* flips between 1 and 0 and is maintained in the crc */
	unsigned char padding[0]; /* padding for the sector */
}__attribute__((packed));

struct lsdm_ckpt {
	uint32_t magic;
	__le64 version;
	__le64 user_block_count;
	__le32 nr_invalid_zones;	/* zones that have errors in them */
	__le64 hot_frontier_pba;
	__le64 warm_gc_frontier_pba;
	__le32 nr_free_zones;
	__le64 elapsed_time;		/* records the time elapsed since all the mounts */
	__u8 clean;			/* becomes 0 in ctr and 1 in dtr. Used to identify crash */
	__le64 crc;
	unsigned char padding[0]; /* write all this in the padding */
} __attribute__((packed));

struct lsdm_revmap_bitmaps {
	unsigned char bitmap0[4096];
} __attribute__((packed));


#define STL_SB_SIZE 4096

struct lsdm_sb {
	__le32 magic;			/* Magic Number */
	__le32 version;			/* Superblock version */
	__le32 log_sector_size;		/* log2 sector size in bytes */
	__le32 log_block_size;		/* log2 block size in bytes */
	__le32 log_zone_size;		/* log2 zone size in bytes */
	__le32 checksum_offset;		/* checksum offset inside super block */
	__le64 zone_count;		/* total # of segments */
	__le64 blk_count_revmap;	/* # of blocks for storing reverse mapping */
	__le64 blk_count_ckpt;		/* # of blocks for checkpoint */
	__le64 blk_count_revmap_bm;	/* # of blocks for storing the bitmap of revmap blks availabilty */
	__le64 blk_count_tm;		/* # of segments for Translation map */
	__le64 blk_count_sit;		/* # of segments for SIT */
	__le64 zone_count_reserved;	/* # CMR zones that are reserved */
	__le64 zone_count_main;		/* # of segments for main area */
	__le64 revmap_pba;		/* start block address of revmap*/
	__le64 tm_pba;			/* start block address of translation map */
	__le64 revmap_bm_pba;		/* start block address of reverse map bitmap */
	__le64 ckpt1_pba;		/* start address of checkpoint 1 */
        __le64 ckpt2_pba;		/* start address of checkpoint 2 */
	__le64 sit_pba;			/* start block address of SIT */
	__le32 order_revmap_bm;		/* log of number of blocks used for revmap bitmap */
	__le32 nr_lbas_in_zone;
	__le64 nr_cmr_zones;
	__le64 zone0_pba;		/* start block address of segment 0 */
	__le64 max_pba;                 /* The last lba in the disk */
	//__u8 uuid[16];			/* 128-bit uuid for volume */
	//__le16 volume_name[MAX_VOLUME_NAME];	/* volume name */
	__le32 crc;			/* checksum of superblock */
	__u8 reserved[0];		/* valid reserved region. Rest of the block space */
} __attribute__((packed));

/*
 * In theory we do not need to store the LBA on the disk.
 * We can calculate the LBA, depending on the location of the
 * sequential entry on the disk. However, we do need to store
 * this in memory. For now, not optimizing on disk structure.
 *
 * TODO: We can also store how hot this block is, on the disk and
 * also need to store in memory
 *
 * Note: we do not store an extent based TM on the disk, only a 
 * block based TM
 */
struct tm_entry {
	sector_t pba;
} __attribute__((packed));



