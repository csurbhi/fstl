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
typedef __le64 u64;
typedef __le32 u32;
typedef __le16 u16;
typedef u64 uint64_t;
typedef u32 uint32_t;
typedef u16 uint16_t;
typedef u64 sector_t;


#define STL_SB_MAGIC 0x7853544c
#define STL_HDR_MAGIC 0x4c545353

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
	__le32 vblocks;
	__le64 mtime;
	/* We do not store any valid map here
	 * as an extent map is stored separately
	 * as a part of the translation map
	 */
    	/* can also add Type of segment */
} __attribute__((packed));


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



/* In the worst case, we spend 80 bytes per block. There are 65536
 * such blocks. So we need 65536 such entries */
struct stl_ckpt_extent {
	__le64 lba;
	__le16 len; /* At a maximum there are 65536 blocks in a zone */
}__attribute__((packed));


#define NR_EXT_ENTRIES_PER_BLK 		BLK_SIZE/sizeof(struct stl_ckp_extent)

/* We flush after every 655536 block writes or when the timer goes
 * off. prev_zonenr may not be recorded, in case we are recording the
 * mapping for the current zone alone. In that case prev_count will be
 * 0 as there are 0 entries recorded for previous zone
 */
struct stl_ckpt_entry {
	__le64 prev_zonenr;
	__le64 cur_zonenr;
	__le16 prev_count;
	__le16 cur_count;
	struct stl_ckpt_extent extents[0];
}__attribute__((packed));


struct stl_ckpt {
	uint32_t magic;
	__le64 elapsed_time;
	__le64 checkpoint_ver;
	__le64 user_block_count;
	__le64 valid_block_count;
	__le32 free_segment_count;
	__le64 cur_frontier_pba;
	struct stl_seg_entry prev_seg_entry;
	struct stl_seg_entry cur_seg_entry;
	struct stl_ckpt_entry ckpt_translation_table;
} __attribute__((packed));


#define STL_SB_SIZE 4096

struct stl_sb {
	__le32 magic;			/* Magic Number */
	__le32 version;			/* Superblock version */
	__le32 log_sector_size;		/* log2 sector size in bytes */
	__le32 log_block_size;		/* log2 block size in bytes */
	__le32 log_zone_size;		/* log2 zone size in bytes */
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
	__le64 max_pba;                 /* The last lba in the disk */
	//__u8 uuid[16];			/* 128-bit uuid for volume */
	//__le16 volume_name[MAX_VOLUME_NAME];	/* volume name */
	__le32 crc;			/* checksum of superblock */
	__u8 reserved[0];		/* valid reserved region. Rest of the block space */
} __attribute__((packed));

/*
 *
 * In theory we do not need to store the LBA on the disk.
 * We can calculate the LBA, depending on the location of the
 * sequential entry on the disk. However, we do need to store
 * this in memory. For now, not optimizing on disk structure.
 */
struct extent_map {
	__le64 lba;
	__le64 pba;
	__le32 len;
} __attribute__((packed));



