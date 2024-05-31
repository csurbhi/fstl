#include <linux/types.h>

/*
 * Design: We employ zone chaining - last block in the zone will point to the next zone.
 * We using the logical zone number in this chaining - as a zone will undergo cleaning and
 * so the physical zone nr may change.
 * Zone1: SB
 * Zone2: CKPT log
 * Zone3: CKPT log
 * Zone4: DZONE map
 * Zone5: DZONE map
 *
 * The dzone map contains the dzones used for translation map. Since we are zone chaining, the last
 * block points to the next zone. However, this zone can get GCed. For this reason we use dzone map,
 * we record the logical zone in the zone chaining and the dzone map provides the physical zone.
 * CKPT mentions the first zone that has the TM and the current zone that holds the TM.
 * Each TM zone, the last block records the next zone that holds the TM.
 * Similarly each SIT zone points to the next zone with the SIT information. Again the logical zone nr
 * is used in the chaining.
 *
 * SB, CKPT-Zone, CKPT-Zone, Dzone-Map, DZone-Map, Data
 *
 * So our order of writing is as follows:
 * 1. Write the data
 * 2. Write trans map entries - when one block worth entries are collected.
 * 4. Write checkpoint after 2 is done and after 3 is done. The checkpoint maintains the write pointer
 * of the last TM zone. This way we ensure we don't read past the last block.
 * Writing the TM in a SMR  zone - ensures that the write pointer is stored in the hardware.
 * Every TM block holds two things: 1) the checksum of that block 2) the age of that block
 * If you do this, there is no need to store the SIT separately as it can be calculated based on this.
 * It can be an in-memory thing and fewer data needs to go to the device.
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

#define SIT_ENTRIES_BLK 	(BLK_SIZE/sizeof(struct lsdm_seg_entry))
#define TM_ENTRIES_BLK 		(BLK_SIZE/sizeof(struct tm_entry))
#define SBZN_COUNT 1
#define CKPTZN_COUNT 2
#define TZONE_MAP_COUNT 2
#define STATIC_MD_ZONE_COUNT (SBZN_COUNT + CKPTZN_COUNT + TZONE_MAP_COUNT)

struct lsdm_seg_entry {
	unsigned int vblocks;  /* maximum vblocks currently are 65536 */
	unsigned long mtime;
	/* We do not store any valid map here
	 * as an extent map is stored separately
	 * as a part of the translation map
	 */
    	/* can also add Type of segment */
} __attribute__((packed));


#define NR_SECTORS_PER_BLK		8	/* 4096 / 512 */
#define BLK_SIZE			4096
struct lsdm_ckpt {
	uint32_t magic;
	u64 version;
	u64 user_block_count;
	u64 nr_invalid_zones;	/* zones that have errors in them */
	u64 hot_frontier_pba;
	u64 warm_gc_frontier_pba;
	u64 tt_first_zone;	/* logical zone number */
	u64 tt_curr_zone;	/* logical zone number */
	unsigned int tt_curr_offset;  /* offset within the current zone */
	u64 blk_count_tm;	/* Nr of valid TM blks */
	u32 nr_free_zones;
	u64 elapsed_time;		/* records the time elapsed since all the mounts */
	u64 crc;
	unsigned char padding[0]; /* write all this in the padding */
} __attribute__((packed));

#define STL_SB_SIZE 4096

struct lsdm_sb {
	unsigned int magic;			/* Magic Number */
	unsigned int version;			/* Superblock version */
	unsigned int log_sector_size;		/* log2 sector size in bytes */
	unsigned int log_block_size;		/* log2 block size in bytes */
	unsigned int log_zone_size;		/* log2 zone size in bytes */
	unsigned int checksum_offset;		/* checksum offset inside super block */
	unsigned int zone_count;		/* total # of segments */
	unsigned int zone_count_reserved;	/* # CMR zones that are reserved */
	unsigned int zone_count_main;		/* # of segments for main area */
	unsigned int zone_count_md;	/* # of segment for metadata */
	unsigned int nr_lbas_in_zone;
	unsigned int nr_cmr_zones;
	u64 zone0_pba;
	u64 max_pba;                 /* The last lba in the disk */
	u32 crc;			/* checksum of superblock */
	__u8 reserved[0];		/* valid reserved region. Rest of the block space */
} __attribute__((packed));

/*
 * Now that we are writing the TM entries in a log structured manner, the entries are not written in
 * order of the LBA. Thus every entry specifies both the LBA and the PBA.
 * When both LBA and PBA are 0, the entry is invalid.
 */
struct tm_entry {
	sector_t lba;
	sector_t pba;
} __attribute__((packed));

struct tt_zone_map {
	unsigned int lzonenr;
	unsigned int pzonenr;
} __attribute__((packed));
