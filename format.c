#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stddef.h>
#include <linux/types.h>
#include <linux/fs.h>
#include "format_metadata.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
//#include <zlib.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <linux/blkzoned.h>
#include <string.h>

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64


#define ZONE_SZ (256 * 1024 * 1024)
#define BLK_SZ 4096
#define NR_BLKS_PER_ZONE (ZONE_SZ /BLK_SZ)
#define NR_CKPT_COPIES 2 
#define NR_BLKS_SB 2
#define NR_SECTORS_IN_BLK 8
#define BITS_IN_BYTE 8

/* Our LBA is associated with a block
 * rather than a sector
 */

unsigned int crc32(int d, unsigned char *buf, unsigned int size)
{
	return 0;
}

int open_disk(char *dname)
{
	int fd;

	printf("\n Opening %s ", dname);

	fd = open(dname, O_RDWR);
	if (fd < 0) {
		perror("Could not open the disk: ");
		exit(errno);
	}
	printf("\n %s opened with fd: %d ", dname, fd);
	return fd;
}

int write_to_disk(int fd, char *buf, unsigned long sectornr)
{
	int ret = 0;
	unsigned int offset = sectornr * 512;

	if (fd < 0) {	
		printf("\n Invalid, closed fd sent!");
		return -1;
	}

	//printf("\n %s writing at sectornr: %d", __func__, sectornr);
	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %d ret: %d", offset, sectornr, ret);
		perror("!! (before write) Error in lseek: \n");
		exit(errno);
	}
	ret = write(fd, buf, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing: ");
		exit(errno);
	}
	return (ret);
}



__le32 get_zone_count(int fd)
{
	__le32 zone_count = 0, zonesz;
	char str[400];
    	__u64 capacity = 0;

	/* get the capacity in bytes */
	if (ioctl(fd, BLKGETSIZE64, &capacity) < 0) {
		sprintf(str, "Get capacity failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		exit(errno);
	}
	printf("\n capacity: %llu", capacity);

	if (ioctl(fd, BLKGETZONESZ, &zonesz) < 0) {
		sprintf(str, "Get zone size failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		exit(errno);
	}
	printf("\n ZONE size: %llu", zonesz);

	if (ioctl(fd, BLKGETNRZONES, &zone_count) < 0) {
		sprintf(str, "Get nr of zones ioctl failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		exit(errno);
	}

	printf("\n Nr of zones reported by disk :%llu", zone_count);
	/* Use zone queries and find this eventually
	 * Doing this manually for now for a 20GB 
	 * harddisk and 256MB zone size.
	 */
	if (zone_count < (capacity/ZONE_SZ)) {
		printf("\n Number of zones: %d ", zone_count);
		printf("\n capacity/ZONE_SZ: %d ", capacity/ZONE_SZ);
		return zone_count;
	}
	printf("\n Actual zone count calculated: %d ", (capacity/ZONE_SZ));
	return (capacity/ZONE_SZ);
}

/* Note,  that this also account for the first few metadata zones.
 * but we do not care right now. While writing the extents, its 
 * just for the data area. So we end up allocating slightly
 * more space than is strictly necessary.
 */
__le64 get_nr_blks(struct lsdm_sb *sb)
{
	unsigned long nr_blks = (sb->zone_count << (sb->log_zone_size - sb->log_block_size));
	return nr_blks;
}

/*
 * 131072 reverse map entries are stored as a part of the reverse map
 * irrespective of the size of the disk. These are entries for two zones
 * of data. One block stores 48 entries. To store 131072 entries
 * we need (131072/48) ie 2730 blocks. 
 * Now to store the bitmap of 2730 blocks we need only 1 block of bits
 * since 4096 bits can be represented by this bitmap.
 */

__le32 get_revmap_blk_count(struct lsdm_sb *sb)
{
	unsigned nr_rm_entries = 2 << (sb->log_zone_size - sb->log_block_size);
	unsigned nr_rm_entries_per_blk = NR_EXT_ENTRIES_PER_SEC * NR_SECTORS_IN_BLK;
	unsigned nr_rm_blks = nr_rm_entries / nr_rm_entries_per_blk;


	printf("\n nr_rm_entries: %d", nr_rm_entries);
	if (nr_rm_entries % nr_rm_entries_per_blk)
		nr_rm_blks++;
	return nr_rm_blks;
}

/* We store a tm entry for every 4096 block
 *
 * This accounts for the blks in the main area and excludes the blocks
 * for the metadata. Hence we do NOT call get_nr_blks()
 */
__le32 get_tm_blk_count(struct lsdm_sb *sb)
{
	u64 nr_tm_entries = (sb->max_pba / NR_SECTORS_IN_BLK);
	u32 nr_tm_entries_per_blk = BLOCK_SIZE / sizeof(struct tm_entry);
	u64 nr_tm_blks = nr_tm_entries / nr_tm_entries_per_blk;
	if (nr_tm_entries % nr_tm_entries_per_blk)
		nr_tm_blks++;

	printf("\n %s nr_tm_entries: %llu ", __func__, nr_tm_entries);
	printf("\n %s nr_tm_entries_per_blk: %d ", __func__, nr_tm_entries_per_blk);
	printf("\n %s nr_tm_blks: %llu",  __func__, nr_tm_entries / nr_tm_entries_per_blk);	
	printf("\n %s nr_tm_count: %llu", __func__, nr_tm_blks);
	return nr_tm_blks;
}


__le32 get_revmap_bm_blk_count(struct lsdm_sb *sb)
{
	unsigned nr_revmap_blks = sb->blk_count_revmap;
	unsigned nr_bytes = nr_revmap_blks / BITS_IN_BYTE;
	if (nr_revmap_blks % BITS_IN_BYTE)
		nr_bytes++;
	unsigned nr_blks = nr_bytes / BLOCK_SIZE;
	if (nr_bytes % BLOCK_SIZE)
		nr_blks++;
	printf("\n nr_revmap_blks: %d", nr_revmap_blks);
	printf("\n revmap_bm_blk_count: %d", nr_blks);
	return nr_blks;

}


__le32 get_nr_zones(struct lsdm_sb *sb)
{
	return sb->zone_count;
}

__le32 get_sit_blk_count(struct lsdm_sb *sb)
{
	unsigned int one_sit_sz = 80; /* in bytes */
	unsigned int nr_sits_in_blk = BLK_SZ / one_sit_sz;
	__le32 nr_sits = get_nr_zones(sb);
	unsigned int blks_for_sit = nr_sits / nr_sits_in_blk;
	if (nr_sits % nr_sits_in_blk > 0)
		blks_for_sit = blks_for_sit + 1;

	return blks_for_sit;
}

/* We write all the metadata blks sequentially in the CMR zones
 * that can be rewritten in place
 */
__le32 get_metadata_zone_count(struct lsdm_sb *sb)
{
	/* we add the 2 for the superblock */
	unsigned long metadata_blk_count = NR_BLKS_SB + sb->blk_count_revmap + sb->blk_count_ckpt + sb->blk_count_revmap_bm + sb->blk_count_tm + sb->blk_count_sit;
	unsigned long nr_blks_in_zone = (1 << sb->log_zone_size)/(1 << sb->log_block_size);
	unsigned long metadata_zone_count = metadata_blk_count / nr_blks_in_zone;
	
	printf("\n\n");
	printf("\n ********************************************\n");
	printf("\n sb->blk_count_revmap: %llu ", sb->blk_count_revmap);
	printf("\n sb->blk_count_ckpt: %llu", sb->blk_count_ckpt);
	printf("\n sb->blk_count_revmap_bm: %llu", sb->blk_count_revmap_bm);
	printf("\n sb->blk_count_tm: %llu", sb->blk_count_tm);
	printf("\n sb->blk_count_sit: %llu", sb->blk_count_sit);

	printf("\n metadata_blk_count: %llu", metadata_blk_count);
	if (metadata_blk_count % nr_blks_in_zone > 0)
		metadata_zone_count  = metadata_zone_count + 1;

	printf("\n metadata_zone_count: %llu", metadata_zone_count);
	printf("\n ********************************************\n");
	printf("\n\n");
	return metadata_zone_count;
}

/* we consider the main area to have the 
 * reserved zones
 */
__le32 get_main_zone_count(struct lsdm_sb *sb)
{
	__le32 main_zone_count = 0;
	/* we are not subtracting reserved zones from here */
	main_zone_count = sb->zone_count - get_metadata_zone_count(sb);
	return main_zone_count;
}

#define RESERVED_ZONES	10
/* TODO: use zone querying to find this out */
__le32 get_reserved_zone_count()
{
	__le32 reserved_zone_count = 0;
	/* We will use the remaining reserved zone counts 
	 * for this
	 */
	reserved_zone_count = RESERVED_ZONES;
	return reserved_zone_count;
}


/* We write one raw ckpt in one block
 * If the first LBA is 0, then the first
 * zone has (zone_size/blk_size) - 1 LBAs
 * the first block in the second zone is 
 * the CP and that just takes the next
 * LBA as ZONE_SZ/BLK_SZ
 * The blk adderss is 256MB;
 */
__le64 get_revmap_pba()
{
	return (NR_BLKS_SB * NR_SECTORS_IN_BLK);
}

__le64 get_tm_pba(struct lsdm_sb *sb)
{
	u64 tm_pba = sb->revmap_pba + ( sb->blk_count_revmap * NR_SECTORS_IN_BLK);
	return tm_pba;
}

__le64 get_revmap_bm_pba(struct lsdm_sb *sb)
{
	u64 revmap_bm_pba = sb->tm_pba + (sb->blk_count_tm * NR_SECTORS_IN_BLK);
	return revmap_bm_pba;
}

__le64 get_ckpt1_pba(struct lsdm_sb *sb)
{
	u64 cp1_pba  = sb->revmap_bm_pba + (sb->blk_count_revmap_bm * NR_SECTORS_IN_BLK);
	return cp1_pba;
}


__le64 get_sit_pba(struct lsdm_sb *sb)
{
	return sb->ckpt2_pba + NR_SECTORS_IN_BLK;
}

__le64 get_zone0_pba(struct lsdm_sb *sb)
{
	return sb->sit_pba + (sb->blk_count_sit * NR_SECTORS_IN_BLK);
}

void read_sb(int fd, unsigned long sectornr)
{
	struct lsdm_sb *sb;
	int ret = 0;
	unsigned long long offset = sectornr * 512;

	sb = (struct lsdm_sb *)malloc(BLK_SZ);
	if (!sb)
		exit(-1);
	memset(sb, 0, BLK_SZ);

	printf("\n *********************\n");

	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("\n Could not lseek: ");
		exit(errno);
	}

	ret = read(fd, sb, BLK_SZ);
	if (ret < 0) {
		perror("\n COuld not read the sb: ");
		exit(errno);
	}
	if (sb->magic != STL_SB_MAGIC) {
		printf("\n wrong superblock!");
		exit(-1);
	}
	printf("\n sb->magic: %d", sb->magic);
	printf("\n sb->version %d", sb->version);
	printf("\n sb->log_sector_size %d", sb->log_sector_size);
	printf("\n sb->log_block_size %d", sb->log_block_size);
	printf("\n sb->blk_count_ckpt %d", sb->blk_count_ckpt);
	//printf("\n sb-> %d", sb->);
	printf("\n sb->zone_count: %d", sb->zone_count);
	printf("\n Read verified!!!");
	printf("\n ==================== \n");
	free(sb);
}

__le64 get_max_pba(struct lsdm_sb *sb)
{
	return sb->zone_count * (1 << (sb->log_zone_size - sb->log_sector_size));

}

void write_zeroed_blks(int fd, sector_t pba, unsigned nr_blks)
{
	char buffer[BLK_SZ];
	int i, ret;
	unsigned int offset = pba * 512;

	printf("\n Writing %d zeroed blks, from pba: %llu", nr_blks, pba);
	
	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0 && errno < 0) {
		printf("\n write to disk offset: %u, sectornr: %lld ret: %d", offset, pba, ret);
		perror("!! (before write) Error in lseek: \n");
		exit(errno);
	}
	memset(buffer, 0, BLK_SZ);
	for (i=0; i<nr_blks; i++) {
		//printf("\n i: %d ", i);
		ret = write(fd, buffer, BLK_SZ);
		if (ret < 0) {
			perror("Error while writing: ");
			exit(errno);
		}
	}
}


void read_block(int fd, sector_t pba, unsigned nr_blks)
{

	char buffer[4096];
	int i, ret, j;

	printf("\n Reading %d zeroed blks, from pba: %llu", nr_blks, pba);
	
	/* lseek offset is in  bytes not sectors, pba is in sectors */
	ret = lseek(fd, (pba * 512), SEEK_SET);
	if (ret < 0) {
		perror(">>>> (before read) Error in lseek: \n");
		exit(errno);
	}
	for (i=0; i<nr_blks; i++) {
		ret = read(fd, buffer, 4096);
		if (ret < 4096) {
			perror("Error while reading: ");
			exit(errno);
		}
		for(j=0; j<ret; j++) {
			if(buffer[i] != 0) {
				assert(1);
			}
		}
	}


}

/*
 * Initially the reverse map should be all zero.
 */
void write_revmap(int fd, sector_t revmap_pba, unsigned nr_blks)
{
	write_zeroed_blks(fd, revmap_pba, nr_blks);
}

void write_tm(int fd, sector_t tm_pba, unsigned nr_blks)
{
	return;
	printf("\n ** %s Writing tm blocks at pba: %llu, nrblks: %u", __func__, tm_pba, nr_blks);
	write_zeroed_blks(fd, tm_pba, nr_blks);
	read_block(fd, tm_pba, nr_blks);
}


/* Revmap bitmap: 0 indicates blk is available for writing
 */
void write_revmap_bitmap(int fd, sector_t revmap_bm_pba, unsigned nr_blks)
{
	write_zeroed_blks(fd, revmap_bm_pba, nr_blks);
}

unsigned long long get_current_frontier(struct lsdm_sb *sb)
{
	unsigned long sit_end_pba = sb->sit_pba + sb->blk_count_sit * NR_SECTORS_IN_BLK;
	unsigned long sit_end_blk_nr = sit_end_pba / NR_SECTORS_IN_BLK;
	unsigned int sit_zone_nr = sit_end_blk_nr / NR_BLKS_PER_ZONE;
	if (sit_end_blk_nr % NR_BLKS_PER_ZONE > 0) {
		sit_zone_nr = sit_zone_nr + 1;
	}
	
	/* The data zones start in the next zone of that of the last
	 * metadata zone
	 */
	printf("\n sit_end_pba: %ld", sit_end_pba);
	printf("\n sit_end_blk_nr: %ld", sit_end_blk_nr);
	printf("\n sit_zone_nr: %d", sit_zone_nr);
	return (sit_zone_nr + 1) * (1 << (sb->log_zone_size - sb->log_sector_size));
}

unsigned long long get_current_gc_frontier(struct lsdm_sb *sb, int fd)
{
	int zonenr = get_zone_count(fd);

	zonenr = zonenr - 20000;
	return (zonenr) * (1 << (sb->log_zone_size - sb->log_sector_size));
}


struct lsdm_sb * write_sb(int fd, unsigned long sb_pba)
{
	struct lsdm_sb *sb;
	int ret = 0;

	sb = (struct lsdm_sb *)malloc(BLK_SZ);
	if (!sb)
		exit(-1);
	memset(sb, 0, BLK_SZ);
	
	sb->magic = STL_SB_MAGIC;
	sb->version = 1;
	sb->log_sector_size = 9;
	sb->log_block_size = 12;
	sb->log_zone_size = 28;
	sb->checksum_offset = offsetof(struct lsdm_sb, crc);
	sb->zone_count = get_zone_count(fd);
	printf("\n sb->zone_count: %d", sb->zone_count);
	sb->max_pba = get_max_pba(sb);
	printf("\n sb->max_pba: %llu", sb->max_pba);
    	sb->zone_count_reserved = get_reserved_zone_count(sb);
	printf("\n sb->zone_count_reserved: %d", sb->zone_count_reserved);
	sb->blk_count_revmap = get_revmap_blk_count(sb);
	printf("\n sb->blk_count_revmap: %d", sb->blk_count_revmap);
	sb->blk_count_tm = get_tm_blk_count(sb);
	printf("\n sb->blk_count_tm: %d", sb->blk_count_tm);
	sb->blk_count_revmap_bm = get_revmap_bm_blk_count(sb);
	printf("\n sb->blk_count_revmap_bm: %d", sb->blk_count_revmap_bm);
	sb->blk_count_ckpt = NR_CKPT_COPIES;
	printf("\n sb->blk_count_ckpt: %d", sb->blk_count_ckpt);
	sb->blk_count_sit = get_sit_blk_count(sb);
	printf("\n sb->blk_count_sit: %d", sb->blk_count_sit);
	sb->revmap_pba = get_revmap_pba(sb);
	printf("\n sb->revmap_pba: %u", sb->revmap_pba);
	sb->tm_pba = get_tm_pba(sb);
	printf("\n sb->tm_pba: %u", sb->tm_pba);
	sb->revmap_bm_pba = get_revmap_bm_pba(sb);
	printf("\n sb->revmap_bm_pba: %u", sb->revmap_bm_pba);
	sb->ckpt1_pba = get_ckpt1_pba(sb);
	printf("\n sb->ckpt1_pba: %u", sb->ckpt1_pba);
	sb->ckpt2_pba = sb->ckpt1_pba + NR_SECTORS_IN_BLK;
	printf("\n sb->ckpt2_pba: %u", sb->ckpt2_pba);
	sb->sit_pba = get_sit_pba(sb);
	printf("\n sb->sit_pba: %u", sb->sit_pba);
	sb->zone0_pba = get_current_frontier(sb);
	printf("\n sb->zone0_pba: %d", sb->zone0_pba);
	sb->zone_count_main = get_main_zone_count(sb);
	printf("\n sb->zone_count_main: %d", sb->zone_count_main);

	
	sb->crc = 0;
	sb->crc = crc32(-1, (unsigned char *)sb, STL_SB_SIZE);

	printf("\n sb_pba: %ld", sb_pba);
	ret = write_to_disk(fd, (char *)sb, sb_pba); 
	if (ret < 0)
		exit(-1);
	return sb;
}

__le64 get_lba(unsigned int zonenr, unsigned int blknr)
{
	int nrblks = (zonenr * NR_BLKS_PER_ZONE) + blknr;
	return (nrblks * NR_SECTORS_IN_BLK);
}

void set_bitmap(char *bitmap, unsigned int nrzones, char ch)
{
	int nrbytes = nrzones / 8;
	if (nrzones % 8 > 0)
		nrbytes = nrbytes + 1;
	memset(bitmap, ch, nrbytes);
}



unsigned long long get_user_block_count(struct lsdm_sb *sb)
{
	unsigned long sit_end_pba = sb->sit_pba + sb->blk_count_sit * NR_SECTORS_IN_BLK;
	unsigned long sit_zone_nr = sit_end_pba >> sb->log_zone_size;
	return ((sb->zone_count - sit_zone_nr) * (NR_BLKS_PER_ZONE));

}

void prepare_cur_seg_entry(struct lsdm_seg_entry *entry)
{
	/* Initially no block has any valid data */
	entry->vblocks = 0;
	entry->mtime = 0;
}


void prepare_prev_seg_entry(struct lsdm_seg_entry *entry)
{
	entry->vblocks = 0;
	entry->mtime = 0;
}

struct lsdm_ckpt * read_ckpt(int fd, struct lsdm_sb * sb, unsigned long ckpt_pba)
{
	struct lsdm_ckpt *ckpt;
	unsigned long offset = ckpt_pba * 512;
	int ret;

	ckpt = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, BLK_SZ);
	printf("\n Verifying ckpt contents !!");
	printf("\n Reading from sector: %ld", ckpt_pba);

	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("in read_ckpt lseek failed: ");
		exit(-1);
	}

	ret = read(fd, ckpt, BLK_SZ);
	if (ret < 0) {
		printf("\n read from disk offset: %u, sectornr: %d ret: %d", offset, ckpt_pba, ret);
		perror("\n Could not read from disk because: ");
		exit(errno);
	}
	printf("\n Read checkpoint, ckpt->magic: %d ckpt->hot_frontier_pba: %lld", ckpt->magic, ckpt->hot_frontier_pba);
	if (ckpt->magic != STL_CKPT_MAGIC) {
		free(ckpt);
		printf("\n");
		exit(-1);
	}
	printf("\n hot_frontier_pba: %llu" , ckpt->hot_frontier_pba);
	printf("\n warm_gc_frontier_pba: %llu" , ckpt->warm_gc_frontier_pba);
	return ckpt;
}

void write_ckpt(int fd, struct lsdm_sb * sb, unsigned long ckpt_pba)
{
	struct lsdm_ckpt *ckpt, *ckpt1;
	int ret;

	ckpt = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, BLK_SZ);
	ckpt->magic = STL_CKPT_MAGIC;
	ckpt->version = 0;
	ckpt->user_block_count = sb->zone_count_main << (sb->log_zone_size - sb->log_block_size);
	ckpt->nr_invalid_zones = 0;
	ckpt->hot_frontier_pba = sb->zone0_pba;
	ckpt->warm_gc_frontier_pba = get_current_gc_frontier(sb, fd);
	ckpt->nr_free_zones = sb->zone_count_main - 2; //1 for the current frontier and gc frontier
	ckpt->elapsed_time = 0;
	ckpt->clean = 1;  /* 1 indicates clean datastructures */
	ckpt->crc = 0;
	printf("\n-----------------------------------------------------------\n");
	printf("\n checkpoint written at: %llu cur_frontier_pba: %lld", ckpt_pba, ckpt->hot_frontier_pba);

	unsigned long offset = ckpt_pba * 512;

	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %d ret: %d", offset, ckpt_pba, ret);
		perror("!! (before write) Error in lseek: \n");
		exit(errno);
	}
	ret = write(fd, ckpt, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing: ");
		exit(errno);
	}



	printf("\n ckpt->nr_free_zones: %llu", ckpt->nr_free_zones);
	//write_to_disk(fd, (char *) ckpt, ckpt_pba);
	printf("\n Checkpoint written at pba: %llu", ckpt_pba);
	printf("\n ckpt->magic: %d ckpt->hot_frontier_pba: %lld", ckpt->magic, ckpt->hot_frontier_pba);
	printf("\n sb->zone0_pba: %llu", sb->zone0_pba);
	printf("\n warm_gc_frontier_pba: %llu" , ckpt->warm_gc_frontier_pba);


	ckpt1 = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt1)
		exit(-1);

	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %d ret: %d", offset, ckpt_pba, ret);
		perror("!! (before write) Error in lseek: \n");
		exit(errno);
	}
	ret = read(fd, ckpt1, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing: ");
		exit(errno);
	}

	//ckpt1 = read_ckpt(fd, sb, ckpt_pba);
	printf("\n Read ckpt, ckpt->magic: %d", ckpt1->magic);
	printf("\n hot_frontier_pba: %llu" , ckpt1->hot_frontier_pba);
	printf("\n warm_gc_frontier_pba: %llu" , ckpt1->warm_gc_frontier_pba);
	printf("\n ckpt->nr_free_zones: %llu", ckpt1->nr_free_zones);
	printf("\n-----------------------------------------------------------\n");

	if (memcmp(ckpt, ckpt1, sizeof(struct lsdm_ckpt))) {
		printf("\n checkpoint written and read is different!! ");
		if (ckpt->magic != ckpt1->magic) {
			printf("\n MAGIC mismatch!");
		} 
		if (ckpt->hot_frontier_pba != ckpt1->hot_frontier_pba) {
			printf("\n frontier mismatch!");
		}
		if (ckpt->warm_gc_frontier_pba != ckpt1->warm_gc_frontier_pba) {
			printf("\n gc frontier mismatch!");
		}
		if (ckpt->nr_free_zones != ckpt1->nr_free_zones) {
			printf("\n nr_free_zones mismatch!");

		}
		printf("\n");
		exit(-1);
	}
	free(ckpt);
	free(ckpt1);
}

void write_seg_info_table(int fd, u64 nr_seg_entries, unsigned long seg_entries_pba)
{
	struct lsdm_seg_entry seg_entry;
	unsigned entries_in_blk = BLK_SZ / sizeof(struct lsdm_seg_entry);
	unsigned int nr_sit_blks = (nr_seg_entries)/entries_in_blk;
	unsigned int i, ret;
	if (nr_seg_entries % entries_in_blk > 0)
		nr_sit_blks = nr_sit_blks + 1;

	printf("\n nr of seg entries: %llu", nr_seg_entries);
	printf("\n nr of sit blks: %d", nr_sit_blks);
	printf("\n");

	char * buf = (char *) malloc(BLK_SZ);
	char *orig_addr;
	if (NULL == buf) {
		perror("\n Could not malloc: ");
		exit(-ENOMEM);
	}

	memset(buf, 0, BLK_SZ);

	seg_entry.vblocks = 0;
	seg_entry.mtime = 0;
	orig_addr = buf;
	size_t size = 0;
	printf("\n preparing buf! ");
	i = 0;
	while (i < entries_in_blk) {
		memcpy(buf + size, &seg_entry, sizeof(struct lsdm_seg_entry));
		size = size +  sizeof(struct lsdm_seg_entry);
		i = i + 1;
	}
	printf("\n Buf prepared");
	i = 0;
	buf = orig_addr;
	while (i < nr_sit_blks) {
		ret = write_to_disk(fd, (char *) buf, seg_entries_pba);
		if (ret < 0) {
			exit(ret);
		}
		seg_entries_pba += 8;
		i++;
	}
	free(buf);
}

/*
 *
 * SB1, SB2, Revmap, Translation Map, Revmap Bitmap, CKPT1, CKPT2, SIT, Dataa
 *
 */

int main()
{
	unsigned int pba = 0;
	struct lsdm_sb *sb1, *sb2;
	char cmd[256];
	unsigned long nrblks;
	unsigned int ret = 0;

	//char * blkdev = "/dev/vdb";
	char * blkdev = "/dev/sdb";
	int fd = open_disk(blkdev);

	sb1 = write_sb(fd, 0);
	printf("\n Superblock written at pba: %d", pba);
	printf("\n sizeof sb: %ld", sizeof(struct lsdm_sb));
	sb2 = write_sb(fd, 8);
	read_sb(fd, 0);
	read_sb(fd, 8);
	printf("\n Superblock written at pba: %d", pba + NR_SECTORS_IN_BLK);
	free(sb2);
	write_revmap(fd, sb1->revmap_pba, sb1->blk_count_revmap);
	nrblks = get_nr_blks(sb1);
	printf("\n nrblks: %lu", nrblks);
	//write_zeroed_blks(fd, 0, nrblks);
	printf("\n nrblks: %lu", nrblks);
	write_tm(fd, sb1->tm_pba, sb1->blk_count_tm);
	write_revmap_bitmap(fd, sb1->revmap_bm_pba, sb1->blk_count_revmap_bm);
	write_ckpt(fd, sb1, sb1->ckpt1_pba);
	printf("\n Checkpoint written at offset: %d", sb1->ckpt1_pba);
	write_ckpt(fd, sb1, sb1->ckpt2_pba);
	printf("\n Second Checkpoint written at offset: %d", sb1->ckpt2_pba);
	printf("\n Extent map written");
	printf("\n sb1->zone_count: %d", sb1->zone_count);
	write_seg_info_table(fd, sb1->zone_count, sb1->sit_pba);
	printf("\n Segment Information Table written");
	pba = 0;
	read_ckpt(fd, sb1, sb1->ckpt1_pba);
	free(sb1);
	close(fd);
	/* 0 volume_size: 39321600  lsdm  blkdev: /dev/vdb tgtname: TL1 zone_lbas: 524288 data_end: 41418752 */
	unsigned long zone_lbas = 524288; /* nr of sectors in a zone */
	unsigned long data_zones = 75;
	char * tgtname = "TL1";
	//volume_size = data_zones * zone_lbas;
	unsigned long volume_size = data_zones * zone_lbas;
	unsigned long data_end = volume_size;
	sprintf(cmd, "/sbin/dmsetup create TL1 --table '0 %ld lsdm %s %s %ld %ld'",
            volume_size, blkdev, tgtname, zone_lbas, 
	    data_end);
	printf("\n cmd: %s", cmd);
    	//system(cmd);
	printf("\n \n");
	return(0);
}
