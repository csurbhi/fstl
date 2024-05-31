#include <stdio.h>

#include <sys/stat.h>
#include <fcntl.h>
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

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
#include <unistd.h>

#define BLK_SZ 4096
#define NR_CKPT_COPIES 2 
#define NR_BLKS_SB 2
#define NR_SECTORS_IN_BLK 8
#define SECTORS_SHIFT 3
#define BITS_IN_BYTE 8

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
		printf("\n");
		exit(errno);
	}
	printf("\n %s opened with fd: %d ", dname, fd);
	return fd;
}

int write_to_disk(int fd, char *buf, unsigned long sectornr)
{
	int ret = 0;
	u64 offset = sectornr * SECTOR_SIZE;

	if (fd < 0) {	
		printf("\n Invalid, closed fd sent!");
		return -1;
	}

	//printf("\n %s writing at sectornr: %d", __func__, sectornr);
	ret = lseek64(fd, offset, SEEK_SET);
	if (ret == -1) {
		perror("!! (before write) Error in lseek64: \n");
		printf("\n write to disk offset: %llu, sectornr: %ld ret: %d", offset, sectornr, ret);
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
	printf("\n ZONE size: %u", zonesz);
	/* zonesz is number of addressable lbas */
	zonesz = zonesz << 9;

	if (ioctl(fd, BLKGETNRZONES, &zone_count) < 0) {
		sprintf(str, "Get nr of zones ioctl failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		exit(errno);
	}

	printf("\n Nr of zones reported by disk :%u", zone_count);
	/* Use zone queries and find this eventually
	 * Doing this manually for now for a 20GB 
	 * harddisk and 256MB zone size.
	 */
	if (zone_count >= (capacity/zonesz)) {
		printf("\n Number of zones: %d ", zone_count);
		printf("\n capacity/ZONE_SZ: %lld ", capacity/zonesz);
		return capacity/zonesz;
	}
	printf("\n Actual zone count calculated: %lld ", (capacity/zonesz));
	return zone_count;
	//return 16500;
	//return 28200;
	//return 4500;
	//return 9000;
	//return 10000;
	// 2048 GB
	//return 8192;
	//return 29807;
	//return (capacity/zonesz);
	//return 100;
	//return 1900;
	/* we test with a disk capacity of 1 TB */
	//return 4032;
	//return 500;
	//return 256;
	//return 100;
	/* 
	 * resilvering test1 - returned this
	 * return 3000;
	 *
	 * 2000 zones is 500GB. 450GB is 90% of 500GB.
	 * We are using this to perform the test.
	 *
	 * Next we will perform the test with 1900 zones
	 * ie 475 GB
	 *
	 */
	//return 2500;
	//return 29808;
	//return 209;
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

/* We store a tm entry for every 4096 block
 *
 * This accounts for the blks in the main area and excludes the blocks
 * for the metadata. Hence we do NOT call get_nr_blks()
 */
unsigned long get_tm_blk_count(struct lsdm_sb *sb)
{
	u64 nr_tm_entries = (sb->max_pba / NR_SECTORS_IN_BLK);
	u32 nr_tm_entries_per_blk = BLK_SZ/ sizeof(struct tm_entry);
	u64 nr_tm_blks = nr_tm_entries / nr_tm_entries_per_blk;
	if (nr_tm_entries % nr_tm_entries_per_blk)
		nr_tm_blks++;

	printf("\n %s blocksize: %d sizeof(struct tm_entry): %ld ", __func__, BLK_SZ, sizeof(struct tm_entry));
	printf("\n %s nr_tm_entries: %llu ", __func__, nr_tm_entries);
	printf("\n %s nr_tm_entries_per_blk: %d ", __func__, nr_tm_entries_per_blk);
	printf("\n %s nr_tm_blks: %llu",  __func__, nr_tm_entries / nr_tm_entries_per_blk);	
	printf("\n %s nr_tm_count: %llu", __func__, nr_tm_blks);
	return nr_tm_blks;
}

__le32 get_nr_zones(struct lsdm_sb *sb)
{
	return sb->zone_count;
}

/* We write all the metadata blks sequentially in the CMR zones
 * that can be rewritten in place
 */
unsigned int get_metadata_zone_count(struct lsdm_sb *sb)
{
	/* we add the 2 for the superblock */
	unsigned long tm_blk_count = get_tm_blk_count(sb);
	unsigned long nr_blks_in_zone = (1 << sb->log_zone_size - sb->log_block_size);
	unsigned int tm_zone_count = tm_blk_count / nr_blks_in_zone;
	unsigned int metadata_zone_count = STATIC_MD_ZONE_COUNT +  tm_zone_count;
	if (tm_blk_count % nr_blks_in_zone)
		tm_zone_count = tm_zone_count + 1;

	metadata_zone_count = metadata_zone_count + tm_zone_count;
	
	printf("\n\n");
	printf("\n ********************************************\n");
	printf("\n tm_blk_count: %lu ", tm_blk_count);
	printf("\n tm_zone_count: %u", tm_zone_count);

	printf("\n metadata_zone_count: %u", metadata_zone_count);
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
	main_zone_count = sb->zone_count - get_metadata_zone_count(sb) - sb->zone_count_reserved;
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
	//return reserved_zone_count;
	return 0;
}

void read_sb(int fd, unsigned long sectornr)
{
	struct lsdm_sb *sb;
	int ret = 0;
	unsigned long long offset = sectornr * SECTOR_SIZE;

	sb = (struct lsdm_sb *)malloc(BLK_SZ);
	if (!sb)
		exit(-1);
	memset(sb, 0, BLK_SZ);

	printf("\n *********************\n");

	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("\n Could not lseek64: ");
		printf("\n");
		exit(errno);
	}

	ret = read(fd, sb, BLK_SZ);
	if (ret < 0) {
		perror("\n COuld not read the sb: ");
		printf("\n");
		exit(errno);
	}
	if (sb->magic != STL_SB_MAGIC) {
		printf("\n wrong superblock!");
		printf("\n");
		exit(-1);
	}
	printf("\n sb->magic: %d", sb->magic);
	printf("\n sb->version %d", sb->version);
	printf("\n sb->log_sector_size %d", sb->log_sector_size);
	printf("\n sb->log_block_size %d", sb->log_block_size);
	printf("\n sb->log_zone_size: %d", sb->log_zone_size);
	printf("\n sb->max_pba: %llu ", sb->max_pba);
	//printf("\n sb-> %d", sb->);
	printf("\n sb->zone_count: %d", sb->zone_count);
	printf("\n Read verified!!!");
	printf("\n ==================== \n");
	free(sb);
}

__le64 get_max_pba(struct lsdm_sb *sb)
{
	/* We test with a disk of size 1 TB, that is 4032 zones! */
	printf("\n %s zone_count: %d log_zone_size: %d log_sector_size: %d ", __func__, sb->zone_count , sb->log_zone_size, sb->log_sector_size);
	return (sb->zone_count << (sb->log_zone_size - sb->log_sector_size)); 
}

void write_zeroed_blks(int fd, sector_t pba, unsigned nr_blks)
{
	char buffer[BLK_SZ];
	int i, ret;
	u64 offset = pba * SECTOR_SIZE;

	printf("\n Writing %d zeroed blks, from pba: %llu", nr_blks, pba);
	
	ret = lseek64(fd, offset, SEEK_SET);
	if (ret == -1) {
		perror("!! (before write) Error in lseek64: \n");
		printf("\n write to disk offset: %llu, sectornr: %lld ret: %d", offset, pba, ret);
		printf("\n");
		exit(errno);
	}
	memset(buffer, 0, BLK_SZ);
	for (i=0; i<nr_blks; i++) {
		//printf("\n i: %d ", i);
		ret = write(fd, buffer, BLK_SZ);
		if (ret < 0) {
			perror("Error while writing: ");
			printf("\n");
			exit(errno);
		}
	}
}

unsigned int get_zone0()
{
	int zonenr = STATIC_MD_ZONE_COUNT + 1; /* 1 for TM */
	return zonenr;
}

unsigned long long get_current_gc_frontier(struct lsdm_sb *sb, int fd)
{
	int zonenr = get_zone_count(fd);

	//zonenr = zonenr - 20000;
	zonenr = get_zone0() + 10;
	return (zonenr) * (1 << (sb->log_zone_size - sb->log_sector_size));
}


struct lsdm_sb * write_sb(int fd, unsigned long sb_pba, unsigned long cmr)
{
	struct lsdm_sb *sb;
	int ret = 0;
	unsigned int zonesz, logzonesz, zone_count;
	char str[SECTOR_SIZE];

	sb = (struct lsdm_sb *)malloc(BLK_SZ);
	if (!sb)
		exit(-1);
	memset(sb, 0, BLK_SZ);


	if (ioctl(fd, BLKGETZONESZ, &zonesz) < 0) {
		sprintf(str, "Get zone size failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		printf("\n");
		exit(errno);
	}
	printf("\n ZONE size: %u", zonesz);
	
	logzonesz = 0;
	while(1){
		zonesz = zonesz >> 1;
		if (!zonesz)
			break;
		logzonesz++;
	}
	/* since the zonesz is the number of addressable LBAs */
	logzonesz = logzonesz + 9;
	printf("\n ********************************* LOG ZONE SZ: %d ", logzonesz);
	if (ioctl(fd, BLKGETNRZONES, &zone_count) < 0) {
		sprintf(str, "Get nr of zones ioctl failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		printf("\n");
		exit(errno);
	}

	printf("\n Nr of zones reported by disk :%u", zone_count);

	sb->magic = STL_SB_MAGIC;
	sb->version = 1;
	sb->log_sector_size = 9;
	/* zonesz that we get from the BLKGETZONESZ ioctl is the number of LBAs in the device */
	zonesz = zonesz << sb->log_sector_size;

	sb->log_block_size = 12;
	sb->log_zone_size = logzonesz;
	sb->checksum_offset = offsetof(struct lsdm_sb, crc);
	sb->zone_count = get_zone_count(fd);
	/* For now we are shunting the 7TB disk to a size of 4TB */
	printf("\n sb->zone_count: %d", sb->zone_count);
    	sb->zone_count_reserved = get_reserved_zone_count(sb);
	printf("\n sb->zone_count_reserved: %d", sb->zone_count_reserved);
	sb->max_pba = get_max_pba(sb);
	printf("\n ******* sb->max_pba: %llu", sb->max_pba);
	sb->nr_lbas_in_zone = (1 << (sb->log_zone_size - sb->log_sector_size));
	printf("\n nr_lbas_in_zone: %u ", sb->nr_lbas_in_zone);
	sb->nr_cmr_zones = cmr;
	printf("\n sb->nr_cmr_zones: %u", sb->nr_cmr_zones);
	sb->zone_count_main = get_main_zone_count(sb);
	printf("\n sb->zone_count_main: %d", sb->zone_count_main);
	int zonenr = get_zone0();
	sb->zone0_pba = (zonenr) * (1 << (sb->log_zone_size - sb->log_sector_size));
	sb->crc = 0;
	//sb->crc = crc32(-1, (unsigned char *)sb, STL_SB_SIZE);
	ret = write_to_disk(fd, (char *)sb, 0); 
	if (ret < 0) {
		printf("\n Could not write SB to the disk \n");
		exit(-1);
	}
	ret = write_to_disk(fd, (char *)sb, 8); 
	if (ret < 0) {
		printf("\n Could not write SB to the disk \n");
		exit(-1);
	}
	return sb;
}

void set_bitmap(char *bitmap, unsigned int nrzones, char ch)
{
	int nrbytes = nrzones / 8;
	if (nrzones % 8 > 0)
		nrbytes = nrbytes + 1;
	memset(bitmap, ch, nrbytes);
}


struct lsdm_ckpt * read_ckpt(int fd, struct lsdm_sb * sb, unsigned long ckpt_pba)
{
	struct lsdm_ckpt *ckpt;
	u64 offset = ckpt_pba * SECTOR_SIZE;
	int ret;

	ckpt = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, BLK_SZ);
	printf("\n Verifying ckpt contents !!");
	printf("\n Reading from sector: %ld", ckpt_pba);

	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("in read_ckpt lseek64 failed: ");
		printf("\n");
		exit(-1);
	}

	ret = read(fd, ckpt, BLK_SZ);
	if (ret < 0) {
		printf("\n read from disk offset: %llu, sectornr: %ld ret: %d", offset, ckpt_pba, ret);
		perror("\n Could not read from disk because: ");
		printf("\n");
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

void write_ckpt(int fd, struct lsdm_sb *sb)
{
	unsigned long ckpt_pba = 1 << (sb->log_zone_size - sb->log_sector_size);
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
	ckpt->tt_first_zone = 0;
	ckpt->tt_curr_zone = 0;
	ckpt->tt_curr_offset = 0;
	ckpt->nr_free_zones = sb->zone_count_main - 2; /* 1 for the current frontier and gc frontier */
	ckpt->blk_count_tm = 0;
	ckpt->elapsed_time = 0;
	ckpt->crc = 0;
	printf("\n-----------------------------------------------------------\n");
	printf("\n checkpoint written at: %lu cur_frontier_pba: %lld", ckpt_pba, ckpt->hot_frontier_pba);

	write_to_disk(fd, (char *)ckpt, ckpt_pba);

	printf("\n Checkpoint written at pba: %lu", ckpt_pba);
	printf("\n ckpt->magic: %d ckpt->hot_frontier_pba: %lld", ckpt->magic, ckpt->hot_frontier_pba);
	printf("\n sb->zone0_pba: %llu", sb->zone0_pba);
	printf("\n warm_gc_frontier_pba: %llu" , ckpt->warm_gc_frontier_pba);

	ckpt1 = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt1)
		exit(-1);

	ret = lseek64(fd, (ckpt_pba * SECTOR_SIZE), SEEK_SET);
	if (ret == -1) {
		perror("!! (before write) Error in lseek64: \n");
		printf("\n write to disk offset: %lu, sectornr: %ld ret: %d", (ckpt_pba * SECTOR_SIZE) , ckpt_pba, ret);
		printf("\n");
		exit(errno);
	}
	ret = read(fd, ckpt1, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing: ");
		printf("\n");
		exit(errno);
	}

	printf("\n Read ckpt, ckpt->magic: %d", ckpt1->magic);
	printf("\n hot_frontier_pba: %llu" , ckpt1->hot_frontier_pba);
	printf("\n warm_gc_frontier_pba: %llu" , ckpt1->warm_gc_frontier_pba);
	printf("\n ckpt->nr_free_zones: %u", ckpt1->nr_free_zones);
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

int write_ttzone_map(int fd, struct lsdm_sb *sb)
{
	struct tt_zone_map *tt_zone_map;
	unsigned int pba = (SBZN_COUNT + CKPTZN_COUNT) << (sb->log_zone_size - sb->log_sector_size);

	tt_zone_map = malloc(BLK_SZ);
	if (!tt_zone_map) {
		perror("Could not allocate tt_zone_map, error: ");
		return -ENOMEM;
	}
	memset(tt_zone_map, 0, BLK_SZ);
	tt_zone_map->lzonenr = 0;
	tt_zone_map->pzonenr = STATIC_MD_ZONE_COUNT;
	write_to_disk(fd, (char *)tt_zone_map, pba);
}

void report_zone(unsigned int fd, unsigned long zonenr, struct blk_zone * bzone)
{
	int ret;
	long i = 0;
	struct blk_zone_report *bzr;


	printf("\n-----------------------------------");
	printf("\n %s Zonenr: %ld ", __func__, zonenr);
	bzr = malloc(sizeof(struct blk_zone_report) + sizeof(struct blk_zone));
	bzr->sector = bzone->start;
	bzr->nr_zones = 1;

	ret = ioctl(fd, BLKREPORTZONE, bzr);
	if (ret) {
		fprintf(stderr, "\n blkreportzone for zonenr: %ld ioctl failed, ret: %d ", zonenr, ret);
		perror("\n blkreportzone failed because: ");
		printf("\n");
		return;
	}
	assert(bzr->nr_zones == 1);
	printf("\n start: %lld ", bzr->zones[0].start);
	printf("\n len: %lld ", bzr->zones[0].len);
	printf("\n state: %d ", bzr->zones[0].cond);
	printf("\n reset recommendation: %d ", bzr->zones[0].reset);
	assert(bzr->zones[0].reset == 0);
	printf("\n wp: %llu ", bzr->zones[0].wp);
	assert(bzr->zones[0].wp == bzr->zones[0].start);
	printf("\n non_seq: %d ", bzr->zones[0].non_seq);
	printf("\n-----------------------------------\n"); 
	return;	
}

long reset_shingled_zones(int fd)
{
	int ret;
	long i = 0;
	long zone_count = 0;
	struct blk_zone_report * bzr;
	struct blk_zone_range bz_range;
	long cmr = 0;


	zone_count = get_zone_count(fd);

	printf("\n Nr of zones: %ld ", zone_count);

	bzr = malloc(sizeof(struct blk_zone_report) + sizeof(struct blk_zone) * zone_count);

	bzr->sector = 0;
	bzr->nr_zones = zone_count;

	ret = ioctl(fd, BLKREPORTZONE, bzr);
	if (ret) {
		fprintf(stdout, "\n blkreportzone for zonenr: %d ioctl failed, ret: %d ", 0, ret);
		perror("\n blkreportzone failed because: ");
		printf("\n");
		return -EINVAL;
	}

	printf("\n nr of zones reported: %d", bzr->nr_zones);
	assert(bzr->nr_zones == zone_count);

	if (zone_count < 1024) {
		bzr->nr_zones = zone_count;
	} else {
		bzr->nr_zones = 1024;
	}

	printf("\n zone_count: %d ", bzr->nr_zones);
	for (i=0; i<bzr->nr_zones; i++) {
		if ((bzr->zones[i].type == BLK_ZONE_TYPE_SEQWRITE_PREF)  || 
		   (bzr->zones[i].type == BLK_ZONE_TYPE_SEQWRITE_REQ)) {
			bz_range.sector = bzr->zones[i].start;
			bz_range.nr_sectors = bzr->zones[i].len;
			ret = ioctl(fd, BLKRESETZONE, &bz_range); 
			if (ret) {
				fprintf(stdout, "\n Could not reset zonenr with sector: %lld", bz_range.sector);
				perror("\n blkresetzone failed because: ");
				printf("\n");
			}
			//report_zone(fd, i, &bzr->zones[i]);
		} else {
			printf("\n zonenr: %ld is a non shingled zone! ", i);
			cmr++;
		}
	}
	return cmr;
}

/*
 *
 * SB1, SB2, Revmap, Translation Map, Revmap Bitmap, CKPT1, CKPT2, SIT, Dataa
 *
 */

int main(int argc, char * argv[])
{
	unsigned int pba = 0;
	struct lsdm_sb *sb1;
	char cmd[256];
	unsigned long nrblks;
	unsigned int ret = 0;
	long cmr;
	char * blkdev;
	int fd;

	printf("\n %s argc: %d \n ", __func__, argc);
	if (argc != 2) {
		fprintf(stderr, "\n Usage: %s device-name \n", argv[0]);
		exit(EXIT_FAILURE);
	}
	blkdev = argv[1];
	fd = open_disk(blkdev);
	cmr = reset_shingled_zones(fd);
	printf("\n Number of cmr zones: %ld ", cmr);
	sb1 = write_sb(fd, 0, cmr);
	printf("\n Superblock written at pba: %d", pba);
	printf("\n sizeof sb: %ld", sizeof(struct lsdm_sb));
	read_sb(fd, 0);
	read_sb(fd, 8);
	printf("\n Superblock written at pba: %d", pba + NR_SECTORS_IN_BLK);
	write_ckpt(fd, sb1);
	write_ttzone_map(fd, sb1);
	nrblks = get_nr_blks(sb1);
	printf("\n nrblks: %lu", nrblks);
	//write_zeroed_blks(fd, 0, nrblks);
	printf("\n nrblks: %lu", nrblks);
	printf("\n sb1->zone_count: %d", sb1->zone_count);
	pba = 0;
	free(sb1);
	/* 0 volume_size: 39321600  lsdm  blkdev: /dev/vdb tgtname: TL1 zone_lbas: 524288 data_end: 41418752 */
	unsigned long zone_lbas = 0;
	char str[SECTOR_SIZE];

	/*
	if (ioctl(fd, BLKGETZONESZ, &zone_lbas) < 0) {
		sprintf(str, "Get zone size failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		printf("\n");
		exit(errno);
	}*/

	zone_lbas = 524288;
	printf("\n # lbas in ZONE: %lu", zone_lbas);
	close(fd);
	unsigned long data_zones = sb1->zone_count_main;
	printf("\n sb1->zone_count_main: %ld ", data_zones);
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
