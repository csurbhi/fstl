#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stddef.h>
#include <linux/types.h>
#include "format_metadata.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
//#include <zlib.h>

#define ZONE_SZ (256 * 1024 * 1024)
#define BLK_SZ 4096
#define NR_BLKS_PER_ZONE ZONE_SZ /BLK_SZ
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

	fd = open(dname, O_RDWR);
	if (fd < 0) {
		perror("Could not open the disk: ");
		exit(errno);
	}
	return fd;
}

struct stl_sb * read_sb(int fd, unsigned long sectornr)
{
	struct stl_sb *sb;
	int ret = 0;
	unsigned long long offset = sectornr * 512;

	sb = (struct stl_sb *)malloc(BLK_SZ);
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
	return sb;
}

void read_ckpt(int fd, struct stl_sb * sb, unsigned long ckpt_pba)
{
	struct stl_ckpt *ckpt;
	unsigned int bitmap_sz = sb->zone_count / BITS_IN_BYTE;
	//unsigned long offset = ckpt_pba * 512;
	unsigned long offset = 166504 * 512;
	int ret;

	ckpt = (struct stl_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	printf("\n Reading from sector: %ld", ckpt_pba);

	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("in read_ckpt lseek failed: ");
		exit(-1);
	}

	ret = read(fd, ckpt, BLK_SZ);
	printf("\n Read checkpoint, ckpt->cur_frontier_pba: %lld", ckpt->cur_frontier_pba);
	printf("\n ckp->magic: %u", ckpt->magic);
	free(ckpt);
}

void read_tm(int fd, struct stl_sb * sb, unsigned long pba)
{
	struct tm_entry *entry;
	unsigned int bitmap_sz = sb->zone_count / BITS_IN_BYTE;
	unsigned long offset = pba * 512;
	int ret, i, nr_extents;
	char buff[4096];


	nr_extents = BLK_SZ / sizeof(struct tm_entry);
	printf("\n Reading translation map from sector: %ld", pba);

	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("in read_ckpt lseek failed: ");
		exit(-1);
	}

	ret = read(fd, buff, BLK_SZ);
	entry = (struct tm_entry *) buff;
	i = 0;
	while (i < nr_extents) {
		i++;
		if ((entry->lba) || (entry->pba))
			printf("\n lba: %llu, pba: %llu", entry->lba, entry->pba);
	}
}

/*
 *
 * SB1, SB2, Revmap, Translation Map, Revmap Bitmap, CKPT1, CKPT2, SIT, Dataa
 *
 */

int main()
{
	struct stl_sb *sb;
	char * blkdev = "/dev/vdb";
	int fd = open_disk(blkdev);

	sb = read_sb(fd, 0);
	read_ckpt(fd, sb, sb->ckpt1_pba);
	read_tm(fd, sb, sb->tm_pba);
	free(sb);
	close(fd);
	printf("\n \n");
	return(0);
}
