#include <stdio.h>

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
#include <unistd.h>

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



#define BLK_SZ 4096
#define NR_CKPT_COPIES 2 
#define NR_BLKS_SB 2
#define NR_SECTORS_IN_BLK 8
#define SECTORS_SHIFT 3
#define BITS_IN_BYTE 8
#define NR_BLKS_IN_ZONE 65536

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
	u64 offset = sectornr * 512;

	if (fd < 0) {	
		printf("\n Invalid, closed fd sent!");
		return -1;
	}

	//printf("\n %s writing at sectornr: %d", __func__, sectornr);
	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %d ret: %d", offset, sectornr, ret);
		perror("!! (before write) Error in lseek64: \n");
		exit(errno);
	}
	ret = write(fd, buf, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing: ");
		exit(errno);
	}
	return (ret);
}

void read_sb(int fd, unsigned long sectornr, struct lsdm_sb **sb)
{
	int ret = 0;
	unsigned long long offset = sectornr * 512;

	*sb = (struct lsdm_sb *)malloc(BLK_SZ);
	if (!*sb)
		exit(-1);
	memset(*sb, 0, BLK_SZ);

	printf("\n *********************\n");

	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("\n Could not lseek64: ");
		exit(errno);
	}

	ret = read(fd, *sb, BLK_SZ);
	if (ret < 0) {
		perror("\n COuld not read the sb: ");
		exit(errno);
	}
	if ((*sb)->magic != STL_SB_MAGIC) {
		printf("\n wrong superblock!");
		exit(-1);
	}
	return;
}

char * read_block(int fd, sector_t pba, unsigned nr_blks)
{
	int i, ret, j;
	char * ptr;

	ptr = (char *) malloc(4096);
	if (!ptr)
		return NULL;

	printf("\n Reading %d zeroed blks, from pba: %llu", nr_blks, pba);
	
	/* lseek64 offset is in  bytes not sectors, pba is in sectors */
	ret = lseek64(fd, (pba * 512), SEEK_SET);
	if (ret < 0) {
		perror(">>>> (before read) Error in lseek64: \n");
		exit(errno);
	}
	for (i=0; i<nr_blks; i++) {
		ret = read(fd, ptr, 4096);
		if (ret < 4096) {
			perror("Error while reading: ");
			exit(errno);
		}
	}
	return ptr;
}

/* Each zone has NR_BLKS_IN_ZONE ie 65536 entries */
void write_tm(int fd, struct lsdm_sb *sb, int nr_zones)
{
	sector_t tm_pba;
	unsigned int nr_blks;
	struct tm_entry *entry;
	char buffer[BLK_SZ];
	u64 offset;
	unsigned remaining_entries;
	unsigned long long data_pba;
	int i, j, ret;

	u64 nr_tm_entries = (nr_zones * NR_BLKS_IN_ZONE);
	u32 nr_tm_entries_per_blk = BLK_SZ/ sizeof(struct tm_entry);
        u64 nr_tm_blks = nr_tm_entries / nr_tm_entries_per_blk;
	if (nr_tm_entries % nr_tm_entries_per_blk)
		nr_tm_blks++;

	entry = (struct tm_entry *) malloc(sizeof(struct tm_entry));
	if (!entry) {
		perror("\n Cannot malloc: ");
		printf("\n");
		exit(-1);
	}

	printf("\n ** %s Writing tm blocks at pba: %llu, nrblks: %u", __func__, tm_pba, nr_blks);

	offset = sb->tm_pba * SECTOR_SIZE;
	data_pba = sb->zone0_pba;
	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %lld ret: %d", offset, tm_pba, ret);
		perror("!! (before write) Error in lseek64: \n");
		exit(errno);
	}


	remaining_entries = nr_tm_entries;
	printf("\n remaining entries: %llu #entries per blk: %llu nr_tm_blks: %llu", remaining_entries, nr_tm_entries_per_blk, nr_tm_blks);
	unsigned long long lba = 0;
	for(i=0; i<nr_tm_blks; i++) {
		memset(buffer, 0, BLK_SZ);
		memset(entry, 0, sizeof(struct tm_entry));
		offset = 0;
		for(j=0; j<nr_tm_entries_per_blk; j++) {
			entry->pba = data_pba;
			memcpy(buffer + offset, entry, sizeof(struct tm_entry));
			if ((lba == 9437184) || (data_pba == 9437184)) 
				printf("\n >>>>>>>>>>>>>>>>>>>>>>> lba: %llu, pba: %llu ", lba, data_pba);
			data_pba = data_pba + NR_SECTORS_IN_BLK;
			lba = lba + NR_SECTORS_IN_BLK;
			assert(data_pba < sb->max_pba);
			offset = offset + sizeof(struct tm_entry);
			remaining_entries--;
			if (!remaining_entries)
				break;
		}
		ret = write(fd, buffer, BLK_SZ);
		if (ret < 0) {
			perror("Error while writing: ");
			exit(errno);
		}
	}
	printf("\n lba: %llu, pba: %llu ", lba, data_pba);
	//read_block(fd, tm_pba, nr_blks);
}

void write_sit(int fd, struct lsdm_sb *sb,  unsigned int nr_zones)
{
	unsigned entries_in_blk = BLK_SZ / sizeof(struct lsdm_seg_entry);
	unsigned int nr_sit_blks = (nr_zones)/entries_in_blk;
	struct lsdm_seg_entry *entry;
	int i, j, remaining_entries = 0, mtime, ret;
	char buffer[BLK_SZ];
	u64 offset;

	if (nr_zones % entries_in_blk > 0)
		nr_sit_blks = nr_sit_blks + 1;

	entry = (struct lsdm_seg_entry *) malloc(sizeof(struct lsdm_seg_entry));
	if (!entry) {
		perror("\n Cannot allocate: ");
		printf("\n");
		exit(-1);
	}
	printf("\n writing seg info tables at pba: %llu", sb->sit_pba);
	printf("\n zone_count: %d ", nr_zones);
	printf("\n nr of sit blks: %d", nr_sit_blks);
	printf("\n");

	offset = sb->sit_pba * SECTOR_SIZE;
	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %lld ret: %d", offset, sb->sit_pba, ret);
		perror("!! (before write) Error in lseek64: \n");
		exit(errno);
	}

	mtime = 10;
	remaining_entries = nr_zones;
	for(i=0; i<nr_sit_blks; i++) {
		memset(buffer, 0, BLK_SZ);
		offset = 0;
		for(j=0; j<entries_in_blk; j++) {
			entry->vblocks = NR_BLKS_IN_ZONE;
			entry->mtime = mtime;
			mtime = mtime + 10;
			memcpy(buffer + offset, entry, sizeof(struct lsdm_seg_entry));
			offset = offset + sizeof(struct lsdm_seg_entry);
			remaining_entries--;
			if (!remaining_entries)
				break;
		}
		ret = write(fd, buffer, BLK_SZ);
		if (ret < 0) {
			perror("Error while writing: ");
			exit(errno);
		}
	}
}

unsigned long get_zone_nr(struct lsdm_sb * sb, unsigned long pba)
{
	unsigned long nr_lbas_in_zone = (1 << (sb->log_zone_size - sb->log_sector_size));
	unsigned long phys_zone =  (pba % nr_lbas_in_zone);
	return (phys_zone - sb->zone0_pba / nr_lbas_in_zone);
}

int read_seg_entries_from_block(struct lsdm_sb *sb, struct lsdm_ckpt *ckpt, struct lsdm_seg_entry *entry, unsigned int nr_seg_entries, unsigned int *zonenr)
{
        int i = 0;
        unsigned int nr_blks_in_zone;
	int free = 0;

        nr_blks_in_zone = (1 << (sb->log_zone_size - sb->log_block_size));
        //printk("\n Number of seg entries: %u", nr_seg_entries);

        while (i < nr_seg_entries) {
                /* 0th zonenr is the zone that holds all the metadata */
                if ((*zonenr == get_zone_nr(sb, ckpt->hot_frontier_pba)) ||
                    (*zonenr == get_zone_nr(sb, ckpt->warm_gc_frontier_pba))) {
                        /* 1 indicates zone is free, 0 is the default bit because of kzalloc */
                        printf("\n zonenr: %d vblocks: %llu is our cur_frontier! not marking it free!", *zonenr, entry->vblocks);
                        entry = entry + 1;
                        *zonenr= *zonenr + 1;
                        i++;
                        continue;
                }
                if (entry->vblocks == 0) {
			free++;
                }
                entry = entry + 1;
                *zonenr= *zonenr + 1;
                i++;
        }
        return free;
}

int read_seg_info_table(struct lsdm_sb *sb, struct lsdm_ckpt *ckpt, int fd)
{
	unsigned int nrblks = 0, sectornr = 0;
	int nr_seg_entries_blk = BLK_SZ / sizeof(struct lsdm_seg_entry);
	int free = 0;
	struct lsdm_seg_entry *entry0;
	unsigned int zonenr = 0;
	unsigned long nr_data_zones;
	unsigned long nr_seg_entries_read;
	char * ptr;

	nr_data_zones = sb->zone_count_main; /* these are the number of segment entries to read */
	nr_seg_entries_read = 0;
	
	printf("\n nr_data_zones: %llu", nr_data_zones);

	nrblks = sb->blk_count_sit;
	sectornr = sb->sit_pba;
	printf("\n ckpt->hot_frontier_pba: %llu", ckpt->hot_frontier_pba);
	printf("\n ckpt->warm_gc_frontier_pba: %llu", ckpt->warm_gc_frontier_pba);
	printf("\n %s Read seginfo from pba: %llu sectornr: %d zone0_pba: %llu \n", __func__, sb->sit_pba, sectornr, sb->zone0_pba);
	while (nr_data_zones > 0) {
		//trace_printk("\n zonenr: %u", zonenr);
		if ((sectornr + sb->sit_pba) > sb->zone0_pba) {
			printf("\n Seg entry blknr cannot be bigger than the data blknr");
			return -1;
		}
		ptr = read_block(fd, sectornr, 1);
		if (!ptr) {
			printf("\n could not read! \n");
			exit(-1);
		}
		entry0 = (struct lsdm_seg_entry *) ptr;
		if (nr_data_zones > nr_seg_entries_blk) {
			nr_seg_entries_read = nr_seg_entries_blk;
		}
		else {
			nr_seg_entries_read = nr_data_zones;
		}
		nr_data_zones = nr_data_zones - nr_seg_entries_read;
		free += read_seg_entries_from_block(sb, ckpt, entry0, nr_seg_entries_read, &zonenr);
		sectornr = sectornr + NR_SECTORS_IN_BLK;
	}
	printf("\n %s nr_freezones (2) : %u ckpt->freezones: %llu", __func__, free, ckpt->nr_free_zones);
	return 0;
}

void write_ckpt(int fd, struct lsdm_sb * sb, unsigned nr_zones)
{
	struct lsdm_ckpt *ckpt, *ckpt1;
	int ret;
	unsigned long ckpt_pba = sb->ckpt1_pba;
	char buffer[BLK_SZ];

	ckpt = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, BLK_SZ);
	ckpt->magic = STL_CKPT_MAGIC;
	ckpt->version = 0;
	ckpt->user_block_count = sb->zone_count_main << (sb->log_zone_size - sb->log_block_size);
	ckpt->nr_invalid_zones = 0;
	ckpt->hot_frontier_pba = sb->zone0_pba + ((__le64) nr_zones << (sb->log_zone_size - sb->log_sector_size));
	printf("\n <3 <3 <3 <3 <3 <3 <3...................     ckpt->hot_frontier_pba = %llu ", ckpt->hot_frontier_pba);
	/* Next zone is gc frontier */
	ckpt->warm_gc_frontier_pba = ckpt->hot_frontier_pba + (1 << (sb->log_zone_size - sb->log_sector_size));
	printf("\n <3 <3 <3 <3 <3 <3 <3...................     ckpt->warm_frontier_pba = %llu ", ckpt->hot_frontier_pba);
	printf("\n");
	printf("\n last_gc_pba: %llu , max_pba: %llu \n", ckpt->warm_gc_frontier_pba +  (1 << (sb->log_zone_size - sb->log_sector_size)), sb->max_pba);
	assert(ckpt->warm_gc_frontier_pba  <= sb->max_pba);
	ckpt->nr_free_zones = sb->zone_count_main - 2 - nr_zones; //1 for the current frontier and gc frontier
	ckpt->elapsed_time = 0;
	ckpt->clean = 1;  /* 1 indicates clean datastructures */
	ckpt->crc = 0;
	printf("\n-----------------------------------------------------------\n");
	printf("\n checkpoint written at: %llu cur_frontier_pba: %lld", ckpt_pba, ckpt->hot_frontier_pba);

	u64 offset = sb->ckpt1_pba * SECTOR_SIZE;

	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %d ret: %d", offset, ckpt_pba, ret);
		perror("!! (before write) Error in lseek64: \n");
		exit(errno);
	}

	memset(buffer, 0, BLK_SZ);
	memcpy(buffer, ckpt, sizeof(struct lsdm_ckpt));
	ret = write(fd, buffer, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing ckpt1: ");
		exit(errno);
	}

	offset = sb->ckpt2_pba * 512;
	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %d ret: %d", offset, ckpt_pba, ret);
		perror("!! (before write) Error in lseek64: \n");
		exit(errno);
	}
	ret = write(fd, ckpt, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing ckpt2: ");
		exit(errno);
	}

	printf("\n 1) ckpt->nr_free_zones: %llu", ckpt->nr_free_zones);
	printf("\n Checkpoint written at pba: %llu", ckpt_pba);
	printf("\n ckpt->magic: %d ckpt->hot_frontier_pba: %llu", ckpt->magic, ckpt->hot_frontier_pba);
	printf("\n sb->zone0_pba: %llu", sb->zone0_pba);
	printf("\n warm_gc_frontier_pba: %llu" , ckpt->warm_gc_frontier_pba);


	ckpt1 = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt1)
		exit(-1);

	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		printf("\n write to disk offset: %u, sectornr: %d ret: %d", offset, ckpt_pba, ret);
		perror("!! (before write) Error in lseek64: \n");
		exit(errno);
	}
	ret = read(fd, ckpt1, BLK_SZ);
	if (ret < 0) {
		perror("Error while writing: ");
		exit(errno);
	}

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
	read_seg_info_table(sb, ckpt1, fd);
	free(ckpt);
	free(ckpt1);
	printf("\n checkpoint written to disk ");
}

/*
 *
 * SB1, SB2, Revmap, Translation Map, Revmap Bitmap, CKPT1, CKPT2, SIT, Dataa
 *
 */

int main(int argc, char * argv[])
{
	unsigned int pba = 0;
	struct lsdm_sb *sb;
	char cmd[256];
	unsigned long nrblks;
	unsigned int ret = 0, nr_zones = 0;
	long cmr;
	char * blkdev;
	int fd;

	printf("\n %s argc: %d \n ", __func__, argc);
	if (argc != 3) {
		fprintf(stderr, "\n Usage: %s device-name size \n", argv[0]);
		exit(EXIT_FAILURE);
	}
	blkdev = argv[1];
	fd = open_disk(blkdev);
	/* size in GB */
	nr_zones = atoi(argv[2]);
	if (nr_zones == 0) {
		printf("\n");
		return 0;
	}
	read_sb(fd, 0, &sb);
	if (nr_zones > (sb->zone_count_main - 2)) {
		printf("\n Device has %llu zones, cannot populate more!", sb->zone_count_main);
		printf("\n");
		return 0;
	}
	write_tm(fd, sb,  nr_zones);
	write_sit(fd, sb, nr_zones);
	write_ckpt(fd, sb, nr_zones);
	printf("\n Populated %d zones ", nr_zones);
	printf("\n \n");
	return(0);
}
