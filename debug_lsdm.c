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

/* For YUXIN: 
 * Go to all the functions that call write_to_disk(); these functions
 * need to be converted to read them from the disk and then printing
 * them out in a formatted output.
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

int read_from_disk(int fd, char *buf, int len, int sectornr)
{
	int ret = 0;
	unsigned long long offset = sectornr * 512;


	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("Error in lseek: \n");
		exit(errno);
	}
	ret = read(fd, buf, len);
	if (ret < 0) {
		perror("Error while writing: ");
		exit(errno);
	}
	return (ret);
}



__le32 get_zone_count()
{
	__le32 zone_count = 0;
	/* Use zone queries and find this eventually
	 * Doing this manually for now for a 20GB 
	 * harddisk and 256MB zone size.
	 */
	zone_count = 80;
	return zone_count;
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

/* Stores as many entries as there are 4096 blks. 2 zones is 131072
 * entries. That requires 586 blocks and 1 block of bitmap.
 * 1 block of bitmap can accomodate details for 4096 blocks.
 * Hence we say that as many entries as can be accomodated in 4096
 * blocks. One block has 48 entries.
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
	u64 nr_tm_entries = (sb->zone_count_main << (sb->log_zone_size - sb->log_block_size));
	u32 nr_tm_entries_per_blk = BLOCK_SIZE / sizeof(struct tm_entry);
	u64 nr_tm_blks = nr_tm_entries / nr_tm_entries_per_blk;
	if (nr_tm_entries % nr_tm_entries_per_blk)
		nr_tm_blks++;
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
	unsigned int metadata_blk_count = NR_BLKS_SB + sb->blk_count_revmap + sb->blk_count_ckpt + sb->blk_count_revmap_bm + sb->blk_count_tm + sb->blk_count_sit;
	unsigned int nr_blks_in_zone = (1 << sb->log_zone_size)/(1 << sb->log_block_size);
	unsigned int metadata_zone_count = metadata_blk_count / nr_blks_in_zone;
	if (metadata_blk_count % nr_blks_in_zone > 0)
		metadata_zone_count  = metadata_zone_count + 1;
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

void read_revmap(int fd, sector_t revmap_pba, unsigned nr_blks)
{
}

void read_tm(int fd, sector_t tm_pba, unsigned nr_blks)
{
	printf("\n Writing tm blocks at pba: %llu, nrblks: %u", tm_pba/NR_SECTORS_IN_BLK, nr_blks);
}


/* Revmap bitmap: 0 indicates blk is available for writing
 */
void read_revmap_bitmap(int fd, sector_t revmap_bm_pba, unsigned nr_blks)
{
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

unsigned long long get_current_gc_frontier(struct lsdm_sb *sb)
{
	int zonenr = get_zone_count();

	zonenr = zonenr - 20;
	return (zonenr) * (1 << (sb->log_zone_size - sb->log_sector_size));
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

/* YUXIN: read the structure lsdm_ckpt found in format_metadata.h*/
void read_ckpt(int fd, struct lsdm_sb * sb, unsigned long ckpt_pba)
{
	struct lsdm_ckpt *ckpt;
	unsigned int bitmap_sz = sb->zone_count / BITS_IN_BYTE;

	ckpt = (struct lsdm_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, BLK_SZ);
	read_from_disk(fd, (char *)ckpt, BLK_SZ, ckpt_pba);
	printf("\n checkpoint: cur_frontier_pba: %lld", ckpt->cur_frontier_pba);
	free(ckpt);
}

void read_seg_info_table(int fd, u64 nr_seg_entries, unsigned long seg_entries_pba)
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
		ret = read_from_disk(fd, (char *) buf, BLK_SZ, seg_entries_pba);
		if (ret < 0) {
			exit(ret);
		}
		seg_entries_pba += BLK_SZ;
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

	char * blkdev = "/dev/vdb";
	int fd = open_disk(blkdev);

	read_sb(fd, 0);
	read_sb(fd, 8);
	/*
    	read_revmap(fd, sb1->revmap_pba, sb1->blk_count_revmap);
	nrblks = get_nr_blks(sb1);
	printf("\n nrblks: %lu", nrblks);
	printf("\n nrblks: %lu", nrblks);
	read_tm(fd, sb1->tm_pba, sb1->blk_count_tm);
	read_revmap_bitmap(fd, sb1->revmap_bm_pba, sb1->blk_count_revmap_bm);
	read_ckpt(fd, sb1, sb1->ckpt1_pba);
	printf("\n Checkpoint written at offset: %d", sb1->ckpt1_pba);
	read_ckpt(fd, sb1, sb1->ckpt2_pba);
	printf("\n Extent map written");
	printf("\n sb1->zone_count: %d", sb1->zone_count);
	read_seg_info_table(fd, sb1->zone_count, sb1->sit_pba);
	printf("\n Segment Information Table written");
	pba = 0;
	read_ckpt(fd, sb1, sb1->ckpt1_pba);
	free(sb1);
	close(fd);
	*/
	/* 0 volume_size: 39321600  lsdm  blkdev: /dev/vdb tgtname: TL1 zone_lbas: 524288 data_end: 41418752 */
	/*
	unsigned long zone_lbas = 524288;
	unsigned long data_zones = 28000;
	char * tgtname = "TL1";
	//volume_size = data_zones * zone_lbas;
	unsigned long volume_size = 39321600;
	unsigned long data_end = 41418752;
	sprintf(cmd, "/sbin/dmsetup create TL1 --table '0 %ld lsdm %s %s %ld %ld'",
            volume_size, blkdev, tgtname, zone_lbas, 
	    data_end);
	printf("\n cmd: %s", cmd);
    	//system(cmd);
	*/
	printf("\n \n");
	return(0);
}
