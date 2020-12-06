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

int write_to_disk(int fd, char *buf, int len, int sectornr)
{
	int ret = 0;
	unsigned long long offset = sectornr * 512;

	//printf("\n write to disk offset: %d", offset);

	ret = lseek(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("Error in lseek: ");
		exit(errno);
	}
	ret = write(fd, buf, len);
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
__le64 get_nr_blks(struct stl_sb *sb)
{
	unsigned long nr_blks_per_zone = (1 << (sb->log_zone_size - sb->log_block_size));
	unsigned long nr_blks =  (sb->zone_count * nr_blks_per_zone);
	return nr_blks;

}

__le32 get_map_blk_count(struct stl_sb *sb)
{
	unsigned int one_extent_sz = 16; /* in bytes */
	unsigned int nr_extents_in_blk = BLK_SZ / one_extent_sz;
	__le64 nr_extents = get_nr_blks(sb);
	unsigned int blks_for_extents = nr_extents / nr_extents_in_blk;
	if (nr_extents % nr_extents_in_blk > 0)
		blks_for_extents = blks_for_extents + 1;

	return blks_for_extents;
}

__le32 get_nr_zones(struct stl_sb *sb)
{
	return sb->zone_count;
}

__le32 get_sit_blk_count(struct stl_sb *sb)
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
__le32 get_metadata_zone_count(struct stl_sb *sb)
{
	/* we add the 2 for the superblock */
	unsigned int metadata_blk_count = NR_BLKS_SB + sb->blk_count_ckpt + sb->blk_count_map + sb->blk_count_sit;
	unsigned int nr_blks_in_zone = (1 << sb->log_zone_size)/(1 << sb->log_block_size);
	unsigned int metadata_zone_count = metadata_blk_count / nr_blks_in_zone;
	if (metadata_blk_count % nr_blks_in_zone > 0)
		metadata_zone_count  = metadata_zone_count + 1;
	return metadata_zone_count;
}

/* we consider the main area to have the 
 * reserved zones
 */
__le32 get_main_zone_count(struct stl_sb *sb)
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
__le64 get_cp_pba()
{
	return (NR_BLKS_SB * NR_SECTORS_IN_BLK);
}

__le64 get_map_pba(struct stl_sb *sb)
{

	return sb->cp_pba + (sb->blk_count_ckpt * NR_SECTORS_IN_BLK);
}


__le64 get_sit_pba(struct stl_sb *sb)
{
	printf("\n sb->map_pba: %d, sb->blk_count_map: %d", sb->map_pba, sb->blk_count_map);
	return sb->map_pba + (sb->blk_count_map * NR_SECTORS_IN_BLK);
}

__le64 get_zone0_pba(struct stl_sb *sb)
{
	return sb->sit_pba + (sb->blk_count_sit * NR_SECTORS_IN_BLK);
}

void read_sb(int fd, unsigned long sectornr)
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
	printf("\n sb->map_pba: %d", sb->map_pba);
	printf("\n Read verified!!!");
	printf("\n ==================== \n");
	free(sb);
}

__le64 get_max_pba(struct stl_sb *sb)
{
	return sb->zone_count * (1 << (sb->log_zone_size - sb->log_sector_size));

}

int get_translation_blks_for_zone(struct stl_sb *sb)
{
	int nr_blks_in_zone = 1 << (sb->log_zone_size - sb->log_block_size);
	int sizeof_each_entry = sizeof(struct stl_ckpt_entry);
	int nr_entries_in_blk = BLK_SIZE / sizeof_each_entry;
	int i = 0;
	int rem_size_of_ckpt_blk = BLK_SIZE - sizeof(struct stl_ckpt);
	int entries_in_ckpt_blk = rem_size_of_ckpt_blk / sizeof_each_entry;
	int rem_entries = nr_blks_in_zone - entries_in_ckpt_blk;
	int nr_blks = rem_entries / nr_entries_in_blk;

	return nr_blks + 1; /* we add the 1 for the CKPT block itself */
}

struct stl_sb * write_sb(int fd, unsigned long sb_pba)
{
	struct stl_sb *sb;
	int ret = 0;
	int translation_blks = 0;

	sb = (struct stl_sb *)malloc(BLK_SZ);
	if (!sb)
		exit(-1);
	memset(sb, 0, BLK_SZ);
	
	sb->magic = STL_SB_MAGIC;
	sb->version = 1;
	sb->log_sector_size = 9;
	sb->log_block_size = 12;
	sb->log_zone_size = 28;
	sb->checksum_offset = offsetof(struct stl_sb, crc);
	sb->zone_count = get_zone_count(sb);
	printf("\n sb->zone_count: %d", sb->zone_count);
	translation_blks = get_translation_blks_for_zone(sb);
	sb->blk_count_ckpt = NR_CKPT_COPIES * translation_blks;
	sb->blk_count_map = get_map_blk_count(sb);
	sb->blk_count_sit = get_sit_blk_count(sb);
    	sb->zone_count_reserved = get_reserved_zone_count(sb);
	sb->zone_count_main = get_main_zone_count(sb);
	sb->cp_pba = get_cp_pba();
	sb->map_pba = get_map_pba(sb);
	sb->sit_pba = get_sit_pba(sb);
	sb->zone0_pba = get_zone0_pba(sb);
	sb->max_pba = get_max_pba(sb);
	printf("\n sb->max_pba: %lld", sb->max_pba);
	sb->crc = 0;
	sb->crc = crc32(-1, (unsigned char *)sb, STL_SB_SIZE);

	printf("\n sb_pba: %ld", sb_pba);
	ret = write_to_disk(fd, (char *)sb, BLK_SZ, sb_pba); 
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

unsigned long long get_current_frontier(struct stl_sb *sb)
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

unsigned long long get_user_block_count(struct stl_sb *sb)
{
	unsigned long sit_end_pba = sb->sit_pba + sb->blk_count_sit * NR_SECTORS_IN_BLK;
	unsigned long sit_zone_nr = sit_end_pba >> sb->log_zone_size;
	return ((sb->zone_count - sit_zone_nr) * (NR_BLKS_PER_ZONE));

}

void prepare_cur_seg_entry(struct stl_seg_entry *entry)
{
	/* Initially no block has any valid data */
	entry->vblocks = 0;
	entry->mtime = 0;
}


void prepare_prev_seg_entry(struct stl_seg_entry *entry)
{
	entry->vblocks = 0;
	entry->mtime = 0;
}

void prepare_ckpt_translation_table(struct stl_ckpt_entry * table)
{
	table->prev_zone_pba = 0;
	table->cur_zone_pba = 0;
	table->prev_count = 0;
	table->cur_count = 0;
}

void write_ckpt(int fd, struct stl_sb * sb, unsigned long ckpt_pba)
{
	struct stl_ckpt *ckpt;
	unsigned int bitmap_sz = sb->zone_count / BITS_IN_BYTE;

	ckpt = (struct stl_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, BLK_SZ);
	ckpt->magic = STL_CKPT_MAGIC;
	ckpt->checkpoint_ver = 0;
	ckpt->user_block_count = sb->zone_count_main << (sb->log_zone_size - sb->log_block_size);
	ckpt->valid_block_count = ckpt->user_block_count;
	ckpt->free_segment_count = sb->zone_count_main - 1; //1 for the current frontier
	ckpt->cur_frontier_pba = get_current_frontier(sb);
	printf("\n checkpoint: cur_frontier_pba: %lld", ckpt->cur_frontier_pba);
	prepare_cur_seg_entry(&ckpt->cur_seg_entry);
	prepare_prev_seg_entry(&ckpt->cur_seg_entry);
	/* the next is not needed as it is writing 0s anyway.
	prepare_ckpt_translation_table(&ckp->ckpt_translation_table);
	*/
	write_to_disk(fd, (char *)ckpt, BLK_SZ, ckpt_pba);
	free(ckpt);
}

void read_ckpt(int fd, struct stl_sb * sb, unsigned long ckpt_pba)
{
	struct stl_ckpt *ckpt;
	unsigned int bitmap_sz = sb->zone_count / BITS_IN_BYTE;
	unsigned long offset = ckpt_pba * 512;
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
	free(ckpt);
}

/*
 *
 * If we were to write extents to signify the blks
 * in the disk, at the beginning all the LBA and PBA
 * are LBA, PBA + some offset, len 
 * But we write the extents only when there is some
 * write.
 * Extents are written on the first writes.
 * On first overwrites, the LBA changes and the
 * extents change.
 * Initially there is nothing in the extent space
 * as nothing is mapped. So we write all zeroes.
*/

void write_map(int fd, u64 nr_extents, u64 map_pba)
{
	unsigned int extents_in_blk = BLK_SZ / sizeof(struct extent_map);
	unsigned int nr_extent_blks = nr_extents / extents_in_blk;
	int i = 0, ret=0;
	if (nr_extents % extents_in_blk > 0)
		nr_extent_blks = nr_extent_blks + 1;

	char * buf = (char *) malloc(BLK_SZ);
	if (!buf)
		exit(-1);

	memset(buf, 0, BLK_SZ);
	printf("\n nr_data_blks: %llu ", nr_extents);
	printf("\n nr_extent_blks: %d", nr_extent_blks);
	while (i < nr_extent_blks) {
		ret = write_to_disk(fd, (char *) buf, BLK_SZ, map_pba);
		if (ret < 0) {
			exit(ret);
		}
		map_pba += NR_SECTORS_IN_BLK;
		i++;
		//printf("\n Written blk: %d", i);
	}
	free(buf);
	printf("\n returning from write_map...");
	return;
}

void write_seg_info_table(int fd, u64 nr_seg_entries, unsigned long seg_entries_pba)
{
	struct stl_seg_entry seg_entry;
	unsigned entries_in_blk = BLK_SZ / sizeof(struct stl_seg_entry);
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
		memcpy(buf + size, &seg_entry, sizeof(struct stl_seg_entry));
		size = size +  sizeof(struct stl_seg_entry);
		i = i + 1;
	}
	printf("\n Buf prepared");
	i = 0;
	buf = orig_addr;
	while (i < nr_sit_blks) {
		ret = write_to_disk(fd, (char *) buf, BLK_SZ, seg_entries_pba);
		if (ret < 0) {
			exit(ret);
		}
		seg_entries_pba += BLK_SZ;
		i++;
	}
	free(buf);
}

int main()
{
	unsigned int pba = 0;
	struct stl_sb *sb1, *sb2;
	char cmd[256];
	unsigned long nrblks;
	unsigned int ret = 0;

	char * blkdev = "/dev/vdb";
	int fd = open_disk(blkdev);

	sb1 = write_sb(fd, 0);
	printf("\n Superblock written at pba: %d", pba);
	printf("\n sizeof sb: %ld", sizeof(struct stl_sb));
	sb2 = write_sb(fd, 8);
	read_sb(fd, 0);
	read_sb(fd, 8);
	printf("\n Superblock written at pba: %d", pba + NR_SECTORS_IN_BLK);
	free(sb2);
	write_ckpt(fd, sb1, sb1->cp_pba);
	printf("\n Checkpoint written at offset: %d", sb1->cp_pba);
	write_ckpt(fd, sb1, sb1->cp_pba + NR_SECTORS_IN_BLK);
	printf("\n Checkpoint written at offset: %d", sb1->cp_pba + NR_SECTORS_IN_BLK);
	nrblks = get_nr_blks(sb1);
	printf("\n nrblks: %lu", nrblks);
	write_map(fd, nrblks, sb1->map_pba);
	printf("\n Extent map written");
	printf("\n sb1->zone_count: %d", sb1->zone_count);
	write_seg_info_table(fd, sb1->zone_count, sb1->sit_pba);
	printf("\n Segment Information Table written");
	pba = 0;
	read_ckpt(fd, sb1, sb1->cp_pba);
	read_ckpt(fd, sb1, sb1->cp_pba + NR_SECTORS_IN_BLK);
	free(sb1);
	close(fd);
	/* 0 volume_size: 39321600  nstl  blkdev: /dev/vdb tgtname: TL1 zone_lbas: 524288 data_end: 41418752 */
	unsigned long zone_lbas = 524288;
	unsigned long data_zones = 28000;
	char * tgtname = "TL1";
	//volume_size = data_zones * zone_lbas;
	unsigned long volume_size = 39321600;
	unsigned long data_end = 41418752;
	sprintf(cmd, "/sbin/dmsetup create TL1 --table '0 %ld nstl %s %s %ld %ld'",
            volume_size, blkdev, tgtname, zone_lbas, 
	    data_end);
	printf("\n cmd: %s", cmd);
    	//system(cmd);
	printf("\n \n");
	return(0);
}
