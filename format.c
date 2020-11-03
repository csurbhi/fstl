#include <stdio.h>
#include <metadata.h>
#include <zlib.h>
#include <stdlib.h>


#define ZONE_SZ (256 * 1024 * 1024)
#define BLK_SZ 4096
#define NR_BLKS_PER_ZONE ZONE_SZ /BLK_SZ
#define NR_CKPT_COPIES 2 
#define NR_BLKS_SB 2
#define NR_SECTORS_IN_BLK 8

/* Our LBA is associated with a block
 * rather than a sector
 */

void write_sb_crc(struct stl_sb *sb)
{
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
	unsigned long nr_blks_per_zone = (1 << sb->log_zone_size) / (1 << sb->log_block_size);
	unsigned long nr_blks =  (sb->zone_count * nr_blks_per_zone);
	return nr_blks;

}

__le32 get_map_blk_count()
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

__le32 get_sit_blk_count()
{
	unsigned int one_sit_sz = 80; /* in bytes */
	unsigned int nr_sits_in_pg = PAGE_SIZE / one_sit_sz;
	__le32 nr_sits = get_nr_zones(sb);
	unsigned int blks_for_sit = nr_sits / nr_extents_in_pg;
	if (nr_sits % nr_sits_in_pg > 0)
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
	unsigned int nr_blks_in_zone = (1 << sb->log_zone_size)/(1 << sb->block_size);
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
	reserved_zone_count = RESERVED_ZONES
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
__le64 inline get_cp_pba()
{
	return (NR_BLKS_SB * NR_SECTORS_IN_BLK);
}

/* 
 * We store 2 copies of a checkpoint;
 * one per block
 */

__le64 inline get_map_pba(struct stl_sb *sb)
{

	return sb->cp_pba + (sb->blk_count_ckpt * NR_SECTORS_IN_BLK);
}


__le64 inline get_sit_pba(struct stl_sb *sb)
{
	return sb->map_pba + (sb->blk_count_map * NR_SECTORS_IN_BLK);
}

__le64 get_zone0_pba(struct stl_sb *sb)
{
	return sb->sit_pba + (sb->blk_count_sit * NR_SECTORS_IN_BLK);
}


struct stl_sb * write_sb(u64 sb_pba, unsigned long sb_pba)
{
	struct stl_sb *sb;
	int ret = 0;

	sb = (struct stl_sb *)malloc(BLK_SZ);
	if (!sb)
		exit(-1);
	memset(&sb, 0, BLK_SZ);
	
	sb->magic = STL_SB_MAGIC;
	sb->version = 1;
	sb->log_sector_size = 9;
	sb->block_size = 12;
	sb->log_zone_size = 28;
	sb->checksum_offset = &sb->crc - &sb;
	sb->zone_count = get_zone_count(sb);
	sb->blk_count_ckpt = NR_CKPT_COPIES;
	sb->blk_count_map = get_map_blk_count(sb);
	sb->blk_count_sit = get_sit_blk_count(sb);
    	sb->zone_count_reserved = get_reserved_zone_count(sb);
	sb->zone_count_main = get_main_zone_count(sb);
	sb->cp_pba = get_cp_pba();
	sb->map_pba = get_map_pba(sb);
	sb->sit_pba = get_sit_pba(sb);
	sb->zone0_pba = get_zone0_pba();
	sb->invalid_zones = 0;
	sb->crc = 0;
	sb->crc = crc32(-1, (unsigned char *)sb, STL_SB_SIZE);
	ret = write_to_disk(sb, BLK_SZ, sb_pba); 
	if (ret < 0)
		exit(-1);
	return sb;
}

void prepare_header(struct stl_header *header, unsigned long pba, unsigned long lba)
{
	header->magic = STL_HEADER_MAGIC;
	header->nonce = 0;
	header->crc32 = 0;
	header->flags = 0;
	header->len = 0;
	header->prev_pba = pba; 
	header->next_pba = pba;
	header->lba = lba;
	header->pba = pba;
	header->seq = 1;
}


__le64 get_lba(unsigned int zonenr, unsigned int blknr)
{
	int nrblks = (zonenr * NR_BLKS_PER_ZONE) + blknr;
	return (nrblks * SECTORS_IN_BLK);
}

set_bitmap(char *bitmap, unsigned int nrzones, char ch)
{
	int nrbytes = nrzones / 8;
	if (nrzones % 8 > 0)
		nrblks = nrblks + 1;
	memset(bitmap, ch, nrblks);
}


void write_ckpt(struct stl_sb * sb, unsigned long ckpt_pba)
{
	struct stl_ckpt *ckpt;
	unsigned int bitmap_sz = sb->zone_count / BITS_IN_BYTE;

	ckpt = (struct stl_ckpt *)malloc(BLK_SZ);
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, BLK_SZ);
	ckpt->checkpoint_ver = 0;
	ckpt->valid_block_count = ckpt->user_block_count;
	ckpt->rsvd_segment_count = sb->zone_count_reserved;
	ckpt->free_segment_count = sb->zone_count_main - 1; //1 for the current frontier
	ckpt->cur_frontier = sb->res_zone_end_blk + 1;
	ckpt->blk_nr = 0;
	ckpt->blk_pba = get_lba(ckpt->cur_frontier, ckpt->blk_nr);
	ckpt->user_block_count = (sb->zone_count - ckpt->cur_frontier) * (NR_BLK_PER_ZONE);
	ckpt->elapsed_time = 0;
	prepare_cur_seg_entry();
	prepare_header(&ckpt->header, ckpt->blk_pba, 0);
	write_to_disk(ckpt, BLK_SZ, ckpt_pba);
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

void write_map(u64 nrblks, u64 map_pba)
{
	unsigned int nr_extents = nrblks;
	unsigned int extents_sz = nr_extents * sizeof(struct extent_map);
	unsigned int nr_blks = extents_sz / BLK_SZ;
	int i = 0;
	if (extents_sz % BLK_SZ > 0)
		nr_blks = nr_blks + 1;

	char * buf = (char *) malloc(BLK_SZ);
	if (!buf)
		exit(-1);

	memset(buf, 0, BLK_SZ);
	while (i < nr_blks) {
		ret = write_to_disk(buf, BLK_SZ, map_pba);
		if (ret < 0) {
			exit(ret);
		}
		map_pba += BLK_SZ;
		i++;
	}
	free(buf);
}

void write_seg_info_table(u64 nrzones)
{
	struct stl_seg_entry seg_entry;
	unsigned entries_in_blk = BLK_SZ / sizeof(struct stl_seg_entry);
	unsigned int nr_blks = (BLK_SZ * nrzones)/sizeof(struct stl_seg_entry);
	if (BLKSZ * nrzones % sizeof(struct stl_seg_entry) > 0)
		nr_blks = nr_blks + 1;

	char * buf = (char *) malloc(BLK_SZ);
	if (!buf)
		exit(-ENOMEM);

	memset(buf, 0, BLK_SZ);

	seg_entry.vblocks = NR_BLKS_ZONE;
	seg_entry.mtime = 0;
	while (i < entries_in_blk)
		memcpy(buf, &seg_entry, sizeof(struct stl_seg_entry));
	
	i = 0;
	while (i < nr_blks) {
		ret = write_to_disk(buf, BLK_SZ, seg_entries_pba);
		if (ret < 0) {
			exit(ret);
		}
		seg_entries_pba += BLK_SZ;
		i++;
	}
}

int main()
{
	unsigned int pba = 0;
	struct stl_sb *sb1, *sb2;
	char cmd[256];
	sb1 = write_sb(pba);
	sb2 = write_sb(pba + NR_SECTORS_IN_BLK);
	free(sb2);
	write_ckpt(sb1->cp_pba);
	write_ckpt(sb1->cp_pba + NR_SECTORS_IN_BLK);
	write_map(sb1->map_pba);
	write_seg_info_table(sb->sit_pba);
	free(sb1);
	/* 0 volume_size: 39321600  nstl  blkdev: /dev/vdb tgtname: TL1 zone_lbas: 524288 data_end: 41418752 */
	zone_lbas = 524288;
	data_zones = 28000;
	blkdev = "/dev/vdb";
	tgtname = "TL1";
	//volume_size = data_zones * zone_lbas;
	volume_size = 39321600;
	data_end = 41418752;
	sprintf(cmd, "/sbin/dmsetup create TL1 --table '0 %ld nstl %s %s %d %d'",
            volume_size, blkdev, tgtname, zone_lbas, 
	    data_end)
    	system(cmd);
	return(0);
}
