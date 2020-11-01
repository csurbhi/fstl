#include <stdio.h>
#include <metadata.h>
#include <zlib.h>
#include <stdlib.h>

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

__le64 get_nr_blks(struct stl_sb *sb)
{
	unsigned long nr_blks_per_zone = (1 << sb->log_zone_size) / (1 << sb->log_block_size);
	unsigned long nr_blks =  (sb->zone_count * nr_blks_per_zone);
	return nr_blks;

}

__le32 get_map_blk_count()
{
	unsigned int one_extent_sz = 16; /* in bytes */
	unsigned int nr_extents_in_pg = PAGE_SIZE / one_extent_sz;
	__le64 nr_extents = get_nr_blks(sb);
	unsigned int pages_for_extents = nr_extents / nr_extents_in_pg;
	if (nr_extents % nr_extents_in_pg > 0)
		pages_for_extents = pages_for_extents + 1;

	return pages_for_extents;
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
	unsigned int pages_for_sit = nr_sits / nr_extents_in_pg;
	if (nr_sits % nr_sits_in_pg > 0)
		pages_for_sit = pages_for_sits + 1;

	return pages_for_sits;
}

__le32 get_metadata_zone_count(struct stl_sb *sb)
{
	unsigned int metadata_blk_count = sb->blk_count_ckpt + sb->blk_count_map + sb->blk_count_sit;
	unsigned int nr_blks_in_zone = (1 << sb->log_zone_size)/(1 << sb->block_size);
	unsigned int metadata_zone_count = metadata_blk_count / nr_blks_in_zone;
	if (metadata_blk_count % nr_blks_in_zone > 0)
		metadata_zone_count  = metadata_zone_count + 1;
	return metadata_zone_count;
}

__le32 get_main_zone_count(struct stl_sb *sb)
{
	__le32 main_zone_count = 0;
	main_zone_count = sb->zone_count - sb->reserved_zone_count - get_metadata_zone_count(sb);
	return main_zone_count;
}

/* TODO: use zone querying to find this out */
__le32 get_reserved_zone_count()
{
	__le32 reserved_zone_count = 0;
	/* We will use the remaining reserved zone counts 
	 * for this
	 */
	reserved_zone_count = 10;
	return reserved_zone_count;
}

#define ZONE_SZ (256 * 1024 * 1024)
#define BLK_SZ 4096

/* We write one raw ckpt in one block
 */
__le64 inline get_cp_lba()
{
	return (ZONE_SZ / BLK_SZ);
}

/* 
 * We store 2 copies of a checkpoint;
 * one per block
 */
#define NR_CKPT_COPIES 2 

__le64 inline get_map_lba(struct stl_sb *sb)
{

	return sb->cp_lba + sb->blk_count_ckpt;
}


__le64 inline get_sit_lba(struct stl_sb *sb)
{
	return sb->map_lba + sb->blk_count_map;
}

/*
 * The first few zones from the main LBA are
 * our reserved LBAs
 */
__le64 inline get_main_lba(struct stl_sb *sb)
{
	return sb->sit_lba + sb->blk_count_sit;
}

unsigned int inline get_reserved_zone_start(struct stl *sb)
{
	return 
}

unsigned int inline get_reserved_zone_end(struct stl *sb)
{
	return sb->res_zone_start + sb->zone_count_reserved;
}


void write_sb()
{
	struct stl_sb *sb;

	sb = (struct stl_sb *)malloc(sizeof(struct stl_sb));
	if (!sb)
		exit(-1);
	memset(&sb, 0, sizeof(struct stl_sb));
	
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
	sb->res_zone_start = get_reserved_zone_start();
	sb->res_zone_end = get_reserved_zone_end();
	sb->zone0 = sb->res_zone_end
	sb->zone0_blkaddr = get_zone0_blkaddr();
	sb->cp_lba = get_cp_lba();
	sb->map_lba = get_map_lba(sb);
	sb->sit_lba = get_sit_lba(sb);
	sb->main_lba = get_main_lba(sb);
	sb->invalid_zones = 0;
	sb->crc = crc32(-1, (unsigned char *)sb, STL_SB_SIZE);
}

void prepare_header(struct stl_header *header, unsigned long pba)
{
	header->magic = STL_HEADER_MAGIC;
	header->nonce = 0;
	header->crc32 = 0;
	header->flags = 0;
	header->len = 0;
	header->prev_pba = pba; 
	header->next_pba = pba;
	header->lba = 0;
	header->pba = pba;
	header->seq = 0;
}


void write_ckpt(struct stl_sb * sb)
{
	struct stl_ckpt *ckpt;

	ckpt = (struct stl_ckpt *)malloc(sizeof(struct stl_ckpt));
	if(!ckpt)
		exit(-1);

	memset(ckpt, 0, sizeof(struct stl_ckpt));
	ckpt->checkpoint_ver = 0;
	ckpt->user_block_count = 
	ckpt->valid_block_count = ckpt->user_block_count;
	ckpt->rsvd_segment_count = sb->zone_count_reserved;
	ckpt->free_segment_count = sb->zone_count_main - 1; //1 for the current frontier
	ckpt->cur_frontier = sb->
	ckpt->blk_nr = 0;
	prepare_header(&ckpt->header);
	set_on(ckpt->free_bitmap);
	set_off(ckp->invalid_bitmap);
}

void write_map()
{
}

void write_seg_info_table()
{
}

int main()
{
	write_sb();
	write_sb();
	write_ckpt();
	write_ckpt();
	write_map();
	write_seg_info_table();
	return(0);
}
