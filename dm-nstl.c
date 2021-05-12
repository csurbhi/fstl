/*TODO:
 *
 *  Copyright (C) 2016 Peter Desnoyers and 2020 Surbhi Palande.
 *
 * This file is released under the GPL
 *
 * Note: 
 * bio uses length in terms of 512 byte sectors.
 * LBA on SMR drives is in terms of 512 byte sectors.
 * The write frontier is advanced in terms of 4096 blocks
 * Translation table has one entry for every block (i.e for every 8
 * lbas) recorded on the disk
 * The length in reverse translation table is the number of sectors.
 * The length in the in-memory extent of translation table is in terms
 * of sectors.
 */

#include <linux/slab.h>
#include <linux/blkdev.h>
#include <linux/mempool.h>
#include <linux/slab.h>
#include <linux/device-mapper.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/random.h>
#include <linux/crc32.h>
#include <linux/seq_file.h>
#include <linux/proc_fs.h>
#include <linux/completion.h>
#include <linux/jiffies.h>
#include <linux/sort.h>
#include <linux/freezer.h>
#include <linux/kthread.h>
#include <linux/buffer_head.h>
#include <linux/vmstat.h>
#include <crypto/hash.h>
#include <linux/list.h>
#include <linux/list_sort.h>
#include <linux/sched.h>
#include <linux/async.h>

#include "nstl-u.h"
#include "stl-wait.h"
#include "metadata.h"

#define DM_MSG_PREFIX "stl"


/* TODO:
 * 1) Convert the STL in a block mapped STL rather
 k than a sector mapped STL
 * If needed you can migrate to a track mapped
 * STL
 * 2) write checkpointing - flush the translation table
 * to the checkpoint area
 * 3) GC
 */

static int get_new_zone(struct ctx *ctx);
extern struct bio_set fs_bio_set;

/*
 * Cherry picked by hand from patch c55183c9aaa00d2bbb578169a480e31aff3d397c
 */
static struct bio *bio_clone_bioset(struct bio *bio_src, gfp_t gfp_mask,
               struct bio_set *bs)
{
       struct bvec_iter iter;
       struct bio_vec bv;
       struct bio *bio;

       /*
        * Pre immutable biovecs, __bio_clone() used to just do a memcpy from
        * bio_src->bi_io_vec to bio->bi_io_vec.
        *
        * We can't do that anymore, because:
        *
        *  - The point of cloning the biovec is to produce a bio with a biovec
        *    the caller can modify: bi_idx and bi_bvec_done should be 0.
        *
        *  - The original bio could've had more than BIO_MAX_PAGES biovecs; if
        *    we tried to clone the whole thing bio_alloc_bioset() would fail.
        *    But the clone should succeed as long as the number of biovecs we
        *    actually need to allocate is fewer than BIO_MAX_PAGES.
        *
        *  - Lastly, bi_vcnt should not be looked at or relied upon by code
        *    that does not own the bio - reason being drivers don't use it for
        *    iterating over the biovec anymore, so expecting it to be kept up
        *    to date (i.e. for clones that share the parent biovec) is just
        *    asking for trouble and would force extra work on
        *    __bio_clone_fast() anyways.
        */

       bio = bio_alloc_bioset(gfp_mask, bio_segments(bio_src), bs);
       if (!bio)
               return NULL;
       bio->bi_disk            = bio_src->bi_disk;
       bio->bi_opf             = bio_src->bi_opf;
       bio->bi_write_hint      = bio_src->bi_write_hint;
       bio->bi_iter.bi_sector  = bio_src->bi_iter.bi_sector;
       bio->bi_iter.bi_size    = bio_src->bi_iter.bi_size;

       switch (bio_op(bio)) {
       case REQ_OP_DISCARD:
       case REQ_OP_SECURE_ERASE:
       case REQ_OP_WRITE_ZEROES:
               break;
       case REQ_OP_WRITE_SAME:
               bio->bi_io_vec[bio->bi_vcnt++] = bio_src->bi_io_vec[0];
               break;
       default:
               bio_for_each_segment(bv, bio_src, iter)
                       bio->bi_io_vec[bio->bi_vcnt++] = bv;
               break;
       }

       if (bio_integrity(bio_src)) {
               int ret;

               ret = bio_integrity_clone(bio, bio_src, gfp_mask);
               if (ret < 0) {
                       bio_put(bio);
                       return NULL;
               }
       }

       bio_clone_blkg_association(bio, bio_src);

       return bio;
}

static struct kmem_cache * extents_slab;
#define IN_MAP 0
#define IN_CLEANING 1
#define STALLED_WRITE 2

/* the extent map can have a single ref on an extent, indicated by the
 * IN_MAP flag. Cleaning can have multiple refs, and is indicated by
 * IN_CLEANING, as can stalled writes.
 */
char *_type[] = {"MAP", "CLEAN", "STALL"};
static void extent_get(struct extent *e, int count, int flag)
{
	atomic_add(count, &e->total_refs);
	atomic_add(count, &e->refs[flag]);
}
static void extent_put(struct ctx *sc, struct extent *e, int count, int flag)
{
	atomic_sub(count, &e->refs[flag]);
	if (atomic_sub_and_test(count, &e->total_refs)) {
		mempool_free(e, sc->extent_pool);
	}
}

static void extent_init(struct extent *e, sector_t lba, sector_t pba, unsigned len)
{
	memset(e, 0, sizeof(*e));
	e->lba = lba;
	e->pba = pba;
	e->len = len;
}

#define MIN_EXTENTS 16
#define MIN_POOL_PAGES 16
#define MIN_POOL_IOS 16
#define MIN_COPY_REQS 16

static sector_t zone_start(struct ctx *ctx, sector_t pba)
{
	return pba - (pba % ctx->nr_lbas_in_zone);
}

static sector_t zone_end(struct ctx *ctx, sector_t pba)
{
	return zone_start(ctx, pba) + ctx->nr_lbas_in_zone - 1;
}

/* zone numbers begin from 0.
 * The freebit map is marked with bit 0 representing zone 0
 */
static unsigned get_zone_nr(struct ctx *ctx, sector_t sector)
{
	sector_t zone_begins = zone_start(ctx, sector);
	//printk(KERN_ERR "%s zone_begins: %llu sb->zone0_pba: %llu ctx->nr_lbas_in_zone: %d", __func__, zone_begins, ctx->sb->zone0_pba, ctx->nr_lbas_in_zone);
	return ( (zone_begins - ctx->sb->zone0_pba) / ctx->nr_lbas_in_zone);
}

static sector_t get_first_pba_for_zone(struct ctx *ctx, unsigned int zonenr)
{
	return ctx->sb->zone0_pba + (zonenr * ctx->nr_lbas_in_zone);
}


static sector_t get_last_pba_for_zone(struct ctx *ctx, unsigned int zonenr)
{
	return (ctx->sb->zone0_pba + (zonenr * ctx->nr_lbas_in_zone) + ctx->nr_lbas_in_zone) - 1;
}



/************** Extent map management *****************/

/* find a map entry containing 'lba' or the next higher entry.
 * see Documentation/rbtree.txt
 */
static struct extent *_stl_rb_geq(struct rb_root *root, off_t lba)
{
	struct rb_node *node = root->rb_node;  /* top of the tree */
	struct extent *higher = NULL;
	struct extent *e = NULL;

	while (node) {
		e = container_of(node, struct extent, rb);
		if (e->lba >= lba && (!higher || e->lba < higher->lba)) {
			higher = e;
		}
		if (lba < e->lba) {
			node = node->rb_left;
		} else {
			if (lba >= e->lba + e->len) {
				node = node->rb_right;
			} else {
				/* lba falls within "e"
				 * (lba >= e->lba) && (lba < (e->lba + e->len)) */
				return e;
			}
		}
	}
	return higher;
}

static struct extent *stl_rb_geq(struct ctx *ctx, off_t lba)
{
	struct extent *e = NULL;

	read_lock(&ctx->metadata_update_lock); 
	e = _stl_rb_geq(&ctx->extent_tbl_root, lba);
	read_unlock(&ctx->metadata_update_lock); 

	return e;
}


int _stl_verbose;
static void stl_rb_insert(struct ctx *ctx, struct rb_root *root, struct extent *new)
{
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct extent *e = NULL;

	RB_CLEAR_NODE(&new->rb);

	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		e = container_of(parent, struct extent, rb);
		if (new->lba < e->lba) {
			link = &(*link)->rb_left;
		} else {
			link = &(*link)->rb_right;
		}
	}
	/* Put the new node there */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
	ctx->n_extents++;
}

static struct extent * revmap_rb_search_geq(struct ctx *ctx, sector_t pba)
{
	struct rb_root *root = &ctx->rev_tbl_root;
	struct rb_node *link = root->rb_node;
	struct extent *e = NULL, *higher = NULL;

	/* Go to the bottom of the tree */
	while (link) {
		e = container_of(link, struct extent, rb);
		if (((!higher) && (e->pba > pba)) || ((higher) && (e->pba < higher->pba))) {
			higher = e;
		}
		if (pba < e->pba) {
			link = link->rb_left;
		} else {
			if (pba >= e->pba + e->len) {
				/* lba does not fall within this node
				 */
				link = link->rb_right;
			}
			else {
				/* lba falls within this node and bio
				 * should be using e 
				 */
				higher = e;
				break;
			}
		}
		e = NULL;
	}
	return higher;
}

/* TODO: depending on the root decrement the correct nr of extents */
static void stl_rb_remove(struct ctx *ctx, struct rb_root *root, struct extent *e)
{
	rb_erase(&e->rb, root);
	ctx->n_extents--;
}


static struct extent *stl_rb_next(struct extent *e)
{
	struct rb_node *node = rb_next(&e->rb);
	return (node == NULL) ? NULL : container_of(node, struct extent, rb);
}

static int split_delete_overlapping_nodes(struct ctx *ctx, struct rb_root *root, sector_t lba, sector_t pba, size_t len)
{
	struct extent *e = NULL, *split = NULL;
	struct extent *tmp = NULL;
	struct rb_node *node = root->rb_node;  /* top of the tree */
	int diff = 0;

	BUG_ON(len == 0);
	while (node) {
		e = rb_entry(node, struct extent, rb);
		/* No overlap */
		if (lba + len < e->lba) {
			node = node->rb_left;
			continue;
		}
		if (lba > e->lba + e->len) {
			node = node->rb_right;
			continue;
		}
		/* new overlaps with e
		 * new: ++++
		 * e: --
		 */

		/*  Case 1: Exact length, complete overwrite.
		 *
		 * Replace the old node: same lba but different pba
		 * and same length
		 *
		 * 	++++++++
		 * 	--------
		 */

		if ((lba == e->lba) && (len = e->len)) {
			/* we already merged the node, so this
			 * overlapping node is extra!
			 */
			stl_rb_remove(ctx, root, e);
			mempool_free(e, ctx->extent_pool);
			break;
		}

		/* 
		 * Case 2: Overwrites an existing extent partially;
		 * covers only the right portion of an extent.
		 * (++ merged on the right side)
		 *
		 * 		+++++++++++
		 * ---------------
		 *
		 */
		if ((lba > e->lba) && (lba < e->lba + e->len)) {
			diff = (e->lba + e->len) - lba;
			e->len = e->len - diff;
			/* we don't add any new node as that new node
			 * was merged with the neighboring node
			 */
			break;
		}

		/* 
		 * Case 4: Overwrite many extents completely
		 *	++++++++++++++++++++
		 *	  ----- ------  --------
		 *
		 * Could also be exact same: 
		 * 	+++++
		 * 	-----
		 * But this case is optimized in case 1.
		 * We need to remove all such e
		 * the last e can have case 1
		 *
		 * here we compare left ends and right ends of 
		 * new and existing node e
		 *
		 * +++ could have been merged on either the left or
		 * right side.
		 */
		while ((e!=NULL) && (lba <= e->lba) && ((lba + len) >= (e->lba + e->len))) {
			tmp = stl_rb_next(e);
			stl_rb_remove(ctx, root, e);
			mempool_free(e, ctx->extent_pool);
			e = tmp;
		}
		if (!e || (e->lba > lba + len))  {
			break;
		}
		/* else fall down to the next case for the last
		 * component that overwrites an extent partially
		 */

		/* 
		 * Case 5: 
		 * Partially overwrite an extent
		 *
		 * (++ merged on the left side)
		 *
		 * ++++++++++
		 * 	-------------- OR
		 *
		 * Left end matches!
		 * +++++++
		 * --------------
		 *
		 */
		if ((lba <= e->lba) && (lba + len > e->lba)) {
			diff = lba + len - e->lba;
			e->lba = e->lba + diff;
			e->len = e->len - diff;
			e->pba = e->pba + diff;
			break;
		}
	}
	return 0;
}



/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to map.
 */
static int stl_update_range(struct ctx *ctx, struct rb_root *root, sector_t lba, sector_t pba, size_t len)
{
	struct extent *e = NULL, *new = NULL, *split = NULL;
	struct extent *tmp = NULL;
	struct rb_node *node = root->rb_node;  /* top of the tree */
	int diff = 0;

	BUG_ON(len == 0);
	new = mempool_alloc(ctx->extent_pool, GFP_NOIO);
	if (unlikely(!new)) {
		return -ENOMEM;
	}
	extent_init(new, lba, pba, len);
	extent_get(new, 1, IN_MAP);
	while (node) {
		e = rb_entry(node, struct extent, rb);
		/* No overlap */
		if (lba + len < e->lba) {
			node = node->rb_left;
			continue;
		}
		if (lba > e->lba + e->len) {
			node = node->rb_right;
			continue;
		}
		/* no overlap, but sequential node; MERGE this new
		* node with existing node!
		*
		* Note that stl_update_range is called asynchronously
		* and so may not be called sequentially!
		*
		* In theory this merge can cause a merge with more
		* nodes on the left! But we dont take care of this
		* situation right now. We can always go through the
		* entire tree to merge the nodes!
		*/
		if (lba + len == e->lba) {
			if (pba + len == e->pba) {
				printk(KERN_ERR "\n New node merged! ");
				stl_rb_remove(ctx, root, e);
				e->len += len;
				e->lba = lba;
				e->pba = pba;
				stl_rb_insert(ctx, root, e);
				mempool_free(new, ctx->extent_pool);
				/* You merged this new overwrite;
				 * however, it could have been
				 * overlapping extent and not merged
				 * before because the pbas were
				 * different. You may end up having a
				 * corrupt tree if you don't delete
				 * the overlapping entries.
				 */
				split_delete_overlapping_nodes(ctx, root, lba, pba, len);
				break;
			}
			/* else we cannot merge as physically
			* discontiguous
			*/
			node = node->rb_left;
			continue;
		}

		if (lba == e->lba + e->len) {
			if (pba == e->pba + e->len) {
				e->len = e->len + len;
				printk(KERN_ERR "\n New node merged! ");
				mempool_free(new, ctx->extent_pool);
				/* You merged this new overwrite;
				 * however, it could have been
				 * overlapping extent and not merged
				 * before because the pbas were
				 * different. You may end up having a
				 * corrupt tree if you don't delete
				 * the overlapping entries.
				 */
				split_delete_overlapping_nodes(ctx, root, lba, pba, len);
				break;
			}
			/* else we cannot merge as physically
			 * discontiguous
			 */
			node = node->rb_right;
			continue;
		}
		/* new overlaps with e
		 * new: ++++
		 * e: --
		 */

		/*  Case 1: Exact length, complete overwrite.
		 *
		 * Replace the old node: same lba but different pba
		 * and same length
		 *
		 * 	++++++++
		 * 	--------
		 */

		if ((lba == e->lba) && (len = e->len)) {
			/*  Since the lba/key doesnt change we dont
			 *  have to do anything to rebalance the tree
			 */
			e->pba = pba;
			mempool_free(new, ctx->extent_pool);
			break;
		}

		/* 
		 * Case 2: Overwrites an existing extent partially;
		 * covers only the right portion of an extent.
		 *
		 * 		+++++++++++
		 * ---------------
		 *
		 */
		if ((lba > e->lba) && (lba < e->lba + e->len)) {
			diff = (e->lba + e->len) - lba;
			e->len = e->len - diff;
			stl_rb_insert(ctx, root, new);
			break;
		}

		 /*
		 * Case 3: overwrite a part of the existing extent
		 * 	++++++++++
		 * ----------------------- 
		 *
		 *  No end matches!!
		 */

		if ((lba > e->lba)  && (lba + len < e->lba + e->len)) {
			split = mempool_alloc(ctx->extent_pool, GFP_NOIO);
			if (!split) {
				mempool_free(new, ctx->extent_pool);
				return -ENOMEM;
			}
			diff =  lba - e->lba;
			/* new should be physically discontiguous
			 */
			BUG_ON(e->pba + diff ==  pba);
			e->len = diff;
			stl_rb_insert(ctx, root, new);
			extent_init(split, lba + len, e->pba + (diff + len), e->len - (diff + len));
			extent_get(split, 1, IN_MAP);
			stl_rb_insert(ctx, root, split);
			break;
		}


		/* 
		 * Case 4: Overwrite many extents completely
		 *	++++++++++++++++++++
		 *	  ----- ------  --------
		 *
		 * Could also be exact same: 
		 * 	+++++
		 * 	-----
		 * But this case is optimized in case 1.
		 * We need to remove all such e
		 * the last e can have case 1
		 *
		 * here we compare left ends and right ends of 
		 * new and existing node e
		 */
		while ((e!=NULL) && (lba <= e->lba) && ((lba + len) >= (e->lba + e->len))) {
			tmp = stl_rb_next(e);
			stl_rb_remove(ctx, root, e);
			mempool_free(e, ctx->extent_pool);
			e = tmp;
		}
		if (!e || (e->lba > lba + len))  {
			stl_rb_insert(ctx, root, new);
			break;
		}
		/* else fall down to the next case for the last
		 * component that overwrites an extent partially
		 */

		/* 
		 * Case 5: 
		 * Partially overwrite an extent
		 * ++++++++++
		 * 	-------------- OR
		 *
		 * Left end matches!
		 * +++++++
		 * --------------
		 *
		 */
		if ((lba <= e->lba) && (lba + len > e->lba)) {
			diff = lba + len - e->lba;
			e->lba = e->lba + diff;
			e->len = e->len - diff;
			e->pba = e->pba + diff;
			stl_rb_insert(ctx, root, new);
			break;
		}
	}
	if (!node) {
		/* new node has to be added */
		stl_rb_insert(ctx, root, new);
		printk( "\n %s Inserted (lba: %u pba: %u len: %d) ", __func__, new->lba, new->pba, new->len);
	}
	//merge_more_nodes(ctx, root, lba, pba, len);
	return 0;
}

static inline int is_stl_ioidle(struct ctx *ctx)
{
	return atomic_read(&ctx->ioidle);
}

static inline void * stl_malloc(size_t size, gfp_t flags)
{
	void *addr;

	addr = kmalloc(size, flags);
	if (!addr) {
		addr = kvmalloc(size, flags);
	}

	return addr;
}

/* For BG_GC mode, go to the left most node in the 
 * in mem stl_extents RB tree 
 * For FG_GC; apply greedy to only the left most part of the tree
 * Ideally we want consequitive segments that are in the left most
 * part of the tree; or we want 'n' sequential zones that give the
 * most of any other 'n'
 */
static int select_zone_to_clean(struct ctx *ctx, int mode)
{
	int zonenr = 0;
	struct rb_node *node;

	if (mode == BG_GC) {
		node = rb_first(&ctx->sit_rb_root);
		zonenr = rb_entry(node, struct sit_extent, rb)->zonenr; 
		return zonenr;
	}
	/* TODO: Mode: FG_GC */
	return zonenr;

}

static int add_extent_to_gclist(struct ctx *ctx, struct extent_entry *e)
{
	struct gc_extents *gc_extent;

	gc_extent = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
	if (!gc_extent) {
		return -ENOMEM;
	}
	gc_extent->e.lba = e->lba;
	gc_extent->e.pba = e->pba;
	gc_extent->e.len = e->len;
	/* 
	 * We always want to add the extents in a PBA increasing order
	 */
	list_add_tail(&ctx->gc_extents->list, &gc_extent->list);
	return 0;
}

void read_extent_done(struct bio *bio)
{
	int trial = 0;
	refcount_t *ref;
	
	if (bio->bi_status != BLK_STS_OK) {
		if (bio->bi_status == BLK_STS_IOERR) {
			printk(KERN_ERR "\n GC read failed because of disk error!");
			/* We need to continue this GC; this extent is
			 * lost!
			 */
		}
		else {
			if (bio->bi_status  == BLK_STS_AGAIN) {
				trial ++;
				if (trial < 3)
					submit_bio(bio);
			}
			panic("GC read of an extent failed! Could be a resource issue, write better error handling");
		}
	}
	ref = bio->bi_private;
	refcount_dec(ref);
}

static int setup_extent_bio(struct ctx *ctx, struct gc_extents *gc_extent)
{
	int nr_pages = 0;
	int i=0, s8, nr_sectors;
	struct bio_vec *bv = NULL;
	struct page *page;
	struct bio *bio;
	struct bvec_iter_all iter_all;

	gc_extent->bio = bio_alloc(GFP_KERNEL, 1);
	if (!gc_extent->bio) {
		return -ENOMEM;
	}
	nr_sectors = gc_extent->e.len;
	s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
	gc_extent->e.len = s8;
	/* Now allocate pages to the bio 
	 * 2^3 sectors make 1 page
	 * Since the length is blk aligned, we dont have to add a 1
	 * to the nr_pages calculation
	 */
	nr_pages = (gc_extent->e.len >> 3);
	/* bio_add_page sets the bi_size for the bio */
	 
	for(i=0; i<nr_pages; i++) {
		page = mempool_alloc(ctx->page_pool, GFP_KERNEL);
		/* bio_add_page() sets the bi_size of the bio */
		if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
			bio_for_each_segment_all(bv, bio, iter_all) {
				mempool_free(bv->bv_page, ctx->page_pool);
			}
			bio_put(bio);
			return -ENOMEM;
		}
	}
	bio->bi_iter.bi_sector = gc_extent->e.pba;
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_private = ctx;
	bio->bi_end_io = read_extent_done;
	gc_extent->bio = bio;
	return 0;
}

static void free_gc_list(struct ctx *ctx)
{

	struct list_head *list_head;
	struct gc_extents *gc_extent;
	struct bio_vec *bv = NULL;
	struct bvec_iter_all iter_all;
	struct bio *bio;

	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		bio = gc_extent->bio;
		if (bio) {
			bio_for_each_segment_all(bv, bio, iter_all) {
				mempool_free(bv->bv_page, ctx->page_pool);
			}
			bio_put(bio);
		}
		kmem_cache_free(ctx->gc_extents_cache, gc_extent);
	}
}

void wait_on_refcount(struct ctx *ctx, refcount_t *ref);

static int read_all_bios_and_wait(struct ctx *ctx, struct gc_extents *last_extent)
{

	struct list_head *list_head;
	struct gc_extents *gc_extent;
	struct blk_plug plug;
	refcount_t * ref;

	ref = kzalloc(sizeof(refcount_t), GFP_KERNEL);
	if (!ref) {
		return -ENOMEM;
	}

	refcount_set(ref, 1);

	blk_start_plug(&plug);
	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		gc_extent->bio->bi_private = ref;
		refcount_inc(ref);
		/* setup bio sets bio->bi_end_io = read_extent_done */
		submit_bio(gc_extent->bio);
	}
	/* When we arrive here, we know the last bio has completed.
	 * Since the previous bios were chained to this one, we know
	 * that the previous bios have completed as well
	 */
	blk_finish_plug(&plug);
	wait_on_refcount(ctx, ref);
	return 0;
}

/*
 * TODO: Do  not chain the bios as we do not get notification
 * of what extent reading did not work! We can retry and if
 * the block did not work, we can do something more meaningful.
 */
static void read_gc_extents(struct ctx *ctx)
{
	struct bio *bio;
	struct list_head *list_head;
	struct gc_extents *gc_extent, *last_extent;

	/* If list is empty we have nothing to do */
	BUG_ON(list_empty(&ctx->gc_extents->list));

	/* setup the bio for the first gc_extent */
	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		if (setup_extent_bio(ctx, gc_extent)) {
			free_gc_list(ctx);
			panic("Low memory! TODO: Write code to free memory from translation tables etc ");
		}
	}
	last_extent = gc_extent;
	read_all_bios_and_wait(ctx, last_extent);
}

static int cmp_list_nodes(void *priv, struct list_head *lha, struct list_head *lhb)
{
	struct gc_extents *ga, *gb;

	ga = list_entry(lha, struct gc_extents, list);
	gb = list_entry(lhb, struct gc_extents, list);

	BUG_ON(ga->e.lba == gb->e.lba);
	if (ga->e.lba < gb->e.lba)
		return -1;
	return 1;
}

void mark_disk_full(struct ctx *ctx);
static void move_gc_write_frontier(struct ctx *ctx, sector_t sectors_s8);

/* 
 * The extent that we are about to write will definitely fit into
 * the gc write frontier. No one is writing to the gc frontier
 * other than the gc process; only gc process executes at a time
 * as we take a lock to prevent concurrent execution.
 * We make sure that the length of the extent matches with free space
 * in the zone. Also, since the extent corresponds to what is read
 * from the disk, we are sure that the length is in terms of what is
 * found on disk.
 */
static int write_gc_extent(struct ctx *ctx, struct gc_extents *gc_extent)
{
	struct bio *bio;
	struct list_head *list_head;
	struct gc_extents *last_extent;
	struct stl_ckpt *ckpt;
	struct gc_ctx *gc_ctx;
	int nrsectors;
	int trials = 0;

	bio = gc_extent->bio;
	gc_ctx = bio->bi_private;

	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = ctx->gc_write_frontier;
	nrsectors = bio_sectors(bio);
again:
	submit_bio_wait(bio);
	if (bio->bi_status != BLK_STS_OK) {
		if (bio->bi_status ==  BLK_STS_IOERR) {
			/* For now we are not doing anything else
			 * We should actually be retrying depending on weather
			 * its a disk error or a memory error
			 */
			mark_disk_full(ctx);
			printk(KERN_ERR "\n write end io status not OK");
			return -1;
		}
		if (bio->bi_status == BLK_STS_AGAIN) {
			trials++;
			if (trials < 3)
				goto again;
		}
		panic("GC writes failed! Perhaps a resource error");
	}
	move_gc_write_frontier(ctx, nrsectors);
	return 0;
}


/* TODO:
 * Check if the e that you have is in teh same form in the 
 * REVMAP RB tree.
 * If not, adjus the e that you have with what you find in 
 * the rb tree. If the e you find does not overlap with the 
 * e in consideration, remove that e from the gc list
 */

static int verify_extents(struct ctx *ctx)
{
	struct bio *bio;
	struct list_head *list_head;
	struct gc_extents *gc_extent, *last_extent;
	struct extent *e = NULL;

	/* If list is empty we have nothing to do */
	BUG_ON(list_empty(&ctx->gc_extents->list));

	/* In the Linux kernel DLL, the first node is the head and it
	 * never has data. Data is stored from the second node
	 * onwards.
	 * So now we traverse from the second node onwards */
	list_for_each(list_head, &ctx->gc_extents->list) {
		
	}
	return 0;

}

static void add_revmap_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned int nrsectors);
static void do_checkpoint(struct ctx *ctx);

/*
 * TODO: write code for FG_GC
 *
 * if err_flag is set, we have to mark the zone_to_clean erroneous.
 * This is set in the nstl_clone_endio() path when a block cannot be
 * written to some zone because of disk error. We need to read the
 * remaining blocks from this zone and write to the destn_zone.
 *
 */
static int stl_gc(struct ctx *ctx, unsigned int zone_to_clean, char gc_flag, int err_flag)
{
	int zonenr, i, newzone = 0;
	sector_t pba, last_pba;
	struct extent *e = NULL;
	struct extent_entry temp;
	struct blk_plug plug;
	struct gc_extents *gc_extent, *new, *temp_ptr;
	struct list_head *list_head;
	sector_t diff;
	sector_t nr_sectors, s8;
	struct bio *split, *bio;
	int ret;

	printk(KERN_INFO "\n GC thread polling after every few seconds ");
	/* Take a semaphore lock so that no two gc instances are
	 * started in parallel.
	 */

	if (!down_trylock(&ctx->gc_lock)) {
		printk(KERN_ERR "\n GC is already running!");
		return -1;
	}
	BUG_ON(!list_empty(&ctx->gc_extents->list));

	pba = get_first_pba_for_zone(ctx, zonenr);
	/* Lookup this pba in the reverse table to find the
	 * corresponding LBA. 
	 * TODO: If the valid blocks are sequential, we need to keep
	 * this segment as an open segment that can append data. We do
	 * not need to perform GC on this segment.
	 *
	 * We need to eventually change the
	 * translation map in memory and on disk so that the LBA
	 * points to the new PBA
	 *
	 * Note that pba, lba, and len are all in terms of
	 * 512 byte sectors. ctx->nr_lbas_in_zone are also
	 * the number of sectors in a zone.
	 */
	last_pba = get_last_pba_for_zone(ctx, zonenr);
	
	while(pba <= last_pba) {
		e = revmap_rb_search_geq(ctx, pba);
		if (NULL == e) {
			break;
		}
		if (e->pba > last_pba)
			break;
		temp.pba = e->pba;
		temp.lba = e->lba;
		/* Don't change e directly, as e belongs to the
		 * reverse map rb tree and we have the node address
		 */
		if (e->pba + e->len - 1 > last_pba)
			temp.len = last_pba - e->pba + 1;
		add_extent_to_gclist(ctx, &temp);
		pba = e->pba + temp.len;
	}
	/* This segment has valid extents, thus there should be
	 * atleast one node in the gc list. An empty list indicates
	 * the reverse translation map does not match with the segment
	 * information table
	 */
	BUG_ON(list_empty(&ctx->gc_extents->list));
	read_gc_extents(ctx);
	/* Sort the list on LBAs. Write increasing LBAs in the same
	 * order
	 */
	list_sort(NULL, &ctx->gc_extents->list, cmp_list_nodes);
	/* Since our read_extents call, overwrites could have made
	 * the blocks in this zone invalid. Thus we now take a 
	 * write lock and then re-read the extents metadata; else we
	 * will end up writing invalid blocks and loosing the
	 * overwritten data
	 */
	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		write_lock(&ctx->metadata_update_lock);
		e = revmap_rb_search_geq(ctx, pba);
		/* entire extrents is lost by interim overwrites */
		if (e->pba > gc_extent->e.pba + gc_extent->e.len) {
			temp_ptr = list_next_entry(gc_extent, list);
			list_head = &temp_ptr->list;
			list_del(&gc_extent->list);
			kmem_cache_free(ctx->gc_extents_cache, gc_extent);
			continue;
		}
		/* extents are partially snipped */
		if (e->pba > gc_extent->e.pba) {
			gc_extent->e.pba = e->pba;
			gc_extent->e.lba = e->lba;
			gc_extent->e.len = e->len;
			/* bio advance will advance the bi_sector and bi_size
		 	 */
			bio_advance(gc_extent->bio, diff << 9);
		}
		if (e->len < gc_extent->e.len) {
			diff = gc_extent->e.len - e->len;
			gc_extent->e.len = e->len;
			/* bio advance will advance the bi_sector and bi_size */
			bio_advance(gc_extent->bio, diff<<9);
		}
		/* Now we adjust the gc_extent such that it can be
		 * written without getting split in the gc write
		 * frontier
		 */
		nr_sectors = bio_sectors(gc_extent->bio);
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		if (s8 > ctx->free_sectors_in_gc_wf){
			s8 = round_down(ctx->free_sectors_in_wf, NR_SECTORS_IN_BLK);
			bio = gc_extent->bio;
			split = bio_split(bio, s8, GFP_KERNEL, ctx->bs);
			if (unlikely(!split)) {
				// do something
				panic("No memory, couldnt split! write better code!");
			}
			gc_extent->e.len = s8;
			gc_extent->bio = split;

			new = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
			if (unlikely(!new)) {
				// do something
				panic("No memory, couldnt split! write better code!");
			}
			diff = nr_sectors - s8;
			new->e.pba = e->pba + nr_sectors;
			new->e.lba = e->lba + nr_sectors;
			new->e.len = diff;
			new->bio = bio;
			/* Add new after gc_extent */
			list_add(&new->list, &gc_extent->list);
		}
		write_gc_extent(ctx, gc_extent);
		if (gc_extent->bio->bi_status != BLK_STS_OK) {
			/* write error! disk is in a read mode! we
			 * cannot perform any further GC
			 */
			write_unlock(&ctx->metadata_update_lock);
			goto done;
		}
		ret = stl_update_range(ctx, &ctx->extent_tbl_root, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
		ret = stl_update_range(ctx, &ctx->rev_tbl_root, gc_extent->e.pba, gc_extent->e.lba, gc_extent->e.len);
		add_revmap_entries(ctx, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
		temp_ptr = list_next_entry(gc_extent, list);
		list_head = &temp_ptr->list;
		list_del(&gc_extent->list);
		kmem_cache_free(ctx->gc_extents_cache, gc_extent);
		write_unlock(&ctx->metadata_update_lock);
	}
	do_checkpoint(ctx);
	/* Complete the GC and then sync the block device */
	sync_blockdev(ctx->dev->bdev);
done:
	/* Release GC lock */
	up(&ctx->gc_lock);
	return 0;
}

/*
 * TODO: When we want to mark a zone free create a bio with opf:
 * REQ_OP_ZONE_RESET
 *
 *
 */
static int gc_thread_fn(void * data)
{

	struct ctx *ctx = (struct ctx *) data;
	struct stl_gc_thread *gc_th = ctx->gc_th;
	wait_queue_head_t *wq = &gc_th->stl_gc_wait_queue;
	unsigned int wait_ms;
	unsigned int zonenr, newzone;

	wait_ms = gc_th->min_sleep_time;
	set_freezable();
	do {
		wait_event_interruptible_timeout(*wq,
			kthread_should_stop() || freezing(current) ||
			gc_th->gc_wake,
			msecs_to_jiffies(wait_ms));

		               /* give it a try one time */
                if (gc_th->gc_wake)
                        gc_th->gc_wake = 0;

                if (try_to_freeze()) {
                        continue;
                }
                if (kthread_should_stop())
                        break;
		/* take some lock here as you will start the GC 
		 * For urgent mode mutex_lock()
		 * and for idle time GC mode mutex_trylock()
		 * You need some check for is_idle()
		 * */
		if(!is_stl_ioidle(ctx)) {
			/* increase sleep time */
			wait_ms = wait_ms * 2;
			/* unlock mutex */
			continue;
		}
		zonenr = select_zone_to_clean(ctx, BG_GC);
		stl_gc(ctx, zonenr, newzone, 0);
	} while(!kthread_should_stop());
	return 0;
}

#define DEF_GC_THREAD_URGENT_SLEEP_TIME 500     /* 500 ms */
#define DEF_GC_THREAD_MIN_SLEEP_TIME    30000   /* milliseconds */
#define DEF_GC_THREAD_MAX_SLEEP_TIME    60000
#define DEF_GC_THREAD_NOGC_SLEEP_TIME   300000  /* wait 5 min */
#define LIMIT_INVALID_BLOCK     40 /* percentage over total user space */
#define LIMIT_FREE_BLOCK        40 /* percentage over invalid + free space */


/* 
 * On error returns 0
 *
 */
int stl_gc_thread_start(struct ctx *ctx)
{
	struct stl_gc_thread *gc_th;
	dev_t dev = ctx->dev->bdev->bd_dev;
	int err=0;

	printk(KERN_ERR "\n About to start GC thread");

	gc_th = stl_malloc(sizeof(struct stl_gc_thread), GFP_KERNEL);
	if (!gc_th) {
		return -ENOMEM;
	}

	gc_th->urgent_sleep_time = DEF_GC_THREAD_URGENT_SLEEP_TIME;
        gc_th->min_sleep_time = DEF_GC_THREAD_MIN_SLEEP_TIME;
        gc_th->max_sleep_time = DEF_GC_THREAD_MAX_SLEEP_TIME;
        gc_th->no_gc_sleep_time = DEF_GC_THREAD_NOGC_SLEEP_TIME;

        gc_th->gc_wake= 0;

        ctx->gc_th = gc_th;
	init_waitqueue_head(&gc_th->stl_gc_wait_queue);
	ctx->gc_th->stl_gc_task = kthread_run(gc_thread_fn, ctx,
			"stl-gc%u:%u", MAJOR(dev), MINOR(dev));

	if (IS_ERR(gc_th->stl_gc_task)) {
		err = PTR_ERR(gc_th->stl_gc_task);
		kvfree(gc_th);
		ctx->gc_th = NULL;
		return err;
	}

	printk(KERN_ERR "\n Created a STL GC thread ");
	return 0;	
}

int stl_gc_thread_stop(struct ctx *ctx)
{
	kthread_stop(ctx->gc_th->stl_gc_task);
	kvfree(ctx->gc_th);
	return 0;
}

/* When we find that the system is idle for a long time,
 * then we invoke the stl_gc in a background mode
 */

void invoke_gc(unsigned long ptr)
{
	struct ctx *ctx = (struct ctx *) ptr;
	wake_up_process(ctx->gc_th->stl_gc_task);
}


void stl_is_ioidle(struct kref *kref)
{
	struct ctx *ctx;

	ctx = container_of(kref, struct ctx, ongoing_iocount);
	atomic_set(&ctx->ioidle, 1);
	printk(KERN_ERR "\n Stl is io idle!");
	/* Add the initialized timer to the global list */
	/* TODO: We start the timer only when there is some work to do.
	 * We dont want GC to be invoked for no reason!
	 * When there is little to do, disable the timer.
	 * When an I/O is invoked, disable the timer if its enabled.
	 * so if work to do and  ioidle, then enable the timer.
	 * when no work to do disable the timer.
	 * when read/write invoked, if timer_enabled: disable_timer.
	 */
	/*
	kref_init(kref);
	ctx->timer.function = invoke_gc;
	ctx->timer.data = (unsigned long) ctx;
	ctx->timer.expired = TIME_IDLE_JIFFIES;
	add_timer(ctx->timer);
	*/
}

void nstl_subread_done(struct bio *bio)
{
	struct app_read_ctx *read_ctx = bio->bi_private;
	struct ctx *ctx = read_ctx->ctx;

	bio_endio(read_ctx->bio);
	kref_put(&ctx->ongoing_iocount, stl_is_ioidle);
	kmem_cache_free(ctx->app_read_ctx_cache, read_ctx);
	//printk(KERN_ERR "\n nstl_subread_done! \n");
	bio_put(read_ctx->clone);
}

/*
 * This is an asynchronous read, i.e we submit the request
 * here but do not wait for the request to complete.
 * bio_endio() will complete the request.
 * The callers will be notified upon this.
 *
 *
 * TODO: if read is beyond the disk, return -EINVAL
 */
static int nstl_read_io(struct ctx *ctx, struct bio *bio)
{
	struct bio *split = NULL;
	sector_t lba, pba;
	struct extent *e;
	unsigned nr_sectors, overlap;

	struct app_read_ctx *read_ctx;
	struct bio *clone;

	//printk(KERN_ERR "Read begins! ctx->app_read_ctx_cache: %llu", ctx->app_read_ctx_cache);
	read_ctx = kmem_cache_alloc(ctx->app_read_ctx_cache, GFP_KERNEL);
	if (!read_ctx)
		return -ENOMEM;

	atomic_inc(&ctx->n_reads);
	clone = bio_clone_fast(bio, GFP_KERNEL, NULL);
	if (!clone) {
		kmem_cache_free(ctx->app_read_ctx_cache, read_ctx);
		return -ENOMEM;
	}

	read_ctx->ctx = ctx;
	read_ctx->bio = bio;

	split = NULL;
	while(split != clone) {
		lba = clone->bi_iter.bi_sector;
		e = stl_rb_geq(ctx, lba);
		nr_sectors = bio_sectors(clone);

		/* note that beginning of extent is >= start of bio */
		/* [----bio-----] [eeeeeee] a
		 *
		 * eeeeeeeeeeeeee		eeeeeeeeeeeee
		 * 	bbbbbbbbbbbbbbbbbbb
		 *
		 * bio will be split. The second part of the bio will
		 * fall in this next case; it will be zerofilled.
		 * But we do not want to rush to call end_io yet,
		 * till the first portion has completed.
		 *
		 * However the case could might as well be:
		 *
		 * Where the entire bio did not have a single split
		 * and no overlap either:
		 *
		 *		eeeeeeeeeeee
		 * bbbbbb
		 *
		 */
		if (e == NULL || e->lba >= lba + nr_sectors)  {
			//printk(KERN_ERR "\n Case of no overlap \n");
			zero_fill_bio(clone);
			kref_get(&ctx->ongoing_iocount);
			clone->bi_private = read_ctx;
			clone->bi_end_io = nstl_subread_done;
			read_ctx->clone = clone;
			/* This bio could be the parent of other
			 * chained bios. Its necessary to call
			 * bio_endio and not the endio function
			 * directly
			 */
			bio_endio(clone);
			return 0;
		}
		if (e->lba > lba) {
		/*               [eeeeeeeeeeee]
			       [---------bio------] */
			printk(KERN_ERR "\n e->lba >  lba \n");
			nr_sectors = e->lba - lba;
			split = bio_split(clone, nr_sectors, GFP_NOIO, &fs_bio_set);
			bio_chain(split, clone);
			zero_fill_bio(split);
			bio_endio(split);
			continue;
		}
		else { //(e->lba <= lba)
		/* [eeeeeeeeeeee] eeeeeeeeeeeee]<- could be shorter or longer
		     [---------bio------] */
			overlap = e->lba + e->len - lba;
			printk(KERN_ERR "\n e->lba <= lba  e->lba: %lu lba: %lu overlap: %d \n", e->lba, lba, overlap);
			if (overlap < nr_sectors) {
				split = bio_split(clone, overlap, GFP_NOIO, &fs_bio_set);
				if (!split) {
					printk(KERN_ERR "\n Could not split the clone! ERR ");
					/* other bios could have
					 * been chained to clone. Hence we
					 *  should be calling bio_endio(clone)
					 */
					clone->bi_status = BLK_STS_RESOURCE;
					kref_get(&ctx->ongoing_iocount);
					clone->bi_private = read_ctx;
					clone->bi_end_io = nstl_subread_done;
					read_ctx->clone = clone;
					bio_endio(clone);
					return -ENOMEM;
				}
				bio_chain(split, clone);
			} else {
				/* All the previous splits are chained
				 * to this last one. No more bios will
				 * be submitted once this is
				 * submitted
				 */
				atomic_set(&ctx->ioidle, 0);
				split = clone;
				printk(KERN_INFO "\n read bio, lba: %llu, pba: %llu, len: %d \n", lba, (e->pba + lba - e->lba), overlap);
				kref_get(&ctx->ongoing_iocount);
				split->bi_private = read_ctx;
				split->bi_end_io = nstl_subread_done;
				read_ctx->clone = clone;
			}
			pba = e->pba + lba - e->lba;
			split->bi_iter.bi_sector = pba;
			bio_set_dev(split, ctx->dev->bdev);
			//printk(KERN_INFO "\n read,  sc->n_reads: %d", sc->n_reads);
			generic_make_request(split);
		}
	}
	printk(KERN_INFO "\n Read end");
	return 0;
}


void copy_blocks(u64 zonenr, u64 destn_zonenr) 
{
	return;
}

void nstl_init_zone(struct ctx *ctx, u64 zonenr)
{
	return;
}


void nstl_init_zones(struct ctx *ctx)
{
	/*
 	while (sector < dev->capacity) {
                nr_blkz = DMZ_REPORT_NR_ZONES;
                ret = blkdev_report_zones(dev->bdev, sector, blkz, &nr_blkz); 
                if (ret) { 
                        dmz_dev_err(dev, "Report zones failed %d", ret); 
                        goto out; 
                }

                if (!nr_blkz)
                        break;
                
                for (i = 0; i < nr_blkz; i++) {
                        ret = nstl_init_zone(zmd, zone, &blkz[i]);
                        if (ret)
                                goto out;
                        sector += dev->zone_nr_sectors;
                        zone++;
                }
        }*/
}


int nstl_zone_seq(struct ctx * ctx, sector_t pba)
{
	return 0;
}


/*
 * TODO:
 * REMOVE partial translation map entries
 * for a write that has failed. We will end up
 * reading stale data otherwise.
 *
 * 1) search the RB tree for LBA, len in the bio
 * 2) remove these entries
 * 3) check if the page for the revmap entries exists. Check if it is
 * flushed to the disk already (page flag is uptodate). 
 * 4) If so, create a new revmap entry to the disk. 
 * 5) Else modify this revmap entry in place as it is still in
 * memory. Mark the page dirty.
 */
void remove_partial_entries(struct ctx *ctx, struct bio * bio)
{
	printk(KERN_ERR "\n Error in bio, removing partial entries! lba: %llu", bio->bi_iter.bi_sector);
	dump_stack();
}

void mark_disk_full(struct ctx *ctx)
{
	struct stl_ckpt *ckpt = ctx->ckpt;
	ckpt->nr_free_zones  = 0;
}

int is_disk_full(struct ctx *ctx)
{
	return !(ctx->ckpt->nr_free_zones);
}

/* 
 *
 * TODO: Use the rb APIs for going to the next element and
 * finding the element
 * 
 * pba: PBA from the LBA-PBA pair. This is the PBA of a data block
 * that belongs to a zone. We search for a page that holds the sit
 * entry for this zone.
 *
 * https://lwn.net/Articles/184495/
 */

struct sit_page * search_sit_kv_store(struct ctx *ctx, sector_t pba, struct rb_node **parent)
{
	struct rb_root *rb_root = &ctx->sit_rb_root;
	struct rb_node *node = NULL;
	struct sit_page *node_ent;

	u64 zonenr = get_zone_nr(ctx, pba);
	u64 blknr = zonenr / SIT_ENTRIES_BLK;

	node = rb_root->rb_node;
	while(node) {
		*parent = node;
		node_ent = rb_entry(*parent, struct sit_page, rb);
		if (blknr == node_ent->blknr) {
			return node_ent;
		}
		if (blknr < node_ent->blknr) {
			node = node->rb_left;
		} else {
			node = node->rb_right;
		}
	}
	return NULL;
}



/* Blknr corresponds to the blk that stores the SIT entries and has a
 * page in memory that we try to flush at that blknr
 */
struct sit_page * search_sit_blk(struct ctx *ctx, sector_t blknr)
{
	struct rb_root *rb_root = &ctx->sit_rb_root;
	struct rb_node *node = NULL;
	struct sit_page *node_ent;
	struct rb_node *parent;

	node = rb_root->rb_node;
	while(node) {
		parent = node;
		node_ent = rb_entry(parent, struct sit_page, rb);
		if (blknr == node_ent->blknr) {
			return node_ent;
		}
		if (blknr < node_ent->blknr) {
			node = node->rb_left;
		} else {
			node = node->rb_right;
		}
	}
	return NULL;
}



/*
 * When a segentry says that all blocks are full,
 * but the mtime is 0, then the zone is erroneous.
 * Such a zone will never be the destination of 
 * GC
 */
void mark_zone_erroneous(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct stl_seg_entry *ptr;
	struct rb_node *parent = NULL;
	int index;
	int zonenr, destn_zonenr;

	down_interruptible(&ctx->sit_kv_store_lock);
	/*-----------------------------------------------*/
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	/*-----------------------------------------------*/
	up(&ctx->sit_kv_store_lock);
	if (!sit_page) {
		sit_page = add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, couldnt allocate sit entry");
		}
	}

	spin_lock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	get_page(sit_page->page);
	ptr = (struct stl_seg_entry *)sit_page->page;
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = BLOCKS_IN_ZONE;
	ptr->mtime = 0;
	SetPageDirty(sit_page->page);
	put_page(sit_page->page);
	/*--------------------------------------------*/
	spin_unlock(&ctx->sit_flush_lock);

	spin_lock(&ctx->lock);
	/*-----------------------------------------------*/
	ctx->nr_invalid_zones++;
	pba = get_new_zone(ctx);
	destn_zonenr = get_zone_nr(ctx, pba);
	/*-----------------------------------------------*/
	spin_unlock(&ctx->lock);
	if (0 > pba) {
		printk(KERN_INFO "No more disk space available for writing!");
		return;
	}
	copy_blocks(zonenr, destn_zonenr); 
}
/* 
 * 1 indicates that the zone is free 
 *
 * Zone numbers start from 0
 */
static void mark_zone_free(struct ctx *ctx , int zonenr)
{	
	char *bitmap;
	int bytenr;
	int bitnr;


	if (unlikely(NULL == ctx)) {
		panic("This is a ctx bug");
	}
		
	bitmap = ctx->freezone_bitmap;
	bytenr = zonenr / BITS_IN_BYTE;
	bitnr = zonenr % BITS_IN_BYTE;

	if(unlikely(bytenr > ctx->bitmap_bytes)) {
		panic("bytenr: %d > bitmap_bytes: %d", bytenr, ctx->bitmap_bytes);
	}


	if (unlikely(NULL == bitmap)) {
		panic("This is a ctx freezone bitmap bug!");
	}

	if ((bitmap[bytenr] & (1 << bitnr)) == (1<<bitnr)) {
		/* This bit was 1 and hence already free*/
		panic("\n Trying to free an already free zone! ");
	}

	/* The bit was 0 as its in use and hence we xor it with
	 * one more 1 to unset it
	 */
	bitmap[bytenr] = bitmap[bytenr] | (1 << bitnr);
	ctx->nr_freezones = ctx->nr_freezones + 1;
}

static void mark_zone_gc_candidate(struct ctx *ctx , int zonenr)
{
	char *bitmap = ctx->gc_zone_bitmap;
	int bytenr = zonenr / BITS_IN_BYTE;
	int bitnr = zonenr % BITS_IN_BYTE;

	if ((bitmap[bytenr] & (1 << bitnr)) == (1<<bitnr)) {
		/* This bit was 1 and hence already free*/
		panic("\n Trying to free an already free zone! ");
	}

	/* The bit was 0 as its in use and hence we xor it with
	 * one more 1 to unset it
	 */
	bitmap[bytenr] = bitmap[bytenr] | (1 << bitnr);
}

static u64 get_next_freezone_nr(struct ctx *ctx)
{
	char *bitmap = ctx->freezone_bitmap;
	int bytenr = 0;
	int bitnr = 0;
	unsigned char allZeroes = 0;

	/* 1 indicates that a zone is free. 
	 */
	while(bitmap[bytenr] == allZeroes) {
		/* All these zones are occupied */
		bytenr = bytenr + 1;
		if (bytenr == ctx->bitmap_bytes) {
			break;
		}
	}
	
	if (bytenr == ctx->bitmap_bytes) {
		/* no freezones available */
		return -1;
	}
	/* We have the bytenr from where to return the freezone */
	bitnr = 0;
	while (1) {
		if ((bitmap[bytenr] & (1 << bitnr)) == (1 << bitnr)) {
			/* this bit is free, hence marked 1 */
			break;
		}
		bitnr = bitnr + 1;
		if (bitnr == BITS_IN_BYTE) {
			panic ("Wrong byte calculation!");
		}
	}
	return ((bytenr * BITS_IN_BYTE) + bitnr);
}

void wait_on_zone_barrier(struct ctx *);
static void add_ckpt_new_wf(struct ctx *, sector_t);
static void add_ckpt_new_gc_wf(struct ctx * ctx, sector_t wf);
/* moves the write frontier, returns the LBA of the packet trailer
 *
 * Always called with the ctx->lock held
*/
static int get_new_zone(struct ctx *ctx)
{
	unsigned int zone_nr, toclean;
	int trial;

	trial = 0;
try_again:
	zone_nr = get_next_freezone_nr(ctx);
	if (zone_nr < 0) {
		toclean = select_zone_to_clean(ctx, FG_GC);
		stl_gc(ctx, toclean, FG_GC, 0);
		if (0 == trial) {
			trial++;
			goto try_again;
		}
		printk(KERN_INFO "No more disk space available for writing!");
		mark_disk_full(ctx);
		ctx->app_write_frontier = -1;
		return (ctx->app_write_frontier);
	}


	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->app_write_frontier = ctx->sb->zone0_pba + (zone_nr << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	ctx->app_wf_end = zone_end(ctx, ctx->app_write_frontier);
	printk(KERN_ERR "\n !!!!!!!!!!!!!!! get_new_zone():: zone0_pba: %u zone_nr: %ld app_write_frontier: %llu, wf_end: %llu", ctx->sb->zone0_pba, zone_nr, ctx->app_write_frontier, ctx->app_wf_end);
	if (ctx->app_write_frontier > ctx->app_wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %llu", ctx->free_sectors_in_wf );
	}
	ctx->free_sectors_in_wf = ctx->app_wf_end - ctx->app_write_frontier + 1;
	ctx->nr_freezones--;
	printk(KERN_INFO "Num of free sect.: %llu, diff of end and wf:%llu\n", ctx->free_sectors_in_wf, ctx->app_wf_end - ctx->app_write_frontier);
	add_ckpt_new_wf(ctx, ctx->app_write_frontier);
	return ctx->app_write_frontier;
}


/*
 * moves the write frontier, returns the LBA of the packet trailer
*/
static int get_new_gc_zone(struct ctx *ctx)
{
	unsigned long zone_nr;
	int trial;

	trial = 0;
try_again:
	zone_nr = get_next_freezone_nr(ctx);
	if (zone_nr < 0) {
		mark_disk_full(ctx);
		printk(KERN_INFO "No more disk space available for writing!");
		ctx->app_write_frontier = -1;
		return (ctx->app_write_frontier);
	}

	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->gc_write_frontier = ctx->sb->zone0_pba + (zone_nr << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	ctx->gc_wf_end = zone_end(ctx, ctx->gc_write_frontier);
	printk(KERN_ERR "\n !!!!!!!!!!!!!!! get_new_gc_zone():: zone0_pba: %u zone_nr: %ld gc_write_frontier: %llu, gc_wf_end: %llu", ctx->sb->zone0_pba, zone_nr, ctx->gc_write_frontier, ctx->gc_wf_end);
	if (ctx->gc_write_frontier > ctx->gc_wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %llu", ctx->free_sectors_in_wf );
	}
	ctx->free_sectors_in_gc_wf = ctx->gc_wf_end - ctx->gc_write_frontier + 1;
	add_ckpt_new_gc_wf(ctx, ctx->gc_write_frontier);
	return ctx->gc_write_frontier;
}

/*
 * We do not write any header or trailer. If the write pointer of the
 * SMR zone moved forward, then we know that the write was written.
 * TODO: At mount time, we check the zone write pointer using smr primitives
 * and that is our last valid write.
 *
 * This is the report zones scsi command.
 *
 * We write the data first to the zones. We write the LBA
 * corresponding to the sequential PBAs at checkpoint time.
 * For a 256MB zone i.e 65536 blocks, we need 128 blocks of 
 * 4096 bytes if we store only the LBA.
 * If we store LBA, len, we might need much lesser entries.
 * Worth checking this too.
 *
 * If there is less space in this zone, we need to record the LBAs
 * but we need to allocate a new zone and record this new zone nr.
 * A ckpt will atmost hold entries for blocks in 2 zones.
 */

#define SIZEOF_HEADER 4
#define SIZEOF_TRAILER 4

/* When we take a checkpoint
 * we need to reset the prev_zone_nr to 0
 * and the cur_zone_nr to 0
 *
 * The first prev_count entries in the table
 * belong to the previous wf.
 * The later one belong to the current wf
 */

static void add_ckpt_new_wf(struct ctx * ctx, sector_t wf)
{
	struct stl_ckpt *ckpt = ctx->ckpt;
	ckpt->cur_frontier_pba = wf;
}

static void add_ckpt_new_gc_wf(struct ctx * ctx, sector_t wf)
{
	struct stl_ckpt *ckpt = ctx->ckpt;
	ckpt->cur_gc_frontier_pba = wf;
}


void ckpt_flushed(struct bio *bio)
{
	struct ctx *ctx = bio->bi_private;

	bio_put(bio);
	bio_free_pages(bio);
	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt) {
		atomic_dec(&ctx->ckpt_ref);
	}
	spin_unlock(&ctx->ckpt_lock);
	wake_up(&ctx->ckptq);

	if (BLK_STS_OK != bio->bi_status) {
		/* TODO: Do something more to handle the errors */
		printk(KERN_DEBUG "\n Could not read the translation entry block");
	}
	return;
}

void revmap_bitmap_flushed(struct bio *);
/*
 * Since bitmap can be created in case of a crash, we do not 
 * wait for the bitmap to be flushed.
 * We only initiate the flush for the bitmap.
 */
void flush_revmap_bitmap(struct ctx *ctx)
{
	struct bio * bio;
	struct page *page;
	sector_t pba;

	page = ctx->revmap_bm;
	if (!page)
		return;

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		return;
	}
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return;
	}
	pba = ctx->sb->revmap_bm_pba;
	bio->bi_private = ctx;
	bio->bi_end_io = revmap_bitmap_flushed;
	bio->bi_iter.bi_sector = pba;
	printk(KERN_ERR "\n flushing revmap bitmap at pba: %u", ctx->sb->revmap_bm_pba);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	printk(KERN_ERR "\n About to aquire ckpt_lock!, revmap_bm pba: %llu", pba);
	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_inc(&ctx->ckpt_ref);
	spin_unlock(&ctx->ckpt_lock);
	printk(KERN_ERR "\n ckpt_lock released!\n");
	generic_make_request(bio);
	printk(KERN_ERR "\n %s bio submitted!", __func__);
	return;
}

/* We initiate a flush of a checkpoint,
 * we do not wait for it to complete
 */
void flush_checkpoint(struct ctx *ctx)
{
	struct bio * bio;
	struct page *page;
	sector_t pba;
	struct stl_ckpt * ckpt;

	page = ctx->ckpt_page;
	if (!page)
		return;

	ckpt = (struct stl_ckpt *)page_address(page);
	/* TODO: GC will not change the checkpoint, but will change
	 * the SIT Info.
	 */
	if (ckpt->clean)
		return;

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		return;
	}

	/* bio_add_page sets the bi_size for the bio */
	if (PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return;
	}
	/* Record the pba for the next ckpt */
	if (ctx->ckpt_pba == ctx->sb->ckpt1_pba)
		ctx->ckpt_pba = ctx->sb->ckpt2_pba;
	else
		ctx->ckpt_pba = ctx->sb->ckpt1_pba;
	pba = ctx->ckpt_pba;
	bio->bi_end_io = ckpt_flushed;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	printk(KERN_ERR "\n flushing checkpoint at pba: %llu", pba);
	bio_set_dev(bio, ctx->dev->bdev);
	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_inc(&ctx->ckpt_ref);
	spin_unlock(&ctx->ckpt_lock);
	bio->bi_private = ctx;
	generic_make_request(bio);
	return;
}


void flush_sit_node_page(struct ctx * ctx, struct rb_node *);


void flush_sit_nodes(struct ctx *ctx, struct rb_node *node)
{	
	flush_sit_node_page(ctx, node);
	if (node->rb_left)
		flush_sit_nodes(ctx, node->rb_left);
	if (node->rb_right)
		flush_sit_nodes(ctx, node->rb_right);
}

/*
 * We need to maintain the sit entries in some pages and keep the
 * pages in a linked list.
 * We can then later flush all the sit page in one go
 */
void flush_sit(struct ctx *ctx)
{
	struct rb_root *rb_root = &ctx->sit_rb_root;
	struct rb_node *node = NULL;
	struct blk_plug plug;

	node = rb_root->rb_node;
	if (!node)
		return;

	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	blk_start_plug(&plug);
	flush_sit_nodes(ctx, node);
	blk_finish_plug(&plug);	
}

void wait_for_ckpt_completion(struct ctx *ctx)
{
	spin_lock(&ctx->ckpt_lock);
	atomic_dec(&ctx->ckpt_ref);
	wait_event_lock_irq(ctx->ckptq, (!atomic_read(&ctx->ckpt_ref)), ctx->ckpt_lock);
	spin_unlock(&ctx->ckpt_lock);
}

inline u64 get_elapsed_time(struct ctx *ctx)
{
	time64_t now = ktime_get_real_seconds();

	if (now > ctx->mounted_time)
		return ctx->elapsed_time + now - ctx->mounted_time;

	return ctx->elapsed_time;
}

u32 calculate_crc(struct ctx *ctx, struct page *page)
{
	int err;
	struct {
		struct shash_desc shash;
		char ctx[4];
	} desc;
	const void *address = page_address(page);
	unsigned int length = PAGE_SIZE;

	BUG_ON(crypto_shash_descsize(ctx->s_chksum_driver) != sizeof(desc.ctx));

	desc.shash.tfm = ctx->s_chksum_driver;
	*(u32 *)desc.ctx = NSTL_MAGIC;
	err = crypto_shash_update(&desc.shash, address, length);
	BUG_ON(err);
	return *(u32 *)desc.ctx;
}

/* Only called from do_checkpoint 
 * during dtr 
 * we assume that sit is flushed by this point 
 */
void update_checkpoint(struct ctx *ctx)
{
	struct page * page;
	struct stl_ckpt * ckpt;

	page = ctx->ckpt_page;
	if (!page) {
		printk(KERN_ERR "\n NO checkpoint page found! ");
		return;
	}
	ckpt = (struct stl_ckpt *) page_address(page);
	if (ckpt->clean == 1)
		return;
	ckpt->user_block_count = ctx->user_block_count;
	ckpt->version += 1;
	ckpt->nr_invalid_zones = ctx->nr_invalid_zones;
	ckpt->cur_frontier_pba = ctx->app_write_frontier;
	printk(KERN_ERR "\n %s, ckpt->cur_frontier_pba: %llu version: %d", ckpt->cur_frontier_pba, ckpt->version);
	ckpt->cur_gc_frontier_pba = ctx->gc_write_frontier;
	ckpt->nr_free_zones = ctx->nr_freezones;
	ckpt->elapsed_time = get_elapsed_time(ctx);
	ckpt->clean = 1;
	//ckpt->crc = calculate_crc(ctx, page);
}

/* Always called with the ctx->lock held
 */
static void move_write_frontier(struct ctx *ctx, sector_t sectors_s8)
{
	/* We should have adjusted sectors_s8 to accomodate
	 * for the rooms in the zone before calling this function.
	 * Its how we split the bio
	 */
	if (ctx->free_sectors_in_wf < sectors_s8) {
		panic("Wrong manipulation of wf; used unavailable sectors in a log");
	}

	ctx->app_write_frontier = ctx->app_write_frontier + sectors_s8;
	ctx->free_sectors_in_wf = ctx->free_sectors_in_wf - sectors_s8;
	ctx->user_block_count -= sectors_s8 / NR_SECTORS_IN_BLK;
	if (ctx->free_sectors_in_wf < NR_SECTORS_IN_BLK) {
		if ((ctx->app_write_frontier - 1) != ctx->app_wf_end) {
			printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->app_write_frontier, ctx->app_wf_end);
			BUG_ON(ctx->app_write_frontier != (ctx->app_wf_end + 1));
		}
		get_new_zone(ctx);
		if (ctx->app_write_frontier < 0) {
			panic("No more disk space available for writing!");
		}
	}
}

/* Always called with the ctx->lock held
 */
static void move_gc_write_frontier(struct ctx *ctx, sector_t sectors_s8)
{
	/* We should have adjusted sectors_s8 to accomodate
	 * for the rooms in the zone before calling this function.
	 * Its how we split the bio
	 */
	if (ctx->free_sectors_in_gc_wf < sectors_s8) {
		panic("Wrong manipulation of gc wf; used unavailable sectors in a log");
	}	
	
	ctx->gc_write_frontier = ctx->gc_write_frontier + sectors_s8;
	ctx->free_sectors_in_gc_wf = ctx->free_sectors_in_gc_wf - sectors_s8;
	if (ctx->free_sectors_in_gc_wf < NR_SECTORS_IN_BLK) {
		if ((ctx->gc_write_frontier - 1) != ctx->gc_wf_end) {
			printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->app_write_frontier, ctx->app_wf_end);
			BUG_ON(ctx->gc_write_frontier != (ctx->gc_wf_end + 1));
		}
		get_new_gc_zone(ctx);
		if (ctx->gc_write_frontier < 0) {
			panic("No more disk space available for writing!");
		}
	}
}

/* 
 * We should be ideally be using to make sure that
 * one block worth of translation entries have made it to the 
 * disk. Not using this, will work in most case.
 * But no guarantees can be given
 *
 * Most code will only initiate the transfer of the data
 * but not ensure that the data is on disk.
 */
void wait_on_block_barrier(struct ctx * ctx)
{
	spin_lock(&ctx->rev_flush_lock);
	if (atomic_read(&ctx->nr_pending_writes)) {
		printk(KERN_ERR "\n waiting on block barrier, nr_pending_writes: %d", atomic_read(&ctx->nr_pending_writes));
		wait_event_lock_irq(ctx->rev_blk_flushq, 0 >= atomic_read(&ctx->nr_pending_writes), ctx->rev_flush_lock);
	}
	spin_unlock(&ctx->rev_flush_lock);
}

int is_revmap_block_available(struct ctx *ctx, u64 pba);
void flush_translation_blocks(struct ctx *ctx);

void wait_on_revmap_block_availability(struct ctx *ctx, u64 pba)
{

	spin_lock(&ctx->rev_flush_lock);
	if (1 == is_revmap_block_available(ctx, pba)) {
		spin_unlock(&ctx->rev_flush_lock);
		return;
	}
	spin_unlock(&ctx->rev_flush_lock);
/* You could wait here for ever if the translation blocks are not
 * flushed!
 */
	printk(KERN_ERR "\n About to flush translation blocks! before waiting on revmap blk availability!!!! \n");
	if (down_trylock(&ctx->flush_lock)) {
		flush_translation_blocks(ctx);
		up(&ctx->flush_lock);
	}

	spin_lock(&ctx->rev_flush_lock);
	/* wait until atleast one zone's revmap entries are flushed */
	wait_event_lock_irq(ctx->rev_blk_flushq, (1 == is_revmap_block_available(ctx, pba)), ctx->rev_flush_lock);
	spin_unlock(&ctx->rev_flush_lock);
}

void remove_sit_blk_kv_store(struct ctx *ctx, u64 pba)
{
	struct rb_root *rb_root = &ctx->sit_rb_root;
	struct sit_page *new;

	down_interruptible(&ctx->sit_kv_store_lock);
	/*--------------------------------------------*/
	new = search_sit_blk(ctx, pba);
	if (!new)
		return;

	rb_erase(&new->rb, rb_root);
	atomic_dec(&ctx->nr_sit_pages);
	/*--------------------------------------------*/
	up(&ctx->sit_kv_store_lock);
}


struct page * read_block(struct ctx *ctx, u64 blknr, u64 base, int nrblks);
/*
 * pba: stored in the LBA - PBA translation.
 * This is the PBA of some data block. This PBA belongs to some zone.
 * We are about to update the SIT entry that belongs to that zone
 * Before that we read that corresponding page in memory and then
 * add it to our RB tree that we use for searching.
 *
 */
struct sit_page * add_sit_entry_kv_store(struct ctx * ctx, sector_t pba)
{
	sector_t sit_blknr;
	u64 zonenr = get_zone_nr(ctx, pba);
	struct rb_root *root = &ctx->sit_rb_root;
	struct rb_node *parent = NULL;
	struct sit_page *parent_ent;
	struct sit_page *new;
	struct page *page;

	down_interruptible(&ctx->sit_kv_store_lock);
	/*--------------------------------------------*/
	new = search_sit_kv_store(ctx, pba, &parent);
	if (new)
		return new;

	new = kmem_cache_alloc(ctx->sit_page_cache, GFP_KERNEL);
	if (!new)
		return NULL;

	RB_CLEAR_NODE(&new->rb);

	printk(KERN_ERR " %s zonenr: %lld ", __func__, zonenr);

	sit_blknr = zonenr / SIT_ENTRIES_BLK;
	printk(KERN_ERR " %s sit_blknr: %lld sit_pba: %u", __func__, sit_blknr, ctx->sb->sit_pba);
	page = read_block(ctx, sit_blknr, ctx->sb->sit_pba, 1);
	if (!page) {
		kmem_cache_free(ctx->sit_page_cache, new);
		return NULL;
	}

	/* We put the page when the page gets written to the disk 
	 * We increment the reference so that the page is not freed
	 * under us!
	 */
	get_page(page);
	/* We dont need to lock the page as the reference is not added
	 * for any other thread to access it yet
	 */
	ClearPageDirty(page);
	new->blknr = sit_blknr;
	new->page = page;

	if (parent) {
		/* Add this page to a RB tree based KV store.
		 * Key is: blknr for this corresponding block
		 */
		parent_ent = rb_entry(parent, struct sit_page, rb);
		if (sit_blknr < parent_ent->blknr) {
			/* Attach new node to the left of parent */
			rb_link_node(&new->rb, parent, &parent->rb_left);
		}
		else { 
			/* Attach new node to the right of parent */
			rb_link_node(&new->rb, parent, &parent->rb_right);
		}
	} else {
		rb_link_node(&new->rb, parent, &root->rb_node);
	}
	/* Balance the tree after the blknr is addded to it */
	rb_insert_color(&new->rb, root);
	
	atomic_inc(&ctx->nr_sit_pages);
	atomic_inc(&ctx->sit_flush_count);

	if (atomic_read(&ctx->sit_flush_count) >= MAX_SIT_PAGES) {
	/*--------------------------------------------*/
		up(&ctx->sit_kv_store_lock);

		if (down_trylock(&ctx->flush_lock)) {
			/* Only one flush operation at a time 
			 * but we dont want to wait.
			 */
			atomic_set(&ctx->sit_flush_count, 0);
			flush_sit(ctx);
			up(&ctx->flush_lock);
		}
	} else {
	/*--------------------------------------------*/
		up(&ctx->sit_kv_store_lock);
	}
	return new;
}

/*TODO: IMPLEMENT 
 */
void mark_segment_free(struct ctx *ctx, sector_t zonenr)
{
	return;
}

int update_inmem_sit(struct ctx *, unsigned int , u32 , u64 );

/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_vblocks_decr(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct stl_seg_entry *ptr;
	sector_t zonenr;
	int index;
	struct rb_node *parent = NULL;

	down_interruptible(&ctx->sit_kv_store_lock);
	/*--------------------------------------------*/
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	/*--------------------------------------------*/
	up(&ctx->sit_kv_store_lock);
	if (!sit_page) {
		sit_page= add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, could not allocate sit_entry");
		}
	}

	spin_lock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	get_page(sit_page->page);
	ptr = (struct stl_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = ptr->vblocks - 1;
	if (!ptr->vblocks) {
		spin_lock(&ctx->lock);
		mark_zone_free(ctx , zonenr);
		spin_unlock(&ctx->lock);
	}
	SetPageDirty(sit_page->page);
	put_page(sit_page->page);
	/*--------------------------------------------*/
	spin_unlock(&ctx->sit_flush_lock);
	update_inmem_sit(ctx, zonenr, ptr->vblocks, ptr->mtime);
}

static void mark_zone_occupied(struct ctx *, int );
/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_vblocks_incr(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct stl_seg_entry *ptr;
	struct rb_node *parent = NULL;
	sector_t zonenr;
	int index;

	down_interruptible(&ctx->sit_kv_store_lock);
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	up(&ctx->sit_kv_store_lock);
	if (!sit_page) {
		sit_page = add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, could not allocate sit_entry");
		}
	}
	spin_lock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	get_page(sit_page->page);
	ptr = (struct stl_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	printk(KERN_ERR "\n %s: pba: %llu zonenr: %llu", __func__, pba, zonenr);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	if (!ptr->vblocks) {
		spin_lock(&ctx->lock);
		mark_zone_occupied(ctx, zonenr);
		spin_unlock(&ctx->lock);
	}
	ptr->vblocks = ptr->vblocks + 1;
	SetPageDirty(sit_page->page);
	put_page(sit_page->page);
	/*--------------------------------------------*/
	spin_unlock(&ctx->sit_flush_lock);
}


/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_add_mtime(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct stl_seg_entry *ptr;
	struct rb_node *parent = NULL;
	sector_t zonenr;
	int index;

	down_interruptible(&ctx->sit_kv_store_lock);
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	up(&ctx->sit_kv_store_lock);
	if (unlikely(!sit_page)) {
		sit_page = add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, could not allocate sit_entry");
		}
	}
	spin_lock(&ctx->sit_flush_lock);
	/*------------------------------------------*/
	get_page(sit_page->page);
	SetPageDirty(sit_page->page);
	ptr = (struct stl_seg_entry*) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->mtime = get_elapsed_time(ctx);
	if (ctx->max_mtime < ptr->mtime)
		ctx->max_mtime = ptr->mtime;
	put_page(sit_page->page);
	/*--------------------------------------------*/
	spin_unlock(&ctx->sit_flush_lock);
	update_inmem_sit(ctx, zonenr, ptr->vblocks, ptr->mtime);
}

struct tm_page * search_tm_kv_store(struct ctx *ctx, u64 blknr, struct rb_node **parent);
struct tm_page *add_tm_page_kv_store(struct ctx *ctx, u64 lba, struct revmap_meta_inmem *revmap_bio_ctx);
/*
 * If this length cannot be accomodated in this page
 * search and add another page for this next
 * lba. Remember this translation table will
 * go on the disk and is block based and not 
 * extent based
 *
 * Depending on the location within a page, add the lba.
 */
int add_translation_entry(struct ctx * ctx, struct page *page, unsigned long lba, 
			  unsigned long pba, unsigned long len, struct revmap_meta_inmem *revmap_bio_ctx)
{
	struct tm_entry * ptr;
	int i, index;
	struct rb_node *parent = NULL;
	struct tm_page *tm_page;

	ptr = (struct tm_entry *) page_address(page);
	printk(KERN_ERR "\n %s tm_page address: %p lba: %llu, pba:%llu, len: %d", __func__, ptr, lba, pba, len);
	index = (lba/NR_SECTORS_IN_BLK);
	ptr = ptr + index;

	/* Assuming len is in terms of sectors 
	 * We convert sectors to blocks
	 */
	for (i=0; i<len/8; i++) {
		spin_lock(&ctx->tm_flush_lock);
	/*-----------------------------------------------*/
		get_page(page);
		if (ptr->lba != 0) {
			/* decrement vblocks for the segment that has
			 * the stale block
			 */
			printk(KERN_ERR "\n Overwrite a block at LBA: %llu, PBA: %llu", ptr->lba, ptr->pba);
			sit_ent_vblocks_decr(ctx, ptr->pba);
		}
		ptr->lba = lba;
		ptr->pba = pba;
		sit_ent_vblocks_incr(ctx, ptr->pba);
		if (pba == zone_end(ctx, pba)) {
			sit_ent_add_mtime(ctx, ptr->pba);
		}
		printk(KERN_ERR "Added TM entry at index:%d lba: %lu, pba: %lu, len: 1 block", index, lba, pba);
		lba = lba + NR_SECTORS_IN_BLK;
		pba = pba + NR_SECTORS_IN_BLK;
		ptr = ptr + 1;
		SetPageDirty(page);
		put_page(page);
	/*-----------------------------------------------*/
		spin_unlock(&ctx->tm_flush_lock);
		index = index + 1;
		if (TM_ENTRIES_BLK == index) {
			down_interruptible(&ctx->tm_kv_store_lock);
	/*-----------------------------------------------*/
			tm_page = search_tm_kv_store(ctx, lba, &parent);
			if (!tm_page) {
				tm_page = add_tm_page_kv_store(ctx, lba, revmap_bio_ctx);
				if (!tm_page) {
					up(&ctx->tm_kv_store_lock);
					/* TODO: try freeing some
					 * pages here
					 */
					panic("Low memory, while adding tm entry ");
				}
			}
	/*-----------------------------------------------*/
			up(&ctx->tm_kv_store_lock);
			page = tm_page->page;
			ptr = (struct tm_entry *) page_address(page);
			index = 0;
		}
	}
	return 0;	
}

void wake_up_refcount_waiters(struct ctx *ctx);

/*
 * Note that blknr is 4096 bytes aligned. Whereas our 
 * LBA is 512 bytes aligned. So we convert the blknr
 *
 * TODO: implement the reading of more blks.
 */
struct page * read_block(struct ctx *ctx, u64 blknr, u64 base, int nrblks)
{
	struct bio * bio;
	struct page *page;

	u64 pba = (base + blknr) * NR_SECTORS_IN_BLK;
    	page = alloc_page(__GFP_ZERO|GFP_KERNEL);
	if (!page )
		return NULL;

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		__free_pages(page, 0);
		return NULL;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		__free_pages(page, 0);
		bio_put(bio);
		return NULL;
	}
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_iter.bi_sector = pba;
	bio_set_dev(bio, ctx->dev->bdev);
	submit_bio_wait(bio);
	if (bio->bi_status != BLK_STS_OK) {
		printk(KERN_DEBUG "\n Could not read the translation entry block");
		bio_free_pages(bio);
		bio_put(bio);
		return NULL;
	}
	/* bio_alloc() hence bio_put() */
	bio_put(bio);
	return page;
}

/*
 * If a match is found, then returns the matching
 * entry 
 */
struct tm_page * search_tm_kv_store(struct ctx *ctx, u64 blknr, struct rb_node **parent)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = NULL;

	struct tm_page *node_ent;

	node = root->rb_node;
	while(node) {
		*parent = node;
		node_ent = rb_entry(*parent, struct tm_page, rb);
		if (blknr == node_ent->blknr) {
			return node_ent;
		}
		if (blknr < node_ent->blknr) {
			node = node->rb_left;
		} else {
			node = node->rb_right;
		}
	}
	return NULL;
}


void remove_tm_entry_kv_store(struct ctx *ctx, u64 lba)
{
	u64 start_lba = lba - lba % TM_ENTRIES_BLK;
	u64 blknr = start_lba / TM_ENTRIES_BLK;
	struct rb_node *parent = NULL;
	struct rb_root *rb_root = &ctx->tm_rb_root;

	struct tm_page *new;

	down_interruptible(&ctx->tm_kv_store_lock);
	/*----------------------------------------------*/
	new = search_tm_kv_store(ctx, blknr, &parent);
	if (!new)
		return;

	rb_erase(&new->rb, rb_root);
	atomic_dec(&ctx->nr_tm_pages);
	/*----------------------------------------------*/
	up(&ctx->tm_kv_store_lock);
}


/* Called under a spin lock. Don't put any functions that
 * sleep
 */
void remove_tm_blk_kv_store(struct ctx *ctx, u64 blknr)
{
	struct rb_node *parent = NULL;
	struct rb_root *rb_root = &ctx->tm_rb_root;
	struct tm_page *new;

	cant_sleep();

	down_interruptible(&ctx->tm_kv_store_lock);
	/*----------------------------------------------*/
	new = search_tm_kv_store(ctx, blknr, &parent);
	if (!new) {
		up(&ctx->tm_kv_store_lock);
		return;
	}

	/*----------------------------------------------*/
	up(&ctx->tm_kv_store_lock);
	kmem_cache_free(ctx->tm_page_cache, new);
}


void clear_revmap_bit(struct ctx *, u64);

void revmap_block_release(struct kref *kref)
{
	struct revmap_meta_inmem * revmap_bio_ctx;
	struct ctx *ctx;

	revmap_bio_ctx = container_of(kref, struct revmap_meta_inmem, kref);
	ctx = revmap_bio_ctx->ctx;

	/* now we calculate the pba where the revmap
	 * is flushed. We can reuse that pba as all
	 * entries related to it are on disk in the
	 * right place.
	 */
	spin_lock(&ctx->rev_flush_lock);
	/*-------------------------------------------------------------*/
	clear_revmap_bit(ctx, revmap_bio_ctx->revmap_pba);
	/*-------------------------------------------------------------*/
	spin_unlock(&ctx->rev_flush_lock);
	kmem_cache_free(ctx->revmap_bioctx_cache, revmap_bio_ctx);
	/* Wakeup waiters waiting on revblk bit that 
	 * indicates that revmap block is rewritable
	 * since all the containing translation map
	 * entries are now on disk
	 *
	 * waiters call: wait_on_revmap_block_availability()
	 */
	printk(KERN_ERR "\n %s done!!", __func__);
	wake_up(&ctx->rev_blk_flushq);
	printk(KERN_ERR "\n wait is over!!");
}

/*
 * Note: 
 * When 100 translation pages collect in memory, they are
 * flushed to disk. This flushing function also holds the
 * tm_flush_lock at certain points when it checks the page
 * flag status.
 */
void write_tmbl_complete(struct bio *bio)
{
	struct ctx *ctx;
	struct tm_page_write_ctx *tm_page_write_ctx;
	struct page *page;
	struct tm_page *tm_page;
	struct list_head *temp;
	struct ref_list *refnode;
	struct revmap_meta_inmem * revmap_bio_ctx;
	sector_t blknr;

	tm_page_write_ctx = (struct tm_page_write_ctx *) bio->bi_private;
	ctx = tm_page_write_ctx->ctx;
	tm_page = tm_page_write_ctx->tm_page;
	page = tm_page->page;
	printk(KERN_ERR "\n %s page:%p", __func__, page_address(page));

	/* We notify all those reverse map pages that are stuck on
	 * these respective refcounts
	 */
	list_for_each(temp, &tm_page->reflist.list) {
		refnode = list_entry(temp, struct ref_list, list);
		spin_lock(&ctx->tm_ref_lock);
		revmap_bio_ctx = refnode->revmap_bio_ctx;
		spin_unlock(&ctx->tm_ref_lock);
		kref_put(&revmap_bio_ctx->kref, revmap_block_release);
		printk(KERN_ERR "\n %s %d revmap_bio_ctx->kref -- ", __func__, revmap_bio_ctx->revmap_pba);
		kmem_cache_free(ctx->reflist_cache, refnode);
	}

	if (bio->bi_status != BLK_STS_OK) {
		/*TODO: Perhaps retry!! or do something more
		 * or else you will loose the translation entries.
		 * write them some place else!
		 */
		printk(KERN_ERR "\n %s bi_status: %d \n", bio->bi_status);
		panic("Could not write the translation entry block");
	}

	printk(KERN_ERR "\n %s done! 1. ", __func__);
	/*-----------------------------------------------*/
	/* If the page was locked, then since the page was submitted,
	 * write has been attempted
	 */	
	spin_lock(&ctx->tm_flush_lock);
	if (!PageDirty(page)) {
		/* remove the page from RB tree and reduce the total
		 * count, through the remove_tm_blk_kv_store()
		 */
		blknr = bio->bi_iter.bi_sector - ctx->sb->tm_pba;
		remove_tm_blk_kv_store(ctx, blknr);
		put_page(page);
		bio_free_pages(bio);
	}
	spin_unlock(&ctx->tm_flush_lock);
	atomic_dec(&ctx->tm_flush_count);
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);
	spin_lock(&ctx->ckpt_lock);
	printk(KERN_ERR "\n %s done!!!", __func__);
	if(ctx->flag_ckpt) {
		atomic_dec(&ctx->ckpt_ref);
	}
	spin_unlock(&ctx->ckpt_lock);
	wake_up(&ctx->ckptq);

}

/* We don't wait for the bios to complete 
 * flushing. We only initiate the flushing
 */
void flush_tm_node_page(struct ctx *ctx, struct rb_node *node)
{
	struct page *page; 
	struct tm_page *tm_page;
	u64 pba;
	struct bio * bio;
	struct tm_page_write_ctx *tm_page_write_ctx;

	tm_page = rb_entry(node, struct tm_page, rb);
	if (!tm_page)
		return;

	page = tm_page->page;
	if (!page)
		return;

	printk(KERN_ERR "\n Inside %s \n", __func__);

	/* Only flush if the page needs flushing */

	spin_lock(&ctx->tm_flush_lock);
	/*--------------------------------------*/
	if (!PageDirty(page)) {
		spin_unlock(&ctx->tm_flush_lock);
		printk(KERN_ERR "\n TM Page is not dirty!");
		return;
	}
	/*--------------------------------------*/
	ClearPageDirty(page);
	spin_unlock(&ctx->tm_flush_lock);

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		return;
	}
	
	tm_page_write_ctx = kmem_cache_alloc(ctx->tm_page_write_cache, GFP_KERNEL);
	if (!tm_page_write_ctx) {
		bio_put(bio);
		return;
	}
	tm_page_write_ctx->tm_page = tm_page;
       	tm_page_write_ctx->ctx = ctx;

	/* Sector addressing, LBA is the address of the sector */
	pba = (tm_page->blknr * NR_SECTORS_IN_BLK) + ctx->sb->tm_pba;
	page = tm_page->page;
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		kmem_cache_free(ctx->tm_page_write_cache, tm_page_write_ctx);
		return;
	}
	//printk(KERN_ERR "\n flushing tm block at pba: %llu page: %p", pba, page_address(page));
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_iter.bi_sector = pba;
	bio->bi_private = tm_page_write_ctx;
	bio->bi_end_io = write_tmbl_complete;
	spin_lock(&ctx->ckpt_lock);
	/* The next code is related to synchronizing at dtr() time.
	 */
	if(ctx->flag_ckpt) {
		atomic_inc(&ctx->ckpt_ref);
	}
	spin_unlock(&ctx->ckpt_lock);
printk(KERN_ERR "\n %s bio->bi_sector: %llu bio->bi_size: %u page:%p", __func__, bio->bi_iter.bi_sector, bio->bi_iter.bi_size, page_address(page));
printk(KERN_ERR "\n %s max_pba: %llu", ctx->max_pba);
	bio->bi_status = BLK_STS_OK;
	write_tmbl_complete(bio);
	//generic_make_request(bio);
}


void flush_tm_nodes(struct rb_node *node, struct ctx *ctx)
{	
	flush_tm_node_page(ctx, node);
	if (node->rb_left)
		flush_tm_nodes(node->rb_left, ctx);
	if (node->rb_right)
		flush_tm_nodes(node->rb_right, ctx);
}

int read_translation_map(struct ctx *);

void flush_translation_blocks(struct ctx *ctx)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = NULL;
	struct blk_plug plug;

	node = root->rb_node;
	if (!node) {
		printk(KERN_ERR "\n tm node is NULL!");
		return;
	}

	printk(KERN_ERR "\n %s ", __func__);

	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	spin_lock(&ctx->ckpt_lock);
	ctx->flag_ckpt = 1;
	atomic_set(&ctx->ckpt_ref, 2);
	spin_unlock(&ctx->ckpt_lock);

	//blk_start_plug(&plug);
	flush_tm_nodes(node, ctx);
	//blk_finish_plug(&plug);	
	atomic_dec(&ctx->ckpt_ref);
	wait_for_ckpt_completion(ctx);
	printk(KERN_ERR "\n %s done!!", __func__);
}

void flush_count_tm_nodes(struct ctx *ctx, struct rb_node *node, int *count, bool flush, int nrscan)
{	
	if (flush) {
		flush_tm_node_page(ctx, node);
		if (*count == nrscan)
			return;
	}

	*count = *count + 1;
	if (node->rb_left)
		flush_count_tm_nodes(ctx, node->rb_left, count, flush, nrscan);
	if (node->rb_right)
		flush_count_tm_nodes(ctx, node->rb_right, count, flush, nrscan);
}


void flush_count_tm_blocks(struct ctx *ctx, bool flush, int nrscan)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = NULL;
	struct blk_plug plug;
	int count;

	if (flush && !nrscan)
		return;
	node = root->rb_node;
	if (!node)
		return;
	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	if (flush)
		blk_start_plug(&plug);
	flush_count_tm_nodes(ctx, node, &count, flush, nrscan);
	if (flush)
		blk_finish_plug(&plug);
	return;
}


void write_sitbl_complete(struct bio *bio)
{
	struct ctx *ctx;
	struct sit_page_write_ctx *sit_ctx;
	struct page *page;
	sector_t blknr;

	sit_ctx = (struct sit_page_write_ctx *) bio->bi_private;
	ctx = sit_ctx->ctx;
	page = sit_ctx->page;
	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt) {
		atomic_dec(&ctx->ckpt_ref);
	}
	spin_unlock(&ctx->ckpt_lock);
	wake_up(&ctx->ckptq);

	if (bio->bi_status != BLK_STS_OK) {
		/*TODO: Perhaps retry!! or do something more
		 * or else you will loose the translation entries.
		 * write them some place else!
		 */
		printk(KERN_DEBUG "\n Could not read the translation entry block");
		return;
	}
	spin_lock(&ctx->sit_flush_lock);
	/*-------------------------------------*/
	/* We have stored relative blk address, whereas disk supports
	 * sector addressing
	 * TODO: store the sector address directly 
	 */
	blknr = (bio->bi_iter.bi_sector - ctx->sb->sit_pba) / NR_SECTORS_IN_BLK;
	if (!PageDirty(page)) {
		/* For memory conservation we do this freeing of pages
		 * TODO: we could free them only if our memory usage
		 * is above a certain point
		 */
		remove_sit_blk_kv_store(ctx, blknr);
		put_page(page);
		bio_free_pages(bio);
	}
	/*-------------------------------------*/
	spin_unlock(&ctx->sit_flush_lock);
	atomic_dec(&ctx->sit_flush_count);
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);
}



/* We don't wait for the bios to complete 
 * flushing. We only initiate the flushing
 */
void flush_sit_node_page(struct ctx *ctx, struct rb_node *node)
{
	struct page *page; 
	struct sit_page *node_ent;
	u64 pba;
	struct bio * bio;
	struct sit_page_write_ctx *sit_ctx;

	/* Do not flush if the page is not dirty */

	node_ent = rb_entry(node, struct sit_page, rb);
	page = node_ent->page;
	if (!page)
		return;

	spin_lock(&ctx->sit_flush_lock);
	if (unlikely(!PageDirty(page))) {
		spin_unlock(&ctx->sit_flush_lock);
		return;
	}
	spin_unlock(&ctx->sit_flush_lock);

	sit_ctx = kmem_cache_alloc(ctx->sit_ctx_cache, GFP_KERNEL);
	if (!sit_ctx) {
		printk(KERN_ERR "\n Low Memory ! ");
		return;
	}

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		kmem_cache_free(ctx->sit_ctx_cache, sit_ctx);
		return;
	}
	
		/* Sector addressing */
	pba = (node_ent->blknr * NR_SECTORS_IN_BLK) + ctx->sb->sit_pba;
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return;
	}
	bio_set_dev(bio, ctx->dev->bdev);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	bio->bi_private = sit_ctx;
	bio->bi_end_io = write_sitbl_complete;
	printk(KERN_ERR "\n flush sit node page at lba: %llu", pba);
	sit_ctx->ctx = ctx;
	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_inc(&ctx->ckpt_ref);
	spin_unlock(&ctx->ckpt_lock);
	sit_ctx->page = page;
	spin_lock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	ClearPageDirty(page);
	/*-------------------------*/
	spin_unlock(&ctx->sit_flush_lock);
	generic_make_request(bio);
}

void flush_count_sit_nodes(struct ctx *ctx, struct rb_node *node, int *count, bool flush, int nrscan)
{	
	if (flush) {
		flush_sit_node_page(ctx, node);

		if (*count == nrscan)
			return;
	}

	*count = *count + 1;
	if (node->rb_left)
		flush_count_sit_nodes(ctx, node->rb_left,  count, flush, nrscan);
	if (node->rb_right)
		flush_count_sit_nodes(ctx, node->rb_right, count, flush, nrscan);
}


void flush_count_sit_blocks(struct ctx *ctx, bool flush, int nrscan)
{
	struct rb_root *root = &ctx->sit_rb_root;
	struct rb_node *node = NULL;
	struct blk_plug plug;
	int count;

	if (flush && !nrscan)
		return;
	node = root->rb_node;
	if (!node)
		return;
	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	if (flush)
		blk_start_plug(&plug);
	flush_count_sit_nodes(ctx, node, &count, flush, nrscan);
	if (flush)
		blk_finish_plug(&plug);
}

/*
 * lba: from the LBA-PBA pair of a data block.
 * Should be called with
 *
	(&ctx->tm_kv_store_lock);
 */
struct tm_page *add_tm_page_kv_store(struct ctx *ctx, u64 lba, struct revmap_meta_inmem *revmap_bio_ctx)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *parent = NULL;
	struct tm_page *new_tmpage, *parent_ent;
	u64 blknr = lba / TM_ENTRIES_BLK;
	struct list_head *temp;
	struct ref_list *refnode, *tempref;

	printk(KERN_ERR "\n add_tm_page_kv_store: lba: %llu \n", lba);

	refnode = kmem_cache_alloc(ctx->reflist_cache, GFP_KERNEL);
	if (!refnode) {
		return NULL;
	}
	INIT_LIST_HEAD(&refnode->list);

	new_tmpage = search_tm_kv_store(ctx, blknr, &parent);
	if (new_tmpage) {
		list_for_each(temp, &new_tmpage->reflist.list) {
			tempref = list_entry(temp, struct ref_list, list);
			if (tempref->revmap_bio_ctx->revmap_pba  == revmap_bio_ctx->revmap_pba) {
				kmem_cache_free(ctx->reflist_cache, refnode);
				return new_tmpage;
			}
		}
		printk(KERN_ERR "\n refnode created! \n");
		/* Add a refcount, only when this rev map page is used
		 * for the first time
		 */
		refnode->revmap_bio_ctx = revmap_bio_ctx;
		kref_get(&revmap_bio_ctx->kref);
		/*add refnode after head */
		list_add(&refnode->list, &new_tmpage->reflist.list);
		return new_tmpage;
	}

	printk(KERN_ERR "\n refnode created! Allocating 'new' from tm_page_cache! \n");
	new_tmpage = kmem_cache_alloc(ctx->tm_page_cache, GFP_KERNEL);
	if (!new_tmpage) {
		kmem_cache_free(ctx->reflist_cache, refnode);
		return NULL;
	}
	
	RB_CLEAR_NODE(&new_tmpage->rb);

	new_tmpage->page = read_block(ctx, blknr, ctx->sb->tm_pba, 1);
	if (!new_tmpage->page) {
		kmem_cache_free(ctx->reflist_cache, refnode);
		kmem_cache_free(ctx->tm_page_cache, new_tmpage);
		return NULL;
	}
	printk(KERN_ERR "\n %s tm node->page: %p", __func__, page_address(new_tmpage->page));
	new_tmpage->blknr = blknr;
	/* This is a new page, cannot be dirty, dont flush from a
	 * parallel thread! */
	ClearPageDirty(new_tmpage->page);
	/* We get a reference to this page, so that it is not freed
	 * underneath us. We put the reference in the flush tm page
	 * code, just before freeing it.
	 */
	get_page(new_tmpage->page);
	refnode->revmap_bio_ctx = revmap_bio_ctx;
	kref_get(&revmap_bio_ctx->kref);
	INIT_LIST_HEAD(&new_tmpage->reflist.list);
	list_add(&refnode->list, &new_tmpage->reflist.list);

	printk(KERN_ERR "\n TM Block read! (about to be added to the tm kv store)!) \n");
	/* Add this page to a RB tree based KV store.
	 * Key is: blknr for this corresponding block
	 */
	if (parent) {
		parent_ent = rb_entry(parent, struct tm_page, rb);
		if (blknr < parent_ent->blknr) {
			/* Attach new node to the left of parent */
			rb_link_node(&new_tmpage->rb, parent, &parent->rb_left);
		}
		else { 
			/* Attach new node to the right of parent */
			rb_link_node(&new_tmpage->rb, parent, &parent->rb_right);
		}
	} else {
		rb_link_node(&new_tmpage->rb, parent, &root->rb_node);
	}
	/* Balance the tree after node is addded to it */
	rb_insert_color(&new_tmpage->rb, root);
	atomic_inc(&ctx->nr_tm_pages);
	atomic_inc(&ctx->tm_flush_count);
	if (atomic_read(&ctx->tm_flush_count) >= MAX_TM_PAGES) {
		/*---------------------------------------------*/
		up(&ctx->tm_kv_store_lock);
		if (down_trylock(&ctx->flush_lock)) {
			atomic_set(&ctx->tm_flush_count, 0);
			/* Only one flush operation at a time 
			 * but we dont want to wait.
			 */
			flush_translation_blocks(ctx);
			up(&ctx->flush_lock);
		}
		down_interruptible(&ctx->tm_kv_store_lock);
	}
	return new_tmpage;
}

/* Make the length in terms of sectors or blocks?
 *
 */
int add_block_based_translation(struct ctx *ctx, struct page *page, struct revmap_meta_inmem *revmap_bio_ctx)
{
	
	struct stl_revmap_entry_sector * ptr;
	struct tm_page * tm_page = NULL;
	int i, j, len = 0;
	sector_t lba, pba;

	ptr = (struct stl_revmap_entry_sector *)page_address(page);
	i = 0;
	while (i < NR_SECTORS_IN_BLK) {
		for(j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			if (0 == ptr->extents[j].pba)
				continue;
			lba = ptr->extents[j].lba;
			pba = ptr->extents[j].pba;
			len = ptr->extents[j].len;
			if (len == 0) {
				continue;
			}
			printk(KERN_ERR "Adding TM entry: ptr->extents[j].lba: %llu, ptr->extents[j].pba: %llu, ptr->extents[j].len: %d", lba, pba, len);
			down_interruptible(&ctx->tm_kv_store_lock);
			tm_page = add_tm_page_kv_store(ctx, lba, revmap_bio_ctx);
			up(&ctx->tm_kv_store_lock);
			if (!tm_page)
				return -ENOMEM;
			add_translation_entry(ctx, tm_page->page, lba, pba, len, revmap_bio_ctx);
		}
		ptr = ptr + 1;
		i++;
	}
	return 0;
}

/*
 * Revmap bitmap: 0 indicates that a block is available for reuse.
 * 1 indicates that the entries on that revblock are not yet flushed
 * at its correct location in the translation map.
 * The map is stored at sector pba=sb->revmap. 
 * Revmap store 2 * 655536 entries that is equal to the number
 * of blocks in 2 zones. Each entry is 80 bytes and each sector
 * of 512 bytes stores 6 such entries. To store 131072 entries we need
 * 21845 sectors, that is 2731 blocks. Thus our bitmap should have
 * 2731 bits i.e 342 bytes. Thus the bitmap spans one sector on disk
 */

void revmap_bitmap_flushed(struct bio *bio)
{
	struct ctx *ctx = bio->bi_private;
	
	if(!ctx) {
		bio_put(bio);
		return;
	}

	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt) {
		atomic_dec(&ctx->ckpt_ref);
	}
	spin_unlock(&ctx->ckpt_lock);
	wake_up(&ctx->ckptq);
	switch(bio->bi_status) {
		case BLK_STS_OK:
			/* bio_alloc, hence bio_put */
			bio_put(bio);
			break;
		default:
			/*TODO: do something, for now panicing */
			printk(KERN_ERR "\n Could not flush revmap bitmap");
			panic("IO error while flushing revmap block! Handle this better");
			break;
	}
}

/*
 * Mark a block in use: set appropriate bit to 1
 */
void mark_revmap_bit(struct ctx *ctx, u64 pba)
{
	char *ptr;
	int bytenr;
	int bitnr;
	unsigned char mask = 0;
	struct page *page;

	pba = pba - ctx->sb->revmap_pba;
	pba = pba/NR_SECTORS_IN_BLK;
	if (pba < 0) {
		printk(KERN_ERR "\n WRONG PBA!!");
		panic("Bad PBA for revmap block!");
	}
	bytenr = pba/BITS_IN_BYTE;
	bitnr = pba % BITS_IN_BYTE;
	printk(KERN_ERR "\n %s pba: %llu bytenr: %d bitnr: %d", __func__, pba, bytenr, bitnr);
	mask = (1 << bitnr);
	/* Only one revmap bm block is stored. */
	if (bytenr >= 4096) {
		panic("revmap bm calculations are wrong!");
	}
	page = ctx->revmap_bm;
	ptr = page_address(page);

	ptr = ptr + bytenr;
	*ptr = *ptr | mask;
}

/* 
 * Make a block available for reuse
 *
 * When a block is available, bit is set to 0
 */
void clear_revmap_bit(struct ctx *ctx, u64 pba)
{
	char *ptr;
	int bytenr;
	int bitnr;
	unsigned char mask;
	struct page *page;

	pba = pba - ctx->sb->revmap_pba;
	pba = pba/NR_SECTORS_IN_BLK;
	bytenr = pba/BITS_IN_BYTE;
	bitnr = pba % BITS_IN_BYTE;
	printk(KERN_ERR "\n %s pba: %llu bytenr: %d bitnr: %d", __func__, pba, bytenr, bitnr);
	mask = ~(1 << bitnr);
	/* Only one revmap bm block is stored. */
	if (bytenr >= 4096) {
		panic("revmap bm calculations are wrong!");
	}
	page = ctx->revmap_bm;
	ptr = page_address(page);

	ptr = ptr + bytenr;
	*ptr = *ptr & mask;
}

/* 
 * Returns 1 when block is available
 */
int is_revmap_block_available(struct ctx *ctx, u64 pba)
{
	char *ptr;
	int bytenr;
	int bitnr;
	int i = 0;
	char temp;
	struct page *page;

	printk(KERN_ERR "\n %s pba: %llu, sb->revmap_pba: %llu", __func__, pba, ctx->sb->revmap_pba);
	pba = pba - ctx->sb->revmap_pba;
	pba = pba/NR_SECTORS_IN_BLK;
	bytenr = pba/BITS_IN_BYTE;
	bitnr = pba % BITS_IN_BYTE;
	/* Only one revmap bm block is stored. */
	if (bytenr >= 4096) {
		panic("revmap bm calculations are wrong!");
	}
	page = ctx->revmap_bm;
	ptr = page_address(page);
	ptr = ptr + bytenr;
	temp = *ptr;
	printk(KERN_ERR "\n %s relative pba: %llu bytenr: %d bitnr: %d temp: %d", __func__, pba, bytenr, bitnr, temp);

	while(i < bitnr) {
		i++;
		temp = temp >> 1;
	}
	if ((temp & 1) == 1)
		return 0;
	
	/* else bit is 0 and thus block is available */
	return 1;

}

/* TODO: IMPLEMENT 
 * Analyze if we do need multiple ref queues; one per ref
 */
void wake_up_refcount_waiters(struct ctx *ctx)
{
	wake_up(&ctx->refq);
}

void wait_on_refcount(struct ctx *ctx, refcount_t *ref)
{
	spin_lock(&ctx->tm_ref_lock);
	wait_event_lock_irq(ctx->refq, (1 == refcount_read(ref)), ctx->tm_ref_lock);
	spin_unlock(&ctx->tm_ref_lock);
}


/*
 * TODO: Error handling metadata flushing
 *
 */
int revmap_entries_flushed(void *data)
{
	struct ctx * ctx;
	sector_t pba;
	struct revmap_meta_inmem *revmap_bio_ctx = (struct revmap_meta_inmem *)data;
	struct page *page;

	wait_for_completion(&revmap_bio_ctx->io_done);

	pba = revmap_bio_ctx->revmap_pba;
	ctx = revmap_bio_ctx->ctx;
	printk(KERN_ERR "\n revmap_entries flushed at pba: %llu ", pba);

	page = revmap_bio_ctx->page;
	printk(KERN_ERR "\n %s About to add_block_based_translation(): revmap_bio_ctx->page: %p", __func__, page_address(page));
	add_block_based_translation(ctx, page, revmap_bio_ctx);
	spin_lock(&ctx->rev_flush_lock);
	atomic_dec(&ctx->nr_pending_writes);
	spin_unlock(&ctx->rev_flush_lock);

	/* refcount will be synced when all the
	 * translation entries are flushed
	 * to the disk. We free the page before
	 * since the entries in that page are already
	 * copied.
	 *
	 * TODO: Instead of freeing the pages, we can
	 * mark page->status uptodate. We can free all
	 * these uptodate pages later if necessary on
	 * shrinker call.
	 */
	printk(KERN_ERR "\n %s revmap_bio_ctx->page: %p", __func__,  page_address(page));
	__free_pages(page, 0);
	//printk(KERN_ERR "\n ctx->nr_pending_writes-- done. Val: %d \n. Waking up waiters", atomic_read(&ctx->nr_pending_writes));
	/* Wakeup waiters waiting on the block barrier
	 * */
	wake_up(&ctx->rev_blk_flushq);
	/* By now add_block_based_translations would have
	 * incremented the reference atleast once!
	 */
	printk(KERN_ERR "\n waiting for translation maps to be flushed to disk!");
	/* no need to take tm_ref_lock as this function will be called
	 * only once; when the value turns 0
	 */
	kref_put(&revmap_bio_ctx->kref, revmap_block_release);
	return 0;
}

void revmap_blk_flushed(struct bio *bio)
{
	struct ctx * ctx;
	sector_t pba;
	struct revmap_meta_inmem *revmap_bio_ctx;

	revmap_bio_ctx = bio->bi_private;

	switch(bio->bi_status) {
		case BLK_STS_AGAIN:
			revmap_bio_ctx->retrial++;
			if (revmap_bio_ctx->retrial < 3) {
				submit_bio(bio);
				return;
			}
			// else
			panic("Cannot flush revmap entries! ");
		case BLK_STS_OK:
			complete(&revmap_bio_ctx->io_done);
			bio_put(bio);
			break;
		default:
			/* TODO: do something better?
			 * remove all the entries from the in memory 
			 * RB tree and the reverse map in memory.
			remove_partial_entries();
			 */
			printk(KERN_ERR "\n revmap_entries_flushed ERROR!! \n");
			panic("revmap block write error, needs better handling!");
			break;
	}
}


/*
 * a) create a bio from the page, associate a endio with it.
 * b) flush the page
 * c) make sure the entries are on the disk, before we overwrite.
 *
 * TODO: better error handling!
 *
 * pba: pba from the lba-pba map! We send this here, because we want
 *  to identify if this is the last pba of the zone.
 */
int flush_revmap_block_disk(struct ctx * ctx, struct page *page)
{
	struct bio * bio;
	struct revmap_meta_inmem *revmap_bio_ctx;

	if (!page)
		return -ENOMEM;

	revmap_bio_ctx = kmem_cache_alloc(ctx->revmap_bioctx_cache, GFP_KERNEL);
	if (!revmap_bio_ctx) {
		return -ENOMEM;
	}
	kref_init(&revmap_bio_ctx->kref);
	init_completion(&revmap_bio_ctx->io_done);

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		kmem_cache_free(ctx->revmap_bioctx_cache, revmap_bio_ctx);
		return -ENOMEM;
	}
	
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		kmem_cache_free(ctx->revmap_bioctx_cache, revmap_bio_ctx);
		bio_put(bio);
		return -EFAULT;
	}
	
	printk(KERN_ERR "\n revmap_bio_ctx->page: %p", page_address(page));

	revmap_bio_ctx->ctx = ctx;
	revmap_bio_ctx->page = page;
	revmap_bio_ctx->revmap_pba = ctx->revmap_pba;
	revmap_bio_ctx->retrial = 0;
	bio->bi_iter.bi_sector = ctx->revmap_pba;
	printk(KERN_ERR "Checking if revmap blk at pba:%llu is available for writing! ctx->revmap_pba: %llu", bio->bi_iter.bi_sector, ctx->revmap_pba);
	wait_on_revmap_block_availability(ctx, bio->bi_iter.bi_sector);
	spin_lock(&ctx->rev_flush_lock);
	/*-------------------------------------------*/
	mark_revmap_bit(ctx, bio->bi_iter.bi_sector);
	/*-------------------------------------------*/
	spin_unlock(&ctx->rev_flush_lock);
	//printk(KERN_ERR "\n Marked pba: %llu in use! \n", ctx->revmap_pba);
	
	bio->bi_private = revmap_bio_ctx;
	bio->bi_end_io = revmap_blk_flushed;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	spin_lock(&ctx->rev_flush_lock);
	/*-------------------------------------------*/
	atomic_inc(&ctx->nr_pending_writes);
	/*------------------------------------------*/
	spin_unlock(&ctx->rev_flush_lock);
	printk(KERN_ERR "\n flushing revmap at pba: %llu", bio->bi_iter.bi_sector);
	kthread_run(revmap_entries_flushed, revmap_bio_ctx, "revmap_entries_flushed");
	generic_make_request(bio);
	return 0;
}


/*
 * The unflushed page is stored in ctx->revmap_page.
 */
void flush_revmap_entries(struct ctx *ctx)
{
	struct page *page = ctx->revmap_page;

	if (!page)
		return;

	flush_revmap_block_disk(ctx, page);
	printk(KERN_ERR "%s waiting on block barrier ", __func__);
	/* We wait for translation entries to be added! */
	wait_on_block_barrier(ctx);
	printk(KERN_ERR "%s revmap entries are on disk, wait is over!", __func__);
}



/* We store only the LBA. We can calculate the PBA from the wf
 */
static void add_revmap_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned int nrsectors)
{
	struct stl_revmap_entry_sector * ptr = NULL;
	int i = 0, j = 0;
	struct page * page = NULL;

	//spin_lock(&ctx->rev_entries_lock);
	spin_lock(&ctx->rev_flush_lock);
	/*--------------------------------------------------*/
	i = atomic_read(&ctx->revmap_sector_count);
	j = atomic_read(&ctx->revmap_blk_count);
	atomic_inc(&ctx->revmap_blk_count);
	atomic_inc(&ctx->revmap_sector_count);
	if (NR_EXT_ENTRIES_PER_SEC == (i+1)) {
		atomic_set(&ctx->revmap_sector_count, 0);
		if (NR_EXT_ENTRIES_PER_BLK == (j+1)) {
			atomic_set(&ctx->revmap_blk_count, 0);
	/*--------------------------------------------------*/
			spin_unlock(&ctx->rev_flush_lock);
			//printk(KERN_ERR "\n Waiting on block barrier! \n");
			page = ctx->revmap_page;
			BUG_ON(page == NULL);
			flush_revmap_block_disk(ctx, page);
			wait_on_block_barrier(ctx);
			/* Adjust the revmap_pba for the next block 
			* Addressing is based on 512bytes sector.
			*/
			ctx->revmap_pba += NR_SECTORS_IN_BLK; 

			/* if we have the pba of the translation table,
			* then reset the revmap pba to the original value
			*/
			if (ctx->revmap_pba == ctx->sb->tm_pba) {
				ctx->revmap_pba = ctx->sb->revmap_pba;
			}
		}
		else {
			spin_unlock(&ctx->rev_flush_lock);
		}

	} else {
		/*--------------------------------------------------*/
		spin_unlock(&ctx->rev_flush_lock);
	}
	//printk(KERN_ERR "\n spin lock aquired! i:%d j:%d\n", i, j);
	/* blk count before incrementing */
	if (0 == j) {
		/* we need to make sure the previous block is on the
		 * disk. We cannot overwrite without that.
		 */
		page = alloc_page(__GFP_ZERO|GFP_KERNEL);
		if (!page) {
			/* TODO: Do something more. For now panicking!
			 */
			panic("Low memory, could not allocate page!");
		}
		ctx->revmap_page = page;
		printk(KERN_ERR "\n revmap_page: %p", page_address(page));
	}
	page = ctx->revmap_page;
	ptr = (struct stl_revmap_entry_sector *)page_address(page);
	if (NR_EXT_ENTRIES_PER_SEC == (i+1)) {
		//ptr->crc = calculate_crc(ctx, page);
		ptr->crc = 0;
	}
	/* TODO merge entries by increasing the length if there lies a
	 * matching entry in the revmap page
	 */
	ptr->extents[i].lba = lba;
    	ptr->extents[i].pba = pba;
	ptr->extents[i].len = nrsectors;
	//printk(KERN_ERR "\n revmap entry added! i+1: %d, j+1:%d \n", i+1, j+1);
	printk(KERN_ERR "\n revmap entry added! ptr: %p i: %d, j:%d lba: %llu pba: %llu len: %d \n", ptr, i, j, lba, pba, nrsectors);
	//spin_unlock(&ctx->rev_entries_lock);
	return;
}

/* 
 * TODO: If status is not OK, remove the translation
 * entries for this bio
 * We need to go back to the older values.
 * But right now, we have not saved them.
 */
void write_done(struct kref *kref)
{
	struct bio *bio;
	struct nstl_bioctx * nstl_bioctx;
	struct ctx *ctx;

	nstl_bioctx = container_of(kref, struct nstl_bioctx, ref);
	bio = nstl_bioctx->orig;
	bio_endio(bio);

	printk(KERN_ERR ">>>>>>>>>>>>> I/O done, freeing....! %s \n", __func__);
	ctx = nstl_bioctx->ctx;
	kmem_cache_free(ctx->bioctx_cache, nstl_bioctx);
	//kref_put(&ctx->ongoing_iocount, stl_is_ioidle);
}


void sub_write_done(void *data, async_cookie_t cookie)
{

	struct nstl_sub_bioctx *subbioctx = NULL;
	struct ctx *ctx;
	int ret;
	sector_t lba, pba;
	unsigned int len;

	subbioctx = (struct nstl_sub_bioctx *) data;
	//printk(KERN_ERR "\n * (%s) thread ... waiting.... LBA: %llu &write_done: %llu!! \n", __func__, subbioctx->extent.lba, &subbioctx->write_done);
	wait_for_completion(&subbioctx->write_done);

	ctx = subbioctx->bioctx->ctx;
	kref_put(&subbioctx->bioctx->ref, write_done);

	lba = subbioctx->extent.lba;
	pba = subbioctx->extent.pba;
	len = subbioctx->extent.len;

	write_lock(&ctx->metadata_update_lock);
	/*------------------------------- */
	add_revmap_entries(ctx, lba, pba, len);
	ret = stl_update_range(ctx, &ctx->extent_tbl_root, lba, pba, len);
	ret = stl_update_range(ctx, &ctx->rev_tbl_root, pba, lba, len);
	/*-------------------------------*/
	write_unlock(&ctx->metadata_update_lock);
	printk(KERN_ERR "\n (%s): DONE! lba: %llu, pba: %llu, len: %lu \n", __func__, lba, pba, len);
	kmem_cache_free(ctx->subbio_ctx_cache, subbioctx);
}



/* can you create the translation entry here?
 * What happens if you put a translation entry
 * for some data that did not make it to
 * the disk? Partial writes to such block
 * entries can end up rewriting data that
 * is partially uptodate and correct, and partially
 * stale and wrong. Thus there should be no wrong
 * entry in the translation map.
 */
static void nstl_clone_endio(struct bio * clone)
{
	struct nstl_sub_bioctx *subbioctx = NULL;
	struct bio *bio = NULL;

	subbioctx = (struct nstl_sub_bioctx *) clone->bi_private;
	bio = subbioctx->bioctx->orig;

	if (bio->bi_status == BLK_STS_OK) {
		bio->bi_status = clone->bi_status;
	}
	printk(KERN_ERR "\n (%s) .. completing I/O......lba: %llu, pba: %llu, len: %lu &write_done: %llu", __func__, subbioctx->extent.lba, subbioctx->extent.pba, subbioctx->extent.len, &subbioctx->write_done);
	complete(&subbioctx->write_done);
	bio_put(clone);
	return;
}


/*
 * From the specifications:
 * When addressing these drives in LBA mode, all blocks (sectors) are
 * consecutively numbered from 0 to n–1, where n is the number of guaranteed
 * sectors as defined above. Thus sector address to be used. This
	 * means that addr++ increments the sector number.
 */

/* split a write into one or more journalled packets
 *
 * We add translation map entries only when we know the write has
 * successfully made it to the disk. If not, an interim read will
 * return bytes that will later not go to the disk. So later reads
 * will say "no data found". This is not correct. So we allow the
 * reads only when we know the data has made it to the disk.
 * We will come to nstl code only when vfs did not find the data it was trying
 * to read anyway.
 *
*/
static int nstl_write_io(struct ctx *ctx, struct bio *bio)
{
	struct bio *split = NULL, *pad = NULL;
	struct bio * clone;
	struct nstl_bioctx * bioctx;
	struct nstl_sub_bioctx *subbio_ctx;
	volatile int nbios = 0;
	unsigned nr_sectors = bio_sectors(bio);
	sector_t s8, lba = bio->bi_iter.bi_sector;
	struct blk_plug plug;
	sector_t wf;
	struct stl_ckpt *ckpt;
	async_cookie_t cookie;

	if (is_disk_full(ctx)) {
		bio->bi_status = BLK_STS_NOSPC;
		bio_endio(bio);
		return -1;
	}

	/* ckpt must be updated. The state of the filesystem is
	 * unclean until checkpoint happens!
	 */
	ckpt = (struct stl_ckpt *)page_address(ctx->ckpt_page);
	ckpt->clean = 0;
	nr_sectors = bio_sectors(bio);
	printk(KERN_INFO "\n ******* Inside map_write_io, requesting lba: %llu sectors: %d", bio->bi_iter.bi_sector, nr_sectors);
	if (unlikely(nr_sectors <= 0)) {
		printk(KERN_ERR "\n Less than 0 sectors (%d) requested!, nbios: %u", nr_sectors, nbios);
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		return -1;
	}
	
	clone = bio_clone_fast(bio, GFP_KERNEL, NULL);
	if (!clone) {
		kmem_cache_free(ctx->bioctx_cache, bioctx);
		printk(KERN_ERR "\n Insufficient memory!");
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		/* We dont call bio_put() because the bio should not
		 * get freed by memory management before bio_endio()
		 */
		return -ENOMEM;
	}

	bioctx = kmem_cache_alloc(ctx->bioctx_cache, GFP_KERNEL);
	if (!bioctx) {
		printk(KERN_ERR "\n Insufficient memory!");
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		/* We dont call bio_put() because the bio should not
		 * get freed by memory management before bio_endio()
		 */
		return -ENOMEM;
	}

	bioctx->orig = bio;
	bioctx->ctx = ctx;
	/* TODO: Initialize refcount in bioctx and increment it every
	 * time bio is split or padded */
	kref_init(&bioctx->ref);

	//printk(KERN_ERR "\n write frontier: %llu free_sectors_in_wf: %llu", ctx->app_write_frontier, ctx->free_sectors_in_wf);

	blk_start_plug(&plug);
	
	do {
		nr_sectors = bio_sectors(clone);
		subbio_ctx = NULL;
		subbio_ctx = kmem_cache_alloc(ctx->subbio_ctx_cache, GFP_KERNEL);
		if (!subbio_ctx) {
			printk(KERN_ERR "\n insufficient memory!");
			goto fail;
		}
		//printk(KERN_ERR "\n subbio_ctx: %llu", subbio_ctx);
		spin_lock(&ctx->lock);
		/*-------------------------------*/
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		wf = ctx->app_write_frontier;
		
		/* room_in_zone should be same as
		 * ctx->nr_free_sectors_in_wf
		 */
		if (s8 > ctx->free_sectors_in_wf){
			//printk(KERN_ERR "SPLITTING!!!!!!!! s8: %d ctx->free_sectors_in_wf: %d", s8, ctx->free_sectors_in_wf);
			s8 = round_down(ctx->free_sectors_in_wf, NR_SECTORS_IN_BLK);
			if (s8 <= 0) {
				panic("Should always have atleast a block left ");
			}
			if (!(split = bio_split(clone, s8, GFP_NOIO, ctx->bs))){
				printk(KERN_ERR "\n failed at bio_split! ");
				goto fail;
			}
			/* Add a translation map entry for shortened
			 * length
			 */
			subbio_ctx->extent.len = s8;
		} 
		else {
			split = clone;
			/* s8 might be bigger than nr_sectors. We want
			 * to maintain the exact length in the
			 * translation map, not padded entry
			 */
			subbio_ctx->extent.len = nr_sectors;
		}
		move_write_frontier(ctx, s8);
		/*-------------------------------*/
		spin_unlock(&ctx->lock);
    		

		/* Next we fetch the LBA that our DM got */
		lba = bio->bi_iter.bi_sector;
		atomic_set(&ctx->ioidle, 0);
		kref_get(&bioctx->ref);
		kref_get(&ctx->ongoing_iocount);
		init_completion(&subbio_ctx->write_done);
		printk(KERN_ERR "\n (%s) lba: %llu, &write_done: %llu", __func__, lba, &subbio_ctx->write_done);
		subbio_ctx->extent.lba = lba;
		subbio_ctx->extent.pba = wf;
		subbio_ctx->bioctx = bioctx; /* This is common to all the subdivided bios */

		split->bi_iter.bi_sector = wf; /* we use the save write frontier */
		split->bi_private = subbio_ctx;
		split->bi_end_io = nstl_clone_endio;
		bio_set_dev(split, ctx->dev->bdev);
		generic_make_request(split);
		cookie = async_schedule(sub_write_done, subbio_ctx);
	} while (split != clone);

	/* We might need padding when the original request is not block
	 * aligned.
	 * We want to zero fill the padded portion of the last page.
	 * We know that we always have a page corresponding to a bio.
	 * A page is always 4096 bytes, but the request may be smaller.
	 */
	if (nr_sectors < s8) {
		/* We need to zero out the remaining bytes
		 * from the last page in the bio
		 */
		dump_stack();
		pad = bio_clone_fast(clone, GFP_KERNEL, NULL);
		pad->bi_iter.bi_size = s8 << 9;
		/* We use the saved write frontier */
		pad->bi_iter.bi_sector = wf;
		/* bio advance will advance the bi_sector and bi_size
		 */
		bio_advance(pad, nr_sectors << 9);
		zero_fill_bio_iter(pad, pad->bi_iter);
		/* we dont need to take any action when pad completes
		 * and thus we dont need any endio for pad. Chain it 
		 * with parent, so that we are notified its completion
		 * when parent completes.
		 */
		bio_chain(pad, clone);
		generic_make_request(pad);
	}
	blk_finish_plug(&plug);
	kref_put(&bioctx->ref, write_done);
	atomic_inc(&ctx->nr_writes);
	return 0;

fail:
	printk(KERN_INFO "FAIL!!!!\n");
	bio->bi_status = BLK_STS_RESOURCE;
	bio_endio(bio);
	atomic_inc(&ctx->nr_failed_writes);
	return -1;
}


/*
   argv[0] = devname
   argv[1] = dm-name
   argv[2] = zone size (LBAs)
   argv[3] = max pba
   */
#define BS_NR_POOL_PAGES 128

/* TODO: IMPLEMENT */
void put_free_zone(struct ctx *ctx, u64 pba)
{
	/*
	unsigned long flags;
	unsigned long zonenr = get_zone_nr(ctx, pba);
	*/
}

/* This pba is got from the superblock; it is the address of the
 * sector and not a 4096 block. So, we can directly read from this
 * address.
 */
struct stl_ckpt * read_checkpoint(struct ctx *ctx, unsigned long pba)
{
	struct block_device *bdev = ctx->dev->bdev;
	struct buffer_head *bh;
	struct stl_ckpt *ckpt = NULL;
	sector_t blknr = pba / NR_SECTORS_IN_BLK;

	bh = __bread(bdev, blknr, BLK_SZ);
	if (!bh)
		return NULL;

       	ckpt = (struct stl_ckpt *)bh->b_data;
	printk(KERN_INFO "\n ** blknr: %llu, ckpt->magic: %u, ckpt->cur_frontier_pba: %lld", blknr, ckpt->magic, ckpt->cur_frontier_pba);
	if (ckpt->magic == 0) {
		put_page(bh->b_page);
		return NULL;
	}
	ctx->ckpt_page = bh->b_page;
	/* Do not set ctx->nr_freezones; its calculated while reading segment info table
	 * and then verified against what is recorded in ckpt
	 */
	return ckpt;
}


/*
 * a) Checkpoint is called on a timer. Say every 10 seconds. This
 * timer cannot be modififed.
 * b) When GC completes, it calls checkpoint as well.
 *
 */
static void do_checkpoint(struct ctx *ctx)
{
	struct blk_plug plug;

	printk(KERN_ERR "Inside %s" , __func__);
	
	spin_lock(&ctx->ckpt_lock);
	/*--------------------------------------------*/
	ctx->flag_ckpt = 1;
	/* We expect the ckpt_ref to go to atleast 4 and then back to 0
	 * when all the blocks are flushed!
	 */
	atomic_set(&ctx->ckpt_ref, 2);
	/*--------------------------------------------*/
	spin_unlock(&ctx->ckpt_lock);

	down_interruptible(&ctx->flush_lock);
	/*--------------------------------------------*/
	flush_sit(ctx);
	/*--------------------------------------------*/
	up(&ctx->flush_lock);
	printk(KERN_ERR "\n sit pages flushed!");

	blk_start_plug(&plug);
	flush_revmap_bitmap(ctx);
	printk(KERN_ERR "\n revmap_bitmap flushed!\n");
	update_checkpoint(ctx);
	printk(KERN_ERR "\n checkpoint updated! \n");
	flush_checkpoint(ctx);
	blk_finish_plug(&plug);

	printk(KERN_ERR "\n checkpoint flushed! \n");
	/* We need to wait for all of this to be over before 
	 * we proceed
	 */
	printk(KERN_ERR "\n ckpt_ref: %d \n", atomic_read(&ctx->ckpt_ref));
	atomic_dec(&ctx->ckpt_ref);
	wait_for_ckpt_completion(ctx);
	spin_lock(&ctx->ckpt_lock);
	ctx->flag_ckpt = 0;
	spin_unlock(&ctx->ckpt_lock);
}

/*
 * TODO: IMPLEMENT
 * 
 */
static void reset_ckpt(struct stl_ckpt * ckpt)
{
	ckpt->version += 1;
	/* We do not need to reset the LBA addresses
	 * as we only read upto prev_count + cur_count
	 * addresses and these should be overwritten
	 * appropriately
	 */
}

/* How do you know that recovery is necessary?
 * Go through the existing translation table
 * and check if the records mentioned in the 
 * ckpt match with that in the extent map
 * So read the extent map before this step
 *
 * recovery is necessary is ckpt->clean is 0
 *
 * if recovery is necessary  do the following:
 *
 * ctx->elapsed_time = highest mtime of all segentries.
 *
 */
static int do_recovery(struct ctx *ctx)
{
	/* Once necessary steps are taken for recovery (if needed),
	 * then we can reset the checkpoint and prepare it for the 
	 * next round
	reset_ckpt(ckpt);
	 */
	return 0;
}



/* we write the checkpoints alternately.
 * Only one of them is more recent than
 * the other
 * In case we fail while writing one
 * then we find the older one safely
 * on the disk
 */
struct stl_ckpt * get_cur_checkpoint(struct ctx *ctx)
{
	struct stl_sb * sb = ctx->sb;
	struct stl_ckpt *ckpt1, *ckpt2, *ckpt;
	struct page *page1;

	printk(KERN_INFO "\n !Reading checkpoint 1 from pba: %u", sb->ckpt1_pba);
	ckpt1 = read_checkpoint(ctx, sb->ckpt1_pba);
	if (!ckpt1)
		return NULL;
	page1 = ctx->ckpt_page;
	/* ctx->ckpt_page will be overwritten by the next
	 * call to read_ckpt
	 */
	printk(KERN_INFO "\n !!Reading checkpoint 2 from pba: %u", sb->ckpt2_pba);
	ckpt2 = read_checkpoint(ctx, sb->ckpt2_pba);
	if (!ckpt2) {
		put_page(page1);
		return NULL;
	}
	printk(KERN_INFO "\n %s ckpt versions: %d %d", __func__, ckpt1->version, ckpt2->version);
	if (ckpt1->version >= ckpt2->version) {
		ckpt = ckpt1;
		put_page(ctx->ckpt_page);
		ctx->ckpt_page = page1;
	}
	else {
		ckpt = ckpt2;
		//page2 is rightly set by read_ckpt();
		put_page(page1);
	}
	ctx->user_block_count = ckpt->user_block_count;
	ctx->nr_invalid_zones = ckpt->nr_invalid_zones;
	ctx->app_write_frontier = ckpt->cur_frontier_pba;
	ctx->elapsed_time = ckpt->elapsed_time;
	/* TODO: Do recovery if necessary */
	//do_recovery(ckpt);
	return ckpt;
}

/* 
 * 1 indicates the zone is free 
 */

static void mark_zone_occupied(struct ctx *ctx , int zonenr)
{
	char *bitmap = ctx->freezone_bitmap;
	int bytenr = zonenr / BITS_IN_BYTE;
	int bitnr = zonenr % BITS_IN_BYTE;

	if (bytenr > ctx->bitmap_bytes) {
		panic("\n Trying to set an invalid bit in the free zone bitmap. bytenr > bitmap_bytes");
	}

	if ((bitmap[bytenr] & (1 << bitnr)) == 0) {
		/* This function can be called multiple times for the
		 * same zone. The bit is already unset and the zone 
		 * is marked occupied already.
		 */
		return;
	}
	/* This bit is set and the zone is free. We want to unset it
	 */
	bitmap[bytenr] = bitmap[bytenr] ^ (1 << bitnr);
	ctx->nr_freezones = ctx->nr_freezones + 1;
}

int read_extents_from_block(struct ctx * ctx, struct tm_entry *entry, unsigned int nr_extents)
{
	int i = 0;

	while (i <= nr_extents) {
		i++;
		/* If there are no more recorded entries on disk, then
		 * dont add an entries further
		 */
		if ((entry->lba == 0) && (entry->pba == 0)) {
			continue;
			
		}
		printk(KERN_ERR "\n entry->lba: %llu", entry->lba);
		printk(KERN_ERR "\n entry->pba: %llu \n", entry->pba);
		/* TODO: right now everything should be zeroed out */
		//panic("Why are there any already mapped extents?");
		write_lock(&ctx->metadata_update_lock);
		stl_update_range(ctx, &ctx->extent_tbl_root, entry->lba, entry->pba, NR_SECTORS_IN_BLK);
		stl_update_range(ctx, &ctx->rev_tbl_root, entry->pba, entry->lba, NR_SECTORS_IN_BLK);
		write_unlock(&ctx->metadata_update_lock);
		entry = entry + 1;
	}
	return 0;
}


#define NR_SECTORS_IN_BLK 8

/* 
 * Create a RB tree from the map on
 * disk
 *
 * Each extent covers one 4096 sized block
 * NOT a sector!!
 * TODO
 */
int read_translation_map(struct ctx *ctx)
{
	struct buffer_head *bh = NULL;
	int nr_extents_in_blk = BLK_SZ / sizeof(struct tm_entry);
	unsigned long long pba = ctx->sb->tm_pba;
	unsigned long nrblks = ctx->sb->blk_count_tm;
	struct block_device *bdev = ctx->dev->bdev;
	int i = 0;

	pba = pba / NR_SECTORS_IN_BLK;
	printk(KERN_ERR "\n Reading TM entries from: %llu, nrblks: %ld", pba, nrblks);
	
	ctx->n_extents = 0;
	while(i < nrblks) {
		bh = __bread(bdev, pba, BLK_SZ);
		if (!bh)
			return -1;
		/* We read the extents in the entire block. the
		 * redundant extents should be unpopulated and so
		 * we should find 0 and break out */
		//printk(KERN_ERR "\n pba: %llu", pba);
		read_extents_from_block(ctx, (struct tm_entry *)bh->b_data, nr_extents_in_blk);
		i = i + 1;
		put_bh(bh);
		pba = pba + 1;
	}
	printk(KERN_ERR "\n TM entries read!");
	return 0;
}

int read_revmap_bitmap(struct ctx *ctx)
{
	struct buffer_head *bh = NULL;
	unsigned long long pba = ctx->sb->revmap_bm_pba;
	unsigned long nrblks = ctx->sb->blk_count_revmap_bm;
	struct block_device *bdev = ctx->dev->bdev;
	sector_t blknr = pba /NR_SECTORS_IN_BLK;

	if (nrblks != 1) {
		panic("\n Wrong revmap bitmap calculations!");
	}
	
	bh = __bread(bdev, blknr, BLK_SZ);
	if (!bh) {
		/* free the successful bh till now */
		return -1;
	}
	ctx->revmap_bm = bh->b_page;
	return 0;

}


void process_revmap_entries_on_boot(struct ctx *ctx, struct page *page, struct revmap_meta_inmem *revmap_bio_ctx)
{
	struct stl_revmap_extent *extent;
	struct stl_revmap_entry_sector *entry_sector;
	int i = 0, j;

	//printk(KERN_ERR "\n Inside process revmap_entries_on_boot!" );

	add_block_based_translation(ctx,  page, revmap_bio_ctx);
	//printk(KERN_ERR "\n Added block based translations!, will add memory based extent maps now.... \n");
	entry_sector = (struct stl_revmap_entry_sector *) page_address(page);
	while (i < NR_SECTORS_IN_BLK) {
		extent = entry_sector->extents;
		for (j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			if (extent[j].pba == 0)
				continue;
			write_lock(&ctx->metadata_update_lock);
			stl_update_range(ctx, &ctx->extent_tbl_root, extent[j].lba, extent[j].pba, extent[j].len);
			stl_update_range(ctx, &ctx->rev_tbl_root, extent[j].pba, extent[j].lba, extent[j].len);
			write_unlock(&ctx->metadata_update_lock);
		}
		entry_sector = entry_sector + 1;
		i++;
	}
}

/*
 * Read a blk only if the bitmap says its not available.
 */
int read_revmap(struct ctx *ctx)
{
	struct buffer_head *bh = NULL;
	int i = 0, byte = 0;
	struct page *page;
	char *ptr;
	unsigned long pba;
	struct block_device *bdev = NULL;
	char flush_needed = 0;
	struct revmap_meta_inmem *revmap_bio_ctx;

	revmap_bio_ctx = kmem_cache_alloc(ctx->revmap_bioctx_cache, GFP_KERNEL);
	if (!revmap_bio_ctx) {
		return -ENOMEM;
	}
	kref_init(&revmap_bio_ctx->kref);
	kref_get(&revmap_bio_ctx->kref);

	bdev = ctx->dev->bdev;
	pba = ctx->sb->revmap_pba/NR_SECTORS_IN_BLK;
	printk(KERN_ERR "\n PBA for first revmap blk: %lu", pba);
	printk(KERN_ERR "\n nr of revmap blks: %u", ctx->sb->blk_count_revmap);

	/* We read the revmap bitmap first. If a bit is set,
	 * then the corresponding revmap blk is read
	 */
	page = ctx->revmap_bm;
	if (!page)
		return -1;
	ptr = (char *) page_address(page);
	printk(KERN_ERR "\n page_address(ctx->revmap_bm): %p", ptr);
	for (i = 0; i < BLK_SZ; i++, pba++) {
		byte = *ptr;
		while(byte) {
			if (byte & 1) {
				printk(KERN_ERR "\n WHY IS THIS BYTE SET??");
				flush_needed = 1;
				printk(KERN_ERR "\n read revmap blk: %lu", pba);
				bh = __bread(bdev, pba, BLK_SZ);
				if (!bh) {
					/* free the successful bh till now */
					return -1;
				}
				process_revmap_entries_on_boot(ctx, bh->b_page, revmap_bio_ctx);
			}
			byte = byte >> 1;
		}
		ptr = ptr + 1;
	}
	printk(KERN_ERR "\n flush_needed: %d", flush_needed);
	if (flush_needed) {
		printk(KERN_ERR "\n Why do we need to flush!!");
		down_interruptible(&ctx->flush_lock);
		flush_translation_blocks(ctx);
		up(&ctx->flush_lock);
	}
	/* no need to take tm_ref_lock as this function will be called
	 * only once; when the value turns 0
	 */
	kref_put(&revmap_bio_ctx->kref, revmap_block_release);
	return 0;
}


sector_t get_zone_pba(struct stl_sb * sb, unsigned int segnr)
{
	return (segnr * (1 << (sb->log_zone_size - sb->log_sector_size)));
}


/* 
 * Returns the pba of the last sector in the zone
 */
sector_t get_zone_end(struct stl_sb *sb, sector_t pba_start)
{
	return (pba_start + (1 << (sb->log_zone_size - sb->log_sector_size))) - 1;
}


int allocate_freebitmap(struct ctx *ctx)
{
	char *free_bitmap;
	if (!ctx)
		return -1;
	printk(KERN_INFO "\n ctx->bitmap_bytes: %d ", ctx->bitmap_bytes);
	free_bitmap = (char *)kzalloc(11, GFP_KERNEL);
	if (!free_bitmap)
		return -1;

	ctx->freezone_bitmap = free_bitmap;
	return 0;
}

int allocate_gc_zone_bitmap(struct ctx *ctx)
{
	char *gc_zone_bitmap;

	if (!ctx)
		return -1;

	gc_zone_bitmap = (char *)kzalloc(ctx->bitmap_bytes, GFP_KERNEL);
	if (!gc_zone_bitmap)
		return -ENOMEM;

	ctx->gc_zone_bitmap = gc_zone_bitmap;
	return 0;
}

/*
 * Seginfo tree Management
 */

int _stl_verbose;
static void sit_rb_insert(struct ctx *ctx, struct rb_root *root, struct sit_extent *new)
{
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct sit_extent *e = NULL;

	RB_CLEAR_NODE(&new->rb);
	write_lock(&ctx->sit_rb_lock);

	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		e = container_of(parent, struct sit_extent, rb);
		if (new->cb_cost < e->cb_cost) {
			link = &(*link)->rb_left;
		} else {
			link = &(*link)->rb_right;
		}
	}
	/* Put the new node there */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
	ctx->n_sit_extents++;
	write_unlock(&ctx->sit_rb_lock);
}


static void sit_rb_remove(struct ctx *ctx, struct rb_root *root, struct sit_extent *e)
{
	/* rb_erase resorts and rebalances the tree */
	rb_erase(&e->rb, root);
	ctx->n_sit_extents--;
}


/* 
 * TODO: Unused function right now 
 */
static struct extent *sit_rb_next(struct ctx *ctx, struct sit_extent *se)
{
	struct extent *e;
	read_lock(&ctx->sit_rb_lock);
	struct rb_node *node = rb_next(&se->rb);
	e =  container_of(node, struct extent, rb);
	read_unlock(&ctx->sit_rb_lock);
	return e;
}

static int get_cb_cost(struct ctx *ctx , u32 nrblks, u64 mtime)
{
	unsigned char u, age;
	struct stl_sb *sb = ctx->sb;

	u = (nrblks * 100) >> (sb->log_zone_size - sb->log_block_size);
	age = 100 - div_u64(100 * (mtime - ctx->min_mtime),
				ctx->max_mtime - ctx->min_mtime);

	return UINT_MAX - ((100 * (100 - u) * age)/ (100 + u));
}


static int get_cost(struct ctx *ctx, u32 nrblks, u64 age, char gc_mode)
{
	if (gc_mode == GC_GREEDY) {
		return nrblks;
	}
	return get_cb_cost(ctx, nrblks, age);

}

/*
 *
 * TODO: Current default is Cost Benefit.
 * But in the foreground mode, we want to do GC_GREEDY.
 * Add a parameter in this function and write code for this
 *
 * When nrblks is 0, we update the cb_cost based on mtime only.
 */
int update_inmem_sit(struct ctx *ctx, unsigned int zonenr, u32 nrblks, u64 mtime)
{
	struct rb_root *root = &ctx->sit_rb_root;
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct sit_extent *e = NULL, *new = NULL;
	int cb_cost = 0;

	BUG_ON(nrblks == 0);
	write_lock(&ctx->sit_rb_lock);
	printk(KERN_ERR "\s %s zonenr: %d nrblks: %u, mtime: %llu, GC_GREEDY \n", __func__, zonenr, nrblks, mtime);
	cb_cost = get_cost(ctx, nrblks, mtime, GC_GREEDY);
	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		e = container_of(parent, struct sit_extent, rb);
		if (cb_cost == e->cb_cost) {
			if (zonenr == e->zonenr) {
				sit_rb_remove(ctx, root, e);
				e->zonenr = zonenr;
				e->cb_cost = cb_cost;
				e->nrblks = nrblks;
				sit_rb_insert(ctx, root, e);
				return 0;
			}
			link = &(*link)->rb_left;
			continue;

		}
		if (new->cb_cost < e->cb_cost) {
			link = &(*link)->rb_left;
		} else {
			link = &(*link)->rb_right;
		}
	}
	/* We are essentially adding a new node here */
	new = kmem_cache_alloc(ctx->sit_extent_cache, GFP_KERNEL);
	if (!new)
		return -ENOMEM;

	RB_CLEAR_NODE(&new->rb);
	new->zonenr = zonenr;
	new->cb_cost = cb_cost;
	new->nrblks = nrblks;
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
	ctx->n_sit_extents++;
	write_unlock(&ctx->sit_rb_lock);
	return 0;
}


/* TODO: create a freezone cache
 * create a gc zone cache
 * currently calling kzalloc
 */
int read_seg_entries_from_block(struct ctx *ctx, struct stl_seg_entry *entry, unsigned int nr_seg_entries, unsigned int *zonenr)
{
	int i = 0;
	struct stl_sb *sb;
	unsigned int nr_blks_in_zone;
       
	if (!ctx)
		return -1;

	sb = ctx->sb;

	nr_blks_in_zone = (1 << (sb->log_zone_size - sb->log_block_size));
	printk(KERN_ERR "\n Number of seg entries: %u", nr_seg_entries);
	printk(KERN_ERR "\n cur_frontier_pba: %llu, frontier zone: %u", ctx->ckpt->cur_frontier_pba, get_zone_nr(ctx, ctx->ckpt->cur_frontier_pba));

	while (i < nr_seg_entries) {
		if ((*zonenr == get_zone_nr(ctx, ctx->ckpt->cur_frontier_pba)) ||
		    (*zonenr == get_zone_nr(ctx, ctx->ckpt->cur_gc_frontier_pba))) {
			printk(KERN_ERR "\n zonenr: %d is our cur_frontier! not marking it free!", *zonenr);
			entry = entry + 1;
			*zonenr= *zonenr + 1;
			i++;
			continue;
		}
		if (entry->vblocks == 0) {

			//printk(KERN_ERR "\n *segnr: %u", *zonenr);
			spin_lock(&ctx->lock);
			mark_zone_free(ctx , *zonenr);
			spin_unlock(&ctx->lock);
		}
		else if (entry->vblocks < nr_blks_in_zone) {
			printk(KERN_ERR "\n *segnr: %u", *zonenr);
			mark_zone_gc_candidate(ctx, *zonenr);
			if (ctx->min_mtime > entry->mtime)
				ctx->min_mtime = entry->mtime;
			if (ctx->max_mtime < entry->mtime)
				ctx->max_mtime = entry->mtime;
			if (!update_inmem_sit(ctx, *zonenr, entry->vblocks, entry->mtime))
				panic("Memory error, write a memory shrinker!");
		}
		entry = entry + 1;
		*zonenr= *zonenr + 1;
		i++;
	}
	return 0;
}

/*
 * Create a heap/RB tree from the seginfo
 * on disk. Use this for finding the 
 * victim. We don't need to keep the
 * entire heap in memory. We store only
 * the top x candidates in memory
 *
 * TODO: replace sc with ctx in the entire file
 */
int read_seg_info_table(struct ctx *ctx)
{
	unsigned long pba;
	unsigned int nrblks = 0;
	struct block_device *bdev;
	struct buffer_head *bh = NULL;
	int nr_seg_entries_blk = BLK_SZ / sizeof(struct stl_seg_entry);
	int ret=0;
	struct stl_seg_entry *entry0;
	unsigned long blknr;
	unsigned int zonenr = 0;
	struct stl_sb *sb;
	unsigned long nr_data_zones;
	unsigned long nr_seg_entries_read;
	
	if (NULL == ctx)
		return -1;

	bdev = ctx->dev->bdev;
	sb = ctx->sb;

	nr_data_zones = sb->zone_count_main; /* these are the number of segment entries to read */
	nr_seg_entries_read = 0;
	
	ret = allocate_freebitmap(ctx);
	printk(KERN_INFO "\n Allocated free bitmap, ret: %d", ret);
	if (0 > ret)
		return ret;
	ret = allocate_gc_zone_bitmap(ctx);
	printk(KERN_INFO "\n Allocated gc zone bitmap, ret: %d", ret);
	if (0 > ret) {
		kfree(ctx->freezone_bitmap);
		return ret;
	}


	ctx->min_mtime = ULLONG_MAX;
	ctx->max_mtime = get_elapsed_time(ctx);
	pba = sb->sit_pba;
	/* bread take blocks that are 4096 bytes */
	blknr = pba/NR_SECTORS_IN_BLK;
	nrblks = sb->blk_count_sit;
	printk(KERN_INFO "\n blknr of first SIT is: %lu", blknr);
	printk(KERN_ERR "\n zone_count_main: %u", sb->zone_count_main);
	printk(KERN_ERR "\n nr_freezones (1) : %u", ctx->nr_freezones);
	while (zonenr < sb->zone_count_main) {
		printk(KERN_ERR "\n zonenr: %u", zonenr);
		if (blknr > ctx->sb->zone0_pba) {
			//panic("seg entry blknr cannot be bigger than the data blknr");
			printk(KERN_ERR "seg entry blknr cannot be bigger than the data blknr");
			return -1;
		}
		printk(KERN_INFO "\n blknr: %lu", blknr);
		bh = __bread(bdev, blknr, BLK_SZ);
		if (!bh) {
			kfree(ctx->freezone_bitmap);
			kfree(ctx->gc_zone_bitmap);
			return -1;
		}
		entry0 = (struct stl_seg_entry *) bh->b_data;
		if (nr_data_zones > nr_seg_entries_blk)
			nr_seg_entries_read = nr_seg_entries_blk;
		else
			nr_seg_entries_read = nr_data_zones;
		read_seg_entries_from_block(ctx, entry0, nr_seg_entries_read, &zonenr);
		nr_data_zones = nr_data_zones - nr_seg_entries_read;
		blknr = blknr + 1;
		put_bh(bh);
	}
	printk(KERN_ERR "\n nr_freezones (2) : %u", ctx->nr_freezones);
	return 0;
}

struct stl_sb * read_superblock(struct ctx *ctx, unsigned long pba)
{

	struct block_device *bdev = ctx->dev->bdev;
	unsigned int blknr = pba;
	struct buffer_head *bh;
	struct stl_sb * sb;
	/* TODO: for the future. Right now we keep
	 * the block size the same as the sector size
	 *
	*/
	if (set_blocksize(bdev, BLK_SZ))
		return NULL;

	printk(KERN_INFO "\n reading sb at pba: %lu", pba);

	bh = __bread(bdev, blknr, BLK_SZ);
	if (!bh)
		return NULL;
	
	sb = (struct stl_sb *)bh->b_data;
	if (sb->magic != STL_SB_MAGIC) {
		printk(KERN_INFO "\n Wrong superblock!");
		put_bh(bh);
		return NULL;
	}
	printk(KERN_INFO "\n sb->magic: %u", sb->magic);
	printk(KERN_INFO "\n sb->max_pba: %d", sb->max_pba);
	/* We put the page in case of error in the future (put_page)*/
	ctx->sb_page = bh->b_page;
	return sb;
}

/*
 * SB1, SB2, Revmap, Translation Map, Revmap Bitmap, CKPT, SIT, Dataa
 */
int read_metadata(struct ctx * ctx)
{
	int ret;
	struct stl_sb * sb1, *sb2;
	struct stl_ckpt *ckpt;
	struct page *page;

	sb1 = read_superblock(ctx, 0);
	if (NULL == sb1) {
		printk(KERN_ERR "\n read_superblock failed! cannot read the metadata ");
		return -1;
	}

	printk(KERN_INFO "\n superblock read!");
	/*
	 * we need to verify that sb1 is uptodate.
	 * Right now we do nothing. we assume
	 * sb1 is okay.
	 */
	page = ctx->sb_page;
	sb2 = read_superblock(ctx, 1);
	if (!sb2) {
		printk(KERN_ERR "\n Could not read the second superblock!");
		return -1;
	}
	put_page(page);
	ctx->sb = sb2;
	ctx->max_pba = ctx->sb->max_pba;
	ctx->nr_lbas_in_zone = (1 << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	ctx->revmap_pba = ctx->sb->revmap_pba;
	printk(KERN_ERR "\n ** ctx->revmap_pba (first revmap bm block pba) : %llu", ctx->revmap_pba);

	ckpt = get_cur_checkpoint(ctx);
	if (NULL == ckpt) {
		put_page(ctx->sb_page);
		return -1;
	}	
	ctx->ckpt = ckpt;
	printk(KERN_INFO "\n checkpoint read!");
	if (!ckpt->clean) {
		printk(KERN_ERR "\n Scrubbing metadata after an unclean shutdown...");
		ret = do_recovery(ctx);
		return ret;
	}
	printk(KERN_INFO "\n sb->blk_count_revmap_bm: %d", ctx->sb->blk_count_revmap_bm);
	printk(KERN_ERR "\n nr of revmap blks: %u", ctx->sb->blk_count_revmap);

	ctx->app_write_frontier = ctx->ckpt->cur_frontier_pba;
	printk(KERN_ERR "%s %d ctx->app_write_frontier: %llu\n", __func__, __LINE__, ctx->app_write_frontier);
	ctx->app_wf_end = zone_end(ctx, ctx->app_write_frontier);
	printk(KERN_ERR "%s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->app_wf_end);
	printk(KERN_ERR "max_pba = %d", ctx->max_pba);
	ctx->free_sectors_in_wf = ctx->app_wf_end - ctx->app_write_frontier + 1;
	printk(KERN_ERR "ctx->free_sectors_in_wf: %lld", ctx->free_sectors_in_wf);

	ret = read_revmap_bitmap(ctx);
	if (ret) {
		put_page(ctx->sb_page);
		put_page(ctx->ckpt_page);
		return -1;
	}
	printk(KERN_ERR "\n revmap bitmap read!");
	printk(KERN_ERR "\n before: PBA for first revmap blk: %u", ctx->sb->revmap_pba/NR_SECTORS_IN_BLK);
	read_revmap(ctx);
	printk(KERN_INFO "Reverse map flushed!");
	ret = read_translation_map(ctx);
	if (0 > ret) {
		put_page(ctx->sb_page);
		put_page(ctx->ckpt_page);
		put_page(ctx->revmap_bm);
		printk(KERN_ERR "\n read_extent_map failed! cannot read the metadata ");
		return ret;
	}
	printk(KERN_INFO "\n extent_map read!");

	ctx->nr_freezones = 0;
	ctx->bitmap_bytes = sb1->zone_count_main /BITS_IN_BYTE;
	if (sb1->zone_count_main % BITS_IN_BYTE)
		ctx->bitmap_bytes = ctx->bitmap_bytes + 1;
	printk(KERN_INFO "\n Nr of zones in main are: %u, bitmap_bytes: %d", sb1->zone_count_main, ctx->bitmap_bytes);
	if (sb1->zone_count_main % BITS_IN_BYTE > 0)
		ctx->bitmap_bytes += 1;
	ctx->nr_freezones = 0;
	read_seg_info_table(ctx);
	printk(KERN_INFO "\n read segment entries, free bitmap created! \n");
	if (ctx->nr_freezones != ckpt->nr_free_zones) { 
		/* TODO: Do some recovery here.
		 * We do not wait for confirmation of SIT pages on the
		 * disk. we match the SIT entries to that by the
		 * translation map. we also make sure that the ckpt
		 * entries are based on the translation map
		 */
		do_recovery(ctx);
		put_page(ctx->sb_page);
		put_page(ctx->ckpt_page);
		put_page(ctx->revmap_bm);
		printk(KERN_ERR "\n ctx->nr_freezones: %u, ckpt->nr_free_zones:%llu", ctx->nr_freezones, ckpt->nr_free_zones);
		printk(KERN_ERR "\n SIT and checkpoint does not match!");
		return -1;
	}
	printk(KERN_ERR "\n Metadata read!");
	return 0;
}

/*

unsigned long nstl_pages_to_free_count(struct shrinker *shrinker, struct shrink_control *sc)
{

	int flag = 0;
	count = flush_count_tm_blocks(ctx, false, &flag);
	count += flush_count_sit_blocks(ctx, false, &flag);
}


unsigned long nstl_free_pages()
{
	int nr_to_scan = sc->nr_to_scan;
	gfp_t gfp_mask = sc->gfp_mask;

	if((gfp_mask  & __GFP_IO) != __GFP_IO)
		return SHRINK_STOP;

	count = flush_count_tm_blocks(ctx, true, nr_to_scan);
	if (count < nr_to_scan) {
		flush_count_sit_blocks(ctx, true, nr_to_scan - count);
	}
}



static struct nstl_shrinker {
	.count_objects = nstl_pages_to_free_count;
	.scan_objects = nstl_free_pages;
	.seeks = DEFAULT_SEEKS;
};
*/

static void destroy_caches(struct ctx *ctx)
{	
	kmem_cache_destroy(ctx->gc_extents_cache);
	kmem_cache_destroy(ctx->subbio_ctx_cache);
	kmem_cache_destroy(ctx->tm_page_cache);
	kmem_cache_destroy(ctx->sit_extent_cache);
	kmem_cache_destroy(ctx->reflist_cache);
	kmem_cache_destroy(ctx->sit_ctx_cache);
	kmem_cache_destroy(ctx->sit_page_cache);
	kmem_cache_destroy(ctx->tm_page_write_cache);
	kmem_cache_destroy(ctx->revmap_bioctx_cache);
	kmem_cache_destroy(ctx->bioctx_cache);
	kmem_cache_destroy(ctx->sit_ctx_cache);
}


static int create_caches(struct ctx *ctx)
{
	ctx->bioctx_cache = kmem_cache_create("bioctx_cache", sizeof(struct nstl_bioctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->bioctx_cache) {
		return -1;
	}
	ctx->revmap_bioctx_cache = kmem_cache_create("revmap_bioctx_cache", sizeof(struct revmap_meta_inmem), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->revmap_bioctx_cache) {
		goto destroy_cache_bioctx;
	}
	ctx->tm_page_write_cache = kmem_cache_create("tm_page_write_cache", sizeof(struct tm_page_write_ctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->tm_page_write_cache) {
		goto destroy_revmap_bioctx_cache;
	}
	ctx->sit_page_cache = kmem_cache_create("sit_page_cache", sizeof(struct sit_page), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->sit_page_cache) {
		goto destroy_tm_page_write_cache;
	}
	ctx->sit_ctx_cache  = kmem_cache_create("sit_ctx_cache", sizeof(struct sit_page_write_ctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->sit_ctx_cache) {
		goto destroy_sit_page_cache;
	}
	ctx->reflist_cache = kmem_cache_create("reflist_cache", sizeof(struct ref_list), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->reflist_cache) {
		goto destroy_sit_ctx_cache;
	}
	ctx->tm_page_cache = kmem_cache_create("tm_page_cache", sizeof(struct tm_page), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->tm_page_cache) {
		goto destroy_reflist_cache;
	}
	ctx->sit_extent_cache = kmem_cache_create("sit_extent_cache", sizeof(struct sit_extent), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->sit_page_cache) {
		goto destroy_tm_page_cache;
	}
	ctx->subbio_ctx_cache = kmem_cache_create("subbio_ctx_cache", sizeof(struct nstl_sub_bioctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->subbio_ctx_cache) {
		goto destroy_sit_extent_cache;
	}
	ctx->gc_extents_cache = kmem_cache_create("gc_extents_cache", sizeof(struct gc_extents), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->gc_extents_cache) {
		goto destroy_subbio_ctx_cache;
	}
	printk(KERN_ERR "\n gc_extents_cache initialized to address: %llu", ctx->gc_extents_cache);
	ctx->app_read_ctx_cache = kmem_cache_create("app_read_ctx_cache", sizeof(struct app_read_ctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->app_read_ctx_cache) {
		goto destroy_gc_extents_cache;
	}
	printk(KERN_ERR "\n app_read_ctx_cache initialized to address: %llu", ctx->app_read_ctx_cache);
	return 0;
/* failed case */
destroy_gc_extents_cache:
	kmem_cache_destroy(ctx->gc_extents_cache);
destroy_subbio_ctx_cache:
	kmem_cache_destroy(ctx->subbio_ctx_cache);
destroy_sit_extent_cache:
	kmem_cache_destroy(ctx->sit_extent_cache);
destroy_tm_page_cache:
	kmem_cache_destroy(ctx->tm_page_cache);
destroy_reflist_cache:
	kmem_cache_destroy(ctx->reflist_cache);
destroy_sit_ctx_cache:
	kmem_cache_destroy(ctx->sit_ctx_cache);
destroy_sit_page_cache:
	kmem_cache_destroy(ctx->sit_page_cache);
destroy_tm_page_write_cache:
	kmem_cache_destroy(ctx->tm_page_write_cache);
destroy_revmap_bioctx_cache:
	kmem_cache_destroy(ctx->revmap_bioctx_cache);
destroy_cache_bioctx:
	kmem_cache_destroy(ctx->bioctx_cache);
	return -1;
}

static int stl_ctr(struct dm_target *dm_target, unsigned int argc, char **argv)
{
	int ret = -ENOMEM;
	struct ctx *ctx;
	unsigned long long max_pba;
	loff_t disk_size;

	printk(KERN_INFO "\n argc: %d", argc);
	if (argc < 2) {
		dm_target->error = "dm-stl: Invalid argument count";
		return -EINVAL;
	}

	//printk(KERN_INFO "\n argv[0]: %s, argv[1]: %s", argv[0], argv[1]);

	ctx = kzalloc(sizeof(struct ctx), GFP_KERNEL);
	if (!ctx) {
		return ret;
	}

	dm_target->private = ctx;

	ret = dm_get_device(dm_target, argv[0], dm_table_get_mode(dm_target->table), &ctx->dev);
    	if (ret) {
		dm_target->error = "dm-nstl: Device lookup failed.";
		goto free_ctx;
	}

	/*
	if (bdev_zoned_model(ctx->dev->bdev) == BLK_ZONED_NONE) {
                dm_target->error = "Not a zoned block device";
                ret = -EINVAL;
                goto free_ctx;
        }*/


	//printk(KERN_INFO "\n sc->dev->bdev->bd_part->start_sect: %llu", ctx->dev->bdev->bd_part->start_sect);
	//printk(KERN_INFO "\n sc->dev->bdev->bd_part->nr_sects: %llu", ctx->dev->bdev->bd_part->nr_sects);


	ctx->extent_pool = mempool_create_slab_pool(MIN_EXTENTS, extents_slab);
	if (!ctx->extent_pool)
		goto put_dev;

	disk_size = i_size_read(ctx->dev->bdev->bd_inode);
	//printk(KERN_INFO "\n The disk size as read from the bd_inode: %llu", disk_size);
	//printk(KERN_INFO "\n max sectors: %llu", disk_size/512);
	//printk(KERN_INFO "\n max blks: %llu", disk_size/4096);
	sema_init(&ctx->sit_kv_store_lock, 1);
	sema_init(&ctx->tm_kv_store_lock, 1);
	sema_init(&ctx->flush_lock, 1);

	spin_lock_init(&ctx->lock);
	spin_lock_init(&ctx->tm_ref_lock);
	spin_lock_init(&ctx->rev_entries_lock);
	spin_lock_init(&ctx->tm_flush_lock);
	spin_lock_init(&ctx->sit_flush_lock);
	spin_lock_init(&ctx->rev_flush_lock);
	spin_lock_init(&ctx->ckpt_lock);

	atomic_set(&ctx->io_count, 0);
	atomic_set(&ctx->n_reads, 0);
	atomic_set(&ctx->pages_alloced, 0);
	atomic_set(&ctx->nr_writes, 0);
	atomic_set(&ctx->nr_failed_writes, 0);
	atomic_set(&ctx->revmap_blk_count, 0);
	atomic_set(&ctx->revmap_sector_count, 0);
	ctx->target = 0;
	//printk(KERN_ERR "\n About to read metadata! 1 \n");
	//printk(KERN_ERR "\n About to read metadata! 2 \n");
	init_waitqueue_head(&ctx->tm_blk_flushq);
	//printk(KERN_ERR "\n About to read metadata! 3 \n");
	atomic_set(&ctx->nr_pending_writes, 0);
	atomic_set(&ctx->nr_sit_pages, 0);
	atomic_set(&ctx->nr_tm_pages, 0);
	//printk(KERN_ERR "\n About to read metadata! 4 \n");
	init_waitqueue_head(&ctx->refq);
	init_waitqueue_head(&ctx->rev_blk_flushq);
	init_waitqueue_head(&ctx->tm_blk_flushq);
	init_waitqueue_head(&ctx->ckptq);
	ctx->mounted_time = ktime_get_real_seconds();
	ctx->extent_tbl_root = RB_ROOT;
	ctx->rev_tbl_root = RB_ROOT;
	rwlock_init(&ctx->metadata_update_lock);

	ctx->tm_rb_root = RB_ROOT;

	ctx->sit_rb_root = RB_ROOT;
	rwlock_init(&ctx->sit_rb_lock);

	ctx->sectors_copied = 0;
	ret = -ENOMEM;
	dm_target->error = "dm-stl: No memory";

	ctx->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
	if (!ctx->page_pool)
		goto free_extent_pool;

	//printk(KERN_INFO "about to call bioset_init()");
	ctx->bs = kzalloc(sizeof(*(ctx->bs)), GFP_KERNEL);
	if (!ctx->bs)
		goto destroy_page_pool;

	if(bioset_init(ctx->bs, BS_NR_POOL_PAGES, 0, BIOSET_NEED_BVECS|BIOSET_NEED_RESCUER) == -ENOMEM) {
		printk(KERN_ERR "\n bioset_init failed!");
		goto free_bioset;
	}
	ret = create_caches(ctx);
	if (0 > ret) {
		goto uninit_bioset;
	}
	printk(KERN_ERR "\n caches created!");
	ctx->s_chksum_driver = crypto_alloc_shash("crc32c", 0, 0);
	if (IS_ERR(ctx->s_chksum_driver)) {
		printk(KERN_ERR "Cannot load crc32c driver.");
		ret = PTR_ERR(ctx->s_chksum_driver);
		ctx->s_chksum_driver = NULL;
		goto destroy_cache;
	}

	kref_init(&ctx->ongoing_iocount);
	//printk(KERN_ERR "\n About to read metadata! 5 ! \n");

    	ret = read_metadata(ctx);
	if (ret < 0) 
		goto destroy_cache;

	max_pba = ctx->dev->bdev->bd_inode->i_size / 512;
	sprintf(ctx->nodename, "stl/%s", argv[1]);
	ret = -EINVAL;
	//printk(KERN_INFO "\n device records max_pba: %llu", max_pba);
	//printk(KERN_INFO "\n formatted max_pba: %d", ctx->max_pba);
	if (ctx->max_pba > max_pba) {
		dm_target->error = "dm-stl: Invalid max pba found on sb";
		goto free_metadata_pages;
	}

	printk(KERN_ERR "\n Initializing gc_extents list, ctx->gc_extents_cache: %llu ", ctx->gc_extents_cache);
	ctx->gc_extents = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
	if (!ctx->gc_extents) {
		printk(KERN_ERR "\n Could not allocate gc_extent and hence could not initialized \n");
		goto free_metadata_pages;
	}
	printk(KERN_ERR "\n Extent allocated....! ctx->gc_extents: %llu", ctx->gc_extents);
	INIT_LIST_HEAD(&ctx->gc_extents->list);
	/*
	 * Will work with timer based invocation later
	 * init_timer(ctx->timer);
	 */
	/*
	ret = stl_gc_thread_start(ctx);
	if (ret) {
		goto free_metadata_pages;
	}*/

	/*
	if (register_shrinker(nstl_shrinker))
		goto stop_gc_thread;
	*/
	//printk(KERN_ERR "\n ctr() done!!");
	return 0;
/* failed case */
/*stop_gc_thread:
	stl_gc_thread_stop(ctx);
*/
free_metadata_pages:
	printk(KERN_ERR "\n freeing metadata pages!");
	if (ctx->revmap_bm)
		put_page(ctx->revmap_bm);
	if (ctx->sb_page)
		put_page(ctx->sb_page);
	if (ctx->ckpt_page)
		put_page(ctx->ckpt_page);
	/* TODO : free extent page
	 * and segentries page */
destroy_cache:
	destroy_caches(ctx);
uninit_bioset:
	bioset_exit(ctx->bs);
free_bioset:
	kfree(ctx->bs);
destroy_page_pool:
	if (ctx->page_pool)
		mempool_destroy(ctx->page_pool);

free_extent_pool:
	mempool_destroy(ctx->extent_pool);
put_dev:
	dm_put_device(dm_target, ctx->dev);
free_ctx:
	kfree(ctx);
	return ret;
}

static void stl_dtr(struct dm_target *dm_target)
{
	struct ctx *ctx = dm_target->private;

	sync_blockdev(ctx->dev->bdev);
	printk(KERN_ERR "\n Inside dtr! About to call flush_revmap_entries()");
	flush_revmap_entries(ctx);
	/* At this point we are sure that the revmap
	 * entries have made it to the disk
	 */
	printk(KERN_ERR "\n %s about to flush translation blocks!");
	flush_translation_blocks(ctx);
	do_checkpoint(ctx);
	//printk(KERN_ERR "\n checkpoint done!");
	/* If we are here, then there was no crash while writing out
	 * the disk metadata
	 */

	/* TODO : free extent page
	 * and segentries page */
	if (ctx->revmap_bm)
		put_page(ctx->revmap_bm);
	if (ctx->sb_page)
		put_page(ctx->sb_page);
	if (ctx->ckpt_page)
		put_page(ctx->ckpt_page);
	printk(KERN_ERR "\n metadata pages freed! \n");
	/* timer based gc invocation for later
	 * del_timer_sync(&ctx->timer_list);
	 */
	destroy_caches(ctx);
	printk(KERN_ERR "\n caches destroyed! \n");
	//stl_gc_thread_stop(ctx);
	//printk(KERN_ERR "\n gc thread stopped! \n");
	bioset_exit(ctx->bs);
	//printk(KERN_ERR "\n exited from bioset \n");
	kfree(ctx->bs);
	//printk(KERN_ERR "\n bioset memory freed \n");
	mempool_destroy(ctx->page_pool);
	//printk(KERN_ERR "\n memory pool destroyed\n");
	mempool_destroy(ctx->extent_pool);
	//printk(KERN_ERR "\n extent_pool destroyed \n");
	dm_put_device(dm_target, ctx->dev);
	//printk(KERN_ERR "\n device mapper target released\n");
	kfree(ctx);
	//printk(KERN_ERR "\n ctx memory freed!\n");
	printk(KERN_ERR "\n Goodbye World!\n");
	return;
}

int nstl_zone_reset(struct ctx *ctx, struct bio *bio)
{
	struct block_device *bdev;
	sector_t sector;
	sector_t nr_sectors;
	int ret;

	bdev = ctx->dev->bdev;
	sector = bio->bi_iter.bi_sector;
	nr_sectors = bio_sectors(bio);

	/* TODO: Adjust the segentries, number of free blocks,
	 * number of free zones, etc
	 * If that zone has 0 blocks then nothing much needs to be
	 * done. After that call:
	 * do_checkpoint();
	 */

	ret  = blkdev_reset_zones(bdev, sector, nr_sectors, GFP_KERNEL);
	if (ret)
		return ret;
	
	return 0;
}

/*
 * TODO: IMPLEMENT
 *
 */
int nstl_zone_reset_all(struct ctx *ctx, struct bio *bio)
{
	return 0;
}

static int stl_map(struct dm_target *dm_target, struct bio *bio)
{
	struct ctx *ctx;
	int ret = 0;
       
	if (!dm_target)
		return 0;

	if (!bio) {
		dump_stack();
		return -EINVAL;
	}

	ctx = dm_target->private;

	if(unlikely(bio == NULL)) {
		return 0;
	}

	switch (bio_op(bio)) {
		case REQ_OP_READ:
			ret = nstl_read_io(ctx, bio);
			break;
		case REQ_OP_WRITE:
			ret = nstl_write_io(ctx, bio);
			break;
		/*
		case REQ_OP_ZONE_RESET:
			ret = nstl_zone_reset(ctx, bio);
			break;
		case REQ_OP_ZONE_RESET_ALL:
			ret = nstl_zone_reset_all(ctx, bio);
			break;
		*/
		default:
			break;
	}
	
	return (ret? ret: DM_MAPIO_SUBMITTED);
}

static struct target_type stl_target = {
	.name            = "nstl",
	.version         = {1, 0, 0},
	.module          = THIS_MODULE,
	.ctr             = stl_ctr,
	.dtr             = stl_dtr,
	.map             = stl_map,
	.status          = 0 /*stl_status*/,
	.prepare_ioctl   = 0 /*stl_prepare_ioctl*/,
	.message         = 0 /*stl_message*/,
	.iterate_devices = 0 /*stl_iterate_devices*/,
};

/* Called on module entry (insmod) */
static int __init dm_stl_init(void)
{
	int r = -ENOMEM;

	if (!(extents_slab = KMEM_CACHE(extent, 0)))
		return r;
	if ((r = dm_register_target(&stl_target)) < 0)
		goto fail;
	printk(KERN_INFO "dm-nstl\n %s %d", __func__, __LINE__);
	return 0;

fail:
	if (extents_slab)
		kmem_cache_destroy(extents_slab);
	return r;
}

/* Called on module exit (rmmod) */
static void __exit dm_stl_exit(void)
{
	dm_unregister_target(&stl_target);
	if(extents_slab)
		kmem_cache_destroy(extents_slab);
}


module_init(dm_stl_init);
module_exit(dm_stl_exit);

MODULE_DESCRIPTION(DM_NAME "log structured SMR Translation Layer");
MODULE_LICENSE("GPL");
