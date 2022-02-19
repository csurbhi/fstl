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

#include "metadata.h"

#define DM_MSG_PREFIX "lsdm"


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

static struct kmem_cache * extents_slab;
#define IN_MAP 0
#define IN_CLEANING 1
#define STALLED_WRITE 2

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
	//trace_printk("\n %s zone_begins: %llu sb->zone0_pba: %u ctx->nr_lbas_in_zone: %llu", __func__, zone_begins, ctx->sb->zone0_pba, ctx->nr_lbas_in_zone);
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
static struct extent *_lsdm_rb_geq(struct rb_root *root, off_t lba, int print)
{
	struct rb_node *node = root->rb_node;  /* top of the tree */
	struct extent *higher = NULL;
	struct extent *e = NULL;

	//print = 1;

	while (node) {
		e = container_of(node, struct extent, rb);
		if (print) {
			printk(KERN_ERR "\n %s Searching lba: %llu, found:  (e->lba: %llu, e->pba: %llu e->len: %lu)", __func__, lba, e->lba, e->pba, e->len); 
		}
		if ((e->lba >= lba) && (!higher || (e->lba < higher->lba))) {
			higher = e;
			//printk(KERN_ERR "\n %s Searching lba: %llu, higher:  (e->lba: %llu, e->pba: %llu e->len: %lu)", __func__, lba, e->lba, e->pba, e->len); 
		}
		if (e->lba > lba) {
			node = node->rb_left;
			//printk(KERN_ERR "\n \t Going left! ");
		} else {
			/* e->lba <= lba */
			if ((e->lba + e->len) <= lba) {
				node = node->rb_right;
			//	printk(KERN_ERR "\n \t\t Going right! ");
			} else {
				/* lba falls within "e"
				 * (e->lba <= lba) && ((e->lba + e->len) > lba) */
				//printk(KERN_ERR "\n Found an overlapping e, returning e");
				return e;
			}
		}
	}
	printk(KERN_ERR "\n did not find a natural match and so returning the next higher: %s ", (higher == NULL) ? "null" : "not null");
	return higher;
}

static struct extent *lsdm_rb_geq(struct ctx *ctx, off_t lba, int print)
{
	struct extent *e = NULL;

	down_read(&ctx->metadata_update_lock); 
	e = _lsdm_rb_geq(&ctx->extent_tbl_root, lba, print);
	up_read(&ctx->metadata_update_lock); 

	return e;
}

/* Not called under any lock
 */
static struct extent * revmap_rb_search_geq(struct ctx *ctx, sector_t pba)
{
	struct rb_root *root = &ctx->rev_tbl_root;
	unsigned int print = 1;
	struct extent * e;

	e = _lsdm_rb_geq(root, pba, print);
	return e;
}

/* TODO: depending on the root decrement the correct nr of extents */
static void lsdm_rb_remove(struct ctx *ctx, struct rb_root *root, struct extent *e)
{
	rb_erase(&e->rb, root);
	ctx->n_extents--;
}


static struct extent *lsdm_rb_next(struct extent *e)
{
	struct rb_node *node = rb_next(&e->rb);
	return (node == NULL) ? NULL : container_of(node, struct extent, rb);
}

static struct extent *lsdm_rb_prev(struct extent *e)
{
	struct rb_node *node = rb_prev(&e->rb);
	return (node == NULL) ? NULL : container_of(node, struct extent, rb);
}

static int check_node_contents(struct rb_node *node)
{
	int ret = 0;
	struct extent *e, *next, *prev;

	if (!node)
		return -1;

	e = rb_entry(node, struct extent, rb);

	if (e->lba < 0) {
		printk(KERN_ERR "\n LBA is <=0, tree corrupt!! \n");
		return -1;
	}
	if (e->pba < 0) {
		printk(KERN_ERR "\n PBA is <=0, tree corrupt!! \n");
		return -1;
	}
	if (e->len <= 0) {
		printk(KERN_ERR "\n len is <=0, tree corrupt!! \n");
		return -1;
	}

	next = lsdm_rb_next(e);
	prev = lsdm_rb_prev(e);

	if (next  && next->lba == e->lba) {
		printk(KERN_ERR "\n LBA corruption (next) ! lba: %lld is present in two nodes!", next->lba);
		printk(KERN_ERR "\n next->lba: %lld next->pba: %lld next->len: %d", next->lba, next->pba, next->len);
		return -1;
	}

	if (next && e->lba + e->len == next->lba) {
		if (e->pba + e->len == next->pba) {
			printk(KERN_ERR "\n Nodes not merged! ");
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d", e->lba, e->pba, e->len);
			printk(KERN_ERR "\n next->lba: %lld next->pba: %lld next->len: %d", next->lba, next->pba, next->len);
			return -1;
		}
	}

	if (prev && prev->lba == e->lba) {
		printk(KERN_ERR "\n LBA corruption (prev)! lba: %lld is present in two nodes!", prev->lba);
		return -1;
	}

	if (prev && prev->lba + prev->len == e->lba) {
		if (prev->pba + prev->len == e->pba) {
			printk(KERN_ERR "\n Nodes not merged! ");
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d", e->lba, e->pba, e->len);
			printk(KERN_ERR "\n prev->lba: %lld prev->pba: %lld prev->len: %d", prev->lba, prev->pba, prev->len);
			return -1;

		}
	}

	if(node->rb_left)
		ret = check_node_contents(node->rb_left);

	if (ret < 0)
		return ret;

	if (node->rb_right)
		ret = check_node_contents(node->rb_right);

	return ret;

}

static int lsdm_tree_check(struct rb_root *root)
{
	struct rb_node *node = root->rb_node;
	int ret = 0;

	ret = check_node_contents(node);
	return ret;

}


/* Check if we can be merged with the left or the right node */
static void merge(struct ctx *ctx, struct rb_root *root, struct extent *e)
{
	struct extent *prev, *next;

	if (!e)
		BUG();

	prev = lsdm_rb_prev(e);
	next = lsdm_rb_next(e);
	if (prev) {
		/*
		printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d", e->lba, e->pba, e->len);
		printk(KERN_ERR "\n prev->lba: %lld prev->pba: %lld prev->len: %d \n", prev->lba, prev->pba, prev->len);
		*/
		if((prev->lba + prev->len) == e->lba) {
			if ((prev->pba + prev->len) == e->pba) {
				prev->len += e->len;
				lsdm_rb_remove(ctx, root, e);
				mempool_free(e, ctx->extent_pool);
				e = prev;
				ctx->n_extents--;
			}
		}
	}
	if (next) {
		/*
		printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d", e->lba, e->pba, e->len);
		printk(KERN_ERR "\n next->lba: %lld next->pba: %lld next->len: %d \n", next->lba, next->pba, next->len);
		*/
		if (next->lba == (e->lba + e->len)) {
			if (next->pba == (e->pba + e->len)) {
				e->len += next->len;
				lsdm_rb_remove(ctx, root, next);
				mempool_free(next, ctx->extent_pool);
				ctx->n_extents--;
			}
		}
	}
	ctx->n_extents++;
}

int _lsdm_verbose;
/* 
 * Returns NULL if the new node code be added.
 * Return overlapping extent otherwise (overlapping node already exists)
 */
static struct extent * lsdm_rb_insert(struct ctx *ctx, struct rb_root *root, struct extent *new)
{
	struct rb_node **link = NULL, *parent = NULL;
	struct extent *e = NULL;
	int ret = 0;

	if (!root || !new) {
		printk(KERN_ERR "\n Root is NULL! \n");
		BUG();
	}

	RB_CLEAR_NODE(&new->rb);

	link = &root->rb_node;

	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		e = rb_entry(parent, struct extent, rb);
		//printk(KERN_ERR "\n %s parent->lba: %llu, parent->pba: %llu, parent->len: %lu", __func__, e->lba, e->pba, e->len);
		if (e->lba >= (new->lba + new->len)) {
			//printk(KERN_ERR "\n \t Going left now! ");
			link = &(parent->rb_left);
		} else if ((e->lba + e->len) <= new->lba){
			//printk(KERN_ERR "\n \t Going right now! ");
			link = &(parent->rb_right);
		} else {
			//printk(KERN_ERR "\n Found e (same as parent above)! returning!");
			return e;
		}
	}
	//printk( KERN_ERR "\n %s Inserting (lba: %llu pba: %llu len: %u) ", __func__, new->lba, new->pba, new->len);
	/* Put the new node there */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
	merge(ctx, root, new);
	/* new should be physically discontiguous
	 */
	
	ret = lsdm_tree_check(root);
	if (ret < 0) {
		printk(KERN_ERR"\n !!!! Corruption while Inserting: lba: %lld pba: %lld len: %d", new->lba, new->pba, new->len);
		BUG();
	}
	return NULL;
}


/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to map.
 */
static int lsdm_update_range(struct ctx *ctx, struct rb_root *root, sector_t lba, sector_t pba, size_t len)
{
	struct extent *e = NULL, *new = NULL, *split = NULL, *prev = NULL;
	struct extent *tmp = NULL;
	int diff = 0;

	//printk(KERN_ERR "\n Entering %s lba: %llu, pba: %llu, len:%ld ", __func__, lba, pba, len);
	
	if (root == NULL) {
		printk(KERN_ERR "\n %f root: NULL! \n");
		BUG_ON(root == NULL);
	}
	BUG_ON(len == 0);

	//printk(KERN_ERR "\n lba: %lld, len: %lld ", lba, len);
	new = mempool_alloc(ctx->extent_pool, GFP_NOIO);
	if (unlikely(!new)) {
		BUG();
		printk(KERN_ERR "\n Could not allocate memory!");
		return -ENOMEM;
	}
	RB_CLEAR_NODE(&new->rb);
	extent_init(new, lba, pba, len);

	e = lsdm_rb_insert(ctx, root, new);
	/* No overlapping extent found, inserted new */
	if (!e)
		return 0;

	//printk(KERN_ERR "\n e->lba: %d, e->len: %ld ", e->lba, e->len);
	/* new overlaps with e
	 * new: ++++
	 * e: --
	 */

	 /*
	 * Case 1: overwrite a part of the existing extent
	 * 	++++++++++
	 * ----------------------- 
	 *
	 *  No end matches!! new overlaps with ONLY ONE extent e
	 */

	if ((lba > e->lba) && ((lba + len) < (e->lba + e->len))) {
		diff =  new->lba - e->lba;
		split = mempool_alloc(ctx->extent_pool, GFP_NOIO);
		if (!split) {
			mempool_free(new, ctx->extent_pool);
			return -ENOMEM;
		}
		diff =  lba - e->lba;
		/* Initialize split before e->len changes!! */
		extent_init(split, lba + len, e->pba + (diff + len), e->len - (diff + len));

		e->len = diff;
		//printk("\n Case1: Modified: (new->lba: %llu, new->pba: %llu, new->len: %lu) ", e->lba, e->pba, e->len);
		if (lsdm_rb_insert(ctx, root, new)) {
			printk(KERN_ERR"\n Corruption in case 1.1, diff: %d !! ", diff);
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d diff:%d ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();
		} 
		if (lsdm_rb_insert(ctx, root, split)) {
			printk(KERN_ERR"\n Corruption in case 1.2!! diff: %d", diff);
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d diff:%d ", e->lba, e->pba, e->len, diff);
			printk(KERN_ERR "\n split->lba: %lld split->pba: %lld split->len: %d ", split->lba, split->pba, split->len);
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();
		} 
		return(0);
	}



	/* Start from the smallest node that overlaps*/
	 while(1) {
		prev = lsdm_rb_prev(e);
		if (!prev)
			break;
		if (prev->lba + prev->len <= new->lba)
			break;
		e = prev;
	 }

	/* Now we consider the overlapping "e's" in an order of
         * increasing LBA
         */

	/*
	* Case 2: Overwrites an existing extent partially;
	* covers only the right portion of an existing extent (e)
	*      ++++++++
	* -----------
	*  e
	*
	*
	*      ++++++++
	* -------------
	*
	* (Right end of e1 and + could match!)
	*
	*/
	if ((lba > e->lba) && ((lba + len) >= (e->lba + e->len)) && (lba < (e->lba + e->len))) {
		//printk(KERN_ERR "\n Case2, Modified e->len: %lu to %lu ", e->len, lba - e->lba);
		e->len = lba - e->lba;
		e = lsdm_rb_next(e);
		/*  
		 *  process the next overlapping segments!
		 *  Fall through to the next case.
		 */
	}


	/* 
	 * Case 3: Overwrite many extents completely
	 *	++++++++++++++++++++
	 *	  ----- ------  --------
	 *
	 * Could also be exact same: 
	 * 	+++++
	 * 	-----
	 * We need to remove all such e
	 *
	 * here we compare left ends and right ends of 
	 * new and existing node e
	 */
	while ((e!=NULL) && (lba <= e->lba) && ((lba + len) >= (e->lba + e->len))) {
		tmp = lsdm_rb_next(e);
		lsdm_rb_remove(ctx, root, e);
		//printk("\n Case3, Removed: (e->lba: %llu, e->pba: %llu, e->len: %lu) ", e->lba, e->pba, e->len);
		mempool_free(e, ctx->extent_pool);
		e = tmp;
	}
	if (!e || (e->lba >= lba + len))  {
		if (lsdm_rb_insert(ctx, root, new)) {
			printk(KERN_ERR"\n Corruption in case 3!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d diff:%d ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();
		}
		return(0);
	}
	/* else fall down to the next case for the last
	 * component that overwrites an extent partially
	 */

	/* 
	 * Case 4: 
	 * Partially overwrite an extent
	 * ++++++++++
	 * 	-------------- OR
	 *
	 * Left end of + and - matches!
	 * +++++++
	 * --------------
	 *
	 */
	if ((lba <= e->lba) && (lba + len > e->lba) && (lba + len < e->lba + e->len))  {
		diff = lba + len - e->lba;
		lsdm_rb_remove(ctx, root, e);
		//printk("\n Case4, Removed: (e->lba: %llu, e->pba: %llu, e->len: %lu) ", e->lba, e->pba, e->len);
		e->lba = e->lba + diff;
		e->len = e->len - diff;
		e->pba = e->pba + diff;
		if (lsdm_rb_insert(ctx, root, new)) {
			printk(KERN_ERR"\n Corruption in case 4.1!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d diff:%d ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();
		}
		if (lsdm_rb_insert(ctx, root, e)) {
			printk(KERN_ERR"\n Corruption in case 4.2!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %d diff:%d ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();
		}
		return(0);
	}
	/* If you are here then you haven't covered some
	 * case!
	 */
	printk(KERN_ERR "\n You should not be here!! \n");
	printk(KERN_ERR "\n \n");
	BUG();
	return 0;
}
/*
 * extent_pool
 */
static void lsdm_free_rb_tree(struct ctx *ctx, struct rb_root *root)
{
	struct extent *e = rb_entry(root->rb_node, struct extent, rb);
	struct extent *next;

	while(e) {
		next = lsdm_rb_next(e);
		lsdm_rb_remove(ctx, root, e);
		mempool_free(e, ctx->extent_pool);
		e = next;
	}
}


static void lsdm_free_rb_mappings(struct ctx *ctx)
{
	lsdm_free_rb_tree(ctx, &ctx->rev_tbl_root);
	lsdm_free_rb_tree(ctx, &ctx->extent_tbl_root);
}

static inline int is_lsdm_ioidle(struct ctx *ctx)
{
	return atomic_read(&ctx->ioidle);
}

static inline void * lsdm_malloc(size_t size, gfp_t flags)
{
	void *addr;

	addr = kmalloc(size, flags);
	if (!addr) {
		addr = kvmalloc(size, flags);
	}

	return addr;
}

/* For BG_GC mode, go to the left most node in the 
 * in mem lsdm_extents RB tree 
 * For FG_GC; apply greedy to only the left most part of the tree
 * Ideally we want consequitive segments that are in the left most
 * part of the tree; or we want 'n' sequential zones that give the
 * most of any other 'n'
 */
static int select_zone_to_clean(struct ctx *ctx, int mode)
{
	int zonenr = -2;
	struct rb_node *node;

	if (mode == BG_GC) {
		node = rb_first(&ctx->gc_rb_root);
		if (node)
			zonenr = rb_entry(node, struct gc_rb_node, rb)->zonenr; 
		else 
			return -1;
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
	INIT_LIST_HEAD(&gc_extent->list);
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
			//trace_printk("\n GC read failed because of disk error!");
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
	static int count = 0;

	count ++;
	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		printk(KERN_ERR "\n %s could not allocate memory for bio ", __func__);
		return -ENOMEM;
	}
	nr_sectors = gc_extent->e.len;
	s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
	BUG_ON(gc_extent->e.len != s8);
	printk(KERN_ERR "\n %s count: %d gc_extent->e.len = %d ", __func__, count, s8);
	/* Now allocate pages to the bio 
	 * 2^3 sectors make 1 page
	 * Since the length is blk aligned, we dont have to add a 1
	 * to the nr_pages calculation
	 */
	nr_pages = (s8 >> 3);
	/* bio_add_page sets the bi_size for the bio */
	printk(KERN_ERR "\n %s nr_pages: %d, gc_extent->e.len: %d s8: %d", __func__, nr_pages, gc_extent->e.len, s8);
	BUG_ON(nr_pages == 0);
	 
	for(i=0; i<nr_pages; i++) {
		page = mempool_alloc(ctx->page_pool, GFP_KERNEL);
		if ((!page) || ( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0))) {
			bio_for_each_segment_all(bv, bio, iter_all) {
				mempool_free(bv->bv_page, ctx->page_pool);
				printk(KERN_ERR "\n %s could not add page to a bio ", __func__);
			}
			bio_put(bio);
			return -ENOMEM;
		}
	}
	bio->bi_iter.bi_sector = gc_extent->e.pba;
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio_set_dev(bio, ctx->dev->bdev);
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

/* All the bios share the same reference. The address of this common
 * reference is stored in the bi_private field of the bio
 */
static int read_all_bios_and_wait(struct ctx *ctx, struct gc_extents *last_extent)
{

	struct list_head *list_head;
	struct gc_extents *gc_extent;
	struct blk_plug plug;
	refcount_t * ref;

	blk_start_plug(&plug);
	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		ref = gc_extent->bio->bi_private;
		refcount_inc(ref);
		/* setup bio sets bio->bi_end_io = read_extent_done */
		submit_bio(gc_extent->bio);
	}
	/* When we arrive here, we know the last bio has completed.
	 * Since the previous bios were chained to this one, we know
	 * that the previous bios have completed as well
	 */
	blk_finish_plug(&plug);
	printk(KERN_ERR "\n %s Submitted reads, waiting for them to complete.....\n", __func__);
	wait_on_refcount(ctx, ref);
	kfree(ref);
	printk(KERN_ERR "\n %s reads done!", __func__);
	return 0;
}

/*
 * TODO: Do  not chain the bios as we do not get notification
 * of what extent reading did not work! We can retry and if
 * the block did not work, we can do something more meaningful.
 */
static int read_gc_extents(struct ctx *ctx)
{
	struct list_head *list_head;
	struct gc_extents *gc_extent, *last_extent;
	int count;
	refcount_t * ref;

	ref = kzalloc(sizeof(refcount_t), GFP_KERNEL);
	if (!ref) {
		return -ENOMEM;
	}

	refcount_set(ref, 1);


	/* If list is empty we have nothing to do */
	BUG_ON(list_empty(&ctx->gc_extents->list));

	count = 0;
	/* setup the bio for the first gc_extent */
	list_for_each(list_head, &ctx->gc_extents->list) {
		count++;
		gc_extent = list_entry(list_head, struct gc_extents, list);
		printk(KERN_ERR "\n %s lba: %llu, pba: %llu, len:%d ", gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
		if (setup_extent_bio(ctx, gc_extent)) {
			free_gc_list(ctx);
			panic("Low memory! TODO: Write code to free memory from translation tables etc ");
		}
		gc_extent->bio->bi_private = ref;
	}
	printk(KERN_ERR "\n %s count: %d", __func__, count);
	last_extent = gc_extent;
	read_all_bios_and_wait(ctx, last_extent);
	printk(KERN_ERR "\n %s Read all the pbas in zone for gc! Done ", __func__);
	return 0;
}

/*
 * We sort on the LBA
 */
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
	struct gc_ctx *gc_ctx;
	sector_t nrsectors, s8;
	int trials = 0;

	bio = gc_extent->bio;
	gc_ctx = bio->bi_private;

	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = ctx->gc_write_frontier;
	nrsectors = bio_sectors(bio);
	s8 = round_up(nrsectors, NR_SECTORS_IN_BLK);
	BUG_ON (nrsectors != s8);
again:
	submit_bio_wait(bio);
	if (bio->bi_status != BLK_STS_OK) {
		if (bio->bi_status ==  BLK_STS_IOERR) {
			/* For now we are not doing anything else
			 * We should actually be retrying depending on weather
			 * its a disk error or a memory error
			 */
			mark_disk_full(ctx);
			//trace_printk("\n write end io status not OK");
			return -1;
		}
		if (bio->bi_status == BLK_STS_AGAIN) {
			trials++;
			if (trials < 3)
				goto again;
		}
		panic("GC writes failed! Perhaps a resource error");
	}
	return 0;
}

static void add_revmap_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned int nrsectors);
static void do_checkpoint(struct ctx *ctx);
static void mark_zone_free(struct ctx *ctx , int zonenr);

/*
 * TODO: write code for FG_GC
 *
 * if err_flag is set, we have to mark the zone_to_clean erroneous.
 * This is set in the lsdm_clone_endio() path when a block cannot be
 * written to some zone because of disk error. We need to read the
 * remaining blocks from this zone and write to the destn_zone.
 *
 */
static int lsdm_gc(struct ctx *ctx, int zone_to_clean, char gc_flag, int err_flag)
{
	int zonenr;
	sector_t pba, last_pba;
	struct extent *e = NULL;
	struct extent_entry temp;
	struct gc_extents *gc_extent, *new, *temp_ptr;
	struct list_head *list_head;
	sector_t diff;
	sector_t nr_sectors, s8;
	struct bio *split, *bio;
	struct rb_node *node;
	int ret;
	static int count = 0;

	printk(KERN_ERR "\n GC thread polling after every few seconds, zone_to_clean: %u", zone_to_clean);
	
	if (zone_to_clean < 0) {
		printk(KERN_ERR "\n Nothing to clean! ");
		return -1;
	}


	/* Take a semaphore lock so that no two gc instances are
	 * started in parallel.
	 */
	if (!down_trylock(&ctx->gc_lock)) {
		printk(KERN_ERR "\n GC is already running! zone_to_clean: %u count: %d", zone_to_clean, count);
		dump_stack();
		return -1;
	} 
	count = 1;
	printk(KERN_ERR "\n Running GC!! zone_to_clean: %u count: %d ", zone_to_clean, count);
	BUG_ON(!list_empty(&ctx->gc_extents->list));
	INIT_LIST_HEAD(&ctx->gc_extents->list);

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

	printk(KERN_ERR "\n %s first_pba: %llu last_pba: %llu", __func__, pba, last_pba);
	
	down_write(&ctx->metadata_update_lock);
	while(pba <= last_pba) {
		printk(KERN_ERR "\n %s Looking for pba: %llu" , __func__, pba);
		e = revmap_rb_search_geq(ctx, pba);
		if (NULL == e) {
			printk(KERN_ERR "\n Found NULL! ");
			break;
		}
		printk(KERN_ERR "\n %s Found e, PBA: %llu, LBA: %llu, len: %llu", __func__, e->lba, e->pba, e->len);
		/* In a reverse table extent, e->lba is actually the
		 * PBA and e->pba is actually the LBA. This was done
		 * for code reusability.
		 * We are storing the actual lba and pba pair in e
		 * Don't change e directly, as e belongs to the
		 * reverse map rb tree and we have the node address
		 */
		if (e->lba > last_pba)
			break;
		temp.pba = pba;
		if (e->lba < pba) {
			/* 
			 * Overlapping e found
			 * e-------
			 * 	pba
			 */

			diff = pba - e->lba;
			/* Bug when e is non overlapping */
			BUG_ON(diff >= e->len);
			temp.pba = pba;
			temp.lba = e->pba + diff;
			temp.len = e->len - diff;
		} else {
			/*
			 * 	e------
			 * pba
			 */
			temp.pba = e->lba;
			temp.lba = e->pba;
			temp.len = e->len;
			/* if start is 0, len is 4, then you
			 * want to read 4 sectors.
			 * If last_pba is 3, you want len to be 4
			 */
			if (temp.pba + temp.len > last_pba + 1) {
				diff = (temp.pba + temp.len - 1) - last_pba;
				temp.len = temp.len - diff;
			}
		}
		printk(KERN_ERR "\n %s pba: %llu, lba: %llu e->len: %d last_pba: %llu", __func__, temp.pba, temp.lba, temp.len, last_pba);
		add_extent_to_gclist(ctx, &temp);
		pba = temp.pba + temp.len;
	}
	up_write(&ctx->metadata_update_lock);
	/* This segment has valid extents, thus there should be
	 * atleast one node in the gc list. An empty list indicates
	 * the reverse translation map does not match with the segment
	 * information table
	 */
	BUG_ON(list_empty(&ctx->gc_extents->list));
	/* Sort the list on LBAs. Write increasing LBAs in the same
	 * order
	 */
	read_gc_extents(ctx);

	/* Since our read_extents call, overwrites could have made
	 * the blocks in this zone invalid. Thus we now take a 
	 * write lock and then re-read the extents metadata; else we
	 * will end up writing invalid blocks and loosing the
	 * overwritten data
	 */
	down_write(&ctx->metadata_update_lock);
	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		/* Reverse map stores PBA as e->lba and LBA as e->pba
		 * This was done for code reuse between map and revmap
		 * Thus e->lba is actually the PBA
		 */
		e = revmap_rb_search_geq(ctx, gc_extent->e.pba);
		/* entire extrents is lost by interim overwrites */
		if (e->lba > (gc_extent->e.pba + gc_extent->e.len)) {
			printk(KERN_ERR "\n %s:%d entire extent is lost! \n", __func__, __LINE__);
			temp_ptr = list_next_entry(gc_extent, list);
			list_head = &temp_ptr->list;
			list_del(&gc_extent->list);
			kmem_cache_free(ctx->gc_extents_cache, gc_extent);
			continue;
		}
		/* extents are partially snipped */
		if (e->lba > gc_extent->e.pba) {
			printk(KERN_ERR "\n %s:%d extent is snipped! \n", __func__, __LINE__);
			/* e->pba is actually the stored lba */
			gc_extent->e.lba = e->pba;
			diff = gc_extent->e.len - e->len;
			gc_extent->e.len = e->len;
			/* bio advance will advance the bi_sector and bi_size
		 	 */
			bio_advance(gc_extent->bio, diff << 9);
		}
		/* Now we adjust the gc_extent such that it can be
		 * written in the available space in the gc segment.
		 * If less space is available than is required by a
		 * bio, we split that bio. Else we write it as it is.
		 *
		 * We also note the new PBA which is the PBA where the
		 * contents of the cleaned segment should be
		 * transferred to! 
		 */
		nr_sectors = bio_sectors(gc_extent->bio);
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		BUG_ON(nr_sectors != s8);
		if (s8 > ctx->free_sectors_in_gc_wf){
			//trace_printk("SPLITTING!!!!!!!! s8: %d ctx->free_sectors_in_wf: %d", s8, ctx->free_sectors_in_wf);
			s8 = round_down(ctx->free_sectors_in_gc_wf, NR_SECTORS_IN_BLK);
			if (s8 <= 0) {
				panic("Should always have atleast a block left ");
			}
			if (!(split = bio_split(gc_extent->bio, s8, GFP_NOIO, ctx->bs))){
				printk(KERN_ERR "\n %s:%d failed at bio_split! GC failed!! ", __func__, __LINE__);
				panic("\n Low memory, cannot split bio!");
				goto done1;
			}
			gc_extent->e.len = s8;
			gc_extent->bio = split;
			new = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
			if (unlikely(!new)) {
				panic("No memory, couldnt split! write better code!");
			}
			diff = nr_sectors - s8;
			/* This is the old pba that we use for
			 * searching */
			new->e.pba = gc_extent->e.pba + s8;
			new->e.lba = gc_extent->e.lba + s8;
			new->e.len = diff;
			/* Add new after gc_extent */
			list_add(&new->list, &gc_extent->list);
		}
		// else we do nothing
		gc_extent->e.pba = ctx->gc_write_frontier;
		move_gc_write_frontier(ctx, s8);
		write_gc_extent(ctx, gc_extent);
		if (gc_extent->bio->bi_status != BLK_STS_OK) {
			/* write error! disk is in a read mode! we
			 * cannot perform any further GC
			 */
			up_write(&ctx->metadata_update_lock);
			goto done;
		}
		ret = lsdm_update_range(ctx, &ctx->extent_tbl_root, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
		ret = lsdm_update_range(ctx, &ctx->rev_tbl_root, gc_extent->e.pba, gc_extent->e.lba, gc_extent->e.len);
		add_revmap_entries(ctx, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
		temp_ptr = list_next_entry(gc_extent, list);
		list_head = &temp_ptr->list;
		list_del(&gc_extent->list);
		kmem_cache_free(ctx->gc_extents_cache, gc_extent);
	}
	up_write(&ctx->metadata_update_lock);
	do_checkpoint(ctx);
	/* Complete the GC and then sync the block device */
	sync_blockdev(ctx->dev->bdev);
	/* Free the zone */
	mark_zone_free(ctx , zonenr);
	/* TODO: Mode: FG_GC */
done1:
	//printk(KERN_ERR "\n %s ctx:%llu, &ctx->gc_lock:%llu ", ctx, &ctx->gc_lock);
	node = rb_first(&ctx->gc_rb_root);
	if (node) {
		rb_erase(node, &ctx->gc_rb_root);
		kmem_cache_free(ctx->gc_rb_node_cache, node);
	}
	printk(KERN_ERR "\n %s removed zone from the gc_rb_root tree", __func__);
/* Remove the zone from the gc_rb_root */

done:
	/* Release GC lock */
	up(&ctx->gc_lock);
	printk(KERN_ERR "\n %s going out! \n", __func__);
	return 0;
}
/*

	mark_zone_free(ctx , zonenr);
	up(&ctx->gc_lock);
	return (0);
*/


/*
 * TODO: When we want to mark a zone free create a bio with opf:
 * REQ_OP_ZONE_RESET
 *
 *
 */
static int gc_thread_fn(void * data)
{

	struct ctx *ctx = (struct ctx *) data;
	struct lsdm_gc_thread *gc_th = ctx->gc_th;
	wait_queue_head_t *wq = &gc_th->lsdm_gc_wait_queue;
	unsigned int wait_ms;
	int zonenr, newzone;

	wait_ms = gc_th->min_sleep_time;
	sema_init(&ctx->gc_lock, 1);
	printk(KERN_ERR "\n %s executing! ", __func__);
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
                if (kthread_should_stop()) {
			printk(KERN_ERR "\n GC thread is stopping! ");
                        break;
		}
		/* take some lock here as you will start the GC 
		 * For urgent mode mutex_lock()
		 * and for idle time GC mode mutex_trylock()
		 * You need some check for is_idle()
		 * */
		
		//if(!is_lsdm_ioidle(ctx)) {
			/* increase sleep time */
		//	wait_ms = wait_ms * 2;
			/* unlock mutex */
		//	continue;
		//}
		zonenr = select_zone_to_clean(ctx, BG_GC);
		printk(KERN_ERR "\n Selecting zonenr: %d ", zonenr);
		/* Doing this for now! ret part */
		if (zonenr >= 0) {
			lsdm_gc(ctx, zonenr, newzone, 0);
		} /*else {
		    wait_ms = wait_ms* 2;
		    continue;
		} */
	} while(!kthread_should_stop());
	return 0;
}

#define DEF_GC_THREAD_URGENT_SLEEP_TIME 500     /* 500 ms */
#define DEF_GC_THREAD_MIN_SLEEP_TIME    30000   /* milliseconds */
#define DEF_GC_THREAD_MAX_SLEEP_TIME    60000
#define DEF_GC_THREAD_NOGC_SLEEP_TIME   120000  /* wait 2 min */
#define LIMIT_INVALID_BLOCK     40 /* percentage over total user space */
#define LIMIT_FREE_BLOCK        40 /* percentage over invalid + free space */


/* 
 * On error returns 0
 *
 */
int lsdm_gc_thread_start(struct ctx *ctx)
{
	struct lsdm_gc_thread *gc_th;
	dev_t dev = ctx->dev->bdev->bd_dev;
	int err=0;

	printk(KERN_ERR "\n About to start GC thread");

	gc_th = lsdm_malloc(sizeof(struct lsdm_gc_thread), GFP_KERNEL);
	if (!gc_th) {
		return -ENOMEM;
	}

	gc_th->urgent_sleep_time = DEF_GC_THREAD_URGENT_SLEEP_TIME;
        gc_th->min_sleep_time = DEF_GC_THREAD_MIN_SLEEP_TIME;
        gc_th->max_sleep_time = DEF_GC_THREAD_MAX_SLEEP_TIME;
        gc_th->no_gc_sleep_time = DEF_GC_THREAD_NOGC_SLEEP_TIME;

        gc_th->gc_wake= 0;

        ctx->gc_th = gc_th;
	init_waitqueue_head(&gc_th->lsdm_gc_wait_queue);
	ctx->gc_th->lsdm_gc_task = kthread_run(gc_thread_fn, ctx,
			"lsdm-gc%u:%u", MAJOR(dev), MINOR(dev));

	if (IS_ERR(gc_th->lsdm_gc_task)) {
		err = PTR_ERR(gc_th->lsdm_gc_task);
		kvfree(gc_th);
		ctx->gc_th = NULL;
		return err;
	}

	printk(KERN_ERR "\n Created a STL GC thread ");
	return 0;	
}

int lsdm_gc_thread_stop(struct ctx *ctx)
{
	kthread_stop(ctx->gc_th->lsdm_gc_task);
	kvfree(ctx->gc_th);
	printk(KERN_ERR "\n GC thread stopped! ");
	return 0;
}

/* When we find that the system is idle for a long time,
 * then we invoke the lsdm_gc in a background mode
 */

void invoke_gc(unsigned long ptr)
{
	struct ctx *ctx = (struct ctx *) ptr;
	wake_up_process(ctx->gc_th->lsdm_gc_task);
}


void lsdm_is_ioidle(struct kref *kref)
{
	struct ctx *ctx;

	ctx = container_of(kref, struct ctx, ongoing_iocount);
	atomic_set(&ctx->ioidle, 1);
	//trace_printk("\n Stl is io idle!");
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

void lsdm_subread_done(struct bio *clone)
{
	struct app_read_ctx *read_ctx = clone->bi_private;
	struct ctx *ctx = read_ctx->ctx;
	struct bio * bio = read_ctx->bio;

	bio = read_ctx->bio;

	bio_endio(bio);
	//kref_put(&ctx->ongoing_iocount, lsdm_is_ioidle);
	bio_put(clone);
	kmem_cache_free(ctx->app_read_ctx_cache, read_ctx);
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
static int lsdm_read_io(struct ctx *ctx, struct bio *bio)
{
	struct bio *split = NULL;
	sector_t lba, pba;
	struct extent *e;
	unsigned nr_sectors, overlap;

	struct app_read_ctx *read_ctx;
	struct bio *clone;
	int print = 0;

	read_ctx = kmem_cache_alloc(ctx->app_read_ctx_cache, GFP_KERNEL);
	if (!read_ctx) {
		printk(KERN_ERR "\n %s could not allocate memory to read_ctx ", __func__);
		bio->bi_status = -ENOMEM;
		bio_endio(bio);
		return -ENOMEM;
	}

	atomic_inc(&ctx->n_reads);
	clone = bio_clone_fast(bio, GFP_KERNEL, NULL);
	if (!clone) {
		printk(KERN_ERR "\n %s could not allocate memory to clone", __func__);
		kmem_cache_free(ctx->app_read_ctx_cache, read_ctx);
		bio->bi_status = -ENOMEM;
		bio_endio(bio);
		return -ENOMEM;
	}

	read_ctx->ctx = ctx;
	read_ctx->bio = bio;


	nr_sectors = bio_sectors(clone);

	split = NULL;
print_repeat:
	lba = clone->bi_iter.bi_sector;
	//printk(KERN_ERR "\n %s lba: %llu, nrsectors: %lu", __func__, bio->bi_iter.bi_sector, nr_sectors);
	e = lsdm_rb_geq(ctx, lba, print);
	while(split != clone) {
		nr_sectors = bio_sectors(clone);
		lba = clone->bi_iter.bi_sector;

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
		if ((e == NULL) || (e->lba >= lba + nr_sectors) || (e->lba + e->len == lba))  {
			//printk(KERN_ERR "\n %s did not find a matching extent, e: %s , so zero filling! ", __func__, ((e != NULL) ?  "not null" : "null"));
			/*
			if (e) {
				printk(KERN_ERR "\n e is not null,  e->lba: %llu, e->pba: %llu, e->len: %lu \n", e->lba, e->pba, e->len);
			}*/
			zero_fill_bio(clone);
			kref_get(&ctx->ongoing_iocount);
			clone->bi_private = read_ctx;
			clone->bi_end_io = lsdm_subread_done;
			read_ctx->clone = clone;
			/* This bio could be the parent of other
			 * chained bios. Its necessary to call
			 * bio_endio and not the endio function
			 * directly
			 */
			bio_endio(clone);
			break;
		}
		//printk(KERN_ERR "\n %s Searching: %llu len: %lu. Found e->lba: %llu, e->pba: %llu, e->len: %lu", __func__, lba, nr_sectors, e->lba, e->pba, e->len);

		if (e->lba > lba) {
		/*               [eeeeeeeeeeee]
			       [---------bio------] */
			nr_sectors = e->lba - lba;
			//printk(KERN_ERR "\n 1.  %s e->lba: %llu >  lba: %llu split_sectors: %d \n", __func__, e->lba, lba, nr_sectors);
			split = bio_split(clone, nr_sectors, GFP_NOIO, ctx->bs);
			if (!split) {
				printk(KERN_ERR "\n Could not split the clone! ERR ");
				/* other bios could have
				 * been chained to clone. Hence we
				 *  should be calling bio_endio(clone)
				 */
				clone->bi_status = BLK_STS_RESOURCE;
				kref_get(&ctx->ongoing_iocount);
				bio->bi_status = -ENOMEM;
				clone->bi_private = read_ctx;
				clone->bi_end_io = lsdm_subread_done;
				read_ctx->clone = clone;
				bio_endio(clone);
				return -ENOMEM;
			}
			bio_chain(split, clone);
			zero_fill_bio(split);
			bio_endio(split);
			/* bio is front filled with zeroes, but we
			 * need to compare now with the same e
			 */
			continue;
		}
		else { //(e->lba <= lba)
		/* [eeeeeeeeeeee] eeeeeeeeeeeee]<- could be shorter or longer
		     [---------bio------] */
			overlap = e->lba + e->len - lba;
			if (overlap < nr_sectors) {
				if (overlap == 0) {
					printk(KERN_ERR "\n **** %s e->lba <= lba  e->lba: %llu e->len: %lu lba: %llu overlap: %d nr_sectors: %d \n", __func__, e->lba, e->len, lba, overlap, nr_sectors);
					panic("\n Overlap cannot be zero! ");
				}

				split = bio_split(clone, overlap, GFP_NOIO, ctx->bs);
				if (!split) {
					printk(KERN_INFO "\n Could not split the clone! ERR ");
					/* other bios could have
					 * been chained to clone. Hence we
					 *  should be calling bio_endio(clone)
					 */
					clone->bi_status = BLK_STS_RESOURCE;
					kref_get(&ctx->ongoing_iocount);
					bio->bi_status = -ENOMEM;
					clone->bi_private = read_ctx;
					clone->bi_end_io = lsdm_subread_done;
					read_ctx->clone = clone;
					bio_endio(clone);
					return -ENOMEM;
				}
				bio_chain(split, clone);

				pba = e->pba + lba - e->lba;
				split->bi_iter.bi_sector = pba;
				bio_set_dev(split, ctx->dev->bdev);
			//	printk(KERN_ERR "\n 2. (SPLIT) %s lba: %llu, pba: %llu nr_sectors: %d e->lba: %llu e->pba: %llu e->len:%llu ", __func__, lba, pba, bio_sectors(split), e->lba, e->pba, e->len);
				submit_bio(split);
				/* Search for the next lba, as this
				 * was processed */
				lba = lba + e->len;
				e = lsdm_rb_geq(ctx, lba, print);
				continue;
			} 
			//else overlap == e->len
			/* All the previous splits are chained
			 * to this last one. No more bios will
			 * be submitted once this is
			 * submitted
			 */
			atomic_set(&ctx->ioidle, 0);
			split = clone;
			kref_get(&ctx->ongoing_iocount);
			split->bi_private = read_ctx;
			split->bi_end_io = lsdm_subread_done;
			read_ctx->clone = clone;

			split->bi_iter.bi_sector = e->pba;
			bio_set_dev(split, ctx->dev->bdev);
			//printk(KERN_ERR "\n 3* (FINAL) %s lba: %llu, nr_sectors: %d e->lba: %llu e->pba: %llu e->len:%llu ", __func__, lba, bio_sectors(split), e->lba, e->pba, e->len);
			submit_bio(split);
			break;
		}
	}
	//printk(KERN_INFO "\n %s end", __func__);
	return 0;
}


void copy_blocks(u64 zonenr, u64 destn_zonenr) 
{
	return;
}

void lsdm_init_zone(struct ctx *ctx, u64 zonenr)
{
	return;
}


void lsdm_init_zones(struct ctx *ctx)
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
                        ret = lsdm_init_zone(zmd, zone, &blkz[i]);
                        if (ret)
                                goto out;
                        sector += dev->zone_nr_sectors;
                        zone++;
                }
        }*/
}


int lsdm_zone_seq(struct ctx * ctx, sector_t pba)
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
	//trace_printk("\n Error in bio, removing partial entries! lba: %llu", bio->bi_iter.bi_sector);
	dump_stack();
}

void mark_disk_full(struct ctx *ctx)
{
	struct lsdm_ckpt *ckpt = ctx->ckpt;
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
	//trace_printk("\n %s pba: %llu zonenr: %lld blknr: %lld", __func__, pba, zonenr, blknr);

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


struct sit_page * add_sit_page_kv_store(struct ctx * ctx, sector_t pba);

/*
 * When a segentry says that all blocks are full,
 * but the mtime is 0, then the zone is erroneous.
 * Such a zone will never be the destination of 
 * GC
 */
void mark_zone_erroneous(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct lsdm_seg_entry *ptr;
	int index;
	int zonenr, destn_zonenr;

	down_interruptible(&ctx->sit_kv_store_lock);
	/*-----------------------------------------------*/
	sit_page = add_sit_page_kv_store(ctx, pba);
	if (!sit_page) {
		/* TODO: do something, low memory */
		panic("Low memory, couldnt allocate sit entry");
	}
	/*-----------------------------------------------*/
	up(&ctx->sit_kv_store_lock);

	spin_lock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	set_bit(PG_dirty, &sit_page->page->flags);
	ptr = (struct lsdm_seg_entry *)sit_page->page;
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = BLOCKS_IN_ZONE;
	ptr->mtime = 0;
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
static void mark_zone_occupied(struct ctx *, int );

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
		printk(KERN_ERR "\n Could not find a clean zone for writing. Calling lsdm_gc on zonenr: %llu", toclean);
		lsdm_gc(ctx, toclean, FG_GC, 0);
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
	printk(KERN_ERR "\n !!!!!!!!!!!!!!! get_new_zone():: zone0_pba: %u zone_nr: %d app_write_frontier: %llu, wf_end: %llu", ctx->sb->zone0_pba, zone_nr, ctx->app_write_frontier, ctx->app_wf_end);
	if (ctx->app_write_frontier > ctx->app_wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %lld", ctx->free_sectors_in_wf );
	}
	ctx->free_sectors_in_wf = ctx->app_wf_end - ctx->app_write_frontier + 1;
	ctx->nr_freezones--;
	//printk(KERN_INFO "Num of free sect.: %llu, diff of end and wf:%llu\n", ctx->free_sectors_in_wf, ctx->app_wf_end - ctx->app_write_frontier);
	mark_zone_occupied(ctx, zone_nr);
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
	printk("\n !!!!!!!!!!!!!!! get_new_gc_zone():: zone0_pba: %u zone_nr: %ld gc_write_frontier: %llu, gc_wf_end: %llu", ctx->sb->zone0_pba, zone_nr, ctx->gc_write_frontier, ctx->gc_wf_end);
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
	struct lsdm_ckpt *ckpt = ctx->ckpt;
	ckpt->cur_frontier_pba = wf;
}

static void add_ckpt_new_gc_wf(struct ctx * ctx, sector_t wf)
{
	struct lsdm_ckpt *ckpt = ctx->ckpt;
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
	//trace_printk("\n %s done \n", __func__);

	if (BLK_STS_OK != bio->bi_status) {
		/* TODO: Do something more to handle the errors */
		printk(KERN_DEBUG "\n Could not write the checkpoint to disk");
	}
	return;
}

void revmap_bitmap_flushed(struct bio * bio)
{
	struct ctx *ctx = bio->bi_private;

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
			//trace_printk("\n Could not flush revmap bitmap");
			panic("IO error while flushing revmap block! Handle this better");
			break;
	}
	__free_pages(ctx->revmap_bm, 0);
}
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
	//trace_printk("\n %s flushing revmap bitmap at pba: %u", __func__, ctx->sb->revmap_bm_pba);
	pba = ctx->sb->revmap_bm_pba;
	bio->bi_private = ctx;
	bio->bi_iter.bi_sector = pba;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_end_io = revmap_bitmap_flushed;
	submit_bio_wait(bio);
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
	struct lsdm_ckpt * ckpt;

	page = ctx->ckpt_page;
	if (!page)
		return;

	ckpt = (struct lsdm_ckpt *)page_address(page);
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
	if (ctx->ckpt_pba == ctx->sb->ckpt1_pba) {
		ctx->ckpt_pba = ctx->sb->ckpt2_pba;
		printk(KERN_ERR "\n Updating the second checkpoint! pba: %lld", ctx->ckpt_pba);
	}
	else {
		ctx->ckpt_pba = ctx->sb->ckpt1_pba;
		printk(KERN_ERR "\n Updating the first checkpoint! pba: %lld", ctx->ckpt_pba);
	}
	pba = ctx->ckpt_pba;
	bio->bi_end_io = ckpt_flushed;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	//printk(KERN_ERR "\n flushing checkpoint at pba: %llu", pba);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_private = ctx;
	submit_bio_wait(bio);
	return;
}


void flush_sit_node_page(struct ctx * ctx, struct rb_node *);


void flush_sit_nodes(struct ctx *ctx, struct rb_node *node)
{	
	if (!node)
		return;
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
void flush_sit(void * data, async_cookie_t cookie)
{
	struct ctx *ctx = (struct ctx *) data;
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

/*
 *
 */
void remove_gc_rb_nodes(struct ctx *ctx, struct rb_node *node)
{
	struct gc_rb_node *e = NULL;

	printk(KERN_ERR "\n Inside %s ", __func__);
	if (!node) {
		printk(KERN_ERR "\n %s node is null! ", __func__);
		return;
	}

	if (node->rb_left)
		remove_gc_rb_nodes(ctx, node->rb_left);
	if (node->rb_right)
		remove_gc_rb_nodes(ctx, node->rb_right);
	rb_erase(node, &ctx->gc_rb_root);
	e = container_of(node, struct gc_rb_node, rb);
	printk(KERN_ERR "\n segment nr: %u, nrblks: %u cost: %u", e->zonenr, e->nrblks, e->cb_cost);
	kmem_cache_free(ctx->gc_rb_node_cache, node);
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
	struct lsdm_ckpt * ckpt;

	page = ctx->ckpt_page;
	if (!page) {
		//trace_printk("\n NO checkpoint page found! ");
		return;
	}
	ckpt = (struct lsdm_ckpt *) page_address(page);
	//trace_printk("\n Inside %s ckpt: %p", __func__, ckpt);
	if (ckpt->clean == 1) {
		//trace_printk("\n ckpt has not changed since last flush!");
		return;
	}
	ckpt->user_block_count = ctx->user_block_count;
	ckpt->version += 1;
	//printk(KERN_ERR "\n %s ckpt->user_block_count = %lld version: %d ", __func__, ctx->user_block_count, ckpt->version);
	ckpt->nr_invalid_zones = ctx->nr_invalid_zones;
	ckpt->cur_frontier_pba = ctx->app_write_frontier;
	//trace_printk("\n %s, ckpt->cur_frontier_pba: %llu version: %lld", __func__, ckpt->cur_frontier_pba, ckpt->version);
	ckpt->cur_gc_frontier_pba = ctx->gc_write_frontier;
	ckpt->nr_free_zones = ctx->nr_freezones;
	ckpt->elapsed_time = get_elapsed_time(ctx);
	ckpt->clean = 0;
	return;
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
		//printk(KERN_INFO "Num of free sect.: %llu, about to call get_new_zone() \n", ctx->free_sectors_in_wf);
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
			printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->gc_write_frontier, ctx->gc_wf_end);
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
		//trace_printk("\n waiting on block barrier, nr_pending_writes: %d", atomic_read(&ctx->nr_pending_writes));
		wait_event_lock_irq(ctx->rev_blk_flushq, 0 >= atomic_read(&ctx->nr_pending_writes), ctx->rev_flush_lock);
	}
	spin_unlock(&ctx->rev_flush_lock);
}

int is_revmap_block_available(struct ctx *ctx, u64 pba);
void flush_translation_blocks(void *, async_cookie_t);
void flush_tm_nodes(struct rb_node *node, struct ctx *ctx);

void wait_on_revmap_block_availability(struct ctx *ctx, u64 pba)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = NULL;
	spin_lock(&ctx->rev_flush_lock);
	if (1 == is_revmap_block_available(ctx, pba)) {
		spin_unlock(&ctx->rev_flush_lock);
		return;
	}
	spin_unlock(&ctx->rev_flush_lock);
/* You could wait here for ever if the translation blocks are not
 * flushed!
 */
	//printk(KERN_ERR "\n About to flush translation blocks! before waiting on revmap blk availability!!!! \n");

	node = root->rb_node;
	if (node) {
		flush_tm_nodes(node, ctx);
	}

	//spin_lock(&ctx->rev_flush_lock);
	/* wait until atleast one zone's revmap entries are flushed */
	//wait_event_lock_irq(ctx->rev_blk_flushq, (1 == is_revmap_block_available(ctx, pba)), ctx->rev_flush_lock);
	//spin_unlock(&ctx->rev_flush_lock);
}

void remove_sit_blk_kv_store(void * data, async_cookie_t cookie)
{
	struct sit_page_write_ctx * sit_page_write_ctx = (struct sit_page_write_ctx *) data;
	struct ctx *ctx = sit_page_write_ctx->ctx;
	struct page * page = sit_page_write_ctx->page;
       	u64 sit_nr = sit_page_write_ctx->sit_nr;
	struct rb_root *rb_root = &ctx->sit_rb_root;
	struct sit_page *new;

	down_interruptible(&ctx->sit_kv_store_lock);
	/*--------------------------------------------*/
	new = search_sit_blk(ctx, sit_nr);
	if (!new) {
		up(&ctx->sit_kv_store_lock);
		return;
	}

	spin_lock(&ctx->sit_flush_lock);
	if (!test_bit(PG_dirty, &page->flags)) {
		rb_erase(&new->rb, rb_root);
		__free_pages(page, 0);
		atomic_dec(&ctx->nr_sit_pages);
	}
	spin_unlock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	up(&ctx->sit_kv_store_lock);
}


struct page * read_block(struct ctx *ctx, u64 blknr, u64 base);
/*
 * pba: stored in the LBA - PBA translation.
 * This is the PBA of some data block. This PBA belongs to some zone.
 * We are about to update the SIT entry that belongs to that zone
 * Before that we read that corresponding page in memory and then
 * add it to our RB tree that we use for searching.
 *
 */
struct sit_page * add_sit_page_kv_store(struct ctx * ctx, sector_t pba)
{
	sector_t sit_blknr;
	u64 zonenr = get_zone_nr(ctx, pba);
	struct rb_root *root = &ctx->sit_rb_root;
	struct rb_node *parent = NULL;
	struct sit_page *parent_ent;
	struct sit_page *new;
	struct page *page;
	struct lsdm_seg_entry *ptr;

	new = search_sit_kv_store(ctx, pba, &parent);
	if (new)
		return new;

	new = kmem_cache_alloc(ctx->sit_page_cache, GFP_KERNEL);
	if (!new)
		return NULL;

	RB_CLEAR_NODE(&new->rb);
	sit_blknr = zonenr / SIT_ENTRIES_BLK;
	//printk("\n %s sit_blknr: %lld sit_pba: %u", __func__, sit_blknr, ctx->sb->sit_pba);
	page = read_block(ctx, sit_blknr, ctx->sb->sit_pba);
	if (!page) {
		kmem_cache_free(ctx->sit_page_cache, new);
		return NULL;
	}
	//trace_printk("\n %s zonenr: %lld page: %p ", __func__, zonenr, page_address(page));
	new->blknr = sit_blknr;
	new->page = page;
	ptr = (struct lsdm_seg_entry *) page_address(page);
	//printk(KERN_ERR "\n %s ptr->vblocks: %lu ", __func__, ptr->vblocks);
	clear_bit(PG_dirty, &page->flags);
	if (parent) {
		/* Add this page to a RB tree based KV store.
		 * Key is: blknr for this corresponding block
		 */
		parent_ent = rb_entry(parent, struct sit_page, rb);
		if (new->blknr < parent_ent->blknr) {
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
		atomic_set(&ctx->sit_flush_count, 0);
		up(&ctx->sit_kv_store_lock);
		async_schedule(flush_sit, ctx);
		down_interruptible(&ctx->sit_kv_store_lock);
	}

	//trace_printk("\n %s pba: %llu page_address: %p", __func__, pba, page_address(new->page));
	return new;
}

/*TODO: IMPLEMENT 
 */
void mark_segment_free(struct ctx *ctx, sector_t zonenr)
{
	return;
}

int update_gc_rb_tree(struct ctx *, unsigned int , u32 , u64 );
void sit_ent_add_mtime(struct ctx *ctx, sector_t pba);

/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_vblocks_decr(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct lsdm_seg_entry *ptr;
	sector_t zonenr;
	int index;

	down_interruptible(&ctx->sit_kv_store_lock);
	/*--------------------------------------------*/
	sit_page= add_sit_page_kv_store(ctx, pba);
	if (!sit_page) {
	/* TODO: do something, low memory */
		panic("Low memory, could not allocate sit_entry");
	}

	spin_lock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	set_bit(PG_dirty, &sit_page->page->flags);
	/*--------------------------------------------*/
	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = ptr->vblocks - 1;
	spin_unlock(&ctx->sit_flush_lock);
	/* Send the older vblocks, mtime along with the new vblocks,mtime to this
	 * function. The older vblocks, mtime is used to calculated
	 * the older cost which is stored in the tree. The newer ones
	 * on the other hand are used to modify the tree
	 */
	if ((zonenr != get_zone_nr(ctx, ctx->ckpt->cur_frontier_pba)) &&
                    (zonenr != get_zone_nr(ctx, ctx->ckpt->cur_gc_frontier_pba))) {
		// printk(KERN_ERR "\n updating gc rb zonenr: %d nrblks: %d", ptr->vblocks);
		/* add mtime here */
		ptr->mtime = get_elapsed_time(ctx);
		if (ctx->max_mtime < ptr->mtime)
			ctx->max_mtime = ptr->mtime;
		if (ptr->vblocks)
			update_gc_rb_tree(ctx, zonenr, ptr->vblocks, ptr->mtime);

		//printk(KERN_ERR "\n %s done! vblocks: %d pba: %lu, index: %d , ptr: %p", __func__, ptr->vblocks, pba, index, ptr);
	}
	if (!ptr->vblocks) {
		spin_lock(&ctx->lock);
		mark_zone_free(ctx , zonenr);
		spin_unlock(&ctx->lock);
	}
	up(&ctx->sit_kv_store_lock);
}


int read_seg_entries_from_block(struct ctx *ctx, struct lsdm_seg_entry *entry, unsigned int nr_seg_entries, unsigned int *zonenr);

/*
 * pba: from the LBA-PBA pair. Of a data block
 * this function will be always called for the current write frontier
 * or current gc frontier. This zone is already marked as in
 * use/occupied. We do not need to do it everytime a block is added
 * here.
 */
void sit_ent_vblocks_incr(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct lsdm_seg_entry *ptr;
	sector_t zonenr;
	int index;
	static int print = 0;
	long vblocks = 0;
	int temp;

	down_interruptible(&ctx->sit_kv_store_lock);
	sit_page = add_sit_page_kv_store(ctx, pba);
	if (!sit_page) {
		/* TODO: do something, low memory */
		panic("Low memory, could not allocate sit_entry");
	}
	up(&ctx->sit_kv_store_lock);
	spin_lock(&ctx->sit_flush_lock);
	/*--------------------------------------------*/
	set_bit(PG_dirty, &sit_page->page->flags);
	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	//trace_printk("\n %s: pba: %llu zonenr: %llu", __func__, pba, zonenr);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = ptr->vblocks + 1;
	vblocks = ptr->vblocks;
	/*--------------------------------------------*/
	spin_unlock(&ctx->sit_flush_lock);
	if(vblocks > BLOCKS_IN_ZONE) {
		if (!print) {
			printk(KERN_ERR "\n !!!!!!!!!!!!!!!!!!!!! zone: %lu has more than %d blocks! ", zonenr, ptr->vblocks);
			print = 0;
		}
	} /*else {
		printk(KERN_ERR "\n  zone: %lu has %d blocks! pba: %llu index: %u ptr: %p", zonenr, vblocks, pba, index, ptr);
	}*/
	return;
}


/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_add_mtime(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct lsdm_seg_entry *ptr;
	sector_t zonenr;
	int index;

	down_interruptible(&ctx->sit_kv_store_lock);
	sit_page = add_sit_page_kv_store(ctx, pba);
	if (!sit_page) {
		/* TODO: do something, low memory */
		panic("Low memory, could not allocate sit_entry");
	}
	spin_lock(&ctx->sit_flush_lock);
	/*------------------------------------------*/
	set_bit(PG_dirty, &sit_page->page->flags);
	ptr = (struct lsdm_seg_entry*) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	/* Send the older vblocks, mtime along with the new vblocks,mtime to this
	 * function. The older vblocks, mtime is used to calculated
	 * the older cost which is stored in the tree. The newer ones
	 * on the other hand are used to modify the tree
	 * Also dont call this function under sit_flush_lock
	 */
	ptr->mtime = get_elapsed_time(ctx);
	if (ctx->max_mtime < ptr->mtime)
		ctx->max_mtime = ptr->mtime;
	/*--------------------------------------------*/
	spin_unlock(&ctx->sit_flush_lock);
	up(&ctx->sit_kv_store_lock);
}

struct tm_page * search_tm_kv_store(struct ctx *ctx, u64 blknr, struct rb_node **parent);
struct tm_page *add_tm_page_kv_store(struct ctx *ctx, u64 lba);
/*
 * If this length cannot be accomodated in this page
 * search and add another page for this next
 * lba. Remember this translation table will
 * go on the disk and is block based and not 
 * extent based
 *
 * Depending on the location within a page, add the lba.
 */
int add_translation_entry(struct ctx * ctx, struct page *page, unsigned long lba, unsigned long pba, unsigned long len) 
{
	struct tm_entry * ptr;
	int i, index;
	struct tm_page *tm_page;

	set_bit(PG_dirty, &page->flags);
	ptr = (struct tm_entry *) page_address(page);
	index = (lba/NR_SECTORS_IN_BLK);
	index = index  %  TM_ENTRIES_BLK;
	//printk(KERN_ERR "\n %s tm_page address: %p lba: %lu, pba:%lu , len: %ld index: %d", __func__, ptr, lba, pba, len, index);
	ptr = ptr + index;
	//printk(KERN_ERR "\n 1. ptr->lba: %llu, ptr->pba: %llu", ptr->lba, ptr->pba);
	/*-----------------------------------------------*/
	/* lba can be 0, but pba cannot be */
	if (ptr->pba != 0) {
		/* decrement vblocks for the segment that has
		 * the stale block
		 */
		//printk(KERN_ERR "\n Overwrite a block at LBA: %llu, PBA: %llu", ptr->lba, ptr->pba);
		sit_ent_vblocks_decr(ctx, ptr->pba);
	}
	ptr->lba = lba;
	ptr->pba = pba;
	//printk(KERN_ERR "\n 2. ptr->lba: %llu, ptr->pba: %llu", ptr->lba, ptr->pba);
	sit_ent_vblocks_incr(ctx, ptr->pba);
	if (pba == zone_end(ctx, pba)) {
		sit_ent_add_mtime(ctx, ptr->pba);
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
struct page * read_block(struct ctx *ctx, u64 blknr, u64 base)
{
	struct bio * bio;
	struct page *page;

	u64 pba = base + (blknr * NR_SECTORS_IN_BLK);
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
	//printk(KERN_ERR "\n read a block from pba: %llu", pba);
	if (bio->bi_status != BLK_STS_OK) {
		//printk(KERN_DEBUG "\n Could not read the translation entry block");
		bio_free_pages(bio);
		bio_put(bio);
		return NULL;
	}
	/* bio_alloc() hence bio_put() */
	bio_put(bio);
	return page;
}
//
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
		node_ent = rb_entry(node, struct tm_page, rb);
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

/* Called asynchronously and can sleep in down_interruptible.
 * remove the page from RB tree and reduce the total
 * count, through the remove_tm_blk_kv_store()
 */
void remove_tm_blk_kv_store(void * data, async_cookie_t cookie)
{
	struct tm_page_write_ctx *tm_page_write_ctx = (struct tm_page_write_ctx *) data;
	struct rb_node *parent = NULL;
	struct tm_page *new;
	struct ctx *ctx = tm_page_write_ctx->ctx;
	struct tm_page *tm_page = tm_page_write_ctx->tm_page;
	sector_t blknr = tm_page->blknr;
	struct page *page  = tm_page->page;

	down_interruptible(&ctx->tm_kv_store_lock);
	/*----------------------------------------------*/
	new = search_tm_kv_store(ctx, blknr, &parent);
	if (!new) {
		up(&ctx->tm_kv_store_lock);
		return;
	}

	spin_lock(&ctx->tm_flush_lock);
	if (!test_bit(PG_dirty, &page->flags)) {
		rb_erase(&new->rb, &ctx->tm_rb_root);
		kmem_cache_free(ctx->tm_page_cache, new);
		__free_pages(page, 0);
	}
	spin_unlock(&ctx->tm_flush_lock);
	/*----------------------------------------------*/
	up(&ctx->tm_kv_store_lock);
	kmem_cache_free(ctx->tm_page_write_cache, tm_page_write_ctx);
	return;
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
	struct revmap_meta_inmem * revmap_bio_ctx;
	sector_t blknr;
	async_cookie_t cookie;

	cant_sleep();
	tm_page_write_ctx = (struct tm_page_write_ctx *) bio->bi_private;
	ctx = tm_page_write_ctx->ctx;
	tm_page = tm_page_write_ctx->tm_page;
	page = tm_page->page;

	/* We notify all those reverse map pages that are stuck on
	 * these respective refcounts
	 */
	if (bio->bi_status != BLK_STS_OK) {
		/*TODO: Perhaps retry!! or do something more
		 * or else you will loose the translation entries.
		 * write them some place else!
		 */
		//trace_printk("\n %s bi_status: %d \n", __func__, bio->bi_status);
		panic("Could not write the translation entry block");
	}

	/* bio_alloc(), hence bio_put() */
	bio_put(bio);


	/* If the page was locked, then since the page was submitted,
	 * write has been attempted
	 */	
	if (!test_bit(PG_dirty, &page->flags)) {
		async_schedule(remove_tm_blk_kv_store, tm_page_write_ctx);
	} else {
		kmem_cache_free(ctx->tm_page_write_cache, tm_page_write_ctx);
	}
	/*-----------------------------------------------*/
	atomic_dec(&ctx->tm_flush_count);
	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt) {
		atomic_dec(&ctx->ckpt_ref);
	}
	spin_unlock(&ctx->ckpt_lock);
	wake_up(&ctx->ckptq);
	return;
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

	//printk(KERN_ERR "\n %s: 1 tm_page:%p, tm_page->page:%p \n", __func__, tm_page, page_address(tm_page->page));

	/* Only flush if the page needs flushing */

	spin_lock(&ctx->tm_flush_lock);
	/*--------------------------------------*/
	if (!test_bit(PG_dirty, &page->flags)) {
		spin_unlock(&ctx->tm_flush_lock);
		//printk(KERN_INFO "\n TM Page is not dirty!");
		return;
	}
	/*--------------------------------------*/
	spin_unlock(&ctx->tm_flush_lock);
	clear_bit(PG_dirty, &page->flags);

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

	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		kmem_cache_free(ctx->tm_page_write_cache, tm_page_write_ctx);
		printk(KERN_ERR "\n Inside %s 2 - Going.. Bye!! \n", __func__);
		return;
	}
	/* blknr is the relative blknr within the translation blocks
	 */
	pba = (tm_page->blknr * NR_SECTORS_IN_BLK) + ctx->sb->tm_pba;
	//printk(KERN_ERR "\n %s Flushing TM page: %p at pba: %lu", __func__,  page_address(page), pba);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_iter.bi_sector = pba;
	bio->bi_private = tm_page_write_ctx;
	bio->bi_end_io = write_tmbl_complete;
	//spin_lock(&ctx->ckpt_lock);
	/* The next code is related to synchronizing at dtr() time.
	 */
	if(ctx->flag_ckpt) {
		atomic_inc(&ctx->ckpt_ref);
	}
	spin_unlock(&ctx->ckpt_lock);
	submit_bio(bio);
	//printk(KERN_INFO "\n 2. Leaving %s! flushed dirty page! \n", __func__);
	return;
}


void flush_tm_nodes(struct rb_node *node, struct ctx *ctx)
{
	if (!node) {
		printk(KERN_INFO "\nNo tm node found! returning!");
		return;
	}
	flush_tm_node_page(ctx, node);
	if (node->rb_left)
		flush_tm_nodes(node->rb_left, ctx);
	if (node->rb_right)
		flush_tm_nodes(node->rb_right, ctx);
	return;
}

int read_translation_map(struct ctx *);

/* This function waits for all the translation blocks to be flushed to
 * the disk by calling wait_for_ckpt_completion and ckpt_ref
 * The non blocking call is flush_tm_nodes
 */
void flush_translation_blocks(void *data, async_cookie_t cookie)
{
	struct ctx * ctx = (struct ctx *) data;
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = NULL;
	struct blk_plug plug;

	node = root->rb_node;
	if (!node) {
		//trace_printk("\n tm node is NULL!");
		return;
	}

	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	//spin_lock(&ctx->ckpt_lock);
	//ctx->flag_ckpt = 1;
	atomic_set(&ctx->ckpt_ref, 1);
	//spin_unlock(&ctx->ckpt_lock);

	blk_start_plug(&plug);
	//printk(KERN_ERR "\n %s nr_tm_pages: %llu", ctx->nr_tm_pages);
	flush_tm_nodes(node, ctx);
	blk_finish_plug(&plug);	
	atomic_dec(&ctx->ckpt_ref);
	//wait_for_ckpt_completion(ctx);
	ctx->flag_ckpt = 0;
	//printk(KERN_INFO "\n %s done!!", __func__);
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

void remove_sit_blk_kv_store(void * data, async_cookie_t cookie);

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
		bio_put(bio);
		printk(KERN_ERR "\n Could not write the SIT page to disk! ");
		return;
	}
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);
	/* We have stored relative blk address, whereas disk supports
	 * sector addressing
	 * TODO: store the sector address directly 
	 */

	async_schedule(remove_sit_blk_kv_store, sit_ctx);

	atomic_dec(&ctx->sit_flush_count);
}



/* We don't wait for the bios to complete 
 * flushing. We only initiate the flushing
 */
void flush_sit_node_page(struct ctx *ctx, struct rb_node *node)
{
	struct page *page; 
	struct sit_page *sit_page;
	u64 pba;
	struct bio * bio;
	struct sit_page_write_ctx *sit_ctx;

	/* Do not flush if the page is not dirty */

	sit_page = rb_entry(node, struct sit_page, rb);
	if (!sit_page) {
		printk(KERN_ERR "\n sit_page is NULL!");
		return;
	}
	page = sit_page->page;
	if (!page)
		return;

	//trace_printk("\n sit page address: %p ", page_address(page));

	spin_lock(&ctx->sit_flush_lock);
	if (!test_bit(PG_dirty, &page->flags)) {
		spin_unlock(&ctx->sit_flush_lock);
		//printk(KERN_ERR "\n sit_page is not dirty!");
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
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return;
	}

	sit_ctx->ctx = ctx;
	sit_ctx->page = page;
	sit_ctx->sit_nr = (pba - ctx->sb->sit_pba) / NR_SECTORS_IN_BLK;
	bio_set_dev(bio, ctx->dev->bdev);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	/* Sector addressing */
	pba = (sit_page->blknr * NR_SECTORS_IN_BLK) + ctx->sb->sit_pba;
	bio->bi_iter.bi_sector = pba;
	bio->bi_private = sit_ctx;
	bio->bi_end_io = write_sitbl_complete;
	//printk(KERN_ERR "\n flush sit node page at lba: %llu", pba);
	spin_lock(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_inc(&ctx->ckpt_ref);
	spin_unlock(&ctx->ckpt_lock);
	/*--------------------------------------------*/
	clear_bit(PG_dirty, &page->flags);
	/*-------------------------*/
	submit_bio(bio);
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

int read_extents_from_block(struct ctx * ctx, struct tm_entry *entry, unsigned int nr_extents);

/*
 * lba: from the LBA-PBA pair of a data block.
 * Should be called with
 *
 * LBAs are sector addresses. We expect these LBA to be divisible by 8
 * perfectly, ie. they are always the address of the first sector in a
 * block. The LBA of the next block increases by 8.
 * We expect LBAs to be
 *
 *	(&ctx->tm_kv_store_lock);
 *
 * Here blknr: is the relative TM block number that holds the entry
 * against this LBA. 
 */
struct tm_page *add_tm_page_kv_store(struct ctx *ctx, u64 lba)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *parent = NULL;
	struct tm_page *new_tmpage, *parent_ent;
	struct tm_entry *tm_entry;
	/* convert the sector lba to a blknr and then find out the relative
	 * blknr where we find the translation entry from the first
	 * translation block.
	 *
	u64 blknr = (lba / (NR_SECTORS_IN_BLK) / TM_ENTRIES_BLK;
	 */
	u64 blknr = lba / (TM_ENTRIES_BLK * NR_SECTORS_IN_BLK);
	int var;
	
	new_tmpage = search_tm_kv_store(ctx, blknr, &parent);
	if (new_tmpage) {
		return new_tmpage;
	}

	new_tmpage = kmem_cache_alloc(ctx->tm_page_cache, GFP_KERNEL);
	if (!new_tmpage) {
		return NULL;
	}
	
	RB_CLEAR_NODE(&new_tmpage->rb);

	//printk("\n %s blknr: %d tm_pba: %llu", __func__, blknr, ctx->sb->tm_pba);
	new_tmpage->page = read_block(ctx, blknr, ctx->sb->tm_pba);
	if (!new_tmpage->page) {
		printk(KERN_ERR "\n %s read_block  failed! could not allocate page! \n");
		kmem_cache_free(ctx->tm_page_cache, new_tmpage);
		return NULL;
	}
	new_tmpage->blknr = blknr;
	/* We get a reference to this page, so that it is not freed
	 * underneath us. We put the reference in the flush tm page
	 * code, just before freeing it.
	 */
	get_page(new_tmpage->page);
	/* This is a new page, cannot be dirty, dont flush from a
	 * parallel thread! */
	clear_bit(PG_dirty, &new_tmpage->page->flags);

	//printk("\n %s: lba: %lu blknr: %lu  NR_SECTORS_IN_BLK * TM_ENTRIES_BLK: %d \n", __func__, lba, blknr, NR_SECTORS_IN_BLK * TM_ENTRIES_BLK);
	/* Testing */
	tm_entry = (struct tm_entry *) page_address(new_tmpage->page);
	read_extents_from_block(ctx, tm_entry, TM_ENTRIES_BLK);
	/* Until here */

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
		//printk(KERN_ERR "%s parent is not null, added page! ", __func__);
	} else {
		//printk(KERN_ERR "%s parent is NULL, added page! ", __func__);
		rb_link_node(&new_tmpage->rb, parent, &root->rb_node);
	}
	/* Balance the tree after node is addded to it */
	rb_insert_color(&new_tmpage->rb, root);
	atomic_inc(&ctx->nr_tm_pages);
	var = atomic_read(&ctx->nr_tm_pages);
	//printk(KERN_ERR "\n %s nr_tm_pages: %d", __func__, var);
	atomic_inc(&ctx->tm_flush_count);
	if (atomic_read(&ctx->tm_flush_count) >= MAX_TM_PAGES) {
		/*---------------------------------------------*/
		atomic_set(&ctx->tm_flush_count, 0);
		/* Only one flush operation at a time 
		* but we dont want to wait.
		*/
		async_schedule(flush_translation_blocks, ctx);
	}
	return new_tmpage;
}


/* Make the length in terms of sectors or blocks?
 *
 */
int add_block_based_translation(struct ctx *ctx, struct page *page)
{
	
	struct lsdm_revmap_entry_sector * ptr;
	struct tm_page * tm_page = NULL;
	int i, j, len = 0;
	sector_t lba, pba;

	ptr = (struct lsdm_revmap_entry_sector *)page_address(page);
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
			//printk(KERN_ERR "\n Adding TM entry: ptr->extents[j].lba: %llu, ptr->extents[j].pba: %llu, ptr->extents[j].len: %d", lba, pba, len);
			down_interruptible(&ctx->tm_kv_store_lock);
			tm_page = add_tm_page_kv_store(ctx, lba);
			if (!tm_page) {
				up(&ctx->tm_kv_store_lock);
				return -ENOMEM;
			}

			if (len < 8) {
				printk(KERN_ERR "\n len cannot be less than 8! ");
			}
			add_translation_entry(ctx, tm_page->page, lba, pba, len);
			up(&ctx->tm_kv_store_lock);

		}
		ptr = ptr + 1;
		i++;
	}
	//printk(KERN_ERR "\n %s BYE lba: %llu pba: %llu len: %d!", __func__, lba, pba, len);
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
		//trace_printk("\n WRONG PBA!!");
		panic("Bad PBA for revmap block!");
	}
	bytenr = pba/BITS_IN_BYTE;
	bitnr = pba % BITS_IN_BYTE;
	//trace_printk("\n %s pba: %llu bytenr: %d bitnr: %d", __func__, pba, bytenr, bitnr);
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
	//trace_printk("\n %s pba: %llu bytenr: %d bitnr: %d", __func__, pba, bytenr, bitnr);
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

	//trace_printk("\n %s pba: %llu, sb->revmap_pba: %u", __func__, pba, ctx->sb->revmap_pba);
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
	//trace_printk("\n %s relative pba: %llu bytenr: %d bitnr: %d temp: %d", __func__, pba, bytenr, bitnr, temp);

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

/* Waits until the value of refcount becomes 1  */
void wait_on_refcount(struct ctx *ctx, refcount_t *ref)
{
	spin_lock(&ctx->tm_ref_lock);
	wait_event_lock_irq(ctx->refq, (1 == refcount_read(ref)), ctx->tm_ref_lock);
	spin_unlock(&ctx->tm_ref_lock);
}


void process_tm_entries(void *data, async_cookie_t cookie)
{
	struct revmap_bioctx * revmap_bio_ctx = (struct revmap_bioctx *) data;
	struct page *page = revmap_bio_ctx->page;
	struct ctx * ctx = revmap_bio_ctx->ctx;

	kmem_cache_free(ctx->revmap_bioctx_cache, revmap_bio_ctx);
	add_block_based_translation(ctx, page);
	//printk(KERN_ERR "\n %s revmap_bio_ctx->page: %p", __func__,  page_address(page));
	__free_pages(page, 0);
}


void revmap_blk_flushed(struct bio *bio)
{
	struct ctx *ctx;
	sector_t pba;
	struct revmap_bioctx *revmap_bio_ctx = bio->bi_private;
	async_cookie_t cookie;

	ctx = revmap_bio_ctx->ctx;
	pba = revmap_bio_ctx->revmap_pba;

	switch(bio->bi_status) {
		case BLK_STS_OK:
			//printk(KERN_ERR "\n %s done! ", __func__);
			break;
		case BLK_STS_AGAIN:
		default:
			/* TODO: do something better?
			 * remove all the entries from the in memory 
			 * RB tree and the reverse map in memory.
			remove_partial_entries();
			 */
			//trace_printk("\n revmap_entries_flushed ERROR!! \n");
			//panic("revmap block write error, needs better handling!");
			break;
	}
	bio_put(bio);
	/* Wakeup waiters waiting on the block barrier
	 * */
	wake_up(&ctx->rev_blk_flushq);
	atomic_dec(&ctx->nr_pending_writes);

	cookie = async_schedule(process_tm_entries, revmap_bio_ctx);
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
	struct revmap_bioctx *revmap_bio_ctx;

	if (!page)
		return -ENOMEM;

	revmap_bio_ctx = kmem_cache_alloc(ctx->revmap_bioctx_cache, GFP_KERNEL);
	if (!revmap_bio_ctx) {
		return -ENOMEM;
	}

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

	revmap_bio_ctx->ctx = ctx;
	revmap_bio_ctx->page = page;
	revmap_bio_ctx->revmap_pba = ctx->revmap_pba;
	revmap_bio_ctx->retrial = 0;

	bio->bi_iter.bi_sector = ctx->revmap_pba;
	//printk(KERN_ERR "%s Checking if revmap blk at pba:%llu is available for writing! ctx->revmap_pba: %llu", __func__, bio->bi_iter.bi_sector, ctx->revmap_pba);
	
	bio->bi_end_io = revmap_blk_flushed;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_private = revmap_bio_ctx;
	atomic_inc(&ctx->nr_pending_writes);
	//printk(KERN_ERR "\n flushing revmap at pba: %llu", bio->bi_iter.bi_sector);
	
	submit_bio(bio);

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
	//trace_printk("%s waiting on block barrier ", __func__);
	/* We wait for translation entries to be added! */
	//wait_on_block_barrier(ctx);
	//trace_printk("%s revmap entries are on disk, wait is over!", __func__);
}


/*
 *
 * Revmap entries are not ordered. They are entered as and when the
 * write completes, which could be in any order (as it depends on
 * schedule()
 */
static void shrink_next_entries(struct ctx *ctx, sector_t lba, sector_t pba, unsigned long len, struct page *page)
{
	struct lsdm_revmap_entry_sector * ptr = NULL;
	int i = 0, j = 0;
	int entry_nr, sector_nr, max_entries;
	unsigned long diff = 0;

	entry_nr = atomic_read(&ctx->revmap_entry_nr);
	sector_nr = atomic_read(&ctx->revmap_sector_nr);
	ptr = (struct lsdm_revmap_entry_sector *)page_address(page);

	for(i=0; i<=sector_nr; i++) {
		if (i == sector_nr)
			max_entries = entry_nr;
		else
			max_entries = NR_EXT_ENTRIES_PER_SEC;
		/* SECTOR ENTRIES */
		for (j=0; j<max_entries; j++) {
			if((ptr->extents[j].lba >= lba) && (ptr->extents[j].lba < lba + diff)){
				if (ptr->extents[j].len > diff) {
					ptr->extents[j].lba = lba + diff;
					ptr->extents[j].pba = pba + diff;
					ptr->extents[j].len = len - diff;
				} else {
					ptr->extents[j].lba = 0;
					ptr->extents[j].pba = 0;
					ptr->extents[j].len = 0;
					if (ptr->extents[j].len < diff) {
						lba = lba + ptr->extents[j].len;
						pba = pba + ptr->extents[j].len;
						diff = ptr->extents[j].len - diff;
						shrink_next_entries(ctx, lba, pba, diff, page);
					}

				}
				return;
			}
		}
		ptr++;
	}
}


static int merge_rev_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned long len, struct page *page)
{

	struct lsdm_revmap_entry_sector * ptr = NULL;
	int i = 0, j = 0, found = 0;
	int entry_nr, sector_nr, max_entries;
	unsigned long diff = 0;

	entry_nr = atomic_read(&ctx->revmap_entry_nr);
	sector_nr = atomic_read(&ctx->revmap_sector_nr);
	ptr = (struct lsdm_revmap_entry_sector *)page_address(page);

	for(i=0; i<=sector_nr; i++) {
		if (found)
			break;
		if (i == sector_nr)
			max_entries = entry_nr;
		else
			max_entries = NR_EXT_ENTRIES_PER_SEC;
		/* SECTOR ENTRIES */
		for (j=0; j<max_entries; j++) {
			if(ptr->extents[j].lba == lba) {
				if(ptr->extents[j].len <= len) {
					/* replace pba */
					ptr->extents[j].pba = pba;
					if (ptr->extents[j].len < len) {
						diff = len - ptr->extents[j].len;
						lba = lba + ptr->extents[j].len;
						pba = pba + ptr->extents[j].len;
						shrink_next_entries(ctx, lba, pba, diff, page);
					}
					ptr->extents[j].len = len;
					found = 1;
					break;
				} else {
					/* Shrink the entry to exclude
					 * what shall be added later
					 */
					ptr->extents[j].lba += len;
					found = 0;
					return found;
				}
			
			} else if (ptr->extents[j].lba + ptr->extents[j].len == lba) {
				/* merge if pba allows merging */
				if (ptr->extents[j].pba + ptr->extents[j].len == pba) {
					ptr->extents[j].len += len;
					found = 1;
					break;
				}
			} else if (lba + len == ptr->extents[j].lba) {
				/* merge if pba allows merging */
				if (pba + len == ptr->extents[j].pba) {
					ptr->extents[j].lba = lba;
					ptr->extents[j].pba = pba;
					ptr->extents[j].len += len;
					found = 1;
					break;
				}
			}
		}
		ptr++;
	}

	return found;
}



/* We store only the LBA. We can calculate the PBA from the wf
 * 
 * Always called with the metadata_update_lock held!
 */
static void add_revmap_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned int nrsectors)
{
	struct lsdm_revmap_entry_sector * ptr = NULL;
	int found = 0;
	int entry_nr, sector_nr, max_entries;
	struct page * page = NULL;

	/* Merge entries by increasing the length if there lies a
	 * matching entry in the revmap page
	 */
	entry_nr = atomic_read(&ctx->revmap_entry_nr);
	sector_nr = atomic_read(&ctx->revmap_sector_nr);
	//printk(KERN_ERR "\n ******* %s entry_nr: %d, sector_nr: %d ctx->revmap_pba: %d ", __func__, entry_nr, sector_nr, ctx->revmap_pba);
	if ((entry_nr > 0) || (sector_nr > 0)) {
		page = ctx->revmap_page;
		ptr = (struct lsdm_revmap_entry_sector *)page_address(page);
		/*
		found = merge_rev_entries(ctx, lba, pba, nrsectors, page);
		if (found) {
			//printk(KERN_ERR "\n (Merged) revmap entry added! ptr: %p i: %d, j:%d lba: %llu pba: %llu len: %d \n", ptr, i, j, lba, pba, nrsectors);
			return;
		} */

		atomic_inc(&ctx->revmap_entry_nr);
		if (NR_EXT_ENTRIES_PER_SEC == (entry_nr + 1)) {
			atomic_set(&ctx->revmap_entry_nr, 0);
			atomic_inc(&ctx->revmap_sector_nr);
			if (NR_SECTORS_PER_BLK == (sector_nr + 1)) {
				//trace_printk("\n Waiting on block barrier! \n");
				page = ctx->revmap_page;
				BUG_ON(page == NULL);
				flush_revmap_block_disk(ctx, page);
				/* we do not wait for the write to
				 * complete as we are in the write
				 * context
				 */
				atomic_set(&ctx->revmap_sector_nr, 0);
				/* Adjust the revmap_pba for the next block 
				* Addressing is based on 512bytes sector.
				*/
				ctx->revmap_pba += NR_SECTORS_IN_BLK; 
				//printk(KERN_ERR "\n %s ctx->revmap_pba: %d", __func__, ctx->revmap_pba);

				/* if we have the pba of the translation table,
				* then reset the revmap pba to the original value
				*/
				if (ctx->revmap_pba == ctx->sb->tm_pba) {
					ctx->revmap_pba = ctx->sb->revmap_pba;
					/* we might have to flush
					 * translation blocks here,
					 * but we do it in the wait
					 * function anyway.
					 */
				}
			}
		}
	}
	//trace_printk("\n spin lock aquired! i:%d j:%d\n", i, j);
	/* blk count before incrementing */
	if ((0 == entry_nr) && (0 == sector_nr)) {
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
		//printk(KERN_ERR "\n revmap_page: %p", page_address(page));
	}
	page = ctx->revmap_page;
	ptr = (struct lsdm_revmap_entry_sector *)page_address(page);
	ptr = ptr + sector_nr;
	ptr->extents[entry_nr].lba = lba;
    	ptr->extents[entry_nr].pba = pba;
	ptr->extents[entry_nr].len = nrsectors;
	if (NR_EXT_ENTRIES_PER_SEC == (entry_nr+1)) {
		//ptr->crc = calculate_crc(ctx, page);
		ptr->crc = 0;
	}
	if ((entry_nr == 0) && (sector_nr == 0))  {
		atomic_inc(&ctx->revmap_entry_nr);
	}
	//printk(KERN_ERR "\n revmap entry added! ptr: %p entry_nr: %d, sector_nr: %d lba: %llu pba: %llu len: %d \n", ptr, entry_nr, sector_nr, lba, pba, nrsectors);
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
	struct lsdm_bioctx * lsdm_bioctx;
	struct ctx *ctx;

	lsdm_bioctx = container_of(kref, struct lsdm_bioctx, ref);
	bio = lsdm_bioctx->orig;
	bio_endio(bio);

	ctx = lsdm_bioctx->ctx;
	//printk(KERN_ERR "\n %s bioctx released to bioctx_cache, %p ", __func__, lsdm_bioctx);
	kmem_cache_free(ctx->bioctx_cache, lsdm_bioctx);
	//kref_put(&ctx->ongoing_iocount, lsdm_is_ioidle);
	//atomic_dec(&ctx->nr_writes);
}


void sub_write_done(void *data, async_cookie_t cookie)
{

	struct lsdm_sub_bioctx *subbioctx = NULL;
	struct lsdm_bioctx * bioctx;
	struct ctx *ctx;
	sector_t lba, pba;
	unsigned int len;
	static unsigned long timeout = 100000;

	subbioctx = (struct lsdm_sub_bioctx *) data;
	bioctx = rcu_dereference(subbioctx->bioctx);
	ctx = subbioctx->bioctx->ctx;
	if (wait_for_completion_timeout(&subbioctx->write_done, msecs_to_jiffies(timeout)) == 0)  {
		if (spin_trylock(&subbioctx->spin_lock)) {
			/* Could aquire lock, because
			 * wait_for_completion_timeout() did not race with
			 * bio_endio()
			 */
			atomic_inc(&ctx->nr_failed_writes);
			bioctx->orig->bi_status =  -EAGAIN;
			kref_put(&bioctx->ref, write_done);
			return;
		}
	}
	lba = subbioctx->extent.lba;
	pba = subbioctx->extent.pba;
	len = subbioctx->extent.len;
	kmem_cache_free(ctx->subbio_ctx_cache, subbioctx);
	kref_put(&bioctx->ref, write_done);

	//printk(KERN_ERR "\n %s done; %llu at %llu len: %lu", __func__, lba, pba, len);
	down_write(&ctx->metadata_update_lock);
	/*------------------------------- */
	add_revmap_entries(ctx, lba, pba, len);
	lsdm_update_range(ctx, &ctx->extent_tbl_root, lba, pba, len);
	lsdm_update_range(ctx, &ctx->rev_tbl_root, pba, lba, len);
	/*-------------------------------*/
	up_write(&ctx->metadata_update_lock);
	//printk(KERN_ERR "\n * (%s) thread ... waiting.... LBA: %llu done \n", __func__, lba);
	return;
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
static void lsdm_clone_endio(struct bio * clone)
{
	struct lsdm_sub_bioctx *subbioctx = NULL;
	struct bio *bio = NULL;
	struct ctx *ctx;

	subbioctx = clone->bi_private;
	ctx = subbioctx->bioctx->ctx;
	if (spin_trylock(&subbioctx->spin_lock)) {
		/* Did not race with wait_for_completion_timeout() 
		 * Reached here first 
		 */
		bio = subbioctx->bioctx->orig;
		if (bio && bio->bi_status == BLK_STS_OK) {
			bio->bi_status = clone->bi_status;
			//printk(KERN_INFO "\n (%s) .. completing I/O......lba: %llu, pba: %llu, len: %lu &write_done: %llu", __func__, subbioctx->extent.lba, subbioctx->extent.pba, subbioctx->extent.len, &subbioctx->write_done);
		} 
		complete(&subbioctx->write_done);
		/* bio_put can race with the wait_completion_timeout() */
	} else {
		/* wait_for_completion_timeout() timedout before the
		 * bio could complete writing. We release subbioctx
		 * here as it is never needed in the timeout case
		 */
		kmem_cache_free(ctx->subbio_ctx_cache, subbioctx);
	}
	bio_put(clone);
	return;
}


/*
 * NOTE: LBA is the address of a sector. We expect the LBAs to be
 * block aligned ie they are always divisible by 8 or always the 8th
 * sector.
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
 * We will come to lsdm code only when vfs did not find the data it was trying
 * to read anyway.
 *
*/
static int lsdm_write_io(struct ctx *ctx, struct bio *bio)
{
	struct bio *split = NULL, *pad = NULL;
	struct bio * clone;
	struct lsdm_bioctx * bioctx;
	struct lsdm_sub_bioctx *subbio_ctx;
	unsigned nr_sectors = bio_sectors(bio);
	sector_t s8, lba = bio->bi_iter.bi_sector;
	struct blk_plug plug;
	sector_t wf;
	struct lsdm_ckpt *ckpt;
	async_cookie_t cookie;

	if (is_disk_full(ctx)) {
		bio->bi_status = BLK_STS_NOSPC;
		bio_endio(bio);
		return -1;
	}

	/* ckpt must be updated. The state of the filesystem is
	 * unclean until checkpoint happens!
	 */
	ckpt = (struct lsdm_ckpt *)page_address(ctx->ckpt_page);
	ckpt->clean = 0;
	nr_sectors = bio_sectors(bio);
	//printk(KERN_ERR "\n ******* Inside %s, requesting lba: %llu sectors: %d nr_tm_pages: %u", __func__, bio->bi_iter.bi_sector, nr_sectors, atomic_read(&ctx->nr_tm_pages));
	if (unlikely(nr_sectors <= 0)) {
		//trace_printk("\n Less than 0 sectors (%d) requested!", nr_sectors);
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		return -1;
	}
	
	clone = bio_clone_fast(bio, GFP_KERNEL, NULL);
	if (!clone) {
		//trace_printk("\n Insufficient memory!");
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		/* We dont call bio_put() because the bio should not
		 * get freed by memory management before bio_endio()
		 */
		return -ENOMEM;
	}

	bioctx = kmem_cache_alloc(ctx->bioctx_cache, GFP_KERNEL);
	if (!bioctx) {
		//trace_printk("\n Insufficient memory!");
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		bio_put(clone);
		/* We dont call bio_put() because the bio should not
		 * get freed by memory management before bio_endio()
		 */
		return -ENOMEM;
	}
	//printk(KERN_ERR "\n bioctx allocated from bioctx_cache, ptr: %p", bioctx);

	atomic_inc(&ctx->nr_writes);
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
			//trace_printk("\n insufficient memory!");
			goto fail;
		}
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		//trace_printk("\n subbio_ctx: %llu", subbio_ctx);
		spin_lock(&ctx->lock);
		/*-------------------------------*/
		wf = ctx->app_write_frontier;
		
		/* room_in_zone should be same as
		 * ctx->nr_free_sectors_in_wf
		 */
		if (s8 > ctx->free_sectors_in_wf){
			//trace_printk("SPLITTING!!!!!!!! s8: %d ctx->free_sectors_in_wf: %d", s8, ctx->free_sectors_in_wf);
			s8 = round_down(ctx->free_sectors_in_wf, NR_SECTORS_IN_BLK);
			if (s8 <= 0) {
				panic("Should always have atleast a block left ");
			}
			if (!(split = bio_split(clone, s8, GFP_NOIO, ctx->bs))){
				//trace_printk("\n failed at bio_split! ");
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
		subbio_ctx->extent.lba = lba;
		subbio_ctx->extent.pba = wf;
		subbio_ctx->bioctx = bioctx; /* This is common to all the subdivided bios */
		spin_lock_init(&subbio_ctx->spin_lock);

		split->bi_iter.bi_sector = wf; /* we use the save write frontier */
		split->bi_private = subbio_ctx;
		split->bi_end_io = lsdm_clone_endio;
		bio_set_dev(split, ctx->dev->bdev);
		submit_bio(split);
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
		zero_fill_bio(pad);
		/* we dont need to take any action when pad completes
		 * and thus we dont need any endio for pad. Chain it 
		 * with parent, so that we are notified its completion
		 * when parent completes.
		 */
		bio_chain(pad, clone);
		submit_bio(pad);
	}
	blk_finish_plug(&plug);
	kref_put(&bioctx->ref, write_done);
	return 0;

fail:
	printk(KERN_ERR "%s FAIL!!!!\n", __func__);
	clone->bi_status = BLK_STS_RESOURCE;
	kref_put(&bioctx->ref, write_done);
	bio_endio(clone);
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
struct lsdm_ckpt * read_checkpoint(struct ctx *ctx, unsigned long pba)
{
	struct block_device *bdev = ctx->dev->bdev;
	struct lsdm_ckpt *ckpt = NULL;
	sector_t blknr = pba / NR_SECTORS_IN_BLK;
	struct page *page;

	page = read_block(ctx, 0, pba);
	if (!page)
		return NULL;

       	ckpt = (struct lsdm_ckpt *) page_address(page);
	//printk(KERN_INFO "\n ** blknr: %llu, ckpt->magic: %u, ckpt->cur_frontier_pba: %lld", blknr, ckpt->magic, ckpt->cur_frontier_pba);
	if (ckpt->magic == 0) {
		__free_pages(page, 0);
		return NULL;
	}
	ctx->ckpt_page = page;
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
	async_cookie_t cookie;
	//struct blk_plug plug;

	//trace_printk("Inside %s" , __func__);
	spin_lock(&ctx->ckpt_lock);
	/*--------------------------------------------*/
	ctx->flag_ckpt = 1;
	/* We expect the ckpt_ref to go to atleast 4 and then back to 0
	 * when all the blocks are flushed!
	 */
	atomic_set(&ctx->ckpt_ref, 1);
	/*--------------------------------------------*/
	spin_unlock(&ctx->ckpt_lock);

	/*--------------------------------------------*/
	flush_sit(ctx, cookie);
	/*--------------------------------------------*/
	//printk(KERN_ERR "\n sit pages flushed!");

	//blk_start_plug(&plug);
	flush_revmap_bitmap(ctx);
	update_checkpoint(ctx);
	atomic_dec(&ctx->ckpt_ref);
	//wait_for_ckpt_completion(ctx);
	/* At this point we know that SIT pages and revmap bitmap has
	 * found its way on the disk
	 */

	/*--------------------------------------------*/
	flush_checkpoint(ctx);
	//blk_finish_plug(&plug);

	//printk(KERN_ERR "\n checkpoint flushed! \n");
	/* We need to wait for all of this to be over before 
	 * we proceed
	 */
	spin_lock(&ctx->ckpt_lock);
	ctx->flag_ckpt = 0;
	spin_unlock(&ctx->ckpt_lock);
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
struct lsdm_ckpt * get_cur_checkpoint(struct ctx *ctx)
{
	struct lsdm_sb * sb = ctx->sb;
	struct lsdm_ckpt *ckpt1, *ckpt2, *ckpt;
	struct page *page1;

	//printk(KERN_INFO "\n !Reading checkpoint 1 from pba: %u", sb->ckpt1_pba);
	ckpt1 = read_checkpoint(ctx, sb->ckpt1_pba);
	if (!ckpt1)
		return NULL;
	page1 = ctx->ckpt_page;
	/* ctx->ckpt_page will be overwritten by the next
	 * call to read_ckpt
	 */
	//printk(KERN_INFO "\n !!Reading checkpoint 2 from pba: %u", sb->ckpt2_pba);
	ckpt2 = read_checkpoint(ctx, sb->ckpt2_pba);
	if (!ckpt2) {
		put_page(page1);
		return NULL;
	}
	//printk(KERN_INFO "\n %s ckpt versions: %lld %lld", __func__, ckpt1->version, ckpt2->version);
	if (ckpt1->version >= ckpt2->version) {
		ckpt = ckpt1;
		__free_pages(ctx->ckpt_page, 0);
		ctx->ckpt_page = page1;
		ctx->ckpt_pba == ctx->sb->ckpt2_pba;
		//printk(KERN_ERR "\n Setting ckpt 1 version: %d ckpt2 version: %d", ckpt1->version, ckpt2->version);
	}
	else {
		ckpt = ckpt2;
		ctx->ckpt_pba == ctx->sb->ckpt1_pba;
		//page2 is rightly set by read_ckpt();
		__free_pages(page1, 0);
		//printk(KERN_ERR "\n Setting ckpt 1 version: %d ckpt2 version: %d", ckpt1->version, ckpt2->version);
	}
	ctx->user_block_count = ckpt->user_block_count;
	//printk(KERN_ERR "\n %s Nr of free blocks: %d",  __func__, ctx->user_block_count);
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
		//printk(KERN_ERR "\n entry->lba: %llu entry->pba: %llu", entry->lba, entry->pba);
		/* TODO: right now everything should be zeroed out */
		//panic("Why are there any already mapped extents?");
		down_write(&ctx->metadata_update_lock);
		lsdm_update_range(ctx, &ctx->extent_tbl_root, entry->lba, entry->pba, NR_SECTORS_IN_BLK);
		lsdm_update_range(ctx, &ctx->rev_tbl_root, entry->pba, entry->lba, NR_SECTORS_IN_BLK);
		up_write(&ctx->metadata_update_lock);
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
	int nr_extents_in_blk = TM_ENTRIES_BLK;
	unsigned long long pba;
	unsigned long nrblks = ctx->sb->blk_count_tm;
	struct block_device *bdev = ctx->dev->bdev;
	struct page *page;
	int i = 0;
	struct tm_entry * tm_entry = NULL;

	printk("\n Reading TM entries from: %llu, nrblks: %ld", ctx->sb->tm_pba, nrblks);
	
	ctx->n_extents = 0;
	pba = 0;
	while(i < nrblks) {
		page = read_block(ctx, pba, ctx->sb->tm_pba);
		if (!page)
			return -1;
		/* We read the extents in the entire block. the
		 * redundant extents should be unpopulated and so
		 * we should find 0 and break out */
		//trace_printk("\n pba: %llu", pba);
		tm_entry = (struct tm_entry *) page_address(page);
		read_extents_from_block(ctx, tm_entry, nr_extents_in_blk);
		i = i + 1;
		__free_pages(page, 0);
		pba = pba + 1;
	}
	//trace_printk("\n TM entries read!");
	return 0;
}

int read_revmap_bitmap(struct ctx *ctx)
{
	unsigned long nrblks = ctx->sb->blk_count_revmap_bm;

	if (nrblks != 1) {
		panic("\n Wrong revmap bitmap calculations!");
	}
	
	ctx->revmap_bm = read_block(ctx, 0, ctx->sb->revmap_bm_pba);
	if (!ctx->revmap_bm) {
		/* free the successful bh till now */
		return -1;
	}
	return 0;

}


void process_revmap_entries_on_boot(struct ctx *ctx, struct page *page)
{
	struct lsdm_revmap_extent *extent;
	struct lsdm_revmap_entry_sector *entry_sector;
	int i = 0, j;

	//trace_printk("\n Inside process revmap_entries_on_boot!" );

	add_block_based_translation(ctx,  page);
	//trace_printk("\n Added block based translations!, will add memory based extent maps now.... \n");
	entry_sector = (struct lsdm_revmap_entry_sector *) page_address(page);
	while (i < NR_SECTORS_IN_BLK) {
		extent = entry_sector->extents;
		for (j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			if (extent[j].pba == 0)
				continue;
			down_write(&ctx->metadata_update_lock);
			lsdm_update_range(ctx, &ctx->extent_tbl_root, extent[j].lba, extent[j].pba, extent[j].len);
			lsdm_update_range(ctx, &ctx->rev_tbl_root, extent[j].pba, extent[j].lba, extent[j].len);
			up_write(&ctx->metadata_update_lock);
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
	int i = 0, byte = 0;
	struct page *page;
	char *ptr;
	unsigned long pba;
	struct block_device *bdev = NULL;
	char flush_needed = 0;
	struct page * revmap_page;
	unsigned int nr_revmap_blks = ctx->sb->blk_count_revmap;

	bdev = ctx->dev->bdev;
	/* We read the revmap bitmap first. If a bit is set,
	 * then the corresponding revmap blk is read
	 */
	page = ctx->revmap_bm;
	if (!page)
		return -1;
	ptr = (char *) page_address(page);
	//trace_printk("\n page_address(ctx->revmap_bm): %p", ptr);
	pba = 0;
	for (i = 0; i < BLK_SZ; i++, pba++) {
		byte = *ptr;
		while(byte) {
			if (byte & 1) {
				//trace_printk("\n WHY IS THIS BYTE SET??");
				flush_needed = 1;
				//trace_printk("\n read revmap blk: %lu", pba);
				revmap_page = read_block(ctx, pba, ctx->revmap_pba);
				if (!revmap_page) {
					return -1;
				}
				process_revmap_entries_on_boot(ctx, revmap_page);
				__free_pages(revmap_page, 0);
				pba = pba + 1;
				if (pba >= nr_revmap_blks)
					break;
			}
			byte = byte >> 1;
		}
		if (pba >= nr_revmap_blks)
			break;
		ptr = ptr + 1;
	}
	//printk("\n %s flush_needed: %d", __func__, flush_needed);
	if (flush_needed) {
		async_cookie_t cookie;
		//trace_printk("\n Why do we need to flush!!");
		flush_translation_blocks(ctx, cookie);
	}
	return 0;
}


sector_t get_zone_pba(struct lsdm_sb * sb, unsigned int segnr)
{
	return (segnr * (1 << (sb->log_zone_size - sb->log_sector_size)));
}


/* 
 * Returns the pba of the last sector in the zone
 */
sector_t get_zone_end(struct lsdm_sb *sb, sector_t pba_start)
{
	return (pba_start + (1 << (sb->log_zone_size - sb->log_sector_size))) - 1;
}


int allocate_freebitmap(struct ctx *ctx)
{
	char *free_bitmap;
	if (!ctx)
		return -1;
	//printk(KERN_INFO "\n ctx->bitmap_bytes: %d ", ctx->bitmap_bytes);
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

int _lsdm_verbose;
static void gc_rb_insert(struct ctx *ctx, struct rb_root *root, struct gc_rb_node *new)
{
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct gc_rb_node *e = NULL;

	RB_CLEAR_NODE(&new->rb);
	write_lock(&ctx->sit_rb_lock);

	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		e = container_of(parent, struct gc_rb_node, rb);
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


static void sit_rb_remove(struct ctx *ctx, struct rb_root *root, struct gc_rb_node*e)
{
	/* rb_erase resorts and rebalances the tree */
	rb_erase(&e->rb, root);
	ctx->n_sit_extents--;
}

static unsigned int get_cb_cost(struct ctx *ctx , u32 nrblks, u64 mtime)
{
	unsigned char u, age;
	struct lsdm_sb *sb = ctx->sb;

	u = (nrblks * 100) >> (sb->log_zone_size - sb->log_block_size);
	age = 100 - div_u64(100 * (mtime - ctx->min_mtime),
				ctx->max_mtime - ctx->min_mtime);

	return UINT_MAX - ((100 * (100 - u) * age)/ (100 + u));
}


static unsigned int get_cost(struct ctx *ctx, u32 nrblks, u64 age, char gc_mode)
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
int update_gc_rb_tree(struct ctx *ctx, unsigned int zonenr, u32 nrblks, u64 mtime)
{
	/* TODO: There is a tree with SIT pages and there is one for
	 * GC cost. These are separate trees. So use another root.
	 * Do not mix these trees!
	 */
	struct rb_root *root = &ctx->gc_rb_root;
	struct rb_node **link = &root->rb_node, *parent = NULL, *prev = NULL;
	struct gc_rb_node *e = NULL, *new = NULL;
	unsigned int cb_cost = 0;
	static int count = 0;

	BUG_ON(nrblks == 0);

	/* We are essentially adding a new node here */
	new = kmem_cache_alloc(ctx->gc_rb_node_cache, GFP_KERNEL);
	if (!new) {
		return -ENOMEM;
	}
	RB_CLEAR_NODE(&new->rb);
	new->zonenr = zonenr;
	cb_cost = get_cost(ctx, nrblks, mtime, GC_CB);
	new->cb_cost = cb_cost;
	new->nrblks = nrblks;

	//printk(KERN_ERR "\n %s zonenr: %d nrblks: %u, mtime: %llu, GC_GREEDY \n", __func__, zonenr, nrblks, mtime);
	write_lock(&ctx->sit_rb_lock);
	count = 0;
	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		e = container_of(parent, struct gc_rb_node, rb);
		/* The key is cost, not zonenr, so, you may not want
		 * to add this new cost to the same parent. So we
		 * continue searching the appropriate parent
		 */
		if (zonenr == e->zonenr) {
			parent = rb_parent(&e->rb);
			rb_erase(&e->rb, root);
			kmem_cache_free(ctx->gc_rb_node_cache, e);
			//printk(KERN_ERR "\n %s Found the zone: %d and deleted it, will read again later! ", __func__, zonenr);
			link = &parent;
			break;
		}
		if (cb_cost == e->cb_cost) {
			/* We prefer older segments when the cost is the same
			 * mtime of segments is always unique and the
			 * cost is the same. Thus nrblks is the
			 * differentiting factor. More nrblks, less
			 * mtime when the cost is the same.
			 */
			if (nrblks > e->nrblks)
				link = &(*link)->rb_left;
			else
				link = &(*link)->rb_right;

			continue;
		}
		/* CB Cost is calculated as UINT_MAX - cost;
		 * For Greedy, cost is #valid blks.
		 * Bigger cost is what we prefer!
		 */
		if (cb_cost > e->cb_cost) {
			link = &(*link)->rb_left;
		} else {
			link = &(*link)->rb_right;
		}
	}

	/* link holds the address of left or the right pointer
	 * appropriately
	 */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
	write_unlock(&ctx->sit_rb_lock);
	//printk(KERN_ERR "\n %s Added zonenr: %d cost: %u to the RB tree ", __func__, zonenr, cb_cost);
	return 0;
}


/* TODO: create a freezone cache
 * create a gc zone cache
 * currently calling kzalloc
 */
int read_seg_entries_from_block(struct ctx *ctx, struct lsdm_seg_entry *entry, unsigned int nr_seg_entries, unsigned int *zonenr)
{
	int i = 0;
	struct lsdm_sb *sb;
	unsigned int nr_blks_in_zone;
       
	if (!ctx)
		return -1;

	sb = ctx->sb;

	nr_blks_in_zone = (1 << (sb->log_zone_size - sb->log_block_size));
	//trace_printk("\n Number of seg entries: %u", nr_seg_entries);
	//trace_printk("\n cur_frontier_pba: %llu, frontier zone: %u", ctx->ckpt->cur_frontier_pba, get_zone_nr(ctx, ctx->ckpt->cur_frontier_pba));

	while (i < nr_seg_entries) {
		if ((*zonenr == get_zone_nr(ctx, ctx->ckpt->cur_frontier_pba)) ||
		    (*zonenr == get_zone_nr(ctx, ctx->ckpt->cur_gc_frontier_pba))) {
			//trace_printk("\n zonenr: %d is our cur_frontier! not marking it free!", *zonenr);
			entry = entry + 1;
			*zonenr= *zonenr + 1;
			i++;
			continue;
		}
		if (entry->vblocks == 0) {

			//trace_printk("\n *segnr: %u", *zonenr);
			spin_lock(&ctx->lock);
			mark_zone_free(ctx , *zonenr);
			spin_unlock(&ctx->lock);
		}
		else if (entry->vblocks < nr_blks_in_zone) {
			//trace_printk("\n *segnr: %u", *zonenr);
			mark_zone_gc_candidate(ctx, *zonenr);
			if (ctx->min_mtime > entry->mtime)
				ctx->min_mtime = entry->mtime;
			if (ctx->max_mtime < entry->mtime)
				ctx->max_mtime = entry->mtime;
			if (!update_gc_rb_tree(ctx, *zonenr, entry->vblocks, entry->mtime))
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
	int nr_seg_entries_blk = BLK_SZ / sizeof(struct lsdm_seg_entry);
	int ret=0;
	struct lsdm_seg_entry *entry0;
	unsigned int zonenr = 0;
	struct lsdm_sb *sb;
	unsigned long nr_data_zones;
	unsigned long nr_seg_entries_read;
	struct page * sit_page;
	
	if (NULL == ctx)
		return -1;

	bdev = ctx->dev->bdev;
	sb = ctx->sb;

	nr_data_zones = sb->zone_count_main; /* these are the number of segment entries to read */
	nr_seg_entries_read = 0;
	
	ret = allocate_freebitmap(ctx);
	//printk(KERN_INFO "\n Allocated free bitmap, ret: %d", ret);
	if (0 > ret)
		return ret;
	ret = allocate_gc_zone_bitmap(ctx);
	//printk(KERN_INFO "\n Allocated gc zone bitmap, ret: %d", ret);
	if (0 > ret) {
		kfree(ctx->freezone_bitmap);
		return ret;
	}


	ctx->min_mtime = ULLONG_MAX;
	ctx->max_mtime = get_elapsed_time(ctx);
	nrblks = sb->blk_count_sit;
	pba = 0;
	while (zonenr < sb->zone_count_main) {
		//trace_printk("\n zonenr: %u", zonenr);
		if (((pba * NR_SECTORS_IN_BLK) + sb->sit_pba) > ctx->sb->zone0_pba) {
			panic("seg entry blknr cannot be bigger than the data blknr");
			printk(KERN_ERR "seg entry blknr cannot be bigger than the data blknr");
			return -1;
		}
		//printk(KERN_INFO "\n blknr: %lu", blknr);
		sit_page = read_block(ctx, pba, sb->sit_pba);
		if (!sit_page) {
			kfree(ctx->freezone_bitmap);
			kfree(ctx->gc_zone_bitmap);
			return -1;
		}
		entry0 = (struct lsdm_seg_entry *) page_address(sit_page);
		if (nr_data_zones > nr_seg_entries_blk)
			nr_seg_entries_read = nr_seg_entries_blk;
		else
			nr_seg_entries_read = nr_data_zones;
		read_seg_entries_from_block(ctx, entry0, nr_seg_entries_read, &zonenr);
		nr_data_zones = nr_data_zones - nr_seg_entries_read;
		__free_pages(sit_page, 0);
		pba = pba + 1;
	}
	//printk(KERN_ERR "\n nr_freezones (2) : %u", ctx->nr_freezones);
	return 0;
}

struct lsdm_sb * read_superblock(struct ctx *ctx, unsigned long pba)
{

	struct block_device *bdev = ctx->dev->bdev;
	unsigned int blknr = pba;
	struct buffer_head *bh;
	struct lsdm_sb * sb;
	/* TODO: for the future. Right now we keep
	 * the block size the same as the sector size
	 *
	*/
	if (set_blocksize(bdev, BLK_SZ))
		return NULL;

	//printk(KERN_INFO "\n reading sb at pba: %lu", pba);

	bh = __bread(bdev, blknr, BLK_SZ);
	if (!bh)
		return NULL;
	
	sb = (struct lsdm_sb *)bh->b_data;
	if (sb->magic != STL_SB_MAGIC) {
		printk(KERN_INFO "\n Wrong superblock!");
		put_bh(bh);
		return NULL;
	}
	//printk(KERN_INFO "\n sb->magic: %u", sb->magic);
	//printk(KERN_INFO "\n sb->max_pba: %d", sb->max_pba);
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
	struct lsdm_sb * sb1, *sb2;
	struct lsdm_ckpt *ckpt;
	struct page *page;

	sb1 = read_superblock(ctx, 0);
	if (NULL == sb1) {
		//trace_printk("\n read_superblock failed! cannot read the metadata ");
		return -1;
	}

	//printk(KERN_INFO "\n superblock read!");
	/*
	 * we need to verify that sb1 is uptodate.
	 * Right now we do nothing. we assume
	 * sb1 is okay.
	 */
	page = ctx->sb_page;
	sb2 = read_superblock(ctx, 1);
	if (!sb2) {
		//trace_printk("\n Could not read the second superblock!");
		return -1;
	}
	put_page(page);
	ctx->sb = sb2;
	ctx->max_pba = ctx->sb->max_pba;
	ctx->nr_lbas_in_zone = (1 << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	ctx->revmap_pba = ctx->sb->revmap_pba;
	//printk("\n ** ctx->revmap_pba (first revmap bm block pba) : %llu", ctx->revmap_pba);

	ckpt = get_cur_checkpoint(ctx);
	if (NULL == ckpt) {
		put_page(ctx->sb_page);
		return -1;
	}	
	ctx->ckpt = ckpt;
	//printk(KERN_INFO "\n checkpoint read!");
	if (!ckpt->clean) {
		//trace_printk("\n Scrubbing metadata after an unclean shutdown...");
		ret = do_recovery(ctx);
		return ret;
	}
	//printk(KERN_ERR "\n sb->blk_count_revmap_bm: %d", ctx->sb->blk_count_revmap_bm);
	//printk(KERN_ERR "\n nr of revmap blks: %u", ctx->sb->blk_count_revmap);

	ctx->app_write_frontier = ctx->ckpt->cur_frontier_pba;
	//printk(KERN_ERR "\n %s %d ctx->app_write_frontier: %llu\n", __func__, __LINE__, ctx->app_write_frontier);
	ctx->app_wf_end = zone_end(ctx, ctx->app_write_frontier);
	//printk(KERN_ERR "\n %s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->app_wf_end);
	//printk(KERN_ERR "\n max_pba = %d", ctx->max_pba);
	ctx->free_sectors_in_wf = ctx->app_wf_end - ctx->app_write_frontier + 1;
	//printk(KERN_ERR "\n ctx->free_sectors_in_wf: %lld", ctx->free_sectors_in_wf);
	
	ctx->gc_write_frontier = ctx->ckpt->cur_gc_frontier_pba;
	//printk(KERN_ERR "\n %s %d ctx->app_write_frontier: %llu\n", __func__, __LINE__, ctx->app_write_frontier);
	ctx->gc_wf_end = zone_end(ctx, ctx->gc_write_frontier);
	//printk(KERN_ERR "\n %s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->app_wf_end);
	//printk(KERN_ERR "\n max_pba = %d", ctx->max_pba);
	ctx->free_sectors_in_gc_wf = ctx->gc_wf_end - ctx->gc_write_frontier + 1;
	//printk(KERN_ERR "\n ctx->free_sectors_in_wf: %lld", ctx->free_sectors_

	ret = read_revmap_bitmap(ctx);
	if (ret) {
		put_page(ctx->sb_page);
		put_page(ctx->ckpt_page);
		return -1;
	}
	printk(KERN_ERR "\n before: PBA for first revmap blk: %u", ctx->sb->revmap_pba/NR_SECTORS_IN_BLK);
	read_revmap(ctx);
	printk(KERN_INFO "Reverse map flushed!");
	ret = read_translation_map(ctx);
	if (0 > ret) {
		put_page(ctx->sb_page);
		put_page(ctx->ckpt_page);
		put_page(ctx->revmap_bm);
		//trace_printk("\n read_extent_map failed! cannot read the metadata ");
		return ret;
	}
	printk(KERN_INFO "\n extent_map read!");

	ctx->nr_freezones = 0;
	ctx->bitmap_bytes = sb1->zone_count_main /BITS_IN_BYTE;
	if (sb1->zone_count_main % BITS_IN_BYTE)
		ctx->bitmap_bytes = ctx->bitmap_bytes + 1;
	//printk(KERN_INFO "\n Nr of zones in main are: %u, bitmap_bytes: %d", sb1->zone_count_main, ctx->bitmap_bytes);
	if (sb1->zone_count_main % BITS_IN_BYTE > 0)
		ctx->bitmap_bytes += 1;
	ctx->nr_freezones = 0;
	read_seg_info_table(ctx);
	printk(KERN_INFO "\n read segment entries, free bitmap created! \n");
	printk(KERN_INFO "\n ctx->nr_freezones: %u, ckpt->nr_free_zones:%llu", ctx->nr_freezones, ckpt->nr_free_zones);
	if (ctx->nr_freezones != ckpt->nr_free_zones) { 
		/* TODO: Do some recovery here.
		 * We do not wait for confirmation of SIT pages on the
		 * disk. we match the SIT entries to that by the
		 * translation map. we also make sure that the ckpt
		 * entries are based on the translation map
		 */
		printk(KERN_ERR "\n SIT and checkpoint does not match!");
		do_recovery(ctx);
		put_page(ctx->sb_page);
		put_page(ctx->ckpt_page);
		put_page(ctx->revmap_bm);
		return -1;
	}
	printk(KERN_ERR "\n Metadata read! \n");
	return 0;
}

/*

unsigned long lsdm_pages_to_free_count(struct shrinker *shrinker, struct shrink_control *sc)
{

	int flag = 0;
	count = flush_count_tm_blocks(ctx, false, &flag);
	count += flush_count_sit_blocks(ctx, false, &flag);
}


unsigned long lsdm_free_pages()
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



static struct lsdm_shrinker {
	.count_objects = lsdm_pages_to_free_count;
	.scan_objects = lsdm_free_pages;
	.seeks = DEFAULT_SEEKS;
};
*/

static void destroy_caches(struct ctx *ctx)
{	
	//kmem_cache_destroy(ctx->gc_extents_cache);
	kmem_cache_destroy(ctx->subbio_ctx_cache);
	kmem_cache_destroy(ctx->tm_page_cache);
	kmem_cache_destroy(ctx->gc_rb_node_cache);
	kmem_cache_destroy(ctx->sit_ctx_cache);
	kmem_cache_destroy(ctx->sit_page_cache);
	kmem_cache_destroy(ctx->tm_page_write_cache);
	kmem_cache_destroy(ctx->revmap_bioctx_cache);
	kmem_cache_destroy(ctx->bioctx_cache);
	kmem_cache_destroy(ctx->sit_ctx_cache);
}


static int create_caches(struct ctx *ctx)
{
	ctx->bioctx_cache = kmem_cache_create("bioctx_cache", sizeof(struct lsdm_bioctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->bioctx_cache) {
		return -1;
	}
	ctx->revmap_bioctx_cache = kmem_cache_create("revmap_bioctx_cache", sizeof(struct revmap_bioctx), 0, SLAB_ACCOUNT, NULL);
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
	ctx->tm_page_cache = kmem_cache_create("tm_page_cache", sizeof(struct tm_page), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->tm_page_cache) {
		goto destroy_sit_ctx_cache;
	}
	ctx->gc_rb_node_cache = kmem_cache_create("gc_rb_node_cache", sizeof(struct gc_rb_node), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->sit_page_cache) {
		goto destroy_tm_page_cache;
	}
	ctx->subbio_ctx_cache = kmem_cache_create("subbio_ctx_cache", sizeof(struct lsdm_sub_bioctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->subbio_ctx_cache) {
		goto destroy_gc_rb_node_cache;
	}
	ctx->gc_extents_cache = kmem_cache_create("gc_extents_cache", sizeof(struct gc_extents), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->gc_extents_cache) {
		goto destroy_subbio_ctx_cache;
	}
	//trace_printk("\n gc_extents_cache initialized to address: %p", ctx->gc_extents_cache);
	ctx->app_read_ctx_cache = kmem_cache_create("app_read_ctx_cache", sizeof(struct app_read_ctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->app_read_ctx_cache) {
		goto destroy_subbio_ctx_cache;
		//goto destroy_gc_extents_cache;
	}
	//trace_printk("\n app_read_ctx_cache initialized to address: %p", ctx->app_read_ctx_cache);
	return 0;
/* failed case */
//destroy_gc_extents_cache:
//	kmem_cache_destroy(ctx->gc_extents_cache);
destroy_subbio_ctx_cache:
	kmem_cache_destroy(ctx->subbio_ctx_cache);
destroy_gc_rb_node_cache:
	kmem_cache_destroy(ctx->gc_rb_node_cache);
destroy_tm_page_cache:
	kmem_cache_destroy(ctx->tm_page_cache);
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

static int ls_dm_dev_init(struct dm_target *dm_target, unsigned int argc, char **argv)
{
	int ret = -ENOMEM;
	struct ctx *ctx;
	unsigned long long max_pba;
	loff_t disk_size;

	//printk(KERN_INFO "\n argc: %d", argc);
	if (argc < 2) {
		dm_target->error = "dm-lsdm: Invalid argument count";
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
		dm_target->error = "lsdm: Device lookup failed.";
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

	spin_lock_init(&ctx->lock);
	spin_lock_init(&ctx->tm_ref_lock);
	spin_lock_init(&ctx->tm_flush_lock);
	spin_lock_init(&ctx->sit_flush_lock);
	spin_lock_init(&ctx->rev_flush_lock);
	spin_lock_init(&ctx->ckpt_lock);

	atomic_set(&ctx->io_count, 0);
	atomic_set(&ctx->n_reads, 0);
	atomic_set(&ctx->pages_alloced, 0);
	atomic_set(&ctx->nr_writes, 0);
	atomic_set(&ctx->nr_failed_writes, 0);
	atomic_set(&ctx->revmap_entry_nr, 0);
	atomic_set(&ctx->revmap_sector_nr, 0);
	ctx->target = 0;
	//trace_printk("\n About to read metadata! 1 \n");
	//trace_printk("\n About to read metadata! 2 \n");
	init_waitqueue_head(&ctx->tm_blk_flushq);
	//trace_printk("\n About to read metadata! 3 \n");
	atomic_set(&ctx->nr_pending_writes, 0);
	atomic_set(&ctx->nr_sit_pages, 0);
	atomic_set(&ctx->nr_tm_pages, 0);
	//trace_printk("\n About to read metadata! 4 \n");
	init_waitqueue_head(&ctx->refq);
	init_waitqueue_head(&ctx->rev_blk_flushq);
	init_waitqueue_head(&ctx->tm_blk_flushq);
	init_waitqueue_head(&ctx->ckptq);
	ctx->mounted_time = ktime_get_real_seconds();
	ctx->extent_tbl_root = RB_ROOT;
	ctx->rev_tbl_root = RB_ROOT;
	init_rwsem(&ctx->metadata_update_lock);

	ctx->tm_rb_root = RB_ROOT;
	ctx->sit_rb_root = RB_ROOT;
	ctx->gc_rb_root = RB_ROOT;

	rwlock_init(&ctx->sit_rb_lock);

	ctx->sectors_copied = 0;
	ret = -ENOMEM;
	dm_target->error = "dm-lsdm: No memory";

	ctx->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
	if (!ctx->page_pool)
		goto free_extent_pool;

	//printk(KERN_INFO "about to call bioset_init()");
	ctx->bs = kzalloc(sizeof(*(ctx->bs)), GFP_KERNEL);
	if (!ctx->bs)
		goto destroy_page_pool;

	if(bioset_init(ctx->bs, BS_NR_POOL_PAGES, 0, BIOSET_NEED_BVECS|BIOSET_NEED_RESCUER) == -ENOMEM) {
		//trace_printk("\n bioset_init failed!");
		goto free_bioset;
	}
	ret = create_caches(ctx);
	if (0 > ret) {
		goto uninit_bioset;
	}
	//trace_printk("\n caches created!");
	ctx->s_chksum_driver = crypto_alloc_shash("crc32c", 0, 0);
	if (IS_ERR(ctx->s_chksum_driver)) {
		//trace_printk("Cannot load crc32c driver.");
		ret = PTR_ERR(ctx->s_chksum_driver);
		ctx->s_chksum_driver = NULL;
		goto destroy_cache;
	}

	kref_init(&ctx->ongoing_iocount);
	//trace_printk("\n About to read metadata! 5 ! \n");

    	ret = read_metadata(ctx);
	if (ret < 0) 
		goto destroy_cache;

	max_pba = ctx->dev->bdev->bd_inode->i_size / 512;
	sprintf(ctx->nodename, "lsdm/%s", argv[1]);
	ret = -EINVAL;
	//printk(KERN_INFO "\n device records max_pba: %llu", max_pba);
	//printk(KERN_INFO "\n formatted max_pba: %d", ctx->max_pba);
	if (ctx->max_pba > max_pba) {
		dm_target->error = "dm-lsdm: Invalid max pba found on sb";
		goto free_metadata_pages;
	}

	//trace_printk("\n Initializing gc_extents list, ctx->gc_extents_cache: %p ", ctx->gc_extents_cache);
	ctx->gc_extents = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
	if (!ctx->gc_extents) {
		//trace_printk("\n Could not allocate gc_extent and hence could not initialized \n");
		goto free_metadata_pages;
	}
	//trace_printk("\n Extent allocated....! ctx->gc_extents: %p", ctx->gc_extents);
	INIT_LIST_HEAD(&ctx->gc_extents->list);
	/*
	 * Will work with timer based invocation later
	 * init_timer(ctx->timer);
	 */
	ret = lsdm_gc_thread_start(ctx);
	if (ret) {
		goto free_metadata_pages;
	}

	/*
	if (register_shrinker(lsdm_shrinker))
		goto stop_gc_thread;
	*/
	printk(KERN_ERR "\n ctr() done!!");
	return 0;
/* failed case */
/*stop_gc_thread:
	lsdm_gc_thread_stop(ctx);
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

/* For individual device removal */
static void ls_dm_dev_exit(struct dm_target *dm_target)
{
	struct ctx *ctx = dm_target->private;
	async_cookie_t cookie;
	struct rb_root *root = &ctx->gc_rb_root;
	//struct completion write_done;
	unsigned long timeout = 99999;

	sync_blockdev(ctx->dev->bdev);
	schedule();
	async_synchronize_full();
	//printk(KERN_ERR "\n Inside dtr! About to call flush_revmap_entries()");

	flush_revmap_entries(ctx);
	//printk(KERN_ERR "flush_revmap_entries done!");
	/* At this point we are sure that the revmap
	 * entries have made it to the disk but since the translation
	 * entries are not yet on the disk, the revmap bitmap is not
	 * yet updated and the revmap is not yet ready to be freed.
	 * 
	 * So, we need to wait for the revmap block to be completely on
	 * disk. This will update the bitmap and only after that do we
	 * want to flush the updated revmap bitmap
	 * We call flush_translation_blocks under the flush_lock so
	 * that the wait_on_revmap_block_availability does try another
	 * flush_translation_blocks path!
	 */
	flush_translation_blocks(ctx, cookie);
	flush_translation_blocks(ctx, cookie);
	//printk(KERN_ERR "\n translation blocks flushed! ");
	//wait_on_revmap_block_availability(ctx, ctx->revmap_pba);
	do_checkpoint(ctx);
	//trace_printk("\n checkpoint done!");
	/* If we are here, then there was no crash while writing out
	 * the disk metadata
	 */
	lsdm_free_rb_mappings(ctx);
	remove_gc_rb_nodes(ctx, root->rb_node);
	//init_completion(&write_done);
	async_synchronize_full();
	//wait_for_completion_timeout(&write_done, msecs_to_jiffies(timeout));
	printk(KERN_ERR "\n RB mappings freed! ");
	//printk(KERN_ERR "\n Nr of free blocks: %lld",  ctx->user_block_count);
	/* TODO : free extent page
	 * and segentries page */
	if (ctx->revmap_bm)
		put_page(ctx->revmap_bm);
	if (ctx->sb_page)
		put_page(ctx->sb_page);
	if (ctx->ckpt_page)
		put_page(ctx->ckpt_page);
	//trace_printk("\n metadata pages freed! \n");
	/* timer based gc invocation for later
	 * del_timer_sync(&ctx->timer_list);
	 */
	//trace_printk("\n caches destroyed! \n");
	lsdm_gc_thread_stop(ctx);
	//trace_printk("\n gc thread stopped! \n");
	bioset_exit(ctx->bs);
	//trace_printk("\n exited from bioset \n");
	kfree(ctx->bs);
	//trace_printk("\n bioset memory freed \n");
	mempool_destroy(ctx->page_pool);
	//trace_printk("\n memory pool destroyed\n");
	mempool_destroy(ctx->extent_pool);
	//trace_printk("\n extent_pool destroyed \n");
	dm_put_device(dm_target, ctx->dev);
	//trace_printk("\n device mapper target released\n");
	printk(KERN_ERR "\n nr_writes: %llu", atomic_read(&ctx->nr_writes));
	printk(KERN_ERR "\n nr_failed_writes: %llu", atomic_read(&ctx->nr_failed_writes));
	destroy_caches(ctx);
	printk(KERN_ERR "\n nr_writes: %llu", atomic_read(&ctx->nr_writes));
	kfree(ctx);
	//trace_printk("\n ctx memory freed!\n");
	printk(KERN_ERR "\n Goodbye World!\n");
	return;
}

int lsdm_zone_reset(struct ctx *ctx, struct bio *bio)
{
	struct block_device *bdev;
	sector_t sector;
	sector_t nr_sectors;
	//int ret;

	bdev = ctx->dev->bdev;
	sector = bio->bi_iter.bi_sector;
	nr_sectors = bio_sectors(bio);

	/* TODO: Adjust the segentries, number of free blocks,
	 * number of free zones, etc
	 * If that zone has 0 blocks then nothing much needs to be
	 * done. After that call:
	 * do_checkpoint();
	 */

	/*ret  = blkdev_reset_zones(bdev, sector, nr_sectors, GFP_KERNEL);
	if (ret)
		return ret;
	*/
	return 0;
}

/*
 * TODO: IMPLEMENT
 *
 */
int lsdm_zone_reset_all(struct ctx *ctx, struct bio *bio)
{
	return 0;
}

static int ls_dm_dev_map_io(struct dm_target *dm_target, struct bio *bio)
{
	struct ctx *ctx;
	int ret = 0;
       
	if (unlikely(!dm_target)) {
		dump_stack();
		return 0;
	}

	ctx = dm_target->private;

	if(unlikely(bio == NULL)) {
		printk(KERN_ERR "\n %s bio is null \n", __func__);
		dump_stack();
		return 0;
	}

	switch (bio_op(bio)) {
		case REQ_OP_READ:
			ret = lsdm_read_io(ctx, bio);
			break;
		case REQ_OP_WRITE:
			ret = lsdm_write_io(ctx, bio);
			break;
		/*
		case REQ_OP_ZONE_RESET:
			ret = lsdm_zone_reset(ctx, bio);
			break;
		case REQ_OP_ZONE_RESET_ALL:
			ret = lsdm_zone_reset_all(ctx, bio);
			break;
		*/
		default:
			break;
	}
	
	return (ret? ret: DM_MAPIO_SUBMITTED);
}

static struct target_type lsdm_target = {
	.name            = "lsdm",
	.version         = {1, 0, 0},
	.module          = THIS_MODULE,
	.ctr             = ls_dm_dev_init,
	.dtr             = ls_dm_dev_exit,
	.map             = ls_dm_dev_map_io,
	.status          = 0 /*lsdm_status*/,
	.prepare_ioctl   = 0 /*lsdm_prepare_ioctl*/,
	.message         = 0 /*lsdm_message*/,
	.iterate_devices = 0 /*lsdm_iterate_devices*/,
};

/* Called on module entry (insmod) */
static int __init ls_dm_init(void)
{
	int r = -ENOMEM;

	if (!(extents_slab = KMEM_CACHE(extent, 0)))
		return r;
	if ((r = dm_register_target(&lsdm_target)) < 0)
		goto fail;
	//printk(KERN_INFO "lsdm: \n %s %d", __func__, __LINE__);
	return 0;

fail:
	if (extents_slab)
		kmem_cache_destroy(extents_slab);
	return r;
}

/* Called on module exit (rmmod) */
static void __exit ls_dm_exit(void)
{
	dm_unregister_target(&lsdm_target);
	if(extents_slab)
		kmem_cache_destroy(extents_slab);
}


module_init(ls_dm_init);
module_exit(ls_dm_exit);

MODULE_DESCRIPTION(DM_NAME "log structured SMR Translation Layer");
MODULE_LICENSE("GPL");
