 /*
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

#include <linux/module.h>
#include <linux/device-mapper.h>
#include <linux/slab.h>
#include <linux/blkdev.h>
#include <linux/mempool.h>
#include <linux/slab.h>
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
#include <linux/workqueue.h>

#include "metadata.h"
#define DM_MSG_PREFIX "lsdm"
#define BIO_BUG_ON	1

#define ZONE_SZ (256 * 1024 * 1024)
#define BLK_SZ 4096
#define NR_BLKS_PER_ZONE (ZONE_SZ /BLK_SZ)

long nrpages;



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
#define GC_POOL_PAGES 65536/* pages for 4 zones - reserved for GC */

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
			printk(KERN_ERR "\n %s Searching lba: %lu, found:  (e->lba: %llu, e->pba: %llu e->len: %u)", __func__, lba, e->lba, e->pba, e->len); 
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
	//printk(KERN_ERR "\n did not find a natural match and so returning the next higher: %s ", (higher == NULL) ? "null" : "not null");
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
	unsigned int print = 0;
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
		dump_stack();
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
				kmem_cache_free(ctx->extent_cache, e);
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
				kmem_cache_free(ctx->extent_cache, next);
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
	/*
	ret = lsdm_tree_check(root);
	if (ret < 0) {
		printk(KERN_ERR"\n !!!! Corruption while Inserting: lba: %lld pba: %lld len: %d", new->lba, new->pba, new->len);
		BUG();
	} */
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
		printk(KERN_ERR "\n root: NULL! \n");
		BUG_ON(root == NULL);
	}
	BUG_ON(len == 0);

	//printk(KERN_ERR "\n lba: %lld, len: %lld ", lba, len);
	new = kmem_cache_alloc(ctx->extent_cache, GFP_KERNEL);
	if (!new) {
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
		split = kmem_cache_alloc(ctx->extent_cache, GFP_KERNEL);
		if (!split) {
			kmem_cache_free(ctx->extent_cache, new);
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
		kmem_cache_free(ctx->extent_cache, e);
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
 * extent_cache
 */
static void lsdm_free_rb_tree(struct ctx *ctx, struct rb_root *root)
{
	struct extent *e = rb_entry(root->rb_node, struct extent, rb);
	struct extent *next;

	while(e) {
		next = lsdm_rb_next(e);
		lsdm_rb_remove(ctx, root, e);
		kmem_cache_free(ctx->extent_cache, e);
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
	printk(KERN_ERR "\n %s nr_writes: %lu", __func__, atomic_read(&ctx->nr_writes));
	printk(KERN_ERR "\n %s nr_reads: %lu", __func__, atomic_read(&ctx->nr_reads));
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

void flush_translation_blocks(struct work_struct * w);

#define DEF_FLUSH_TIME 50 /* (milliseconds) */


static int flush_thread_fn(void * data)
{

	struct ctx *ctx = (struct ctx *) data;
	struct lsdm_flush_thread *flush_th = ctx->flush_th;
	wait_queue_head_t *wq = &flush_th->lsdm_flush_wait_queue;
	unsigned int wait_ms;

	wait_ms = flush_th->sleep_time; 
	printk(KERN_ERR "\n %s executing! ", __func__);
	set_freezable();
	do {
		wait_event_interruptible_timeout(*wq,
			kthread_should_stop() || freezing(current) ||
			flush_th->wake || (nrpages > 10),
			msecs_to_jiffies(wait_ms));

		if (try_to_freeze()) {
                        continue;
                }
		flush_workqueue(ctx->writes_wq);
		if (atomic_read(&ctx->nr_tm_pages)) {
			wait_ms = 50;
			flush_translation_blocks(&ctx->tb_work);
		} else {
			wait_ms = 5000;
		}
	} while(!kthread_should_stop());
	return 0;
}


int lsdm_flush_thread_start(struct ctx * ctx)
{
	struct lsdm_flush_thread * flush_th;
	dev_t dev = ctx->dev->bdev->bd_dev;
	int err=0;

	printk(KERN_ERR "\n About to start GC thread");

	flush_th = lsdm_malloc(sizeof(struct lsdm_flush_thread), GFP_KERNEL);
	if (!flush_th) {
		return -ENOMEM;
	}

	init_waitqueue_head(&flush_th->lsdm_flush_wait_queue);
	flush_th->sleep_time = DEF_FLUSH_TIME;
	flush_th->wake = 0;
	flush_th->lsdm_flush_task = kthread_run(flush_thread_fn, ctx,
			"lsdm-flush%u:%u", MAJOR(dev), MINOR(dev));

        ctx->flush_th = flush_th;
	if (IS_ERR(flush_th->lsdm_flush_task)) {
		err = PTR_ERR(flush_th->lsdm_flush_task);
		kvfree(flush_th);
		ctx->flush_th = NULL;
		return err;
	}

	printk(KERN_ERR "\n Created a lsdm flush thread ");
	return 0;	
}


int lsdm_flush_thread_stop(struct ctx *ctx)
{
	kthread_stop(ctx->flush_th->lsdm_flush_task);
	kvfree(ctx->flush_th);
	printk(KERN_ERR "\n flush thread stopped! ");
	return 0;
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
	int maxlen = (BIO_MAX_PAGES * NR_SECTORS_IN_BLK);
	int temp = 0;

	while (1) {
		temp = 0;
		if (e->len > maxlen) {
			temp = e->len - maxlen;
			e->len = maxlen;
		}
		gc_extent = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
		if (!gc_extent) {
			printk(KERN_ERR "\n Could not allocate memory to gc_extent! ");
			return -ENOMEM;
		}
		gc_extent->e.lba = e->lba;
		gc_extent->e.pba = e->pba;
		gc_extent->e.len = e->len;
		printk(KERN_ERR "\n %s lba: %llu, pba: %llu e->len: %ld maxlen: %ld temp: %d", __func__, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len, (maxlen), temp);
		INIT_LIST_HEAD(&gc_extent->list);
		/* 
		 * We always want to add the extents in a PBA increasing order
		 */
		list_add_tail(&ctx->gc_extents->list, &gc_extent->list);
		if (temp == 0) {
			break;
		}
		e->lba = e->lba + maxlen;
		e->pba = e->pba + maxlen;
		e->len = temp;
	}
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
	printk(KERN_ERR "\n %s nr_pages: %d, gc_extent->e.len: %ld s8: %d", __func__, nr_pages, gc_extent->e.len, s8);
	BUG_ON(nr_pages == 0);
	 
	for(i=0; i<nr_pages; i++) {
		//page = mempool_alloc(ctx->gc_page_pool, GFP_KERNEL);
    		page = alloc_page(__GFP_ZERO|GFP_KERNEL);
		if ((!page) || ( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0))) {
			bio_for_each_segment_all(bv, bio, iter_all) {
				//mempool_free(bv->bv_page, ctx->gc_page_pool);
				__free_pages(page, 0);
				printk(KERN_ERR "\n %s could not add page to a bio ", __func__);
				nrpages--;
				printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
			}
			bio_put(bio);
			return -ENOMEM;
		}
		nrpages++;
		printk(KERN_ERR "\n %s page added to the bio, nrpages: %llu ", __func__, nrpages);
	}
	bio->bi_iter.bi_sector = gc_extent->e.pba;
	printk(KERN_ERR "\n %s bio->bi_iter.bi_sector: %llu nr_pages: %d", __func__,  bio->bi_iter.bi_sector, nr_pages);
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
				//mempool_free(bv->bv_page, ctx->gc_page_pool);
				__free_pages(bv->bv_page, 0);
				nrpages--;
				printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
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
	struct bio * bio;

	blk_start_plug(&plug);
	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		ref = gc_extent->bio->bi_private;
		refcount_inc(ref);
		bio = gc_extent->bio;
		printk(KERN_ERR "\n %s bio::pba: %llu, bio::lba: %llu bio::len: %u", __func__, bio->bi_iter.bi_sector, gc_extent->e.lba, bio_sectors(gc_extent->bio));
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
		printk(KERN_ERR "\n %s lba: %llu, pba: %llu, len: %ld ", __func__, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
		if (setup_extent_bio(ctx, gc_extent)) {
			free_gc_list(ctx);
			printk(KERN_ERR "Low memory! TODO: Write code to free memory from translation tables etc ");
			return -1;
		}
		gc_extent->bio->bi_private = ref;
	}
	printk(KERN_ERR "\n %s gc_extents #count: %d", __func__, count);
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
void move_gc_write_frontier(struct ctx *ctx, sector_t sectors_s8);

static void add_revmap_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned int nrsectors);
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
	int ret;


	bio = gc_extent->bio;
	gc_ctx = bio->bi_private;

	bio_set_dev(bio, ctx->dev->bdev);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = ctx->warm_gc_wf_pba;
	nrsectors = bio_sectors(bio);
	printk(KERN_ERR "\n %s pba: %llu , len: %llu", __func__, ctx->warm_gc_wf_pba, nrsectors);
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
	bio_free_pages(bio);
	bio_put(bio);
	move_gc_write_frontier(ctx, s8);
	printk(KERN_ERR "\n %s write pointer adjusted! ", __func__);
	ret = lsdm_update_range(ctx, &ctx->extent_tbl_root, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
	ret = lsdm_update_range(ctx, &ctx->rev_tbl_root, gc_extent->e.pba, gc_extent->e.lba, gc_extent->e.len);
	add_revmap_entries(ctx, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
	return 0;
}

static void do_checkpoint(struct ctx *ctx);
static void mark_zone_free(struct ctx *ctx , int zonenr);

/* Since our read_extents call, overwrites could have made
 * the blocks in this zone invalid. Thus we now take a 
 * write lock and then re-read the extents metadata; else we
 * will end up writing invalid blocks and loosing the
 * overwritten data
 */
static int write_valid_gc_extents(struct ctx *ctx)
{
	int zonenr;
	sector_t pba, last_pba;
	struct extent *e = NULL;
	struct extent_entry temp;
	struct gc_extents *gc_extent, *new, *temp_ptr;
	struct list_head *list_head;
	sector_t diff;
	sector_t nr_sectors, s8, s8_new;
	struct bio *split, *bio;
	struct rb_node *node;
	int ret;
	static int count = 0;
	struct lsdm_gc_thread *gc_th = ctx->gc_th;
	wait_queue_head_t *wq = &gc_th->lsdm_gc_wait_queue;


	list_for_each(list_head, &ctx->gc_extents->list) {
		gc_extent = list_entry(list_head, struct gc_extents, list);
		/* Reverse map stores PBA as e->lba and LBA as e->pba
		 * This was done for code reuse between map and revmap
		 * Thus e->lba is actually the PBA
		 */
		down_write(&ctx->metadata_update_lock);
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
			diff = e->lba - gc_extent->e.pba;
			gc_extent->e.len = gc_extent->e.len - diff;
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
			}
			gc_extent->e.len = s8;
			bio = gc_extent->bio;
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
			new->bio = bio;
			/* Add new after gc_extent */
			list_add(&new->list, &gc_extent->list);
		}
		// else we do nothing
		gc_extent->e.pba = ctx->warm_gc_wf_pba;
		ret = write_gc_extent(ctx, gc_extent);
		up_write(&ctx->metadata_update_lock);
		if (ret) {
			/* write error! disk is in a read mode! we
			 * cannot perform any further GC
			 */
			printk(KERN_ERR "\n GC failed due to disk error! ");
			return -1;
		}
		printk(KERN_ERR "\n %s extent entries updated!", __func__);
		temp_ptr = list_next_entry(gc_extent, list);
		list_head = &temp_ptr->list;
		list_del(&gc_extent->list);
		kmem_cache_free(ctx->gc_extents_cache, gc_extent);
	}

}

/*
 * TODO: write code for FG_GC
 *
 * if err_flag is set, we have to mark the zone_to_clean erroneous.
 * This is set in the lsdm_clone_endio() path when a block cannot be
 * written to some zone because of disk error. We need to read the
 * remaining blocks from this zone and write to the destn_zone.
 *
 */
static int lsdm_gc(struct ctx *ctx, int zone_to_clean, char gc_mode, int err_flag)
{
	int zonenr;
	sector_t pba, last_pba;
	struct extent *e = NULL;
	struct extent_entry temp;
	struct gc_extents *gc_extent, *new, *temp_ptr;
	struct list_head *list_head;
	sector_t diff;
	sector_t nr_sectors, s8, s8_new;
	struct bio *split, *bio;
	struct rb_node *node;
	int ret;
	static int count = 0;
	struct lsdm_ckpt *ckpt = NULL;
	struct lsdm_gc_thread *gc_th = ctx->gc_th;
	wait_queue_head_t *wq = &gc_th->lsdm_gc_wait_queue;


	flush_workqueue(ctx->writes_wq);

	printk(KERN_ERR "\n GC thread polling after every few seconds, zone_to_clean: %u", zone_to_clean);
	
	if (zone_to_clean < 0) {
		printk(KERN_ERR "\n Nothing to clean! ");
		return -1;
	}

	return (0);


	/* Take a semaphore lock so that no two gc instances are
	 * started in parallel.
	 */
	if (!mutex_trylock(&ctx->gc_lock)) {
		printk(KERN_ERR "\n GC is already running! zone_to_clean: %u count: %d", zone_to_clean, count);
		dump_stack();
		return -1;
	}
	count = 1;
	printk(KERN_ERR "\n Running GC!! zone_to_clean: %u count: %d ", zone_to_clean, count);
	flush_workqueue(ctx->writes_wq);
again:
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
		printk(KERN_ERR "\n %s Found e, PBA: %llu, LBA: %llu, len: %u", __func__, e->lba, e->pba, e->len);
		/* In a reverse table extent, e->lba is actually the
		 * PBA and e->pba is actually the LBA. This was done
		 * for code reusability.
		 * We are storing the actual lba and pba pair in e
		 * Don't change e directly, as e belongs to the
		 * reverse map rb tree and we have the node address
		 */
		if (e->lba > last_pba)
			break;
		if (e->lba < pba) {
			/* 
			 * Overlapping e found
			 * e-------
			 * 	pba
			 */

			diff = pba - e->lba;
			/* Bug when e is non overlapping */
			BUG_ON(diff >= e->len);
			temp.pba = e->lba + diff;
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
		printk(KERN_ERR "\n %s pba: %llu, lba: %llu e->len: %ld last_pba: %lld", __func__, temp.pba, temp.lba, temp.len, last_pba);
		add_extent_to_gclist(ctx, &temp);
		pba = temp.pba + temp.len;
	}
	up_write(&ctx->metadata_update_lock);

	if (gc_mode == BG_GC) {
		/* Wait here till the system becomes IO Idle, if system is
		 * iodle don't wait */
		wait_event_interruptible_timeout(*wq, is_lsdm_ioidle(ctx) ||
			kthread_should_stop() || freezing(current) ||
			gc_th->gc_wake,
			msecs_to_jiffies(gc_th->urgent_sleep_time));
	}

	/* This segment has valid extents, thus there should be
	 * atleast one node in the gc list. An empty list indicates
	 * the reverse translation map does not match with the segment
	 * information table
	 */
	BUG_ON(list_empty(&ctx->gc_extents->list));
	/* Sort the list on LBAs. Write increasing LBAs in the same
	 * order
	 */
	ret = read_gc_extents(ctx);
	if (ret)
		goto failed;

	if (gc_mode == BG_GC) {
		/* Wait here till the system becomes IO Idle, if system is
		 * iodle don't wait */
		wait_event_interruptible_timeout(*wq, is_lsdm_ioidle(ctx) ||
			kthread_should_stop() || freezing(current) ||
			gc_th->gc_wake,
			msecs_to_jiffies(gc_th->urgent_sleep_time));
	}

	printk(KERN_ERR "%s:%d All GC extents read! \n", __func__, __LINE__);

	/*
	ret = write_valid_gc_extents(ctx);
	if (ret < 0)
		goto failed;
	ckpt = (struct lsdm_ckpt *)page_address(ctx->ckpt_page);
	ckpt->clean = 0;
	do_checkpoint(ctx);
	*/
	/* Complete the GC and then sync the block device */
	sync_blockdev(ctx->dev->bdev);
	/* Zone gets freed when the valid blocks count is adjusted and
	 * becomes zero. This happens on writes.
	 */

	//printk(KERN_ERR "\n %s ctx:%llu, &ctx->gc_lock:%llu ", ctx, &ctx->gc_lock);
	node = rb_first(&ctx->gc_rb_root);
	if (node) {
		rb_erase(node, &ctx->gc_rb_root);
		kmem_cache_free(ctx->gc_rb_node_cache, node);
	}
	printk(KERN_ERR "\n %s removed zone from the gc_rb_root tree", __func__);

	/* TODO: Mode: FG_GC */
	if (gc_mode == FG_GC) {
		/* check if you have hit w1 or else keep cleaning */
		if (ctx->nr_freezones <= ctx->w1) {
			zone_to_clean = select_zone_to_clean(ctx, FG_GC);	
			goto again;
		}

	}
/* Remove the zone from the gc_rb_root */

failed:
	/* Release GC lock */
	mutex_unlock(&ctx->gc_lock);
	printk(KERN_ERR "\n %s going out! \n", __func__);
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
	struct lsdm_gc_thread *gc_th = ctx->gc_th;
	wait_queue_head_t *wq = &gc_th->lsdm_gc_wait_queue;
	unsigned int wait_ms;
	int zonenr, newzone;

	//wait_ms = gc_th->min_sleep_time * 10;
	wait_ms = gc_th->min_sleep_time; 
	mutex_init(&ctx->gc_lock);
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
		flush_workqueue(ctx->writes_wq);
		//if(!is_lsdm_ioidle(ctx)) {
			/* increase sleep time */
		//	printk(KERN_ERR "\n %s not ioidle! \n ", __func__);
		//	wait_ms = wait_ms * 2;
			/* unlock mutex */
		//	continue;
		//}
		/* We sleep again and see if we are still idle, then
		 * we declare "io idleness" and perform BG GC
		 */
		/*
		wait_event_interruptible_timeout(*wq,
			kthread_should_stop() || freezing(current) ||
			gc_th->gc_wake,
			msecs_to_jiffies(gc_th->urgent_sleep_time));
		*/
		//if(!is_lsdm_ioidle(ctx)) {
			/* increase sleep time */
		//	printk(KERN_ERR "\n %s not ioidle! \n ", __func__);
		//	wait_ms = wait_ms * 2;
			/* unlock mutex */
		//	continue;
		//}
		//printk(KERN_ERR "\n %s ioidle = 1! \n ", __func__);
		zonenr = select_zone_to_clean(ctx, BG_GC);
		printk(KERN_ERR "\n Selecting zonenr: %d \n", zonenr);
		/* Doing this for now! ret part */
		if (zonenr >= 0) {
			lsdm_gc(ctx, zonenr, newzone, 0);
			printk(KERN_ERR "\n zone: %d marked free! \n", zonenr);
		} /*else {
		    wait_ms = wait_ms* 2;
		    continue;
		} */
	} while(!kthread_should_stop());
	return 0;
}
#define DEF_GC_THREAD_URGENT_SLEEP_TIME 500     /* 500 ms */
#define DEF_GC_THREAD_MIN_SLEEP_TIME    90000   /* milliseconds */
#define DEF_GC_THREAD_MAX_SLEEP_TIME    120000
#define DEF_GC_THREAD_NOGC_SLEEP_TIME   150000  /* wait 2 min */
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


void lsdm_ioidle(struct kref *kref)
{
	struct ctx *ctx;

	ctx = container_of(kref, struct ctx, ongoing_iocount);
	atomic_set(&ctx->ioidle, 1);
	//printk(KERN_ERR "\n Stl is io idle!", __func__);
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
	ctx->timer.function = invoke_gc;
	ctx->timer.data = (unsigned long) ctx;
	ctx->timer.expired = TIME_IDLE_JIFFIES;
	add_timer(ctx->timer);
	*/
}


void no_op(struct kref *kref) { }

void lsdm_subread_done(struct bio *clone)
{
	struct app_read_ctx *read_ctx = clone->bi_private;
	struct ctx *ctx = read_ctx->ctx;
	struct bio * bio = read_ctx->bio;

	bio_endio(bio);
	kref_put(&ctx->ongoing_iocount, lsdm_ioidle);
	bio_put(clone);
	kmem_cache_free(ctx->app_read_ctx_cache, read_ctx);
}

static int zero_fill_clone(struct ctx *ctx, struct app_read_ctx *read_ctx, struct bio *clone) 
{
	bio_set_dev(clone, ctx->dev->bdev);
	zero_fill_bio(clone);
	clone->bi_private = read_ctx;
	clone->bi_end_io = lsdm_subread_done;
	/* This bio could be the parent of other
	 * chained bios. Its necessary to call
	 * bio_endio and not the endio function
	 * directly
	 */
	bio_endio(clone);
	atomic_inc(&ctx->nr_reads);
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
	unsigned nr_sectors, overlap, diff;

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

	read_ctx->ctx = ctx;
	read_ctx->bio = bio;

	atomic_set(&ctx->ioidle, 0);
	clone = bio_clone_fast(bio, GFP_KERNEL, NULL);
	if (!clone) {
		printk(KERN_ERR "\n %s could not allocate memory to clone", __func__);
		kmem_cache_free(ctx->app_read_ctx_cache, read_ctx);
		bio->bi_status = -ENOMEM;
		bio_endio(bio);
		return -ENOMEM;
	}

	bio_set_dev(clone, ctx->dev->bdev);
	nr_sectors = bio_sectors(clone);
	split = NULL;

	while(split != clone) {
		nr_sectors = bio_sectors(clone);
		lba = clone->bi_iter.bi_sector;
		e = lsdm_rb_geq(ctx, lba, print);

		/* case of no overlap, technically, e->lba + e->len cannot be less than lba
		 * because of the lsdm_rb_geq() design
		 */
		if ((e == NULL) || (e->lba >= lba + nr_sectors) || (e->lba + e->len <= lba))  {
			zero_fill_clone(ctx, read_ctx, clone);
			break;
		}
		//printk(KERN_ERR "\n %s Searching: %llu len: %lu. Found e->lba: %llu, e->pba: %llu, e->len: %lu", __func__, lba, nr_sectors, e->lba, e->pba, e->len);
		/* Case of Overlap, e always overlaps with bio */
		if (e->lba > lba) {
		/*   		 [eeeeeeeeeeee]
		 *	[---------bio------] 
		 */
			nr_sectors = e->lba - lba;
			//printk(KERN_ERR "\n 1.  %s e->lba: %llu >  lba: %llu split_sectors: %d \n", __func__, e->lba, lba, nr_sectors);
			split = bio_split(clone, nr_sectors, GFP_NOIO, ctx->bs);
			if (!split) {
				printk(KERN_ERR "\n Could not split the clone! ERR ");
				bio->bi_status = -ENOMEM;
				clone->bi_status = BLK_STS_RESOURCE;
				zero_fill_clone(ctx, read_ctx, clone);
				return -ENOMEM;
			}
			bio_chain(split, clone);
			zero_fill_bio(split);
			bio_endio(split);
			/* bio is front filled with zeroes, but we
			 * need to compare 'clone' now with the same e
			 */
			nr_sectors = bio_sectors(clone);
			lba = clone->bi_iter.bi_sector;
			/* we fall through as e->lba == lba now */
		} 
		//(e->lba <= lba) 
		/* [eeeeeeeeeeee] eeeeeeeeeeeee]<- could be shorter or longer
		 */
		/*  [---------bio------] */
		overlap = e->lba + e->len - lba;
		diff = lba - e->lba;
		pba = e->pba + diff;
		if (overlap >= nr_sectors) { 
		/* e is bigger than bio, so overlap >= nr_sectors, no further
		 * splitting is required. Previous splits if any, are chained
		 * to the last one as 'clone' is their parent.
		 */
			atomic_inc(&ctx->nr_reads);
			clone->bi_private = read_ctx;
			clone->bi_end_io = lsdm_subread_done;
			clone->bi_iter.bi_sector = pba;
			bio_set_dev(clone, ctx->dev->bdev);
			//printk(KERN_ERR "\n 3* (FINAL) %s lba: %llu, nr_sectors: %d e->lba: %llu e->pba: %llu e->len:%llu ", __func__, lba, bio_sectors(clone), e->lba, e->pba, e->len);
			////printk(KERN_ERR "\n %s {lba: %llu, pba: %llu, len: %llu}",  __func__, lba, pba, e->len);
			submit_bio(clone);
			break;
		}
		/* else e is smaller than bio */
		split = bio_split(clone, overlap, GFP_NOIO, ctx->bs);
		if (!split) {
			printk(KERN_INFO "\n Could not split the clone! ERR ");
			bio->bi_status = -ENOMEM;
			clone->bi_status = BLK_STS_RESOURCE;
			zero_fill_clone(ctx, read_ctx, clone);
			return -ENOMEM;
		}
		//printk(KERN_ERR "\n 2. (SPLIT) %s lba: %llu, pba: %llu nr_sectors: %d e->lba: %llu e->pba: %llu e->len:%llu ", __func__, lba, pba, bio_sectors(split), e->lba, e->pba, e->len);
		bio_chain(split, clone);
		split->bi_iter.bi_sector = pba;
		bio_set_dev(split, ctx->dev->bdev);
		submit_bio(split);
		/* Since e was smaller, we want to search for the next e */
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
	printk(KERN_ERR "\n %s marking disk full! ", __func__);
	dump_stack();
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
	
	*parent = NULL;

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
void flush_sit(struct work_struct *);
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

	ptr = (struct lsdm_seg_entry *)sit_page->page;
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = BLOCKS_IN_ZONE;
	ptr->mtime = 0;

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

	if (atomic_read(&ctx->sit_flush_count) >= MAX_SIT_PAGES) {
		if (index == (SIT_ENTRIES_BLK-1)) {
			atomic_set(&ctx->sit_flush_count, 0);
			/* We dont want to wait for the flush to complete,
			 * just initiate here
			 */
			INIT_WORK(&ctx->sit_work, flush_sit);
			//queue_work(ctx->writes_wq, &ctx->sit_work);
			flush_sit(&ctx->sit_work);
		}
	}
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
		
	spin_lock(&ctx->lock);
	bitmap = ctx->freezone_bitmap;
	bytenr = zonenr / BITS_IN_BYTE;
	bitnr = zonenr % BITS_IN_BYTE;
	//printk(KERN_ERR "\n %s zonenr: %lu, bytenr: %d bitnr: %d bitmap[%d]: %d", __func__, zonenr, bytenr, bitnr,  bytenr, bitmap[bytenr]);

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

	spin_unlock(&ctx->lock);
	

	//printk(KERN_ERR "\n %s ctx->nr_freezones: %llu", __func__, ctx->nr_freezones);
}

static void mark_zone_gc_candidate(struct ctx *ctx , int zonenr)
{
	char *bitmap = ctx->gc_zone_bitmap;
	int bytenr = zonenr / BITS_IN_BYTE;
	int bitnr = zonenr % BITS_IN_BYTE;

	//printk(KERN_ERR "\n %s zonenr: %lu, bytenr: %d bitnr: %d bitmap[%d]: %d", __func__, zonenr, bytenr, bitnr,  bytenr, bitmap[bytenr]);

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
	int trial, ret = 0;

	trial = 0;
try_again:
	zone_nr = get_next_freezone_nr(ctx);
	if (zone_nr < 0) {
		toclean = select_zone_to_clean(ctx, FG_GC);
		if (toclean != -1) {
			printk(KERN_ERR "\n Could not find a clean zone for writing. Calling lsdm_gc on zonenr: %u \n", toclean);
			lsdm_gc(ctx, toclean, FG_GC, 0);
			if (0 == trial) {
				trial++;
				goto try_again;
			}
		}
		printk(KERN_INFO "No more disk space available for writing!");
		mark_disk_full(ctx);
		ctx->hot_wf_pba = 0;
		return (ctx->hot_wf_pba);
	}

	if (ctx->nr_freezones <= ctx->w2) {
		toclean = select_zone_to_clean(ctx, FG_GC);
		if (toclean != -1) {
			lsdm_gc(ctx, toclean, FG_GC, 0);
		}
	}


	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->hot_wf_pba = ctx->sb->zone0_pba + (zone_nr << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	ctx->hot_wf_end = zone_end(ctx, ctx->hot_wf_pba);

	//printk(KERN_ERR "\n !!!!!!!!!!!!!!! get_new_zone():: zone0_pba: %u zone_nr: %d hot_wf_pba: %llu, wf_end: %llu", ctx->sb->zone0_pba, zone_nr, ctx->hot_wf_pba, ctx->hot_wf_end);
	if (ctx->hot_wf_pba > ctx->hot_wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %lld", ctx->free_sectors_in_wf );
	}
	ctx->free_sectors_in_wf = ctx->hot_wf_end - ctx->hot_wf_pba + 1;
	//printk(KERN_ERR "\n %s ctx->nr_freezones: %llu", __func__, ctx->nr_freezones);
	mark_zone_occupied(ctx, zone_nr);
	add_ckpt_new_wf(ctx, ctx->hot_wf_pba);
	return ctx->hot_wf_pba;
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
		ctx->hot_wf_pba = -1;
		return (ctx->hot_wf_pba);
	}

	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->warm_gc_wf_pba = ctx->sb->zone0_pba + (zone_nr << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	ctx->warm_gc_wf_end = zone_end(ctx, ctx->warm_gc_wf_pba);
	printk("\n !!!!!!!!!!!!!!! get_new_gc_zone():: zone0_pba: %u zone_nr: %ld warm_gc_wf_pba: %llu, gc_wf_end: %llu", ctx->sb->zone0_pba, zone_nr, ctx->warm_gc_wf_pba, ctx->warm_gc_wf_end);
	if (ctx->warm_gc_wf_pba > ctx->warm_gc_wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %llu", ctx->free_sectors_in_wf );
	}
	ctx->free_sectors_in_gc_wf = ctx->warm_gc_wf_end - ctx->warm_gc_wf_pba + 1;
	add_ckpt_new_gc_wf(ctx, ctx->warm_gc_wf_pba);
	return ctx->warm_gc_wf_pba;
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
	ckpt->hot_frontier_pba = wf;
}

static void add_ckpt_new_gc_wf(struct ctx * ctx, sector_t wf)
{
	struct lsdm_ckpt *ckpt = ctx->ckpt;
	ckpt->warm_gc_frontier_pba = wf;
}

/*
 * Since bitmap can be created in case of a crash, we do not 
 * wait for the bitmap to be flushed.
 * We only initiate the flush for the bitmap.
 * DO not free revmap_bitmap_page. Do it exit function.
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
	bio->bi_iter.bi_sector = pba;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	/* Not doing this right now as the conventional zones are too little.
	 * we need to log structurize the metadata
	 */
	//submit_bio_wait(bio);

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
	return;
}

/* We initiate a flush of a checkpoint,
 * DO not free ckpt page. Do it exit function.
 */
void flush_checkpoint(struct ctx *ctx)
{
	struct bio * bio;
	struct page *page;
	sector_t pba;
	struct lsdm_ckpt * ckpt;

	page = ctx->ckpt_page;
	if (!page) {
		printk(KERN_ERR "\n %s no ckpt_page! ");
		return;
	}

	ckpt = (struct lsdm_ckpt *)page_address(page);
	/* TODO: GC will not change the checkpoint, but will change
	 * the SIT Info.
	 */
	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		printk(KERN_ERR "\n %s could not alloc bio ", __func__);
		return;
	}

	/* bio_add_page sets the bi_size for the bio */
	if (PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		printk(KERN_ERR "\n %s could not add page! ", __func__);
		bio_put(bio);
		return;
	}
	printk("\n %s ckpt1_pba: %llu, ckpt2_pba: %llu", __func__, ctx->sb->ckpt1_pba, ctx->sb->ckpt2_pba);
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
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	printk(KERN_ERR "\n flushing checkpoint at pba: %llu", pba);
	bio_set_dev(bio, ctx->dev->bdev);

	/* Not doing this right now as the conventional zones are too little.
	 * we need to log structurize the metadata
	 */
	//submit_bio_wait(bio);
	/* we do not free ckpt page, because we need it. we free it in
	 * dtr()
	 */

	bio_put(bio);

	if (BLK_STS_OK != bio->bi_status) {
		/* TODO: Do something more to handle the errors */
		printk(KERN_DEBUG "\n Could not write the checkpoint to disk");
	}
	return;
}


void flush_sit_node_page(struct ctx * ctx, struct rb_node *);


void flush_sit_nodes(struct ctx *ctx, struct rb_node *node)
{	
	if (!node) {
		//printk(KERN_ERR "\n %s Sit node is null, returning", __func__);
		return;
	}
	flush_sit_node_page(ctx, node);
	if (node->rb_left)
		flush_sit_nodes(ctx, node->rb_left);
	if (node->rb_right)
		flush_sit_nodes(ctx, node->rb_right);
}

void free_sit_pages(struct ctx *);


/*
 * We need to maintain the sit entries in some pages and keep the
 * pages in a linked list.
 * We can then later flush all the sit page in one go
 */
void flush_sit(struct work_struct *w)
{
	struct ctx *ctx = container_of(w, struct ctx, sit_work);
	struct rb_root *rb_root = &ctx->sit_rb_root;
	struct blk_plug plug;



	if (!rb_root->rb_node) {
		printk(KERN_ERR "\n %s Sit node is null, returning", __func__);
		return;
	}
	if (!atomic_read(&ctx->sit_ref)) {
		atomic_set(&ctx->sit_ref, 1);
	}

	if (!mutex_trylock(&ctx->sit_flush_lock))
		return;

	free_sit_pages(ctx);
	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	blk_start_plug(&plug);
	flush_sit_nodes(ctx, rb_root->rb_node);
	blk_finish_plug(&plug);

	mutex_unlock(&ctx->sit_flush_lock);
	atomic_dec(&ctx->sit_ref);
	wait_event(ctx->sitq, (!atomic_read(&ctx->sit_ref)));
	/* When all the nodes are flushed we are here */
}

static void remove_gc_rb_nodes(struct ctx *, struct rb_node *);

static void remove_gc_rb_nodes(struct ctx *ctx, struct rb_node *node)
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
	printk(KERN_ERR "\n %s ckpt->user_block_count = %lld version: %d ", __func__, ctx->user_block_count, ckpt->version);
	ckpt->nr_invalid_zones = ctx->nr_invalid_zones;
	ckpt->hot_frontier_pba = ctx->hot_wf_pba;
	//trace_printk("\n %s, ckpt->hot_frontier_pba: %llu version: %lld", __func__, ckpt->hot_frontier_pba, ckpt->version);
	ckpt->warm_gc_frontier_pba = ctx->warm_gc_wf_pba;
	ckpt->nr_free_zones = ctx->nr_freezones;
	//printk(KERN_ERR "\n %s ckpt->nr_free_zones: %llu, ctx->nr_freezones: %llu", __func__, ckpt->nr_free_zones, ctx->nr_freezones);
	ckpt->elapsed_time = get_elapsed_time(ctx);
	ckpt->clean = 1;
	return;
	//ckpt->crc = calculate_crc(ctx, page);
}

/* Always called with the ctx->lock held
 */
void move_write_frontier(struct ctx *ctx, sector_t sectors_s8)
{
	/* We should have adjusted sectors_s8 to accomodate
	 * for the rooms in the zone before calling this function.
	 * Its how we split the bio
	 */
	if (ctx->free_sectors_in_wf < sectors_s8) {
		panic("Wrong manipulation of wf; used unavailable sectors in a log");
	}

	ctx->hot_wf_pba = ctx->hot_wf_pba + sectors_s8;
	ctx->free_sectors_in_wf = ctx->free_sectors_in_wf - sectors_s8;
	ctx->user_block_count -= sectors_s8 / NR_SECTORS_IN_BLK;
	if (ctx->free_sectors_in_wf < NR_SECTORS_IN_BLK) {
		//printk(KERN_INFO "Num of free sect.: %llu, about to call get_new_zone() \n", ctx->free_sectors_in_wf);
		if ((ctx->hot_wf_pba - 1) != ctx->hot_wf_end) {
			printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->hot_wf_pba, ctx->hot_wf_end);
			BUG_ON(ctx->hot_wf_pba != (ctx->hot_wf_end + 1));
		}
		get_new_zone(ctx);
		if (ctx->hot_wf_pba == 0) {
			panic("No more disk space available for writing!");
		}
	}
}

/* Always called with the ctx->lock held
 */
void move_gc_write_frontier(struct ctx *ctx, sector_t sectors_s8)
{
	/* We should have adjusted sectors_s8 to accomodate
	 * for the rooms in the zone before calling this function.
	 * Its how we split the bio
	 */
	if (ctx->free_sectors_in_gc_wf < sectors_s8) {
		panic("Wrong manipulation of gc wf; used unavailable sectors in a log");
	}	
	
	ctx->warm_gc_wf_pba = ctx->warm_gc_wf_pba + sectors_s8;
	ctx->free_sectors_in_gc_wf = ctx->free_sectors_in_gc_wf - sectors_s8;
	if (ctx->free_sectors_in_gc_wf < NR_SECTORS_IN_BLK) {
		if ((ctx->warm_gc_wf_pba - 1) != ctx->warm_gc_wf_end) {
			printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->warm_gc_wf_pba, ctx->warm_gc_wf_end);
			BUG_ON(ctx->warm_gc_wf_pba != (ctx->warm_gc_wf_end + 1));
		}
		get_new_gc_zone(ctx);
		if (ctx->warm_gc_wf_pba < 0) {
			panic("No more disk space available for writing!");
		}
	}
}

int is_revmap_block_available(struct ctx *ctx, u64 pba);
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

	spin_lock(&ctx->rev_flush_lock);
	/* wait until atleast one zone's revmap entries are flushed */
	wait_event_lock_irq(ctx->rev_blk_flushq, (1 == is_revmap_block_available(ctx, pba)), ctx->rev_flush_lock);
	spin_unlock(&ctx->rev_flush_lock);
}

struct page * read_block(struct ctx *, u64 , u64 );
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

	//printk(KERN_ERR "\n %s Inside! ", __func__);

	new = search_sit_kv_store(ctx, pba, &parent);
	if (new) {
		//printk("\n %s FOUND!! sit_blknr: %lld sit_pba: %u", __func__, sit_blknr, ctx->sb->sit_pba);
		return new;
	}

	new = kmem_cache_alloc(ctx->sit_page_cache, GFP_KERNEL);
	if (!new)
		return NULL;

	RB_CLEAR_NODE(&new->rb);
	sit_blknr = zonenr / SIT_ENTRIES_BLK;
	printk("\n %s sit_blknr: %lld sit_pba: %u", __func__, sit_blknr, ctx->sb->sit_pba);
	page = read_block(ctx, ctx->sb->sit_pba, (sit_blknr * NR_SECTORS_IN_BLK));
	if (!page) {
		kmem_cache_free(ctx->sit_page_cache, new);
		return NULL;
	}
	//printk("\n %s zonenr: %lld page: %p ", __func__, zonenr, page_address(page));
	new->flag = NEEDS_FLUSH;
	new->blknr = sit_blknr;
	new->page = page;
	ptr = (struct lsdm_seg_entry *) page_address(page);
	//printk(KERN_ERR "\n %s ptr->vblocks: %lu ", __func__, ptr->vblocks);
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
		rb_link_node(&new->rb, NULL, &root->rb_node);
	}
	/* Balance the tree after the blknr is addded to it */
	rb_insert_color(&new->rb, root);
	
	atomic_inc(&ctx->nr_sit_pages);
	atomic_inc(&ctx->sit_flush_count);

	//trace_printk("\n %s pba: %llu page_address: %p", __func__, pba, page_address(new->page));
	return new;
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

	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = ptr->vblocks - 1;
	/* Send the older vblocks, mtime along with the new vblocks,mtime to this
	 * function. The older vblocks, mtime is used to calculated
	 * the older cost which is stored in the tree. The newer ones
	 * on the other hand are used to modify the tree
	 */
	if ((zonenr != get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba)) &&
		    (zonenr != get_zone_nr(ctx, ctx->ckpt->warm_gc_frontier_pba))) {
		printk(KERN_ERR "\n updating gc rb zonenr: %d nrblks: %d", zonenr, ptr->vblocks);
		/* add mtime here */
		ptr->mtime = get_elapsed_time(ctx);
		if (ctx->max_mtime < ptr->mtime)
			ctx->max_mtime = ptr->mtime;
		if (ptr->vblocks)
			update_gc_rb_tree(ctx, zonenr, ptr->vblocks, ptr->mtime);

		//printk(KERN_ERR "\n %s done! vblocks: %d pba: %lu, index: %d , ptr: %p", __func__, ptr->vblocks, pba, index, ptr);
	}
	if (!ptr->vblocks) {
		int ret = 0;
		u64 sector_pba = 0;
		mark_zone_free(ctx , zonenr);
		/* we need to reset the  zone that we are about to use */
		sector_pba = get_first_pba_for_zone(ctx, zonenr);
		ret = blkdev_reset_zones(ctx->dev->bdev, sector_pba, (1<< (ctx->sb->log_zone_size - ctx->sb->log_sector_size)),GFP_NOIO);
		if (ret) {
			printk(KERN_ERR "\n Zone reset for zonenr: %llu failed! ");
			/* TODO: chose another zone, mark this failed! but for now
			 * doing nothing */
		}
	}
	up(&ctx->sit_kv_store_lock);


	if (atomic_read(&ctx->sit_flush_count) >= MAX_SIT_PAGES) {
		if (index == (SIT_ENTRIES_BLK - 1)) {
			atomic_set(&ctx->sit_flush_count, 0);
			/* We dont want to wait for the flush to complete,
			 * just initiate here
			 */
			INIT_WORK(&ctx->sit_work, flush_sit);
			//queue_work(ctx->writes_wq, &ctx->sit_work);
			flush_sit(&ctx->sit_work);
		}
	}
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

	down_interruptible(&ctx->sit_kv_store_lock);
	sit_page = add_sit_page_kv_store(ctx, pba);
	if (!sit_page) {
		/* TODO: do something, low memory */
		panic("Low memory, could not allocate sit_entry");
	}
	up(&ctx->sit_kv_store_lock);

	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	//trace_printk("\n %s: pba: %llu zonenr: %llu", __func__, pba, zonenr);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = ptr->vblocks + 1;
	vblocks = ptr->vblocks;
	if(vblocks > BLOCKS_IN_ZONE) {
		if (!print) {
			printk(KERN_ERR "\n !!!!!!!!!!!!!!!!!!!!! zone: %llu has more than %d blocks! ", zonenr, ptr->vblocks);
			print = 0;
			panic("Zone has more blocks that it should!");
		}
	} /* else {
		printk(KERN_ERR "\n  zone: %lu has %d blocks! pba: %llu index: %u ptr: %p to be flushed at: %llu", zonenr, vblocks, pba, index, ptr, sit_page->blknr);
	} */


	if (atomic_read(&ctx->sit_flush_count) >= MAX_SIT_PAGES) {
		if (index == (SIT_ENTRIES_BLK - 1)) {
			atomic_set(&ctx->sit_flush_count, 0);
			/* We dont want to wait for the flush to complete,
			 * just initiate here
			 */
			INIT_WORK(&ctx->sit_work, flush_sit);
			//queue_work(ctx->writes_wq, &ctx->sit_work);
			flush_sit(&ctx->sit_work);
		}
	}

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
	ptr = (struct lsdm_seg_entry*) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	/* Send the older vblocks, mtime along with the new vblocks,mtime to this
	 * function. The older vblocks, mtime is used to calculated
	 * the older cost which is stored in the tree. The newer ones
	 * on the other hand are used to modify the tree
	 */
	ptr->mtime = get_elapsed_time(ctx);
	if (ctx->max_mtime < ptr->mtime)
		ctx->max_mtime = ptr->mtime;
	up(&ctx->sit_kv_store_lock);


	if (atomic_read(&ctx->sit_flush_count) >= MAX_SIT_PAGES) {
		if (index == (SIT_ENTRIES_BLK - 1)) {
			atomic_set(&ctx->sit_flush_count, 0);
			/* We dont want to wait for the flush to complete,
			 * just initiate here
			 */
			INIT_WORK(&ctx->sit_work, flush_sit);
			//queue_work(ctx->writes_wq, &ctx->sit_work);
			flush_sit(&ctx->sit_work);
		}
	}
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
	int index; 

	BUG_ON(pba == 0);
	ptr = (struct tm_entry *) page_address(page);
	index = (lba/NR_SECTORS_IN_BLK);
	index = index  %  TM_ENTRIES_BLK;
	ptr = ptr + index;
	printk(KERN_ERR "\n 1. ptr->lba: %llu, ptr->pba: %llu", ptr->lba, ptr->pba);
	/*-----------------------------------------------*/
	//BUG_ON ((ptr->lba == lba) && (ptr->pba == pba))
	/* lba can be 0, but pba cannot be */
	if (ptr->pba != 0) {
		/* decrement vblocks for the segment that has
		 * the stale block
		 */
		printk(KERN_ERR "\n Overwrite a block at LBA: %llu, PBA: %llu", ptr->lba, ptr->pba);
		sit_ent_vblocks_decr(ctx, ptr->pba);
	}
	ptr->lba = lba;
	ptr->pba = pba;
	printk(KERN_ERR "\n 2. ptr->lba: %llu, ptr->pba: %llu", ptr->lba, ptr->pba);
	sit_ent_vblocks_incr(ctx, ptr->pba);
	if (pba == zone_end(ctx, pba)) {
		sit_ent_add_mtime(ctx, ptr->pba);
	}

	return 0;	
}

/*
 * Note that blknr is 4096 bytes aligned. Whereas our 
 * LBA is 512 bytes aligned. So we convert the blknr
 *
 * TODO: implement the reading of more blks.
 */
struct page * read_block(struct ctx *ctx, u64 base, u64 sectornr)
{
	struct bio * bio;
	struct page *page;

	u64 pba = base + sectornr;
	if (pba > ctx->max_pba) {
		dump_stack();
		printk(KERN_ERR "\n %s base: %llu, sectornr: %llu \n", __func__, base, sectornr);
		return NULL;
	}
    	page = alloc_page(__GFP_ZERO|GFP_KERNEL);
	if (!page )
		return NULL;

	nrpages++;
	printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return NULL;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		__free_pages(page, 0);
		bio_put(bio);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return NULL;
	}
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_iter.bi_sector = pba;
	bio_set_dev(bio, ctx->dev->bdev);

	submit_bio_wait(bio);
	//printk(KERN_ERR "\n read a block from pba: %llu", pba);
	if (bio->bi_status != BLK_STS_OK) {
		printk(KERN_ERR "\n Could not read the block");
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

	*parent = NULL;
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

void remove_translation_pages(struct ctx *ctx)
{
	struct tm_page *tm_page;
	struct page *page;
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = root->rb_node;

	//printk(KERN_ERR "\n Inside %s ", __func__);
	
	while(node) {
		tm_page = rb_entry(node, struct tm_page, rb);
		if (!tm_page)
			return;
		//if (tm_page->flag == NEEDS_NO_FLUSH) {
			rb_erase(node, &ctx->tm_rb_root);
			if (!tm_page->page) {
				panic("Page cannot be null now! ");
				return;
			}
			__free_pages(tm_page->page, 0);
			nrpages--;
			kmem_cache_free(ctx->tm_page_cache, tm_page);
			atomic_dec(&ctx->nr_tm_pages);
		//}
		node = root->rb_node;
	}
	printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
	return;
}

void free_translation_pages(struct ctx *ctx)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node;

	node = root->rb_node;
	if (!node) {
		//printk(KERN_ERR "\n %s tm node is NULL!", __func__);
		return;
	}
	down_interruptible(&ctx->tm_kv_store_lock);
	remove_translation_pages(ctx);
	up(&ctx->tm_kv_store_lock);
}

void remove_sit_pages(struct ctx *ctx, struct rb_node *node)
{
	struct rb_node *left=NULL, *right=NULL;
	struct sit_page *sit_page;
	struct page *page;

	if (!node)
		return;

	left = node->rb_left;
	right = node->rb_right;

	sit_page = rb_entry(node, struct sit_page, rb);
	if (!sit_page) {
		up(&ctx->sit_kv_store_lock);
		printk(KERN_ERR "\n sit_page is NULL!");
		return;
	}

	if (sit_page->flag == NEEDS_FLUSH) {
		up(&ctx->sit_kv_store_lock);
		return;
	}

	page = sit_page->page;
	if (!page) {
		up(&ctx->sit_kv_store_lock);
		return;
	}
	sit_page->page = NULL;
	__free_pages(page, 0);
	nrpages--;
	printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
	rb_erase(&sit_page->rb, &ctx->sit_rb_root);
	kmem_cache_free(ctx->sit_page_cache, sit_page);

	if (left)
		remove_sit_pages(ctx, left);
	if (right)
		remove_sit_pages(ctx, right);

	printk(KERN_ERR "\n %s: Page freed! ", __func__);

	return;

}

void free_sit_pages(struct ctx *ctx)
{
	struct rb_root *root = &ctx->sit_rb_root;

	if (!root->rb_node) {
		printk(KERN_ERR "\n %s sit root is NULL!", __func__);
		return;
	}
	down_interruptible(&ctx->sit_kv_store_lock);
	remove_sit_pages(ctx, root->rb_node);
	up(&ctx->sit_kv_store_lock);
}

/*
 * Note: 
 * When some translation pages collect in memory, they are
 * flushed to disk.
 */
void write_tmbl_complete(struct bio *bio)
{
	struct ctx *ctx;
	struct tm_page_write_ctx *tm_page_write_ctx;
	struct page *page;

	//cant_sleep();
	tm_page_write_ctx = (struct tm_page_write_ctx *) bio->bi_private;
	ctx = tm_page_write_ctx->ctx;
	page = tm_page_write_ctx->tm_page->page;

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
	kmem_cache_free(ctx->tm_page_write_ctx_cache, tm_page_write_ctx);
	atomic_dec(&ctx->tm_ref);
	wake_up(&ctx->tmq);
	return;
}

/* We don't wait for the bios to complete 
 * flushing. We only initiate the flushing
 */
void flush_tm_node_page(struct ctx *ctx, struct tm_page * tm_page)
{
	struct page *page; 
	u64 pba;
	struct bio * bio;
	struct tm_page_write_ctx *tm_page_write_ctx;
	loff_t disk_size;

	page = tm_page->page;
	if (!page)
		return;

	tm_page_write_ctx = kmem_cache_alloc(ctx->tm_page_write_ctx_cache, GFP_KERNEL);
	if (!tm_page_write_ctx) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return;
	}

	tm_page_write_ctx->ctx = ctx;
	tm_page_write_ctx->tm_page = tm_page;

	//printk(KERN_ERR "\n %s: 1 tm_page:%p, tm_page->page:%p \n", __func__, tm_page, page_address(tm_page->page));

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		kmem_cache_free(ctx->tm_page_write_ctx_cache, tm_page_write_ctx);
		return;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		bio_put(bio);
		kmem_cache_free(ctx->tm_page_write_ctx_cache, tm_page_write_ctx);
		printk(KERN_ERR "\n Inside %s 2 - Going.. Bye!! \n", __func__);
		return;
	}

	tm_page->flag = NEEDS_NO_FLUSH;
	/* blknr is the relative blknr within the translation blocks.
	 * We convert it to sector number as bio_submit expects that.
	 */
	pba = (tm_page->blknr * NR_SECTORS_IN_BLK) + ctx->sb->tm_pba;

	disk_size = i_size_read(ctx->dev->bdev->bd_inode);
	//printk(KERN_ERR "\n %s Flushing TM page: %p at pba: %llu disk_size: %llu blknr:%llu max_tm_blks:%u", __func__,  page_address(page), pba,  disk_size, tm_page->blknr, ctx->sb->revmap_bm_pba - ctx->sb->tm_pba + 1);
	BUG_ON(pba > disk_size/SECTOR_SIZE);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_iter.bi_sector = pba;
	bio->bi_private = tm_page_write_ctx;
	bio->bi_end_io = write_tmbl_complete;
	/* The next code is related to synchronizing at dtr() time.
	 */
	atomic_inc(&ctx->tm_ref);
	/* Not doing this right now as the conventional zones are too little.
	 * we need to log structurize the metadata
	 */
	//submit_bio_wait(bio);
	bio->bi_status = BLK_STS_OK;
	write_tmbl_complete(bio);
	//printk(KERN_INFO "\n 2. Leaving %s! flushed dirty page! \n", __func__);
	printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
	return;
}


void flush_tm_nodes(struct rb_node *node, struct ctx *ctx)
{
	struct tm_page * tm_page; 
	struct rb_node *left, *right;

	if (!node) {
		//printk(KERN_INFO "\n %s No tm node found! returning!", __func__);
		return;
	}

	tm_page = rb_entry(node, struct tm_page, rb);
	if (!tm_page) {
		return;
	}

	flush_tm_nodes(node->rb_left, ctx);
	flush_tm_nodes(node->rb_right, ctx);
	
	//if(tm_page->flag == NEEDS_FLUSH) {
	flush_tm_node_page(ctx, tm_page);
	//}
	return;
}

int read_translation_map(struct ctx *);

/* This function waits for all the translation blocks to be flushed to
 * the disk by calling wait_for_ckpt_completion and tm_ref
 * The non blocking call is flush_tm_nodes
 */
void flush_translation_blocks(struct work_struct *w)
{
	struct ctx * ctx = container_of(w, struct ctx, tb_work);
	struct rb_root *root = &ctx->tm_rb_root;
	//struct blk_plug plug;

	//printk(KERN_ERR "\n Inside %s ", __func__);
	if (!mutex_trylock(&ctx->tm_lock)) {
			return;
	}

	if (!root->rb_node) {
		mutex_unlock(&ctx->tm_lock);
		printk(KERN_ERR "\n %s tm node is NULL!", __func__);
		return;
	}

	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	atomic_set(&ctx->tm_ref, 1);

	//blk_start_plug(&plug);
	flush_tm_nodes(root->rb_node, ctx);
	mutex_unlock(&ctx->tm_lock);

	down_interruptible(&ctx->tm_kv_store_lock);
	remove_translation_pages(ctx);
	up(&ctx->tm_kv_store_lock);
	//blk_finish_plug(&plug);	
	atomic_dec(&ctx->tm_ref);
	printk(KERN_ERR "\n %s nr_tm_pages: %d", __func__, atomic_read(&ctx->nr_tm_pages));
	//printk(KERN_ERR "\n %s flushed: %d pages ", __func__, atomic_read(&ctx->tm_ref));
	//printk(KERN_INFO "\n %s done!!", __func__);
}

void flush_count_tm_nodes(struct ctx *ctx, struct rb_node *node, int *count, bool flush, int nrscan)
{
	struct tm_page * tm_page;

	if (flush && node) {
		tm_page = rb_entry(node, struct tm_page, rb);
		flush_tm_node_page(ctx, tm_page);
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

	//cant_sleep();
	sit_ctx = (struct sit_page_write_ctx *) bio->bi_private;
	ctx = sit_ctx->ctx;

	if (bio->bi_status != BLK_STS_OK) {
		/*TODO: Perhaps retry!! or do something more
		 * or else you will loose the translation entries.
		 * write them some place else!
		 */
		//printk(KERN_ERR "\n Could not write the SIT page to disk! ");
	}
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);

	atomic_dec(&ctx->sit_ref);
	wake_up(&ctx->sitq);
	kmem_cache_free(ctx->sit_ctx_cache, sit_ctx);
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

	if (!node)
		return;

	/* Do not flush if the page is not dirty */

	sit_page = rb_entry(node, struct sit_page, rb);
	if (!sit_page) {
		printk(KERN_ERR "\n sit_page is NULL!");
		return;
	}

	if(sit_page->flag == NEEDS_NO_FLUSH)
		return;

	sit_page->flag = NEEDS_NO_FLUSH;

	page = sit_page->page;
	if (!page)
		return;

	/* pba denotes a relative sit blknr that is 4096 sized
	 * bio works on a LBA that is sector sized.
	 */
	pba = sit_page->blknr;

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
	/* sit_pba is a sector aligned LBA */
	sit_ctx->sit_nr = pba;

	bio_set_dev(bio, ctx->dev->bdev);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	/* Sector addressing */
	pba = (pba * NR_SECTORS_IN_BLK) + ctx->sb->sit_pba;
	bio->bi_iter.bi_sector = pba;
	//printk("\n sit page address: %p pba: %llu", page_address(page), pba);
	bio->bi_private = sit_ctx;
	bio->bi_end_io = write_sitbl_complete;

	atomic_inc(&ctx->sit_ref);

	/* Not doing this right now as the conventional zones are too little.
	 * we need to log structurize the metadata
	 */
	//submit_bio(bio);
	write_sitbl_complete(bio);
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

int read_extents_from_block(struct ctx * ctx, struct tm_entry *entry, unsigned int nr_extents, u64 pba);

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
	struct rb_node *parent = NULL, **link = &root->rb_node;
	struct tm_page *new_tmpage, *parent_ent;
	/* convert the sector lba to a blknr and then find out the relative
	 * blknr where we find the translation entry from the first
	 * translation block.
	 *
	 */
	u64 blknr = lba/TM_ENTRIES_BLK;
	
	new_tmpage = search_tm_kv_store(ctx, blknr, &parent);
	if (new_tmpage) {
		return new_tmpage;
	}

	if (parent) {
		parent_ent = rb_entry(parent, struct tm_page, rb);
		BUG_ON (blknr == parent_ent->blknr);
		if (blknr < parent_ent->blknr) {
			/* Attach new node to the left of parent */
			link = &parent->rb_left;
		}
		else { 
			/* Attach new node to the right of parent */
			link = &parent->rb_right;
		}
	} 

	new_tmpage = kmem_cache_alloc(ctx->tm_page_cache, GFP_KERNEL);
	if (!new_tmpage) {
		printk(KERN_ERR "\n %s cannot allocate new_tmpage ", __func__);
		return NULL;
	}
	
	RB_CLEAR_NODE(&new_tmpage->rb);

	//printk("\n %s blknr: %d tm_pba: %llu \n", __func__, blknr, ctx->sb->tm_pba);
	
	new_tmpage->page = read_block(ctx, ctx->sb->tm_pba, (blknr * NR_SECTORS_IN_BLK));
	if (!new_tmpage->page) {
		printk(KERN_ERR "\n %s read_block  failed! could not allocate page! \n", __func__);
		kmem_cache_free(ctx->tm_page_cache, new_tmpage);
		return NULL;
	}
	new_tmpage->blknr = blknr;
	//printk("\n %s: lba: %llu blknr: %llu  NR_SECTORS_IN_BLK * TM_ENTRIES_BLK: %ld \n", __func__, lba, blknr, NR_SECTORS_IN_BLK * TM_ENTRIES_BLK);
	/* Add this page to a RB tree based KV store.
	 * Key is: blknr for this corresponding block
	 */

	rb_link_node(&new_tmpage->rb, parent, link);
	/* Balance the tree after node is addded to it */
	rb_insert_color(&new_tmpage->rb, root);
	atomic_inc(&ctx->nr_tm_pages);
    	atomic_inc(&ctx->tm_flush_count);
	
	return new_tmpage;
}


void remove_tm_entry_kv_store(struct ctx *ctx, u64 lba);


/* Make the length in terms of sectors or blocks?
 * 
 * page is revmap page!
 */
int add_block_based_translation(struct ctx *ctx, struct page *page)
{
	
	struct lsdm_revmap_entry_sector * ptr;
	struct tm_page * tm_page = NULL;
	int i, j, len = 0;
	sector_t lba, pba;
	u64 blknr;
	struct rb_node *parent = NULL;

	ptr = (struct lsdm_revmap_entry_sector *)page_address(page);
	//printk(KERN_ERR "%s revmap_page address is: %p ", __func__, page_address(page));
	i = 0;
	while (i < NR_SECTORS_IN_BLK) {
		for(j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			if (0 == ptr->extents[j].pba) {
				//printk(KERN_ERR "\n %s ptr: %p ptr->extents[%d].pba: 0 ", __func__, ptr, j);
				continue;
			}
			lba = ptr->extents[j].lba;
			pba = ptr->extents[j].pba;
			len = ptr->extents[j].len;
			if (len == 0) {
				//printk(KERN_ERR "\n %s ptr: %p ptr->extents[%d].len: 0 ", __func__, ptr, j);
				continue;
			}
			if (len % 8) {
				printk(KERN_ERR "\n  %s lba: %llu, pba: %llu, len: %d \n", __func__, lba, pba, len);
				printk(KERN_ERR "\n");
				panic("len has to be a multiple of 8");
			}
			//printk(KERN_ERR "\n %s waiting for tm_kv_store_lock", __func__);
			down_interruptible(&ctx->tm_kv_store_lock);
/*-------------------------------------------------------------*/
			tm_page = add_tm_page_kv_store(ctx, lba);
			if (!tm_page) {
				up(&ctx->tm_kv_store_lock);
				printk(KERN_ERR "%s NO memory! ", __func__);
				return -ENOMEM;
			}
			 
			tm_page->flag = NEEDS_FLUSH;
			/* Call this under the kv store lock, else it
			 * will race with removal/flush code
			 */
			add_translation_entry(ctx, tm_page->page, lba, pba, len);
/*-------------------------------------------------------------*/
			up(&ctx->tm_kv_store_lock);
			//printk(KERN_ERR "%s DONE!", __func__);
			//printk(KERN_ERR "\n Adding TM entry: ptr: %p ptr->extents[j].lba: %llu, ptr->extents[j].pba: %llu, ptr->extents[j].len: %d", ptr, lba, pba, len);
		}
		ptr = ptr + 1;
		i++;
	}
	if (atomic_read(&ctx->tm_flush_count) >= MAX_TM_PAGES) {
		atomic_set(&ctx->tm_flush_count, 0);
		/* Only one flush operation at a time 
		* but we dont want to wait.
		*/
		//printk(KERN_ERR "%s About to to flush translation blocks! Nr tm pages: %d \n", __func__, atomic_read(&ctx->nr_tm_pages));
		flush_translation_blocks(&ctx->tb_work);
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

/* Waits until the value of refcount becomes 1  */
void wait_on_refcount(struct ctx *ctx, refcount_t *ref)
{
	spin_lock(&ctx->tm_ref_lock);
	wait_event_lock_irq(ctx->refq, (1 == refcount_read(ref)), ctx->tm_ref_lock);
	spin_unlock(&ctx->tm_ref_lock);
}

void process_tm_entries(struct work_struct * w)
{
	struct revmap_bioctx * revmap_bio_ctx = container_of(w, struct revmap_bioctx, process_tm_work);
	struct page *page = revmap_bio_ctx->page;
	struct ctx * ctx = revmap_bio_ctx->ctx;

	kmem_cache_free(ctx->revmap_bioctx_cache, revmap_bio_ctx);
	//printk(KERN_ERR "\n %s revmap_bio_ctx->page: %p", __func__,  page_address(page));
	add_block_based_translation(ctx, page);
	//printk(KERN_ERR "\n %s revmap_bio_ctx->page: %p", __func__,  page_address(page));
	__free_pages(page, 0);
	nrpages--;
	printk(KERN_ERR "\n %s nrpages: %llu tm_flush_count: %u", __func__, nrpages, atomic_read(&ctx->tm_flush_count));
}


void revmap_blk_flushed(struct bio *bio)
{
	struct ctx *ctx;
	sector_t pba;
	struct revmap_bioctx *revmap_bio_ctx = bio->bi_private;

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
	
	/* queuing this work as we cannot use a sleeping semaphore in
	 * bio_endio irq context 
	 */
	INIT_WORK(&revmap_bio_ctx->process_tm_work, process_tm_entries);
	//queue_work(ctx->writes_wq, &revmap_bio_ctx->process_tm_work);
	
	if (revmap_bio_ctx->wait) {
		atomic_dec(&ctx->nr_pending_writes);
		/* Wakeup waiters waiting on the block barrier after the
		 * process_tm_entries work is queued.
		 * */
		wake_up(&ctx->rev_blk_flushq);
	}

	process_tm_entries(&revmap_bio_ctx->process_tm_work);

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
int flush_revmap_block_disk(struct ctx * ctx, struct page *page, u32 wait)
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
	revmap_bio_ctx->wait = wait;

	bio->bi_iter.bi_sector = ctx->revmap_pba;
	//printk(KERN_ERR "%s Flushing revmap blk at pba:%llu ctx->revmap_pba: %llu", __func__, bio->bi_iter.bi_sector, ctx->revmap_pba);
	
	bio->bi_end_io = revmap_blk_flushed;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_private = revmap_bio_ctx;
	if (wait) {
		atomic_inc(&ctx->nr_pending_writes);
	}
	//printk(KERN_ERR "\n flushing revmap at pba: %llu", bio->bi_iter.bi_sector);
	
	/* Not doing this right now as the conventional zones are too little.
	 * we need to log structurize the metadata
	 */
	//submit_bio(bio);
	revmap_blk_flushed(bio);
	return 0;
}


/*
 * The unflushed page is stored in ctx->revmap_page.
 */
void flush_revmap_entries(struct ctx *ctx)
{
	struct page *page = ctx->revmap_page;
	u32 wait = 1;

	if (!page) {
		printk(KERN_ERR "\n No revmap page!! ");
		return;
	}

	atomic_set(&ctx->nr_pending_writes, 1);
	flush_revmap_block_disk(ctx, page, wait);
	atomic_dec(&ctx->nr_pending_writes);
	//trace_printk("%s waiting on block barrier ", __func__);
	/* We wait for translation entries to be added! */
	//wait_event(ctx->rev_blk_flushq, 0 >= atomic_read(&ctx->nr_pending_writes));
	printk(KERN_ERR "\n %s revmap entries are on disk, wait is over! \n", __func__);
}


/*
 *
 * Revmap entries are not ordered. They are entered as and when the
 * write completes, which could be in any order (as it depends on
 * schedule()
 */
void shrink_next_entries(struct ctx *ctx, sector_t lba, sector_t pba, unsigned long len, struct page *page)
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


int merge_rev_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned long len, struct page *page)
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
	int entry_nr, sector_nr;
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
				page = ctx->revmap_page;
				BUG_ON(page == NULL);
				flush_revmap_block_disk(ctx, page, 0);
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
		atomic_inc(&ctx->revmap_entry_nr);
		nrpages++;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
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
	if (bio->bi_status)
		printk(KERN_ERR "\n %s bio status: %d ", __func__, bio->bi_status);
	bio_endio(bio);

	ctx = lsdm_bioctx->ctx;
	kmem_cache_free(ctx->bioctx_cache, lsdm_bioctx);
	kref_put(&ctx->ongoing_iocount, lsdm_ioidle);
}


void sub_write_done(struct work_struct * w)
{

	struct lsdm_sub_bioctx *subbioctx = NULL;
	struct lsdm_bioctx * bioctx;
	struct ctx *ctx;
	sector_t lba, pba;
	unsigned int len;

	subbioctx = container_of(w, struct lsdm_sub_bioctx, work);
	bioctx = subbioctx->bioctx;
	ctx = bioctx->ctx;
	/* Task completed successfully */
	lba = subbioctx->extent.lba;
	pba = subbioctx->extent.pba;
	len = subbioctx->extent.len;

	kmem_cache_free(ctx->subbio_ctx_cache, subbioctx);
	kref_put(&bioctx->ref, write_done);

	/* for now updating the metadata in the write request itself
	 * (part of debug)
	 */
	down_write(&ctx->metadata_update_lock);
	/*------------------------------- */
	add_revmap_entries(ctx, lba, pba, len);
	/*-------------------------------*/
	up_write(&ctx->metadata_update_lock);
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
void lsdm_clone_endio(struct bio * clone)
{
	struct lsdm_sub_bioctx *subbioctx = NULL;
	struct bio *bio = NULL;
	struct ctx *ctx;
	struct lsdm_bioctx *bioctx;

	subbioctx = clone->bi_private;
	bioctx = subbioctx->bioctx;
	ctx = bioctx->ctx;
	bio = bioctx->orig;
	if (bio && bio->bi_status == BLK_STS_OK) {
		bio->bi_status = clone->bi_status;
	} 
	bio_put(clone);
	INIT_WORK(&subbioctx->work, sub_write_done);
	queue_work(ctx->writes_wq, &subbioctx->work);
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
int lsdm_write_io(struct ctx *ctx, struct bio *bio)
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
	static int count = 0;

	//printk(KERN_ERR "\n ******* Inside %s, requesting lba: %llu sectors: %d ", __func__, bio->bi_iter.bi_sector, bio_sectors(bio));
	/* Just debbuging purpose
	bio->bi_status = BLK_STS_OK;
	bio_advance(bio, bio->bi_iter.bi_size);
	bio_endio(bio);
	return 0;
	*/

	if (is_disk_full(ctx)) {
		printk(KERN_ERR "\n %s No more space! ", __func__);
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
	if (unlikely(nr_sectors <= 0)) {
		//trace_printk("\n Less than 0 sectors (%d) requested!", nr_sectors);
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		return -1;
	}
	
	bio->bi_status = BLK_STS_OK;
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
	count++;
	//printk(KERN_ERR "\n %s bioctx allocated from bioctx_cache, ptr: %p count: %d" , __func__, bioctx, count);
	bioctx->orig = bio;
	bioctx->ctx = ctx;
	/* TODO: Initialize refcount in bioctx and increment it every
	 * time bio is split or padded */
	kref_init(&bioctx->ref);

	//printk(KERN_ERR "\n write frontier: %llu free_sectors_in_wf: %llu", ctx->hot_wf_pba, ctx->free_sectors_in_wf);

	//blk_start_plug(&plug);
	
	do {
		nr_sectors = bio_sectors(clone);
		subbio_ctx = NULL;
		subbio_ctx = kmem_cache_alloc(ctx->subbio_ctx_cache, GFP_KERNEL);
		if (!subbio_ctx) {
			//trace_printk("\n insufficient memory!");
			kmem_cache_free(ctx->bioctx_cache, bioctx);
			goto fail;
		}
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		//trace_printk("\n subbio_ctx: %llu", subbio_ctx);
		spin_lock(&ctx->lock);
		/*-------------------------------*/
		wf = ctx->hot_wf_pba;
		
		/* room_in_zone should be same as
		 * ctx->nr_free_sectors_in_wf
		 */
		if (s8 > ctx->free_sectors_in_wf){
			//trace_printk("SPLITTING!!!!!!!! s8: %d ctx->free_sectors_in_wf: %d", s8, ctx->free_sectors_in_wf);
			s8 = round_down(ctx->free_sectors_in_wf, NR_SECTORS_IN_BLK);
			if (s8 <= 0) {
				panic("Should always have atleast a block left ");
			}
			move_write_frontier(ctx, s8);
		/*-------------------------------*/
			spin_unlock(&ctx->lock);
			/* We cannot call bio_split with spinlock held! */
			if (!(split = bio_split(clone, s8, GFP_NOIO, ctx->bs))){
				//trace_printk("\n failed at bio_split! ");
				kmem_cache_free(ctx->subbio_ctx_cache, subbio_ctx);
				goto fail;
			}
			subbio_ctx->extent.len = s8;
		} 
		else {
			move_write_frontier(ctx, s8);
		/*-------------------------------*/
			spin_unlock(&ctx->lock);
			split = clone;
			/* s8 might be bigger than nr_sectors. We want
			 * to maintain the exact length in the
			 * translation map, not padded entry
			 */
			subbio_ctx->extent.len = nr_sectors;
		}
    		

		/* Next we fetch the LBA that our DM got */
		lba = bio->bi_iter.bi_sector;
		kref_get(&bioctx->ref);
		//kref_get(&ctx->ongoing_iocount);
		atomic_inc(&ctx->nr_writes);
		subbio_ctx->extent.lba = lba;
		subbio_ctx->extent.pba = wf;
		subbio_ctx->bioctx = bioctx; /* This is common to all the subdivided bios */

		split->bi_iter.bi_sector = wf; /* we use the save write frontier */
		split->bi_private = subbio_ctx;
		split->bi_end_io = lsdm_clone_endio;
		bio_set_dev(split, ctx->dev->bdev);
		submit_bio(split);
		//printk(KERN_ERR "\n %s lba: {%llu, pba: %llu, len: %d},", __func__, lba, wf, subbio_ctx->extent.len);
		down_write(&ctx->metadata_update_lock);
		/*------------------------------- */
		lsdm_update_range(ctx, &ctx->extent_tbl_root, lba, wf, subbio_ctx->extent.len);
		lsdm_update_range(ctx, &ctx->rev_tbl_root, wf, lba, subbio_ctx->extent.len);
		/*-------------------------------*/
		up_write(&ctx->metadata_update_lock);
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
	//blk_finish_plug(&plug);
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
#define BS_NR_POOL_PAGES 8192

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
	struct lsdm_ckpt *ckpt = NULL;
	struct page *page;

	page = read_block(ctx, 0, pba);
	if (!page)
		return NULL;

       	ckpt = (struct lsdm_ckpt *) page_address(page);
	printk(KERN_INFO "\n ** sector_nr: %llu, ckpt->magic: %u, ckpt->hot_frontier_pba: %lld", pba, ckpt->magic, ckpt->hot_frontier_pba);
	if (ckpt->magic == 0) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
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
	printk("Inside %s" , __func__);
	/*--------------------------------------------*/
	flush_workqueue(ctx->writes_wq);
	INIT_WORK(&ctx->sit_work, flush_sit);
	flush_sit(&ctx->sit_work);
	printk(KERN_ERR "\n sit pages flushed! %llu", atomic_read(&ctx->nr_sit_pages));
	free_sit_pages(ctx);
	/*--------------------------------------------*/

	flush_revmap_bitmap(ctx);
	update_checkpoint(ctx);
	flush_checkpoint(ctx);

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
int do_recovery(struct ctx *ctx)
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
		free_pages(page1, 0);
		return NULL;
	}
	printk(KERN_INFO "\n %s ckpt versions: %lld %lld", __func__, ckpt1->version, ckpt2->version);
	if (ckpt1->version >= ckpt2->version) {
		ckpt = ckpt1;
		__free_pages(ctx->ckpt_page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		ctx->ckpt_page = page1;
		ctx->ckpt_pba = ctx->sb->ckpt2_pba;
		printk(KERN_ERR "\n Setting ckpt 1 version: %lld ckpt2 version: %lld \n", ckpt1->version, ckpt2->version);
	}
	else {
		ckpt = ckpt2;
		ctx->ckpt_pba = ctx->sb->ckpt1_pba;
		//page2 is rightly set by read_ckpt();
		__free_pages(page1, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		printk(KERN_ERR "\n Setting ckpt 1 version: %lld ckpt2 version: %lld \n", ckpt1->version, ckpt2->version);
	}
	ctx->user_block_count = ckpt->user_block_count;
	printk(KERN_ERR "\n %s Nr of free blocks: %d \n",  __func__, ctx->user_block_count);
	ctx->nr_invalid_zones = ckpt->nr_invalid_zones;
	ctx->hot_wf_pba = ckpt->hot_frontier_pba;
	ctx->elapsed_time = ckpt->elapsed_time;
	/* TODO: Do recovery if necessary */
	//do_recovery(ckpt);
	return ckpt;
}

/* 
 * 1 indicates the zone is free 
 */

void mark_zone_occupied(struct ctx *ctx , int zonenr)
{
	char *bitmap = ctx->freezone_bitmap;
	int bytenr = zonenr / BITS_IN_BYTE;
	int bitnr = zonenr % BITS_IN_BYTE;

	if (bytenr > ctx->bitmap_bytes) {
		panic("\n Trying to set an invalid bit in the free zone bitmap. bytenr > bitmap_bytes");
	}

	/*
	 * printk(KERN_ERR "\n %s zonenr: %d, bytenr: %d, bitnr: %d ", __func__, zonenr, bytenr, bitnr);
	 *  printk(KERN_ERR "\n %s bitmap[%d]: %d ", __func__, bytenr, bitnr, bitmap[bytenr]);
	 */

	if ((bitmap[bytenr] & (1 << bitnr)) == 0) {
		/* This function can be called multiple times for the
		 * same zone. The bit is already unset and the zone 
		 * is marked occupied already.
		 */
		//printk(KERN_ERR "\n %s zone is already occupied! bitmap[%d]: %d ", __func__, bytenr, bitnr, bitmap[bytenr]);
		return;
	}
	/* This bit is set and the zone is free. We want to unset it
	 */
	bitmap[bytenr] = bitmap[bytenr] ^ (1 << bitnr);
	ctx->nr_freezones = ctx->nr_freezones - 1;
	//printk(KERN_ERR "\n %s ctx->nr_freezones (2) : %u", __func__, ctx->nr_freezones);
}

int read_extents_from_block(struct ctx * ctx, struct tm_entry *entry, unsigned int nr_extents, u64 pba)
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
		//printk(KERN_ERR "\n %s TM-BLKNR: %llu entry->lba: %llu entry->pba: %llu", __func__, pba, entry->lba, entry->pba);
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
	unsigned long blknr, sectornr;
	/* blk_count_tm is 4096 bytes aligned number */
	unsigned long nrblks = ctx->sb->blk_count_tm;
	struct page *page;
	int i = 0;
	struct tm_entry * tm_entry = NULL;

	printk(KERN_ERR "\n %s Reading TM entries from: %llu, nrblks: %ld", __func__, ctx->sb->tm_pba, nrblks);
	
	ctx->n_extents = 0;
	sectornr = 0;
	while(i < nrblks) {
		page = read_block(ctx, ctx->sb->tm_pba, sectornr);
		if (!page)
			return -1;
		/* We read the extents in the entire block. the
		 * redundant extents should be unpopulated and so
		 * we should find 0 and break out */
		//trace_printk("\n pba: %llu", pba);
		tm_entry = (struct tm_entry *) page_address(page);
		read_extents_from_block(ctx, tm_entry, nr_extents_in_blk, blknr);
		i = i + 1;
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		sectornr = sectornr + NR_SECTORS_IN_BLK;
		blknr = blknr + 1;
	}
	printk(KERN_ERR "\n %s TM entries read!", __func__);
	return 0;
}

int read_revmap_bitmap(struct ctx *ctx)
{
	unsigned long nrblks = ctx->sb->blk_count_revmap_bm;

	printk(KERN_ERR "\n %s nrblks: %llu \n", __func__, nrblks);
	if (nrblks != 1) {
		panic("\n Wrong revmap bitmap calculations!");
	}
	
	ctx->revmap_bm = read_block(ctx, ctx->sb->revmap_bm_pba, 0);
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
	int bits = 0;
	unsigned int blknr;

	bdev = ctx->dev->bdev;
	/* We read the revmap bitmap first. If a bit is set,
	 * then the corresponding revmap blk is read
	 */
	page = ctx->revmap_bm;
	if (!page)
		return -1;
	ptr = (char *) page_address(page);
	//trace_printk("\n page_address(ctx->revmap_bm): %p", ptr);
	blknr = 0;
	for (i = 0; i < BLK_SZ; i++) {
		byte = *ptr;
		if (!byte) {
			bits = 8;
			blknr = blknr + bits; /* BITS_IN_BYTE */
		}

		while(byte) {
			if (byte & 1) {
				//trace_printk("\n WHY IS THIS BYTE SET??");
				flush_needed = 1;
				//trace_printk("\n read revmap blk: %lu", pba);
				revmap_page = read_block(ctx, ctx->revmap_pba, blknr);
				if (!revmap_page) {
					return -1;
				}
				process_revmap_entries_on_boot(ctx, revmap_page);
				__free_pages(revmap_page, 0);
				nrpages--;
				printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
				if (blknr >= nr_revmap_blks)
					break;
			}
			byte = byte >> 1;
			bits = bits + 1;
			blknr = blknr + 1;
		}
		if (bits != 7) {
			blknr = blknr + (7 - bits);
		}
		if (blknr >= nr_revmap_blks)
			break;
		ptr = ptr + 1;
	}
	printk("\n %s flush_needed: %d", __func__, flush_needed);
	if (flush_needed) {
		//trace_printk("\n Why do we need to flush!!");
		INIT_WORK(&ctx->tb_work, flush_translation_blocks);
		//flush_translation_blocks(&ctx->tb_work);
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
	printk(KERN_INFO "\n ctx->bitmap_bytes: %d ", ctx->bitmap_bytes);
	free_bitmap = (char *)kzalloc(ctx->bitmap_bytes, GFP_KERNEL);
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
void gc_rb_insert(struct ctx *ctx, struct rb_root *root, struct gc_rb_node *new)
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


void sit_rb_remove(struct ctx *ctx, struct rb_root *root, struct gc_rb_node*e)
{
	/* rb_erase resorts and rebalances the tree */
	rb_erase(&e->rb, root);
	ctx->n_sit_extents--;
}

unsigned int get_cb_cost(struct ctx *ctx , u32 nrblks, u64 mtime)
{
	unsigned char u, age;
	struct lsdm_sb *sb = ctx->sb;

	u = (nrblks * 100) >> (sb->log_zone_size - sb->log_block_size);

	if (!(ctx->max_mtime - ctx->min_mtime)) {
		dump_stack();
		return u;
	}
	age = 100 - div_u64(100 * (mtime - ctx->min_mtime),
				ctx->max_mtime - ctx->min_mtime);

	return UINT_MAX - ((100 * (100 - u) * age)/ (100 + u));
}


unsigned int get_cost(struct ctx *ctx, u32 nrblks, u64 age, char gc_mode)
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
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct gc_rb_node *e = NULL, *new = NULL;
	unsigned int cb_cost = 0;
	static int count = 0;

	BUG_ON(nrblks == 0);

	/* We are essentially adding a new node here */
	new = kmem_cache_alloc(ctx->gc_rb_node_cache, GFP_KERNEL);
	if (!new) {
		printk(KERN_ERR "\n %s could not allocate memory for gc_rb_node \n", __func__);
		return -ENOMEM;
	}
	RB_CLEAR_NODE(&new->rb);
	new->zonenr = zonenr;
	cb_cost = get_cost(ctx, nrblks, mtime, GC_CB);
	new->cb_cost = cb_cost;
	new->nrblks = nrblks;

	//printk(KERN_ERR "\n %s zonenr: %d nrblks: %u, mtime: %llu \n", __func__, zonenr, nrblks, mtime);
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
			if (&e->cb_cost == cb_cost)
				return 0;
			rb_erase(&e->rb, root);
			kmem_cache_free(ctx->gc_rb_node_cache, e);
			//printk(KERN_ERR "\n %s Found the zone: %d and deleted it, will read again later! ", __func__, zonenr);
			link = &root->rb_node;
			parent = NULL;
			continue;
			
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

	printk(KERN_ERR "\n %s Added zonenr: %d cost: %u to the RB tree ", __func__, zonenr, cb_cost);
	zonenr = select_zone_to_clean(ctx, BG_GC);
	printk(KERN_ERR "\n %s zone to clean: %d ", __func__, zonenr);
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
	//printk("\n Number of seg entries: %u", nr_seg_entries);
	//printk("\n hot_frontier_pba: %llu, frontier zone: %u", ctx->ckpt->hot_frontier_pba, get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba));

	while (i < nr_seg_entries) {
		if ((*zonenr == get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba)) ||
		    (*zonenr == get_zone_nr(ctx, ctx->ckpt->warm_gc_frontier_pba))) {
			//printk(KERN_ERR "\n zonenr: %d vblocks: %llu is our cur_frontier! not marking it free!", *zonenr, entry->vblocks);
			spin_lock(&ctx->lock);
			mark_zone_occupied(ctx , *zonenr);
			spin_unlock(&ctx->lock);

			entry = entry + 1;
			*zonenr= *zonenr + 1;
			i++;
			continue;
		}
		if (entry->vblocks == 0) {
    			//trace_printk("\n *segnr: %u", *zonenr);
			mark_zone_free(ctx , *zonenr);
		}
		else if (entry->vblocks < nr_blks_in_zone) {
			printk(KERN_ERR "\n *segnr: %u entry->vblocks: %llu entry->mtime: %llu", *zonenr, entry->vblocks, entry->mtime);
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
	unsigned int nrblks = 0, sectornr = 0;
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
	
	printk(KERN_ERR "\n nr_data_zones: %llu", nr_data_zones);
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
	nrblks = sb->blk_count_sit;
	sectornr = 0;
	printk(KERN_ERR "\n ctx->ckpt->hot_frontier_pba: %llu", ctx->ckpt->hot_frontier_pba);
	printk(KERN_ERR "\n ctx->ckpt->warm_gc_frontier_pba: %llu", ctx->ckpt->warm_gc_frontier_pba);
	printk(KERN_ERR "\n get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba): %u", get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba));
	printk(KERN_ERR "\n %s Read seginfo from pba: %llu sectornr: %d zone0_pba: %llu \n", __func__, sb->sit_pba, sectornr, ctx->sb->zone0_pba);
	while (zonenr < sb->zone_count_main) {
		//trace_printk("\n zonenr: %u", zonenr);
		if ((sectornr + sb->sit_pba) > ctx->sb->zone0_pba) {
			printk(KERN_ERR "\n Seg entry blknr cannot be bigger than the data blknr");
			return -1;
		}
		sit_page = read_block(ctx, sb->sit_pba, sectornr);
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
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		sectornr = sectornr + 8;
	}
	//printk(KERN_ERR "\n %s ctx->nr_freezones (2) : %u zonenr: %llu", __func__, ctx->nr_freezones, zonenr);
	return 0;
}

struct lsdm_sb * read_superblock(struct ctx *ctx, unsigned long pba)
{

	struct block_device *bdev = ctx->dev->bdev;
	struct lsdm_sb * sb;
	struct page *page;
	/* TODO: for the future. Right now we keep
	 * the block size the same as the sector size
	 *
	if (set_blocksize(bdev, 512))
		return NULL;
	*/

	printk(KERN_INFO "\n reading sb at pba: %lu", pba);

	page = read_block(ctx, 0, pba);
	
	if (!page) {
		printk(KERN_ERR "\n Could not read the superblock");
		return NULL;
	}
	
	sb = (struct lsdm_sb *)page_address(page);
	if (sb->magic != STL_SB_MAGIC) {
		printk(KERN_INFO "\n Wrong superblock!");
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return NULL;
	}
	printk(KERN_INFO "\n sb->magic: %u", sb->magic);
	printk(KERN_INFO "\n sb->max_pba: %llu", sb->max_pba);
	ctx->sb_page = page;
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

	//printk(KERN_ERR "\n %s Inside ", __func__);

	ctx->max_pba = LONG_MAX;
	sb1 = read_superblock(ctx, 0);
	if (NULL == sb1) {
		printk("\n read_superblock failed! cannot read the metadata ");
		return -1;
	}

	printk(KERN_INFO "\n superblock read!");
	/*
	 * we need to verify that sb1 is uptodate.
	 * Right now we do nothing. we assume
	 * sb1 is okay.
	 */
	page = ctx->sb_page;
	sb2 = read_superblock(ctx, 8);
	if (!sb2) {
		printk("\n Could not read the second superblock!");
		return -1;
	}
	__free_pages(page, 0);
	nrpages--;
	printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
	ctx->sb = sb2;
	printk(KERN_INFO "\n sb->max_pba: %llu", sb2->max_pba);
	ctx->max_pba = ctx->sb->max_pba;
	ctx->nr_lbas_in_zone = (1 << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	printk(KERN_ERR "\n nr_lbas_in_zone: %llu", ctx->nr_lbas_in_zone);
	ctx->revmap_pba = ctx->sb->revmap_pba;
	printk(KERN_ERR "\n ** ctx->revmap_pba (first revmap bm block pba) : %llu", ctx->revmap_pba);

	ckpt = get_cur_checkpoint(ctx);
	if (NULL == ckpt) {
		__free_pages(ctx->sb_page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return -1;
	}	
	ctx->ckpt = ckpt;
	printk(KERN_INFO "\n checkpoint read!, ckpt->clean: %d", ckpt->clean);
	/*
	if (!ckpt->clean) {
		printk("\n Scrubbing metadata after an unclean shutdown...");
		ret = do_recovery(ctx);
		return ret;
	} */
	//printk(KERN_ERR "\n sb->blk_count_revmap_bm: %d", ctx->sb->blk_count_revmap_bm);
	//printk(KERN_ERR "\n nr of revmap blks: %u", ctx->sb->blk_count_revmap);

	ctx->hot_wf_pba = ctx->ckpt->hot_frontier_pba;
	//printk(KERN_ERR "\n %s %d ctx->hot_wf_pba: %llu\n", __func__, __LINE__, ctx->hot_wf_pba);
	ctx->hot_wf_end = zone_end(ctx, ctx->hot_wf_pba);
	//printk(KERN_ERR "\n %s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->hot_wf_end);
	//printk(KERN_ERR "\n max_pba = %d", ctx->max_pba);
	ctx->free_sectors_in_wf = ctx->hot_wf_end - ctx->hot_wf_pba + 1;
	//printk(KERN_ERR "\n ctx->free_sectors_in_wf: %lld", ctx->free_sectors_in_wf);
	
	ctx->warm_gc_wf_pba = ctx->ckpt->warm_gc_frontier_pba;
	//printk(KERN_ERR "\n %s %d ctx->hot_wf_pba: %llu\n", __func__, __LINE__, ctx->hot_wf_pba);
	ctx->warm_gc_wf_end = zone_end(ctx, ctx->warm_gc_wf_pba);
	printk(KERN_ERR "\n %s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->hot_wf_end);
	printk(KERN_ERR "\n max_pba = %llu", ctx->max_pba);
	ctx->free_sectors_in_gc_wf = ctx->warm_gc_wf_end - ctx->warm_gc_wf_pba + 1;

	ret = read_revmap_bitmap(ctx);
	if (ret) {
		__free_pages(ctx->sb_page, 0);
		nrpages--;
		free_pages(ctx->ckpt_page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return -1;
	}
	printk(KERN_ERR "\n before: PBA for first revmap blk: %u", ctx->sb->revmap_pba/NR_SECTORS_IN_BLK);
	read_revmap(ctx);
	printk(KERN_INFO "\n Reverse map Read!");
	/*
	ret = read_translation_map(ctx);
	if (0 > ret) {
		__free_pages(ctx->sb_page, 0);
		free_pages(ctx->ckpt_page, 0);
		__free_pages(ctx->revmap_bm, 0);
		printk(KERN_ERR "\n read_extent_map failed! cannot read the metadata ");
		return ret;
	}*/
	printk(KERN_INFO "\n %s extent_map read!", __func__);

	ctx->nr_freezones = 0;
	ctx->bitmap_bytes = sb2->zone_count_main /BITS_IN_BYTE;
	if (sb2->zone_count_main % BITS_IN_BYTE)
		ctx->bitmap_bytes = ctx->bitmap_bytes + 1;
	printk(KERN_INFO "\n ************ %s Nr of zones in main are: %u, bitmap_bytes: %d", __func__, sb2->zone_count_main, ctx->bitmap_bytes);
	if (sb2->zone_count_main % BITS_IN_BYTE > 0)
	ctx->bitmap_bytes += 1;
	ctx->nr_freezones = 0;
	read_seg_info_table(ctx);
	printk(KERN_INFO "\n %s ctx->nr_freezones: %u, ckpt->nr_free_zones:%llu", __func__, ctx->nr_freezones, ckpt->nr_free_zones);
	if (ctx->nr_freezones != ckpt->nr_free_zones) { 
		/* TODO: Do some recovery here.
		 * We do not wait for confirmation of SIT pages on the
		 * disk. we match the SIT entries to that by the
		 * translation map. we also make sure that the ckpt
		 * entries are based on the translation map
		 */
		printk(KERN_ERR "\n SIT and checkpoint does not match!");
		do_recovery(ctx);
		__free_pages(ctx->sb_page, 0);
		nrpages--;
		free_pages(ctx->ckpt_page, 0);
		nrpages--;
		__free_pages(ctx->revmap_bm, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
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
	kmem_cache_destroy(ctx->extent_cache);
	kmem_cache_destroy(ctx->app_read_ctx_cache);
	kmem_cache_destroy(ctx->gc_extents_cache);
	kmem_cache_destroy(ctx->subbio_ctx_cache);
	kmem_cache_destroy(ctx->tm_page_cache);
	kmem_cache_destroy(ctx->gc_rb_node_cache);
	kmem_cache_destroy(ctx->sit_ctx_cache);
	kmem_cache_destroy(ctx->sit_page_cache);
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
	ctx->sit_page_cache = kmem_cache_create("sit_page_cache", sizeof(struct sit_page), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->sit_page_cache) {
		goto destroy_revmap_bioctx_cache;
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
		goto destroy_gc_extents_cache;
	}
	//trace_printk("\n app_read_ctx_cache initialized to address: %p", ctx->app_read_ctx_cache);
	ctx->tm_page_write_ctx_cache = kmem_cache_create("tmem_write_ctx_cache", sizeof(struct tm_page_write_ctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->tm_page_write_ctx_cache) {
		goto destroy_app_read_ctx_cache;
	}
	ctx->extent_cache = kmem_cache_create("lsdm_extent_cache", sizeof(struct extent), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->extent_cache) {
		goto destroy_tm_page_write_ctx_cache;
	}

	return 0;
/* failed case */
destroy_tm_page_write_ctx_cache:
	kmem_cache_destroy(ctx->extent_cache);
destroy_app_read_ctx_cache:
	kmem_cache_destroy(ctx->app_read_ctx_cache);
destroy_gc_extents_cache:
	kmem_cache_destroy(ctx->gc_extents_cache);
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
	struct request_queue *q;

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
	/* 13 comes from 9 + 3, where 2^9 is the number of bytes in a sector
	 * and 2^3 is the number of sectors in a block.
	 */
	dm_target->max_io_len = ZONE_SZ;

	ret = dm_get_device(dm_target, argv[0], dm_table_get_mode(dm_target->table), &ctx->dev);
    	if (ret) {
		dm_target->error = "lsdm: Device lookup failed.";
		goto free_ctx;
	}

	if (bdev_zoned_model(ctx->dev->bdev) == BLK_ZONED_NONE) {
                dm_target->error = "Not a zoned block device";
                ret = -EINVAL;
                goto free_ctx;
        }

	q = bdev_get_queue(ctx->dev->bdev);
	printk(KERN_ERR "\n number of sectors in a zone: %llu", blk_queue_zone_sectors(q));
	printk(KERN_ERR "\n number of zones in device: %llu", blkdev_nr_zones(ctx->dev->bdev));


	printk(KERN_INFO "\n sc->dev->bdev->bd_part->start_sect: %llu", ctx->dev->bdev->bd_part->start_sect);
	printk(KERN_INFO "\n sc->dev->bdev->bd_part->nr_sects: %llu", ctx->dev->bdev->bd_part->nr_sects);


	disk_size = i_size_read(ctx->dev->bdev->bd_inode);
	printk(KERN_INFO "\n The disk size as read from the bd_inode: %llu", disk_size);
	printk(KERN_INFO "\n max sectors: %llu", disk_size/512);
	printk(KERN_INFO "\n max blks: %llu", disk_size/4096);
	ctx->writes_wq = create_workqueue("writes_queue");
	mutex_init(&ctx->tm_lock);
	sema_init(&ctx->sit_kv_store_lock, 1);
	sema_init(&ctx->tm_kv_store_lock, 1);

	spin_lock_init(&ctx->lock);
	spin_lock_init(&ctx->tm_ref_lock);
	spin_lock_init(&ctx->tm_flush_lock);
	mutex_init(&ctx->sit_flush_lock);
	spin_lock_init(&ctx->rev_flush_lock);
	spin_lock_init(&ctx->ckpt_lock);

	atomic_set(&ctx->io_count, 0);
	atomic_set(&ctx->nr_reads, 0);
	atomic_set(&ctx->pages_alloced, 0);
	atomic_set(&ctx->nr_writes, 0);
	atomic_set(&ctx->nr_failed_writes, 0);
	atomic_set(&ctx->revmap_entry_nr, 0);
	atomic_set(&ctx->revmap_sector_nr, 0);
	atomic_set(&ctx->tm_ref, 0);
	atomic_set(&ctx->sit_ref, 0);
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
	init_waitqueue_head(&ctx->sitq);
	init_waitqueue_head(&ctx->tmq);
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

	ctx->gc_page_pool = mempool_create_page_pool(GC_POOL_PAGES, 0);
	if (!ctx->gc_page_pool)
		goto put_dev;

	//printk(KERN_INFO "about to call bioset_init()");
	ctx->bs = kzalloc(sizeof(*(ctx->bs)), GFP_KERNEL);
	if (!ctx->bs)
		goto destroy_gc_page_pool;

	if(bioset_init(ctx->bs, BS_NR_POOL_PAGES, 0, 0) == -ENOMEM) {
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
	kref_put(&ctx->ongoing_iocount, no_op);
	//trace_printk("\n About to read metadata! 5 ! \n");

    	ret = read_metadata(ctx);
	if (ret < 0) 
		goto destroy_cache;

	max_pba = (ctx->dev->bdev->bd_inode->i_size) / 512;
	sprintf(ctx->nodename, "lsdm/%s", argv[1]);
	ret = -EINVAL;
	printk(KERN_ERR "\n device records max_pba: %llu", max_pba);
	printk(KERN_ERR "\n formatted max_pba: %llu", ctx->max_pba);
	if (ctx->max_pba > max_pba) {
		dm_target->error = "dm-lsdm: Invalid max pba found on sb";
		goto free_metadata_pages;
	}

	if (ctx->sb->zone_count < SMALL_NR_ZONES) {
		ctx->w1 = ctx->sb->zone_count / 10; 
		ctx->w2 = ctx->w1 << 1; 
	} else {
		ctx->w1 = ctx->sb->zone_count / 20; 
		ctx->w2 = ctx->w1 << 1; 
	}


	printk(KERN_ERR "\n Initializing gc_extents list, ctx->gc_extents_cache: %p ", ctx->gc_extents_cache);
	ctx->gc_extents = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
	if (!ctx->gc_extents) {
		printk(KERN_ERR "\n Could not allocate gc_extent and hence could not initialized \n");
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
	ret = lsdm_flush_thread_start(ctx);
	if (ret) {
		goto stop_gc_thread;
	}

	/*
	if (register_shrinker(lsdm_shrinker))
		goto stop_gc_thread;
	*/
	printk(KERN_ERR "\n ctr() done!!");
	return 0;
/* failed case */
stop_gc_thread:
	lsdm_gc_thread_stop(ctx);
free_metadata_pages:
	printk(KERN_ERR "\n freeing metadata pages!");
	if (ctx->revmap_bm)
		__free_pages(ctx->revmap_bm, 0);
		nrpages--;
	if (ctx->sb_page)
		__free_pages(ctx->sb_page, 0);
		nrpages--;
	if (ctx->ckpt_page)
		__free_pages(ctx->ckpt_page, 0);
		nrpages--;
	/* TODO : free extent page
	 * and segentries page */
destroy_cache:
	destroy_caches(ctx);
uninit_bioset:
	bioset_exit(ctx->bs);
free_bioset:
	kfree(ctx->bs);

destroy_gc_page_pool:
	if (ctx->gc_page_pool)
		mempool_destroy(ctx->gc_page_pool);
put_dev:
	dm_put_device(dm_target, ctx->dev);
free_ctx:
	kfree(ctx);
	printk(KERN_ERR "\n %s nrpages: %llu", nrpages);
	return ret;
}

/* For individual device removal */
static void ls_dm_dev_exit(struct dm_target *dm_target)
{
	struct ctx *ctx = dm_target->private;
	struct rb_root *root = &ctx->gc_rb_root;

	flush_workqueue(ctx->writes_wq);
	sync_blockdev(ctx->dev->bdev);
	printk(KERN_ERR "\n Inside dtr! About to call flush_revmap_entries()");
	flush_revmap_entries(ctx);
	printk(KERN_ERR "\n %s flush_revmap_entries done!", __func__);
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
	//flush_workqueue(ctx->writes_wq);
	INIT_WORK(&ctx->tb_work, flush_translation_blocks);
	flush_translation_blocks(&ctx->tb_work);
	printk(KERN_ERR "\n %s translation blocks flushed process started! ", __func__);
	wait_event(ctx->tmq, (!atomic_read(&ctx->tm_ref)));
	/* Wait for the ALL the translation pages to be flushed to the
	 * disk. The removal work is queued.
	 */
	free_translation_pages(ctx);	
	//printk(KERN_ERR "\n translation blocks flushed! ");
	//wait_on_revmap_block_availability(ctx, ctx->revmap_pba);
	do_checkpoint(ctx);
	//trace_printk("\n checkpoint done!");
	/* If we are here, then there was no crash while writing out
	 * the disk metadata
	 */
	destroy_workqueue(ctx->writes_wq);
	lsdm_free_rb_mappings(ctx);
	remove_gc_rb_nodes(ctx, root->rb_node);
	kmem_cache_free(ctx->gc_extents_cache, ctx->gc_extents);
	printk(KERN_ERR "\n RB mappings freed! ");
	//printk(KERN_ERR "\n Nr of free blocks: %lld",  ctx->user_block_count);
	/* TODO : free extent page
	 * and segentries page */
	if (ctx->sb_page) {
		__free_pages(ctx->sb_page, 0);
		nrpages--;
	}
	if (ctx->ckpt_page) {
		__free_pages(ctx->ckpt_page, 0);
		nrpages--;
	}

	__free_pages(ctx->revmap_bm, 0);
	printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);

	//trace_printk("\n metadata pages freed! \n");
	/* timer based gc invocation for later
	 * del_timer_sync(&ctx->timer_list);
	 */
	//trace_printk("\n caches destroyed! \n");
	lsdm_gc_thread_stop(ctx);
	lsdm_flush_thread_stop(ctx);
	printk(KERN_ERR "\n gc thread stopped! \n");
	bioset_exit(ctx->bs);
	//trace_printk("\n exited from bioset \n");
	kfree(ctx->bs);
	//trace_printk("\n bioset memory freed \n");
	mempool_destroy(ctx->gc_page_pool);
	//trace_printk("\n memory pool destroyed\n");
	dm_put_device(dm_target, ctx->dev);
	//trace_printk("\n device mapper target released\n");
	printk(KERN_ERR "\n nr_writes: %d", atomic_read(&ctx->nr_writes));
	printk(KERN_ERR "\n nr_failed_writes: %u", atomic_read(&ctx->nr_failed_writes));
	destroy_caches(ctx);
	kfree(ctx);
	//trace_printk("\n ctx memory freed!\n");
	printk(KERN_ERR "\n Goodbye World!\n");
	return;
}


int ls_dm_dev_map_io(struct dm_target *dm_target, struct bio *bio)
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

	bio_set_dev(bio, ctx->dev->bdev);

	switch (bio_op(bio)) {
		case REQ_OP_READ:
			ret = lsdm_read_io(ctx, bio);
			break;
		case REQ_OP_WRITE:
			ret = lsdm_write_io(ctx, bio);
			break;
		default:
			printk(KERN_ERR "\n %s Received bio, op: %d ! doing nothing with it", __func__, bio_op(bio));
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
int __init ls_dm_init(void)
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
void __exit ls_dm_exit(void)
{
	dm_unregister_target(&lsdm_target);
	if(extents_slab)
		kmem_cache_destroy(extents_slab);
}


module_init(ls_dm_init);
module_exit(ls_dm_exit);

MODULE_DESCRIPTION(DM_NAME "log structured SMR Translation Layer");
MODULE_AUTHOR("Surbhi Palande <csurbhi@gmail.com>");
MODULE_LICENSE("GPL");
