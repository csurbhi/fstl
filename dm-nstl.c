/* i*- c-basic-offset: 8; indent-tabs-mode: t; compile-command: "make" -*-
 *
 * Copyright (C) 2016 Peter Desnoyers. All rights reserved.
 *
 * This file is released under the GPL.
 */

/*TODO:
 * Convert length to be that of blocks.
 * Currently it is that of sectors
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

static sector_t zone_start(struct ctx *ctx, sector_t pba) {
	return pba - (pba % ctx->nr_lbas_in_zone);
}
static sector_t zone_end(struct ctx *ctx, sector_t pba) {
	return zone_start(ctx, pba) + ctx->nr_lbas_in_zone;
}
static unsigned room_in_zone(struct ctx *ctx, sector_t sector) {
	return zone_end(ctx, sector) - sector + 1;   
}

/* zone numbers begin from 0.
 * The freebit map is marked with bit 0 representing zone 0
 */
static unsigned get_zone_nr(struct ctx *ctx, sector_t sector) {
	sector_t zone_begins = zone_start(ctx, sector);
	return (zone_begins / ctx->nr_lbas_in_zone);
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
		} else if (lba >= e->lba + e->len) {
			node = node->rb_right;
		} else {
			return e;
		}
	}
	return higher;
}

static struct extent *stl_rb_geq(struct ctx *ctx, off_t lba)
{
	struct extent *e = NULL;
	unsigned long flags;

	read_lock_irqsave(&ctx->extent_tbl_lock, flags); 
	e = _stl_rb_geq(&ctx->extent_tbl_root, lba);
	read_unlock_irqrestore(&ctx->extent_tbl_lock, flags); 

	return e;
}


int _stl_verbose;
static void stl_rb_insert(struct ctx *ctx, struct extent *new)
{
	struct rb_root *root = &ctx->extent_tbl_root;
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


static void stl_rb_remove(struct ctx *ctx, struct extent *e)
{
	struct rb_root *root = &ctx->extent_tbl_root;
	rb_erase(&e->rb, root);
	ctx->n_extents--;
}


static struct extent *stl_rb_next(struct extent *e)
{
	struct rb_node *node = rb_next(&e->rb);
	return (node == NULL) ? NULL : container_of(node, struct extent, rb);
}

struct stl_gc_thread {
	struct task_struct *stl_gc_task;
	wait_queue_head_t stl_gc_wait_queue;
	/* for gc sleep time */
	unsigned int urgent_sleep_time;
	unsigned int min_sleep_time;
        unsigned int max_sleep_time;
        unsigned int no_gc_sleep_time;

	/* for changing gc mode */
        unsigned int gc_wake;
};

static inline int stl_is_idle(void)
{
	return 1;
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

static int stl_gc(void)
{
	printk(KERN_INFO "\n GC thread polling after every few seconds ");
	return 0;
}

static int gc_zonenr(int zonenr)
{
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

	struct ctx *sc = (struct ctx *) data;
	struct stl_gc_thread *gc_th = sc->gc_th;
	wait_queue_head_t *wq = &gc_th->stl_gc_wait_queue;
	unsigned int wait_ms;

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
		if(!stl_is_idle()) {
			//increase_sleep_time();
			/* unlock mutex */
			continue;

		}
		stl_gc();


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
	kvfree(ctx->gc_th);
	return 0;
}




/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to map.
 * if blocked by cleaning, returns the extent which we're blocked on.
 */
static int stl_update_range(struct ctx *ctx, sector_t lba, sector_t pba, size_t len)
{
	struct extent *e = NULL, *new = NULL, *split = NULL;
	unsigned long flags;
	off_t new_lba, new_pba;
	size_t new_len;
	struct extent *tmp = NULL;
	int diff;

	BUG_ON(len == 0);

	if (unlikely(!(split = mempool_alloc(ctx->extent_pool, GFP_NOIO))))
		return -ENOMEM;

	if (unlikely(!(new = mempool_alloc(ctx->extent_pool, GFP_NOIO)))) {
		mempool_free(split, ctx->extent_pool);
		return -ENOMEM;
	}

	write_lock_irqsave(&ctx->extent_tbl_lock, flags);
	e = _stl_rb_geq(&ctx->extent_tbl_root, lba);

	if (e != NULL) {
		/* [----------------------]        e     new     split
		 *        [++++++]           -> [-----][+++++][--------]
		 */
		if (e->lba < lba && ((e->lba + e->len) > (lba + len))) {
			/* do this *before* inserting below */
			e->len = lba - e->lba;
			/* new is added at the end, first we split */
			new_lba = lba + len;
			new_len = e->lba + e->len - new_lba;
			new_pba = e->pba + (new_lba - e->lba);

			extent_init(split, new_lba, new_pba, new_len);
			extent_get(split, 1, IN_MAP);
			stl_rb_insert(ctx, split);

			e = new;
			split = NULL;
		}
		/* [------------]
		 *        [+++++++++]        -> [------][+++++++++]
		 */
		else if (e->lba < lba) {
			e->len = lba - e->lba;
			if (e->len == 0) {
				panic("Wrong length recorded in extent map table");
			}
			e = stl_rb_next(e);
			/* no split needed */
		}
		/*          [------]
		 *   [+++++++++++++++]        -> [+++++++++++++++]
		 */
		while (e != NULL && ((e->lba + e->len) <= (lba + len))) {
			tmp = stl_rb_next(e);
			stl_rb_remove(ctx, e);
			extent_put(ctx, e, 1, IN_MAP);
			e = tmp;
			/* no split needed */
		}
		/*          [------]
		 *   [+++++++++]        -> [++++++++++][---]
		 */
		if (e != NULL && ((lba + len) > e->lba)) {
			diff = (lba + len) - e->lba;
			e->lba += diff;
			e->pba += diff;
			e->len -= diff;
			/* no split needed */
		}
	}

	extent_init(new, lba, pba, len);
	extent_get(new, 1, IN_MAP);
	stl_rb_insert(ctx, new);
	write_unlock_irqrestore(&ctx->extent_tbl_lock, flags);
	if (split) {
		mempool_free(split, ctx->extent_pool);
		return 0;
	}
	return 0;
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
	sector_t sector;
	struct extent *e;
	unsigned nr_sectors, overlap;

	printk(KERN_INFO "Read begins! ");

	atomic_inc(&ctx->n_reads);
	while(split != bio) {
		sector = bio->bi_iter.bi_sector;
		e = stl_rb_geq(ctx, sector);
		nr_sectors = bio_sectors(bio);

		/* note that beginning of extent is >= start of bio */
		/* [----bio-----] [eeeeeee]  */
		if (e == NULL || e->lba >= sector + nr_sectors)  {
			printk(KERN_ERR "\n Case of no overlap");
			zero_fill_bio(bio);
			bio_endio(bio);
			break;
		}
		/* [eeeeeeeeeeee] eeeeeeeeeeeee]<- could be shorter or longer
		   [---------bio------] */
		else if (e->lba <= sector) {
			printk(KERN_ERR "\n e->lba <= sector");
			overlap = e->lba + e->len - sector;
			if (overlap < nr_sectors)
				nr_sectors = overlap;
			sector = e->pba + sector - e->lba;
		}
		/*             [eeeeeeeeeeee]
			       [---------bio------] */
		else {
			printk(KERN_ERR "\n e->lba >  sector");
			nr_sectors = e->lba - sector;
			split = bio_split(bio, nr_sectors, GFP_NOIO, &fs_bio_set);
			bio_chain(split, bio);
			zero_fill_bio(split);
			bio_endio(split);
			continue;
		}

		if (nr_sectors < bio_sectors(bio)) {
			split = bio_split(bio, nr_sectors, GFP_NOIO, &fs_bio_set);
			bio_chain(split, bio);
		} else {
			split = bio;
		}

		split->bi_iter.bi_sector = sector;
		bio_set_dev(bio, ctx->dev->bdev);
		//printk(KERN_INFO "\n read,  sc->n_reads: %d", sc->n_reads);
		generic_make_request(split);
	}
	printk(KERN_INFO "\n Read end");
	return 0;
}

static void add_revmap_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned int nrsectors);

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
	unsigned long flags;
	struct sit_page *sit_page;
	struct stl_seg_entry *ptr;
	struct rb_node *parent = NULL;
	int index;
	int zonenr, destn_zonenr;

	spin_lock(&ctx->sit_kv_store_lock);
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	if (!sit_page) {
		sit_page = add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, couldnt allocate sit entry");
		}
	}
	get_page(sit_page->page);
	ptr = (struct stl_seg_entry *)sit_page->page;
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = BLOCKS_IN_ZONE;
	ptr->mtime = 0;
	SetPageDirty(sit_page->page);
	put_page(sit_page->page);
	spin_unlock(&ctx->sit_kv_store_lock);

	spin_lock_irqsave(&ctx->lock, flags);
	ctx->nr_invalid_zones++;
	pba = get_new_zone(ctx);
	destn_zonenr = get_zone_nr(ctx, pba);
	spin_unlock_irqrestore(&ctx->lock, flags);
	if (0 > pba) {
		printk(KERN_INFO "No more disk space available for writing!");
		return;
	}
	copy_blocks(zonenr, destn_zonenr); 
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
	struct nstl_bioctx *bioctx = NULL;
	struct bio *bio = NULL;
	struct ctx * ctx = NULL;
	int ret;
	u64 wf;

	subbioctx = clone->bi_private;
	if (!subbioctx)
		panic("subbioctx is NULL !");

	bioctx = subbioctx->bioctx;
	if(!bioctx)
		panic("bioctx is NULL!");
	bio = bioctx->orig;
	ctx = bioctx->ctx;


	if (subbioctx->magic != SUBBIOCTX_MAGIC) {
		/* private has been overwritten */
		return;
	}

	/* If a single segment of the bio fails, the bio should be
	 * recorded with this status. No translation entry
	 * for the entire original bio should be maintained
	 *
	 * TODO: should we keep trying till the write succeed?
	 * Maybe to another zone, maybe invoke the shrinker if
	 * memory is low etc.
	 */

	if (clone->bi_status != BLK_STS_OK) {
		/* we don't keep partial bio translation map entries.
		 * The write either succeeds atomically or does not at
		 * all
		 */
		remove_partial_entries(ctx, bio);
		/* Remove the partial STL entry as well */
		/* If this is a SMR zone, then a sector write failure
		 * causes all further I/O in that zone to fail.
		 * Hence we can no longer write to this zone
		 */

		switch(clone->bi_status) {
			case BLK_STS_IOERR:
				if(!nstl_zone_seq(ctx, clone->bi_iter.bi_sector))
					mark_zone_erroneous(ctx, clone->bi_iter.bi_sector);
			/* If this zone is sequential, then we do not have
			 * to do anything. There will be a gap in this
			 * segment. But thats about it.
			 */
				/*TODO: try writing the data elsewhere and
				 * complete the write
				 */
				wf = ctx->write_frontier;
				subbioctx->extent.pba = wf;
				clone->bi_iter.bi_sector = wf; /* we use the save write frontier */
				generic_make_request(clone);
				return;

			case BLK_STS_NOSPC:
			/* our meta information does not match that of
			 * the disk's state. We dont know what to do.
			 */
				panic("No space on disk! Mismanaged meta data");

			case BLK_STS_RESOURCE:
			/* TODO: memory is low. Can you try rewriting? */
				panic("Low memory, cannot function!");
			case BLK_STS_AGAIN:
			/* You need to try only a few number of times.
			 * TODO: keep count, or else you will loop on
			 * this.
			 */
				subbioctx->retry++;
				if (subbioctx->retry < 5) {
					generic_make_request(clone);
					return;
				}
				break;
			default:
				panic("Unknown IO error! better handling needed!");

		}
		
		if (bio->bi_status == BLK_STS_OK) {
			bio->bi_status = clone->bi_status;
		}
	}


	/* We add the translation entry only when we know
	 * that the write has made it to the disk
	 */
	if(clone->bi_status == BLK_STS_OK) {
		ret = stl_update_range(ctx, subbioctx->extent.lba, subbioctx->extent.pba, subbioctx->extent.len);
		add_revmap_entries(ctx, subbioctx->extent.lba, subbioctx->extent.pba, subbioctx->extent.len);
	}
	/* When the refcount becomes 0, we need to call the bio_endio
	 * of the original bio
	 */
	if (refcount_dec_and_test(&bioctx->ref)) {
		bio_endio(bio);
	}

	bio_put(clone);
}

/* 
 * 1 indicates that the zone is free 
 *
 * Zone numbers start from 0
 */
static void mark_zone_free(struct ctx *ctx , int zonenr)
{	
	char *bitmap;
	int nr_freezones;
	int bytenr;
	int bitnr;


	if (unlikely(NULL == ctx)) {
		panic("This is a ctx bug");
	}
		
	bitmap = ctx->freezone_bitmap;
	nr_freezones = ctx->nr_freezones;
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
/* moves the write frontier, returns the LBA of the packet trailer
*/
static int get_new_zone(struct ctx *ctx)
{
	unsigned long zone_nr;
	int trial;


	atomic_inc(&ctx->zone_revmap_count);
	wait_on_zone_barrier(ctx);

	if (ctx->write_frontier > ctx->wf_end)
		printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->write_frontier, ctx->wf_end);
	BUG_ON(ctx->write_frontier > ctx->wf_end);
	trial = 0;
try_again:
	zone_nr = get_next_freezone_nr(ctx);
	if (zone_nr < 0) {
		stl_gc();
		if (0 == trial) {
			trial++;
			goto try_again;
		}
		printk(KERN_INFO "No more disk space available for writing!");
		mark_disk_full(ctx);
		ctx->write_frontier = -1;
		return (ctx->write_frontier);
	}


	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->write_frontier = zone_start(ctx, zone_nr) + ctx->sb->zone0_pba;
	ctx->wf_end = zone_end(ctx, ctx->write_frontier);
	if (ctx->write_frontier > ctx->wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %llu", ctx->free_sectors_in_wf );
	}
	ctx->free_sectors_in_wf = ctx->wf_end - ctx->write_frontier + 1;
	ctx->nr_freezones--;
	printk(KERN_INFO "Num of free sect.: %llu, diff of end and wf:%llu\n", ctx->free_sectors_in_wf, ctx->wf_end - ctx->write_frontier);
	add_ckpt_new_wf(ctx, ctx->write_frontier);
	return ctx->write_frontier;
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

void ckpt_flushed(struct bio *bio)
{
	struct ctx *ctx = bio->bi_private;

	spin_lock_irq(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_dec(&ctx->ckpt_ref);
	spin_unlock_irq(&ctx->ckpt_lock);
	wake_up(&ctx->ckptq);
	if (BLK_STS_OK == bio->bi_status) {
		/* TODO: do something more! */
		bio_put(bio);
		bio_free_pages(bio);
		return;
	} 
	/* TODO: Do something more to handle the errors */
	printk(KERN_DEBUG "\n Could not read the translation entry block");
	bio_free_pages(bio);
	/* bio_alloc, hence bio_put */
	bio_put(bio);
	return;
}

void revmap_bitmap_flushed(struct bio *);
/*
 * Since bitmap can be created in case of a crash, we do not 
 * wait for the bitmap to be flushed.
 * We only initiate the flush for the bitmap.
 */
struct page * flush_revmap_bitmap(struct ctx *ctx)
{
	struct bio * bio;
	struct page *page;
	int i, nr_pages;
	sector_t pba;
	struct page ** revmap_bm_pages;

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		return NULL;
	}
	nr_pages = ctx->sb->blk_count_revmap_bm;
	revmap_bm_pages = ctx->revmap_bm;
	for(i=0; i<nr_pages; i++) {
		page = *revmap_bm_pages;
		/* bio_add_page sets the bi_size for the bio */
		if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
			bio_put(bio);
			return NULL;
		}
		revmap_bm_pages = revmap_bm_pages + 1;
	}
	pba = ctx->sb->revmap_pba;
	bio->bi_end_io = revmap_bitmap_flushed;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	spin_lock_irq(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_inc(&ctx->ckpt_ref);
	spin_unlock_irq(&ctx->ckpt_lock);
	bio->bi_private = ctx;
	submit_bio(bio);
	return page;
}

/* We initiate a flush of a checkpoint,
 * we do not wait for it to complete
 */
struct page * flush_checkpoint(struct ctx *ctx)
{
	struct bio * bio;
	struct page *page;
	sector_t pba;

	page = ctx->ckpt_page;
	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		return NULL;
	}

	/* bio_add_page sets the bi_size for the bio */
	if (PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return NULL;
	}
	pba = ctx->ckpt_pba;
	/* Record the pba for the next ckpt */
	if (pba == ctx->sb->ckpt1_pba)
		ctx->ckpt_pba = ctx->sb->ckpt2_pba;
	else
		ctx->ckpt_pba = ctx->sb->ckpt1_pba;
	bio->bi_end_io = ckpt_flushed;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	spin_lock_irq(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_inc(&ctx->ckpt_ref);
	spin_unlock_irq(&ctx->ckpt_lock);
	bio->bi_private = ctx;
	submit_bio(bio);
	return page;
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
	spin_lock_irq(&ctx->ckpt_lock);
	atomic_dec(&ctx->ckpt_ref);
	wait_event_lock_irq(ctx->ckptq, (!atomic_read(&ctx->ckpt_ref)), ctx->ckpt_lock);
	spin_unlock_irq(&ctx->ckpt_lock);
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


void update_checkpoint(struct ctx *ctx)
{
	struct page * page;
	struct stl_ckpt * ckpt;

	page = ctx->ckpt_page;
	ckpt = (struct stl_ckpt *) page_address(page);
	ckpt->version += 1;
	ckpt->user_block_count = ctx->user_block_count;
	ckpt->nr_invalid_zones = ctx->nr_invalid_zones;
	ckpt->cur_frontier_pba = ctx->write_frontier;
	ckpt->nr_free_zones = ctx->nr_freezones;
	ckpt->elapsed_time = get_elapsed_time(ctx);
	ckpt->clean = 1;
	ckpt->crc = calculate_crc(ctx, page);
}

/* TODO: We need to do this under a lock for concurrent writes
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
	
	ctx->write_frontier = ctx->write_frontier + sectors_s8;
	ctx->free_sectors_in_wf = ctx->free_sectors_in_wf - sectors_s8;
	ctx->user_block_count -= sectors_s8 / NR_SECTORS_IN_BLK;
	if (ctx->free_sectors_in_wf < NR_SECTORS_IN_BLK) {
		get_new_zone(ctx);
		if (ctx->write_frontier < 0) {
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
	spin_lock_irq(&ctx->rev_flush_lock);
	wait_event_lock_irq(ctx->rev_blk_flushq, !atomic_read(&ctx->nr_pending_writes), ctx->rev_flush_lock);
	spin_unlock_irq(&ctx->rev_flush_lock);
}

int is_revmap_block_available(struct ctx *ctx, u64 pba);
void flush_translation_blocks(struct ctx *ctx);

void wait_on_revmap_block_availability(struct ctx *ctx, u64 pba)
{
	spin_lock_irq(&ctx->rev_flush_lock);
	if (1 != is_revmap_block_available(ctx, pba)) {
/* You could wait here for ever if the translation blocks are not
 * flushed!
 */
		spin_unlock_irq(&ctx->rev_flush_lock);
		if (spin_trylock(&ctx->flush_lock)) {
			flush_translation_blocks(ctx);
			spin_unlock(&ctx->flush_lock);
		}
		spin_lock_irq(&ctx->rev_flush_lock);
	}
	/* wait until atleast one zone's revmap entries are flushed */
	wait_event_lock_irq(ctx->rev_blk_flushq, (1 == is_revmap_block_available(ctx, pba)), ctx->rev_flush_lock);
	spin_unlock_irq(&ctx->rev_flush_lock);
}

void wait_on_zone_barrier(struct ctx * ctx)
{
	spin_lock_irq(&ctx->flush_zone_lock);
	/* wait until atleast one zone's revmap entries are flushed */
	wait_event_lock_irq(ctx->zone_entry_flushq, (MAX_ZONE_REVMAP >= atomic_read(&ctx->zone_revmap_count)), ctx->flush_zone_lock);
	spin_unlock_irq(&ctx->flush_zone_lock);
	/* TODO: do we need to call checkpoint here?
	 * checkpoint();
	 */
}


void remove_sit_blk_kv_store(struct ctx *ctx, u64 pba)
{
	struct rb_root *rb_root = &ctx->sit_rb_root;
	struct sit_page *new;

	new = search_sit_blk(ctx, pba);
	if (!new)
		return;

	rb_erase(&new->rb, rb_root);
	atomic_dec(&ctx->nr_sit_pages);
}


struct page * read_block(struct ctx *ctx, u64 blknr, u64 base, int nrblks);
/*
 * pba: stored in the LBA - PBA translation.
 * This is the PBA of some data block. This PBA belongs to some zone.
 * We are about to update the SIT entry that belongs to that zone
 * Before that we read that corresponding page in memory and then
 * add it to our RB tree that we use for searching.
 *
 * This function should be called only after taking the
 * ctx->sit_kv_store_lock
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

	new = search_sit_kv_store(ctx, pba, &parent);
	if (new)
		return new;

	new = kmem_cache_alloc(ctx->sit_page_cache, GFP_KERNEL);
	if (!new)
		return NULL;

	RB_CLEAR_NODE(&new->rb);

	sit_blknr = zonenr / SIT_ENTRIES_BLK;
	page = read_block(ctx, sit_blknr, ctx->sb->sit_pba, 1);
	if (!page) {
		kmem_cache_free(ctx->sit_page_cache, new);
		return NULL;
	}

	ClearPageDirty(page);

	/* We put the page when the page gets written to the disk */
	get_page(page);
	new->blknr = sit_blknr;
	new->page = page;

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
	/* Balance the tree after the blknr is addded to it */
	rb_insert_color(&new->rb, root);
	atomic_inc(&ctx->nr_tm_pages);
	atomic_inc(&ctx->sit_flush_count);
	if (atomic_read(&ctx->sit_flush_count) >= MAX_SIT_PAGES) {
		if (spin_trylock(&ctx->flush_lock)) {
			/* Only one flush operation at a time 
			 * but we dont want to wait.
			 */
			atomic_set(&ctx->sit_flush_count, 0);
			flush_sit(ctx);
			spin_unlock(&ctx->flush_lock);
		}
	}

	return new;
}

/*TODO: IMPLEMENT 
 */
void mark_segment_free(struct ctx *ctx, sector_t zonenr)
{
	return;
}

/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_vblocks_decr(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct stl_seg_entry *ptr;
	sector_t zonenr;
	unsigned long flags;
	int index;
	struct rb_node *parent = NULL;

	spin_lock(&ctx->sit_kv_store_lock);
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	if (!sit_page) {
		sit_page= add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, could not allocate sit_entry");
		}
	}
	get_page(sit_page->page);
	ptr = (struct stl_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = ptr->vblocks - 1;
	if (!ptr->vblocks) {
		spin_lock_irqsave(&ctx->lock, flags);
		mark_zone_free(ctx , zonenr);
		spin_unlock_irqrestore(&ctx->lock, flags);
	}
	SetPageDirty(sit_page->page);
	put_page(sit_page->page);
	spin_unlock(&ctx->sit_kv_store_lock);
}

static void mark_zone_occupied(struct ctx *ctx , int zonenr);
/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_vblocks_incr(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct stl_seg_entry *ptr;
	struct rb_node *parent = NULL;
	unsigned long flags;
	sector_t zonenr;
	int index;

	spin_lock(&ctx->sit_kv_store_lock);
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	if (!sit_page) {
		sit_page = add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, could not allocate sit_entry");
		}
	}
	get_page(sit_page->page);
	ptr = (struct stl_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	if (!ptr->vblocks) {
		spin_lock_irqsave(&ctx->lock, flags);
		mark_zone_occupied(ctx, zonenr);
		spin_unlock_irqrestore(&ctx->lock, flags);
	}
	ptr->vblocks = ptr->vblocks + 1;
	SetPageDirty(sit_page->page);
	put_page(sit_page->page);
	spin_unlock(&ctx->sit_kv_store_lock);
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

	spin_lock(&ctx->sit_kv_store_lock);
	sit_page = search_sit_kv_store(ctx, pba, &parent);
	if (unlikely(!sit_page)) {
		sit_page = add_sit_entry_kv_store(ctx, pba);
		if (!sit_page) {
		/* TODO: do something, low memory */
			panic("Low memory, could not allocate sit_entry");
		}
	}
	get_page(sit_page->page);
	SetPageDirty(sit_page->page);
	ptr = (struct stl_seg_entry*) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->mtime = get_elapsed_time(ctx);
	put_page(sit_page->page);
	spin_unlock(&ctx->sit_kv_store_lock);
}

struct tm_page * search_tm_kv_store(struct ctx *ctx, u64 blknr, struct rb_node **parent);
struct tm_page *add_tm_entry_kv_store(struct ctx *ctx, u64 lba, refcount_t *ref);
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
			  unsigned long pba, unsigned long len, refcount_t *ref)
{
	struct tm_entry * ptr;
	int i, index;
	struct rb_node *parent = NULL;
	struct tm_page *tm_page;

	ptr = (struct tm_entry *) page_address(page);
	index = lba % TM_ENTRIES_BLK;
	ptr = ptr + index;

	/* Assuming len is in terms of sectors 
	 * We convert sectors to blocks
	 */
	for (i=0; i<len/8; i++) {
		lock_page(page);
		SetPageDirty(page);
		get_page(page);
		if (ptr->lba != 0) {
			/* decrement vblocks for the segment that has
			 * the stale block
			 */
			sit_ent_vblocks_decr(ctx, ptr->pba);
		}
		ptr->lba = lba;
		ptr->pba = pba;
		sit_ent_vblocks_incr(ctx, ptr->pba);
		if (pba == zone_end(ctx, pba)) {
			sit_ent_add_mtime(ctx, ptr->pba);
		}
		lba = lba + BLK_SIZE;
		pba = pba + BLK_SIZE;
		ptr = ptr + 1;
		put_page(page);
		index = index + 1;
		if (TM_ENTRIES_BLK == index) {
			tm_page = search_tm_kv_store(ctx, lba, &parent);
			if (!tm_page) {
				tm_page = add_tm_entry_kv_store(ctx, lba, ref);
				if (!tm_page) {
					/* TODO: try freeing some
					 * pages here
					 */
					panic("Low memory, while adding tm entry ");
				}
			}
			ptr = (struct tm_entry *) page_address(tm_page->page);
			index = 0;
		}
	}
	return 0;	
}

void wakeup_refcount_waiters(struct ctx *ctx);

void read_complete(struct bio * bio)
{
	refcount_t *ref;
       	struct ctx *ctx;
	struct read_ctx *read_ctx = bio->bi_private;

	ctx = read_ctx->ctx;
	ref = read_ctx->ref;

	refcount_dec(ref);
	wakeup_refcount_waiters(ctx);
	/* bio_alloc done, hence bio_put */
	bio_put(bio);
}

void wait_on_refcount(struct ctx *ctx, refcount_t *ref);

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
	refcount_t *ref;
	struct read_ctx *read_ctx;

	u64 pba = (base + blknr * NR_SECTORS_IN_BLK);
    	page = alloc_page(__GFP_ZERO|GFP_KERNEL);
	if (!page )
		return NULL;

	ref = kzalloc(sizeof(refcount_t), GFP_KERNEL);
	if (!ref) {
		__free_pages(page, 0);
		return NULL;
	}

	read_ctx = kmem_cache_alloc(ctx->read_ctx_cache, GFP_KERNEL);
	if (!read_ctx) {
		__free_pages(page, 0);
		kfree(ref);
		return NULL;
	}

	refcount_set(ref, 1);
	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		__free_pages(page, 0);
		kfree(ref);
		kmem_cache_free(ctx->read_ctx_cache, read_ctx);
		return NULL;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		__free_pages(page, 0);
		kfree(ref);
		kmem_cache_free(ctx->read_ctx_cache, read_ctx);
		bio_put(bio);
		return NULL;
	}
	bio->bi_end_io = read_complete;
	bio->bi_private = &read_ctx;
	read_ctx->ctx = ctx;
	read_ctx->ref = ref;
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_iter.bi_sector = pba;
	submit_bio(bio);
	wait_on_refcount(ctx, ref);
    	kfree(ref);
	kmem_cache_free(ctx->read_ctx_cache, read_ctx);
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

	new = search_tm_kv_store(ctx, blknr, &parent);
	if (!new)
		return;

	rb_erase(&new->rb, rb_root);
	atomic_dec(&ctx->nr_tm_pages);
}


void remove_tm_blk_kv_store(struct ctx *ctx, u64 blknr)
{
	struct rb_node *parent = NULL;
	struct rb_root *rb_root = &ctx->tm_rb_root;
	struct tm_page *new;

	new = search_tm_kv_store(ctx, blknr, &parent);
	if (!new)
		return;
	rb_erase(&new->rb, rb_root);
	atomic_dec(&ctx->nr_tm_pages);
}

/*
 *
 * ctx->tm_flush_lock is used for two things:
 * a) refcount for every translation page
 * b) for translation page flags: lock, dirty flag.
 *
 * Note: add_block_based_translation() is called with the ctx->tm_flush_lock
 * held. When 100 translation pages collect in memory, they are
 * flushed to disk by the same function and thus under the same tm_flush_lock.
 * 
 * write_tmbl_complete() calls this same tm_flush_lock. It will be blocked
 * till all the pages are submitted and eventually
 * add_block_based_translation() completes. The flush also has a plug
 * function, where all the pages are flushed together.
 *
 */
void write_tmbl_complete(struct bio *bio)
{
	struct ctx *ctx;
	struct tm_page_write_ctx *tm_page_write_ctx;
	struct page *page;
	struct tm_page *tm_page;
	struct list_head *temp;
	struct ref_list *refnode;
	sector_t blknr;

	tm_page_write_ctx = (struct tm_page_write_ctx *) bio->bi_private;
	ctx = tm_page_write_ctx->ctx;
	tm_page = tm_page_write_ctx->tm_page;
	page = tm_page->page;
	put_page(page);

	/* We notify all those reverse map pages that are stuck on
	 * these respective refcounts
	 */
	list_for_each(temp, &tm_page->reflist) {
		refnode = list_entry(temp, struct ref_list, list);
		spin_lock(&ctx->tm_flush_lock);
		refcount_dec(refnode->ref);
		spin_unlock(&ctx->tm_flush_lock);
		kmem_cache_free(ctx->reflist_cache, refnode);
		/* Wakeup anyone who is waiting on this reference */
		wakeup_refcount_waiters(ctx);
	}

	if (bio->bi_status != BLK_STS_OK) {
		/*TODO: Perhaps retry!! or do something more
		 * or else you will loose the translation entries.
		 * write them some place else!
		 */
		panic("Could not read the translation entry block");
	}
	spin_lock(&ctx->tm_flush_lock);
	/* If the page was locked, then since the page was submitted,
	 * write has been attempted
	 */
	if (!PageLocked(page)) {
		blknr = bio->bi_iter.bi_sector - ctx->sb->tm_pba;
		/* remove the page from RB tree and reduce the total
		 * count, through the remove_tm_blk_kv_store()
		 */
		remove_tm_blk_kv_store(ctx, blknr);
		put_page(page);
		bio_free_pages(bio);
	}
	spin_unlock(&ctx->tm_flush_lock);
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);
	kmem_cache_free(ctx->tm_page_write_cache, tm_page_write_ctx);
}

/* We don't wait for the bios to complete 
 * flushing. We only initiate the flushing
 */
void flush_tm_node_page(struct ctx *ctx, struct rb_node *node)
{
	struct page *page; 
	struct tm_page *node_ent;
	u64 pba;
	struct bio * bio;
	struct tm_page_write_ctx *tm_page_write_ctx;

	node_ent = rb_entry(node, struct tm_page, rb);
	page = node_ent->page;

	/* Only flush if the page needs flushing */

	spin_lock(&ctx->tm_flush_lock);
	if (!PageDirty(page)) {
		spin_unlock(&ctx->tm_flush_lock);
		return;
	}
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

	/* Sector addressing, LBA is the address of the sector */
	pba = (node_ent->blknr * NR_SECTORS_IN_BLK) + ctx->sb->tm_pba;
	page = node_ent->page;
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		kmem_cache_free(ctx->tm_page_write_cache, tm_page_write_ctx);
		return;
	}
	bio->bi_end_io = write_tmbl_complete;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	tm_page_write_ctx->tm_page = node_ent;
       	tm_page_write_ctx->ctx = ctx;
	bio->bi_private = tm_page_write_ctx;
	/* Note we are called with the ctx->tm_flush_lock held here!!
	 * We unlock the page here. If the page is locked again
	 * then it was done from the write path.
	 * page is not flushed again unless its dirtied again!
	 */
	ClearPageDirty(page);
	/* We increment the reference, so that the page is not freed
	 * under us
	 */
	get_page(page);
	unlock_page(page);
	submit_bio(bio);
}


void flush_tm_nodes(struct rb_node *node, struct ctx *ctx)
{	
	flush_tm_node_page(ctx, node);
	if (node->rb_left)
		flush_tm_nodes(node->rb_left, ctx);
	if (node->rb_right)
		flush_tm_nodes(node->rb_right, ctx);
}

void flush_translation_blocks(struct ctx *ctx)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = NULL;
	struct blk_plug plug;

	node = root->rb_node;
	if (!node)
		return;

	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	blk_start_plug(&plug);
	flush_tm_nodes(node, ctx);
	blk_finish_plug(&plug);	
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
	spin_lock_irq(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_dec(&ctx->ckpt_ref);
	spin_unlock_irq(&ctx->ckpt_lock);
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
	/* We have stored relative blk address, whereas disk supports
	 * sector addressing
	 * TODO: store the sector address directly 
	 */
	blknr = (bio->bi_iter.bi_sector - ctx->sb->sit_pba) / NR_SECTORS_IN_BLK;
	if (!PageLocked(page)) {
		/* For memory conservation we do this freeing of pages
		 * TODO: we could free them only if our memory usage
		 * is above a certain point
		 */
		remove_sit_blk_kv_store(ctx, blknr);
		put_page(page);
		bio_free_pages(bio);
		atomic_dec(&ctx->nr_sit_pages);
	}
	spin_unlock(&ctx->sit_flush_lock);
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
	bio->bi_end_io = write_sitbl_complete;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	sit_ctx->ctx = ctx;
	spin_lock_irq(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_inc(&ctx->ckpt_ref);
	spin_unlock_irq(&ctx->ckpt_lock);
	sit_ctx->page = page;
	bio->bi_private = sit_ctx;
	spin_lock(&ctx->sit_flush_lock);
	unlock_page(page);
	spin_unlock(&ctx->sit_flush_lock);
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

/*
 * TODO: Use locks while adding entries to the KV store to protect
 * against concurrent access
 *
 * lba: from the LBA-PBA pair of a data block.
 */
struct tm_page *add_tm_entry_kv_store(struct ctx *ctx, u64 lba, refcount_t *ref)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *parent = NULL;
	struct tm_page *new, *parent_ent;
	u64 blknr = lba / TM_ENTRIES_BLK;
	struct list_head *temp;
	struct ref_list *refnode, *tempref;

	refnode = kmem_cache_alloc(ctx->reflist_cache, GFP_KERNEL);
	if (!refnode) {
		return NULL;
	}
	INIT_LIST_HEAD(&refnode->list);


	new = search_tm_kv_store(ctx, blknr, &parent);
	if (new) {
		list_for_each(temp, &new->reflist) {
			tempref = list_entry(temp, struct ref_list, list);
			if (tempref->ref == ref) {
				kmem_cache_free(ctx->reflist_cache, refnode);
				return new;
			}
		}
		/* Add a refcount, only when this rev map page is used
		 * for the first time
		 */
		refnode->ref = ref;
		refcount_inc(ref);
		list_add(&refnode->list, &new->reflist);
		return new;
	}

	new = kmem_cache_alloc(ctx->tm_page_cache, GFP_KERNEL);
	if (!new) {
		kmem_cache_free(ctx->reflist_cache, refnode);
		return NULL;
	}
	
	RB_CLEAR_NODE(&new->rb);

	new->page = read_block(ctx, blknr, ctx->sb->tm_pba, 1);
	if (!new->page) {
		kmem_cache_free(ctx->reflist_cache, refnode);
		kmem_cache_free(ctx->tm_page_cache, new);
		return NULL;
	}

	/* Add this page to a RB tree based KV store.
	 * Key is: blknr for this corresponding block
	 */
	parent_ent = rb_entry(parent, struct tm_page, rb);
	if (blknr < parent_ent->blknr) {
		/* Attach new node to the left of parent */
		rb_link_node(&new->rb, parent, &parent->rb_left);
	}
	else { 
		/* Attach new node to the right of parent */
		rb_link_node(&new->rb, parent, &parent->rb_right);
	}
	INIT_LIST_HEAD(&new->reflist);
	new->blknr = blknr;
	lock_page(new->page);
	/* This is a new page, cannot be dirty */
	ClearPageDirty(new->page);
	refnode->ref = ref;
	refcount_inc(ref);
	list_add(&refnode->list, &new->reflist);
	rb_insert_color(&new->rb, root);
	atomic_inc(&ctx->nr_tm_pages);
	atomic_inc(&ctx->tm_flush_count);
	if (atomic_read(&ctx->tm_flush_count) >= MAX_TM_PAGES) {
		if (spin_trylock(&ctx->flush_lock)) {
			atomic_set(&ctx->tm_flush_count, 0);
			/* Only one flush operation at a time 
			 * but we dont want to wait.
			 */
			flush_translation_blocks(ctx);
			spin_unlock(&ctx->flush_lock);
		}
	}
	return new;
}

/* Make the length in terms of sectors or blocks?
 *
 * This function is called with ctx->tm_flush_lock held!
 *
 */
int add_block_based_translation(struct ctx *ctx, struct page *page, refcount_t *ref)
{
	
	struct stl_revmap_entry_sector * ptr;
	struct tm_page * tm_page = NULL;
	int i, nr_entries, j;

	ptr = (struct stl_revmap_entry_sector *)page_address(page);
	
	nr_entries = NR_EXT_ENTRIES_PER_SEC * NR_SECTORS_IN_BLK;
	i = 0;
	while (i < nr_entries) {
		for(j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			tm_page = add_tm_entry_kv_store(ctx, ptr->extents[j].lba, ref);
			if (!tm_page)
				return -ENOMEM;
			add_translation_entry(ctx, tm_page->page, ptr->extents[j].lba, ptr->extents[j].pba, ptr->extents[j].len, ref);
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
	spin_lock_irq(&ctx->ckpt_lock);
	if(ctx->flag_ckpt)
		atomic_dec(&ctx->ckpt_ref);
	spin_unlock_irq(&ctx->ckpt_lock);
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
	unsigned char mask;
	struct page ** revmap_bm_pages;
	struct page *page;
	unsigned int blknr = 0;

	revmap_bm_pages = ctx->revmap_bm;

	pba = pba - ctx->sb->revmap_pba;
	bytenr = pba/BITS_IN_BYTE;
	bitnr = pba % BITS_IN_BYTE;
	mask = (1 << bitnr);
	blknr = bytenr / BLOCK_SIZE;
	revmap_bm_pages += blknr;
	page = *revmap_bm_pages;
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
	struct page ** revmap_bm_pages;
	struct page *page;
	unsigned int blknr = 0;

	revmap_bm_pages = ctx->revmap_bm;

	pba = pba - ctx->sb->revmap_pba;
	bytenr = pba/BITS_IN_BYTE;
	bitnr = pba % BITS_IN_BYTE;
	mask = ~(1 << bitnr);
	blknr = bytenr / BLOCK_SIZE;
	revmap_bm_pages += blknr;
	page = *revmap_bm_pages;
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
	struct page ** revmap_bm_pages;
	struct page *page;
	unsigned int blknr = 0;

	revmap_bm_pages = ctx->revmap_bm;

	pba = pba - ctx->sb->revmap_pba;
	bytenr = pba/BITS_IN_BYTE;
	bitnr = pba % BITS_IN_BYTE;
	blknr = bytenr / BLOCK_SIZE;
	revmap_bm_pages += blknr;
	page = *revmap_bm_pages;
	ptr = page_address(page);

	ptr = ptr + bytenr;
	temp = *ptr;

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
void wakeup_refcount_waiters(struct ctx *ctx)
{
	wake_up(&ctx->refq);
}

void wait_on_refcount(struct ctx *ctx, refcount_t *ref)
{
	spin_lock_irq(&ctx->tm_ref_lock);
	wait_event_lock_irq(ctx->refq, !refcount_read(ref), ctx->tm_ref_lock);
	spin_unlock_irq(&ctx->tm_ref_lock);
}


/*
 * TODO: Error handling metadata flushing
 *
 */
void revmap_entries_flushed(struct bio *bio)
{
	struct ctx * ctx = bio->bi_private;
	struct revmap_meta_inmem *revmap_bio_ctx;
	sector_t pba;

	switch(bio->bi_status) {
		case BLK_STS_OK:
			atomic_dec(&ctx->nr_pending_writes);
			revmap_bio_ctx = bio->bi_private;
			pba = revmap_bio_ctx->revmap_pba;
			if (pba == zone_end(ctx, pba)) {
				atomic_dec(&ctx->zone_revmap_count);
				wake_up(&ctx->zone_entry_flushq);
			}
			spin_lock(&ctx->tm_flush_lock);
			add_block_based_translation(ctx, revmap_bio_ctx->page, revmap_bio_ctx->ref);
			spin_unlock(&ctx->tm_flush_lock);
			/* Wakeup waiters waiting on the block barrier
			 * */
			wake_up(&ctx->rev_blk_flushq);
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
			bio_free_pages(bio);
			/* By now add_block_based_translations would have
			 * incremented the reference atleast once!
			 */
			wait_on_refcount(ctx, revmap_bio_ctx->ref);
			kfree(&revmap_bio_ctx->ref);
			/* now we calculate the pba where the revmap
			 * is flushed. We can reuse that pba as all
			 * entries related to it are on disk in the
			 * right place.
			 */
			pba = bio->bi_iter.bi_sector;
			clear_revmap_bit(ctx, pba);
			/* Wakeup waiters waiting on revblk bit that 
			 * indicates that revmap block is rewritable
			 * since all the containing translation map
			 * entries are now on disk
			 */
			wake_up(&ctx->rev_blk_flushq);
			break;
		case BLK_STS_AGAIN:
			/* retry a limited number of times.
			 * Maintain a retrial count in
			 * bio->bi_private
			 */
			break;
		default:
			/* TODO: do something better?
			 * remove all the entries from the in memory 
			 * RB tree and the reverse map in memory.
			remove_partial_entries();
			 */
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
int flush_revmap_block_disk(struct ctx * ctx, struct page *page, u64 pba)
{
	struct bio * bio;
	struct revmap_meta_inmem *revmap_bio_ctx;
	refcount_t *ref;

	ref = (refcount_t *) kzalloc(sizeof(refcount_t), GFP_KERNEL);
	if (!ref) {
		return -ENOMEM;
	}

	refcount_set(ref, 1);
	revmap_bio_ctx = kmem_cache_alloc(ctx->revmap_bioctx_cache, GFP_KERNEL);
	if (!revmap_bio_ctx) {
		kfree(ref);
		return -ENOMEM;
	}

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		kmem_cache_free(ctx->revmap_bioctx_cache, revmap_bio_ctx);
		kfree(ref);
		return -ENOMEM;
	}
	
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		kmem_cache_free(ctx->revmap_bioctx_cache, revmap_bio_ctx);
		kfree(ref);
		bio_put(bio);
		return -EFAULT;
	}
	
	revmap_bio_ctx->ctx = ctx;
	revmap_bio_ctx->magic = REVMAP_PRIV_MAGIC;
	revmap_bio_ctx->page = page;
	revmap_bio_ctx->ref = ref;
	bio->bi_iter.bi_sector = ctx->revmap_pba;

	wait_on_revmap_block_availability(ctx, bio->bi_iter.bi_sector);

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
	bio->bi_end_io = revmap_entries_flushed;
	bio->bi_private = revmap_bio_ctx;
	atomic_inc(&ctx->nr_pending_writes);
	generic_make_request(bio);
	return 0;
}


/*
 * There are 4 remap bitmap pages
 * These are NOT contiguous. But at any given time,
 * only one of these should not be flushed.
 * So, we dont have to track all of them.
 * The unflushed page is stored in ctx->revmap_page.
 */
void flush_revmap_entries(struct ctx *ctx)
{
	struct page *page = ctx->revmap_page;
	sector_t pba;
	struct stl_revmap_entry_sector * ptr;
	int sector_nr, revmap_blk_count, index;

	if (!page) {
		panic("revmap page is NULL!");
	}

	spin_lock(&ctx->rev_flush_lock);
	ptr = (struct stl_revmap_entry_sector *)page_address(page);
	/* The revmap_blk_count is the count where the next entry
	 * will be added
	 */
	revmap_blk_count = atomic_read(&ctx->revmap_blk_count) - 1;
	sector_nr = revmap_blk_count / NR_EXT_ENTRIES_PER_SEC;
	index = atomic_read(&ctx->revmap_sector_count);
	ptr = ptr + sector_nr;
	/* Flush the entries to the disk */
	pba = ptr->extents[index].pba;
	/* TODO: Alternatively, we could flag, that
	 * this is the last pba of the zone
	 * from the caller of this function
	 */
	flush_revmap_block_disk(ctx, page, pba);
	spin_unlock(&ctx->rev_flush_lock);
	wait_on_block_barrier(ctx);
}



/* We store only the LBA. We can calculate the PBA from the wf
 */
static void add_revmap_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned int nrsectors)
{
	struct stl_revmap_entry_sector * ptr = NULL;
	int i, j;
	struct page * page = NULL;

	spin_lock(&ctx->rev_flush_lock);

	j = atomic_read(&ctx->revmap_blk_count);
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
		mark_revmap_bit(ctx, ctx->revmap_pba);
	}
	page = ctx->revmap_page;
	ptr = (struct stl_revmap_entry_sector *)page_address(page);
	i = atomic_read(&ctx->revmap_sector_count);
	ptr->extents[i].lba = lba;
    	ptr->extents[i].pba = pba;
	ptr->extents[i].len = nrsectors;
	atomic_inc(&ctx->revmap_blk_count);
	atomic_inc(&ctx->revmap_sector_count);

	if (j  % NR_EXT_ENTRIES_PER_SEC == 0) {
		ptr->crc = calculate_crc(ctx, page);
		atomic_set(&ctx->revmap_sector_count, 0);
		if (NR_EXT_ENTRIES_PER_BLK == j) {
#ifdef BARRIER_NSTL
			/* We only need to wait here to minimize
			 * the loss of data, in case we end up
			 * sending translation entries for write
			 * sooner than they can be drained.
			 */
			wait_on_block_barrier(ctx);
#endif
			/* Flush the entries to the disk */
			pba = ptr->extents[NR_EXT_ENTRIES_PER_SEC - 1].pba;
			/* TODO: Alternatively, we could flag, that
			 * this is the last pba of the zone
			 * from the caller of this function
			 */
			flush_revmap_block_disk(ctx, page, pba);
			atomic_set(&ctx->revmap_blk_count, 0);
		} else {
			ptr = ptr + SECTOR_SIZE;
		}
	}
	spin_unlock(&ctx->rev_flush_lock);
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
	unsigned long flags;
	unsigned nr_sectors = bio_sectors(bio);
	sector_t s8, sector = bio->bi_iter.bi_sector;
	struct blk_plug plug;
	sector_t wf;

	if (is_disk_full(ctx)) {
		bio->bi_status = BLK_STS_NOSPC;
		bio_endio(bio);
		return -1;
	}

	//printk(KERN_INFO "\n ******* Inside map_write_io, requesting sectors: %d", sectors);
	nr_sectors = bio_sectors(bio);
	if (unlikely(nr_sectors <= 0)) {
		printk(KERN_ERR "\n Less than 0 sectors (%d) requested!, nbios: %u", nr_sectors, nbios);
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		return -1;
	}
	
	/* wait until there's room
	*/

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
	bioctx->orig = bio;
	bioctx->clone = clone;
	/* TODO: Initialize refcount in bioctx and increment it every
	 * time bio is split or padded */
	refcount_set(&bioctx->ref, 1);

	printk(KERN_ERR "\n write frontier: %llu free_sectors_in_wf: %llu", ctx->write_frontier, ctx->free_sectors_in_wf);

	blk_start_plug(&plug);
	
	do {

		subbio_ctx = kmem_cache_alloc(ctx->subbio_ctx_cache, GFP_KERNEL);
		if (!subbio_ctx) {
			printk(KERN_ERR "\n insufficient memory!");
			goto fail;
		}

		spin_lock_irqsave(&ctx->lock, flags);
		/*-------------------------------*/
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		wf = ctx->write_frontier;
		/* room_in_zone should be same as
		 * ctx->nr_free_sectors_in_wf
		 */
		if (s8 > ctx->free_sectors_in_wf){
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
		spin_unlock_irqrestore(&ctx->lock, flags);
    		

		/* Next we fetch the LBA that our DM got */
		sector = bio->bi_iter.bi_sector;
		refcount_inc(&bioctx->ref);
		subbio_ctx->extent.lba = sector;
		subbio_ctx->extent.pba = wf;
		subbio_ctx->bioctx = bioctx; /* This is common to all the subdivided bios */
		split->bi_private = subbio_ctx;
		split->bi_iter.bi_sector = wf; /* we use the save write frontier */
		split->bi_end_io = nstl_clone_endio;
		bio_set_dev(split, ctx->dev->bdev);
		generic_make_request(split);
		nr_sectors = bio_sectors(clone);
	} while (split != clone);

	/* When we did not split, we might need padding when the 
	 * original request is not block aligned.
	 * Note: we always split at block aligned. The remaining
	 * portion may not be block aligned.
	 *
	 * We want to zero fill the padded portion of the last page.
	 * We know that we always have a page corresponding to a bio.
	 * The page is always block aligned.
	 */
	if (nr_sectors < s8) {
		/* We need to zero out the remaining bytes
		 * from the last page in the bio
		 */
		pad = bio_clone_fast(bio, GFP_KERNEL, NULL);
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

struct stl_ckpt * read_checkpoint(struct ctx *ctx, unsigned long pba)
{
	struct block_device *bdev = ctx->dev->bdev;
	unsigned int blknr = pba / NR_SECTORS_IN_BLK;
	struct buffer_head *bh = __bread(bdev, blknr, BLOCK_SIZE);
	struct stl_ckpt *ckpt;
	if (!bh)
		return NULL;
       	ckpt = (struct stl_ckpt *)bh->b_data;
	printk(KERN_INFO "\n ** pba: %lu, ckpt->cur_frontier_pba: %lld", pba, ckpt->cur_frontier_pba);
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
	
	spin_lock_irq(&ctx->ckpt_lock);
	ctx->flag_ckpt = 1;
	/* We expect the ckpt_ref to go to atleast 4 and then back to 0
	 * when all the blocks are flushed!
	 */
	atomic_set(&ctx->ckpt_ref, 1);
	spin_unlock_irq(&ctx->ckpt_lock);
	blk_start_plug(&plug);
	flush_revmap_bitmap(ctx);
	update_checkpoint(ctx);
	flush_checkpoint(ctx);
	blk_finish_plug(&plug);
	flush_sit(ctx);
	/* We need to wait for all of this to be over before 
	 * we proceed
	 */
	wait_for_ckpt_completion(ctx);
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
	page1 = ctx->ckpt_page;
	/* ctx->ckpt_page will be overwritten by the next
	 * call to read_ckpt
	 */
	printk(KERN_INFO "\n !!Reading checkpoint 2 from pba: %u", sb->ckpt2_pba);
	ckpt2 = read_checkpoint(ctx, sb->ckpt2_pba);
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
	ctx->write_frontier = ckpt->cur_frontier_pba;
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

	if (bytenr < ctx->bitmap_bytes)
		panic("\n Trying to set an invalid bit in the free zone bitmap. bytenr > bitmap_bytes");

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
		/* If there are no more recorded entries on disk, then
		 * dont add an entries further
		 */
		if ((entry->lba == 0) && (entry->pba == 0)) {
			i++;
			continue;
			
		}
		/* TODO: right now everything should be zeroed out */
		panic("Why are there any already mapped extents?");
		stl_update_range(ctx, entry->lba, entry->pba, NR_SECTORS_IN_BLK);
		entry = entry + 1;
		i++;
	}
	return 0;
}


#define NR_SECTORS_PER_BLK 8

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
	int nr_extents_in_blk = BLOCK_SIZE / sizeof(struct tm_entry);
	unsigned long long pba = ctx->sb->tm_pba;
	unsigned long nrblks = ctx->sb->blk_count_tm;
	struct block_device *bdev = ctx->dev->bdev;
	int i = 0;
	
	ctx->n_extents = 0;
	while(i < nrblks) {
		bh = __bread(bdev, pba, BLOCK_SIZE);
		if (!bh)
			return -1;
		/* We read the extents in the entire block. the
		 * redundant extents should be unpopulated and so
		 * we should find 0 and break out */
		read_extents_from_block(ctx, (struct tm_entry *)bh->b_page, nr_extents_in_blk);
		i = i + 1;
		put_bh(bh);
		pba = pba + NR_SECTORS_IN_BLK;
	}
	return 0;
}

int read_revmap_bitmap(struct ctx *ctx)
{
	struct buffer_head *bh = NULL;
	unsigned long long pba = ctx->sb->revmap_bm_pba;
	unsigned long nrblks = ctx->sb->blk_count_revmap_bm;
	struct block_device *bdev = ctx->dev->bdev;
	int i = 0;
	struct page ** revmap_bm = ctx->revmap_bm;
	
	while(i < nrblks) {
		bh = __bread(bdev, pba, BLOCK_SIZE);
		if (!bh) {
			/* free the successful bh till now */
			return -1;
		}
		*revmap_bm  = bh->b_page;
		revmap_bm = revmap_bm + 1;
		i = i + 1;
		pba = pba + NR_SECTORS_IN_BLK;
	}
	return 0;

}


void process_revmap_entries_on_boot(struct ctx *ctx, struct page *page, refcount_t *ref)
{
	struct stl_revmap_extent *extent;
	struct stl_revmap_entry_sector *entry_sector;
	int i = 0, j;

	add_block_based_translation(ctx,  page, ref);
	entry_sector = (struct stl_revmap_entry_sector *) page;
	while (i < NR_SECTORS_IN_BLK) {
		extent = entry_sector->extents;
		for (j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			stl_update_range(ctx, extent[j].lba, extent[j].pba, extent[j].len);
		}
	}
}

/*
 * Read a blk only if the bitmap says its not available.
 */
int read_revmap(struct ctx *ctx)
{
	struct buffer_head *bh = NULL;
	struct page ** revmap_bm_pages = ctx->revmap_bm;
	unsigned int nrblks = ctx->sb->blk_count_revmap_bm;
	int i = 0, j, byte = 0;
	struct page *page;
	char *ptr;
	unsigned int blknr = 0;
	unsigned long pba;
	struct block_device *bdev = NULL;
	refcount_t ref;
	refcount_set(&ref, 2);

	while (i < nrblks) {
		page = *revmap_bm_pages;
		ptr = (char *) page_address(page);
		for (j = 0; j < BLOCK_SIZE; j++) {
			byte = *ptr;
			while(byte) {
				if (byte & 1) {
					pba = (blknr + j ) * NR_SECTORS_IN_BLK;
					bh = __bread(bdev, pba, BLOCK_SIZE);
					if (!bh) {
						/* free the successful bh till now */
						return -1;
					}
					process_revmap_entries_on_boot(ctx, bh->b_page, &ref);
				}
				byte = byte >> 1;
			}
			ptr = ptr + 1;
		}
		revmap_bm_pages += 1;
		blknr = blknr + BLOCK_SIZE;
	}
	spin_lock(&ctx->flush_lock);
	flush_translation_blocks(ctx);
	spin_unlock(&ctx->flush_lock);
	refcount_dec(&ref);
	wait_on_refcount(ctx, &ref);
	return 0;
}


sector_t get_zone_pba(struct stl_sb * sb, unsigned int segnr)
{
	return (segnr * (1 << (sb->log_zone_size - sb->log_sector_size)));
}


sector_t get_zone_end(struct stl_sb *sb, sector_t pba_start)
{
	return (pba_start + (1 << (sb->log_zone_size - sb->log_sector_size)));
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

/* TODO: create a freezone cache
 * create a gc zone cache
 * currently calling kzalloc
 */
int read_seg_entries_from_block(struct ctx *ctx, struct stl_seg_entry *entry, unsigned int nr_seg_entries, unsigned int *zonenr)
{
	int i = 0;
	struct stl_sb *sb;
	unsigned long flags;
	unsigned int nr_blks_in_zone;
       
	if (!ctx)
		return -1;

	sb = ctx->sb;

	nr_blks_in_zone = (1 << (sb->log_zone_size - sb->log_block_size));
	printk(KERN_ERR "\n Number of seg entries: %u", nr_seg_entries);

	while (i < nr_seg_entries) {
		if (entry->vblocks == 0) {
			printk(KERN_ERR "\n *segnr: %u", *zonenr);
			spin_lock_irqsave(&ctx->lock, flags);
			mark_zone_free(ctx , *zonenr);
			spin_unlock_irqrestore(&ctx->lock, flags);
		}
		else if (entry->vblocks < nr_blks_in_zone) {
			mark_zone_gc_candidate(ctx, *zonenr);
		}
		entry = entry + sizeof(struct stl_seg_entry);
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
	int nr_seg_entries_blk = BLOCK_SIZE / sizeof(struct stl_seg_entry);
	int ret=0;
	struct stl_seg_entry *entry0;
	unsigned long blknr;
	unsigned int zonenr = 0;
	struct stl_sb *sb;
	unsigned long nr_data_zones;
	unsigned long nr_seg_entries_read;
	
	/*
	if (NULL == ctx)
		return -1;
	*/
	bdev = ctx->dev->bdev;
	/*
	if (bdev) {
		panic("bdev is not set, is NULL!");
	}*/

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


	pba = sb->sit_pba;
	blknr = pba/NR_SECTORS_PER_BLK;
	nrblks = sb->blk_count_sit;
	printk(KERN_INFO "\n blknr of first SIT is: %lu", blknr);
	printk(KERN_ERR "\n zone_count_main: %u", sb->zone_count_main);
	while (zonenr < sb->zone_count_main) {
		printk(KERN_ERR "\n zonenr: %u", zonenr);
		if (blknr > ctx->sb->zone0_pba) {
			//panic("seg entry blknr cannot be bigger than the data blknr");
			printk(KERN_ERR "seg entry blknr cannot be bigger than the data blknr");
			break;
		}
		printk(KERN_INFO "\n blknr: %lu", blknr);
		bh = __bread(bdev, blknr, BLOCK_SIZE);
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
	return 0;
}

struct stl_sb * read_superblock(struct ctx *ctx, unsigned long pba)
{

	struct block_device *bdev = ctx->dev->bdev;
	unsigned int blknr = pba / NR_SECTORS_PER_BLK;
	struct buffer_head *bh;
	struct stl_sb * sb;
	/* TODO: for the future. Right now we keep
	 * the block size the same as the sector size
	 *
	*/
	if (set_blocksize(bdev, BLOCK_SIZE))
		return NULL;

	printk(KERN_INFO "\n sb found at pba: %lu", pba);

	bh = __bread(bdev, blknr, BLOCK_SIZE);
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
	struct stl_sb * sb1;
	unsigned long pba = 0;
	struct stl_ckpt *ckpt;

	pba = 0;
	sb1 = read_superblock(ctx, pba);
	if (NULL == sb1) {
		printk(KERN_ERR "\n read_superblock failed! cannot read the metadata ");
		return -1;
	}

	printk(KERN_INFO "\n superblock read!");
	/*
	 * we need to verify that sb1 is uptodate.
	 * Right now we do nothing. we assume
	 * sb1 is okay.
	page = ctx->sb_page;
	pba = pba + SECTORS_PER_BLK;
	sb2 = read_superblock(bdev, pba);
	put_page(page);
	 */
	ctx->sb = sb1;

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

	read_revmap_bitmap(ctx);
	read_revmap(ctx);
	printk(KERN_INFO "Reverse map flushed!");

	ret = read_translation_map(ctx);
	if (0 > ret) {
		put_page(ctx->sb_page);
		put_page(ctx->ckpt_page);
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
	read_seg_info_table(ctx);
	printk(KERN_INFO "\n read segment entries, free bitmap created!");
	if (ctx->nr_freezones != ckpt->nr_free_zones) { 
		/* TODO: Do some recovery here.
		 * For now we panic!!
		 * SIT is flushed before CP. So CP could be stale.
		 * Update checkpoint accordingly and record!
		 */
		//panic("free zones in SIT and checkpoint does not match!");
	}
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
	kmem_cache_destroy(ctx->subbio_ctx_cache);
	kmem_cache_destroy(ctx->tm_page_cache);
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
	ctx->subbio_ctx_cache = kmem_cache_create("subbio_ctx_cache", sizeof(struct nstl_sub_bioctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->subbio_ctx_cache) {
		goto destroy_tm_page_cache;
	}
	ctx->read_ctx_cache = kmem_cache_create("read_ctx_cache", sizeof(struct read_ctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->read_ctx_cache) {
		goto destroy_subbio_ctx_cache;
	}
	ctx->subbio_ctx_cache = kmem_cache_create("subbioctx_cache", sizeof(struct nstl_sub_bioctx), 0, SLAB_ACCOUNT, NULL);
	if (!ctx->subbio_ctx_cache) {
		goto destroy_read_ctx_cache;
	}
	return 0;
/* failed case */
destroy_read_ctx_cache:
	kmem_cache_destroy(ctx->read_ctx_cache);
destroy_subbio_ctx_cache:
	kmem_cache_destroy(ctx->subbio_ctx_cache);
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

	printk(KERN_INFO "\n argv[0]: %s, argv[1]: %s", argv[0], argv[1]);

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


	printk(KERN_INFO "\n sc->dev->bdev->bd_part->start_sect: %llu", ctx->dev->bdev->bd_part->start_sect);
	printk(KERN_INFO "\n sc->dev->bdev->bd_part->nr_sects: %llu", ctx->dev->bdev->bd_part->nr_sects);

	ctx->extent_pool = mempool_create_slab_pool(MIN_EXTENTS, extents_slab);
	if (!ctx->extent_pool)
		goto put_dev;

	ret = read_metadata(ctx);
	if (ret < 0)
		goto clean_ctx;

	max_pba = ctx->dev->bdev->bd_inode->i_size / 512;
	sprintf(ctx->nodename, "stl/%s", argv[1]);
	ret = -EINVAL;
	ctx->nr_lbas_in_zone = (1 << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	printk(KERN_INFO "\n device records max_pba: %llu", max_pba);
	ctx->max_pba = ctx->sb->max_pba;
	printk(KERN_INFO "\n formatted max_pba: %d", ctx->max_pba);
	if (ctx->max_pba > max_pba) {
		dm_target->error = "dm-stl: Invalid max pba found on sb";
		goto clean_ctx;
	}
	ctx->write_frontier = ctx->ckpt->cur_frontier_pba;

	printk(KERN_INFO "%s %d kernel wf: %llu\n", __func__, __LINE__, ctx->write_frontier);
	ctx->wf_end = zone_end(ctx, ctx->write_frontier);
	printk(KERN_INFO "%s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->wf_end);
	printk(KERN_INFO "max_pba = %d", ctx->max_pba);
	ctx->free_sectors_in_wf = ctx->wf_end - ctx->write_frontier + 1;


	disk_size = i_size_read(ctx->dev->bdev->bd_inode);
	printk(KERN_INFO "\n The disk size as read from the bd_inode: %llu", disk_size);
	printk(KERN_INFO "\n max sectors: %llu", disk_size/512);
	printk(KERN_INFO "\n max blks: %llu", disk_size/4096);
	spin_lock_init(&ctx->lock);
	spin_lock_init(&ctx->sit_kv_store_lock);
	spin_lock_init(&ctx->tm_ref_lock);

	atomic_set(&ctx->io_count, 0);
	atomic_set(&ctx->n_reads, 0);
	atomic_set(&ctx->pages_alloced, 0);
	atomic_set(&ctx->nr_writes, 0);
	atomic_set(&ctx->nr_failed_writes, 0);
	atomic_set(&ctx->revmap_blk_count, 0);
	atomic_set(&ctx->revmap_sector_count, 0);
	atomic_set(&ctx->zone_revmap_count, 0);
	ctx->target = 0;

	ret = -ENOMEM;
	dm_target->error = "dm-stl: No memory";

	ctx->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
	if (!ctx->page_pool)
		goto clean_ctx;

	//printk(KERN_INFO "about to call bioset_init()");
	ctx->bs = kzalloc(sizeof(*(ctx->bs)), GFP_KERNEL);
	if (!ctx->bs)
		goto destroy_page_pool;

	if(bioset_init(ctx->bs, BS_NR_POOL_PAGES, 0, BIOSET_NEED_BVECS|BIOSET_NEED_RESCUER) == -ENOMEM) {
		printk(KERN_ERR "\n bioset_init failed!");
		goto free_bioset;
	}

	ctx->extent_tbl_root = RB_ROOT;
	rwlock_init(&ctx->extent_tbl_lock);

	ctx->tm_rb_root = RB_ROOT;

	ctx->sit_rb_root = RB_ROOT;
	rwlock_init(&ctx->sit_rb_lock);

	ctx->sectors_copied = 0;

	ret = stl_gc_thread_start(ctx);
	if (ret) {
		goto uninit_bioset;
	}

	ret = create_caches(ctx);
	if (0 > ret) {
		goto stop_gc_thread;
	}
	ctx->revmap_bm = kmalloc(sizeof(struct page *) * ctx->sb->blk_count_revmap_bm, GFP_KERNEL);
	if (!ctx->revmap_bm)
		goto destroy_cache;

	/*
	if (register_shrinker(nstl_shrinker))
		goto free_revmap_bm;
	*/
	
	ctx->revmap_pba = ctx->sb->revmap_pba;
	ctx->ckpt->clean = 0;
	spin_lock_init(&ctx->flush_lock);
	init_waitqueue_head(&ctx->tm_blk_flushq);
	spin_lock_init(&ctx->tm_flush_lock);
	init_waitqueue_head(&ctx->zone_entry_flushq);
	spin_lock_init(&ctx->flush_zone_lock);
	atomic_set(&ctx->nr_pending_writes, 0);
	atomic_set(&ctx->nr_sit_pages, 0);
	atomic_set(&ctx->nr_tm_pages, 0);
	init_waitqueue_head(&ctx->refq);
	init_waitqueue_head(&ctx->rev_blk_flushq);
	ctx->mounted_time = ktime_get_real_seconds();
	ctx->s_chksum_driver = crypto_alloc_shash("crc32c", 0, 0);
	if (IS_ERR(ctx->s_chksum_driver)) {
		printk(KERN_ERR "Cannot load crc32c driver.");
		ret = PTR_ERR(ctx->s_chksum_driver);
		ctx->s_chksum_driver = NULL;
		goto destroy_cache;
	}
	return 0;
/* failed case */
destroy_cache:
	destroy_caches(ctx);
stop_gc_thread:
	stl_gc_thread_stop(ctx);
uninit_bioset:
	bioset_exit(ctx->bs);
free_bioset:
	kfree(ctx->bs);
destroy_page_pool:
	if (ctx->page_pool)
		mempool_destroy(ctx->page_pool);
clean_ctx:
	if (ctx->sb_page)
		put_page(ctx->sb_page);
	if (ctx->ckpt_page)
		put_page(ctx->ckpt_page);
	/* TODO : free extent page
	 * and segentries page */
		
	if (ctx->extent_pool)
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
	dm_target->private = NULL;

	flush_revmap_entries(ctx);
	/* At this point we are sure that the revmap
	 * entries have made it to the disk
	 */
	flush_translation_blocks(ctx);
	do_checkpoint(ctx);
	/* If we are here, then there was no crash while writing out
	 * the disk metadata
	 */

	/* TODO : free extent page
	 * and segentries page */
	kfree(ctx->revmap_bm);		
	destroy_caches(ctx);
	stl_gc_thread_stop(ctx);
	bioset_exit(ctx->bs);
	kfree(ctx->bs);
	mempool_destroy(ctx->page_pool);
	put_page(ctx->sb_page);
	put_page(ctx->ckpt_page);
	mempool_destroy(ctx->extent_pool);
	dm_put_device(dm_target, ctx->dev);
	kfree(ctx);
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
		case REQ_OP_ZONE_RESET:
			ret = nstl_zone_reset(ctx, bio);
			break;
		case REQ_OP_ZONE_RESET_ALL:
			ret = nstl_zone_reset_all(ctx, bio);
			break;
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
