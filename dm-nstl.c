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

#include "nstl-u.h"
#include "stl-wait.h"
#include "metadata.h"

#define DM_MSG_PREFIX "stl"


/* TODO:
 * 1) Convert the STL in a block mapped STL rather
 * than a sector mapped STL
 * If needed you can migrate to a track mapped
 * STL
 * 2) write checkpointing - flush the translation table
 * to the checkpoint area
 * 3) GC
 */

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
/* an extent is busy if it's being GCed
*/
static int extent_busy(struct extent *e)
{
	return atomic_read(&e->refs[IN_CLEANING]) > 0;
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

	while (node) {
		struct extent *e = container_of(node, struct extent, rb);
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

static struct extent *stl_rb_geq(struct ctx *sc, off_t lba)
{
	struct extent *e = NULL;
	unsigned long flags;

	read_lock_irqsave(&sc->rb_lock, flags); 
	e = _stl_rb_geq(&sc->rb, lba);
	read_unlock_irqrestore(&sc->rb_lock, flags); 

	return e;
}


int _stl_verbose;
static void stl_rb_insert(struct ctx *sc, struct extent *new)
{
	struct rb_root *root = &sc->rb;
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
	sc->n_extents++;
}


static void stl_rb_remove(struct ctx *sc, struct extent *e)
{
	struct rb_root *root = &sc->rb;
	rb_erase(&e->rb, root);
	sc->n_extents--;
}


#if 0
static struct extent *stl_rb_first(struct ctx *sc)
{
	struct rb_root *root = &sc->rb;
	struct rb_node *node = rb_first(root);
	return container_of(node, struct extent, rb);
}
#endif

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
}




/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to map.
 * if blocked by cleaning, returns the extent which we're blocked on.
 */
static struct extent *stl_update_range(struct ctx *sc, sector_t lba, sector_t pba, size_t len)
{
	struct extent *e = NULL, *_new = NULL, *_new2 = NULL;
	unsigned long flags;

	BUG_ON(len == 0);

	if (unlikely(!(_new = mempool_alloc(sc->extent_pool, GFP_NOIO))))
		return NULL;
	_new2 = mempool_alloc(sc->extent_pool, GFP_NOIO);

	write_lock_irqsave(&sc->rb_lock, flags);
	e = _stl_rb_geq(&sc->rb, lba);

	if (e != NULL) {
		/* [----------------------]        e     new     new2
		 *        [++++++]           -> [-----][+++++][--------]
		 */
		if (e->lba < lba && e->lba+e->len > lba+len) {
			off_t new_lba = lba+len;
			size_t new_len = e->lba + e->len - new_lba;
			off_t new_pba = e->pba + (e->len - new_len);

			if (extent_busy(e))
				goto blocked;
			if (_new2 == NULL)
				goto fail;
			extent_init(_new2, lba+len, new_pba, new_len);
			extent_get(_new2, 1, IN_MAP);
			e->len = lba - e->lba; /* do this *before* inserting below */
			stl_rb_insert(sc, _new2);
			e = _new2;
			_new2 = NULL;
		}
		/* [------------]
		 *        [+++++++++]        -> [------][+++++++++]
		 */
		else if (e->lba < lba) {
			if (extent_busy(e))
				goto blocked;
			e->len = lba - e->lba;
			if (e->len == 0) {
				DMERR("zero-length extent");
				goto fail;
			}
			e = stl_rb_next(e);
		}
		/*          [------]
		 *   [+++++++++++++++]        -> [+++++++++++++++]
		 */
		while (e != NULL && e->lba+e->len <= lba+len) {
			struct extent *tmp = stl_rb_next(e);
			if (extent_busy(e))
				goto blocked;
			stl_rb_remove(sc, e);
			extent_put(sc, e, 1, IN_MAP);
			e = tmp;
		}
		/*          [------]
		 *   [+++++++++]        -> [++++++++++][---]
		 */
		if (e != NULL && lba+len > e->lba) {
			int n = (lba+len) - e->lba;
			if (extent_busy(e))
				goto blocked;
			e->lba += n;
			e->pba += n;
			e->len -= n;
		}
	}

	/* TRIM indicated by pba = -1 */
	if (pba != -1) {
		extent_init(_new, lba, pba, len);
		extent_get(_new, 1, IN_MAP);
		stl_rb_insert(sc, _new);
	}
	write_unlock_irqrestore(&sc->rb_lock, flags);
	if (_new2 != NULL)
		mempool_free(_new2, sc->extent_pool);

	return NULL;

fail:
	write_unlock_irqrestore(&sc->rb_lock, flags);
	DMERR("could not allocate extent");
	if (_new)
		mempool_free(_new, sc->extent_pool);
	if (_new2)
		mempool_free(_new2, sc->extent_pool);

	return NULL;

blocked:
	write_unlock_irqrestore(&sc->rb_lock, flags);
	if (_new)
		mempool_free(_new, sc->extent_pool);
	if (_new2)
		mempool_free(_new2, sc->extent_pool);
	return e;
}

/************** Received I/O handling *****************/

static void split_read_io(struct ctx *sc, struct bio *bio, int not_used)
{
	struct bio *split = NULL;

	if (!bio) {
		dump_stack();
		return;
	}

	bio_end_io_t *fn_end_io = bio->bi_end_io;

	printk(KERN_ERR "Read begins! ");

	atomic_inc(&sc->n_reads);
	while(split != bio) {
		if(unlikely(&bio->bi_end_io < 0x1000) || (bio->bi_end_io != fn_end_io)) {
			dump_stack();
		}

		sector_t sector = bio->bi_iter.bi_sector;
		struct extent *e = stl_rb_geq(sc, sector);
		unsigned sectors = bio_sectors(bio);

		/* note that beginning of extent is >= start of bio */
		/* [----bio-----] [eeeeeee]  */
		if (e == NULL || e->lba >= sector + sectors)  {
			printk(KERN_ERR "\n Case of no overlap");
			zero_fill_bio(bio);
			bio_endio(bio);
			break;
		}
		/* [eeeeeeeeeeee] eeeeeeeeeeeee]<- could be shorter or longer
		   [---------bio------] */
		else if (e->lba <= sector) {
			printk(KERN_ERR "\n e->lba <= sector");
			unsigned overlap = e->lba + e->len - sector;
			if (overlap < sectors)
				sectors = overlap;
			sector = e->pba + sector - e->lba;
		}
		/*             [eeeeeeeeeeee]
			       [---------bio------] */
		else {
			printk(KERN_ERR "\n e->lba >  sector");
			sectors = e->lba - sector;
			split = bio_split(bio, sectors, GFP_NOIO, &fs_bio_set);
			bio_chain(split, bio);
			zero_fill_bio(split);
			bio_endio(split);
			continue;
		}

		if (sectors < bio_sectors(bio)) {
			split = bio_split(bio, sectors, GFP_NOIO, &fs_bio_set);
			bio_chain(split, bio);
		} else {
			split = bio;
		}

		split->bi_iter.bi_sector = sector;
		bio_set_dev(bio, sc->dev->bdev);
		//printk(KERN_INFO "\n read,  sc->n_reads: %d", sc->n_reads);
		generic_make_request(split);
	}
	printk(KERN_ERR "\n Read end");
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
	struct nstl_sub_bioctx *subbioctx = clone->private;
	struct nstl_bioctx *bioctx;
	struct bio *bio = bioctx->orig;
	struct ctx * ctx = bioctx->ctx;

	if (private->magic != SUBBIOCTX_MAGIC) {
		/* private has been overwritten */
		return;
	}
	/* If a single segment of the bio fails, the bio should be
	 * recorded with this status. No translation entry
	 * for the entire original bio should be maintained
	 */
	if (clone->bi_status != BLK_STATS_OK && bio->bi_status == BLK_STATS_OK) {
		bio->bi_status = clone->bi_satus;
		/* we don't keep partial bio translation map entries.
		 * The write either succeeds atomically or does not at
		 * all
		 */
		reduce_nr_valid_blocks(ctx, bio);
		/* If this is a SMR zone, then a sector write failure
		 * causes all further I/O in that zone to fail.
		 * Hence we can no longer write to this zone
		 */
		if(nstl_zone_seq())
			mark_zone_erroneous();
		/* If this zone is sequential, then we do not have
		 * to do anything. There will be a gap in this
		 * segment. But thats about it.
		 */
	}

	/* We add the translation entry only when we know
	 * that the write has made it to the disk
	 */
	if(clone->bi_status == BLK_STATS_OK) {
		e = stl_update_range(ctx, subbioctx->extent.lba, subbioctx->extent.pba, subbioctx->extent.len);
		add_revmap_entries(ctx, subbioctx->extent.pba, subbioctx->extent.lba, subbioctx->extent.len);
	}
	/* When the refcount becomes 0, we need to call the bio_endio
	 * of the original bio
	 */
	if (refcount_dec_and_test(&bioctx->ref)) {
		bio_endio(bio);
	}

	bio_put(clone);
}

/* could be a macro, I guess */
static void setup_bio(struct bio *bio, bio_end_io_t endio, struct block_device *bdev,
		sector_t sector, void *private, int dir)
{
	bio->bi_end_io = endio;
	bio_set_dev(bio, bdev);
	bio->bi_iter.bi_sector = sector;
	bio->bi_private = private;
	bio->bi_opf = dir;
}

/* note that ppage returns a pointer to the last page in the bio -
 * i.e. the only page if it's a 1-page bio for header/trailer
 */
static struct bio *stl_alloc_bio(struct ctx *sc, unsigned sectors, struct page **ppage)
{
	int i, val, remainder, npages;
	struct bio_vec *bv = NULL;
	struct bio *bio;
	struct page *page;
	struct bvec_iter_all iter_all;

	npages = sectors / 8;
	remainder = (sectors * 512) - npages * PAGE_SIZE;
	printk(KERN_ERR "\n npages: %d, remainder: %d sc->bs: %x", npages, remainder, sc->bs);
	printk(KERN_ERR "\n npages + (remainder > 0): %d", npages + (remainder > 0));


	if (!(bio = bio_alloc_bioset(GFP_NOIO, npages + (remainder > 0), sc->bs))) {
		printk(KERN_ERR "bio_alloc_bioset failed! ");
		goto fail0;
	}

	printk(KERN_ERR "\n bio_alloc_bioset is successful. ");
	for (i = 0; i < npages; i++) {
		if (!(page = mempool_alloc(sc->page_pool, GFP_NOIO)))
			goto fail;
		atomic_inc(&sc->pages_alloced);
		val = bio_add_page(bio, page, PAGE_SIZE, 0);
		if (ppage != NULL)
			*ppage = page;
	}
	printk(KERN_ERR "\n mempool alloc. to npages successful. will try one more page allocation");
	if (remainder > 0) {
		if (!(page = mempool_alloc(sc->page_pool, GFP_NOIO)))
			goto fail;
		atomic_inc(&sc->pages_alloced);
		if (ppage != NULL)
			*ppage = page;
		val = bio_add_page(bio, page, remainder, 0);
	}
	return bio;

fail:
	printk(KERN_INFO "stl_alloc_bio: FAIL (%d pages + %d) %d\n", npages, sectors%8,
			atomic_read(&sc->pages_alloced));
	WARN_ON(1);
	if (bio != NULL) {
		bio_for_each_segment_all(bv, bio, iter_all) {
			mempool_free(bv->bv_page, sc->page_pool);
			atomic_dec(&sc->pages_alloced);
		}
	}
fail0:
	return NULL;
}

/* 
 * 1 indicates that the zone is free 
 *
 * Zone numbers start from 0
 */
static void mark_zone_free(struct ctx *ctx , int zonenr)
{

	if (unlikely(NULL == ctx)) {
		panic("This is a ctx bug");
	}
		
	char *bitmap = ctx->freezone_bitmap;
	int nr_freezones = ctx->nr_freezones;
	int bytenr = zonenr / BITS_IN_BYTE;
	int bitnr = zonenr % BITS_IN_BYTE;

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
	int nr_gc_zones = ctx->nr_gc_zones;
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
	ctx->nr_gc_zones = ctx->nr_gc_zones + 1;
}

static u64 get_next_freezone_nr(struct ctx *ctx)
{
	char *bitmap = ctx->freezone_bitmap;
	int nr_freezones = ctx->nr_freezones;
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



void update_elapsed_time(struct ctx *ctx, struct stl_seg_entry * cur_entry)
{
	time64_t elapsed_time, now = ktime_get_real_seconds();

	elapsed_time = now - ctx->mounted_time;
	cur_entry->mtime = elapsed_time;
}

/* moves the write frontier, returns the LBA of the packet trailer
*/
static int get_new_zone(struct ctx *ctx)
{
	unsigned long zone_nr;
	sector_t old_free = ctx->free_sectors_in_wf;


	atomic_inc(&ctx->zone_revmap_count);
	wait_on_zone_barrier(ctx);

	if (ctx->write_frontier > ctx->wf_end)
		printk(KERN_INFO "kernel wf before BUG: %ld - %ld\n", ctx->write_frontier, ctx->wf_end);
	BUG_ON(ctx->write_frontier > ctx->wf_end);
	zone_nr = get_next_freezone_nr(ctx);
	if (zone_nr < 0) {
		printk(KERN_WARNING "\n Disk is full, no more writing possible! ");
		ctx->write_frontier = -1;
		return (zone_nr);
	}

	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->write_frontier = zone_start(ctx, zone_nr) + ctx->sb->zone0_pba;
	ctx->wf_end = zone_end(ctx, ctx->write_frontier);
	ctx->free_sectors_in_wf = ctx->wf_end - ctx->write_frontier + 1;
	ctx->nr_freezones--;
	printk(KERN_INFO "Num of free sect.: %ld, diff of end and wf:%ld\n", ctx->free_sectors_in_wf, ctx->wf_end - ctx->write_frontier);
	/*printk(KERN_INFO "new zone: %d (%ld->%ld) left %d\n", (int)(fz->start / sc->nr_lbas_in_zone),
			old_free, sc->free_sectors_in_wf, sc->n_free_zones);
	*/

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
	struct stl_ckpt_entry * table = &ckpt->ckpt_translation_table;
	table->prev_zone_pba = table->cur_zone_pba;
	table->prev_count = table->cur_count;
	table->cur_zone_pba = wf;
	table->cur_count = 0;
}

static void do_checkpoint(struct ctx *ctx)
{
	struct stl_ckpt *ckpt = ctx->ckpt;
	
	ctx->flag_ckpt = 0;
}

/* TODO: We need to do this under a lock for concurrent writes
 */
static void move_write_frontier(struct ctx *ctx, sector_t sectors_s8)
{
	static int nrwrites = 0;
	sector_t wf = ctx->write_frontier;

	/* We should have adjusted sectors_s8 to accomodate
	 * for the rooms in the zone before calling this function.
	 * Its how we split the bio
	 */
	if (ctx->free_sectors_in_wf < sectors_s8) {
		panic("Wrong manipulation of wf; used unavailable sectors in a log");
	}
	
	ctx->write_frontier = ctx->write_frontier + sectors_s8;
	ctx->free_sectors_in_wf = ctx->free_sectors_in_wf - sectors_s8;
	if (ctx->flag_ckpt == 0)
		update_elapsed_time(ctx, &ctx->ckpt->prev_seg_entry);
	else
		update_elapsed_time(ctx, &ctx->ckpt->cur_seg_entry);
	if (ctx->free_sectors_in_wf < NR_SECTORS_IN_BLK) {
		if (ctx->flag_ckpt == 1) {
			/* prev_seg_entry and cur_seg_entry are both
			 * full. Its time to flush the checkpoint
			 */
			//do_checkpoint();

		}
		get_new_zone(ctx);
		add_ckpt_new_wf(ctx, wf);
		if (ctx->write_frontier < 0) {
			printk(KERN_INFO "No more disk space available for writing!");
			return -1;
		}
	}
	if (ctx->write_frontier > ctx->wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %d", ctx->free_sectors_in_wf );
	}
}

/* 
 * We should be ideally be using to make sure that
 * one block worth of translation entries have made it to the 
 * disk. Not using this, will work in most case.
 * But no guarantees can be given
 */
void wait_on_block_barrier(struct ctx * ctx);
{
	spin_lock_irq(&ctx->flush_lock);
	wait_event_lock_irq(&ctx->blk_of_ent_flushq, !atomic_read(ctx->pending_writes), &ctx->flush_lock);
	spin_unlock_irq(&ctx->flush_lock);
}

void wait_on_zone_barrier(struct ctx * ctx)
{
	spin_lock_irq(&ctx->flush_lock);
	/* wait until atleast one zone's revmap entries are flushed */
	wait_event_lock_irq(&ctx->zone_entry_flushq, (MAX_ZONE_REVMAP >= atomic_read(ctx->zone_revmap_count), &ctx->flush_lock);
	spin_unlock_irq(&ctx->flush_lock);
	checkpoint();
}


/*
 * If this length cannot be accomodated in this page
 * search and add another page for this next
 * lba. Remember this translation table will
 * go on the disk and is block based and not 
 * extent based
 *
 * Depending on the location within a page, add the lba.
 */
int add_translation_entry(struct page *page, unsigned long lba, unsigned long pba, unsigned long len)
{
	struct translation_map_entry * ptr;
	int i;

	ptr = (translation_map_entry *) page_address(page);
	entry_nr = get_entry_nr(lba);
	ptr = ptr + entry_nr;

	/* Assuming len is in terms of sectors 
	 * We convert sectors to blocks
	 */
	for (i=0; i<len/8; i++) {
		ptr->lba = lba;
		ptr->pba = pba;
		lba = lba + BLK_SIZE;
		pba = pba + BLK_SIZE;
		ptr = ptr + 1;
		entry_nr = entry_nr + 1;
		if (entry_nr == ENTRIES_IN_BLK) {
			trans_page = search_kv_store(lba);
			if (!trans_page) {
				trans_page = add_entry_kv_store(lba);
			}
			ptr = (translation_map_entry *) page_address(page);
			entry_nr = 0;
		}
	}	

}

void read_transbl_complete(struct bio * bio)
{
	struct closure *cl = bio->private;

	closure_put(&cl);
}

struct page * read_translation_block(u64 pba)
{
	struct bio * bio;
	struct closure *cl;
	struct page *page;


	page = get_zeroed_page(GFP_KERNEL);
	if (!page)
		return NULL;

	closure_init_stack(&cl);
	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		return NULL;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return NULL;
	}
	bio->bi_end_io = read_transbl_complete();
	bio->bi_private = &cl;
	bio_set_op_attrs(&bio, REQ_OP_READ, 0);
	bio->bi_sector = pba;
	bio_submit(bio);
	closure_sync(&cl);
	if (bio->bi_status != BLK_STS_OK) {
		printk(KERN_DEBUG "\n Could not read the translation entry block");
		bio_free_pages();
		return NULL;
	}
	/* bio_alloc() hence bio_put() */
	bio_put();
	return page;
}

/*
 * If a match is found, then returns the matching
 * entry 
 */
struct trans_entry_mem * search_kv_store(struct ctx *ctx, u64 blknr, struct trans_entry_mem **parent)
{
	struct rb_root *rb_root = ctx->trans_rb_root;
	struct rb_node *node = NULL;

	struct trans_entry_mem *node_ent;

	node = root->rb_node;
	while(node) {
		*parent = node;
		node_ent = container_of(*parent, struct trans_ent_mem, rb);
		if (blknr == node_ent->blknr) {
			return node_ent;
		}
		if (blknr < node_ent->blknr) {
			node = node->left;
		} else {
			node = node->right;
		}
	}
	return NULL;
}


struct page *remove_entry_kv_store(struct ctx *ctx, u64 lba)
{
	u64 start_lba = lba - lba % TRANS_ENT_PER_BLK;
	u64 blknr = start_lba / TRANS_ENT_PER_BLK;
	struct rb_node *parent = NULL;
	struct rb_root *rb_root = ctx->trans_rb_root;

	struct trans_entry_mem *new;

	new = search_kv_store(blknr, &parent);
	if (new)
		return new;

	rb_erase(&new->rb, rb_root);
	atomic_dec(ctx->trans_pages);
}

void write_transbl_complete(struct bio *bio)
{
	struct ctx *ctx;
	struct trans_page_write_ctx *trans_page_write_ctx;

	trans_page_write_ctx = (struct trans_page_write_ctx *) bio->private;
	ctx = trans_page_write_ctx->ctx;
	cl = trans_page_write_ctx->cl;

	if (bio->bi_status != BLK_STS_OK) {
		/*TODO: Perhaps retry!! or do something more
		 * or else you will loose the translation entries.
		 * write them some place else!
		 */
		printk(KERN_DEBUG "\n Could not read the translation entry block");
		return NULL;
	}
	closure_put(cl);	
	bio_free_page(bio);
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);
	atomic_dec(ctx->trans_pages);
}

/* We don't wait for the bios to complete 
 * flushing. We only initiate the flushing
 */
void flush_node_page(struct rb_node *node, struct ctx *ctx)
{
	struct page *page; 
	struct trans_entry_mem *node_ent;
	u64 pba;
	struct bio * bio;
	struct trans_page_write_ctx *trans_page_write_ctx;

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		return NULL;
	}
	


	trans_page_write_ctx = kmem_cache_alloc(ctx->trans_page_write_cache, GFP_KERNEL);
	if (!trans_page_write_ctx) {
		bio_put(bio);
		return NULL;
	}

	node_ent = container_of(*parent, struct trans_ent_mem, rb);
	page = node_ent->page;
	pba = node_ent->blkrnr + ctx->sb->tm_map;
	page = node_ent->page;
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return NULL;
	}
	bio->bi_end_io = write_transbl_complete();
	bio_set_op_attrs(&bio, REQ_OP_WRITE, 0);
	bio->bi_sector = pba;
	trans_page_write_ctx->cl = node_ent->cl;
       	trans_page_write_ctx->ctx = ctx;	
	bio->private = trans_page_write_ctx;
	bio_submit(bio);
	return page;


}


void flush_nodes(struct rb_node *node, struct ctx *ctx)
{	
	flush_node_page(node, ctx);
	if (node->left)
		flush_nodes(node->left, ctx);
	if (node->right)
		flush_node(node->right, ctx);
}

void flush_translation_blocks(ctx *ctx)
{
	struct rb_root *rb_root = ctx->trans_rb_root;
	struct rb_node *node = NULL;
	struct blk_plug plug;

	struct trans_entry_mem *node_ent;
	blknr = blknr + ctx->sb->tm_pba;


	node = root->rb_node;
	if (!node)
		return;

	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	blk_start_plug(&plug);
	flush_nodes(node, ctx);
	blk_finish_plug(&plug);	
}
/*
 * TODO: Use locks while adding entries to the KV store to protect
 * against concurrent access
 */
struct page *add_entry_kv_store(struct ctx *ctx, u64 lba, struct closure *cl)
{
	u64 start_lba = lba - lba % TRANS_ENT_PER_BLK;
	u64 blknr = start_lba / TRANS_ENT_PER_BLK;
	struct rb_node *parent = NULL;

	struct trans_entry_mem *new;

	new = search_kv_store(ctx, blknr, &parent);
	if (new)
		return new->page;

	new = kmem_cache_alloc(ctx->trans_mem_ent_cache, GFP_KERNEL);
	if (!new)
		return NULL;

	RB_CLEAR_NODE(&new->rb);

	page = read_translation_block(blknr);
	if (!page) {
		kmem_cache_free(ctx->trans_mem_ent_cache, new);
		return NULL;
	}

	/* Add this page to a RB tree based KV store.
	 * Key is: blknr for this corresponding block
	 */
	if (blknr < parent->blknr) {
		/* Attach new node to the left of parent */
		rb_link_node(&new->rb, parent, &parent->left);
	}
	else { 
		/* Attach new node to the right of parent */
		rb_link_node(&new->rb, parent, &parent->right);
	}
	new->blknr = blknr;
	new->page = page;
	new->cl = cl;
	closure_get(&cl);
	rb_insert_color(&new->rb, root);
	atomic_inc(ctx->trans_pages);
	if (atomic_read(ctx->trans_pages) >= MAX_TRANS_PAGES) {
		if (spin_trylock(&ctx->flush_lock)) {
			/* Only one flush operation at a time 
			 * but we dont want to wait.
			 */
			flush_translation_blocks(ctx);
			spin_unlock(&ctx->flush_lock);
		}
	}

	return page;
}

/* Make the length in terms of sectors or blocks?
 */
int add_block_based_translation(struct ctx, struct page *page, u64 pba, struct closure *cl)
{
	
	struct stl_revmap_entry_sector * ptr;
	struct page * trans_page = NULL;

	ptr = (struct stl_revmap_entry_sector *)page_address(page);
	
	nr_entries = PAGE_SIZE/sizeof(struct stl_revmap_entry_sector);
	i = 0;
	while (i < nr_entries;
		trans_page = add_entry_kv_store(ctx, ptr->lba, cl);
		if (!trans_page)
			return -ENOMEM;
		add_translation_entry(trans_page, ptr->lba, ptr->pba, ptr->len);
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

void read_revmap_bitmap(struct ctx *ctx)
{
}

void flush_revmap_bitmap(struct ctx *ctx)
{
}

void mark_revmap_bit(struct ctx *ctx, u64 pba)
{

}


void clear_revmap_bit(struct ctx *ctx, u64 pba)
{
	char *ptr = ctx->revmap_bitmap;

}


/*
 * TODO: Error handling metadata flushing
 *
 */
void revmap_entries_flushed(struct bio *bio)
{
	struct ctx * ctx = bio->private;
	struct revmap_meta_inmem *revmap_bio_ctx;
	sector_t pba;
	struct page *page;

	switch(bio->bi_status) {
		case BLK_STS_OK:
			atomic_dec(&ctx->pending_writes);
			//wake_up(&ctx->flushq_head);
			revmap_bio_ctx = bio->bi_private;
			if (revmap_bio_ctx->priv_magic != REVMAP_PRIV_MAGIC) {
				printk(KERN_ERR "\n bio->bi_private modified!!");
				panic();
				return;
			}
			pba = revmap_bio_ctx->pba;
			if (pba == zone_end(ctx, pba)) {
				atomic_dec(&ctx->zone_revmap_count);
			}
			wake_up(&ctx->zone_entry_flushq);
			add_block_based_translation(ctx, revmap_bio_ctx->page, revmap_bio_ctx->revmap_pba, revmap_bio_ctx->cl);
			/* closure will be synced when all the
			 * translation entries are flushed
			 * to the disk. We free the page before
			 * since the entries in that page are already
			 * copied.
			 */
			bio_free_pages(bio);
			closure_sync(&revmap_bio_ctx->cl);
			/* now we calculate the pba where the revmap
			 * is flushed. We can reuse that pba as all
			 * entries related to it are on disk in the
			 * right place.
			 */
			pba = bio->bi_sector.
			mark_revmap_bitmap(ctx, pba);
			break;
		case BLK_STS_AGAIN:
			/* retry a limited number of times.
			 * Maintain a retrial count in
			 * bio->bi_private
			 */
			break;
		default:
			break;
	}
}


/*
 * a) create a bio from the page, associate a endio with it.
 * b) flush the page
 * c) make sure the entries are on the disk, before we overwrite.
 *
 * TODO: better error handling!
 */
int flush_revmap_block_disk(struct ctx * ctx, struct page *page, u64 pba)
{
	struct bio * bio;
	struct revmap_meta_inmem *revmap_bio_ctx;
	struct closure *cl;

	cl = (struct closure *) kmalloc(sizeof(struct closure), GFP_KERNEL);
	if (!cl) {
		return -ENOMEM;
	}

	closure_init_stack(&cl);
	revmap_bio_ctx = kmem_cache_alloc(ctx->revmap_bioctx_cache, GFP_KERNEL);
	if (!revmap_bio_ctx) {
		kfree(cl);
		return -ENOMEM;
	}

	bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio) {
		kmem_cache_free(revmap_bio_ctx);
		kfree(cl);
		return -ENOMEM;
	}
	
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		kmem_cache_free(revmap_bio_ctx);
		kfree(cl);
		bio_put(bio);
		return -EFAULT;
	}
	
	revmap_bio_ctx->pba = pba;
	revmap_bio_ctx->ctx = ctx;
	revmap_bio_ctx->magic = REVMAP_PRIV_MAGIC;
	revmap_bio_ctx->page = page;
	revmap_bio_ctx->cl = cl;
	bio->bi_sector = ctx->revmap_pba;
	/* Adjust the revmap_pba for the next block 
	 * Addressing is based on 512bytes sector.
	 * TODO: Verify. 
	 * We need a 64 bit pba? sector_t is 32 bit
	 * But one address is for 512 bytes. Does this
	 * work?
	 */
	ctx->revmap_pba += NR_SECTORS_IN_BLK; 
	/* if we have the pba of the translation table,
	 * then reset the revmap pba to the original value
	 */
	if (ctx->revmap_pba == ctx->sb->extent_map) {
		ctx->revmap_pba = ctx->sb->revmap_pba;
	}
	bio->bi_end_io = revmap_entries_flushed();
	bio->bi_private = revmap_bio_ctx;
	atomic_inc(&ctx->pending_writes);
	generic_make_request(bio);
}

/* We store only the LBA. We can calculate the PBA from the wf
 */
static void add_revmap_entries(struct ctx * ctx, sector_t lba, unsigned int nrsectors)
{
	struct stl_revmap_entry_sector * ptr;
	int i, j;
	struct page * page;
/* We start the count from 0. So we increment first
 * and then check
 */
	atomic_inc(&ctx->revmap_sector_count);
	atomic_inc(&ctx->revmap_blk_count);

	j = atomic_read(&ctx->revmap_blk_count);
	if (0 == j) {
		/* we need to make sure the previous block is on the
		 * disk. We cannot overwrite without that.
		 */
		page = get_zeroed_page(GFP_KERNEL);
		ptr = (struct stl_revmap_entry_sector *)page_address(page);
	}
	i = atomic_read(&ctx->revmap_sector_count);
	if (i % NR_EXT_ENTRIES_PER_SEC == 0) {
		ptr->crc = calculate_crc();
		atomic_set(&ctx->revmap_sector_count, 0);
		if (NR_ENTRIES_PER_BLK == j) {
#ifdef BARRIER_NSTL
			/* We only need to wait here to minimize
			 * the loss of data, in case we end up
			 * sending translation entries for write
			 * sooner than they can be drained.
			 */
			wait_on_block_barrier();
#endif
			/* Flush the entries to the disk */
			pba = ptr->extents[NR_EXT_ENTRIES_PER_SEC - 1];
			/* TODO: Alternatively, we could flag, that
			 * this is the last pba of the zone
			 * from the caller of this function
			 */
			flush_revmap_block_disk(ctx, page, index, pba);
			atomic_set(&ctx->revmap_blk_count, 0);
			page = get_zeroed_page(GFP_KERNEL);
			ptr = page_address(page);
		} else {
			ptr = ptr + SECTOR_SIZE;
		}
	}
	ptr->extents[i].lba = lba;
    	ptr->extents[i].pba = pba; /* TODO: for now we keep this, dump this and later we can remove */
	/* we store the len in terms of sectors. If we stored in
	 * terms of number of blocks, then we would have to
	 * store 0s on the disk when the writes are not
	 * complete blocks
	 */
	ptr->extents[i].len = nrsectors;
	return;
}

/* split a write into one or more journalled packets
*/
static void map_write_io(struct ctx *ctx, struct bio *bio, int priority)
{
	struct bio *split = NULL, *pad = NULL;
	struct bio *bios[100];
	struct bio * clone;
	struct nstl_bioctx * bioctx;
	int i;
	volatile int nbios = 0;
	unsigned long flags, t1, t2;
	unsigned nr_sectors = bio_sectors(bio);
	sector_t s8, sector = bio->bi_iter.bi_sector;
	struct extent *e;
	sector_t next_header;
	struct stl_rev_map_entry_sector revmap_sector;
	

	/* wait until there's room
	*/

	bioctx = kmem_cache_alloc(ctx->bioctx_cache, GFP_KERNEL);
	if (!bioctx) {
		printf(KERN_ERR "\n Insufficient memory!");
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		/* We dont call bio_put() because the bio should not
		 * get freed by memory management before bio_endio()
		 */
		return;
	}

	bioctx->clone = bio_clone_fast(bio, GFP_KERNEL, NULL);
	if (!clone) {
		printf(KERN_ERR "\n Insufficient memory!");
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		/* We dont call bio_put() because the bio should not
		 * get freed by memory management before bio_endio()
		 */
		return;
	}
	bioctx->orig = bio;
	clone = bioctx->clone;
	/* TODO: Initialize refcount in bioctx and increment it every
	 * time bio is split or padded */
	refcount_set(&bioctx->ref, 1);

	printk(KERN_ERR "\n write frontier: %lu free_sectors_in_wf: %lu", ctx->write_frontier, ctx->free_sectors_in_wf);

	//printk(KERN_INFO "\n ******* Inside map_write_io, requesting sectors: %d", sectors);
	do {
		WARN_ON(nbios >= 40);
		/* 8 sectors (s8) forms a nr of BLOCKs of 4096 bytes */
		nr_sectors = bio_sectors(clone);
		if (unlikely(nr_sectors <= 0)) {
			printk(KERN_ERR "\n Less than 0 sectors (%d) requested!, nbios: %u", nr_sectors, nbios);
			break;
		}
		spin_lock_irqsave(&ctx->lock, flags);
		/*-------------------------------*/

		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		wf = get_write_frontier(ctx);
		/* room_in_zone should be same as
		 * ctx->nr_free_sectors_in_wf
		 */
		if (s8 > ctx->free_sectors_in_wf){
			s8 = round_down(ctx->free_sectors_in_wf, NR_SECTORS_IN_BLK);
			if (s8 <= 0) {
				panic("Should always have atleast a block left ");
			}
			nr_sectors = s8;
		} /* else we need to add a pad to rounded up value (nr_sectors < s8) */
		move_write_frontier(ctx, s8);

		/*-------------------------------*/
		spin_unlock_irqrestore(&ctx->lock, flags);
		printk(KERN_ERR "\n nbios: %d", nbios);
		if (s8 < bio_sectors(clone)) {
			if (!(split = bio_split(clone, nr_sectors, GFP_NOIO, ctx->bs))){
				printk(KERN_ERR "\n failed at bio_split! nbios: %d", nbios);
				goto fail;
			}
			bio_chain(split, clone);
			bios[nbios++] = split;
		} else {
			bios[nbios++] = clone;
			split = clone;
		}
		/* Next we fetch the LBA that our DM got */
		sector = bio->bi_iter.bi_sector;
		/* pad length is in sectors (like arg to alloc_bio)
		*/
		if (nr_sectors < s8) {
			/* we are here when we did not split and when
			 * the request is not block size aligned,
			 * and can be satisfied from the number of
			 * free blocks in the current write frontier.
			 */
			int padlen = s8 - nr_sectors;
			if (!(pad = stl_alloc_bio(ctx, padlen, NULL))){
				printk(KERN_ERR "\n failed at stl_alloc_bio, padlen: %d!", padlen);
				goto fail;
			}
			setup_bio(pad, nstl_clone_endio, ctx->dev->bdev, sector + nr_sectors, ctx, WRITE);
			bios[nbios++] = pad;
		}

    		subbio_ctx = kmem_cache_alloc(ctx->subbio_ctx_cache, gfp_kernel);
		if (!subbio_ctx) {
			printf(kern_err "\n insufficient memory!");
			bio->bi_status = BLK_STS_RESOURCE;
			return;
		}
		refcount_inc(&bioctx->ref);
		subbio_ctx->extent.lba = sector;
		subbio_ctx->extent.pba = wf;
		subbio_ctx->extent.nr_sectors = nr_sectors;
		subbio_ctx->bioctx = bioctx; /* This is common to all the subdivided bios */
		clone->private = subbio_ctx;
		split->bi_iter.bi_sector = wf; /* we use the save write frontier */
		//printk(KERN_ERR "\n nr_sectors: %d, s8 = %d, free_sectors_in_wf: %d, PBA: %ull", nr_sectors, s8, ctx->free_sectors_in_wf, ctx->write_frontier);
		/* Add this mapping the ckpt region */
	} while (split != clone);


	//printk(KERN_ERR "\n %s %s %d nbios: %d ", __FILE__, __func__, __LINE__, nbios);
	for (i = 0; i < nbios; i++) {
		bio_set_dev(bios[i], ctx->dev->bdev);
		bios[i]->bi_end_io = nstl_clone_endio;
		generic_make_request(bios[i]); /* preserves ordering, unlike submit_bio */
	}
	atomic_inc(&nr_writes);
	return;

fail:
	printk(KERN_INFO "FAIL!!!!\n");
	bio->bi_status = BLK_STS_IOERR; 	
	for (i = 0; i < nbios; i++)
		if (bios[i] && bios[i]->bi_private == ctx)
			nstl_clone_endio(bio);
	atomic_inc(&nr_failed_writes);
}


/*
   argv[0] = devname
   argv[1] = dm-name
   argv[2] = zone size (LBAs)
   argv[3] = max pba
   */
#define BS_NR_POOL_PAGES 128

void put_free_zone(struct ctx *ctx, u64 pba)
{
	unsigned long flags;
	unsigned long zonenr = get_zone_nr(ctx, pba);

	spin_lock_irqsave(&ctx->lock, flags);
	mark_zone_free(ctx , zonenr);
	spin_unlock_irqrestore(&ctx->lock, flags);
}

#define BLK_SZ 4096

struct stl_ckpt * read_checkpoint(struct ctx *ctx, unsigned long pba)
{
	struct block_device *bdev = ctx->dev->bdev;
	unsigned int blknr = pba / NR_SECTORS_IN_BLK;
	struct buffer_head *bh = __bread(bdev, blknr, BLK_SZ);
	if (!bh)
		return NULL;
	struct stl_ckpt *ckpt = (struct stl_ckpt *)bh->b_data;
	printk(KERN_INFO "\n ** pba: %lu, ckpt->cur_frontier_pba: %lld", pba, ckpt->cur_frontier_pba);
	ctx->ckpt_page = bh->b_page;
	return ckpt;
}

static void reset_ckpt(struct stl_ckpt * ckpt)
{
	ckpt->checkpoint_ver += 1;
	ckpt->ckpt_translation_table.prev_zone_pba = 0;
	ckpt->ckpt_translation_table.prev_count = 0;
	ckpt->ckpt_translation_table.cur_zone_pba = 0;
	ckpt->ckpt_translation_table.cur_count = 0;
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
 */
static void do_recovery(struct stl_ckpt * ckpt)
{
	/* Once necessary steps are taken for recovery (if needed),
	 * then we can reset the checkpoint and prepare it for the 
	 * next round
	 */
	reset_ckpt(ckpt);
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
	struct block_device *bdev = ctx->dev->bdev;
	unsigned long pba = sb->cp_pba;
	struct stl_ckpt *ckpt1, *ckpt2, *ckpt;
	struct page *page1;

	printk(KERN_INFO "\n !Reading checkpoint 1 from pba: %lu", pba);
	ckpt1 = read_checkpoint(ctx, pba);
	pba = pba + NR_SECTORS_IN_BLK;
	page1 = ctx->ckpt_page;
	/* ctx->ckpt_page will be overwritten by the next
	 * call to read_ckpt
	 */
	printk(KERN_INFO "\n !!Reading checkpoint 2 from pba: %lu", pba);
	ckpt2 = read_checkpoint(ctx, pba);
	if (ckpt1->checkpoint_ver >= ckpt2->checkpoint_ver) {
		ckpt = ckpt1;
		put_page(ctx->ckpt_page);
		ctx->ckpt_page = page1;
	}
	else {
		ckpt = ckpt2;
		//page2 is rightly set by read_ckpt();
		put_page(page1);
	}
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
	int nr_freezones = ctx->nr_freezones;
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

int read_extents_from_block(struct ctx * ctx, struct extent_entry *entry, unsigned int nr_extents)
{
	int i = 0;
	int ret;
	unsigned long zonenr = 0;

	if (ret < 0)
		return ret;
	
	while (i <= nr_extents) {
		/* If there are no more recorded entries on disk, then
		 * dont add an entries further
		 */
		if ((entry->lba == 0) && (entry->len == 0)) {
			i++;
			continue;
			
		}
		/* TODO: right now everything should be zeroed out */
		panic("Why are there any already mapped extents?");
		stl_update_range(ctx, entry->lba, entry->pba, entry->len);
		zonenr = get_zone_nr(ctx, entry->pba);
		entry = entry + sizeof(struct extent_entry);
		i++;
	}
}


#define NR_SECTORS_PER_BLK 8

/* 
 * Create a RB tree from the map on
 * disk
 *
 * Each extent covers one 4096 sized block
 * NOT a sector!!
 */
int read_extent_map(struct ctx *sc)
{
	struct buffer_head *bh = NULL;
	int nr_extents_in_blk = BLK_SZ / sizeof(struct extent_entry);
	struct extent_entry * entry0;
	unsigned long long pba = sc->sb->map_pba;
	unsigned long nrblks = sc->sb->blk_count_map;
	struct block_device *bdev = sc->dev->bdev;
	unsigned int blknr;
	blknr = pba/NR_SECTORS_PER_BLK;
	
	int i = 0;
	sc->n_extents = 0;
	while(i < nrblks) {
		bh = __bread(bdev, blknr, BLK_SZ);
		if (!bh)
			return -1;
		entry0 = (struct extent_entry *) bh->b_data;
		/* We read the extents in the entire block. the
		 * redundant extents should be unpopulated and so
		 * we should find 0 and break out */
		read_extents_from_block(sc, entry0, nr_extents_in_blk);
		i = i + 1;
		blknr = blknr + 1;
		put_bh(bh);
	}
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
	if (!ctx)
		return -1;
	printk(KERN_INFO "\n ctx->bitmap_bytes: %d ", ctx->bitmap_bytes);
	char * free_bitmap = (char *)kzalloc(11, GFP_KERNEL);
	if (!free_bitmap)
		return -1;

	ctx->freezone_bitmap = free_bitmap;
	return 0;
}

int allocate_gc_zone_bitmap(struct ctx *ctx)
{
	if (!ctx)
		return -1;
	char * gc_zone_bitmap = (char *)kzalloc(ctx->bitmap_bytes, GFP_KERNEL);
	if (!gc_zone_bitmap)
		return -1;

	ctx->gc_zone_bitmap = gc_zone_bitmap;
	return 0;
}

/* TODO: create a freezone cache
 * create a gc zone cache
 * currently calling kzalloc
 */
int read_seg_entries_from_block(struct ctx *ctx, struct stl_seg_entry *entry, unsigned int nr_seg_entries, unsigned int *segnr)
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
			printk(KERN_ERR "\n *segnr: %u", *segnr);
			mark_zone_free(ctx, *segnr);
		}
		else if (entry->vblocks < nr_blks_in_zone) {
			mark_zone_gc_candidate(ctx, *segnr);
		}
		entry = entry + sizeof(struct stl_seg_entry);
		*segnr = *segnr + 1;
		i++;
	}
	return;
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
	int i = 0, ret=0;
	struct stl_seg_entry *entry0;
	unsigned int blknr;
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
	printk(KERN_INFO "\n blknr of first SIT is: %u", blknr);
	printk(KERN_ERR "\n zone_count_main: %u", sb->zone_count_main);
	while (zonenr < sb->zone_count_main) {
		printk(KERN_ERR "\n zonenr: %u", zonenr);
		if (blknr > ctx->sb->zone0_pba) {
			//panic("seg entry blknr cannot be bigger than the data blknr");
			printk(KERN_ERR "seg entry blknr cannot be bigger than the data blknr");
			break;
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
}

struct stl_sb * read_superblock(struct ctx *ctx, unsigned long pba)
{

	struct stl_sb_info *sb_info;
	struct block_device *bdev = ctx->dev->bdev;
	unsigned int blknr = pba / NR_SECTORS_PER_BLK;
	/* TODO: for the future. Right now we keep
	 * the block size the same as the sector size
	 *
	*/
	if (set_blocksize(bdev, BLK_SZ))
		return NULL;

	printk(KERN_INFO "\n sb found at pba: %lu", pba);

	struct buffer_head *bh = __bread(bdev, blknr, BLK_SZ);
	if (!bh)
		return NULL;
	
	struct stl_sb * sb = (struct stl_sb *)bh->b_data;
	if (sb->magic != STL_SB_MAGIC) {
		printk(KERN_INFO "\n Wrong superblock!");
		put_bh(bh);
		return NULL;
	}
	printk(KERN_INFO "\n sb->magic: %u", sb->magic);
	printk(KERN_INFO "\n sb->map_pba: %lu, sb->blk_count_map: %lu", sb->map_pba, sb->blk_count_map);
	printk(KERN_INFO "\n sb->zone_count: %d", sb->zone_count);
	printk(KERN_INFO "\n sb->max_pba: %d", sb->max_pba);
	/* We put the page in case of error in the future (put_page)*/
	ctx->sb_page = bh->b_page;
	return sb;
}


int read_metadata(struct ctx * ctx)
{
	int ret;
	struct stl_sb * sb1, *sb2;
	struct page *page;
	unsigned long pba = 0;
	struct stl_ckpt *ckpt;
	struct block_device *bdev = ctx->dev->bdev;

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
	ret = read_extent_map(ctx);
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
	printk(KERN_INFO "\n Nr of zones in main are: %lu, bitmap_bytes: %d", sb1->zone_count_main, ctx->bitmap_bytes);
	if (sb1->zone_count_main % BITS_IN_BYTE > 0)
		ctx->bitmap_bytes += 1;
	read_seg_info_table(ctx);
	printk(KERN_INFO "\n read segment entries, free bitmap created!");
	if (ctx->nr_freezones != ckpt->free_segment_count) { 
		/* TODO: Do some recovery here.
		 * For now we panic!!
		 * SIT is flushed before CP. So CP could be stale.
		 * Update checkpoint accordingly and record!
		 */
		//panic("free zones in SIT and checkpoint does not match!");
	}
	return 0;
}

static int stl_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	int r = -ENOMEM;
	struct ctx *ctx;
	unsigned long long tmp, max_pba;
	char d;

	//dump_stack();

	// DMINFO("ctr %s %s %s %s", argv[0], argv[1], argv[2], argv[3]);

	printk(KERN_INFO "\n argc: %d", argc);
	if (argc < 2) {
		ti->error = "dm-stl: Invalid argument count";
		return -EINVAL;
	}

	printk(KERN_INFO "\n argv[0]: %s, argv[1]: %s", argv[0], argv[1]);

	if (!(ctx = kzalloc(sizeof(struct ctx), GFP_KERNEL)))
		goto fail;
	ti->private = ctx;

	if ((r = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &ctx->dev))) {
		ti->error = "dm-nstl: Device lookup failed.";
		goto fail0;
	}

	printk(KERN_INFO "\n sc->dev->bdev->bd_part->start_sect: %llu", ctx->dev->bdev->bd_part->start_sect);
	printk(KERN_INFO "\n sc->dev->bdev->bd_part->nr_sects: %llu", ctx->dev->bdev->bd_part->nr_sects);

	ctx->extent_pool = mempool_create_slab_pool(MIN_EXTENTS, extents_slab);
	if (!ctx->extent_pool)
		goto fail1;

	r = read_metadata(ctx);
	if (r < 0)
		goto fail2;

	max_pba = ctx->dev->bdev->bd_inode->i_size / 512;


	sprintf(ctx->nodename, "stl/%s", argv[1]);

	r = -EINVAL;

	ctx->nr_lbas_in_zone = (1 << (ctx->sb->log_zone_size - ctx->sb->log_sector_size));
	printk(KERN_INFO "\n device records max_pba: %llu", max_pba);
	ctx->max_pba = ctx->sb->max_pba;
	printk(KERN_INFO "\n formatted max_pba: %llu", ctx->max_pba);
	if (ctx->max_pba > max_pba) {
		ti->error = "dm-stl: Invalid max pba found on sb";
		goto fail3;
	}
	ctx->write_frontier = ctx->ckpt->cur_frontier_pba;

	printk(KERN_INFO "%s %d kernel wf: %ld\n", __func__, __LINE__, ctx->write_frontier);
	ctx->wf_end = zone_end(ctx, ctx->write_frontier);
	printk(KERN_INFO "%s %d kernel wf end: %ld\n", __func__, __LINE__, ctx->wf_end);
	printk(KERN_INFO "max_pba = %lu", ctx->max_pba);
	ctx->free_sectors_in_wf = ctx->wf_end - ctx->write_frontier + 1;


	loff_t disk_size = i_size_read(ctx->dev->bdev->bd_inode);
	printk(KERN_INFO "\n The disk size as read from the bd_inode: %llu", disk_size);
	printk(KERN_INFO "\n max sectors: %llu", disk_size/512);
	printk(KERN_INFO "\n max blks: %llu", disk_size/4096);
	spin_lock_init(&ctx->lock);

	atomic_set(&ctx->io_count, 0);
	atomic_set(&ctx->n_reads, 0);
	atomic_set(&ctx->pages_alloced, 0);
	atomic_set(&ctx->nr_writes, 0);
	atomic_set(&ctx->nr_failed_writes, 0);
	atomic_set(&ctx->revmap_sector_count, 0);
	atomic_set(&ctx->revmap_blk_count, 0);
	atomic_set(&ctx->zone_revmap_count, 0);
	ctx->target = 0;

	r = -ENOMEM;
	ti->error = "dm-stl: No memory";

	ctx->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
	if (!ctx->page_pool)
		goto fail5;

	//printk(KERN_INFO "about to call bioset_init()");
	ctx->bs = kzalloc(sizeof(*(ctx->bs)), GFP_KERNEL);
	if (!ctx->bs)
		goto fail6;
	if(bioset_init(ctx->bs, BS_NR_POOL_PAGES, 0, BIOSET_NEED_BVECS|BIOSET_NEED_RESCUER) == -ENOMEM) {
		printk(KERN_ERR "\n bioset_init failed!");
		goto fail7;
	}

	ctx->rb = RB_ROOT;
	rwlock_init(&ctx->rb_lock);

	ctx->sit_rb = RB_ROOT;
	rwlock_init(&ctx->sit_rb_lock);
	ctx->sectors_copied = 0;


	r = stl_gc_thread_start(ctx);
	if (r) {
		goto fail7;;
	}

	ctx->bioctx_cache = kmem_cache_create("nstl_bioctx_cache", sizeof(struct nstl_bioctx), 0, NULL);
	if (!ctx->bioctx_cache) {
		goto stop_gc_thread;
	}
	ctx->revmap_bioctx_cache = kmem_cache_create("nstl_revmapbioctx_cache", sizeof(struct nstl_bioctx), 0, NULL);
	if (!ctx->revmap_bioctx_cache) {
		goto destroy_cache_bioctx;
	}
	ctx->trans_page_write_cache = kmem_cache_create("nstl_transwrite_cache", sizeof(struct trans_page_write_ctx), 0, NULL);
	if (!ctx->trans_page_write_cache) {
		goto destroy_cache;
	}
	ctx->revmap_pba = ctx->sb->revmap_pba;
	DEFINE(ctx->flush_lock);
/* failed case */
destroy_cache:
		kmem_cache_destroy(ctx->revmap_bioctx_cache);
destroy_cache_bioctx:
		kmem_cache_destroy(ctx->bioctx_cache);
stop_gc_thread:
	stl_gc_thread_stop(ctx);
fail7:
	bioset_exit(ctx->bs);
fail6:
	kfree(ctx->bs);
fail5:
	if (ctx->page_pool)
		mempool_destroy(ctx->page_pool);
fail3:
	put_page(ctx->sb_page);
	put_page(ctx->ckpt_page);
fail2:
	if (ctx->sb_page)
		put_page(ctx->sb_page);
	if (ctx->ckpt_page)
		put_page(ctx->ckpt_page);
	/* TODO : free extent page
	 * and segentries page */
		
	if (ctx->extent_pool)
		mempool_destroy(ctx->extent_pool);
fail1:
	dm_put_device(ti, ctx->dev);
fail0:
	kfree(ctx);

fail:
	return r;
}

static void stl_dtr(struct dm_target *ti)
{
	struct ctx *ctx = ti->private;
	ti->private = NULL;

	kmem_cache_destroy(ctx->bioctx_cache);
	kmem_cache_destroy(ctx->revmap_bioctx_cache);
	kmem_cache_destroy(ctx->trans_page_write_cache);
	stl_gc_thread_stop(ctx);
	bioset_exit(ctx->bs);
	kfree(ctx->bs);
	mempool_destroy(ctx->page_pool);
	mempool_destroy(ctx->extent_pool);
	dm_put_device(ti, ctx->dev);
	kfree(ctx);
}

static int stl_map(struct dm_target *ti, struct bio *bio)
{
	volatile struct ctx *sc;
       
	if (!ti)
		return 0;

	sc = ti->private;

	if(unlikely(bio == NULL)) {
		return 0;
	}

	switch (bio_op(bio)) {
		case REQ_OP_DISCARD:
		case REQ_OP_FLUSH:	
			//		case REQ_OP_SECURE_ERASE:
			//		case REQ_OP_WRITE_ZEROES:
			//printk(KERN_INFO "Discard or Flush: %d \n", bio_op(bio));
			/* warn on panic is on. so removing this warn on
			 WARN_ON(bio_sectors(bio));
			*/
			if(bio_sectors(bio)) {
				printk(KERN_ERR "\n sectors in bio when FLUSH requested");
			}
			bio_set_dev(bio, sc->dev->bdev);
			return DM_MAPIO_REMAPPED;
		default:
			break;
	}

	if (bio_data_dir(bio) == READ) 
		split_read_io(sc, bio, 0);
	else{
		map_write_io(sc, bio, 0);
	}
	return DM_MAPIO_SUBMITTED;
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
