/* i*- c-basic-offset: 8; indent-tabs-mode: t; compile-command: "make" -*-
 *
 * Copyright (C) 2016 Peter Desnoyers. All rights reserved.
 *
 * This file is released under the GPL.
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

static struct kmem_cache *_extent_cache;
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
	return zone_start(ctx, pba) + ctx->nr_lbas_in_zone - 1; 
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
int stl_gc_thread_start(struct ctx *sc)
{
	struct stl_gc_thread *gc_th;
	dev_t dev = sc->dev->bdev->bd_dev;
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

        sc->gc_th = gc_th;
	init_waitqueue_head(&gc_th->stl_gc_wait_queue);
	sc->gc_th->stl_gc_task = kthread_run(gc_thread_fn, sc,
			"stl-gc%u:%u", MAJOR(dev), MINOR(dev));

	if (IS_ERR(gc_th->stl_gc_task)) {
		err = PTR_ERR(gc_th->stl_gc_task);
		kvfree(gc_th);
		sc->gc_th = NULL;
		return err;
	}

	printk(KERN_ERR "\n Created a STL GC thread ");
	return 0;	
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


static void stl_endio(struct bio *bio)
{
	int i = 0;
	struct bio_vec * bv;
	struct ctx *sc = bio->bi_private;
	struct bvec_iter_all iter_all;

	bio_for_each_segment_all(bv, bio, iter_all) {
		WARN_ON(!bv->bv_page);
		mempool_free(bv->bv_page, sc->page_pool);
		atomic_dec(&sc->pages_alloced);
		bv->bv_page = NULL;
		i++;
	}
	WARN_ON(i != 1);
	bio_put(bio);
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

/* moves the write frontier, returns the LBA of the packet trailer
*/
static int get_new_zone(struct ctx *ctx)
{
	sector_t old_free = ctx->free_sectors_in_wf;
	unsigned long zone_nr;
	if (ctx->write_frontier > ctx->wf_end)
		printk(KERN_INFO "kernel wf before BUG: %ld - %ld\n", ctx->write_frontier, ctx->wf_end);
	BUG_ON(ctx->write_frontier > ctx->wf_end);
	zone_nr = get_next_freezone_nr(ctx);
	if (zone_nr < 0) {
		printk(KERN_WARNING "\n Disk is full, no more writing possible! ");
		return (zone_nr);
	}
	ctx->write_frontier = zone_start(ctx, zone_nr);
	ctx->wf_end = zone_end(ctx, ctx->write_frontier);
	ctx->free_sectors_in_wf -= ctx->wf_end - ctx->write_frontier;
	ctx->nr_freezones--;
	//printk(KERN_INFO "Num of free sect.: %ld, diff of end and wf:%ld\n", sc->free_sectors_in_wf, sc->wf_end - sc->write_frontier);
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
	struct stl_ckpt_entry * table = &ctx->ckpt->ckpt_translation_table;
	table->prev_zone_pba = table->cur_zone_pba;
	table->prev_count = table->cur_count;
	table->cur_zone_pba = wf;
	table->cur_count = 0;
}

static void move_write_frontier(struct ctx *ctx, sector_t sectors_s8)
{
	sector_t wf = ctx->write_frontier;

	if (ctx->free_sectors_in_wf < sectors_s8) {
		panic("Wrong manipulation of wf; used unavailable sectors in a log");
	}
	if (ctx->free_sectors_in_wf < sectors_s8 + 1) {
		ctx->write_frontier = get_new_zone(ctx);
		add_ckpt_new_wf(ctx, wf);
		if (ctx->write_frontier < 0) {
			printk(KERN_INFO "No more disk space available for writing!");
			return -1;
		}
	}
}

/* We store only the LBA. We can calculate the PBA from the wf
 */
static void add_ckpt_mapping(struct ctx * ctx, sector_t lba, unsigned int nrsectors)
{
	struct stl_ckpt_entry * table = &ctx->ckpt->ckpt_translation_table;
	table->cur_count += 1;
	unsigned int index = table->cur_count + table->prev_count;

	table->extents[index].lba = lba;
	/* we store the length in terms of blocks */
	table->extents[index].len = nrsectors/NR_SECTORS_IN_BLK;
	return;
}

/* split a write into one or more journalled packets
*/
static void map_write_io(struct ctx *ctx, struct bio *bio, int priority)
{
	struct bio *split = NULL, *pad = NULL;
	struct bio *bios[100];
	int i;
	volatile int nbios = 0;
	unsigned long flags, t1, t2;
	unsigned nr_sectors = bio_sectors(bio);
	sector_t s8, sector = bio->bi_iter.bi_sector;
	struct extent *e;
	sector_t next_header;

	/* wait until there's room
	*/

	//printk(KERN_INFO "\n ******* Inside map_write_io, requesting sectors: %d", sectors);
	do {
		printk(KERN_ERR "\n %s %s %d nbios: %d ", __FILE__, __func__, __LINE__, nbios);
		unsigned seq;
		volatile sector_t _pba, wf, prev;

		WARN_ON(nbios >= 40);
		/* 8 sectors (s8) forms a nr of BLOCKs of 4096 bytes */
		nr_sectors = bio_sectors(bio);
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		/* Next we fetch the LBA that our DM got */
		sector = bio->bi_iter.bi_sector;

		spin_lock_irqsave(&ctx->lock, flags);
		/*-------------------------------*/
		wf = ctx->write_frontier;

		if (s8 > room_in_zone(ctx, wf)){
			s8 = round_down(room_in_zone(ctx, wf), NR_SECTORS_IN_BLK);
			nr_sectors = s8;
		} /* else we need to add a pad to rounded up value (nr_sectors < s8) */
		/*-------------------------------*/
		spin_unlock_irqrestore(&ctx->lock, flags);
		printk(KERN_ERR "\n nbios: %d", nbios);
		if (s8 < bio_sectors(bio)) {
			if (!(split = bio_split(bio, nr_sectors, GFP_NOIO, ctx->bs))){
				printk(KERN_ERR "\n failed at bio_split! nbios: %d", nbios);
				goto fail;
			}
			bio_chain(split, bio);
			bios[nbios++] = split;
		} else {
			bios[nbios++] = bio;
			split = bio;
		}
again:
		/* pad length is in sectors (like arg to alloc_bio)
		*/
		if (nr_sectors < s8) {
			/* we are here when we did not split */
			int padlen = s8 - nr_sectors;
			if (!(pad = stl_alloc_bio(ctx, padlen, NULL))){
				printk(KERN_ERR "\n failed at stl_alloc_bio, padlen: %d!", padlen);
				goto fail;
			}
			setup_bio(pad, stl_endio, ctx->dev->bdev, wf+nr_sectors, ctx, WRITE);
			wf += padlen;
			bios[nbios++] = pad;
		}

		/*Update the translation table with the LBA:sector and
		 * our new PBA: wf
		 */
		e = stl_update_range(ctx, sector, wf, nr_sectors);
		split->bi_iter.bi_sector = wf;
		_pba = wf;
		add_ckpt_mapping(ctx, sector, s8);
		move_write_frontier(ctx, s8);
		wf = ctx->write_frontier;
		/* Add this mapping the ckpt region */
		//printk(KERN_ERR "\n split!=bio");
	} while (split != bio);

	for (i = 0; i < nbios; i++) {
		bio_set_dev(bios[i], ctx->dev->bdev);
		generic_make_request(bios[i]); /* preserves ordering, unlike submit_bio */
	}
	atomic_inc(&c4);
	return;

fail:
	printk(KERN_INFO "FAIL!!!!\n");
	bio->bi_status = BLK_STS_IOERR; 	
	for (i = 0; i < nbios; i++)
		if (bios[i] && bios[i]->bi_private == ctx)
			stl_endio(bio);
	atomic_inc(&c5);
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
	char * free_bitmap = (char *)kzalloc(ctx->bitmap_bytes, GFP_KERNEL);
	if (!free_bitmap)
		return -1;

	ctx->freezone_bitmap = free_bitmap;
	return 0;
}

int allocate_gc_zone_bitmap(struct ctx *ctx)
{
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

	while (i < nr_seg_entries) {
		if (entry->vblocks == 0) {
			printk(KERN_ERR "\m *segnr: %u", *segnr);
			mark_zone_free(ctx, *segnr);
		}
		else if (entry->vblocks < nr_blks_in_zone) {
			mark_zone_gc_candidate(ctx, *segnr);
		}
		entry = entry + sizeof(struct stl_seg_entry);
		*segnr++;
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

	unsigned long nr_data_zones = sb->zone_count_main; /* these are the number of segment entries to read */
	unsigned long nr_seg_entries_read = 0;
       
	if (!ctx)
		return -1;

	ret = allocate_freebitmap(ctx);
	if (0 > ret)
		return ret;
	ret = allocate_gc_zone_bitmap(ctx);
	if (0 > ret) {
		kfree(ctx->freezone_bitmap);
		return ret;
	}

	bdev = ctx->dev->bdev;
	pba = ctx->sb->sit_pba;
	blknr = pba/NR_SECTORS_PER_BLK;
	nrblks = ctx->sb->blk_count_sit;

	while (zonenr < sb->zone_count_main) {
		printk(KERN_INFO "\n blknr: %lu", blknr);
		if (blknr > ctx->sb->zone0_pba) {
			panic("seg entry blknr cannot be bigger than the data blknr");
		}
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

	ctx->extent_pool = mempool_create_slab_pool(MIN_EXTENTS, _extent_cache);
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
	ctx->free_sectors_in_wf = ctx->wf_end - ctx->write_frontier;


	loff_t disk_size = i_size_read(ctx->dev->bdev->bd_inode);
	printk(KERN_INFO "\n The disk size as read from the bd_inode: %llu", disk_size);
	printk(KERN_INFO "\n max sectors: %llu", disk_size/512);
	printk(KERN_INFO "\n max blks: %llu", disk_size/4096);
	spin_lock_init(&ctx->lock);

	atomic_set(&ctx->io_count, 0);
	atomic_set(&ctx->n_reads, 0);
	atomic_set(&ctx->pages_alloced, 0);
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
	if (!r) {
		return 0;
	}
/* failed case */
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
	struct ctx *sc = ti->private;
	ti->private = NULL;

	bioset_exit(sc->bs);
	kfree(sc->bs);
	mempool_destroy(sc->page_pool);
	mempool_destroy(sc->extent_pool);
	dm_put_device(ti, sc->dev);
	kfree(sc);
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

	if (!(_extent_cache = KMEM_CACHE(extent, 0)))
		return r;
	if ((r = dm_register_target(&stl_target)) < 0)
		goto fail;
	printk(KERN_INFO "dm-nstl\n %s %d", __func__, __LINE__);
	return 0;

fail:
	if (_extent_cache)
		kmem_cache_destroy(_extent_cache);
	return r;
}

/* Called on module exit (rmmod) */
static void __exit dm_stl_exit(void)
{
	dm_unregister_target(&stl_target);
	if(_extent_cache)
		kmem_cache_destroy(_extent_cache);
}


module_init(dm_stl_init);
module_exit(dm_stl_exit);

MODULE_DESCRIPTION(DM_NAME "log structured SMR Translation Layer");
MODULE_LICENSE("GPL");
