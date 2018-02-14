/* -*- c-basic-offset: 8; indent-tabs-mode: t; compile-command: "make" -*-
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

#include "stl-u.h"
#include "stl-wait.h"

#define DM_MSG_PREFIX "stl"

static struct kmem_cache *_extent_cache;

/* total size = 40 bytes (64b). fits in 1 cache line 
   for 32b is 32 bytes, fits in ARM cache line */
struct extent {
	struct rb_node rb;	/* 20 bytes */
	sector_t lba;		/* 512B LBA */
	sector_t pba;		
	u16   len;
	u16   flags;		
};

struct io {
	struct bio *bio;
	struct page *pad;
	struct page *head;
	struct page *tail;
};

/* this has grown kind of organically, and needs to be cleaned up.
 */
struct ctx {
	sector_t zone_size;	/* in 512B LBAs */
	int      zone_count;
	sector_t cache_start;	/* LBA */
	sector_t data_start;	/* LBA */
	sector_t data_end;	/* LBA. plus 1 */
	sector_t write_frontier; /* LBA */
	spinlock_t lock;
	struct rb_root rb;
	rwlock_t rb_lock;
	mempool_t *extent_pool;
	mempool_t *io_pool;
	mempool_t *page_pool;
	struct dm_dev *dev;
	sector_t previous;
	int extents;
	struct stl_msg msg;
	sector_t list_lba;
	unsigned n_writes;
        struct completion init_wait; /* before START issued */
        wait_queue_head_t write_wait;
	wait_queue_head_t space_wait;
        sector_t clean_start;
        sector_t clean_end;
        atomic_t n_gather;
        wait_queue_head_t gather_wait;
        struct completion gather_done;
        atomic_t n_reads;
	struct bio_set *bs;
	sector_t tail_pba;
};

#define MIN_EXTENTS 16
#define MIN_POOL_PAGES 16
#define MIN_POOL_IOS 16

sector_t zone_start(struct ctx *sc, sector_t pba) {
    return pba - (pba % sc->zone_size);
}
sector_t zone_end(struct ctx *sc, sector_t pba) {
    return zone_start(sc, pba) + sc->zone_size;
}

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
		if (new->lba < e->lba)
			link = &(*link)->rb_left;
		else
			link = &(*link)->rb_right;
	}
	/* Put the new node there */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
	sc->extents++;
}


static void stl_rb_remove(struct ctx *sc, struct extent *e)
{
	struct rb_root *root = &sc->rb;
	rb_erase(&e->rb, root);
	sc->extents--;
}


static struct extent *stl_rb_first(struct ctx *sc)
{
	struct rb_root *root = &sc->rb;
	struct rb_node *node = rb_first(root);
	return container_of(node, struct extent, rb);
}


static struct extent *stl_rb_next(struct extent *e)
{
	struct rb_node *node = rb_next(&e->rb);
	return (node == NULL) ? NULL : container_of(node, struct extent, rb);
}


/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to forward and reverse map.
 */
static struct extent * stl_update_range(struct ctx *sc, sector_t lba, sector_t pba, size_t len)
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

			BUG_ON(new_len <= 0);

			if (_new2 == NULL)
				goto fail;
			*_new2 = (struct extent){.lba = lba+len, .pba = new_pba,
						.len = new_len, .flags = 0};
			e->len = lba - e->lba; /* do this *before* inserting below */
			stl_rb_insert(sc, _new2);
			e = _new2;
			_new2 = NULL;
		}
		/* [------------]
		 *        [+++++++++]        -> [------][+++++++++]
		 */
		else if (e->lba < lba) {
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
			stl_rb_remove(sc, e);
			mempool_free(e, sc->extent_pool);
			e = tmp;
		}
		/*          [------]
		 *   [+++++++++]        -> [++++++++++][---]
		 */
		if (e != NULL && lba+len > e->lba) {
			int n = (lba+len) - e->lba;
			e->lba += n;
			e->pba += n;
			e->len -= n;
		}
	}

	/* TRIM indicated by pba = -1 */
	if (pba != -1) {
		*_new = (struct extent){.lba = lba, .pba = pba, .len = len, .flags = 0};
		stl_rb_insert(sc, _new);
	}
	write_unlock_irqrestore(&sc->rb_lock, flags);
	if (_new2 != NULL)
		mempool_free(_new2, sc->extent_pool);

	return _new;
	
fail:
	write_unlock_irqrestore(&sc->rb_lock, flags);
	DMERR("could not allocate extent");
	if (_new)
		mempool_free(_new, sc->extent_pool);
	if (_new2)
		mempool_free(_new2, sc->extent_pool);

	return NULL;
}


static void split_read_io(struct ctx *sc, struct bio *bio)
{
        struct bio *split = NULL;

        atomic_inc(&sc->n_reads);
	do {
		sector_t sector = bio->bi_iter.bi_sector;
                struct extent *e = stl_rb_geq(sc, sector);
		unsigned sectors = bio_sectors(bio);

		/* note that beginning of extent is >= start of bio */
		/* [----bio-----] [eeeeeee]  */
		if (e == NULL || e->lba >= sector + sectors)  {
			sector = sc->data_start + sector;
#if 0
                        zero_fill_bio(bio);
			bio_endio(bio);
                        return;
#endif
		}
                /* [eeeeeeeeeeee] eeeeeeeeeeeee]<- could be shorter or longer
		        [---------bio------] */
		else if (e->lba <= sector) {
			unsigned overlap = e->lba + e->len - sector;
			if (overlap < sectors)
				sectors = overlap;
			sector = e->pba + sector - e->lba;
		}
                /*             [eeeeeeeeeeee]
		     [---------bio------] */
		else {
			sectors = e->lba - sector;
			sector = sc->data_start + sector;
#if 0
                        split = bio_split(bio, sectors, GFP_NOIO, fs_bio_set);
			bio_chain(split, bio);
                        zero_fill_bio(split);
			bio_endio(split);
                        continue;
#endif
                            
		}

		if (sectors < bio_sectors(bio)) {
			split = bio_split(bio, sectors, GFP_NOIO, fs_bio_set);
			bio_chain(split, bio);
		} else {
			split = bio;
		}

		printk(KERN_INFO "read %lu %lu %d %p\n", split->bi_iter.bi_sector,
		       sector, sectors, bio);
		split->bi_iter.bi_sector = sector;
		split->bi_bdev = sc->dev->bdev;
		generic_make_request(split);
	} while (split != bio);
}


static void stl_endio(struct bio *bio)
{
	int i;
	struct bio_vec *bv = NULL;
	struct ctx *sc = bio->bi_private;

        bio_for_each_segment_all(bv, bio, i) {
                WARN_ON(!bv->bv_page);
                mempool_free(bv->bv_page, sc->page_pool);
                bv->bv_page = NULL;
        }
	WARN_ON(i != 1);
        bio_put(bio);
}

/* create a bio for a journal header (if len=0) or trailer (if len>0).
 */
struct bio *make_header(struct ctx *sc, unsigned seq, sector_t here, sector_t prev,
			unsigned sectors, sector_t lba, sector_t pba, unsigned len)
{
	struct page *page = NULL;
	struct bio *bio = NULL;
	struct stl_header *h = NULL;
        sector_t next = here + 4 + sectors;
        
	if (!(bio = bio_alloc_bioset(GFP_NOIO, 1, sc->bs)))
		return NULL;
	if (!(page = mempool_alloc(sc->page_pool, GFP_NOIO))) {
		bio_put(bio);
		return NULL;
	}

	h = page_address(page);
	memset(h, 0, STL_HDR_SIZE);
	h->magic = STL_HDR_MAGIC;
	get_random_bytes(&h->nonce, sizeof(h->nonce));

        /* handle zone wrap cases. min space for an extent at the end
         * of a zone is 8K, so if there's less than that we wrap to
         * the beginning of the next zone.
         */
        if (sc->zone_size - (next % sc->zone_size) < 16) {
                next = next + 16;
                next = next - (next % sc->zone_size);
        }
        
	h->prev_pba = prev;
	h->next_pba = here + 4 + sectors;
	h->lba = lba;
	h->pba = pba;
	h->len = len;
	h->seq = seq;
	h->crc32 = crc32_le(0, (u8 *)h, STL_HDR_SIZE);

	bio_add_page(bio, page, STL_HDR_SIZE, 0);
	bio->bi_end_io = stl_endio;
	bio->bi_bdev = sc->dev->bdev;
	bio->bi_iter.bi_sector = here;
	bio->bi_private = sc;
	bio->bi_opf = WRITE;
//	printk(KERN_INFO "hdr seq %u lba %llu pba %llu len %d here %lu\n",
//	       h->seq, h->lba, h->pba, h->len, here);
	
	return bio;
}

/* we pass in the rw flags because I don't know how they work yet :-)
 */
static struct bio *stl_pad_bio(struct ctx *sc, unsigned rw, unsigned len)
{
	struct page *page = NULL;
	struct bio *bio = NULL;
	
	if (!(page = mempool_alloc(sc->page_pool, GFP_NOIO)))
		return NULL;
	if (!(bio = bio_alloc_bioset(GFP_NOIO, 1, sc->bs))) {
                mempool_free(page, sc->page_pool);
		return NULL;
	}
	
	bio->bi_opf = rw;
	bio->bi_private = sc;
	bio_add_page(bio, page, len, 0);
	bio->bi_end_io = stl_endio;

	return bio;
}


static void map_write_io(struct ctx *sc, struct bio *bio, int priority)
{
	struct bio *split = NULL, *pad = NULL;
	struct bio *bios[7];
	int i, nbios = 0;
        unsigned long flags;
        unsigned sectors = bio_sectors(bio);
        sector_t s8, sector = bio->bi_iter.bi_sector;

        /* wait if we're writing to an LBA range that is currently
         * being cleaned.
         */
        spin_lock_irqsave(&sc->lock, flags);
	if (!priority && sector < sc->clean_end && sector+sectors >= sc->clean_start) {
		printk(KERN_INFO "STALL: lba %ld\n", sector);
		wait_event_lock_irqsave(sc->write_wait,
					sector >= sc->clean_end || sector+sectors < sc->clean_start,
					sc->lock, flags);
		printk(KERN_INFO "unSTALL: lba %ld\n", sector);
	}
        spin_unlock_irqrestore(&sc->lock, flags);

	/* wait until there's room in the cache. This would be easier
	 * if we just accounted for free space.
	 */
	sector_t cache_size = sc->data_start - sc->cache_start;
	sector_t min_room = sc->zone_size * 5 / 4;

	if (!priority && ((((sc->tail_pba-1) - sc->write_frontier +
			    cache_size) % cache_size) < min_room)) {
		printk(KERN_INFO "GC WAIT\n");
		wait_event(sc->space_wait,
			   ((((sc->tail_pba-1) - sc->write_frontier) +
			    cache_size) % cache_size) > min_room);
		printk(KERN_INFO "GC UNWAIT\n");
	}

	do {
		unsigned seq, room;
		sector_t _pba, wf, prev, endwf;

                sectors = bio_sectors(bio);
                s8 = (sectors + 7) & ~7;
		sector = bio->bi_iter.bi_sector;

		spin_lock_irqsave(&sc->lock, flags);
		endwf = zone_end(sc, sc->write_frontier);
		room =  endwf - sc->write_frontier;
		sectors = min(sectors, room - 8);
		wf = sc->write_frontier;
		sc->write_frontier += (s8+8);
		if (sc->write_frontier + 16 > endwf) {
			sc->write_frontier = endwf;
			if (sc->write_frontier >= sc->data_start)
				sc->write_frontier = sc->cache_start;
		}
		seq = sc->n_writes++;
		prev = sc->previous;
		sc->previous = wf + s8 + 4;
		spin_unlock_irqrestore(&sc->lock, flags);
		
		if (!(bios[nbios++] = make_header(sc, seq, wf, prev, s8, 0, 0, 0))) 
			goto fail;
                prev = wf;
		wf += 4;
		
		if (sectors < bio_sectors(bio)) {
			if (!(split = bio_split(bio, sectors, GFP_NOIO, sc->bs)))
				goto fail;
			bio_chain(split, bio);
			bios[nbios++] = split;
		} else {
			bios[nbios++] = bio;
			split = bio;
		}

		printk(KERN_INFO "write %lu %lu %d %p\n", split->bi_iter.bi_sector,
		       wf, sectors, bio);

		stl_update_range(sc, sector, wf, sectors);
		split->bi_iter.bi_sector = wf;
		_pba = wf;
		wf += sectors;

		if (sectors & 7) {
			int padlen = 4096 - (bio->bi_iter.bi_size & ~PAGE_MASK);
			if (!(pad = stl_pad_bio(sc, bio->bi_opf, padlen)))
				goto fail;
			pad->bi_iter.bi_sector = wf;
			wf += padlen / 512;
			bios[nbios++] = pad;
		}

		if (!(bios[nbios++] = make_header(sc, seq, wf, prev, 0, sector, _pba, sectors)))
			goto fail;
		wf += 4;
	} while (split != bio);

        BUG_ON(nbios > sizeof(bios)/sizeof(bios[0]));
	for (i = 0; i < nbios; i++) {
		bios[i]->bi_bdev = sc->dev->bdev;
		generic_make_request(bios[i]); /* preserves ordering, unlike submit_bio */
	}
	return;

fail:
        printk(KERN_INFO "FAIL!!!!\n");
	bio->bi_error = -ENOMEM;
	for (i = 0; i < nbios; i++)
		if (bios[i] && bios[i]->bi_private == sc)
			stl_endio(bio);
}


static int stl_map(struct dm_target *ti, struct bio *bio)
{
        struct ctx *sc = ti->private;

        wait_for_completion(&sc->init_wait); /* START */
        
	/* don't handle flush with data */
//	if (unlikely(bio->bi_rw & (REQ_FLUSH | REQ_DISCARD))) {
//        if (unlikely(bio->bi_opf & (REQ_PREFLUSH | REQ_OP_DISCARD))) {
	if (unlikely(bio->bi_opf & ((1ULL << REQ_OP_DISCARD) | (1ULL << REQ_OP_FLUSH )))) {
	WARN_ON(bio_sectors(bio));
                bio->bi_bdev = sc->dev->bdev;
                return DM_MAPIO_REMAPPED;
        }

	if (bio_data_dir(bio) == READ) 
		split_read_io(sc, bio);
	else
		map_write_io(sc, bio, 0);

        return DM_MAPIO_SUBMITTED;
}


static void stl_dtr(struct dm_target *ti)
{
	struct ctx *sc = ti->private;
	ti->private = NULL;

	mempool_destroy(sc->extent_pool);
	mempool_destroy(sc->io_pool);
	mempool_destroy(sc->page_pool);
	bioset_free(sc->bs);
	dm_put_device(ti, sc->dev);
	kfree(sc);
}

static struct ctx *_sc;

/*
  argv[0] = devname
  argv[1] = band size (LBAs)
  argv[2] = cache_start (band#)
  argv[3] = data_start (bands#)
  argv[4] = data_end (band#)
 */
static int stl_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	int r = -ENOMEM;
	struct ctx *sc;
        unsigned long long tmp;
        char d;

	DMINFO("ctr %s %s %s %s %s", argv[0], argv[1], argv[2], argv[3], argv[4]);

	if (argc != 5) {
		ti->error = "dm-stl: Invalid argument count";
		return -EINVAL;
	}
	
	if (!(_sc = sc = kzalloc(sizeof(*sc), GFP_KERNEL)))
		goto fail;
	ti->private = sc;

	/* device */
	
	r = -EINVAL;
	if (sscanf(argv[1], "%llu%c", &tmp, &d) != 1) {
		ti->error = "dm-stl: Invalid zone size";
		goto fail;
	}
	sc->zone_size = tmp;

	if (sscanf(argv[2], "%llu%c", &tmp, &d) != 1) {
		ti->error = "dm-stl: Invalid cache start";
		goto fail;
	}
	sc->cache_start = tmp * sc->zone_size;

	if (sscanf(argv[3], "%llu%c", &tmp, &d) != 1) {
		ti->error = "dm-stl: Invalid data start";
		goto fail;
	}
	sc->data_start = tmp * sc->zone_size;

	if (sscanf(argv[4], "%llu%c", &tmp, &d) != 1) {
		ti->error = "dm-stl: Invalid data end";
		goto fail;
	}
	sc->data_end = tmp * sc->zone_size;

	sc->write_frontier = sc->cache_start;
	sc->tail_pba = sc->write_frontier;

	spin_lock_init(&sc->lock);
        init_completion(&sc->init_wait);
        init_waitqueue_head(&sc->write_wait);
        init_waitqueue_head(&sc->gather_wait);
	init_completion(&sc->gather_done);
        init_waitqueue_head(&sc->space_wait);
	
	r = -ENOMEM;
	ti->error = "dm-stl: No memory";

	sc->extent_pool = mempool_create_slab_pool(MIN_EXTENTS, _extent_cache);
	if (!sc->extent_pool)
		goto fail;

        sc->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
        if (!sc->page_pool)
		goto fail;

	sc->io_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
	if (!sc->io_pool)
		goto fail;
	
        sc->bs = bioset_create(32, 0);
	if (!sc->bs)
		goto fail;
	
	sc->rb = RB_ROOT;
	rwlock_init(&sc->rb_lock);
	
	if ((r = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev))) {
		ti->error = "dm-sadc: Device lookup failed.";
		goto fail;
	}

	return 0;

fail:
	if (sc->bs)
		bioset_free(sc->bs);
	if (sc->io_pool)
		mempool_destroy(sc->page_pool);
	if (sc->page_pool)
		mempool_destroy(sc->page_pool);
	if (sc->extent_pool)
		mempool_destroy(sc->extent_pool);
	kfree(sc);
	
	return r;
}

static struct target_type stl_target = {
        .name            = "stl",
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

struct extent *stl_get_tail(struct ctx *sc)
{
	struct extent *e, *highest;
	s64 cachelen = (sc->data_start - sc->cache_start);
	s64 diff = 0, wf = sc->write_frontier;
	unsigned long flags;
	
	read_lock_irqsave(&sc->rb_lock, flags);
	for (e = highest = stl_rb_first(sc); e != NULL; e = stl_rb_next(e)) {
		s64 d2 = (wf - e->pba + cachelen) % cachelen;
		if (d2 > diff) {
			diff = d2;
			highest = e;
		}
	}
	read_unlock_irqrestore(&sc->rb_lock, flags);

	sc->tail_pba = highest ? highest->pba : sc->write_frontier;
	return highest;
}

#if 0
static char *cmds[] = {
	"STL_NO_OP",
	"STL_GET_EXT",
	"STL_PUT_EXT",
	"STL_GET_WF",
	"STL_PUT_WF",
	"STL_GET_TAIL",
	"STL_VAL_TAIL",
	"STL_GET_WC", 
	"STL_VAL_WC", 
	"STL_GET_RC", 
	"STL_VAL_RC", 
	"STL_CMD_START",
	"STL_CMD_GATHER",
	"STL_CMD_REWRITE",
	"STL_END"
};
#endif

static ssize_t stl_proc_read(struct file *filp, char *buf, size_t count, loff_t *offp)
{
	struct stl_msg m = {.cmd = 0, .flags = 0, .lba = 0, .pba = 0, .len = 0};
	ssize_t ocount = count;
	struct extent *e;
	unsigned long flags;
	
	/* reads must be a multiple of the message size */
	if (count % (sizeof(m)) != 0)
		return -EINVAL;

#if 0
	if (_sc->msg.cmd > STL_END)
		printk(KERN_INFO "proc_read: invalid cmd: %d\n", _sc->msg.cmd);
	else
		printk(KERN_INFO "proc_read: %s\n", cmds[_sc->msg.cmd]);
#endif
	
	switch (_sc->msg.cmd) {
	case STL_GET_WF:
		m.cmd = STL_PUT_WF;
		m.pba = _sc->write_frontier;
		if (copy_to_user(buf, &m, sizeof(m)))
			return -EFAULT;
		buf += sizeof(m); count -= sizeof(m);
		break;

	case STL_GET_WC:
		m.cmd = STL_VAL_WC;
		m.lba = _sc->n_writes;
		if (copy_to_user(buf, &m, sizeof(m)))
			return -EFAULT;
		buf += sizeof(m); count -= sizeof(m);
		break;

        case STL_GET_RC:
                m.cmd = STL_VAL_RC;
                m.lba = atomic_read(&_sc->n_reads);
		if (copy_to_user(buf, &m, sizeof(m)))
			return -EFAULT;
		buf += sizeof(m); count -= sizeof(m);
		break;

	case STL_GET_TAIL:
		m.cmd = STL_VAL_TAIL;
		e = stl_get_tail(_sc);
		if (e == NULL) {
			m.pba = _sc->write_frontier;
			m.lba = m.pba;
		} else {
			m.pba = e->pba;
			m.lba = e->lba;
		}
		if (copy_to_user(buf, &m, sizeof(m)))
			return -EFAULT;
		buf += sizeof(m); count -= sizeof(m);
		break;

	case STL_GET_EXT:
		read_lock_irqsave(&_sc->rb_lock, flags); 
		e = _stl_rb_geq(&_sc->rb, _sc->list_lba);
		m.cmd = STL_PUT_EXT;
		while (e != NULL && count > 0) {
			m.lba = e->lba;
			m.pba = e->pba;
			m.len = e->len;
			_sc->list_lba = e->lba + e->len;
			if (copy_to_user(buf, &m, sizeof(m))) {
				read_unlock_irqrestore(&_sc->rb_lock, flags);
				return -EFAULT;
			}
			buf += sizeof(m); count -= sizeof(m);
			e = stl_rb_next(e);
		}
		read_unlock_irqrestore(&_sc->rb_lock, flags); 
		break;
	}

	if (count > 0) {
		m.cmd = STL_END;
		if (copy_to_user(buf, &m, sizeof(m)))
			return -EFAULT;
		buf += sizeof(m); count -= sizeof(m);
		_sc->msg.cmd = STL_NO_OP;
	}

	return ocount - count;
}

void stl_start(struct ctx *sc)
{
        complete_all(&sc->init_wait);
}

static void stl_gather_endio(struct bio *bio)
{
	int i;
	struct bio_vec *bv = NULL;
	struct ctx *sc = bio->bi_private;

	int tmp = atomic_dec_return(&sc->n_gather);
	//printk(KERN_INFO "gather_endio: %d -> %d pba %ld\n", tmp+1, tmp, bio->bi_iter.bi_sector);

	if (bio_data_dir(bio) == WRITE) {
		bio_for_each_segment_all(bv, bio, i) {
			WARN_ON(!bv->bv_page);
			mempool_free(bv->bv_page, sc->page_pool);
			bv->bv_page = NULL;
		}
	}
	bio_put(bio);
	if (tmp <= 0)
		complete(&sc->gather_done);
}

struct bio *alloc_gather_bio(struct ctx *sc, unsigned sectors)
{
        int i, val, remainder, npages;
        struct bio_vec *bv = NULL;
        struct bio *bio;
        struct page *page;

	npages = sectors / 8;
	remainder = (sectors * 512) - npages * PAGE_SIZE;
	
	if (!(bio = bio_alloc_bioset(GFP_NOIO, npages + (remainder > 0), sc->bs))) 
		goto fail;

        for (i = 0; i < npages; i++) {
                if (!(page = mempool_alloc(sc->page_pool, GFP_NOIO)))
                        goto fail;
                val = bio_add_page(bio, page, PAGE_SIZE, 0);
	}

	if (remainder > 0) {
                if (!(page = mempool_alloc(sc->page_pool, GFP_NOIO)))
                        goto fail;
                val = bio_add_page(bio, page, remainder, 0);
	}
        return bio;
        
fail:
	printk(KERN_INFO "alloc gather: FAIL (%d pages + %d)\n", npages, sectors%8);
	if (bio != NULL) {
		bio_for_each_segment_all(bv, bio, i) {
			mempool_free(bv->bv_page, sc->page_pool);
		}
		bio_put(bio);
	}
        return NULL;
}

struct tuple {
	struct tuple *next;
	sector_t lba;
	sector_t pba;
	int len;
	struct bio *bio;
};
static void set_tuple(struct tuple *t, sector_t lba, sector_t pba, int len) {
	t->lba = lba; t->pba = pba; t->len = len;
}
#define add_to_list(head, tail, elmt) do {if ((head) == NULL) {(head) = (tail) = (elmt);} \
		else { (tail)->next = (elmt); (tail) = (elmt); }} while (0)


void add_gap(struct tuple **headp, struct tuple **tailp, sector_t lba, int len, int maxlen)
{
	int offset;
	struct tuple *tmp;
	for (offset = 0; offset < len; offset += maxlen) {
		int _len = min(len-offset, maxlen);
		tmp = kzalloc(sizeof(*tmp), GFP_NOIO);
		BUG_ON(_len == 0);
		set_tuple(tmp, lba + offset, -1, _len);
		add_to_list(*headp, *tailp, tmp);
        }
}

/* get all the gaps in a certain part of the LBA map. Note that we set
 * tuple->pba to -1, and that we split overly long gaps in the add_gap
 * function so that there's one bio submitted per tuple.
 */
struct tuple *get_gaps(struct ctx *sc, sector_t lba_start, sector_t lba_end)
{
	struct tuple *head = NULL, *tail = NULL;
	unsigned long flags;
	struct extent *e;
	sector_t lba = lba_start;
	int maxlen = 2048;

        read_lock_irqsave(&sc->rb_lock, flags);
        e = _stl_rb_geq(&sc->rb, lba);
        while (e != NULL && e->lba <= lba_end) {
		/*   |   [eeeeeeeee]
		     ^lba          ^new lba*/
                if (e->lba > lba) 
			add_gap(&head, &tail, lba, e->lba - lba, maxlen);
		/* else: [eeeeeeeeeeee]
		              ^lba    ^new lba */
                lba = e->lba + e->len;
                e = stl_rb_next(e);
        }
	/*  |        |     [eeeeee]
	    ^lba     ^clean_end   */
        if (lba < lba_end && (e == NULL || e->lba > lba_end))
		add_gap(&head, &tail, lba, (lba_end - lba), maxlen);
        read_unlock_irqrestore(&sc->rb_lock, flags);

	return head;
}

/* get all the extents between lba_start and lba_end, truncating at
 * either end. Returns (lba, pba, len) for each extent.
 */
struct tuple *get_extents_lba(struct ctx *sc, sector_t lba_start, sector_t lba_end)
{
	unsigned long flags;
	struct extent *e;
	struct tuple *head = NULL, *tail = NULL, *tmp;
	
        read_lock_irqsave(&sc->rb_lock, flags);
        e = _stl_rb_geq(&sc->rb, lba_start);

	/* note that the exception cases don't overlap, since they
	 * happen at opposite ends of the interval.
	 */
        while (e != NULL && e->lba < lba_end) {
		/* [eeeeeeeeee]
		       ^lba_start */
		sector_t lba = max(e->lba, lba_start);
		/*  |   [eeeeeeeeee]
		    ^lba_end */
                int len = min((int)e->len, (int)(lba_end - e->lba));
		if (len == 0)
			continue;

		tmp = kzalloc(sizeof(*tmp), GFP_NOIO);
		set_tuple(tmp, lba, e->pba + (lba-e->lba), len);
		BUG_ON(len == 0);
		add_to_list(head, tail, tmp);
                e = stl_rb_next(e);
        }
        read_unlock_irqrestore(&sc->rb_lock, flags);

	return head;
}

/* Note that we're going to have to block writes that modify records
 * in this PBA range, too. That will be a bit of a pain...
 */
struct tuple *get_extents_pba(struct ctx *sc, sector_t pba_start, sector_t pba_end)
{
	unsigned long flags;
	struct extent *e;
	struct tuple *head = NULL, *tail = NULL, *tmp;
	
        read_lock_irqsave(&sc->rb_lock, flags);
	for (e = stl_rb_first(sc); e != NULL; e = stl_rb_next(e)) {
		if (e->pba >= pba_end || e->pba + e->len < pba_start)
			continue;
		BUG_ON(e->len == 0);
		tmp = kzalloc(sizeof(*tmp), GFP_NOIO);
		set_tuple(tmp, e->lba, e->pba, e->len);
		add_to_list(head, tail, tmp);
	}
        read_unlock_irqrestore(&sc->rb_lock, flags);
	return head;
}

void free_tuples(struct ctx *sc, struct tuple *t)
{
	struct tuple *tmp;
	while (t) {
		tmp = t;
		t = t->next;
		kfree(tmp);
	}
}

typedef void (*bio_fn_t)(struct ctx *, struct bio *, int);
void do_generic_request(struct ctx *sc, struct bio *bio, int priority) {
	generic_make_request(bio);
}
void do_split_read(struct ctx *sc, struct bio *bio, int priority) {
	split_read_io(sc, bio);
}

void move_data(struct ctx *sc, struct tuple *list, bio_fn_t read_fn, 
	       int read_pba, bio_fn_t write_fn, int write_pba)
{
	struct tuple *t;
	struct bio *clone;
	
	reinit_completion(&sc->gather_done);
	atomic_set(&sc->n_gather, 0);

	for (t = list; t != 0; t = t->next) {
		t->bio = alloc_gather_bio(sc, t->len);
		clone = bio_clone_bioset(t->bio, GFP_NOIO, sc->bs);
		clone->bi_iter.bi_sector = read_pba ? t->pba : t->lba;
		clone->bi_end_io = stl_gather_endio;
		clone->bi_private = sc;
		clone->bi_opf = READ;
		clone->bi_bdev = sc->dev->bdev;

#if 0
		/* gr lba pba len ptr */
		printk(KERN_INFO "gr %lu %lu %d (%d) %p %p\n", t->lba,
		       clone->bi_iter.bi_sector, bio_sectors(clone), t->len, t->bio, clone);
#endif
		atomic_inc(&sc->n_gather);
		read_fn(sc, clone, 1);
	}
	wait_for_completion(&sc->gather_done);

        atomic_set(&sc->n_gather, 0);
	reinit_completion(&sc->gather_done);

	for (t = list; t != 0; t = t->next) {
                t->bio->bi_iter.bi_sector = write_pba ? t->pba : t->lba;
		t->bio->bi_private = sc;
		t->bio->bi_end_io = stl_gather_endio;
		t->bio->bi_bdev = sc->dev->bdev;
                t->bio->bi_opf = WRITE;
#if 0
		/* gw lba x len ptr */
		printk(KERN_INFO "gw %lu x %d %p\n", t->lba, 
		       bio_sectors(t->bio), t->bio);
#endif
		atomic_inc(&sc->n_gather);
                write_fn(sc, t->bio, 1);
        }
	wait_for_completion(&sc->gather_done);
}

#define OP_LBA 0
#define OP_PBA 1

/* Note that we have to read and re-write both data from the data
 * bands and from the media cache, as otherwise we advance the write
 * frontier by 1 band but in the worst case only move the tail pointer
 * slightly. 
 */
void stl_gather(struct ctx *sc, struct stl_msg *m)
{
        unsigned long flags;
	struct tuple *t, *list;
	sector_t start = m->lba, end = m->pba;
	
	if (end > (sc->data_end - sc->data_start)) {
		printk(KERN_INFO "ERROR gather bound too high: %ld\n", (sector_t)m->pba);
		m->pba = (sc->data_end - sc->data_start);
	}

        spin_lock_irqsave(&sc->lock, flags); /* pause any conflicting writes */
        sc->clean_start = start;
        sc->clean_end = end;
        spin_unlock_irqrestore(&sc->lock, flags);

	list = get_gaps(sc, start, end);
	for (t = list; t != NULL; t = t->next)
		t->pba = sc->data_start + t->lba;
	move_data(sc, list, do_generic_request, OP_PBA, map_write_io, OP_LBA);
	free_tuples(sc, list);

        spin_lock_irqsave(&sc->lock, flags); /* unpause any writes */
        sc->clean_start = -1;
        sc->clean_end = -1;
        wake_up_all(&sc->write_wait);
        spin_unlock_irqrestore(&sc->lock, flags);

	/* I should split this out into a separate command.
	 */
	stl_get_tail(sc);
	sector_t cache_size = sc->data_start - sc->cache_start;
	sector_t min_room = sc->zone_size * 2;
	sector_t room = ((sc->tail_pba-1) - sc->write_frontier + cache_size) % cache_size;
	if (room < min_room) {
		int nsectors = end - start;
		sector_t pstart = sc->tail_pba - (sc->tail_pba % nsectors);
		list = get_extents_pba(sc, pstart, pstart + nsectors);
		move_data(sc, list, do_split_read, OP_LBA, map_write_io, OP_LBA);
		free_tuples(sc, list);
	}
        return;
}


/* these two functions have totally the same structure. Is there any
 * way to factor out the common logic?
 */
void stl_rewrite(struct ctx *sc, struct stl_msg *m)
{
        unsigned long flags;
	struct tuple *t, *list;
	sector_t start = m->lba, end = m->pba;
        
        spin_lock_irqsave(&sc->lock, flags); /* pause any  */
        sc->clean_start = start;
        sc->clean_end = end;
        spin_unlock_irqrestore(&sc->lock, flags);

	list = get_extents_lba(sc, start, end);
	for (t = list; t != NULL; t = t->next)
		t->pba = sc->data_start + t->lba;
	move_data(sc, list, do_split_read, OP_LBA, do_generic_request, OP_PBA);
	for (t = list; t != NULL; t = t->next)
		stl_update_range(sc, t->lba, -1, t->len);
	free_tuples(sc, list);

        spin_lock_irqsave(&sc->lock, flags); /* resume I/Os */
        sc->clean_start = -1;
        sc->clean_end = -1;
        wake_up_all(&sc->write_wait);
        spin_unlock_irqrestore(&sc->lock, flags);

	stl_get_tail(sc);
	wake_up_all(&sc->space_wait);

        return;
}


static ssize_t stl_proc_write(struct file *filp, const char *buf, 
                          size_t count, loff_t *offp)
{
	struct stl_msg m;
	size_t ocount = count;

	/* writes must be a multiple of the message size */
	if (count % (sizeof(m)) != 0)
		return -EINVAL;

	while (count > 0) {
		if (copy_from_user(&m, buf, sizeof(m)))
			return -EFAULT;
		buf += sizeof(m); count -= sizeof(m);

#if 0
		if (m.cmd > STL_END)
			printk(KERN_INFO "proc_read: invalid cmd: %d\n", m.cmd);
		else
			printk(KERN_INFO "proc_write: %s\n", cmds[m.cmd]);
#endif

		switch (m.cmd) {
		case STL_GET_EXT: /* more than 1 before next read will result */
                        _sc->list_lba = 0;
		case STL_GET_WF:  /* in the first ones being dropped */
		case STL_GET_TAIL:
		case STL_GET_WC:
		case STL_GET_RC:
			_sc->msg = m;
			break;
		case STL_PUT_WF: /* only before startup: no locking */
			if (m.pba < _sc->cache_start || m.pba >= _sc->data_start)
				return -EINVAL;
			_sc->write_frontier = m.pba;
			break;
		case STL_PUT_EXT:
			/* maybe validate the data ? */
			stl_update_range(_sc, m.lba, m.pba, m.len);
			break;
		case STL_CMD_START:
			stl_start(_sc);
			break;
		case STL_CMD_GATHER:
			if (m.lba > (_sc->data_end - _sc->data_start))
				return -EINVAL;
			stl_gather(_sc, &m);
			break;
		case STL_CMD_REWRITE:
			if (m.lba > (_sc->data_end - _sc->data_start))
				return -EINVAL;
			stl_rewrite(_sc, &m);
			break;
		case STL_NO_OP:
			break;
		default:
			printk(KERN_INFO "invalid: %d\n", m.cmd);
			return -EINVAL;
		}
	}

	return ocount;
}

static int stl_proc_open(struct inode *inode, struct file *file)
{
	if (_sc != NULL)
		_sc->list_lba = 0;
	return 0;
}

/* -------- not sure if we'll need this one  ------- */
static int stl_proc_release(struct inode *inode, struct file *file)
{
	return 0;
}
/* --------                                ------- */


static const struct file_operations stl_file_ops = {
	.owner   = THIS_MODULE,
	.open    = stl_proc_open,
	.read    = stl_proc_read,
        .write   = stl_proc_write,
	.release = stl_proc_release
};


static int __init dm_stl_init(void)
{
	int r = -ENOMEM;

	if (!(_extent_cache = KMEM_CACHE(extent, 0)))
		goto fail;
	if ((r = dm_register_target(&stl_target)) < 0)
		goto fail;
	proc_create("stl0", 0770, NULL, &stl_file_ops);
	return 0;

fail:
	if (_extent_cache)
		kmem_cache_destroy(_extent_cache);
	return r;
}

static void __exit dm_stl_exit(void)
{
	dm_unregister_target(&stl_target);
	kmem_cache_destroy(_extent_cache);
	remove_proc_entry("stl0", NULL);
}


module_init(dm_stl_init);
module_exit(dm_stl_exit);

MODULE_DESCRIPTION(DM_NAME " simple media cache-based SMR Translation Layer");
MODULE_LICENSE("GPL");
