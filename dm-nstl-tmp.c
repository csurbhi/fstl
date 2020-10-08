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
#include <linux/miscdevice.h>

#include <linux/vmstat.h>

#include "nstl-u.h"
#include "stl-wait.h"

#define DM_MSG_PREFIX "stl"


/* used to hold a range to be copied.
*/
struct copy_req {
	struct list_head list;
	sector_t lba;
	sector_t pba;
	int len;
	int flags;
	struct bio *bio;
	struct extent *e;
};
static struct kmem_cache *_copyreq_cache;

/* zone free list entry
*/
struct free_zone {
	struct list_head list;
	sector_t start;
	sector_t end;
};

/* this has grown kind of organically, and needs to be cleaned up.
*/
struct ctx {
	sector_t          zone_size;	/* in 512B LBAs */
	int               max_pba;

	spinlock_t        lock;
	sector_t          write_frontier; /* LBA, protected by lock */
	sector_t          wf_end;
	sector_t          previous;	  /* protected by lock */
	unsigned          sequence;
	struct list_head  free_zones;
	int               n_free_zones;
	sector_t          n_free_sectors;

	struct rb_root    rb;	          /* map RB tree */
	rwlock_t          rb_lock;
	int               n_extents;      /* map size */

	mempool_t        *extent_pool;
	mempool_t        *copyreq_pool;
	mempool_t        *page_pool;
	struct bio_set   *bs;

	struct dm_dev    *dev;

	struct stl_msg    msg;
	sector_t          list_lba;

	struct completion init_wait; /* before START issued */
	wait_queue_head_t cleaning_wait;
	wait_queue_head_t space_wait;
	atomic_t          io_count;
	struct completion move_done;

	atomic_t          n_reads;
	sector_t          target;
	unsigned long     target_seq;
	unsigned          sectors_copied;
	struct list_head  copyreqs;
	atomic_t          pages_alloced;

	char              nodename[32];
	struct miscdevice misc;
};

/* total size = xx bytes (64b). fits in 1 cache line 
   for 32b is xx bytes, fits in ARM cache line */
struct extent {
	struct rb_node rb;	/* 20 bytes */
	sector_t lba;		/* 512B LBA */
	sector_t pba;		
	u32      len;
	atomic_t refs[3];
	atomic_t total_refs;
	unsigned seq;
}; /* xx bytes including padding after 'rb', xx on 32-bit */

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

static sector_t zone_start(struct ctx *sc, sector_t pba) {
	return pba - (pba % sc->zone_size);
}
static sector_t zone_end(struct ctx *sc, sector_t pba) {
	return zone_start(sc, pba) + sc->zone_size - 1;  // homa : added -1
}
static unsigned room_in_zone(struct ctx *sc, sector_t sector) {
	return zone_end(sc, sector) - sector + 1;    // homa: added +1
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

	atomic_inc(&sc->n_reads);
	do {
		sector_t sector = bio->bi_iter.bi_sector;
		struct extent *e = stl_rb_geq(sc, sector);
		unsigned sectors = bio_sectors(bio);

		/* note that beginning of extent is >= start of bio */
		/* [----bio-----] [eeeeeee]  */
		if (e == NULL || e->lba >= sector + sectors)  {
			zero_fill_bio(bio);
			bio_endio(bio);
			return;
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
			split = bio_split(bio, sectors, GFP_NOIO, fs_bio_set);
			bio_chain(split, bio);
			zero_fill_bio(split);
			bio_endio(split);
			continue;
		}

		if (sectors < bio_sectors(bio)) {
			split = bio_split(bio, sectors, GFP_NOIO, fs_bio_set);
			bio_chain(split, bio);
		} else {
			split = bio;
		}

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
		atomic_dec(&sc->pages_alloced);
		bv->bv_page = NULL;
	}
	WARN_ON(i != 1);
	bio_put(bio);
}

/* could be a macro, I guess */
static void setup_bio(struct bio *bio, bio_end_io_t endio, struct block_device *bdev,
		sector_t sector, void *private, int dir)
{
	bio->bi_end_io = endio;
	bio->bi_bdev = bdev;
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

	npages = sectors / 8;
	remainder = (sectors * 512) - npages * PAGE_SIZE;

	if (!(bio = bio_alloc_bioset(GFP_NOIO, npages + (remainder > 0), sc->bs))) 
		goto fail;
	for (i = 0; i < npages; i++) {
		if (!(page = mempool_alloc(sc->page_pool, GFP_NOIO)))
			goto fail;
		atomic_inc(&sc->pages_alloced);
		val = bio_add_page(bio, page, PAGE_SIZE, 0);
		if (ppage != NULL)
			*ppage = page;
	}
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
		bio_for_each_segment_all(bv, bio, i) {
			mempool_free(bv->bv_page, sc->page_pool);
			atomic_dec(&sc->pages_alloced);
		}
		bio_put(bio);
	}
	return NULL;
}


/* create a bio for a journal header (if len=0) or trailer (if len>0).
*/
struct bio *make_header(struct ctx *sc, unsigned seq, sector_t here, sector_t prev,
		unsigned sectors, sector_t lba, sector_t pba, unsigned len)
{
	struct page *page;
	struct bio *bio = stl_alloc_bio(sc, 4, &page);
	struct stl_header *h = NULL;
	sector_t next = here + 4 + sectors;

	if (bio == NULL) {
		printk(KERN_ERR "\n stl_alloc_bio for 4 sectors failed");
		return NULL;
	}

	/* min space for an extent at the end of a zone is 8K - wrap if less.
	*/
//	if (sc->zone_size - (next % sc->zone_size) < 16) {
//		next = next + 16;
//		next = next - (next % sc->zone_size);
//	}

	h = page_address(page);
	*h = (struct stl_header){.magic = STL_HDR_MAGIC, .nonce = 0, .crc32 = 0, .flags = 0,
		.prev_pba = prev, .next_pba = next /*here+4+sectors*/, .lba = lba,
		.pba = pba, .len = len, .seq = seq};
	get_random_bytes(&h->nonce, sizeof(h->nonce));
	h->crc32 = crc32_le(0, (u8 *)h, STL_HDR_SIZE);
	bio_add_page(bio, page, STL_HDR_SIZE, 0);
	setup_bio(bio, stl_endio, sc->dev->bdev, here, sc, WRITE);

	return bio;
}


struct bio *make_trailer(struct ctx *sc, unsigned seq, sector_t here, sector_t prev,
		unsigned sectors, sector_t lba, sector_t pba, unsigned len, sector_t next_head)
{
	struct page *page;
	struct bio *bio = stl_alloc_bio(sc, 4, &page);
	struct stl_header *h = NULL;
//	sector_t next = here + 4;

	if (bio == NULL)
		return NULL;

	/* min space for an extent at the end of a zone is 8K - wrap if less.
	 *          */
//	if (sc->zone_size - (next % sc->zone_size) < 16) {
//		next = next + 16;
//		next = next - (next % sc->zone_size);
//	}

	h = page_address(page);
	*h = (struct stl_header){.magic = STL_HDR_MAGIC, .nonce = 0, .crc32 = 0, .flags = 0,
		.prev_pba = prev, .next_pba = next_head/*here+4+sectors*/, .lba = lba,
		.pba = pba, .len = len, .seq = seq};
	get_random_bytes(&h->nonce, sizeof(h->nonce));
	h->crc32 = crc32_le(0, (u8 *)h, STL_HDR_SIZE);
	bio_add_page(bio, page, STL_HDR_SIZE, 0);
	setup_bio(bio, stl_endio, sc->dev->bdev, here, sc, WRITE);

	return bio;
}





/* moves the write frontier, returns the LBA of the packet trailer
*/
static int get_new_zone(struct ctx *sc)
{
	sector_t old_free = sc->n_free_sectors;
	struct free_zone *fz = list_first_entry_or_null(&sc->free_zones,
			struct free_zone, list);
	if (fz == NULL) {
		printk(KERN_INFO "no free zones: space %ld\n", sc->n_free_sectors);
		return 0;
	}

	list_del(&fz->list);
	sc->n_free_zones--;
	if (sc->write_frontier > sc->wf_end)
		printk(KERN_INFO "kernel wf before BUG: %ld - %ld\n", sc->write_frontier, sc->wf_end);
	BUG_ON(sc->write_frontier > sc->wf_end);
//	printk(KERN_INFO "Num of free sect.: %ld, diff of end and wf:%ld\n", sc->n_free_sectors, sc->wf_end - sc->write_frontier);
	sc->n_free_sectors -= (sc->wf_end - sc->write_frontier);
	sc->write_frontier = fz->start;
	sc->wf_end = zone_end(sc, sc->write_frontier);

	printk(KERN_INFO "new zone: %d (%ld->%ld) left %d\n", (int)(fz->start / sc->zone_size),
			old_free, sc->n_free_sectors, sc->n_free_zones);

	kfree(fz);
	return sc->write_frontier;
}


static sector_t move_write_frontier(struct ctx *sc, sector_t sectors_s8, sector_t* next_header)
{
	sector_t prev; 

	prev = sc->write_frontier + sectors_s8 -4;
	if (room_in_zone(sc, sc->write_frontier + sectors_s8 -1 ) - 1 < 16 ){ // We do "-1 ) + -1" because sc->write_frontier + sectors_s8 can move to the next zone in some scenarios
		if (!(sc->write_frontier = get_new_zone(sc))){
			printk(KERN_INFO "fail due to get_new_zone at line 626");
			return -1;
	//		goto fail;
		}
	} else {
		sc->write_frontier += sectors_s8;
		sc->n_free_sectors -= (sectors_s8);
	}

	*next_header = sc->write_frontier;
	sc->wf_end = zone_end(sc, sc->write_frontier); //homa
	return prev;
}

static atomic_t c1, c2, c3, c4, c5;

/* split a write into one or more journalled packets
*/
static void map_write_io(struct ctx *sc, struct bio *bio, int priority)
{
	struct bio *split = NULL, *pad = NULL;
	struct bio *bios[7];
	int i, nbios = 0;
	unsigned long flags, t1, t2;
	unsigned sectors = bio_sectors(bio);
	sector_t s8, sector = bio->bi_iter.bi_sector;
	struct extent *e;
	sector_t next_header;

	/* wait until there's room
	*/
	atomic_inc(&c1);
	spin_lock_irqsave(&sc->lock, flags);
	t1 = jiffies;
	atomic_inc(&c2);
	wait_event_lock_irqsave(sc->space_wait,
			priority || sc->n_free_sectors >= sc->zone_size,
			sc->lock, flags);
	t2 = jiffies;
	atomic_inc(&c3);
	spin_unlock_irqrestore(&sc->lock, flags);
	if (t2-t1 > HZ / 10)
		printk(KERN_INFO "freespace stall: %u ms\n", jiffies_to_msecs(t2-t1));

	do {
		unsigned seq;
		sector_t _pba, wf, prev;

		sectors = bio_sectors(bio);
		s8 = round_up(sectors, 8);
		sector = bio->bi_iter.bi_sector;

		spin_lock_irqsave(&sc->lock, flags);
		wf = sc->write_frontier;

		if (s8 + 8 > room_in_zone(sc, wf)){
			s8 = sectors = round_down(room_in_zone(sc, wf) - 8, 8);
		}

		seq = sc->sequence++;
		prev = sc->previous;
		sc->previous = move_write_frontier(sc, s8+8, &next_header);
		if (sc->previous == -1)
		       goto fail;	
		spin_unlock_irqrestore(&sc->lock, flags);

		if (!(bios[nbios++] = make_header(sc, seq, wf, prev, s8, 0, 0, 0))){
			goto fail;
		}
		prev = wf;
		wf += 4;

		if (sectors < bio_sectors(bio)) {
			if (!(split = bio_split(bio, sectors, GFP_NOIO, sc->bs))){
				goto fail;
			}
			bio_chain(split, bio);
			bios[nbios++] = split;
		} else {
			bios[nbios++] = bio;
			split = bio;
		}

		/* if we try to touch an extent that is being cleaned,
		 * stl_update_range will return that extent, and we can
		 * wait and try again.
		 */
		t1 = jiffies;
again:
		e = stl_update_range(sc, sector, wf, sectors);
		if (e != NULL) {
			extent_get(e, 1, STALLED_WRITE);
			wait_event(sc->cleaning_wait,
					!extent_busy(e));
			extent_put(sc, e, 1, STALLED_WRITE);
			goto again;
		}
		t2 = jiffies;
		if (t2 - t1 > HZ/10) 
			printk(KERN_INFO "GC stall: %u ms\n", jiffies_to_msecs(t2-t1));

		split->bi_iter.bi_sector = wf;
		_pba = wf;
		wf += sectors;

		/* pad length is in sectors (like arg to alloc_bio)
		*/
		if (sectors != s8) {
			int padlen = 8 - (bio->bi_iter.bi_size & ~PAGE_MASK)/512;
			if (!(pad = stl_alloc_bio(sc, padlen, NULL))){
				goto fail;
			}
			setup_bio(pad, stl_endio, sc->dev->bdev, wf, sc, WRITE);
			wf += padlen;
			bios[nbios++] = pad;
		}
		
		if (!(bios[nbios++] = make_trailer(sc, seq, wf, prev, 0, sector, _pba, sectors, next_header))){
			goto fail;

		}
		wf += 4;

	} while (split != bio);

	for (i = 0; i < nbios; i++) {
		bios[i]->bi_bdev = sc->dev->bdev;
		generic_make_request(bios[i]); /* preserves ordering, unlike submit_bio */
	}
	atomic_inc(&c4);
	return;

fail:
	printk(KERN_INFO "FAIL!!!!\n");
	bio->bi_error = -ENOMEM;
	for (i = 0; i < nbios; i++)
		if (bios[i] && bios[i]->bi_private == sc)
			stl_endio(bio);
	atomic_inc(&c5);
}


static int stl_map(struct dm_target *ti, struct bio *bio)
{
	struct ctx *sc = ti->private;

	switch (bio_op(bio)) {
		case REQ_OP_DISCARD:
		case REQ_OP_FLUSH:	
			//		case REQ_OP_SECURE_ERASE:
			//		case REQ_OP_WRITE_ZEROES:
			printk(KERN_INFO "Discard or Flush: %d \n", bio_op(bio));
			WARN_ON(bio_sectors(bio));
			bio->bi_bdev = sc->dev->bdev;
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


static void stl_dtr(struct dm_target *ti)
{
	struct ctx *sc = ti->private;
	ti->private = NULL;

	misc_deregister(&sc->misc);
	mempool_destroy(sc->extent_pool);
	mempool_destroy(sc->copyreq_pool);
	mempool_destroy(sc->page_pool);
	bioset_free(sc->bs);
	dm_put_device(ti, sc->dev);
	kfree(sc);
}

static struct ctx *_sc;
static const struct file_operations stl_misc_fops;

/*
   argv[0] = devname
   argv[1] = dm-name
   argv[2] = zone size (LBAs)
   argv[3] = max pba
   */
static int stl_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	int r = -ENOMEM;
	struct ctx *sc;
	unsigned long long tmp, max_pba;
	char d;

	DMINFO("ctr %s %s %s %s", argv[0], argv[1], argv[2], argv[3]);

	if (argc != 4) {
		ti->error = "dm-stl: Invalid argument count";
		return -EINVAL;
	}

	if (!(_sc = sc = kzalloc(sizeof(*sc), GFP_KERNEL)))
		goto fail;
	ti->private = sc;

	if ((r = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev))) {
		ti->error = "dm-nstl: Device lookup failed.";
		goto fail;
	}
	max_pba = sc->dev->bdev->bd_inode->i_size / 512;

	sprintf(sc->nodename, "stl/%s", argv[1]);

	r = -EINVAL;
	if (sscanf(argv[2], "%llu%c", &tmp, &d) != 1) {
		ti->error = "dm-stl: Invalid zone size";
		goto fail;
	}
	sc->zone_size = tmp;

	if (sscanf(argv[3], "%llu%c", &tmp, &d) != 1 || tmp > max_pba) {
		ti->error = "dm-stl: Invalid max pba";
		goto fail;
	}
	sc->max_pba = tmp;

	sc->write_frontier = 0;
	printk(KERN_INFO "kernel wf: %ld\n", sc->write_frontier);
	sc->wf_end = zone_end(sc, sc->write_frontier); // homa

	spin_lock_init(&sc->lock);
	init_completion(&sc->init_wait);
	init_waitqueue_head(&sc->cleaning_wait);
	init_completion(&sc->move_done);
	init_waitqueue_head(&sc->space_wait);

	INIT_LIST_HEAD(&sc->free_zones);
	sc->n_free_zones = 0;
	INIT_LIST_HEAD(&sc->copyreqs);

	r = -ENOMEM;
	ti->error = "dm-stl: No memory";

	sc->extent_pool = mempool_create_slab_pool(MIN_EXTENTS, _extent_cache);
	if (!sc->extent_pool)
		goto fail;
	sc->copyreq_pool = mempool_create_slab_pool(MIN_COPY_REQS, _copyreq_cache);
	if (!sc->copyreq_pool)
		goto fail;
	sc->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
	if (!sc->page_pool)
		goto fail;

	sc->bs = bioset_create(32, 0);
	if (!sc->bs)
		goto fail;

	sc->rb = RB_ROOT;
	rwlock_init(&sc->rb_lock);

	ti->error = "dm-stl: misc_register failed";
	sc->misc.minor = MISC_DYNAMIC_MINOR;
	sc->misc.name = "dm-nstl";
	sc->misc.nodename = sc->nodename;
	sc->misc.fops = &stl_misc_fops;

	if (misc_register(&sc->misc))
		goto fail;

	return 0;

fail:
	if (sc->bs)
		bioset_free(sc->bs);
	if (sc->copyreq_pool)
		mempool_destroy(sc->copyreq_pool);
	if (sc->page_pool)
		mempool_destroy(sc->page_pool);
	if (sc->extent_pool)
		mempool_destroy(sc->extent_pool);
	kfree(sc);

	return r;
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


static ssize_t stl_proc_read(struct file *filp, char *buf, size_t count, loff_t *offp)
{
	struct stl_msg m = {.cmd = 0, .flags = 0, .lba = 0, .pba = 0, .len = 0};
	ssize_t ocount = count;
	struct extent *e;
	unsigned long flags;
	struct list_head *p;

	/* reads must be a multiple of the message size */
	if (count % (sizeof(m)) != 0) {
		printk(KERN_INFO "invalid read count %ld\n", count);
		return -EINVAL;
	}

	switch (_sc->msg.cmd) {
		case STL_GET_WF:
			m.cmd = STL_PUT_WF;
			spin_lock_irqsave(&_sc->lock, flags); //homa
			m.pba = _sc->write_frontier;
			spin_unlock_irqrestore(&_sc->lock, flags); //homa
			if (copy_to_user(buf, &m, sizeof(m)))
				return -EFAULT;
			buf += sizeof(m);
			count -= sizeof(m);
			break;

		case STL_GET_SEQ:
			m.cmd = STL_PUT_SEQ;
			m.lba = _sc->sequence;
			if (copy_to_user(buf, &m, sizeof(m)))
				return -EFAULT;
			buf += sizeof(m);
			count -= sizeof(m);
			break;

		case STL_GET_READS:
			m.cmd = STL_VAL_READS;
			m.lba = atomic_read(&_sc->n_reads);
			if (copy_to_user(buf, &m, sizeof(m)))
				return -EFAULT;
			buf += sizeof(m);
			count -= sizeof(m);
			break;

		case STL_GET_SPACE:
			m.cmd = STL_PUT_SPACE;
			spin_lock_irqsave(&_sc->lock, flags); //homa
			m.lba = _sc->n_free_sectors;
			m.pba = _sc->n_free_zones;
			spin_unlock_irqrestore(&_sc->lock, flags); //homa
			if (copy_to_user(buf, &m, sizeof(m)))
				return -EFAULT;
			buf += sizeof(m);
			count -= sizeof(m);
			break;

		case STL_CMD_DOIT:
			m.cmd = STL_VAL_COPIED;
			m.lba = _sc->sectors_copied;
			m.pba = _sc->target;
			if (copy_to_user(buf, &m, sizeof(m)))
				return -EFAULT;
			buf += sizeof(m);
			count -= sizeof(m);
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

		case STL_GET_FREEZONE:
			spin_lock_irqsave(&_sc->lock, flags);
			list_for_each(p, &_sc->free_zones) {
				struct free_zone *fz = list_entry(p, struct free_zone, list);
				m.cmd = STL_PUT_FREEZONE;
				m.lba = fz->start;
				m.pba = fz->end;
				if (count <= 0)
					break;
				if (copy_to_user(buf, &m, sizeof(m))) {
					spin_unlock_irqrestore(&_sc->lock, flags); // this unlock had been forgotten
					return -EFAULT;
				}
				buf += sizeof(m); count -= sizeof(m);
			}
			spin_unlock_irqrestore(&_sc->lock, flags);
			break;

		default:
			printk(KERN_INFO "invalid cmd: %d\n", _sc->msg.cmd);
			return -EINVAL;
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



static void stl_move_endio(struct bio *bio)
{
	int i;
	struct bio_vec *bv = NULL;
	struct ctx *sc = bio->bi_private;

	if (bio_data_dir(bio) == WRITE) {
		bio_for_each_segment_all(bv, bio, i) {
			WARN_ON(!bv->bv_page);
			mempool_free(bv->bv_page, sc->page_pool);
			atomic_dec(&sc->pages_alloced);
			bv->bv_page = NULL;
		}
	}
	bio_put(bio);
	if (atomic_dec_and_test(&sc->io_count))
		complete(&sc->move_done);
}




/* store a request to copy data for cleaning. If it overlaps with an
 * extent, it takes a reference on that extent and marks it as being
 * cleaned so that writes modifying that extent will be stalled.
 */
static void stl_add_copy_cmd(struct ctx *sc, struct stl_msg *m)
{
	struct extent *e = NULL;
	unsigned long flags;
	struct copy_req *cr;
	int offset, maxlen = 2048; /* 2K sectors = 1MB */

	/* we rely on user space to send requests that don't cross
	 * multiple extents, but we may have to split requests into
	 * multiple bios
	 */
	int nbios = round_up((int)m->len, maxlen) / maxlen;

	read_lock_irqsave(&sc->rb_lock, flags);
	e = _stl_rb_geq(&sc->rb, m->lba);
	if (e && e->lba < m->lba + m->len && e->seq <= sc->target_seq) 
		extent_get(e, nbios, IN_CLEANING);
	else
		e = NULL;		
	read_unlock_irqrestore(&sc->rb_lock, flags);

	/* note that you can't move data without the LBA */
	if (!e)
		return;

	for (offset = 0; offset < m->len; offset += maxlen) {
		int len = min(m->len - offset, maxlen);
		cr = mempool_alloc(sc->copyreq_pool, GFP_NOIO);
		if (cr == NULL) {
			int remaining = round_up(m->len - offset, maxlen) / maxlen;
			extent_put(sc, e, remaining, IN_CLEANING);
			break;
		}
		memset(cr, 0, sizeof(*cr));
		*cr = (struct copy_req){.lba = m->lba + offset, .pba = m->pba + offset,
			.len = len, .flags = m->flags, .e = e};
		list_add_tail(&cr->list, &sc->copyreqs);
	}
}

int cmp_req_pba(const void *r1, const void *r2)
{
	struct copy_req *cr1 = *(struct copy_req**)r1;
	struct copy_req *cr2 = *(struct copy_req**)r2;
	return (cr1->e->pba < cr2->e->pba) ? -1 : (cr1->e->pba > cr2->e->pba);
}

/*
   global_page_state(NR_SLAB_RECLAIMABLE) +
   global_page_state(NR_SLAB_UNRECLAIMABLE)
   */

static void stl_do_data_move(struct ctx *sc)
{
	struct list_head *p, *n;
	struct copy_req *cr;
	struct bio *bio, *clone;
	sector_t sector; 
	struct copy_req **reqs;
	int i, m;

	sc->sectors_copied = 0;
	if (list_empty(&sc->copyreqs))
		goto done;

	/* read everything...
	 * TODO - read should be sorted in PBA order
	 */
	reinit_completion(&sc->move_done);
	atomic_set(&sc->io_count, 0);

	m = 0;
	list_for_each(p, &sc->copyreqs) {
		m++;
	}

	i = 0;
	reqs = kmalloc(m*sizeof(*reqs), GFP_NOIO);
	list_for_each(p, &sc->copyreqs) {
		reqs[i++] = list_entry(p, struct copy_req, list);
	}
	sort(reqs, m, sizeof(*reqs), cmp_req_pba, 0);
	for (i = 0; i < m; i++) {
		cr = reqs[i];
		cr->bio = stl_alloc_bio(sc, cr->len, NULL);
		clone = bio_clone_bioset(cr->bio, GFP_NOIO, sc->bs);
		sector = cr->e->pba + (cr->lba - cr->e->lba);
		setup_bio(clone, stl_move_endio, sc->dev->bdev, sector, sc, READ);
		atomic_inc(&sc->io_count);
		generic_make_request(clone);
	}
	kfree(reqs);
#if 0
	list_for_each(p, &sc->copyreqs) {
		cr = list_entry(p, struct copy_req, list);
		cr->bio = stl_alloc_bio(sc, cr->len, NULL);
		clone = bio_clone_bioset(cr->bio, GFP_NOIO, sc->bs);
		sector = cr->e->pba + (cr->lba - cr->e->lba);
		setup_bio(clone, stl_move_endio, sc->dev->bdev, sector, sc, READ);
		atomic_inc(&sc->io_count);
		generic_make_request(clone);
	}
#endif
	wait_for_completion(&sc->move_done);

	/* write it back
	*/
	atomic_set(&sc->io_count, 0);
	reinit_completion(&sc->move_done);
	list_for_each(p, &sc->copyreqs) {
		cr = list_entry(p, struct copy_req, list);
		bio = cr->bio;
		cr->pba = sector = sc->target;
		setup_bio(bio, stl_move_endio, sc->dev->bdev, sector, sc, WRITE);
		atomic_inc(&sc->io_count);
		generic_make_request(bio);
		sc->sectors_copied += cr->len;
		sc->target += cr->len;
	}
	wait_for_completion(&sc->move_done);

	/* release refs on the old extents before we update the map
	*/
	list_for_each_safe(p, n, &sc->copyreqs) {
		cr = list_entry(p, struct copy_req, list);
		extent_put(sc, cr->e, 1, IN_CLEANING);
	}
	list_for_each_safe(p, n, &sc->copyreqs) {
		cr = list_entry(p, struct copy_req, list);
		stl_update_range(sc, cr->lba, cr->pba, cr->len);
		list_del(&cr->list);
		mempool_free(cr, sc->copyreq_pool);
	}

	/* notify blocked writes that cleaning is done.
	*/
done:
	wake_up_all(&sc->cleaning_wait);
}

void put_free_zone(struct ctx *sc, struct stl_msg *m)
{
	unsigned long flags;
	struct free_zone *fz = kzalloc(sizeof(*fz), GFP_KERNEL);

	printk(KERN_INFO "put free zone %d %ld (wf %ld ns %ld)\n",
			(int)(m->lba / sc->zone_size), (long)m->lba, sc->write_frontier, sc->n_free_sectors);
	if (fz == NULL)
		return;
	fz->start = m->lba;
	fz->end = m->pba;

	spin_lock_irqsave(&sc->lock, flags);
	list_add_tail(&fz->list, &sc->free_zones);
	sc->n_free_zones++;
	spin_unlock_irqrestore(&sc->lock, flags);
}

static ssize_t stl_proc_write(struct file *filp, const char *buf, 
		size_t count, loff_t *offp)
{
	struct stl_msg m;
	size_t ocount = count;
	unsigned long flags;

	/* writes must be a multiple of the message size */
	if (count % (sizeof(m)) != 0) {
		printk(KERN_INFO "invalid count %ld\n", count);
		return -EINVAL;
	}

	while (count > 0) {
		if (copy_from_user(&m, buf, sizeof(m)))
			return -EFAULT;

		switch (m.cmd) {
			case STL_GET_EXT: /* more than 1 before next read will result */
				_sc->list_lba = 0;
			case STL_GET_WF:  /* in the first ones being dropped */
			case STL_GET_SEQ:
			case STL_GET_READS:
			case STL_GET_FREEZONE:
			case STL_GET_SPACE:
				_sc->msg = m;
				break;
			case STL_PUT_WF: /* only before startup: no locking */
				spin_lock_irqsave(&_sc->lock, flags);
				_sc->write_frontier = m.pba;
				printk(KERN_INFO "PUT_WF %ld\n", (long)m.pba);
				_sc->wf_end = zone_end(_sc, m.pba);
				spin_unlock_irqrestore(&_sc->lock, flags);
				break;
			case STL_PUT_EXT:
				/* maybe validate the data ? */
				stl_update_range(_sc, m.lba, m.pba, m.len);
				break;
			case STL_PUT_FREEZONE:
				put_free_zone(_sc, &m);
				break;
			case STL_PUT_SPACE:
				printk(KERN_INFO "put_space: %ld (%ld)\n", (long)m.lba, _sc->n_free_sectors);
				spin_lock_irqsave(&_sc->lock, flags);
				_sc->n_free_sectors += m.lba;
				wake_up_all(&_sc->space_wait);
				spin_unlock_irqrestore(&_sc->lock, flags);
				break;
			case STL_CMD_START:
				printk(KERN_INFO "start: wf=%ld\n", _sc->write_frontier);
				complete_all(&_sc->init_wait);
				break;
			case STL_PUT_TGT:
				_sc->target = m.pba;
				_sc->target_seq = m.lba;
				break;
			case STL_PUT_COPY:
				stl_add_copy_cmd(_sc, &m);
				break;
			case STL_CMD_DOIT:
				_sc->msg = m;
				stl_do_data_move(_sc);
				break;
			case STL_NO_OP:
			case STL_END:
				break;
			default:
				printk(KERN_INFO "invalid: %d\n", m.cmd);
				return -EINVAL;
		}
		buf += sizeof(m); count -= sizeof(m);
	}
	return ocount;
}

static int stl_proc_open(struct inode *inode, struct file *file)
{
	if (_sc != NULL)
		_sc->list_lba = 0;
	printk(KERN_INFO "OPEN\n");

	return 0;
}

static long stl_dev_ioctl(struct file *fp, unsigned int num, unsigned long arg)
{
	printk(KERN_INFO "ioctl %d\n", num);
	switch (num) {
		case 1:
			printk(KERN_INFO "ioctl 1\n");
			break;
		case 2:
			printk(KERN_INFO "ioctl 2\n");
			break;
		case 3:
			printk(KERN_INFO "ioctl 3\n");
			break;
		default:
			break;
	}

	return 0;
}

static const struct file_operations stl_misc_fops = {
	.owner          = THIS_MODULE,
	.open           = stl_proc_open,
	.read           = stl_proc_read,
	.write          = stl_proc_write,
	.unlocked_ioctl = stl_dev_ioctl,
};

static int __init dm_stl_init(void)
{
	int r = -ENOMEM;

	printk(KERN_INFO "dm-nstl\n");
	if (!(_extent_cache = KMEM_CACHE(extent, 0)))
		goto fail;
	if (!(_copyreq_cache = KMEM_CACHE(copy_req, 0)))
		goto fail;
	if ((r = dm_register_target(&stl_target)) < 0)
		goto fail;
	return 0;

fail:
	if (_extent_cache)
		kmem_cache_destroy(_extent_cache);
	if (_copyreq_cache)
		kmem_cache_destroy(_extent_cache);
	return r;
}


static void __exit dm_stl_exit(void)
{
	dm_unregister_target(&stl_target);
	kmem_cache_destroy(_extent_cache);
}


module_init(dm_stl_init);
module_exit(dm_stl_exit);

MODULE_DESCRIPTION(DM_NAME " generic user-space-controlled SMR Translation Layer");
MODULE_LICENSE("GPL");
