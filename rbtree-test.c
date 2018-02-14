/*
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

#include <linux/proc_fs.h>
#include <asm/uaccess.h>

#define PROCFS_NAME 		"rbtest"

#undef DMINFO
#define DMINFO(...) printk(KERN_INFO __VA_ARGS__)
#undef DMWARN
#define DMWARN(...) printk(KERN_WARNING __VA_ARGS__)
#undef DMERR
#define DMERR(...) printk(KERN_ERR __VA_ARGS__)

#define mempool_alloc(...) alloc_extent()
#define mempool_free(a, b) free_extent(a)

/* total size = 40 bytes. fits in 1 cache line */
struct extent {
	struct rb_node rb;	/* 20 bytes */
	sector_t lba;		/* 512B LBA */
	sector_t pba;		
	u16   len;
	u16   flags;		
};

#define NODES       100

struct ctx {
	struct rb_root rb;
} _sc = {.rb = RB_ROOT};

static struct extent *alloc_extent(void)
{
	return kmalloc(sizeof(struct extent), GFP_KERNEL);
}

static void free_extent(struct extent *e)
{
	kfree(e);
}

/* find a map entry containing 'lba' or the next higher entry.
 * see Documentation/rbtree.txt
 */
static struct extent *stl_rb_geq(struct rb_root *root, off_t lba)
{
        struct rb_node *node = root->rb_node;  /* top of the tree */
	struct extent *higher = NULL;

	while (node) {
		struct extent *e = container_of(node, struct extent, rb);
		DMINFO(" geq(%lu): %lu", lba, e->lba);
		if (e->lba >= lba && (!higher || e->lba < higher->lba)) {
			DMINFO(" higher=e");
			higher = e;
		}
		if (lba < e->lba) {
			DMINFO(" rb_left");
			node = node->rb_left;
		} else if (lba >= e->lba + e->len) {
			DMINFO(" rb_right");
			node = node->rb_right;
		} else {
			DMINFO(" return");
			return e;
		}
	}
	DMINFO("higher");
	return higher;
}


static void stl_rb_insert(struct rb_root *root, struct extent *new)
{
        struct rb_node **link = &root->rb_node, *parent = NULL;
	struct extent *e;

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
}


static void stl_rb_remove(struct rb_root *root, struct extent *e)
{
	rb_erase(&e->rb, root);
}

static struct extent *stl_rb_first(struct rb_root *root)
{
	struct rb_node *node = rb_first(root);
	return container_of(node, struct extent, rb);
}


static struct extent *stl_rb_next(struct extent *e)
{
	struct rb_node *node = rb_next(&e->rb);
	return container_of(node, struct extent, rb);
}

/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to forward and reverse map.
 */
static struct extent * stl_update_range(struct ctx *sc, sector_t lba, sector_t pba, size_t len)
{
	struct extent *e, *_new = NULL, *_new2 = NULL;

	BUG_ON(len == 0);

	if (pba != -1 && unlikely(!(_new = mempool_alloc(sc->extent_pool, GFP_NOIO))))
		goto fail;
	e = stl_rb_geq(&sc->rb, lba);

	if (e != NULL) {
		/* [----------------------]        e     new     new2
		 *        [++++++]           -> [-----][+++++][--------]
		 */
		if (e->lba < lba && e->lba+e->len > lba+len) {
			off_t new_lba = lba+len;
			size_t new_len = e->lba + e->len - new_lba;
			off_t new_pba = e->pba + (e->len - new_len);

			BUG_ON(new_len <= 0);

			if (!(_new2 = mempool_alloc(sc->extent_pool, GFP_NOIO)))
				goto fail;
			*_new2 = (struct extent){.lba = lba+len, .pba = new_pba,
						.len = new_len, .flags = 0};

			DMINFO("split %lu,+%d -> %lu into ", e->lba, e->len, e->pba);

			e->len = lba - e->lba; /* do this *before* inserting below */
			DMINFO("  %lu,+%d -> %lu %lu,+%d -> %lu", e->lba, e->len,
			       e->pba, _new2->lba, _new2->len, _new2->pba);

			stl_rb_insert(&sc->rb, _new2);
			e = _new2;
		}
		/* [------------]
		 *        [+++++++++]        -> [------][+++++++++]
		 */
		else if (e->lba < lba) {
			DMINFO("overlap tail %lu,+%d -> %lu becomes", e->lba, e->len, e->pba);
			e->len = lba - e->lba;
			if (e->len == 0) {
				DMERR("zero-length extent");
				goto fail;
			}
			DMINFO("   %lu,+%d -> %lu", e->lba, e->len, e->pba);
			e = stl_rb_next(e);
		}
		/*          [------]
		 *   [+++++++++++++++]        -> [+++++++++++++++]
		 */
		while (e != NULL && e->lba+e->len <= lba+len) {
			struct extent *tmp = stl_rb_next(e);
			DMINFO("overwriting %lu,+%d -> %lu",  e->lba, e->len, e->pba);
			stl_rb_remove(&sc->rb, e);
			e = tmp;
		}
		/*          [------]
		 *   [+++++++++]        -> [++++++++++][---]
		 */
		if (e != NULL && lba+len > e->lba) {
			int n = (lba+len) - e->lba;
			DMINFO("overlap head %lu,+%d -> %lu becomes", e->lba, e->len, e->pba);
			e->lba += n;
			e->pba += n;
			e->len -= n;
			DMINFO(" %lu,+%d -> %lu", e->lba, e->len, e->pba);
		}
	}

	/* TRIM indicated by pba = -1 */
	if (pba != -1) {
		*_new = (struct extent){.lba = lba, .pba = pba, .len = len, .flags = 0};
		stl_rb_insert(&sc->rb, _new);
	}
	return _new;
	
fail:
	DMERR("could not allocate extent");
	if (_new)
		mempool_free(_new, sc->extent_pool);
	if (_new2)
		mempool_free(_new2, sc->extent_pool);

	return NULL;
}

static struct extent *last_result;

/* NOTE - don't use 'cat', as this returns the same data for each call to 'read'. 
 */
static ssize_t proc_read(struct file *filp, char *buf, size_t count, loff_t *offp)
{
	char tmp[128];
	struct extent *e = last_result;
	int n;

	if (e)
		snprintf(tmp, sizeof(tmp), "%lu,+%d -> %lu\n", e->lba, e->len, e->pba);
	else
		snprintf(tmp, sizeof(tmp), "NULL\n");

	n = strlen(tmp);
	if (n > count)
		n = count;
	if (copy_to_user(buf, tmp, n))
		return -EFAULT;
	return n;
}

static ssize_t proc_write(struct file *filp, const char *buf, size_t count, loff_t *offp)
{
	char tmp[128];
	int size = count, len, argc;
	char cmd;
	sector_t lba, pba;
	struct extent *e;
	struct rb_root *root = &_sc.rb;

	if (size > sizeof(tmp)-1) 
		size = sizeof(tmp)-1;

	/* write data to the buffer */
	if (copy_from_user(tmp, buf, size)) 
		return -EFAULT;
	tmp[size] = 0;
	
	/* i lba len pba : insert
	 * q lba         : query_geq
	 * r             : remove (after q, f, or n)
	 * f             : first
	 * n             : next (after q, f, or n)
         * u lba len pba : update
	 */
	argc = sscanf(tmp, "%c %lu %d %lu", &cmd, &lba, &len, &pba);

	if (cmd == 'u' && argc == 4) {
		last_result = stl_update_range(&_sc, lba, pba, len);
	} else if (cmd == 'i' && argc == 4) {
		if (!(e = alloc_extent())) {
			DMINFO("rbtest: out of memory");
			return -EIO;
		}
		*e = (struct extent){.lba = lba, .len = len, .pba = pba};
		stl_rb_insert(root, e);
                last_result = e;
		DMINFO("inserted\n");
	} else if (cmd == 'q' && argc == 2) {
		if ((e = last_result = stl_rb_geq(root, lba)))
			DMINFO("%lu,+%d -> %lu\n", e->lba, e->len, e->pba);
		else
			DMINFO("NULL");
	} else if (cmd == 'r' && argc == 1) {
		if (last_result) 
			stl_rb_remove(root, last_result);
		last_result = NULL;
	} else if (cmd == 'f' && argc == 1) {
		last_result = stl_rb_first(root);
	} else if (cmd == 'n' && argc == 1) {
		if (last_result)
			last_result = stl_rb_next(last_result);
	} else {
		DMINFO("invalid command: %d %s\n", cmd, tmp);
	}
	
	return size;
}

static const struct file_operations proc_fops = {
	.read = proc_read,
        .write = proc_write,
};

static int __init rb_test_init(void)
{
	proc_create("rbtest", 0777, NULL, &proc_fops);
	return 0;
}

static void __exit rb_test_exit(void)
{
	remove_proc_entry("rbtest", NULL);
}


module_init(rb_test_init);
module_exit(rb_test_exit);

MODULE_DESCRIPTION("Test RBtree logic for dm-stl");
MODULE_LICENSE("GPL");
