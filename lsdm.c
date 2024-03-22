/*
 *  Copyright (C) 2016 Peter Desnoyers and 2020 Surbhi Palande.
 *
 * This file is released under the GPL
 *
 * Note: 
 * bio uses length in terms of 512 byte sectors.
 * LBA on SMR drives is in terms of 512 byte or 4096 bytes sectors.
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
#include <linux/vmstat.h>
#include <linux/mm.h>
#include <linux/debug_locks.h>
#include <linux/mutex.h>
#include <linux/lockdep.h>
#include <linux/spinlock.h>
#include <linux/rwlock.h>
#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/blk_types.h>
#include <linux/blkdev.h>
#include <linux/refcount.h>
#include <linux/debugfs.h>

#include "metadata.h"
#define DM_MSG_PREFIX "lsdm"
#define BIO_MAX_PAGES 256
#define BLK_SZ 4096

#undef LSDM_DEBUG

void print_sub_tree(struct rb_node *parent);
void print_revmap_tree(struct ctx *ctx);
struct rev_extent * lsdm_revmap_find_print(struct ctx *ctx, u64 pba, size_t len, u64 last_pba);
struct rev_extent * lsdm_rb_revmap_find(struct ctx *ctx, u64 pba, size_t len, u64 last_pba, const char * caller);
struct rev_extent * lsdm_rb_revmap_insert(struct ctx *ctx, struct extent *extent);
int lsdm_flush_thread_start(struct ctx * ctx);
int lsdm_flush_thread_stop(struct ctx *ctx);
void read_gcextent_done(struct bio * bio);
int complete_revmap_blk_flush(struct ctx * ctx, struct page *page);
int verify_gc_zone(struct ctx *ctx, int zonenr, sector_t pba);
void print_memory_usage(struct ctx *ctx, const char *action);
int create_gc_extents(struct ctx *ctx, int zonenr);
void print_sub_extents(struct rb_node *parent);
int lsdm_gc_thread_start(struct ctx *ctx);
int lsdm_gc_thread_stop(struct ctx *ctx);
void invoke_gc(unsigned long ptr);
void no_op(struct kref *kref);
void complete_small_reads(struct bio *clone);
struct bio * construct_smaller_bios(struct ctx * ctx, sector_t pba, struct app_read_ctx * readctx);
void request_start_unaligned(struct ctx *ctx, struct bio *clone, struct app_read_ctx *read_ctx, sector_t pba, sector_t zerolen);
int zero_fill_inital_bio(struct ctx *ctx, struct bio *bio, struct bio *clone, sector_t zerolen, struct app_read_ctx *read_ctx);
int handle_partial_overlap(struct ctx *ctx, struct bio * bio, struct bio *clone, sector_t overlap, struct app_read_ctx *read_ctx, sector_t pba);
int handle_full_overlap(struct ctx *ctx, struct bio * bio, struct bio *clone, sector_t nr_sectors, sector_t pba, struct app_read_ctx *read_ctx, int print);
void remove_partial_entries(struct ctx *ctx, struct bio * bio);
int is_disk_full(struct ctx *ctx);
struct sit_page * search_sit_kv_store(struct ctx *ctx, sector_t pba, struct rb_node **parent);
struct sit_page * search_sit_blk(struct ctx *ctx, sector_t blknr);
void mark_zone_erroneous(struct ctx *ctx, sector_t pba);
void get_byte_string(char byte, char *str);
void flush_revmap_bitmap(struct ctx *ctx);
void flush_checkpoint(struct ctx *ctx);
void flush_sit_nodes(struct ctx *ctx, struct rb_node *node);
void remove_gc_cost_nodes(struct ctx *ctx);
void remove_gc_zone_nodes(struct ctx *ctx);
u32 calculate_crc(struct ctx *ctx, struct page *page);
void update_checkpoint(struct ctx *ctx);
void move_write_frontier(struct ctx *ctx, sector_t s8);
struct page * read_tm_page(struct ctx * ctx, u64 lba);
void free_translation_pages(struct ctx *ctx);
void remove_sit_page(struct ctx *ctx, struct rb_node *node);
void mark_revmap_bit(struct ctx *ctx, u64 pba);
void clear_revmap_bit(struct ctx *ctx, u64 pba);
void revmap_blk_flushed(struct bio *bio);
int flush_revmap_block_disk(struct ctx * ctx, struct page *page, sector_t revmap_pba);
void shrink_next_entries(struct ctx *ctx, sector_t lba, sector_t pba, unsigned long len, struct page *page);
int merge_rev_entries(struct ctx * ctx, sector_t lba, sector_t pba, unsigned long len, struct page *page);
void write_done(struct kref *kref);
void sub_write_done(struct work_struct * w);
void sub_write_err(struct work_struct * w);
void lsdm_clone_endio(struct bio * clone);
int lsdm_write_checks(struct ctx *ctx, struct bio *bio);
void fill_bio(struct bio *bio, sector_t pba, sector_t len, struct block_device *bdev, struct lsdm_sub_bioctx * subbio_ctx);
void fill_subbioctx(struct lsdm_sub_bioctx * subbio_ctx, struct lsdm_bioctx *bioctx, sector_t lba, sector_t pba, sector_t len);
int prepare_bio(struct bio * clone, sector_t s8, sector_t wf);
struct bio * split_submit(struct bio *clone, sector_t s8, sector_t wf);
int submit_bio_write(struct ctx *ctx, struct bio *clone);
void lsdm_handle_write(struct ctx *ctx);
int lsdm_write_io(struct ctx *ctx, struct bio *bio);
void put_free_zone(struct ctx *ctx, u64 pba);
struct lsdm_ckpt * read_checkpoint(struct ctx *ctx, unsigned long pba);
int do_recovery(struct ctx *ctx);
struct lsdm_ckpt * get_cur_checkpoint(struct ctx *ctx);
int add_tm_page_kv_store_by_blknr(struct ctx *ctx, struct page *page, int blknr);
int read_revmap_bitmap(struct ctx *ctx);
void process_revmap_entries_on_boot(struct ctx *ctx, struct page *page);
int read_revmap(struct ctx *ctx);
sector_t get_zone_pba(struct lsdm_sb * sb, unsigned int segnr);
void lsdm_subread_done(struct bio *clone);
sector_t get_zone_end(struct lsdm_sb *sb, sector_t pba_start);
int allocate_freebitmap(struct ctx *ctx);
unsigned int get_cb_cost(struct ctx *ctx , u32 nrblks, u64 mtime);
unsigned int get_cost(struct ctx *ctx, u32 nrblks, u64 age, char gc_mode);
struct gc_zone_node * add_zonenr_gc_zone_tree(struct ctx *ctx, unsigned int zonenr, u32 nrblks);
int add_sit_page_kv_store_by_blknr(struct ctx *ctx, struct page *page, sector_t sector_nr);
int read_seg_info_table(struct ctx *ctx);
struct lsdm_sb * read_superblock(struct ctx *ctx, unsigned long pba);
int read_metadata(struct ctx * ctx);
static void add_revmap_entry(struct ctx * ctx, __le64 lba, __le64 pba, int nrsectors);
int lsdm_map_io(struct dm_target *dm_target, struct bio *bio);
int __init ls_dm_init(void);
void __exit ls_dm_exit(void);

long nrpages;

struct dentry * debug_dir;

void lsdm_ioidle(struct mykref *kref);

static inline void mykref_init(struct mykref *kref)
{
	refcount_set(&kref->refcount, 1);
}

static inline void mykref_get(struct mykref *kref)
{
        refcount_inc(&kref->refcount);
}

/* At the end we call refcount_dec */
static inline int mykref_put(struct mykref *kref, void (*callback)(struct mykref *kref))
{
	bool val;
	val = refcount_dec_not_one(&kref->refcount);
	if (val) {
		//WARN_ONCE(val, "\n mykref Value is already 1 \n");
	}
	if (refcount_read(&kref->refcount) == 1) {
		/* refcount is one */
                callback(kref);
                return 1;
        }
	//printk(KERN_ERR "\n iocount: %d", refcount_read(&kref->refcount));
        return 0;
}

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
#define IN_MAP 0
#define IN_CLEANING 1
#define STALLED_WRITE 2

static void extent_init(struct extent *e, sector_t lba, sector_t pba, sector_t len)
{
	memset(e, 0, sizeof(*e));
	e->lba = lba;
	e->pba = pba;
	e->len = len;
	e->ptr_to_rev = NULL;
}


static void gcextent_init(struct gc_extents *gce, sector_t lba, sector_t pba, unsigned len)
{
	memset(gce, 0, sizeof(*gce));
	INIT_LIST_HEAD(&gce->list);
	gce->e.lba = lba;
	gce->e.pba = pba;
	gce->e.len = len;
	refcount_set(&gce->ref, 1);
}

void print_extents(struct ctx *ctx);
void free_gc_extent(struct ctx * ctx, struct gc_extents * gc_extent);

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
			printk(KERN_ERR "\n %s Searching lba: %lu, found:  (e->lba: %llu, e->pba: %llu e->len: %llu)", __func__, lba, e->lba, e->pba, e->len); 
		}
		if ((e->lba >= lba) && (!higher || (e->lba < higher->lba))) {
			higher = e;
			//printk(KERN_ERR "\n %s Searching lba: %llu, higher:  (e->lba: %llu, e->pba: %llu e->len: %llu)", __func__, lba, e->lba, e->pba, e->len); 
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

	down_read(&ctx->lsdm_rb_lock); 
	e = _lsdm_rb_geq(&ctx->extent_tbl_root, lba, print);
	up_read(&ctx->lsdm_rb_lock); 

	return e;
}

void sit_ent_vblocks_decr(struct ctx *ctx, sector_t pba);

/* TODO: depending on the root decrement the correct nr of extents */
static void lsdm_rb_remove(struct ctx *ctx, struct extent *e)
{
	struct rev_extent *rev_e = e->ptr_to_rev;

	rb_erase(&rev_e->rb, &ctx->rev_tbl_root);
	kmem_cache_free(ctx->rev_extent_cache, rev_e);
	ctx->n_extents--;

	rb_erase(&e->rb, &ctx->extent_tbl_root);
	/* e will be freed in the called */
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

/* DEBUGGING CODE */
#ifdef LSDM_DEBUG
static int check_node_contents(struct rb_node *node)
{
	int ret = 0;
	struct extent *e, *next, *prev;
	struct rev_extent *re;

	if (!node)
		return -1;

	e = rb_entry(node, struct extent, rb);
	if (!e)
		return -1;
	re = e->ptr_to_rev;
	if (!re)
		return -1;
	if (re->pba != e->pba) {
		printk(KERN_ERR "\n %s PBAs do not match in e and re:: re->pba: %llu e->pba: %llu ", __func__, re->pba, e->pba);
		return -1;
	}

	/*

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
	} */

	if(node->rb_left)
		ret = check_node_contents(node->rb_left);

	if (ret < 0)
		return ret;

	if (node->rb_right)
		ret = check_node_contents(node->rb_right);

	return ret;

}
#endif

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
				lsdm_rb_remove(ctx, e);
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
				lsdm_rb_remove(ctx, next);
				kmem_cache_free(ctx->extent_cache, next);
				ctx->n_extents--;
			}
		}
	}
}


void print_sub_tree(struct rb_node *parent)
{
	struct rev_extent *rev_e, *left, *right;
	struct extent *e;
	if (!parent)
		return;

	rev_e = rb_entry(parent, struct rev_extent, rb);
	e = rev_e->ptr_to_tm;
	print_sub_tree(parent->rb_left);
	if (parent->rb_left) {
		left= rb_entry(parent->rb_left, struct rev_extent, rb);
		BUG_ON(left->pba >= rev_e->pba);
	}
	printk(KERN_ERR "\n %s pba: %llu points to (lba: %llu, pba: %llu, len: %llu) ",  __func__, rev_e->pba, e->lba, e->pba, e->len);
	BUG_ON(rev_e->pba != e->pba);
	if (parent->rb_right) {
		right = rb_entry(parent->rb_right, struct rev_extent, rb);
		BUG_ON(rev_e->pba > right->pba);
	}
	print_sub_tree(parent->rb_right);
}

void print_revmap_tree(struct ctx *ctx)
{
	struct rb_root * root = &ctx->rev_tbl_root;

	printk(KERN_ERR "\n-------------------------------------------------\n");
	print_sub_tree(root->rb_node);
	printk(KERN_ERR "\n-------------------------------------------------\n");
}

void print_extents(struct ctx *ctx);

#ifdef LSDM_DEBUG
static int lsdm_tree_check(struct rb_root *root)
{
	struct rb_node *node = root->rb_node;
	int ret = 0;

	ret = check_node_contents(node);
	return ret;

}
#endif


struct rev_extent * lsdm_revmap_find_print(struct ctx *ctx, u64 pba, size_t len, u64 last_pba)
{
	struct rb_root *root = &ctx->rev_tbl_root;
	struct rb_node **link = NULL, *parent = NULL;
	struct rev_extent *rev_e = NULL, *higher_e = NULL;
	struct extent * e;
	int count = 0;

	link = &root->rb_node;

	printk(KERN_ERR "\n Looking for pba: %llu ", pba);

	/* Go to the bottom of the tree */
	while (*link) {
		count++;
		if (count > 50) {
			return NULL;
		}

		parent = *link;
		rev_e = rb_entry(parent, struct rev_extent, rb);
		if (rev_e->pba > pba) {
			link = &(parent->rb_left);
			if (!higher_e || (rev_e->pba < higher_e->pba)) {
				if (rev_e->pba <= last_pba) {
					higher_e = rev_e;
				}
			}
			printk(KERN_ERR "\n rev_e->pba: %llu, going left ", rev_e->pba);
			continue;
		} 
		if (rev_e->pba < pba) {
			e = rev_e->ptr_to_tm;
			BUG_ON(!e);
			if (rev_e->pba + e->len > pba) {
				/* We find an overlapping pba when the gc_extent was split due to space
				 * requirements in the gc frontier
				 */
				BUG_ON((rev_e->pba + e->len) < (pba + len));
				return rev_e;
			}

			link = &(parent->rb_right);
			printk(KERN_ERR "\n rev_e->pba: %llu, going right", rev_e->pba);
			continue;
		}
		return rev_e;
	}
	return higher_e;


}

struct rev_extent * lsdm_rb_revmap_find(struct ctx *ctx, u64 pba, size_t len, u64 last_pba, const char * caller)
{
	struct rb_root *root = &ctx->rev_tbl_root;
	struct rb_node **link = NULL, *parent = NULL;
	struct rev_extent *rev_e = NULL, *higher_e = NULL;
	struct extent * e;
#ifdef LSDM_DEBUG
	struct rev_extent *temp;
	int count = 0, ret = 0;
#endif


	if (!root)  {
		printk(KERN_ERR "\n Root is NULL! \n");
		BUG();
	}

	if (pba > last_pba) {
		printk(KERN_ERR "\n %s pba: %llu last_pba: %llu caller: %s \n", __func__, pba, last_pba, caller);
		dump_stack();
		last_pba = ctx->sb->max_pba;
		if (last_pba == 0) {
			last_pba = 65536;
		}
	}

	BUG_ON(pba == 0);

	link = &root->rb_node;

	/* Go to the bottom of the tree */
	while (*link) {
	#ifdef LSDM_DEBUG
		count++;
		if (count > 50) {
			printk(KERN_ERR "\n %s Stuck while searching pba: %llu, last_pba: %llu \n", __func__, pba, last_pba);
			printk(KERN_ERR "\n %s On node with pba: %llu", __func__, rev_e->pba);
			print_revmap_tree(ctx);
			print_extents(ctx);
			printk(KERN_ERR "\n %s Stuck while searching pba: %llu, last_pba: %llu \n", __func__, pba, last_pba);
			printk(KERN_ERR "\n %s Stuck while searching pba: %llu, last_pba: %llu \n", __func__, pba, last_pba);
			temp = lsdm_revmap_find_print(ctx, pba, len, last_pba);
			BUG_ON(temp);
			ret = lsdm_tree_check(&ctx->extent_tbl_root);
			if (ret < 0) {
				printk(KERN_ERR "\n lsdm_tree_check failed!! ");
				BUG();
			} 
			BUG_ON(1);
		}
	#endif
		parent = *link;
		rev_e = rb_entry(parent, struct rev_extent, rb);
		if (rev_e->pba > pba) {
			link = &(parent->rb_left);
			if (!higher_e || (rev_e->pba < higher_e->pba)) {
				if (rev_e->pba <= last_pba) {
					higher_e = rev_e;
				}
			}
			continue;
		} 
		if (rev_e->pba < pba) {
			e = rev_e->ptr_to_tm;
			BUG_ON(!e);
			BUG_ON(e->pba != rev_e->pba);
			if (rev_e->pba + e->len > pba) {
				/* We find an overlapping pba when the gc_extent was split due to space
				 * requirements in the gc frontier
				 */
				BUG_ON((rev_e->pba + e->len) < (pba + len));
				return rev_e;
			}
			link = &(parent->rb_right);
			continue;
		}
		//rev_e->pba == pba
		return rev_e;
	}
	return higher_e;
}

struct rev_extent * lsdm_rb_revmap_insert(struct ctx *ctx, struct extent *extent)
{
	struct rb_root *root = &ctx->rev_tbl_root;
	struct rb_node **link = NULL, *parent = NULL;
	struct rev_extent *r_e = NULL, *r_new=NULL;
	struct extent *e = NULL;

	if (!root)  {
		printk(KERN_ERR "\n Root is NULL! \n");
		BUG();
	}

	if (!extent) {
		printk(KERN_ERR "\n %s extent is NULL ", __func__);
		BUG();
	}

	//printk(KERN_ERR "\n lba: %lld, len: %lld ", lba, len);
	r_new = kmem_cache_alloc(ctx->rev_extent_cache, GFP_KERNEL);
	if (!r_new) {
		printk(KERN_ERR "\n Could not allocate memory!");
		BUG();
	}
	RB_CLEAR_NODE(&r_new->rb);
	extent->ptr_to_rev = r_new;
	r_new->ptr_to_tm = extent;
	r_new->pba = extent->pba;
	link = &root->rb_node;

	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		r_e = rb_entry(parent, struct rev_extent, rb);
		/* DEBUG PURPOSE */
		e = r_e->ptr_to_tm;
		BUG_ON(!e);
		BUG_ON(e->pba != r_e->pba);
		if (r_e->pba > r_new->pba) {
			if (r_e->pba + e->len < r_new->pba) {
				/* We find an overlapping pba when the gc_extent was split due to space
				 * requirements in the gc frontier
				 */
				printk(KERN_ERR "\n %s Error while Inserting pba: %llu, Existing -> pointing to (lba, pba (zone), len): (%llu, %llu (%u), %llu) \n", __func__, r_new->pba, e->lba, e->pba, get_zone_nr(ctx, e->pba), e->len);
				printk(KERN_ERR "\n %s Error while Inserting pba: %llu, New -> pointing to (lba, pba (zone), len): (%llu, %llu (%u), %llu) \n", __func__, r_new->pba, extent->lba, extent->pba, get_zone_nr(ctx, extent->pba), extent->len);
				BUG_ON("1. Bug while adding revmap entry ! (less than case)");
			}

			link = &(parent->rb_left);
			continue;
		} 
		if (r_e->pba  < r_new->pba) {
			if (r_e->pba + e->len > r_new->pba) {
				/* We find an overlapping pba when the gc_extent was split due to space
				 * requirements in the gc frontier
				 */
				printk(KERN_ERR "\n %s Error while Inserting pba: %llu, Existing -> pointing to (lba, pba (zone), len): (%llu, %llu (%u), %llu) \n", __func__, r_new->pba, e->lba, e->pba, get_zone_nr(ctx, e->pba), e->len);
				printk(KERN_ERR "\n %s Error while Inserting pba: %llu, New -> pointing to (lba, pba (zone), len): (%llu, %llu (%u), %llu) \n", __func__, r_new->pba, extent->lba, extent->pba, get_zone_nr(ctx, extent->pba), extent->len);
				BUG_ON("2. Bug while adding revmap entry !");
			}
			link = &(parent->rb_right);
			continue;
		} 
		printk(KERN_ERR "\n %s Error while Inserting pba: %llu, Existing -> pointing to (lba, pba (zone), len): (%llu, %llu (%u), %llu) \n", __func__, r_new->pba, e->lba, e->pba, get_zone_nr(ctx, e->pba), e->len);
		printk(KERN_ERR "\n %s Error while Inserting pba: %llu, New -> pointing to (lba, pba (zone), len): (%llu, %llu (%u), %llu) \n", __func__, r_new->pba, extent->lba, extent->pba, get_zone_nr(ctx, extent->pba), extent->len);
		BUG_ON("3. Bug while adding revmap entry !");
	}
	//printk( KERN_ERR "\n %s Inserting pba: %llu, pointing to (lba, len): (%llu, %u)", __func__, r_new->pba, extent->lba, extent->len);
	/* Put the new node there */
	rb_link_node(&r_new->rb, parent, link);
	rb_insert_color(&r_new->rb, root);
	return r_new;
}

int _lsdm_verbose;
void sit_ent_vblocks_incr(struct ctx *ctx, sector_t pba);
void sit_ent_add_mtime(struct ctx *ctx, sector_t pba);
/* 
 * Returns NULL if the new node code be added.
 * Return overlapping extent otherwise (overlapping node already exists)
 */
static struct extent * lsdm_rb_insert(struct ctx *ctx, struct extent *new)
{
	struct rb_root *root = &ctx->extent_tbl_root;
	struct rb_node **link = NULL, *parent = NULL;
	struct extent *e = NULL;
	struct rev_extent *r_e = NULL, *r_insert; 

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
		r_e = e->ptr_to_rev;
		BUG_ON(!r_e);
		if (e->pba != r_e->pba) {
			printk(KERN_ERR "\n %s parent->lba: %llu, parent->pba: %llu, parent->len: %llu r_e->pba: %llu", __func__, e->lba, e->pba, e->len, r_e->pba);
			BUG();
		}
		if (e->lba >= (new->lba + new->len)) {
			//printk(KERN_ERR "\n \t Going left now! ");
			link = &(parent->rb_left);
		} else if ((e->lba + e->len) <= new->lba){
			//printk(KERN_ERR "\n \t Going right now! ");
			link = &(parent->rb_right);
		} else {
			//printk(KERN_ERR "\n Found e with the same lba as beinwith the same lba as being inserted in the tree.. returning that!");
			return e;
		}
	}
	//printk( KERN_ERR "\n %s Inserting (lba: %llu pba: %llu len: %u) ", __func__, new->lba, new->pba, new->len);
	/* Put the new node there */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
	r_insert = lsdm_rb_revmap_insert(ctx, new);
#ifdef LSDM_DEBUG
	struct rev_extent *r_find;
	r_find = lsdm_rb_revmap_find(ctx, new->pba, new->len, ctx->sb->max_pba, __func__);
	if (r_insert != r_find) {
		printk(KERN_ERR "\n %s inserted revmap address is different than found one! ");
		printk(KERN_ERR "\n revmap_find(): %p, revmap_insert(): %p", r_find, r_insert);
		printk(KERN_ERR "\n revmap_find()::pba: %llu, revmap_insert()::pba: %llu", r_find->pba, r_insert->pba);
		BUG();
	}
	if (r_insert->pba != r_find->pba) {
		printk(KERN_ERR "\n %s inserted revmap pba is different than found one! ");
		printk(KERN_ERR "\n revmap_find()::pba: %llu, revmap_insert()::pba: %llu", r_find->pba, r_insert->pba);
		BUG();
	}
#endif
	merge(ctx, root, new);
	/* new should be physically discontiguous
	 */
	/*
	int ret = 0;
	ret = lsdm_tree_check(root);
	if (ret < 0) {
		printk(KERN_ERR"\n !!!! Corruption while Inserting: lba: %lld pba: %lld len: %u", new->lba, new->pba, new->len);
		BUG();
	} */
	return NULL;
}




/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to map.
 *
 */
static int lsdm_rb_update_range(struct ctx *ctx, sector_t lba, sector_t pba, size_t len)
{
	struct extent *e = NULL, *new = NULL, *split = NULL, *prev = NULL;
	struct extent *tmp = NULL;
	sector_t diff = 0;

	//printk(KERN_ERR "\n Entering %s lba: %llu, pba: %llu, len:%ld ", __func__, lba, pba, len);
	
	BUG_ON(len <= 0);
	BUG_ON(pba == 0);
	BUG_ON(pba > ctx->sb->max_pba);
	BUG_ON(lba > ctx->sb->max_pba);
	new = kmem_cache_alloc(ctx->extent_cache, GFP_KERNEL);
	if (!new) {
		BUG();
		printk(KERN_ERR "\n Could not allocate memory!");
		return -ENOMEM;
	}
	RB_CLEAR_NODE(&new->rb);
	extent_init(new, lba, pba, len);
	BUG_ON(new->len <= 0);

	e = lsdm_rb_insert(ctx, new);
	/* No overlapping extent found, inserted new */
	if (!e) {
		return 0;
	}
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
		split = kmem_cache_alloc(ctx->extent_cache, GFP_KERNEL);
		if (!split) {
			kmem_cache_free(ctx->extent_cache, new);
			return -ENOMEM;
		}
		diff =  lba - e->lba;
		/* Initialize split before e->len changes!! */
		extent_init(split, lba + len, e->pba + (diff + len), e->len - (diff + len));
		lsdm_rb_remove(ctx, e);
		e->len = diff;
		BUG_ON(e->len <= 0);
		if (lsdm_rb_insert(ctx, e)) {
			printk(KERN_ERR"\n Corruption in case 1!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %llu diff:%llu ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();

		}
		/* find e in revmap and reduce the size */

		//printk("\n Case1: Modified: (new->lba: %llu, new->pba: %llu, new->len: %lu) ", e->lba, e->pba, e->len);
		if (lsdm_rb_insert(ctx, new)) {
			printk(KERN_ERR"\n Corruption in case 1.1, diff: %llu !! ", diff);
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %llu diff:%llu ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();
		}
		BUG_ON(split->len <= 0);
		if (lsdm_rb_insert(ctx, split)) {
			printk(KERN_ERR"\n Corruption in case 1.2!! diff: %llu", diff);
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %llu diff:%llu ", e->lba, e->pba, e->len, diff);
			printk(KERN_ERR "\n split->lba: %lld split->pba: %lld split->len: %llu ", split->lba, split->pba, split->len);
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
		lsdm_rb_remove(ctx, e);
		e->len = lba - e->lba;
		BUG_ON(e->len <= 0);
		if (lsdm_rb_insert(ctx, e)) {
			printk(KERN_ERR"\n Corruption in case 2!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %llu diff:%llu ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();

		}
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
		lsdm_rb_remove(ctx, e);
		kmem_cache_free(ctx->extent_cache, e);
		e = tmp;
	}
	if (!e || (e->lba >= lba + len))  {
		if (lsdm_rb_insert(ctx, new)) {
			printk(KERN_ERR"\n Corruption in case 3!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %llu diff:%llu ", e->lba, e->pba, e->len, diff); 
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
	if ((lba <= e->lba) && ((lba + len) > e->lba) && ((lba + len) < (e->lba + e->len)))  {
		diff = lba + len - e->lba;
		//printk("\n Case4, Removing: (e->lba: %llu, e->pba: %llu, e->len: %lu) diff: %llu", e->lba, e->pba, e->len, diff);
		lsdm_rb_remove(ctx, e);
		if (lsdm_rb_insert(ctx, new)) {
			printk(KERN_ERR"\n Corruption in case 4.1!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %llu diff:%llu ", e->lba, e->pba, e->len, diff); 
			printk(KERN_ERR"\n"); 
			printk(KERN_ERR "\n RBTree corruption!!" );
			BUG();
		}
		//printk("\n Case4.1, Inserted: (new->lba: %llu, new->pba: %llu, new->len: %lu) ", new->lba, new->pba, new->len);
		e->lba = lba + len;
		e->len = e->len - diff;
		e->pba = e->pba + diff;
		//printk("\n Case4.2, Inserting: (e->lba: %llu, e->pba: %llu, e->len: %lu) ", e->lba, e->pba, e->len);
		BUG_ON(e->len <= 0);
		if (lsdm_rb_insert(ctx, e)) {
			printk(KERN_ERR"\n Corruption in case 4.2!! ");
			printk(KERN_ERR "\n lba: %lld pba: %lld len: %ld ", lba, pba, len); 
			printk(KERN_ERR "\n e->lba: %lld e->pba: %lld e->len: %llu diff:%llu ", e->lba, e->pba, e->len, diff); 
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
static void lsdm_free_rb_tree(struct ctx *ctx)
{
	struct rb_root *root = &ctx->extent_tbl_root;
	struct extent *e = NULL;
	struct rb_node *node;

	node = root->rb_node;
	while(node) {
		e = rb_entry(node, struct extent, rb);
		lsdm_rb_remove(ctx, e);
		kmem_cache_free(ctx->extent_cache, e);
		node = root->rb_node;
	}
	BUG_ON(root->rb_node);
}

static inline int is_lsdm_ioidle(struct ctx *ctx)
{
	//printk(KERN_ERR "\n %s nr_app_writes: %lu", __func__, ctx->nr_app_writes);
	//printk(KERN_ERR "\n %s nr_reads: %lu", __func__, ctx->nr_reads);
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

void flush_translation_blocks(struct ctx *ctx);
void flush_sit(struct ctx *ctx);

#define DEF_FLUSH_TIME 10000 /* (milliseconds) */
#define DEF_GC_TIME	10000000 /*1000 seconds */

void do_checkpoint(struct ctx *ctx);
void remove_translation_pages(struct ctx *ctx);

static int flush_thread_fn(void * data)
{

	struct ctx *ctx = (struct ctx *) data;
	struct lsdm_flush_thread *flush_th = ctx->flush_th;
	unsigned int wait_ms;
	wait_queue_head_t *wq = &flush_th->flush_waitq;

	wait_ms = flush_th->sleep_time; 
	printk(KERN_ERR "\n %s executing! ", __func__);
	set_freezable();
	do {
		wait_event_interruptible_timeout(*wq,
			kthread_should_stop() || freezing(current) ||
			flush_th->wake,
			msecs_to_jiffies(wait_ms));
		if (try_to_freeze()) {
                        continue;
                }
		do_checkpoint(ctx);
	} while(!kthread_should_stop());
	return 0;
}


int lsdm_flush_thread_start(struct ctx * ctx)
{
	struct lsdm_flush_thread * flush_th;
	dev_t dev = ctx->dev->bdev->bd_dev;
	int err=0;

	printk(KERN_ERR "\n About to start flush thread");

	flush_th = lsdm_malloc(sizeof(struct lsdm_flush_thread), GFP_KERNEL);
	if (!flush_th) {
		return -ENOMEM;
	}

	init_waitqueue_head(&flush_th->flush_waitq);
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
static int select_zone_to_clean(struct ctx *ctx, int mode, const char *func)
{
	struct rb_node *node = NULL;
	struct gc_cost_node * cnode = NULL;
	struct gc_zone_node * znode = NULL;

	//if (mode == BG_GC) {
		node = rb_first(&ctx->gc_cost_root);
		if (!node)
			return -1;

		cnode = rb_entry(node, struct gc_cost_node, rb);
		list_for_each_entry(znode, &cnode->znodes_list, list) {
			//printk(KERN_ERR "\n %s zone: %d has %d blks caller: %s", __func__, znode->zonenr, znode->vblks, func);
			return znode->zonenr;
		}

	//}
	/* TODO: Mode: FG_GC */
	return -1;
}

static int add_extent_to_gclist(struct ctx *ctx, struct extent_entry *e)
{
	struct gc_extents *gc_extent;
	/* maxlen is the maximum number of sectors permitted by BIO */
	int maxlen = (BIO_MAX_PAGES >> 1) << SECTOR_BLK_SHIFT;
	int temp = 0;
	unsigned int pagecount, s8, count = 0;
	
	if (!e->len)
		return 0;

	while (1) {
		BUG_ON(e->len == 0);
		BUG_ON(e->pba > ctx->sb->max_pba);
		temp = 0;
		count++;
		if (e->len > maxlen) {
			temp = e->len - maxlen;
			e->len = maxlen;
		}
		gc_extent = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
		if (!gc_extent) {
			printk(KERN_ERR "\n Could not allocate memory to gc_extent! ");
			BUG();
			return -ENOMEM;
		}
		gcextent_init(gc_extent, 0, 0 , 0);
		gc_extent->e.lba = e->lba;
		gc_extent->e.pba = e->pba;
		gc_extent->e.len = e->len;
		gc_extent->bio = NULL;
		gc_extent->read = 0;
		s8 = gc_extent->e.len;
		BUG_ON(s8 > (BIO_MAX_PAGES << SECTOR_BLK_SHIFT));
		pagecount = (s8 >> SECTOR_BLK_SHIFT);
		gc_extent->nrpages = pagecount;
		gc_extent->bio_pages = kmalloc(pagecount * sizeof(void *), GFP_KERNEL);
		if (!gc_extent->bio_pages) {
			printk(KERN_ERR "\n %s could not allocate memory for gc_extent->bio_pages", __func__);
			return -ENOMEM;
		}
		//printk(KERN_ERR "\n %s (lba: %llu, pba: %llu e->len: %ld) maxlen: %ld temp: %d", __func__, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len, (maxlen), temp);
		/* 
		 * We always want to add the extents in a PBA increasing order
		 */
		list_add_tail(&gc_extent->list, &ctx->gc_extents->list);
		if (temp == 0) {
			break;
		}
		e->lba = e->lba + maxlen;
		e->pba = e->pba + maxlen;
		e->len = temp;
	}
	return count;
}

void read_gcextent_done(struct bio * bio)
{
	struct gc_extents *gc_extent = (struct gc_extents *) bio->bi_private;
	
	//refcount_dec(&gc_extent->ref);
	bio_put(bio);
	gc_extent->bio = NULL;
}

static int read_extent_bio(struct ctx *ctx, struct gc_extents *gc_extent)
{
	struct bio_vec *bv = NULL;
	struct page *page;
	struct bio *bio;
	struct bvec_iter_all iter_all;
	unsigned int s8, pagecount;
	int i;

	//refcount_inc(&gc_extent->ref);
	s8 = gc_extent->e.len;
	BUG_ON(s8 > (BIO_MAX_PAGES << SECTOR_BLK_SHIFT));
	pagecount = (s8 >> SECTOR_BLK_SHIFT);

	/* create a bio with "nr_pages" bio vectors, so that we can add nr_pages (nrpages is different)
	 * individually to the bio vectors
	 */
	bio = bio_alloc_bioset(ctx->dev->bdev, pagecount, REQ_OP_READ, GFP_KERNEL, ctx->gc_bs);
	if (!bio) {
		printk(KERN_ERR "\n %s could not allocate memory for bio ", __func__);
		return -ENOMEM;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	for(i=0; i<pagecount; i++) {
		page = mempool_alloc(ctx->gc_page_pool, GFP_KERNEL);
		if ((!page) || ( !bio_add_page(bio, page, PAGE_SIZE, 0))) {
			bio_for_each_segment_all(bv, bio, iter_all) {
				mempool_free(bv->bv_page, ctx->gc_page_pool);
			}
			bio_put(bio);
			return -ENOMEM;
		}
		gc_extent->bio_pages[i] = page;
	}
	bio->bi_iter.bi_sector = gc_extent->e.pba;
	bio_set_dev(bio, ctx->dev->bdev);
	//bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_private = gc_extent;
	//bio->bi_end_io = read_gcextent_done;
	gc_extent->bio = bio;
#ifdef LSDM_DEBUG
	if (gc_extent->read == 1)
		BUG();
#endif
	gc_extent->read = 1;
	/* submiting the bio in read_all_bios_and_wait */
	submit_bio_wait(gc_extent->bio);
	//refcount_dec(&gc_extent->ref);
	bio_put(bio);
	return 0;
}



static void free_gc_list(struct ctx *ctx)
{

	struct list_head *list_head;
	struct gc_extents *gc_extent, *next_extent;

	list_head = &ctx->gc_extents->list;
	list_for_each_entry_safe(gc_extent, next_extent, list_head, list) {
		free_gc_extent(ctx, gc_extent);
	}
	list_del(&ctx->gc_extents->list);
	printk(KERN_ERR "\n %s done! \n ", __func__);
}

void wait_on_refcount(struct ctx *ctx, refcount_t *ref, spinlock_t *lock);

/*
 * TODO: Do  not chain the bios as we do not get notification
 * of what extent reading did not work! We can retry and if
 * the block did not work, we can do something more meaningful.
 */
static int read_gc_extents(struct ctx *ctx)
{
	struct list_head *pos;
	struct gc_extents *gc_extent;
	int count = 0;
	
	/* If list is empty we have nothing to do */
	BUG_ON(list_empty(&ctx->gc_extents->list));

	/* setup the bio for the first gc_extent */
	list_for_each(pos, &ctx->gc_extents->list) {
		gc_extent = list_entry(pos, struct gc_extents, list);
		if ((gc_extent->e.len == 0) || ((gc_extent->e.pba + gc_extent->e.len) > ctx->sb->max_pba)) {
			printk(KERN_ERR "\n %s lba: %llu, pba: %llu, len: %llu ", __func__, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
			BUG();
		}
		if (read_extent_bio(ctx, gc_extent)) {
			free_gc_list(ctx);
			printk(KERN_ERR "Low memory! TODO: Write code to free memory from translation tables etc ");
			BUG();
		}
		count = count + 1;
	}
	//printk(KERN_ERR "\n GC extents submitted for read: %d ", count);
	return 0;
}

/*
 * We sort on the LBA
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
 */
void mark_disk_full(struct ctx *ctx);
void move_gc_write_frontier(struct ctx *ctx, sector_t sectors_s8);

static int setup_extent_bio_write(struct ctx *ctx, struct gc_extents *gc_extent);
struct tm_page *add_tm_page_kv_store(struct ctx *, sector_t);
int add_translation_entry(struct ctx *, sector_t , sector_t , size_t ); 
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
static int write_metadata_extent(struct ctx *ctx, struct gc_extents *gc_extent)
{
	struct bio *bio;
	sector_t s8;
	int ret;

	/*
	ret = refcount_read(&gc_extent->ref);
	if (ret > 1) {
		printk(KERN_ERR "\n waiting on refcount \n");
		wait_on_refcount(ctx, &gc_extent->ref, &ctx->gc_ref_lock);
	}*/
	setup_extent_bio_write(ctx, gc_extent);
	bio = gc_extent->bio;
	s8 = bio_sectors(bio);

	//down_write(&ctx->wf_lock);
	bio->bi_iter.bi_sector = ctx->warm_gc_wf_pba;
	gc_extent->e.pba = ctx->warm_gc_wf_pba;
	move_gc_write_frontier(ctx, gc_extent->e.len);
	//up_write(&ctx->wf_lock);

	//printk(KERN_ERR "\n %s gc_extent: (lba: %d pba: %d len: %d), s8: %d max_pba: %llu", __func__, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len, s8, ctx->sb->max_pba);
#ifdef LSDM_DEBUG
	BUG_ON(gc_extent->e.len != s8);
	BUG_ON(gc_extent->e.pba == 0 );
	BUG_ON(gc_extent->e.pba >= ctx->sb->max_pba);
	BUG_ON(gc_extent->e.lba >= ctx->sb->max_pba);
#endif
	bio->bi_status = BLK_STS_OK;
	
	down_write(&ctx->lsdm_rev_lock);
	/*---------------------*/
	add_revmap_entry(ctx, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
	/*---------------------*/
	up_write(&ctx->lsdm_rev_lock);
	add_translation_entry(ctx, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
	//printk(KERN_ERR "\n %s About to add: lba: %llu pba: %llu , len: %llu e.len: %u" , __func__, gc_extent->e.lba, gc_extent->e.pba, s8, gc_extent->e.len);
	ret = lsdm_rb_update_range(ctx, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len);
	//printk(KERN_ERR "\n %s Added rb entry ! lba: %llu pba: %llu , len: %llu e.len: %u" , __func__, gc_extent->e.lba, gc_extent->e.pba, s8, gc_extent->e.len);
	return 0;
}

static void mark_zone_free(struct ctx *ctx , int zonenr, int resetZone);



static int setup_extent_bio_write(struct ctx *ctx, struct gc_extents *gc_extent)
{
	int i=0, s8;
	struct bio_vec *bv = NULL;
	struct page *page;
	struct bio *bio;
	struct bvec_iter_all iter_all;
	int bio_pages;

	s8 = gc_extent->e.len;
	BUG_ON(s8 > (BIO_MAX_PAGES << SECTOR_BLK_SHIFT));
	//printk(KERN_ERR "\n %s gc_extent->e.len = %d ", __func__, s8);
	bio_pages = (s8 >> SECTOR_BLK_SHIFT);
	BUG_ON(bio_pages != gc_extent->nrpages);
	//printk(KERN_ERR "\n 1) %s gc_extent->e.len (in sectors): %ld s8: %d bio_pages:%d", __func__, gc_extent->e.len, s8, bio_pages);

	/* create a bio with "nr_pages" bio vectors, so that we can add nr_pages (nrpages is different)
	 * individually to the bio vectors
	 */
	bio = bio_alloc_bioset(ctx->dev->bdev, bio_pages, REQ_OP_WRITE, GFP_KERNEL, ctx->gc_bs);
	if (!bio) {
		printk(KERN_ERR "\n %s could not allocate memory for bio ", __func__);
		return -ENOMEM;
	}

	/* bio_add_page sets the bi_size for the bio */
	for(i=0; i<bio_pages; i++) {
		page = gc_extent->bio_pages[i];
		if (!page) {
			printk(KERN_ERR "\n %s i: %d ", __func__, i);
			BUG_ON(!page);
		}
		if (!bio_add_page(bio, page, PAGE_SIZE, 0)) {
			printk(KERN_ERR "\n %s Could not add page to the bio ", __func__);
			printk(KERN_ERR "bio->bi_vcnt: %d bio->bi_iter.bi_size: %d bi_max_vecs: %d \n", bio->bi_vcnt, bio->bi_iter.bi_size, bio->bi_max_vecs);
			bio_for_each_segment_all(bv, bio, iter_all) {
				mempool_free(bv->bv_page, ctx->gc_page_pool);
			}
			bio_put(bio);
			return -ENOMEM;
		}
	}
	//printk(KERN_ERR "\n %s bio_sectors(bio): %llu nr_pages: %d", __func__,  bio_sectors(bio), bio_pages);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_opf = REQ_OP_WRITE;
	//bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	gc_extent->bio = bio;
	return 0;
}

void free_gc_extent(struct ctx * ctx, struct gc_extents * gc_extent)
{
	unsigned int pagecount = 0;
	int i;

	list_del(&gc_extent->list);
	pagecount = gc_extent->nrpages;
	if (gc_extent->bio_pages) {
		for (i=0; i<pagecount; i++) {
			if(gc_extent->bio_pages[i]) {
				mempool_free(gc_extent->bio_pages[i], ctx->gc_page_pool);
				gc_extent->bio_pages[i] = NULL;
			}
		}
		kfree(gc_extent->bio_pages);
		gc_extent->bio_pages = NULL;
	}
	gc_extent->nrpages = 0;
	gcextent_init(gc_extent, 0, 0 , 0);
	if (gc_extent->bio) {
		bio_put(gc_extent->bio);
	}
	kmem_cache_free(ctx->gc_extents_cache, gc_extent);
}


int add_block_based_translation(struct ctx *ctx, struct page *page, const char * caller);

int complete_revmap_blk_flush(struct ctx * ctx, struct page *page)
{
	struct bio * bio;
	unsigned int revmap_entry_nr, revmap_sector_nr;	
	sector_t pba;
	
	down_write(&ctx->lsdm_rev_lock);
	pba = ctx->revmap_pba;
	revmap_entry_nr = atomic_read(&ctx->revmap_entry_nr);
	revmap_sector_nr = atomic_read(&ctx->revmap_sector_nr);
	//printk(KERN_ERR "\n %s entry_nr: %d sector_nr: %d page: %p", __func__, revmap_entry_nr, revmap_sector_nr, page_address(page));
	if ((revmap_entry_nr == 0) && (revmap_sector_nr == 0)) {
		up_write(&ctx->lsdm_rev_lock);
		return 0;
	}
	atomic_set(&ctx->revmap_entry_nr, 0);
	atomic_set(&ctx->revmap_sector_nr, 0);
	up_write(&ctx->lsdm_rev_lock);

	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_WRITE, GFP_KERNEL);
	if (!bio) {
		return -ENOMEM;
	}
	
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return -EFAULT;
	}

	bio->bi_iter.bi_sector = pba;
	//printk(KERN_ERR "%s Flushing revmap blk at pba:%llu ctx->revmap_pba: %llu", __func__, bio->bi_iter.bi_sector, ctx->revmap_pba);
	bio->bi_opf = REQ_OP_WRITE;
	//bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	BUG_ON(ctx->revmap_pba > ctx->sb->max_pba);
	submit_bio_wait(bio);
	mark_revmap_bit(ctx, pba);
	bio_put(bio);
	__free_pages(page, 0);
	nrpages--;
	return 0;
}

int print_gc_extents(struct ctx *ctx, int zonenr);
int get_sit_ent_vblocks(struct ctx *ctx, int zonenr);
/* Since our read_extents call, overwrites could have made
 * the blocks in this zone invalid. Thus we now take a 
 * write lock and then re-read the extents metadata; else we
 * will end up writing invalid blocks and loosing the
 * overwritten data
 */
static int write_valid_gc_extents(struct ctx *ctx, int zonenr)
{
	struct extent *e = NULL;
	struct rev_extent *rev_e = NULL;
	struct gc_extents *gc_extent, *temp_ptr, *newgc_extent, *next_ptr;
	sector_t diff;
	sector_t nr_sectors, s8, len;
	int count = 0, pagecount = 0, diffcount = 0;
	int i, j, total_vblks = 0;
	u64 last_pba_read;
	u64 gc_writes = 0;
	//int total = 0;

	
	total_vblks = get_sit_ent_vblocks(ctx, zonenr);
	last_pba_read = get_last_pba_for_zone(ctx, zonenr);
	list_for_each_entry_safe(gc_extent, next_ptr, &ctx->gc_extents->list, list) {
		/* Reverse map stores PBA as e->lba and LBA as e->pba
		 * This was done for code reuse between map and revmap
		 * Thus e->lba is actually the PBA
		 */
		gc_extent->bio = NULL;
		//printk(KERN_ERR "\n %s gc_extent::(lba: %llu, pba: %llu, len: %d) last_pba_read: %llu", __func__, gc_extent->e.lba, gc_extent->e.pba, gc_extent->e.len, last_pba_read);
		down_write(&ctx->lsdm_rb_lock);
		/* You cannot use rb_next here as pba ie the key, changes when we write to a new frontier,
		 * so the node is removed and rewritten at a different place, so the next is the new next 
		 */
		rev_e = lsdm_rb_revmap_find(ctx, gc_extent->e.pba, gc_extent->e.len, last_pba_read, __func__);
		if (!rev_e) {
			up_write(&ctx->lsdm_rb_lock);
			/* snip all the gc_extents from this onwards */
			list_for_each_entry_safe_from(gc_extent, temp_ptr, &ctx->gc_extents->list, list) {
				gc_extent->bio = NULL;
				free_gc_extent(ctx, gc_extent);
			}
			break;
		}
		e = rev_e->ptr_to_tm;
		/* entire extent is lost by interim overwrites */
		if (e->pba >= (gc_extent->e.pba + gc_extent->e.len)) {
			//printk(KERN_ERR "\n %s:%d entire extent is lost! \n", __func__, __LINE__);
			up_write(&ctx->lsdm_rb_lock);
			gc_extent->bio = NULL;
			free_gc_extent(ctx, gc_extent);
			continue;
		}
		/* extents are partially snipped at the front*/
		if (e->pba > gc_extent->e.pba) {
			//printk(KERN_ERR "\n %s:%d extent is snipped! \n", __func__, __LINE__);
			diff = e->pba - gc_extent->e.pba;
			gc_extent->e.lba = e->lba;
			gc_extent->e.pba = e->pba;
			gc_extent->e.len = gc_extent->e.len - diff;
			BUG_ON(!gc_extent->e.len);
			pagecount = gc_extent->e.len >> SECTOR_BLK_SHIFT;
			BUG_ON(!pagecount);
			diffcount = gc_extent->nrpages - pagecount;
			BUG_ON(!diffcount);
			/* free the extra pages */
			for(i=0; i<diffcount; i++) {
				/* free the front pages already read */
				mempool_free(gc_extent->bio_pages[i], ctx->gc_page_pool);
			}
			for(j=diffcount, i=0; i<pagecount; i++, j++) {
				/* copy from back (j) to the front (i) */
				gc_extent->bio_pages[i] = gc_extent->bio_pages[j];
				/* free the back */
				gc_extent->bio_pages[j] = NULL;
			}
			gc_extent->nrpages = pagecount;
		}
		/* Now we adjust the gc_extent such that it can be  written in the available
		 * space in the gc segment. If less space is available than is required by a
		 * bio, we split that bio. Else we write it as it is.
		 */
		nr_sectors = gc_extent->e.len;
		BUG_ON(!nr_sectors);
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
#ifdef LSDM_DEBUG
		BUG_ON(nr_sectors != s8);
		BUG_ON(ctx->free_sectors_in_gc_wf < NR_SECTORS_IN_BLK);
#endif
		if (nr_sectors > ctx->free_sectors_in_gc_wf) {
			//BUG_ON(!ctx->free_sectors_in_gc_wf);
			//printk(KERN_ERR "\n %s nr_sectors: %llu, ctx->free_sectors_in_gc_wf: %llu gc_extent->nrpages: %d", __func__, nr_sectors, ctx->free_sectors_in_gc_wf, gc_extent->nrpages);
			newgc_extent = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
			if(!newgc_extent) {
				up_write(&ctx->lsdm_rb_lock);
				printk(KERN_ERR "\n Could not allocate memory to new gc_extent! ");
				BUG();
				return -1;
			}
			gcextent_init(newgc_extent, 0, 0 , 0);
			gc_extent->e.len = ctx->free_sectors_in_gc_wf;
			gc_extent->nrpages = gc_extent->e.len >> SECTOR_BLK_SHIFT;
			newgc_extent->e.lba = gc_extent->e.lba + ctx->free_sectors_in_gc_wf;
			newgc_extent->e.pba = gc_extent->e.pba + ctx->free_sectors_in_gc_wf;
			len = nr_sectors - ctx->free_sectors_in_gc_wf;
			newgc_extent->e.len = len;
			/* pagecount: page count of newgc_extent */
			pagecount = len >> SECTOR_BLK_SHIFT;
			BUG_ON(!pagecount);
			newgc_extent->nrpages = pagecount;
			newgc_extent->bio_pages = kmalloc(pagecount * sizeof(void *), GFP_KERNEL);
			if (!newgc_extent->bio_pages) {
				up_write(&ctx->lsdm_rb_lock);
				printk(KERN_ERR "\n %s could not allocate memory for newgc_extent->bio_pages", __func__);
				BUG();
				return -ENOMEM;
			}
			j = gc_extent->nrpages;
			BUG_ON(!j);
			/* Copy the remaining pages from gc_extent to newgc_extent */
			for(i=0; i<pagecount; i++, j++) {
				newgc_extent->bio_pages[i] = gc_extent->bio_pages[j];
				gc_extent->bio_pages[j] = NULL;
			}
			list_add(&newgc_extent->list, &gc_extent->list);
			next_ptr = newgc_extent;
			//printk(KERN_ERR "\n %s gc_extent::len: %d newgc_extent::len: %d ", __func__, gc_extent->e.len, newgc_extent->e.len);
		}

		/* Now you verify all this after holding a lock */
		write_metadata_extent(ctx, gc_extent);
		up_write(&ctx->lsdm_rb_lock);
		//total += gc_extent->e.len;
		/* We do not want to do disk I/O with a lock held. So we write the metadata
		 * and then submit_bio. Now we can have a read on this, before the write completes.
		 * We need to take care of that - such a read will have to wait.
		 * TODO: fix this - read the same block that is about to be written by GC problem.
		 * Mark such a read.
		 */
		submit_bio_wait(gc_extent->bio);
		ctx->nr_gc_writes += s8;
		gc_writes += s8;
		//flush_workqueue(ctx->tm_wq);
		free_gc_extent(ctx, gc_extent);
		count++;
		/*
#ifdef LSDM_DEBUG
		int vblks;
		vblks = get_sit_ent_vblocks(ctx, zonenr);
		BUG_ON(vblks > (total_vblks - (gc_extent->e.len >> SECTOR_BLK_SHIFT)));
		printk(KERN_ERR "\n %s validblks: %d ", __func__, vblks);
#endif
		*/
	}
	/*
#ifdef LSDM_DEBUG
	printk(KERN_ERR "\n %s All %d extents written, total: %d , flushing workqueue now! Segment cleaned! \n", __func__, count, total);
	vblks = get_sit_ent_vblocks(ctx, zonenr);
	//printk(KERN_ERR "\n %s validblks: %d ", __func__, vblks);
	if (vblks != 0) {
		print_gc_extents(ctx, zonenr);
		BUG_ON(1);
	}
#endif
	flush_workqueue(ctx->tm_wq);
	*/
	/* last revmap blk may not be full, we write the partial revmap blk */
	/* clear the revmap bitmap */
	return gc_writes;
}

static int free_gc_extents(struct ctx *ctx)
{
	struct gc_extents *gc_extent, *next_ptr;
	
	list_for_each_entry_safe(gc_extent, next_ptr, &ctx->gc_extents->list, list) {
		gc_extent->bio = NULL;
		free_gc_extent(ctx, gc_extent);
	}
	return 0;
}


struct sit_page * add_sit_page_kv_store(struct ctx * ctx, sector_t pba, const char * caller);

int verify_gc_zone(struct ctx *ctx, int zonenr, sector_t pba)
{
	struct sit_page *sit_page;
	struct lsdm_seg_entry *ptr;
	int index;

	flush_sit(ctx);
	mutex_lock(&ctx->sit_kv_store_lock);
	sit_page = add_sit_page_kv_store(ctx, pba, __func__);
	if (!sit_page) {
		/* TODO: do something, low memory */
		panic("Low memory, couldnt allocate sit entry");
	}
	mutex_unlock(&ctx->sit_kv_store_lock);

	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	if (zonenr != get_zone_nr(ctx, pba))
		BUG_ON(1);

	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	
	if (ptr->vblocks == 65536)
		BUG_ON(1);
	
	//printk(KERN_ERR "\n %s sit_page: %p, index: %d ", __func__, ptr, index);
	//printk(KERN_ERR "\n ***** %s Number of valid blocks in the zone selected for GC is: %u mtime: %llu", __func__, ptr->vblocks, ptr->mtime);
	return 0;
}

int remove_zone_from_gc_tree(struct ctx *ctx, unsigned int zonenr);

void print_memory_usage(struct ctx *ctx, const char *action)
{
	__kernel_ulong_t freeram, available;

	freeram = global_zone_page_state(NR_FREE_PAGES);
	freeram = (freeram << (PAGE_SHIFT - 10)) >> 10;
	printk(KERN_ERR "\n %s: free memory: %lu mB", action, freeram);
	available = si_mem_available();
	available = (available << (PAGE_SHIFT - 10)) >> 10;
	printk(KERN_ERR "\n %s : available memory: %lu mB", action, available);
}

int print_gc_extents(struct ctx *ctx, int zonenr)
{
	sector_t diff;
	struct extent *e = NULL;
	struct rev_extent *rev_e = NULL;
	struct extent_entry temp;
	long total_len = 0;
	sector_t pba, last_pba; 
	int vblks;
	struct rb_node *node;

	pba = get_first_pba_for_zone(ctx, zonenr);
	last_pba = get_last_pba_for_zone(ctx, zonenr);
	vblks = get_sit_ent_vblocks(ctx, zonenr);
	INIT_LIST_HEAD(&ctx->gc_extents->list);

	//print_memory_usage(ctx, "Before GC");
	/* Lookup this pba in the reverse table to find the
	 * corresponding LBA. 
	 * TODO: If the valid blocks are sequential, we need to keep
	 * this segment as an open segment that can append data. We do
	 * not need to perform GC on this segment.
	 */

	temp.pba = 0;
	temp.lba = 0;
	temp.len = 0;
	printk(KERN_ERR "\n %s zonenr: %d first_pba: %llu last_pba: %llu #valid blks: %d", __func__, zonenr, pba, last_pba, vblks);
	rev_e = lsdm_rb_revmap_find(ctx, pba, 0, last_pba, __func__);
	if (!rev_e) {
		return 0;
	}
	e = rev_e->ptr_to_tm;
	while(pba <= last_pba) {
		e = rev_e->ptr_to_tm;
		printk(KERN_ERR "\n %s Looking for pba: %llu! Found e, LBA: %llu, PBA: %llu, len: %llu e->(pba+len): %llu, remaining: %llu", __func__, pba, e->lba, e->pba, e->len, e->pba + e->len, last_pba -(e->pba + e->len));
		
		/* Don't change e directly, as e belongs to the
		 * reverse map rb tree and we have the node address
		 */
		if (e->pba > last_pba) {
			//printk(KERN_ERR "\n %s Found bigger pba than last_pba: %llu , last_found_pba: %llu", __func__, last_pba, e->pba);
			break;
		}
		if (e->pba < pba) {
			/* 
			 * Overlapping e found
			 * e-------
			 * 	pba
			 */
			BUG_ON((e->pba + e->len) < pba);
			diff = pba - e->pba;
			temp.pba = pba;
			temp.lba = e->lba + diff;
			temp.len = e->len - diff;
			BUG_ON(!temp.len);
			printk(KERN_ERR "\n %s Adjusted pba (lba: %llu, pba: %llu len: %llu) last_pba: %lld", __func__, temp.lba, temp.pba, temp.len, last_pba);
		} else {
			/*
			 * 	e------
			 * pba
			 */
			temp.pba = e->pba;
			temp.lba = e->lba;
			temp.len = e->len;
			printk(KERN_ERR "\n %s Copied e (lba: %llu, pba: %llu len: %llu) last_pba: %lld", __func__, temp.lba, temp.pba, temp.len, last_pba);
		}
		/* if start is 0, len is 4, then you want to read 4 sectors. If last_pba is
		 * 3, you want len to be 4.
		 */
		if (temp.pba + temp.len >= last_pba + 1) {
			temp.len = last_pba - temp.pba + 1;
			printk(KERN_ERR "\n %s Adjusted len: (lba: %llu, pba: %llu len: %llu) last_pba: %lld", __func__, temp.lba, temp.pba, temp.len, last_pba);
		}
		total_len = total_len + temp.len;
		pba = temp.pba + temp.len;
		node = rb_next(&rev_e->rb);
		if (NULL == node) {
			printk(KERN_ERR "\n %s Found NULL! No next node!! last_pba: %llu , last_found_pba: %llu", __func__, last_pba, e->pba);
			break;
		}
		rev_e = container_of(node, struct rev_extent, rb);
	}
	printk(KERN_ERR "\n %s total_len: %ld vblks: %d \n", __func__, total_len, vblks);
	return 0;
}


int create_gc_extents(struct ctx *ctx, int zonenr)
{
	sector_t diff;
	struct extent *e = NULL;
	struct rev_extent *rev_e = NULL;
	struct extent_entry temp;
	long total_len = 0, total_extents = 0;
	int count;
	sector_t pba, last_pba; 
	int vblks;

	pba = get_first_pba_for_zone(ctx, zonenr);
	last_pba = get_last_pba_for_zone(ctx, zonenr);
	vblks = get_sit_ent_vblocks(ctx, zonenr);
	INIT_LIST_HEAD(&ctx->gc_extents->list);

	//print_memory_usage(ctx, "Before GC");
	/* Lookup this pba in the reverse table to find the
	 * corresponding LBA. 
	 * TODO: If the valid blocks are sequential, we need to keep
	 * this segment as an open segment that can append data. We do
	 * not need to perform GC on this segment.
	 */

	temp.pba = 0;
	temp.lba = 0;
	temp.len = 0;
	while(pba <= last_pba) {
		//printk(KERN_ERR "\n %s zonenr: %d first_pba: %llu last_pba: %llu #valid blks: %d", __func__, zonenr, pba, last_pba, vblks);
		rev_e = lsdm_rb_revmap_find(ctx, pba, 0, last_pba, __func__);
		//BUG_ON(NULL == rev_e);
		if (!rev_e) {
			/* this zone could be emptied by concurrent i/o */
			break;
		}
		e = rev_e->ptr_to_tm;
		BUG_ON(rev_e->pba != e->pba);
		BUG_ON((e->pba + e->len) < e->pba);
		e = rev_e->ptr_to_tm;
		//printk(KERN_ERR "\n %s Looking for pba: %llu! Found e, LBA: %llu, PBA: %llu, len: %u e->(pba+len): %llu, remaining: %d", __func__, pba, e->lba, e->pba, e->len, e->pba + e->len, last_pba -(e->pba + e->len));
		
		/* Don't change e directly, as e belongs to the
		 * reverse map rb tree and we have the node address
		 */
		if (e->pba > last_pba) {
			//printk(KERN_ERR "\n %s Found bigger pba than last_pba: %llu , last_found_pba: %llu", __func__, last_pba, e->pba);
			break;
		}
		if (e->pba < pba) {
			/* 
			 * Overlapping e found
			 * e-------
			 * 	pba
			 */
			if((e->pba + e->len) < pba) {
				printk(KERN_ERR "\n %s BUG: (GC) considering pba: %llu, found: (lba: %llu, pba: %llu len: %llu) last_pba: %llu", __func__, pba, e->lba, e->pba, e->len, last_pba);
				BUG();
			}
			diff = pba - e->pba;
			temp.pba = pba;
			temp.lba = e->lba + diff;
			temp.len = e->len - diff;
			if (!temp.len) {
				printk(KERN_ERR "\n %s BUG: (GC) considering pba: %llu, found: (lba: %llu, pba: %llu len: %llu) last_pba: %llu", __func__, pba, e->lba, e->pba, e->len, last_pba);
				BUG_ON(!temp.len);
			}
		} else {
			/*
			 * 	e------
			 * pba
			 */
			if (e->pba > last_pba)
				break;
			temp.pba = e->pba;
			temp.lba = e->lba;
			temp.len = e->len;
			//printk(KERN_ERR "\n %s Copied e (lba: %llu, pba: %llu len: %ld) last_pba: %lld", __func__, temp.lba, temp.pba, temp.len, last_pba);
		}
		/* if start is 0, len is 4, then you want to read 4 sectors. If last_pba is
		 * 3, you want len to be 4.
		 */
		if (temp.pba + temp.len >= last_pba + 1) {
			temp.len = last_pba - temp.pba + 1;
			//printk(KERN_ERR "\n %s Adjusted len: (lba: %llu, pba: %llu len: %ld) last_pba: %lld", __func__, temp.lba, temp.pba, temp.len, last_pba);
		}
		if (!temp.len) {
			printk(KERN_ERR "\n %s !!!!!! Copied e (lba: %llu, pba: %llu len: %llu) last_pba: %llu", __func__, e->lba, e->pba, e->len, last_pba);
			break;
		}
		total_len = total_len + temp.len;
		pba = temp.pba + temp.len;
		count = add_extent_to_gclist(ctx, &temp);
		total_extents = total_extents + count;
	}
#ifdef LSDM_DEBUG
	printk(KERN_ERR "\n %s Total extents: %llu, total_len: %llu vblks: %d \n", __func__, total_extents, total_len, vblks);
	if ((total_len >> SECTOR_BLK_SHIFT) < vblks) {
		print_gc_extents(ctx, zonenr);
	}
#endif
	return total_extents;
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
static int lsdm_gc(struct ctx *ctx, int gc_mode, int err_flag)
{
	int zonenr;
	int ret;
	struct lsdm_ckpt *ckpt = NULL;
	struct lsdm_gc_thread *gc_th = ctx->gc_th;
	wait_queue_head_t *wq = &gc_th->lsdm_gc_wait_queue;
	u64 start_t, end_t, interval = 0, gc_count = 0;
	u64 gc_writes = 0;

	//printk(KERN_ERR "\a %s * GC thread polling after every few seconds! gc_mode: %d \n", __func__, gc_mode);
	
	/* Take a semaphore lock so that no two gc instances are
	 * started in parallel.
	 */
	if (!mutex_trylock(&ctx->gc_lock)) {
		printk(KERN_ERR "\n 1. GC is already running! \n");
		return -1;
	}
again:
	if (kthread_should_stop()) {
		printk(KERN_ERR "\n kthread needs to stop ");
		goto failed;
	}
	interval = 0;
	start_t = ktime_get_ns();	
	zonenr = select_zone_to_clean(ctx, gc_mode, __func__);
	if (zonenr < 0) {
		printk(KERN_ERR "\n No zone found for cleaning!! \n");
		if (!gc_count) {
			gc_count = -1;
		}
		goto failed;
	}
	//flush_workqueue(ctx->tm_wq);
	//flush_workqueue(ctx->writes_wq);
	//printk(KERN_ERR "\n Running GC!! zone_to_clean: %u  mode: %d", zonenr, gc_mode);
	if (unlikely(!list_empty(&ctx->gc_extents->list))) {
		free_gc_extents(ctx);
		printk(KERN_ERR "\n %s ************ extent list is not empty! Line: %d ", __func__, __LINE__);
	}

	if (kthread_should_stop()) {
		printk(KERN_ERR "\n kthread needs to stop ");
		goto failed;
	}

	create_gc_extents(ctx, zonenr);

	//print_memory_usage(ctx, "After extents");
	if (list_empty(&ctx->gc_extents->list)) {
		/* Nothing to do, sit_ent_vblocks_decr() is playing catch up with gc cost tree */
		remove_zone_from_gc_tree(ctx, zonenr);	
		goto again;
	}

	//printk(KERN_ERR "\n %s zonenr: %d about to be read, vblocks: %d  \n", __func__, zonenr, get_sit_ent_vblocks(ctx, zonenr));
	ret = read_gc_extents(ctx);
	if (ret)
		goto failed;

	//print_memory_usage(ctx, "After read");
	if (gc_mode != FG_GC) {
		if (ctx->nr_freezones < ctx->middle_watermark) {
			/* while gc thread was running, urgent mode triggered */
			gc_mode = FG_GC;
			gc_th->gc_wake = 1;
		}
	}
	/* if we are in concurrent mode, we can afford to let the application i/o go ahead */
	if (gc_mode == CONC_GC) {
		gc_th->gc_wake = 0;
		wait_event_interruptible_timeout(*wq,
			kthread_should_stop() || freezing(current) ||
			gc_th->gc_wake,
			msecs_to_jiffies(gc_th->urgent_sleep_time));
                if (gc_th->gc_wake) {
			if (ctx->nr_freezones <= ctx->middle_watermark) {
				gc_mode = FG_GC;
			}
		}
		gc_th->gc_wake = 1;
	}
	else if (gc_mode == BG_GC) {
	       	if (!is_lsdm_ioidle(ctx)) {
                	if (!gc_th->gc_wake) {
				/* BG_GC mode and not idle */
				wait_event_interruptible_timeout(*wq,
					kthread_should_stop() || freezing(current) ||
					gc_th->gc_wake,
					msecs_to_jiffies(gc_th->urgent_sleep_time));
			}
			if (ctx->nr_freezones <= ctx->higher_watermark) {
				gc_mode = CONC_GC;
				if (ctx->nr_freezones <= ctx->middle_watermark) {
					gc_mode = FG_GC;
				}
				gc_th->gc_wake = 1;
			}
		} /* else, we are in BG GC and are io idle, so do not wait, continue */
	}
	if (kthread_should_stop()) {
		printk(KERN_ERR "\n kthread needs to stop ");
		goto failed;
	}

	//printk(KERN_ERR "\n %s zonenr: %d about to be written, vblocks: %d  \n", __func__, zonenr, get_sit_ent_vblocks(ctx, zonenr));
	ret = write_valid_gc_extents(ctx, zonenr);
	if (ret < 0) { 
		printk(KERN_ERR "\n write_valid_gc_extents() failed, ret: %d ", ret);
		goto failed;
	}
	gc_writes += ret;
	//print_memory_usage(ctx, "After write");

	ckpt = (struct lsdm_ckpt *)page_address(ctx->ckpt_page);
	ckpt->clean = 0;
	do_checkpoint(ctx);
	end_t = ktime_get_ns();	
	interval += (end_t - start_t)/1000000;
	gc_count++;
	ctx->gc_total += interval;
	ctx->gc_count += gc_count;
	ctx->gc_average = ctx->gc_total/ ctx->gc_count;
	printk(KERN_ERR "\n %s gc_count: %llu total time: %llu (milliseconds) gc_writes: %llu gc_mode:%d ", __func__, gc_count, interval, gc_writes, gc_mode);
	gc_writes = 0;
	//printk(KERN_ERR "\n %s zonenr: %d cleaned! #valid blks: %d \n", __func__, zonenr, get_sit_ent_vblocks(ctx, zonenr));
	/* while gc thread was running, urgent mode triggered */
	if (ctx->nr_freezones <= ctx->higher_watermark) {
		gc_mode = CONC_GC;
		if (ctx->nr_freezones <= ctx->middle_watermark) {
			gc_mode = FG_GC;
		}
	}

	/* TODO: Mode: FG_GC */
	drain_workqueue(ctx->tm_wq);
	if (gc_mode == FG_GC) {
		wake_up_all(&ctx->gc_th->fggc_wq);
		io_schedule();
		if (ctx->nr_freezones <= ctx->middle_watermark) {
			goto again;
		}
		/* else: Paused GC mode */
		gc_mode = CONC_GC;
		io_schedule();
	} 
	if (gc_mode == CONC_GC) {
		/* if we are in concurrent mode, we can afford to let the application i/o go ahead */
		wait_event_interruptible_timeout(*wq,
			kthread_should_stop() || freezing(current) ||
			gc_th->gc_wake,
			msecs_to_jiffies(gc_th->urgent_sleep_time));
		if (ctx->nr_freezones <= ctx->middle_watermark) {
			gc_mode = FG_GC;
		}
		if (ctx->nr_freezones <= ctx->higher_watermark) {
			goto again;
		}
		// else, you dont need to do CONC_GC or FG_GC any more;
	}
	else if (gc_mode == BG_GC) {
		if (is_lsdm_ioidle(ctx))
			goto again;
		/* else we need to stop */
	}
failed:
	free_gc_extents(ctx);
	mutex_unlock(&ctx->gc_lock);
	if (gc_th->gc_wake) {
		gc_th->gc_wake = 0;
		wake_up_all(&ctx->gc_th->fggc_wq);
	}
	return gc_count;
}

/* printing in order */

void print_sub_extents(struct rb_node *parent)
{
	struct extent *e;
	struct rev_extent *r_e;
	if (!parent)
		return;

	e = rb_entry(parent, struct extent, rb);
	r_e = e->ptr_to_rev;
	BUG_ON(e->pba != r_e->pba);
	print_sub_extents(parent->rb_left);
	printk(KERN_ERR "\n %s lba: %llu, pba: %llu, len: %llu", __func__, e->lba, e->pba, e->len);
	print_sub_extents(parent->rb_right);
}


void print_extents(struct ctx *ctx)
{
	struct rb_root * root = &ctx->extent_tbl_root;

	printk(KERN_ERR "\n-------------------------------------------------\n");
	print_sub_extents(root->rb_node);
	printk(KERN_ERR "\n-------------------------------------------------\n");
}


/*
 * TODO: When we want to mark a zone free create a bio with opf:
 * REQ_OP_ZONE_RESET
 *
 */
static int gc_thread_fn(void * data)
{

	struct ctx *ctx = (struct ctx *) data;
	struct lsdm_gc_thread *gc_th = ctx->gc_th;
	wait_queue_head_t *wq = &gc_th->lsdm_gc_wait_queue;
	unsigned int wait_ms;
	int mode = BG_GC, ret = 0;
	struct task_struct *tsk = gc_th->lsdm_gc_task;
	u64 start_t, end_t, interval = 0;

	wait_ms = gc_th->min_sleep_time;
	//wait_ms = gc_th->no_gc_sleep_time;
	mutex_init(&ctx->gc_lock);
	printk(KERN_ERR "\n %s executing! pid: %d", __func__, tsk->pid);
	set_freezable();
	ctx->gc_th->gc_wake = 0;
	ctx->gc_count  = 0;
	ctx->gc_average = 0;
	ctx->gc_total = 0;
	ctx->nr_gc_writes = 0;
	do {
		wait_event_interruptible_timeout(*wq,
			kthread_should_stop() || freezing(current) ||
			gc_th->gc_wake,
			msecs_to_jiffies(wait_ms));

                if (kthread_should_stop()) {
			printk(KERN_ERR "\n GC thread is stopping! ");
                        break;
		}
		//print_extents(ctx);
		 /* give it a try one time */
                if (gc_th->gc_wake) {
			if (ctx->nr_freezones < ctx->lower_watermark) {
				/* concurrent GC, no pauses */
				mode = FG_GC;
			} else {
				/* concurrent, intermittent GC */
				mode = CONC_GC;
			}
		}
		else if(mode == BG_GC) {
			if (!is_lsdm_ioidle(ctx)) {
				/* increase sleep time */
				printk(KERN_ERR "\n %s not ioidle! \n ", __func__);
				/*
				wait_ms = wait_ms * 2;
				if (wait_ms > gc_th->max_sleep_time) {
					wait_ms = gc_th->max_sleep_time;
				}*/
				continue;
			}
			printk(KERN_ERR "\n Starting BG GC ");
			start_t = ktime_get_ns();
		}
		ctx->flush_th->sleep_time = DEF_GC_TIME;
		/* Doing this for now! ret part */
		ret = lsdm_gc(ctx, mode, 0);
		ctx->flush_th->sleep_time = DEF_FLUSH_TIME;
		if (mode == BG_GC) {
			end_t = ktime_get_ns();
			interval += (end_t - start_t)/1000000;
			if (ret > 0)
				printk(KERN_ERR "\n %s BG_GC: gc_count: %d total time: %llu (milliseconds)", __func__, ret, interval);
			wait_ms = gc_th->min_sleep_time;
		} else {
			ctx->gc_th->gc_wake = 0;

		} 
		if (ret < 0) {
			wait_ms = wait_ms * 2;
			if (wait_ms > gc_th->max_sleep_time) {
				wait_ms = gc_th->max_sleep_time;
			}
			continue;

		}
	} while(!kthread_should_stop());
	return 0;
}
#define DEF_GC_THREAD_URGENT_SLEEP_TIME 500     /* 500 ms */
#define DEF_GC_THREAD_MIN_SLEEP_TIME    60000   /* milliseconds */
#define DEF_GC_THREAD_MAX_SLEEP_TIME    120000
#define DEF_GC_THREAD_NOGC_SLEEP_TIME   15000000 /* wait 200 min */
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
	init_waitqueue_head(&gc_th->fggc_wq);
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
	printk(KERN_ERR "\n GC thread stopped! ");
	wake_up_all(&ctx->gc_th->fggc_wq);
	kvfree(ctx->gc_th);
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


void lsdm_ioidle(struct mykref *kref)
{
	struct ctx *ctx;
	ctx = container_of(kref, struct ctx, ongoing_iocount);

	//printk(KERN_ERR "\n LSD is io idle! ctx: %p\n", __func__, ctx);
	atomic_set(&ctx->ioidle, 1);
}


void no_op(struct kref *kref) { }
int is_zone_free(struct ctx *ctx, unsigned int zonenr);

void lsdm_subread_done(struct bio *clone)
{
	struct app_read_ctx *read_ctx = clone->bi_private;
	struct ctx *ctx = read_ctx->ctx;
	struct bio * bio = read_ctx->bio;
	u64 zonenr;

	if (read_ctx->pba < ctx->sb->max_pba) {
		zonenr = get_zone_nr(ctx, read_ctx->pba);
		if ((zonenr > 0) && (zonenr < ctx->sb->zone_count)) {
			if (is_zone_free(ctx, zonenr)) {
				/* fail this read */
				bio->bi_status = BLK_STS_AGAIN;
			}
		}
	}
	//printk(KERN_ERR "\n %s read lba: %llu ! \n", __func__, read_ctx->lba);
	bio_endio(bio);
	mykref_put(&ctx->ongoing_iocount, lsdm_ioidle);
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
	return 0;
}

void complete_small_reads(struct bio *clone)
{
	struct bio_vec bv;
	struct bvec_iter iter;
	char * todata = NULL, *fromdata = NULL;
	struct app_read_ctx *readctx = clone->bi_private;
	sector_t lba = round_down(readctx->lba, NR_SECTORS_IN_BLK);
	sector_t nrsectors = readctx->nrsectors;
	unsigned long diff = 0;

	if (clone->bi_status != BLK_STS_OK) {
		readctx->clone->bi_status = clone->bi_status;
		goto free;
	}

	//printk(KERN_ERR "\n %s smaller bio's address: %p larger bio's address: %p", __func__, clone, readctx->clone);
	//printk(KERN_ERR "\n readctx->lba: %llu, lba: %llu NR_SECTORS_IN_BLK: %d \n", readctx->lba, lba, NR_SECTORS_IN_BLK);
	if ((readctx->lba - lba) > NR_SECTORS_IN_BLK) {
		goto free;
		//BUG();
	}
	diff = (readctx->lba - lba) << LOG_SECTOR_SIZE;
	//printk(KERN_ERR "\n %s 1. diff: %llu nrsectors: %d \n", __func__, diff, nrsectors);
	bio_for_each_segment(bv, readctx->clone, iter) {
		todata = page_address(bv.bv_page);
		todata = todata + bv.bv_offset;
		break;
	}
	fromdata = readctx->data;
	memcpy(todata, fromdata + diff, (nrsectors << LOG_SECTOR_SIZE));
	//printk(KERN_ERR "\n %s todata: %p, fromdata: %p diff: %d  bytes: %d \n", __func__, todata, fromdata, diff, (nrsectors << LOG_SECTOR_SIZE));
free:
	readctx->clone->bi_end_io = lsdm_subread_done;
	bio_endio(readctx->clone);
	bio_free_pages(clone);
	bio_put(clone);
}


struct bio * construct_smaller_bios(struct ctx * ctx, sector_t pba, struct app_read_ctx * readctx)
{
	struct page *page; 
	struct bio * bio;
	struct bio_vec bv;
	struct bvec_iter iter;
	char * data;

	page = alloc_page(__GFP_ZERO|GFP_KERNEL);
	if (!page )
		return NULL;

	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_READ, GFP_KERNEL);
	if (!bio) {
		__free_pages(page, 0);
		nrpages--;
		//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return NULL;
	}

	printk(KERN_ERR "\n %s smaller bio's address: %p larger bio address: %p", __func__, bio, readctx->clone);
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
		bio_put(bio);
		printk(KERN_ERR "\n Inside %s 2 - Going.. Bye!! \n", __func__);
		return NULL;
	}
	bio_for_each_segment(bv, bio, iter) {
		page = bv.bv_page;
		data = page_address(page);
		data = data + bv.bv_offset;
		//printk(KERN_ERR "\n %s (new small bio) data: %p ", __func__, data);
		readctx->data = data;
	}
	bio->bi_opf = REQ_OP_READ;
	//bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_iter.bi_sector = pba;
	bio->bi_end_io = complete_small_reads;
	bio->bi_private = readctx;
	readctx->pba = pba;
	return bio;
}

void request_start_unaligned(struct ctx *ctx, struct bio *clone, struct app_read_ctx *read_ctx, sector_t pba, sector_t zerolen)
{
	sector_t aligned_pba;
	sector_t diff;

	aligned_pba = round_down(pba, NR_SECTORS_IN_BLK);
	diff = pba - aligned_pba;
	bio_advance(clone, zerolen);
	clone = construct_smaller_bios(ctx, aligned_pba, read_ctx);
	if (clone  == NULL) {
		printk(KERN_ERR "\n %s could not construct smaller bio! \n", __func__);
		bio_endio(read_ctx->clone);
	}
	read_ctx->data = read_ctx->data + (diff << LOG_SECTOR_SIZE);
	//read_ctx->more = 1;
	submit_bio(clone);
	/* We have to make sure that complete_smaller_bio() gets called */
}


int zero_fill_inital_bio(struct ctx *ctx, struct bio *bio, struct bio *clone, sector_t zerolen, struct app_read_ctx *read_ctx)
{
	struct bio * split;

	//printk(KERN_ERR "\n 1.  %s lba: %llu > pba: 0 len: %d \n", __func__, lba, 0, zerolen);
	split = bio_split(clone, zerolen, GFP_NOIO, &fs_bio_set);
	if (!split) {
		printk(KERN_ERR "\n Could not split the clone! ERR ");
		bio->bi_status = -ENOMEM;
		clone->bi_status = BLK_STS_RESOURCE;
		zero_fill_clone(ctx, read_ctx, clone);
		return -ENOMEM;
	}
	ctx->nr_reads += zerolen;
	bio_chain(split, clone);
	zero_fill_bio(split);
	bio_endio(split);
	return 0;
}

int handle_partial_overlap(struct ctx *ctx, struct bio * bio, struct bio *clone, sector_t overlap, struct app_read_ctx *read_ctx, sector_t pba)
{
	struct bio * split;

	//printk(KERN_ERR "\n clone: %p clone::nr_sectors: %d len: %d ", clone, bio_sectors(clone), overlap);
	split = bio_split(clone, overlap, GFP_KERNEL, &fs_bio_set);
	if (!split) {
		printk(KERN_INFO "\n Could not split the clone! ERR ");
		bio->bi_status = -ENOMEM;
		clone->bi_status = BLK_STS_RESOURCE;
		zero_fill_clone(ctx, read_ctx, clone);
		return -ENOMEM;
	}
	ctx->nr_reads += overlap;
	//printk(KERN_ERR "\n 2. (SPLIT) %s lba: %llu, pba: %llu nr_sectors: %d ", __func__, split->bi_iter.bi_sector, pba, bio_sectors(split));
	bio_chain(split, clone);
	split->bi_iter.bi_sector = pba;
	read_ctx->pba = pba;
	bio_set_dev(split, ctx->dev->bdev);
	BUG_ON(pba > ctx->sb->max_pba);
	//printk(KERN_ERR "\n %s submitted split! ", __func__);
	submit_bio_noacct(split);
	return 0;
}


int handle_full_overlap(struct ctx *ctx, struct bio * bio, struct bio *clone, sector_t nr_sectors, sector_t pba, struct app_read_ctx *read_ctx, int print)
{
	sector_t s8;
	struct bio *split;
	BUG_ON(pba > ctx->sb->max_pba);

	if (print)
		printk(KERN_ERR "\n %s pba: %llu len: %llu \n", __func__, pba, nr_sectors);

	s8 = round_down(nr_sectors, NR_SECTORS_IN_BLK);
	if (nr_sectors == s8) {
		if (print)
			printk(KERN_ERR "\n %s aligned read \n", __func__);
		ctx->nr_reads += s8;
		clone->bi_end_io = lsdm_subread_done;
		clone->bi_iter.bi_sector = pba;
		read_ctx->pba = pba;
		bio_set_dev(clone, ctx->dev->bdev);
		if (print)
			printk(KERN_ERR "\n %s aligned read submitting....\n", __func__);
		submit_bio_noacct(clone);
	} else {
		if (print)
			printk(KERN_ERR "\n %s Unaligned read \n", __func__);
		/* nr_sectors is not divisible by NR_SECTORS_IN_BLK*/
		if (nr_sectors > NR_SECTORS_IN_BLK) {
			ctx->nr_reads += s8;
			split = bio_split(clone, s8, GFP_NOIO, &fs_bio_set);
			if (!split) {
				printk(KERN_INFO "\n Could not split the clone! ERR ");
				bio->bi_status = -ENOMEM;
				clone->bi_status = BLK_STS_RESOURCE;
				zero_fill_clone(ctx, read_ctx, clone);
				return -ENOMEM;
			}
			bio_chain(split, clone);
			split->bi_iter.bi_sector = pba;
			read_ctx->pba = pba;
			bio_set_dev(split, ctx->dev->bdev);
			BUG_ON(pba > ctx->sb->max_pba);
			submit_bio_noacct(split);
			nr_sectors = bio_sectors(clone);
			pba = pba + s8;
			/* let it fall through to the next case */
		}
		//(nr_sectors < NR_SECTORS_IN_BLK)
		read_ctx->nrsectors = nr_sectors;
		read_ctx->lba = clone->bi_iter.bi_sector;
		read_ctx->clone = clone;
		read_ctx->pba = pba;
		ctx->nr_reads += nr_sectors;
		clone = construct_smaller_bios(ctx, pba, read_ctx);
		if (clone  == NULL) {
			printk(KERN_ERR "\n %s could not construct smaller bio! \n", __func__);
			bio_endio(read_ctx->clone);
		}
		if (print)
			printk(KERN_ERR "\n %s (smaller read) -> lba: %llu pba:%llu len:%llu", __func__, read_ctx->lba, clone->bi_iter.bi_sector, nr_sectors);
		submit_bio_noacct(clone);
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
static int lsdm_read_io(struct ctx *ctx, struct bio *bio)
{
	struct bio *split = NULL;
	sector_t lba, pba, origlba;
	struct extent *e;
	unsigned nr_sectors, overlap, diff, zerolen;

	struct app_read_ctx *read_ctx;
	struct bio *clone;
	int print = 0, ret = 0;

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
	mykref_get(&ctx->ongoing_iocount);
	clone = bio_alloc_clone(ctx->dev->bdev, bio, GFP_KERNEL, &fs_bio_set);
	if (!clone) {
		printk(KERN_ERR "\n %s could not allocate memory to clone", __func__);
		kmem_cache_free(ctx->app_read_ctx_cache, read_ctx);
		bio->bi_status = -ENOMEM;
		bio_endio(bio);
		return -ENOMEM;
	}
	//printk(KERN_ERR "\n %s Read bio::lba: %llu bio::nrsectors: %d", __func__, clone->bi_iter.bi_sector, bio_sectors(clone));

	clone->bi_private = read_ctx;
	bio_set_dev(clone, ctx->dev->bdev);
	split = NULL;
	origlba = clone->bi_iter.bi_sector;
	read_ctx->lba = origlba;
	while(split != clone) {
		nr_sectors = bio_sectors(clone);
		origlba = clone->bi_iter.bi_sector;
		lba = round_down(origlba, NR_SECTORS_IN_BLK);
		e = lsdm_rb_geq(ctx, lba, print);

		/* case of no overlap */
		if ((e == NULL) || (e->lba >= (lba + nr_sectors)) || ((e->lba + e->len) <= lba))  {
			zero_fill_clone(ctx, read_ctx, clone);
			break;
		}

		/* Case of Overlap, e always overlaps with bio */
		if (e->lba > lba) {
		/*   		 [eeeeeeeeeeee]
		 *	[---------bio------] 
		 */
			zerolen = e->lba - lba;
			if (origlba >= 15597042700) {
				printk(KERN_ERR "\n %s Case of partial overlap! (no left overlap), origlba: %llu", __func__, origlba);
			}
			ret = zero_fill_inital_bio(ctx, bio, clone, zerolen, read_ctx);
			if (!ret)
				return ret;
			/* bio is front filled with zeroes, but we need to compare 'clone' now with
			 * the same e
			 */
			lba = lba + zerolen;
			nr_sectors = bio_sectors(clone);
			BUG_ON(lba != e->lba);
			/* we fall through as e->lba == lba now */
		} 
		//(e->lba <= lba) 
		/* [eeeeeeeeeeee] eeeeeeeeeeeee]<- could be shorter or longer
		 */
		/*  [---------bio------] */
		overlap = e->lba + e->len - lba;
		diff = lba - e->lba;
		BUG_ON(diff < 0);
		pba = e->pba + diff;
		if (origlba >= 15597042700) {
			printk(KERN_ERR "\n %s overlap: %u, nr_sectors: %u diff: %u lba: %llu, pba: %llu", __func__, overlap, nr_sectors, diff, lba, pba);
		}
		if (overlap >= nr_sectors) { 
		/* e is bigger than bio, so overlap >= nr_sectors, no further
		 * splitting is required. Previous splits if any, are chained
		 * to the last one as 'clone' is their parent.
		 */
			ret = handle_full_overlap(ctx, bio, clone, nr_sectors, pba, read_ctx, (origlba >= 15597042700) ? 1: 0);
			//printk(KERN_ERR "\n 1) ret: %d \n", ret);
			if (ret)
				return ret;
			break;

		} else {
			/* overlap is smaller than nr_sectors remaining. */
			//printk(KERN_ERR "\n clone: %p clone::nr_sectors: %d e->len: %d overlap: %d", clone, bio_sectors(clone), e->len, overlap);
			ret = handle_partial_overlap(ctx, bio, clone, overlap, read_ctx, pba);
			//printk(KERN_ERR "\n 2) ret: %d clone::lba: %llu", ret, clone->bi_iter.bi_sector);
			if (ret)
				return ret;
			/* Since e was smaller, we want to search for the next e */
		}
	}
	//printk(KERN_INFO "\t %s end \n", __func__);
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
	int index, ret;
	int zonenr, destn_zonenr;
	struct lsdm_sb * sb = ctx->sb;

	//mutex_lock(&ctx->sit_kv_store_lock);
	/*-----------------------------------------------*/
	sit_page = add_sit_page_kv_store(ctx, pba, __func__);
	if (!sit_page) {
		/* TODO: do something, low memory */
		panic("Low memory, couldnt allocate sit entry");
	}
	/*-----------------------------------------------*/
	//mutex_unlock(&ctx->sit_kv_store_lock);

	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	ptr->vblocks = 1 << (sb->log_zone_size - sb->log_block_size);
	ptr->mtime = 0;

	//mutex_lock(&ctx->bm_lock);
	/*-----------------------------------------------*/
	ctx->nr_invalid_zones++;
	ret = get_new_zone(ctx);
	/*-----------------------------------------------*/
	//mutex_unlock(&ctx->bm_lock);
	if (ret > pba) {
		printk(KERN_INFO "No more disk space available for writing!");
		return;
	}
	destn_zonenr = get_zone_nr(ctx, ctx->hot_wf_pba);
	//copy_blocks(zonenr, destn_zonenr); 
}

int is_zone_free(struct ctx *ctx, unsigned int zonenr)
{
	char *bitmap;
	unsigned int bytenr, bitnr;
	int result = 0;

	if (unlikely(NULL == ctx)) {
		panic("This is a ctx bug");
	}
		
	bitmap = ctx->freezone_bitmap;
	bytenr = zonenr / BITS_IN_BYTE;
	bitnr = zonenr % BITS_IN_BYTE;
	//printk(KERN_ERR "\n %s zonenr: %lu, bytenr: %d bitnr: %d bitmap[%d]: %d ", __func__, zonenr, bytenr, bitnr,  bytenr, bitmap[bytenr]);

	if(unlikely(bytenr >= ctx->bitmap_bytes)) {
		panic("bytenr: %d > bitmap_bytes: %d", bytenr, ctx->bitmap_bytes);
	}

	if(unlikely((bytenr == (ctx->bitmap_bytes-1)) && (bitnr > ctx->bitmap_bit))) {
		panic("bytenr: %d, bitnr: %d > bitmap_bytes: %d, bitnr: %d", bytenr, bitnr, ctx->bitmap_bytes, ctx->bitmap_bit);
	}

	if (unlikely(NULL == bitmap)) {
		panic("This is a ctx freezone bitmap bug!");
	}


	//mutex_lock(&ctx->bm_lock);
	if ((bitmap[bytenr] & (1 << bitnr)) == (1<<bitnr)) {
		/* This bit was 1 and hence already free*/
		result = 1;
	}
	//mutex_unlock(&ctx->bm_lock);
	return result;
}

void get_byte_string(char byte, char *str)
{
	int i=7;
	while(byte) {
		if (byte & 1) {
			str[i] = '1';
		}
		else {
			str[i]='0';
		}
		i = i - 1;
		byte = byte >> 1;
		if (i < 0)
			break;
	}
	str[8] = '\0';
}



/* 
 * 1 indicates that the zone is free 
 *
 * Zone numbers start from 0
 */
static void mark_zone_free(struct ctx *ctx , int zonenr, int resetZone)
{	
	char *bitmap;
	int bytenr, bitnr, ret = 0;
	//char str[9];

	if (unlikely(NULL == ctx)) {
		printk(KERN_ERR "\n ctx is null! ");
		return;
	}
	/* zone is not marked free yet, so will not be selected for writing */

	bitmap = ctx->freezone_bitmap;
	bytenr = zonenr / BITS_IN_BYTE;
	bitnr = zonenr % BITS_IN_BYTE;


	if (unlikely(NULL == bitmap)) {
		printk(KERN_ERR "\n This is a ctx freezone bitmap bug!");
		return;
	}

	if(unlikely(bytenr >= ctx->bitmap_bytes)) {
		printk(KERN_ERR "\n 1) BUG %s zonenr: %d, bytenr: %d bitnr: %d bitmap[%d]: %d  bitmap_bytes: %d", __func__, zonenr, bytenr, bitnr,  bytenr, bitmap[bytenr], ctx->bitmap_bytes);
		return;
	}

	if(unlikely((bytenr == ctx->bitmap_bytes) && (bitnr > ctx->bitmap_bit))) {
		printk(KERN_ERR "\n 2) BUG %s zonenr: %d, bytenr: %d bitnr: %d bitmap[%d]: %d  bitmap_bytes: %d", __func__, zonenr, bytenr, bitnr,  bytenr, bitmap[bytenr], ctx->bitmap_bytes);
		return;
	}

	if ((bitmap[bytenr] & (1 << bitnr)) == (1<<bitnr)) {
		/* This bit was 1 and hence already free*/
		printk(KERN_ERR "\n BUG Trying to free an already free zone! ");
		printk(KERN_ERR "\n %s zonenr: %d, bytenr: %d bitnr: %d bitmap[%d]: %d  bitmap_bytes: %d", __func__, zonenr, bytenr, bitnr,  bytenr, bitmap[bytenr], ctx->bitmap_bytes);
		return;
	}

	if (resetZone && (zonenr > ctx->sb->nr_cmr_zones)) {
		//printk(KERN_ERR "\n %s reset zone: %d  \n ", __func__, zonenr);
		ret = blkdev_zone_mgmt(ctx->dev->bdev, REQ_OP_ZONE_RESET, get_first_pba_for_zone(ctx, zonenr), ctx->sb->nr_lbas_in_zone, GFP_NOIO);
		if (ret ) {
			printk(KERN_ERR "\n Failed to reset zonenr: %d, retvalue: %d", zonenr, ret);
		}
	}

	/* The bit was 0 as its in use and hence we or it with
	 * 1 to set it
	 */
	bitmap[bytenr] = bitmap[bytenr] | (1 << bitnr);
	ctx->nr_freezones = ctx->nr_freezones + 1;
	//get_byte_string(bitmap[bytenr], str);
	//printk(KERN_ERR "\n %s Freed zonenr: %d, bytenr: %d, bitnr: %d byte:%s", __func__, zonenr, bytenr, bitnr, str);
	/* we need to reset the  zone that we are about to use */
}

static int mark_zone_occupied(struct ctx *, int );
static int get_next_freezone_nr(struct ctx *ctx)
{
	char *bitmap = ctx->freezone_bitmap;
	int bytenr, bitnr;
	unsigned char allZeroes = 0;
	int zonenr;

	bytenr = 0;
	/* 1 indicates that a zone is free. 
	 */
	while(bitmap[bytenr] == allZeroes) {
		/* All these zones are occupied */
		bytenr = bytenr + 1;
		if (unlikely(bytenr == ctx->bitmap_bytes)) {
		/* no freezones available */
			printk(KERN_ERR "\n No free zone available, disk is full! \n");
			return -1;
		}
	}
	
	/* We have the bytenr from where to return the freezone */
	bitnr = 0;
	while (1) {
		if(1 & (bitmap[bytenr] >> bitnr)) {
			/* this bit is free, hence marked 1 */
			break;
		}
		bitnr = bitnr + 1;
		if(unlikely((bytenr == (ctx->bitmap_bytes-1)) && (bitnr == ctx->bitmap_bit))) {
			printk(KERN_ERR "\n 2) No free zone available, disk is full! \n");
			return -1;
		}
		if (bitnr == BITS_IN_BYTE) {
			panic ("Wrong byte calculation!");
		}
	}
	zonenr = (bytenr * BITS_IN_BYTE) + bitnr;
	if (mark_zone_occupied(ctx, zonenr))
		BUG();

	return zonenr;
}

void wait_on_zone_barrier(struct ctx *);
static void add_ckpt_new_wf(struct ctx *, sector_t);
static void add_ckpt_new_gc_wf(struct ctx * ctx, sector_t wf);

/* moves the write frontier, returns the LBA of the packet trailer
 * Always called with the ctx->wf_lock held.
*/
static int get_new_zone(struct ctx *ctx)
{
	int zone_nr;
	int trial;

	trial = 0;
try_again:
	zone_nr = get_next_freezone_nr(ctx);
	if (zone_nr < 0) {
		printk(KERN_ERR "\n Could not find a clean zone for writing. Calling lsdm_gc \n");
		if (0 == trial) {
			trial++;
			goto try_again;
		}
		printk(KERN_INFO "No more disk space available for writing!");
		mark_disk_full(ctx);
		ctx->hot_wf_pba = 0;
		return -1;
	}
	//trace_printk("%s new-zonenr: %d ", __func__, zone_nr);

	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->hot_wf_pba = get_first_pba_for_zone(ctx, zone_nr);
	ctx->hot_wf_end = get_last_pba_for_zone(ctx, zone_nr);

	//printk(KERN_ERR "\n !!get_new_zone():: zone0_pba: %u zone_nr: %d hot_wf_pba: %llu, wf_end: %llu ctx->warm_wf_pba: %llu \n", ctx->sb->zone0_pba, zone_nr, ctx->hot_wf_pba, ctx->hot_wf_end, ctx->warm_gc_wf_pba);
	if (ctx->hot_wf_pba > ctx->hot_wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %lld", ctx->free_sectors_in_wf );
	}
	ctx->free_sectors_in_wf = ctx->hot_wf_end - ctx->hot_wf_pba + 1;
	//printk(KERN_ERR "\n %s zone_nr: %d  ctx->nr_freezones: %llu", __func__, zone_nr, ctx->nr_freezones);
	add_ckpt_new_wf(ctx, ctx->hot_wf_pba);
	return 0;
}

void print_zones_vblocks(struct ctx *ctx);

/*
 * moves the write frontier, returns the LBA of the packet trailer
 * We know that only one GC thread is working at a time. Thus we take a 
 * lock only while getting a free zone as that can clash with writes moving
 * the frontier and asking for a new zone. We dont want to end up with the same zones
 * for both! get_next_freezone_nr() also marks a zone as not free. We want to do that
 * under a lock. 
*/
static int get_new_gc_zone(struct ctx *ctx)
{
	int zone_nr;
	int trial;

	trial = 0;
again:
	mutex_lock(&ctx->bm_lock);
	zone_nr = get_next_freezone_nr(ctx);
	mutex_unlock(&ctx->bm_lock);
	if (zone_nr < 0) {
		trial++;
		flush_workqueue(ctx->tm_wq);
		if (trial < 2) 
			goto again;
		print_zones_vblocks(ctx);
		mark_disk_full(ctx);
		printk(KERN_ERR "No more disk space available for writing!");
		ctx->warm_gc_wf_pba = ~0;
		BUG_ON(1);
		return -1;
	}

	/* get_next_freezone_nr() starts from 0. We need to adjust
	 * the pba with that of the actual first PBA of data segment 0
	 */
	ctx->warm_gc_wf_pba = get_first_pba_for_zone(ctx, zone_nr);
	ctx->warm_gc_wf_end = get_last_pba_for_zone(ctx, zone_nr);
	if (ctx->warm_gc_wf_pba > ctx->warm_gc_wf_end) {
		panic("wf > wf_end!!, nr_free_sectors: %llu", ctx->free_sectors_in_wf );
	}
	BUG_ON(ctx->warm_gc_wf_end > ctx->sb->max_pba);
	ctx->free_sectors_in_gc_wf = ctx->warm_gc_wf_end - ctx->warm_gc_wf_pba + 1;
	add_ckpt_new_gc_wf(ctx, ctx->warm_gc_wf_pba);
	//printk("\n %s zone0_pba: %llu zone_nr: %d warm_gc_wf_pba: %llu, gc_wf_end: %llu", __func__,  ctx->sb->zone0_pba, zone_nr, ctx->warm_gc_wf_pba, ctx->warm_gc_wf_end);
	return 0;
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

	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_WRITE, GFP_KERNEL);
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
	bio->bi_opf = REQ_OP_WRITE;
	//bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	/* Not doing this right now as the conventional zones are too little.
	 * we need to log structurize the metadata
	 */
	BUG_ON(ctx->sb->revmap_bm_pba > ctx->sb->max_pba);
	submit_bio_wait(bio);

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
		printk(KERN_ERR "\n %s no ckpt_page! ", __func__);
		return;
	}

	ckpt = (struct lsdm_ckpt *)page_address(page);
	/* TODO: GC will not change the checkpoint, but will change
	 * the SIT Info.
	 */
	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_WRITE, GFP_KERNEL);
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
	//printk("\n %s ckpt1_pba: %llu, ckpt2_pba: %llu", __func__, ctx->sb->ckpt1_pba, ctx->sb->ckpt2_pba);
	/* Record the pba for the next ckpt */
	if (ctx->ckpt_pba == ctx->sb->ckpt1_pba) {
		ctx->ckpt_pba = ctx->sb->ckpt2_pba;
		//printk(KERN_ERR "\n Updating the second checkpoint! pba: %lld", ctx->ckpt_pba);
	}
	else {
		ctx->ckpt_pba = ctx->sb->ckpt1_pba;
		//printk(KERN_ERR "\n Updating the first checkpoint! pba: %lld", ctx->ckpt_pba);
	}
	pba = ctx->ckpt_pba;
	bio->bi_opf = REQ_OP_WRITE;
	//bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba;
	//printk(KERN_ERR "\n flushing checkpoint at pba: %llu", pba);
	bio_set_dev(bio, ctx->dev->bdev);

	/* Not doing this right now as the conventional zones are too little.
	 * we need to log structurize the metadata
	 */
	BUG_ON(pba > ctx->sb->max_pba);
	submit_bio_wait(bio);
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
	//printk(KERN_ERR "\n Inside %s --------------------\n", __func__);
	if (node->rb_left)
		flush_sit_nodes(ctx, node->rb_left);
	flush_sit_node_page(ctx, node);
	if (node->rb_right)
		flush_sit_nodes(ctx, node->rb_right);
}

void free_sit_pages(struct ctx *);


/*
 * We need to maintain the sit entries in some pages and keep the
 * pages in a linked list.
 * We can then later flush all the sit page in one go
 */
void flush_sit(struct ctx *ctx)
{
	struct rb_root *rb_root = &ctx->sit_rb_root;
	//struct blk_plug plug;

	if (!rb_root->rb_node) {
		//printk(KERN_ERR "\n %s Sit node is null, returning", __func__);
		return;
	}

	if (!mutex_trylock(&ctx->sit_flush_lock))
		return;

	/* We flush these sequential pages
	 * together so that they can be 
	 * written together
	 */
	//blk_start_plug(&plug);
	flush_sit_nodes(ctx, rb_root->rb_node);
	//blk_finish_plug(&plug);
	mutex_unlock(&ctx->sit_flush_lock);
	/* When all the nodes are flushed we are here */
}

/* We call this function only on exit. The premise is that the gc_zone_nodes 
 * are deleted independently.
 */
void remove_gc_cost_nodes(struct ctx *ctx)
{
	struct rb_root *root = &ctx->gc_cost_root;
	struct rb_node *node;
	struct list_head *list_head = NULL;
	struct gc_zone_node *zone_node, *next_node = NULL;
	struct gc_cost_node *cost_node;

	node = root->rb_node;
	while(node) {
		cost_node = rb_entry(node, struct gc_cost_node, rb);
		list_head = &cost_node->znodes_list;
		/* We remove znode from the list maintained by cost node. If this is the last node on the list 
		 * then we have to remove the cost node from the tree
		 */
		list_for_each_entry_safe(zone_node, next_node, list_head, list) {
			list_del(&zone_node->list);
		}
		list_del(&cost_node->znodes_list);
		rb_erase(node, root);
		kmem_cache_free(ctx->gc_cost_node_cache, cost_node);
		node = root->rb_node;
	}
}

void remove_gc_zone_nodes(struct ctx *ctx)
{

	struct rb_root *root = &ctx->gc_zone_root;
	struct rb_node * node = NULL;
	struct gc_zone_node *zone_node;

	node = root->rb_node;
	while (node) {
		zone_node = rb_entry(node, struct gc_zone_node, rb);
		rb_erase(node, root);
		kmem_cache_free(ctx->gc_zone_node_cache, zone_node);
		node = root->rb_node;
	}
}

static void remove_gc_nodes(struct ctx *ctx)
{
	remove_gc_cost_nodes(ctx);
	remove_gc_zone_nodes(ctx);
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
void move_write_frontier(struct ctx *ctx, sector_t s8)
{
	int ret;
	/* We should have adjusted sectors_s8 to accomodate
	 * for the rooms in the zone before calling this function.
	 * Its how we split the bio
	 */
	if (ctx->free_sectors_in_wf < s8) {
		printk(KERN_ERR "\n %s Wrong manipulation of wf, free_sectors_in_wf: %llu s8: %llu \n", __func__, ctx->free_sectors_in_wf, s8);
		BUG();
	}

	if (!s8) {
		printk(KERN_ERR "\n %s s8 is 0 " , __func__);
		BUG();
	}

	//printk(KERN_ERR "\n %s ctx->hot_wf_pba: %llu ctx->free_sectors_in_wf: %llu \n", __func__, ctx->hot_wf_pba, ctx->free_sectors_in_wf);
	ctx->hot_wf_pba = ctx->hot_wf_pba + s8;
	ctx->free_sectors_in_wf = ctx->free_sectors_in_wf - s8;
	ctx->user_block_count -= s8 / NR_SECTORS_IN_BLK;
	if (ctx->free_sectors_in_wf < NR_SECTORS_IN_BLK) {
		//printk(KERN_INFO "Num of free sect.: %llu, about to call get_new_zone() \n", ctx->free_sectors_in_wf);
		if ((ctx->hot_wf_pba - 1) != ctx->hot_wf_end) {
			printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->hot_wf_pba, ctx->hot_wf_end);
			BUG_ON(ctx->hot_wf_pba != (ctx->hot_wf_end + 1));
		}
		//int zonenr = get_zone_nr(ctx, ctx->hot_wf_pba - 2);
		//trace_printk("\n %s zone: %d #valid blks: %d ", __func__, zonenr, get_sit_ent_vblocks(ctx, zonenr));
		//mutex_lock(&ctx->bm_lock);
		ret = get_new_zone(ctx);
		//mutex_unlock(&ctx->bm_lock);
		if (ret) {
			printk(KERN_ERR "\n No more disk space available for writing!");
			BUG();
		}
	}
}

/* Always called with the ctx->lock held
 */
void move_gc_write_frontier(struct ctx *ctx, sector_t s8)
{
	/* We should have adjusted sectors_s8 to accomodate
	 * for the rooms in the zone before calling this function.
	 * Its how we split the bio
	 */
	if (ctx->free_sectors_in_gc_wf < s8) {
		panic("Wrong manipulation of gc wf; used unavailable sectors in a log");
	}

	BUG_ON(!s8);
	
	//printk(KERN_ERR "\n %s ctx->warm_gc_wf: %llu ctx->free_sectors_in_gc_wf: %llu \n", __func__, ctx->warm_gc_wf_pba, ctx->free_sectors_in_gc_wf);
	ctx->warm_gc_wf_pba = ctx->warm_gc_wf_pba + s8;
	ctx->free_sectors_in_gc_wf = ctx->free_sectors_in_gc_wf - s8;
	if (ctx->free_sectors_in_gc_wf < NR_SECTORS_IN_BLK) {
		if ((ctx->warm_gc_wf_pba - 1) != ctx->warm_gc_wf_end) {
			printk(KERN_INFO "kernel wf before BUG: %llu - %llu\n", ctx->warm_gc_wf_pba, ctx->warm_gc_wf_end);
			BUG_ON(ctx->warm_gc_wf_pba != (ctx->warm_gc_wf_end + 1));
		}
		if (get_new_gc_zone(ctx)) {
			panic("No more disk space available for writing!");
		}
	}
}

int is_revmap_block_available(struct ctx *ctx, u64 pba);
void flush_tm_nodes(struct rb_node *node, struct ctx *ctx);

struct page * read_block(struct ctx *, u64 , u64 );
/*
 * pba: stored in the LBA - PBA translation.
 * This is the PBA of some data block. This PBA belongs to some zone.
 * We are about to update the SIT entry that belongs to that zone
 * Before that we read that corresponding page in memory and then
 * add it to our RB tree that we use for searching.
 *
 */
struct sit_page * add_sit_page_kv_store(struct ctx * ctx, sector_t pba, const char * caller)
{
	sector_t sit_blknr;
	u64 zonenr = get_zone_nr(ctx, pba);
	struct rb_root *root = &ctx->sit_rb_root;
	struct rb_node *parent = NULL;
	struct sit_page *parent_ent;
	struct sit_page *new;
	struct page *page;
	struct lsdm_seg_entry *ptr;
	static int count = 0;


	new = search_sit_kv_store(ctx, pba, &parent);
	if (new) {
		//printk("\n %s FOUND!! zone: %llu sit_blknr: %lld sit_pba: %u sit_page: %p caller: (%s)", __func__, zonenr, sit_blknr, ctx->sb->sit_pba, page_address(new->page), caller);
		new->flag = NEEDS_FLUSH;
		return new;
	}

	new = kmem_cache_alloc(ctx->sit_page_cache, GFP_KERNEL);
	if (!new) {
		print_memory_usage(ctx, __func__);
		printk(KERN_ERR "\n Could not allocate memory to sit page \n");
		return NULL;
	}

	RB_CLEAR_NODE(&new->rb);
	sit_blknr = zonenr / SIT_ENTRIES_BLK;
	count++;
	//printk("\n %s sit_blknr: %lld sit_pba: %u, invocation count:%d caller: (%s)", __func__, sit_blknr, ctx->sb->sit_pba, count, caller);
	//page = read_block(ctx, ctx->sb->sit_pba, (sit_blknr * NR_SECTORS_IN_BLK));
	page = alloc_page(__GFP_ZERO|GFP_KERNEL);
	if (!page) {
		kmem_cache_free(ctx->sit_page_cache, new);
		return NULL;
	}
	//printk("\n %s zonenr: %lld page: %p ", __func__, zonenr, page_address(page));
	new->flag = NEEDS_FLUSH;
	new->blknr = sit_blknr;
	new->page = page;
	ptr = (struct lsdm_seg_entry *) page_address(page);
	if (parent) {
		/* Add this page to a RB tree based KV store.
		 * Key is: blknr for this corresponding block
		 */
		parent_ent = rb_entry(parent, struct sit_page, rb);
		if (new->blknr < parent_ent->blknr) {
			/* Attach new node to the left of parent */
			rb_link_node(&new->rb, parent, &parent->rb_left);
			//printk(KERN_ERR "\n Adding a new node at parent->rb_left: %p, new page address: %p", page_address(parent_ent->page), page_address(new->page));

		}
		else { 
			/* Attach new node to the right of parent */
			rb_link_node(&new->rb, parent, &parent->rb_right);
			//printk(KERN_ERR "\n Adding a new node at parent->rb_right: %p, new page address: %p", page_address(parent_ent->page), page_address(new->page));
		}
	} else {
		rb_link_node(&new->rb, NULL, &root->rb_node);
		parent_ent = rb_entry(root->rb_node, struct sit_page, rb);
		//printk(KERN_ERR "\n Adding a root node: %p, new page address: %p", page_address(parent_ent->page), page_address(new->page));
	}
	/* Balance the tree after the blknr is addded to it */
	rb_insert_color(&new->rb, root);
	
	atomic_inc(&ctx->nr_sit_pages);
	atomic_inc(&ctx->sit_flush_count);
	return new;
}

int update_gc_tree(struct ctx *, unsigned int , u32 , u64 , const char *);

int get_sit_ent_vblocks(struct ctx *ctx, int zonenr)
{

	struct sit_page *sit_page;
	struct lsdm_seg_entry *ptr;
	sector_t pba;
	int index;

	pba = get_first_pba_for_zone(ctx, zonenr);
	sit_page = add_sit_page_kv_store(ctx, pba, __func__);
	if (!sit_page) {
		return -1;
	}
	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	return ptr->vblocks;
}

void print_zones_vblocks(struct ctx *ctx) 
{
	int i, vblocks = 0;

	for (i=0; i<ctx->sb->zone_count; i++) {
		vblocks = get_sit_ent_vblocks(ctx, i);
		printk(KERN_ERR "\n %s zonenr: %d vblocks: %d ", __func__, i, vblocks);
	}
}

/*
 * pba: from the LBA-PBA pair. Of a data block
 */
void sit_ent_vblocks_decr(struct ctx *ctx, sector_t pba)
{
	struct sit_page *sit_page;
	struct lsdm_seg_entry *ptr;
	sector_t zonenr;
	int index;

	BUG_ON(pba == 0);
	BUG_ON(pba > ctx->sb->max_pba);

	//mutex_lock(&ctx->sit_kv_store_lock);
	/*--------------------------------------------*/
	sit_page= add_sit_page_kv_store(ctx, pba, __func__);
	if (!sit_page) {
	/* TODO: do something, low memory */
		panic("Low memory, could not allocate sit_entry");
	}

	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	zonenr = get_zone_nr(ctx, pba);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = ptr + index;
	if (!ptr->vblocks) {
		printk("\n pba: %llu segnr: %llu vblocks: %u mtime:%lu \n", pba, zonenr, ptr->vblocks, ptr->mtime);
	}
	BUG_ON(!ptr->vblocks);
	ptr->vblocks = ptr->vblocks - 1;
	/* Send the older vblocks, mtime along with the new vblocks,mtime to this
	 * function. The older vblocks, mtime is used to calculated
	 * the older cost which is stored in the tree. The newer ones
	 * on the other hand are used to modify the tree
	 */
	if ((zonenr != get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba)) &&
		    (zonenr != get_zone_nr(ctx, ctx->ckpt->warm_gc_frontier_pba))) {
		//trace_printk("\n %s done! zone: %u vblocks: %d pba: %lu, index: %d , ptr: %p", __func__, zonenr, ptr->vblocks, pba, index, ptr);
		/* add mtime here */
		ptr->mtime = get_elapsed_time(ctx);
		if (ctx->max_mtime < ptr->mtime)
			ctx->max_mtime = ptr->mtime;
		update_gc_tree(ctx, zonenr, ptr->vblocks, ptr->mtime, __func__);
		if (!ptr->vblocks) {
			//printk(KERN_ERR "\n %s Freeing zone: %llu \n", __func__, zonenr);
			mark_zone_free(ctx , zonenr, 1);
			
		}
	}
	//mutex_unlock(&ctx->sit_kv_store_lock);
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
	static int print = 1;
	long vblocks = 0;
	struct lsdm_sb * sb = ctx->sb;

	BUG_ON(pba == 0);
	BUG_ON(pba > ctx->sb->max_pba);

	//mutex_lock(&ctx->sit_kv_store_lock);
	sit_page = add_sit_page_kv_store(ctx, pba, __func__);
	if (!sit_page) {
		/* TODO: do something, low memory */
		print_memory_usage(ctx, "During sit_ent_vblocks_incr");
		BUG_ON(1);
		//panic("Low memory, could not allocate sit_entry");
	}

	zonenr = get_zone_nr(ctx, pba);
	//trace_printk("\n %s: pba: %llu zonenr: %llu", __func__, pba, zonenr);
	index = zonenr % SIT_ENTRIES_BLK; 
	ptr = (struct lsdm_seg_entry *) page_address(sit_page->page);
	ptr = ptr + index;
	ptr->vblocks = ptr->vblocks + 1;
	vblocks = ptr->vblocks;
	if (pba == zone_end(ctx, pba)) {
		ptr->mtime = get_elapsed_time(ctx);
		if (ctx->max_mtime < ptr->mtime)
			ctx->max_mtime = ptr->mtime;
	}
	//mutex_unlock(&ctx->sit_kv_store_lock);
	if(vblocks > (1 << (sb->log_zone_size - sb->log_block_size))) {
		if (print) {
			printk(KERN_ERR "\n !!!!!!!!!!!!!!!!!!!!! zone: %llu has more than %d blocks! ", zonenr, ptr->vblocks);
			print = 0;
			panic("Zone has more blocks that it should!");
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

	BUG_ON(pba == 0);
	BUG_ON(pba > ctx->sb->max_pba);

	//mutex_lock(&ctx->sit_kv_store_lock);
	sit_page = add_sit_page_kv_store(ctx, pba, __func__);
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
	//mutex_unlock(&ctx->sit_kv_store_lock);
}

struct tm_page * search_tm_kv_store(struct ctx *ctx, u64 blknr, struct rb_node **parent);
/*
 * If this length cannot be accomodated in this page
 * search and add another page for this next
 * lba. Remember this translation table will
 * go on the disk and is block based and not 
 * extent based
 *
 * Depending on the location within a page, add the lba.
 */
int add_translation_entry(struct ctx * ctx, sector_t lba, sector_t pba, size_t len) 
{
	struct tm_entry * ptr;
	int index, i, zonenr = 0, blknr;
	int nrblks = len >> SECTOR_BLK_SHIFT;
	struct tm_page * tm_page = NULL;
	struct page *page;

	BUG_ON(len < 0);
	BUG_ON(nrblks == 0);
	BUG_ON(pba == 0);
	mutex_lock(&ctx->tm_kv_store_lock);
	tm_page = add_tm_page_kv_store(ctx, lba);
	if (!tm_page) {
		mutex_unlock(&ctx->tm_kv_store_lock);
		printk(KERN_ERR "%s NO memory! ", __func__);
		BUG();
	}
	tm_page->flag = NEEDS_FLUSH;
	/* Call this under the kv store lock, else it will race with removal/flush code
	 */
	page = tm_page->page;
	ptr = (struct tm_entry *) page_address(page);
	/* Do not modify the lba, we will need it later */
	/* lba is sector addressable, whereas translation entry is per block
	 * so we first convert the address to a blk nr and then find out the
	 * position in the page using that blknr
	 */
	blknr = (lba >> SECTOR_BLK_SHIFT);
	index = blknr %  TM_ENTRIES_BLK;
	ptr = ptr + index;
	BUG_ON(ctx->sb->max_pba == 0);
	//trace_printk("\n %s zonenr: %d lba: %llu, pba: %llu, len: %zu nrblks: %d", __func__, get_zone_nr(ctx, pba), lba, pba, len, nrblks);
	for(i=0; i<nrblks; i++) {
		if (pba > ctx->sb->max_pba) {
			printk(KERN_ERR "\n %s lba: %llu pba: %llu max_pba: %llu len: %zu i: %d", __func__, lba, pba, ctx->sb->max_pba, len, i);
			mutex_unlock(&ctx->tm_kv_store_lock);
			BUG();
			return -ENOMEM;
		}
		/*-----------------------------------------------*/
		if (ptr->pba == pba) {
			printk(KERN_ERR "\n 1. lba: %llu, ptr->pba: %llu pba: %llu", lba, ptr->pba, pba);
			/* repeated entry - retrieved either from on-disk tm  */
			/* for now we are not flushing, so this should not be here */
			mutex_unlock(&ctx->tm_kv_store_lock);
			BUG_ON(1);
			return -ENOMEM;
		}
		/* lba can be 0, but pba cannot be, so this entry is empty! */
		if (ptr->pba != 0) {
			/* decrement vblocks for the segment that has
			 * the stale block
			 */
			zonenr = get_zone_nr(ctx, ptr->pba);
			//trace_printk("\n %s Overwrite a block at LBA: %llu, orig PBA: %llu origzone: %d new PBA: %llu ", __func__, lba, ptr->pba, zonenr, pba);
			sit_ent_vblocks_decr(ctx, ptr->pba);
		}
		ptr->pba = pba;
		sit_ent_vblocks_incr(ctx, pba);
		pba = pba + NR_SECTORS_IN_BLK;
		lba = lba + NR_SECTORS_IN_BLK;
		index = index + 1;
		if (index < TM_ENTRIES_BLK) {
			ptr++;
			continue;
		} 
		tm_page = add_tm_page_kv_store(ctx, lba);
		if (!tm_page) {
			printk(KERN_ERR "%s NO memory! ", __func__);
			mutex_unlock(&ctx->tm_kv_store_lock);
			BUG_ON(1);
			return -ENOMEM;
		}
		tm_page->flag = NEEDS_FLUSH;
		page = tm_page->page;
		ptr = (struct tm_entry *) page_address(page);
		index = 0;
	}
	mutex_unlock(&ctx->tm_kv_store_lock);
	return 0;	
}

/* We are reading the lba from the lsdm tree in memory.
 * We will have a problem when the tree becomes so big that
 * it cannot be held in memory
 * But  for now this works! Will have to convert the the block
 * based translation entry to an extent based one. 
 *
 * TODO: Will have to convert the the block
 * based translation entry to an extent based one.
 */
struct page * read_tm_page(struct ctx * ctx, u64 lba)
{
	u64 last_lba; 
	int index = 0;
	struct page * page;
	struct tm_entry * ptr;
	struct extent * e = NULL;
	u64 e_pba = 0;
	u32 nrsectors = (TM_ENTRIES_BLK * NR_SECTORS_IN_BLK);

	/* blknr = lba / nrsectors */
	lba = ((lba / nrsectors) * nrsectors);
	last_lba = lba + nrsectors;

    	page = alloc_page(__GFP_ZERO|GFP_KERNEL);
	if (!page )
		return NULL;

	nrpages++;

	//printk(KERN_ERR "\n %s nrpages: %llu lba: %llu \n", __func__, nrpages, lba);

	ptr = (struct tm_entry *) page_address(page);
	
	while (lba <= last_lba) {
		/* metadata lock held in the calling function add_revmap_entry */
		e = _lsdm_rb_geq(&ctx->extent_tbl_root, lba, 0);
		/* Case of no overlap */
		if ((e == NULL) || (e->lba >= lba + nrsectors))  {
			while (index < TM_ENTRIES_BLK) {
				ptr->pba = 0;
				ptr++;
				index++;
				nrsectors = nrsectors - NR_SECTORS_PER_BLK;
			}
			lba = last_lba;
			break;
		}

		/* some partial overlap, front part does not have a mapping*/
		while ((e->lba > lba) && (e->lba + e->len < lba)){
			ptr->pba = 0;
			ptr++;
			lba =  lba + NR_SECTORS_PER_BLK;
			index = index + NR_SECTORS_PER_BLK;
			if (index == TM_ENTRIES_BLK) {
				break;
			}
			nrsectors = nrsectors - NR_SECTORS_PER_BLK;
		}

		if (index == TM_ENTRIES_BLK) {
			break;
		}


		/* e->lba <= lba */
		e_pba = e->pba;
		while ((e->lba <= lba) && (e->lba + e->len > lba)) {
			ptr->pba = e_pba;
			ptr++;
			lba = lba + NR_SECTORS_PER_BLK;
			e_pba = e_pba + NR_SECTORS_PER_BLK;
			index = index + NR_SECTORS_PER_BLK;
			if (index == TM_ENTRIES_BLK) {
				break;
			}
			nrsectors = nrsectors - NR_SECTORS_PER_BLK;
		}
	}
	return page;
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
	//printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_READ, GFP_KERNEL);
	if (!bio) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
		return NULL;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		__free_pages(page, 0);
		bio_put(bio);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
		return NULL;
	}
	bio->bi_opf = REQ_OP_READ;
	//bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_iter.bi_sector = pba;
	bio_set_dev(bio, ctx->dev->bdev);
	submit_bio_wait(bio);
	//printk(KERN_ERR "\n read a block from pba: %llu", pba);
	if (bio->bi_status != BLK_STS_OK) {
		printk(KERN_ERR "\n %s Could not read the block, status: %d ", __func__, bio->bi_status);
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

void flush_tm_node_page(struct ctx *ctx, struct tm_page * tm_page);


void remove_translation_pages(struct ctx *ctx)
{
	struct tm_page *tm_page;
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *node = root->rb_node;

	//printk(KERN_ERR "\n Inside %s ", __func__);
	
	while(node) {
		tm_page = rb_entry(node, struct tm_page, rb);
		if (tm_page->flag == NEEDS_FLUSH) {
			flush_tm_node_page(ctx, tm_page);
		}
		rb_erase(node, root);
		if (!tm_page->page) {
			panic("Page cannot be null now! ");
			return;
		}
		__free_pages(tm_page->page, 0);
		nrpages--;
		kmem_cache_free(ctx->tm_page_cache, tm_page);
		atomic_dec(&ctx->nr_tm_pages);
		node = root->rb_node;
	}
	//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
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
	//mutex_lock(&ctx->tm_kv_store_lock);
	remove_translation_pages(ctx);
	//mutex_unlock(&ctx->tm_kv_store_lock);
}

void remove_sit_page(struct ctx *ctx, struct rb_node *node)
{

	struct sit_page *sit_page;
	struct page *page;
	sit_page = rb_entry(node, struct sit_page, rb);

	if (sit_page->flag == NEEDS_FLUSH) {
		flush_sit_node_page(ctx, node);
	}

	rb_erase(&sit_page->rb, &ctx->sit_rb_root);
	page = sit_page->page;
	if (!page) {
		return;
	}
	__free_pages(page, 0);
	kmem_cache_free(ctx->sit_page_cache, sit_page);
	nrpages--;
	//printk(KERN_ERR "\n %s: sit page freed! ", __func__);
	return;
}

void free_sit_pages(struct ctx *ctx)
{
	struct rb_root *root = &ctx->sit_rb_root;
	struct rb_node * node = NULL;

	//mutex_lock(&ctx->sit_kv_store_lock);
	node = root->rb_node;
	while(node) {
		remove_sit_page(ctx, node);
		node = root->rb_node;
	}
	//mutex_unlock(&ctx->sit_kv_store_lock);
	BUG_ON(root->rb_node);
	printk(KERN_ERR "\n %s Removed all SIT Pages!", __func__);
}

/* We don't wait for the bios to complete 
 * flushing. We only initiate the flushing
 */
void flush_tm_node_page(struct ctx *ctx, struct tm_page * tm_page)
{
	struct page *page; 
	u64 pba;
	struct bio * bio;

	page = tm_page->page;
	if (!page)
		return;

	//printk(KERN_ERR "\n %s: 1 tm_page:%p, tm_page->page:%p \n", __func__, tm_page, page_address(tm_page->page));
	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_WRITE, GFP_KERNEL);
	if (!bio) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
		return;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
		bio_put(bio);
		printk(KERN_ERR "\n Inside %s 2 - Going.. Bye!! \n", __func__);
		return;
	}

	/* blknr is the relative blknr within the translation blocks.
	 * We convert it to sector number as bio_submit expects that.
	 */
	pba = (tm_page->blknr * NR_SECTORS_IN_BLK) + ctx->sb->tm_pba;
	BUG_ON(pba > (ctx->sb->tm_pba + (ctx->sb->blk_count_tm << NR_SECTORS_IN_BLK)));

	//printk(KERN_ERR "\n %s Flushing TM page: %p at pba: %llu blknr:%llu max_tm_blks:%u", __func__,  page_address(page), pba,  tm_page->blknr, ctx->sb->blk_count_tm);

	bio->bi_opf = REQ_OP_WRITE;
	//bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_iter.bi_sector = pba;
	BUG_ON(pba > ctx->sb->max_pba);
	tm_page->flag = NEEDS_NO_FLUSH;
	submit_bio_wait(bio);
	if (bio->bi_status != BLK_STS_OK) {
		/*TODO: Perhaps retry!! or do something more
		 * or else you will loose the translation entries.
		 * write them some place else!
		 */
		//trace_printk("\n %s bi_status: %d \n", __func__, bio->bi_status);
		//panic("Could not write the translation entry block");
	}
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);
	//printk(KERN_INFO "\n 2. Leaving %s! flushed dirty page! \n", __func__);
	return;
}


void flush_tm_nodes(struct rb_node *node, struct ctx *ctx)
{
	struct tm_page * tm_page; 

	if (!node) {
		//printk(KERN_INFO "\n %s No tm node found! returning!", __func__);
		return;
	}

	tm_page = rb_entry(node, struct tm_page, rb);
	if (!tm_page) {
		return;
	}
	flush_tm_nodes(node->rb_left, ctx);
	if(tm_page->flag == NEEDS_FLUSH) {
		flush_tm_node_page(ctx, tm_page);
	}
	flush_tm_nodes(node->rb_right, ctx);
	return;
}

int read_translation_map(struct ctx *);

/* This function waits for all the translation blocks to be flushed to
 * the disk by calling wait_for_ckpt_completion and tm_ref
 * The non blocking call is flush_tm_nodes
 */
void flush_translation_blocks(struct ctx *ctx)
{
	struct rb_root *root = &ctx->tm_rb_root;
	//struct blk_plug plug;

	//printk(KERN_ERR "\n Inside %s ", __func__);
	if (!mutex_trylock(&ctx->tm_lock)) {
		return;
	}

	if (!root->rb_node) {
		mutex_unlock(&ctx->tm_lock);
		//printk(KERN_ERR "\n %s tm node is NULL!", __func__);
		return;
	}

	//blk_start_plug(&plug);
	flush_tm_nodes(root->rb_node, ctx);
	mutex_unlock(&ctx->tm_lock);
	//blk_finish_plug(&plug);	
	//printk(KERN_INFO "\n %s done!!", __func__);
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

	page = sit_page->page;
	if (!page)
		return;

	sit_page->flag = NEEDS_NO_FLUSH;

	/* pba denotes a relative sit blknr that is 4096 sized
	 * bio works on a LBA that is sector sized.
	 */
	pba = sit_page->blknr;
	//printk(KERN_ERR "\n %s sit_ctx: %p", __func__, sit_ctx);

	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_WRITE, GFP_KERNEL);
	if (!bio) {
		return;
	}
	
	/* bio_add_page sets the bi_size for the bio */
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		printk(KERN_ERR "\n %s: Could not add sit page to the bio ", __func__);
		bio_put(bio);
		return;
	}

	bio_set_dev(bio, ctx->dev->bdev);
	bio->bi_opf = REQ_OP_WRITE;
	//bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	/* Sector addressing */
	pba = (pba * NR_SECTORS_IN_BLK) + ctx->sb->sit_pba;
	bio->bi_iter.bi_sector = pba;
	//printk("\n %s sit page address: %p pba: %llu count: %d ", __func__, page_address(page), pba, count);
	bio->bi_private = sit_ctx;
	BUG_ON(pba > ctx->sb->max_pba);
	submit_bio_wait(bio);
	if (bio->bi_status != BLK_STS_OK) {
		printk(KERN_ERR "\n Could not write the SIT page to disk! ");
	} 
	/* bio_alloc(), hence bio_put() */
	bio_put(bio);
}	

int read_extents_from_block(struct ctx * ctx, struct tm_entry *entry, u64 lba);

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
struct tm_page *add_tm_page_kv_store(struct ctx *ctx, sector_t lba)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *parent = NULL, **link = &root->rb_node;
	struct tm_page *new_tmpage, *parent_ent;
	u64 blknr, lba_blk;
	u64 addr;
	/* convert the sector lba to a blknr and then find out the relative
	 * blknr where we find the translation entry from the first
	 * translation block.
	 *
	 */
	BUG_ON(lba > ctx->sb->max_pba);
	lba_blk = lba >> SECTOR_BLK_SHIFT;
	blknr = lba_blk/TM_ENTRIES_BLK;
	BUG_ON(blknr > ctx->sb->blk_count_tm);

	new_tmpage = search_tm_kv_store(ctx, blknr, &parent);
	if (new_tmpage) {
		//trace_printk("\n %s 1) lba: %llu, tm blk: %d, page: %p tm_entries_blk: #TM_ENTRIES_BLK", __func__, lba, blknr, new_tmpage);
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

	//printk("\n %s lba: %llu blknr: %d tm_pba: %llu \n", __func__, lba, blknr, ctx->sb->tm_pba);

	//new_tmpage->page = read_block(ctx, ctx->sb->tm_pba, (blknr * NR_SECTORS_IN_BLK));
	new_tmpage->page = alloc_page(__GFP_ZERO|GFP_KERNEL);
	if (!new_tmpage->page) {
		printk(KERN_ERR "\n %s read_block  failed! could not allocate page! \n", __func__);
		kmem_cache_free(ctx->tm_page_cache, new_tmpage);
		return NULL;
	}
	addr = (unsigned long) page_address(new_tmpage->page);
	//trace_printk("\n %s 2) Added new! lba: %llu, tm blk: %d, page: %p tm_entries_blk: #TM_ENTRIES_BLK", __func__, lba, blknr, new_tmpage);
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


/* Make the length in terms of sectors or blocks?
 * 
 * page is revmap page!
 */
int add_block_based_translation(struct ctx *ctx, struct page *page, const char * func)
{
	
	struct lsdm_revmap_entry_sector * ptr;
	int i, j;
	unsigned len = 0;
	sector_t lba, pba;

	ptr = (struct lsdm_revmap_entry_sector *)page_address(page);
	//printk(KERN_ERR "%s revmap_page address is: %p ", __func__, page_address(page));
	i = 0;

/*-------------------------------------------------------------*/
	for(i=0; i < NR_SECTORS_IN_BLK; i++, ptr = ptr + 1) {
		for(j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			if (0 == ptr->extents[j].pba) {
				//printk(KERN_ERR "\n %s ptr: %p ptr->extents[%d].pba: 0 ", __func__, ptr, j);
				break;
			}
			lba = ptr->extents[j].lba;
			pba = ptr->extents[j].pba;
			len = ptr->extents[j].len;
			if ((lba > ctx->sb->max_pba) || (pba > ctx->sb->max_pba)) {
				printk(KERN_ERR "\n %s ptr: %p, i: %d , j: %d, lba: %llu, pba: %llu, len: %u caller: %s", __func__,ptr, i, j, lba, pba, len, func);
				BUG();
			}
			if ((len <= 0) || (len % 8)) {
				printk(KERN_ERR "\n %s ptr: %p, i: %d , j: %d, lba: %llu, pba: %llu, len: %u caller: %s", __func__,ptr, i, j, lba, pba, len, func);
				BUG();
			}
			/* Call this under the kv store lock, else it will race with removal/flush code
			 */
			add_translation_entry(ctx, lba, pba, len);
			//printk(KERN_ERR "\n %s Adding TM entry: ptr: %p ptr->extents[j].lba: %llu, ptr->extents[j].pba: %llu, ptr->extents[j].len: %u", __func__, ptr, lba, pba, len);
		}
	}
/*-------------------------------------------------------------*/
	printk(KERN_ERR "\n %s BYE lba: %llu pba: %llu len: %d!", __func__, lba, pba, len);
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
void wait_on_refcount(struct ctx *ctx, refcount_t *ref, spinlock_t *lock)
{
	spin_lock(lock);
	wait_event_lock_irq(ctx->refq, ( 2 >= refcount_read(ref)), *lock);
	spin_unlock(lock);
}

void revmap_blk_flushed(struct bio *bio)
{
	bio_free_pages(bio);
	bio_put(bio);
	nrpages--;
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
int flush_revmap_block_disk(struct ctx * ctx, struct page *page, sector_t revmap_pba)
{
	struct bio * bio;

	BUG_ON(!page);

	bio = bio_alloc(ctx->dev->bdev, 1, REQ_OP_WRITE, GFP_KERNEL);
	if (!bio) {
		return -ENOMEM;
	}
	
	if( PAGE_SIZE > bio_add_page(bio, page, PAGE_SIZE, 0)) {
		bio_put(bio);
		return -EFAULT;
	}

	bio->bi_iter.bi_sector = revmap_pba;
	//printk(KERN_ERR "%s Flushing revmap blk at pba:%llu ctx->revmap_pba: %llu", __func__, bio->bi_iter.bi_sector, ctx->revmap_pba);
	bio->bi_end_io = revmap_blk_flushed;
	bio->bi_opf = REQ_OP_WRITE;
	//bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio_set_dev(bio, ctx->dev->bdev);
	BUG_ON(ctx->revmap_pba > ctx->sb->max_pba);
	submit_bio(bio);
	mark_revmap_bit(ctx, revmap_pba);
	//printk(KERN_ERR "\n flushing revmap at pba: %llu page: %p", bio->bi_iter.bi_sector, page_address(page));
	return 0;
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
 * Always called with the lsdm_rb_lock held!
 */
static void add_revmap_entry(struct ctx * ctx, __le64 lba, __le64 pba, int nrsectors)
{
	struct lsdm_revmap_entry_sector * ptr = NULL;
	int entry_nr, sector_nr;
	struct page * page = NULL;

#ifdef LSDM_DEBUG
	BUG_ON(pba == 0);
	BUG_ON(pba > ctx->sb->max_pba);
	BUG_ON(lba > ctx->sb->max_pba);
	if((pba + nrsectors) > ctx->sb->max_pba) {
		printk(KERN_ERR "\n %s %d lba: %lld pba: %lld nrsectors: %d max_pba: %lld entry_nr: %d ptr: %p\n" , __func__, __LINE__, lba, pba, nrsectors, ctx->sb->max_pba, entry_nr, ptr);
		BUG();
	}
	if((lba + nrsectors) > ctx->sb->max_pba) {
		printk(KERN_ERR "\n %s %d lba: %lld pba: %lld nrsectors: %d max_pba: %llu entry_nr: %d ptr: %p\n" , __func__, __LINE__, lba, pba, nrsectors, ctx->sb->max_pba, entry_nr, ptr);
		BUG();
	}
#endif
	/* Merge entries by increasing the length if there lies a
	 * matching entry in the revmap page
	 */
	entry_nr = atomic_read(&ctx->revmap_entry_nr);
	sector_nr = atomic_read(&ctx->revmap_sector_nr);
	BUG_ON(entry_nr > NR_EXT_ENTRIES_PER_SEC);
	BUG_ON(sector_nr > NR_SECTORS_PER_BLK);
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
		nrpages++;
		ctx->revmap_page = page;
		//printk(KERN_ERR "\n %s revmap page address: %p ", __func__, page_address(page));
	}
	/* blk count before incrementing */
	page = ctx->revmap_page;
	BUG_ON(page == NULL);
	ptr = (struct lsdm_revmap_entry_sector *)page_address(page);
	ptr = ptr + sector_nr;
	ptr->extents[entry_nr].lba = lba;
    	ptr->extents[entry_nr].pba = pba;
	ptr->extents[entry_nr].len = nrsectors;
	atomic_inc(&ctx->revmap_entry_nr);
	if (NR_EXT_ENTRIES_PER_SEC == (entry_nr + 1)) {
		//ptr->crc = calculate_crc(ctx, page);
		ptr->crc = 0;
		atomic_set(&ctx->revmap_entry_nr, 0);
		atomic_inc(&ctx->revmap_sector_nr);
		if (NR_SECTORS_PER_BLK == (sector_nr + 1)) {
			atomic_set(&ctx->revmap_sector_nr, 0);
			/* TODO: do this in parallel, dont wait. Called from the write context */
			//flush_revmap_block_disk(ctx, page, ctx->revmap_pba);
			ctx->revmap_page = 0;
			/* Adjust the revmap_pba for the next block. Addressing is based on 512bytes sector. */
			ctx->revmap_pba += NR_SECTORS_IN_BLK; 
			/* if we have the pba of the translation table,
			* then reset the revmap pba to the original value
			*/
			if (ctx->revmap_pba == ctx->sb->tm_pba) {
				ctx->revmap_pba = ctx->sb->revmap_pba;
				/* TODO: we need to check if the revmap bit is clear here */
			}
		}
	}
	//printk(KERN_ERR "\n revmap entry added! ptr: %p entry_nr: %d, sector_nr: %d lba: %llu pba: %llu len: %d \n", ptr, entry_nr, sector_nr, lba, pba, nrsectors);
	return;
}


static int print_bzr(struct blk_zone *zone, unsigned int num, void *data)
{
	int i; 

	printk(KERN_ERR "\n %s num: %d ", __func__ , num);
	for (i = 0; i < num; i++) {
		printk(KERN_ERR "\n start: %llu len: %llu  wp : %llu type: %s ", zone->start, zone->len, zone->wp, (zone->type == 0x1) ? "conventional" : "sequential");
	}
	return 0;
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
	ctx = lsdm_bioctx->ctx;
	bio = lsdm_bioctx->orig;
	BUG_ON(!bio);
	BUG_ON(!ctx);

	if (BLK_STS_OK != bio->bi_status) {
		printk(KERN_ERR "\n %s bio status: %d ", __func__, bio->bi_status);
	}
	bio_endio(bio);
	//printk(KERN_ERR "\n %s done lba: %llu len: %llu ", __func__, bio->bi_iter.bi_sector, bio_sectors(bio));
	kmem_cache_free(ctx->bioctx_cache, lsdm_bioctx);
	mykref_put(&ctx->ongoing_iocount, lsdm_ioidle);
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
	/* task completed successfully */
	lba = subbioctx->extent.lba;
	pba = subbioctx->extent.pba;
	len = subbioctx->extent.len;
	len = (len >> SECTOR_BLK_SHIFT) << SECTOR_BLK_SHIFT;

	//BUG_ON(pba == 0);
	//BUG_ON(pba > ctx->sb->max_pba);
	//BUG_ON(lba > ctx->sb->max_pba);
	//BUG_ON(len % 8 != 0);
	down_write(&ctx->lsdm_rb_lock);
	/**************************************/
	lsdm_rb_update_range(ctx, lba, pba, len);
	/**************************************/
	up_write(&ctx->lsdm_rb_lock);
	/* Now reads will work! so we can complete the bio */

	//trace_printk("\n %s Entering zonenr: %d lba: %llu, pba: %llu, len: %u kref: %d \n", __func__, get_zone_nr(ctx, pba), lba, pba, len, kref_read(&bioctx->ref));
	down_write(&ctx->lsdm_rev_lock);
	/*------------------------------- */
	add_revmap_entry(ctx, lba, pba, len);
	/*-------------------------------*/
	up_write(&ctx->lsdm_rev_lock);
	add_translation_entry(ctx, lba, pba, len);
	//printk(KERN_ERR "\n %s Done !! zonenr: %d lba: %llu, pba: %llu, len: %u kref: %d \n", __func__, get_zone_nr(ctx, pba), lba, pba, len, kref_read(&bioctx->ref));
	kref_put(&bioctx->ref, write_done);
	kmem_cache_free(ctx->subbio_ctx_cache, subbioctx);
	return;
}


void sub_write_err(struct work_struct * w)
{

	struct lsdm_sub_bioctx *subbioctx = NULL;
	struct lsdm_bioctx * bioctx;
	struct ctx *ctx;
	sector_t lba, pba, zone_begins;
	unsigned int len;
	struct gendisk * disk;

	WARN_ONCE(1, "\n %s write error received, future writes will be non sequential! \n", __func__);

	subbioctx = container_of(w, struct lsdm_sub_bioctx, work);
	bioctx = subbioctx->bioctx;
	ctx = bioctx->ctx;
	/* task completed successfully */
	lba = subbioctx->extent.lba;
	pba = subbioctx->extent.pba;
	len = subbioctx->extent.len;

	kref_put(&bioctx->ref, write_done);
	kmem_cache_free(ctx->subbio_ctx_cache, subbioctx);

	BUG_ON(pba == 0);
	BUG_ON(pba > ctx->sb->max_pba);
	BUG_ON(lba > ctx->sb->max_pba);

	printk(KERN_ERR "\n Error %s, requesting lba: %llu pba: %llu len: %d ", __func__, pba, lba, len);
	zone_begins = zone_start(ctx, pba);
	disk = ctx->dev->bdev->bd_disk;
	//sd_zbc_report_zones(disk, zone_begins, 1, print_bzr, NULL);
	if (!blkdev_report_zones(ctx->dev->bdev, zone_begins, 1, print_bzr, NULL)) {
		printk(KERN_ERR "\n reporting zones failed! \n");
	}
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
	BUG_ON(!subbioctx);
	bioctx = subbioctx->bioctx;
	BUG_ON(!bioctx);
	ctx = bioctx->ctx;
	BUG_ON(!ctx);
	bio = bioctx->orig;
	BUG_ON(!bio);
	if (clone->bi_status != BLK_STS_OK) {
		bio->bi_status = clone->bi_status;
		ctx->err = 1;
		INIT_WORK(&subbioctx->work, sub_write_err);
	} else {
		INIT_WORK(&subbioctx->work, sub_write_done);
	}
	queue_work(ctx->writes_wq, &subbioctx->work);
	bio_put(clone);
	return;
}


int lsdm_write_checks(struct ctx *ctx, struct bio *bio)
{
	unsigned nr_sectors = bio_sectors(bio);
	sector_t lba = bio->bi_iter.bi_sector;
	bio->bi_status = BLK_STS_IOERR;


	if (ctx->err) {
		bio->bi_status = BLK_STS_RESOURCE;
		goto fail;
	}
	if (unlikely(nr_sectors <= 0)) {
		goto fail;
	}
	if (lba > ctx->sb->max_pba) {
		printk(KERN_ERR "\n %s Requested write beyond disk space ! lba: %llu ", __func__, lba);
		goto fail;
	}
	if ((lba + nr_sectors) > ctx->sb->max_pba) {
		printk(KERN_ERR "\n %s Requested write beyond disk space ! lba: %llu ", __func__, lba);
		goto fail;
	}

	//printk(KERN_ERR "\n ******* Inside %s, requesting lba: %llu sectors: %d ", __func__, bio->bi_iter.bi_sector, bio_sectors(bio));
	if (is_disk_full(ctx)) {
		printk(KERN_ERR "\n %s No more space! ", __func__);
		bio->bi_status = BLK_STS_NOSPC;
		goto fail;
	}
	if (ctx->nr_freezones <= ctx->higher_watermark) {
		/* start fg gc but dont wait here unless less than lower_watermark*/
		/* setting gc_wake=1 and the next wakeup will trigger lsdm_gc(ctx, FG_GC, 0) by waking up a sleeping gc thread */
		//printk(KERN_ERR "\n 2. ctx->nr_freezones: %d, ctx->higher_watermark: %d. Starting GC.....\n", ctx->nr_freezones, ctx->higher_watermark);
		ctx->gc_th->gc_wake = 1;
		wake_up(&ctx->gc_th->lsdm_gc_wait_queue);
		if (ctx->nr_freezones <= ctx->lower_watermark) {
			//printk(KERN_ERR "\n 1. ctx->nr_freezones: %d, ctx->lower_watermark: %d. Starting GC.....\n", ctx->nr_freezones, ctx->lower_watermark);
			DEFINE_WAIT(wait);
			prepare_to_wait(&ctx->gc_th->fggc_wq, &wait,
					TASK_UNINTERRUPTIBLE);
			/* setting gc_wake=1 and the next wakeup will trigger lsdm_gc(ctx, FG_GC, 0) by waking up a sleeping gc thread */
			io_schedule();
			finish_wait(&ctx->gc_th->fggc_wq, &wait);
			//printk(KERN_ERR "\n %s %d woken up lba: %llu, nrsectors: %d ", __func__,  __LINE__, lba, nr_sectors);
		}
		if (ctx->nr_freezones < ctx->lower_watermark) {
			/* either someone stopped the GC thread or GC could not find zones to clean */
			goto fail;
		}
	}
	//printk(KERN_ERR "\n (%s) bio: lba: %llu nr_sectors: %llu \n", __func__, lba, nr_sectors);
	return 0;
fail: 
	return -1;

}

void fill_bio(struct bio *bio, sector_t pba, sector_t len, struct block_device *bdev, struct lsdm_sub_bioctx * subbio_ctx)
{
	bio_set_dev(bio, bdev);
	bio->bi_iter.bi_sector = pba; /* we use the saved write frontier */
	bio->bi_iter.bi_size = len << 9;
	bio->bi_private = subbio_ctx;
	bio->bi_end_io = lsdm_clone_endio;
}

void fill_subbioctx(struct lsdm_sub_bioctx * subbio_ctx, struct lsdm_bioctx *bioctx, sector_t lba, sector_t pba, sector_t len)
{
	BUG_ON(!len);
	subbio_ctx->bioctx = bioctx; /* This is common to all the subdivided bios */
	subbio_ctx->extent.lba = lba;
	subbio_ctx->extent.pba = pba;
	subbio_ctx->extent.len = len;
}

int prepare_bio(struct bio * clone, sector_t s8, sector_t wf)
{
	struct lsdm_sub_bioctx *subbio_ctx;
	struct lsdm_bioctx * bioctx = clone->bi_private;
	struct ctx *ctx = bioctx->ctx;
	sector_t lba = clone->bi_iter.bi_sector;
	//BUG_ON(lba > ctx->sb->max_pba);

	subbio_ctx = kmem_cache_alloc(ctx->subbio_ctx_cache, GFP_KERNEL);
	if (!subbio_ctx) {
		printk(KERN_ERR "\n %s Could not allocate memory to subbio_ctx \n", __func__);
		kmem_cache_free(ctx->bioctx_cache, bioctx);
		return -ENOMEM;
	}

	/* Next we fetch the LBA that our DM got */
	kref_get(&bioctx->ref);
	fill_subbioctx(subbio_ctx, bioctx, lba, wf, s8);
	fill_bio(clone, wf, s8, ctx->dev->bdev, subbio_ctx);
	return 0;
}

/*
 * Returns the second part of the split bio.
 * submits the first part
 */
struct bio * split_submit(struct bio *clone, sector_t s8, sector_t wf)
{
	struct lsdm_bioctx *bioctx = clone->bi_private;
	struct ctx *ctx = bioctx->ctx;
	struct bio *split;
	sector_t lba = 0;
	
	lba = clone->bi_iter.bi_sector;
	//BUG_ON(lba > ctx->sb->max_pba);

	//printk(KERN_ERR "\n %s lba: {%llu, len: %d},", __func__, lba, s8);
	/* We cannot call bio_split with spinlock held! */
	if (!(split = bio_split(clone, s8, GFP_NOIO, &fs_bio_set))){
		printk("\n %s failed at bio_split! ", __func__);
		kmem_cache_free(ctx->bioctx_cache, bioctx);
		goto fail;
	}
	/* we split and we realize that free_sectors_in_wf has reduced further by a parallel i/o
	 * we need to split again.
	 */
	split->bi_iter.bi_sector = lba;
	/* for the sake of prepare_bio */
	split->bi_private = bioctx;
	if (prepare_bio(split, s8, wf)) {
		printk(KERN_ERR "\n %s 2. FAILED prepare_bio call ", __func__);
		goto fail;
	}
	//printk(KERN_ERR "\n %s 2. zonenr: %d Submitting {lba: %llu, pba: %llu, len: %d},", __func__, get_zone_nr(ctx, wf), lba, wf, s8);
	submit_bio(split);
	/* we return the second part */
	return clone;
fail:
	return NULL;
}


int submit_bio_write(struct ctx *ctx, struct bio *clone)
{
	unsigned nr_sectors = bio_sectors(clone);
	sector_t s8, lba = clone->bi_iter.bi_sector, wf = 0;
	int maxlen = (BIO_MAX_PAGES >> 1) << SECTOR_BLK_SHIFT;
	int dosplit = 0;
	struct lsdm_bioctx * bioctx = clone->bi_private;

	/*
	BUG_ON(!bioctx);
	BUG_ON(!bioctx->ctx);
	BUG_ON(!bioctx->orig);
	*/
	clone->bi_status = BLK_STS_OK;
	kref_init(&bioctx->ref);
	do {
		/*
		BUG_ON(lba != clone->bi_iter.bi_sector);
		BUG_ON(lba > ctx->sb->max_pba);
		*/
		mykref_get(&ctx->ongoing_iocount);
		nr_sectors = bio_sectors(clone);
		//BUG_ON(!nr_sectors);
		s8 = round_up(nr_sectors, NR_SECTORS_IN_BLK);
		//BUG_ON(s8 != nr_sectors);
		dosplit = 0;
		if (s8 > maxlen) {
			s8 = maxlen;
			dosplit = 1;
		}
		down_write(&ctx->wf_lock);
		ctx->nr_app_writes += s8;
		if (s8 > ctx->free_sectors_in_wf){
			s8 = round_down(ctx->free_sectors_in_wf, NR_SECTORS_IN_BLK);
			//BUG_ON(s8 != ctx->free_sectors_in_wf);
			dosplit = 1;
		}
		//BUG_ON(!s8);
		wf = ctx->hot_wf_pba;
		clone->bi_private = bioctx;
		if (!dosplit) {
			if (prepare_bio(clone, s8, wf)) {
				printk(KERN_ERR "\n %s 1. FAILED prepare_bio call ", __func__);
				goto fail;
			}
			submit_bio(clone);
			/* Move write frontier only after a successful submit */
			//printk(KERN_ERR "\n %s 1. zonenr: %d Submitting lba: {%llu, pba: %llu, len: %d}", __func__, get_zone_nr(ctx, wf), lba, wf, s8);
			move_write_frontier(ctx, s8);
			up_write(&ctx->wf_lock);
			break;
		}
		//dosplit = 1
		clone->bi_iter.bi_sector = lba;
		clone = split_submit(clone, s8, wf);
		if (!clone) {
			goto fail;
		}
		/* Move write frontier only after a successful submit */
		move_write_frontier(ctx, s8);
		up_write(&ctx->wf_lock);
		lba = lba + s8;
	} while (1);

	//blk_finish_plug(&plug);
	kref_put(&bioctx->ref, write_done);
	return 0; 
fail:
	up_write(&ctx->wf_lock);
	printk(KERN_ERR "\n %s FAIL!!!!\n", __func__);
	kref_put(&bioctx->ref, write_done);
	bio_put(clone);
	atomic_inc(&ctx->nr_failed_writes);
	WARN_ONCE(1, "\n Write error seen, no submit! ");
	return -1;
}


void lsdm_handle_write(struct ctx *ctx)
{
	struct bio *bio;

	while ((bio = bio_list_pop(&ctx->bio_list))) {
		if (ctx->err) {
			printk(KERN_ERR "\n cannot write further, I/O error encountered! ");
			break;
		}
		//trace_printk("\n %s Processing bio: lba: %llu, len: %d to biolist", __func__, bio->bi_iter.bi_sector, bio_sectors(bio));
		if (submit_bio_write(ctx, bio)) {
			printk(KERN_ERR "\n write failed, cannot proceed! ");
			break;
		}
	}
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
	struct bio * clone;
	struct lsdm_bioctx * bioctx;
	struct lsdm_ckpt *ckpt;
	int ret;

	ret = lsdm_write_checks(ctx, bio);
	if (0 > ret) {
		printk(KERN_ERR "\n lsdm_write_checks() failed! ");
		goto memfail;
	}
	if (!bio_sectors(bio)) {
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		return DM_MAPIO_SUBMITTED;
	}
	
	atomic_set(&ctx->ioidle, 0);
	/* ckpt must be updated. The state of the filesystem is
	 * unclean until checkpoint happens!
	 */
	ckpt = (struct lsdm_ckpt *)page_address(ctx->ckpt_page);
	ckpt->clean = 0;
	clone = bio_alloc_clone(ctx->dev->bdev, bio, GFP_KERNEL, &fs_bio_set);
	if (!clone) {
		goto memfail;
	}

	bioctx = kmem_cache_alloc(ctx->bioctx_cache, GFP_KERNEL);
	if (!bioctx) {
		//trace_printk("\n Insufficient memory!");
		bio_put(clone);
		goto memfail;
	}
	bioctx->orig = bio;
	bioctx->ctx = ctx;
	/* TODO: Initialize refcount in bioctx and increment it every
	 * time bio is split or padded */
	clone->bi_private = bioctx;
	bio->bi_status = BLK_STS_OK;
	//printk(KERN_ERR "\n %s Write bio: lba: %llu, len: %d ", __func__, bio->bi_iter.bi_sector, bio_sectors(bio));
	submit_bio_write(ctx, clone);
	/*
	bio_list_add(&ctx->bio_list, clone);
	wake_up_all(&ctx->write_th->write_waitq);
	*/
	//flush_workqueue(ctx->writes_wq);
	return DM_MAPIO_SUBMITTED;
memfail:
	bio->bi_status = BLK_STS_RESOURCE;
	bio_endio(bio);
	return DM_MAPIO_KILL;
}


/*
   argv[0] = devname
   argv[1] = dm-name
   argv[2] = zone size (LBAs)
   argv[3] = max pba
   */
#define BS_NR_POOL_PAGES 65536

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
	printk(KERN_INFO "\n ** sector_nr: %lu, ckpt->magic: %u, ckpt->hot_frontier_pba: %llu", pba, ckpt->magic, ckpt->hot_frontier_pba);
	if (ckpt->magic == 0) {
		__free_pages(page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
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
void do_checkpoint(struct ctx *ctx)
{
	//printk("Inside %s" , __func__);
	/*--------------------------------------------*/
	flush_workqueue(ctx->writes_wq);
	flush_translation_blocks(ctx);
	atomic_set(&ctx->tm_flush_count, 0);
	flush_sit(ctx);
	atomic_set(&ctx->sit_flush_count, 0);
	//printk(KERN_ERR "\n sit pages flushed! nr_sit_pages: %llu sit_flush_count: %llu", atomic_read(&ctx->nr_sit_pages), atomic_read(&ctx->sit_flush_count));
	/*--------------------------------------------*/

	flush_revmap_bitmap(ctx);
	update_checkpoint(ctx);
	flush_checkpoint(ctx);

	//printk(KERN_ERR "\n checkpoint flushed! nr_pages: %ld \n", nrpages);
	//printk(KERN_ERR "\n sit pages flushed! nr_sit_pages: %llu sit_flush_count: %llu", atomic_read(&ctx->nr_sit_pages), atomic_read(&ctx->sit_flush_count));
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

	printk(KERN_INFO "\n !Reading checkpoint 1 from pba: %llu", sb->ckpt1_pba);
	ckpt1 = read_checkpoint(ctx, sb->ckpt1_pba);
	if (!ckpt1)
		return NULL;
	page1 = ctx->ckpt_page;
	/* ctx->ckpt_page will be overwritten by the next
	 * call to read_ckpt
	 */
	printk(KERN_INFO "\n !!Reading checkpoint 2 from pba: %llu", sb->ckpt2_pba);
	ckpt2 = read_checkpoint(ctx, sb->ckpt2_pba);
	if (!ckpt2) {
		__free_pages(page1, 0);
		return NULL;
	}
	printk(KERN_INFO "\n %s ckpt versions: %llu %llu", __func__, ckpt1->version, ckpt2->version);
	if (ckpt1->version >= ckpt2->version) {
		ckpt = ckpt1;
		__free_pages(ctx->ckpt_page, 0);
		nrpages--;
		//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		ctx->ckpt_page = page1;
		ctx->ckpt_pba = ctx->sb->ckpt2_pba;
		printk(KERN_ERR "\n Setting ckpt 1 version: %llu ckpt2 version: %llu \n", ckpt1->version, ckpt2->version);
	}
	else {
		ckpt = ckpt2;
		ctx->ckpt_pba = ctx->sb->ckpt1_pba;
		//page2 is rightly set by read_ckpt();
		__free_pages(page1, 0);
		nrpages--;
		//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		printk(KERN_ERR "\n Setting ckpt 1 version: %llu ckpt2 version: %llu \n", ckpt1->version, ckpt2->version);
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

int mark_zone_occupied(struct ctx *ctx , int zonenr)
{
	char *bitmap = ctx->freezone_bitmap;
	int bytenr = zonenr / BITS_IN_BYTE;
	int bitnr = zonenr % BITS_IN_BYTE;
	//char str[9];

	if (bytenr > ctx->bitmap_bytes) {
		panic("\n Trying to set an invalid bit in the free zone bitmap. bytenr > bitmap_bytes");
	}

	//get_byte_string(bitmap[bytenr], str);
	//printk(KERN_ERR "\n %s Occupying zonenr: %d, bytenr: %d, bitnr: %d byte:%s", __func__, zonenr, bytenr, bitnr, str);
	/*
	 *  printk(KERN_ERR "\n %s bitmap[%d]: %d ", __func__, bytenr, bitnr, bitmap[bytenr]);
	 */
	if(!(1 & (bitmap[bytenr] >> bitnr))) {
		/* This function can be called multiple times for the
		 * same zone. The bit is already unset and the zone 
		 * is marked occupied already.
		 */
		printk(KERN_ERR "\n BUG %s zone is already occupied! bitmap[%d]: %d, bitnr: %d ", __func__, bytenr, bitmap[bytenr], bitnr);
		//BUG_ON(1);
		return -1;
	}
	/* This bit is set and the zone is free. We want to unset it
	 */
	bitmap[bytenr] = bitmap[bytenr] ^ (1 << bitnr);
	ctx->nr_freezones = ctx->nr_freezones - 1;
	//printk(KERN_ERR "\n %s ctx->nr_freezones (2) : %u", __func__, ctx->nr_freezones);
	return 0;
}

/*
 *
 * lba: starting lba corresponding to the pba recorded in this block
 *
 */
int read_extents_from_block(struct ctx * ctx, struct tm_entry *entry, u64 lba)
{
	int i = 0;
	int nr_extents = TM_ENTRIES_BLK;
	int ret = 0;

	while (i < nr_extents) {
		i++;
		/* If there are no more recorded entries on disk, then
		 * dont add an entries further
		 */
		if (entry->pba == 0) {
			continue;
			
		}
		//printk(KERN_ERR "\n %s i: %d entry->lba: %llu entry->pba: %llu", __func__, i, lba, entry->pba);
		BUG_ON(entry->pba > ctx->sb->max_pba);
		/* TODO: right now everything should be zeroed out */
		//panic("Why are there any already mapped extents?");
		down_write(&ctx->lsdm_rb_lock);
		lsdm_rb_update_range(ctx, lba, entry->pba, NR_SECTORS_IN_BLK);
		up_write(&ctx->lsdm_rb_lock);
		lba = lba + NR_SECTORS_IN_BLK; /* Every 512 bytes sector has an LBA in a SMR drive */
		entry = entry + 1;
		ret = 1;
	}
	return ret;
}

int add_tm_page_kv_store_by_blknr(struct ctx *ctx, struct page *page, int blknr)
{
	struct rb_root *root = &ctx->tm_rb_root;
	struct rb_node *parent = NULL, **link = &root->rb_node;
	struct rb_node *node = NULL;
	struct tm_page *node_ent;
	struct tm_page *new_tmpage, *parent_ent;

	BUG_ON(blknr > ctx->sb->blk_count_tm);

	parent = NULL;
	node = root->rb_node;
	while(node) {
		parent = node;
		node_ent = rb_entry(node, struct tm_page, rb);
		if (blknr == node_ent->blknr) {
			return -1;
		}
		if (blknr < node_ent->blknr) {
			node = node->rb_left;
		} else {
			node = node->rb_right;
		}
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
		return -ENOMEM;
	}
	RB_CLEAR_NODE(&new_tmpage->rb);
	//printk("\n %s lba: %llu blknr: %d tm_pba: %llu \n", __func__, lba, blknr, ctx->sb->tm_pba);
	new_tmpage->page = page;
	new_tmpage->blknr = blknr;
	/* Add this page to a RB tree based KV store.
	 * Key is: blknr for this corresponding block
	 */
	rb_link_node(&new_tmpage->rb, parent, link);
	/* Balance the tree after node is addded to it */
	rb_insert_color(&new_tmpage->rb, root);
	atomic_inc(&ctx->nr_tm_pages);
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
	unsigned long blknr, sectornr;
	/* blk_count_tm is 4096 bytes aligned number */
	unsigned long nrblks = ctx->sb->blk_count_tm;
	struct page *page;
	int i = 0, ret = 0;
	struct tm_entry * tm_entry = NULL;
	u64 lba = 0;

	printk(KERN_ERR "\n %s Reading TM entries from: %llu, nrblks: %ld", __func__, ctx->sb->tm_pba, nrblks);
	
	ctx->n_extents = 0;
	sectornr = 0;
	while(i < nrblks) {
		//printk(KERN_ERR "\n reading block: %llu, sector: %llu", ctx->sb->tm_pba, sectornr);
		page = read_block(ctx, ctx->sb->tm_pba, sectornr);
		if (!page)
			return -1;
		/* We read the extents in the entire block. the
		 * redundant extents should be unpopulated and so
		 * we should find 0 and break out */
		//trace_printk("\n pba: %llu", pba);
		tm_entry = (struct tm_entry *) page_address(page);
		ret = read_extents_from_block(ctx, tm_entry, lba);
		if (!ret) {
			__free_pages(page, 0);
			nrpages--;
		} else {
			add_tm_page_kv_store_by_blknr(ctx, page, sectornr/NR_SECTORS_IN_BLK);
		}
		/* Every 512 byte sector has a LBA in SMR drives, the translation map is recorded for every block
		 * instead. So every translation entry covers 8 sectors or 8 lbas.
		 */
		lba = lba + (TM_ENTRIES_BLK * 8);
		i = i + 1;
		//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		// sectornr = sectornr + (ctx->q->limits.physical_block_size/ctx->q->limits.logical_block_size);
		sectornr = sectornr + NR_SECTORS_IN_BLK;
		blknr = blknr + 1;
	}
	printk(KERN_ERR "\n %s TM entries read!", __func__);
	return 0;
}

int read_revmap_bitmap(struct ctx *ctx)
{
	unsigned long nrblks = ctx->sb->blk_count_revmap_bm;

	printk(KERN_ERR "\n %s nrblks: %lu \n", __func__, nrblks);
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

	add_block_based_translation(ctx,  page, __func__);
	//trace_printk("\n Added block based translations!, will add memory based extent maps now.... \n");
	entry_sector = (struct lsdm_revmap_entry_sector *) page_address(page);
	while (i < NR_SECTORS_IN_BLK) {
		extent = entry_sector->extents;
		for (j=0; j < NR_EXT_ENTRIES_PER_SEC; j++) {
			if (extent[j].pba == 0)
				continue;
			//down_write(&ctx->lsdm_rb_lock);
			lsdm_rb_update_range(ctx, extent[j].lba, extent[j].pba, extent[j].len);
			//up_write(&ctx->lsdm_rb_lock);
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
	struct block_device *bdev = NULL;
	char flush_needed = 0;
	struct page * revmap_page;
	unsigned int nr_revmap_blks = ctx->sb->blk_count_revmap;
	int bits = 0;
	unsigned int blknr, sectornr;

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
	sectornr = ctx->revmap_pba;
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
				revmap_page = read_block(ctx, sectornr, blknr);
				if (!revmap_page) {
					return -1;
				}
				process_revmap_entries_on_boot(ctx, revmap_page);
				__free_pages(revmap_page, 0);
				nrpages--;
				//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
				if (blknr >= nr_revmap_blks)
					break;
				//sectornr = sectornr + (ctx->q->limits.physical_block_size/ctx->q->limits.logical_block_size);
				sectornr = sectornr + NR_SECTORS_IN_BLK;
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
		//printk(KERN_ERR "\n Why do we need to flush!!");
		//flush_translation_blocks(ctx);
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

unsigned int get_cb_cost(struct ctx *ctx , u32 nrblks, u64 mtime)
{
	unsigned int u, age;
	struct lsdm_sb *sb = ctx->sb;

	u = (nrblks * 100) >> (sb->log_zone_size - sb->log_block_size);

	if (!(ctx->max_mtime - ctx->min_mtime)) {
		dump_stack();
		return u;
	}
	age = 100 - div_u64(100 * (mtime - ctx->min_mtime),
				ctx->max_mtime - ctx->min_mtime);

	return ((100 * (100 - u) * age)/ (100 + u));
}


unsigned int get_cost(struct ctx *ctx, u32 nrblks, u64 age, char gc_mode)
{
	if (gc_mode == GC_GREEDY) {
		return nrblks;
	}
	return get_cb_cost(ctx, nrblks, age);
}

int remove_zone_from_cost_node(struct ctx *ctx, struct gc_cost_node *cost_node, unsigned int zonenr);

int remove_zone_from_gc_tree(struct ctx *ctx, unsigned int zonenr)
{
	struct rb_root *root = &ctx->gc_zone_root;
	struct gc_zone_node *znode = NULL;
	struct gc_cost_node *cost_node = NULL;
	struct rb_node *link = root->rb_node;
	int temp;

	while (link) {
		znode = container_of(link, struct gc_zone_node, rb);
		if (znode->zonenr == zonenr) {
			cost_node = znode->ptr_to_cost_node;
			//printk(KERN_ERR "\n %s removing zone: %d from the gc_cost_root tree", __func__, zonenr);
			remove_zone_from_cost_node(ctx, cost_node, zonenr);
			rb_erase(&znode->rb, root);
			kmem_cache_free(ctx->gc_zone_node_cache, znode);
			/*
			if (zonenr == select_zone_to_clean(ctx, BG_GC, __func__)) {
				printk(KERN_ERR "\n %s zonenr selected next time: %d is same as removed!! \n", __func__, zonenr);
				BUG_ON(1);
			} */
			return (0);
		}
		if (znode->zonenr < zonenr) {
			link = link->rb_right;
		} else {
			link = link->rb_left;
		}
	}
	/* did not find the zone in rb tree */
	temp = select_zone_to_clean(ctx, BG_GC, __func__);
	printk("\n %s could not find zone: %d in the zone tree! zone selected for next round: %d \n", __func__, zonenr, temp);
	return (-1);
}


struct gc_zone_node * add_zonenr_gc_zone_tree(struct ctx *ctx, unsigned int zonenr, u32 nrblks)
{
	struct rb_root *root = &ctx->gc_zone_root;
	struct gc_zone_node * znew = NULL, *e = NULL;
	struct rb_node **link = &root->rb_node, *parent = NULL;

	while (*link) {
		parent = *link;
		e = container_of(parent, struct gc_zone_node, rb);
		if (e->zonenr == zonenr) {
			return e;
		}
		if (e->zonenr < zonenr) {
			link = &(*link)->rb_right;
		} else {
			link = &(*link)->rb_left;
		}
	}
	znew = kmem_cache_alloc(ctx->gc_zone_node_cache, GFP_KERNEL | __GFP_ZERO);
	if (!znew) {
		printk(KERN_ERR "\n %s could not allocate memory for gc_zone_node \n", __func__);
		return NULL;
	}
	znew->zonenr = zonenr;
	znew->vblks = nrblks;
	znew->ptr_to_cost_node = NULL;
	INIT_LIST_HEAD(&znew->list);

	/* link holds the address of left or the right pointer
	 * appropriately
	 */
	rb_link_node(&znew->rb, parent, link);
	rb_insert_color(&znew->rb, root);
	//printk(KERN_ERR "\n %s Added zone: %d nrblks: %d to gc tree!znode: %p  znode->list: %p \n", __func__, zonenr, nrblks, znew, znew->list);
	return znew;
}

int remove_zone_from_cost_node(struct ctx *ctx, struct gc_cost_node *cost_node, unsigned int zonenr)
{
	int zcount = 0;
	struct list_head *list_head = NULL;
	struct gc_zone_node *zone_node = NULL, *next_node = NULL;
	int flag = 0;

	zcount = 0;

	if (!cost_node) {
		return -1;
	}

	list_head = &cost_node->znodes_list;
	//printk(KERN_ERR "\n %s zonenr: %d and cost: %llu list_head: %p cost_node: %p", __func__, zonenr, cost_node->cost, list_head, cost_node);
	BUG_ON(list_head == NULL);
	BUG_ON(list_head->next == NULL);
	/* We remove znode from the list maintained by cost node. If this is the last node on the list 
	 * then we have to remove the cost node from the tree
	 */
	list_for_each_entry_safe(zone_node, next_node, list_head, list) {
		zcount = zcount + 1;
		if (zone_node->zonenr == zonenr) {
			//printk(KERN_ERR "\n %s Deleting zone: %d from the cost node %p zone list! nrblks: %d \n", __func__, zonenr, cost_node, zone_node->vblks);
			zone_node->ptr_to_cost_node = NULL;
			list_del(&zone_node->list);
			zcount = zcount - 1;
			flag = 1;
		}
		if (zcount >= 1 && flag == 1)
			break;
	}
	if (!zcount) {
		//printk(KERN_ERR "\n %s Zone: %d was the only one on the cost node. Deleting the cost_node %p now! \n", __func__, zonenr, cost_node);
		rb_erase(&cost_node->rb, &ctx->gc_cost_root);
		kmem_cache_free(ctx->gc_cost_node_cache, cost_node);
	}
	return 0;
}
/*
 *
 * TODO: Current default is Cost Benefit.
 * But in the foreground mode, we want to do GC_GREEDY.
 * Add a parameter in this function and write code for this
 *
 */
int update_gc_tree(struct ctx *ctx, unsigned int zonenr, u32 nrblks, u64 mtime, const char * caller)
{
	struct rb_root *root = &ctx->gc_cost_root;
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct gc_cost_node *cost_node = NULL, *new = NULL;
	struct gc_zone_node *znode = NULL;
	unsigned int cost = 0;

	if (nrblks == 0) {
		//printk(KERN_ERR "\n %s Removing zone: %d from gc tree! \n", __func__, zonenr);
		remove_zone_from_gc_tree(ctx, zonenr);
		return 0;
	}
	//cost = get_cost(ctx, nrblks, mtime, GC_CB);
	cost = get_cost(ctx, nrblks, mtime, GC_GREEDY);
	znode = add_zonenr_gc_zone_tree(ctx, zonenr, nrblks);
	if (!znode) {
		printk(KERN_ERR "\n %s gc data structure allocation failed!! \n", __func__);
		panic("No memory available for znode");
	}

	if (znode->ptr_to_cost_node) {
		new = znode->ptr_to_cost_node;
		if (new->cost == cost) {
			return 0;
		}
		/* else, we remove znode from the list maintained by cost node. If this is the last node on the list
		 * then we have to remove the cost node from the tree
		 */
		//printk(KERN_ERR "\n %s zonenr: %d vblks: %d cost_node:%p ", __func__, zonenr, nrblks, new);
		remove_zone_from_cost_node(ctx, new, zonenr);
		new = NULL;
		znode->ptr_to_cost_node = NULL;
	}

	znode->vblks = nrblks;
		
	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		cost_node = container_of(parent, struct gc_cost_node, rb);
		if (cost == cost_node->cost) {
			znode->ptr_to_cost_node = cost_node;
			list_add_tail(&znode->list, &cost_node->znodes_list);
			//printk(KERN_ERR "\n %s zonenr: %d nrblks: %u, mtime: %llu caller: (%s) cost:%d cost_node: %p list_head: %p \n", __func__, zonenr, nrblks, mtime, caller, cost, cost_node, cost_node->znodes_list);
			return 0;
		}
		 /* For Greedy, cost is #valid blks. We prefer lower cost.
		 */
		if (cost > cost_node->cost) {
			link = &(*link)->rb_left;
		} else {
			link = &(*link)->rb_right;
		}
	}
	/* We are essentially adding a new node here */
	new = kmem_cache_alloc(ctx->gc_cost_node_cache, GFP_KERNEL | __GFP_ZERO);
	if (!new) {
		printk(KERN_ERR "\n %s could not allocate memory for gc_cost_node \n", __func__);
		return -ENOMEM;
	}
	INIT_LIST_HEAD(&new->znodes_list);
	//printk(KERN_ERR "\n Allocate gc_cost_node: %p, cost: %d zone: %d nrblks: %d list_head: %p \n", new, cost, znode->zonenr, znode->vblks, &new->znodes_list);
	new->cost = cost;
	RB_CLEAR_NODE(&new->rb);
	znode->ptr_to_cost_node = new;
	//printk(KERN_ERR "\n  %s adding znode to the cost node's zone list! \n ", __func__);
	list_add(&znode->list, &new->znodes_list);

	//printk(KERN_ERR "\n %s new cost node initialized, about to add it to the tree! \n", __func__);

	/* link holds the address of left or the right pointer
	 * appropriately
	 */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);

	zonenr = select_zone_to_clean(ctx, BG_GC, __func__);
	//printk(KERN_ERR "\n %s zone to clean: %d ", __func__, zonenr);
	return 0;
}

/* 
 * Returns -1 when a page does not need to be added
 * Return 0 otherwise
 */
int add_sit_page_kv_store_by_blknr(struct ctx *ctx, struct page *page, sector_t sector_nr)
{
	struct rb_root *root = &ctx->sit_rb_root;
        struct sit_page *parent_ent, *new, *node_ent;
        struct rb_node *parent = NULL;
	struct rb_node *node = NULL;
	unsigned int sit_blknr = sector_nr / NR_SECTORS_IN_BLK;

	//trace_printk("\n %s pba: %llu zonenr: %lld blknr: %lld", __func__, pba, zonenr, blknr);
	
	new = kmem_cache_alloc(ctx->sit_page_cache, GFP_KERNEL);
	if (!new) {
		print_memory_usage(ctx, __func__);
		printk(KERN_ERR "\n Could not allocate memory to sit page \n");
		return -ENOMEM;
	}
	
	RB_CLEAR_NODE(&new->rb);
	new->flag = NEEDS_NO_FLUSH;
	new->blknr = sit_blknr;
	new->page = page;

	parent = NULL;
	node = root->rb_node;
	while(node) {
		parent = node;
		node_ent = rb_entry(parent, struct sit_page, rb);
		if (new->blknr == node_ent->blknr) {
			return -1;
		}
		if (new->blknr < node_ent->blknr) {
			node = node->rb_left;
		} else {
			node = node->rb_right;
		}
	}

	if (parent) {
		/* Add this page to a RB tree based KV store.
		 * Key is: blknr for this corresponding block
		 */
		parent_ent = rb_entry(parent, struct sit_page, rb);
		if (new->blknr < parent_ent->blknr) {
			/* Attach new node to the left of parent */
			rb_link_node(&new->rb, parent, &parent->rb_left);
			//printk(KERN_ERR "\n Adding a new node at parent->rb_left: %p, new page address: %p", page_address(parent_ent->page), page_address(new->page));

		}
		else { 
			/* Attach new node to the right of parent */
			rb_link_node(&new->rb, parent, &parent->rb_right);
			//printk(KERN_ERR "\n Adding a new node at parent->rb_right: %p, new page address: %p", page_address(parent_ent->page), page_address(new->page));
		}
	} else {
		rb_link_node(&new->rb, NULL, &root->rb_node);
		parent_ent = rb_entry(root->rb_node, struct sit_page, rb);
		//printk(KERN_ERR "\n Adding a root node: %p, new page address: %p", page_address(parent_ent->page), page_address(new->page));
	}
	/* Balance the tree after the blknr is addded to it */
	rb_insert_color(&new->rb, root);
	atomic_inc(&ctx->nr_sit_pages);
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
       
	sb = ctx->sb;
	nr_blks_in_zone = (1 << (sb->log_zone_size - sb->log_block_size));
	//printk("\n Number of seg entries: %u", nr_seg_entries);

	while (i < nr_seg_entries) {
		/* 0th zonenr is the zone that holds all the metadata */
		if ((*zonenr == get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba)) ||
		    (*zonenr == get_zone_nr(ctx, ctx->ckpt->warm_gc_frontier_pba))) {
			/* 1 indicates zone is free, 0 is the default bit because of kzalloc */
			printk(KERN_ERR "\n zonenr: %d vblocks: %u is our cur_frontier! not marking it free!", *zonenr, entry->vblocks);
			entry = entry + 1;
			*zonenr= *zonenr + 1;
			i++;
			continue;
		}
		else if (entry->vblocks == 0) {
    			//printk(KERN_ERR "\n *segnr: %u", *zonenr);
			mark_zone_free(ctx , *zonenr, 1);
		}
		else if (entry->vblocks < nr_blks_in_zone) {
			//printk(KERN_ERR "\n *segnr: %u entry->vblocks: %llu entry->mtime: %llu", *zonenr, entry->vblocks, entry->mtime);
			if (!update_gc_tree(ctx, *zonenr, entry->vblocks, entry->mtime, __func__))
				panic("Memory error, write a memory shrinker!");
		} else {
			//printk(KERN_ERR "\n %s segnr: %llu, vblocks: %llu, mtime: %llu ", __func__, *zonenr, entry->vblocks, entry->mtime);
		}
		if (ctx->min_mtime > entry->mtime)
			ctx->min_mtime = entry->mtime;
		if (ctx->max_mtime < entry->mtime)
			ctx->max_mtime = entry->mtime;
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
	
	printk(KERN_ERR "\n nr_data_zones: %lu", nr_data_zones);
	ret = allocate_freebitmap(ctx);
	printk(KERN_INFO "\n Allocated free bitmap, ret: %d", ret);
	if (0 > ret)
		return ret;


	ctx->min_mtime = ULLONG_MAX;
	ctx->max_mtime = get_elapsed_time(ctx);
	nrblks = sb->blk_count_sit;
	sectornr = 0;
	printk(KERN_ERR "\n ctx->ckpt->hot_frontier_pba: %llu", ctx->ckpt->hot_frontier_pba);
	printk(KERN_ERR "\n ctx->ckpt->warm_gc_frontier_pba: %llu", ctx->ckpt->warm_gc_frontier_pba);
	printk(KERN_ERR "\n get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba): %u", get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba));
	printk(KERN_ERR "\n %s Read seginfo from pba: %llu sectornr: %d zone0_pba: %llu \n", __func__, sb->sit_pba, sectornr, ctx->sb->zone0_pba);
	printk("\n ctx->hot_frontier_pba: %llu, ckpt->frontier zone: %u", ctx->ckpt->hot_frontier_pba, get_zone_nr(ctx, ctx->ckpt->hot_frontier_pba));
	while (nr_data_zones > 0) {
		//trace_printk("\n zonenr: %u", zonenr);
		if ((sectornr + sb->sit_pba) > ctx->sb->zone0_pba) {
			printk(KERN_ERR "\n Seg entry blknr cannot be bigger than the data blknr");
			return -1;
		}
		sit_page = read_block(ctx, sb->sit_pba, sectornr);
		if (!sit_page) {
			printk(KERN_ERR "\n %s Could not read sit pba: %lld ", __func__, sb->sit_pba);
			kfree(ctx->freezone_bitmap);
			return -1;
		}
		entry0 = (struct lsdm_seg_entry *) page_address(sit_page);
		if (nr_data_zones > nr_seg_entries_blk) {
			nr_seg_entries_read = nr_seg_entries_blk;
		}
		else {
			nr_seg_entries_read = nr_data_zones;
			printk(KERN_ERR "\n Usual segentries: %d, (now) last blk has: %lu \n", nr_seg_entries_blk, nr_data_zones);
		}
		nr_data_zones = nr_data_zones - nr_seg_entries_read;
		ret = read_seg_entries_from_block(ctx, entry0, nr_seg_entries_read, &zonenr);
		add_sit_page_kv_store_by_blknr(ctx, sit_page, sectornr);
		//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		//sectornr = sectornr + (ctx->q->limits.physical_block_size/ctx->q->limits.logical_block_size);
		sectornr = sectornr + NR_SECTORS_IN_BLK;
	}
	printk(KERN_ERR "\n %s ctx->nr_freezones (2) : %u zonenr: %u", __func__, ctx->nr_freezones, zonenr);
	return 0;
}

struct lsdm_sb * read_superblock(struct ctx *ctx, unsigned long pba)
{

	//struct block_device *bdev = ctx->dev->bdev;
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
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
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
	//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
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
		printk(KERN_ERR "\n %s nrpages: %ld", __func__, nrpages);
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
	printk(KERN_ERR "\n %s %d ctx->hot_wf_pba: %llu\n", __func__, __LINE__, ctx->hot_wf_pba);
	ctx->hot_wf_end = zone_end(ctx, ctx->hot_wf_pba);
	printk(KERN_ERR "\n %s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->hot_wf_end);
	printk(KERN_ERR "\n max_pba = %llu", ctx->max_pba);
	ctx->free_sectors_in_wf = ctx->hot_wf_end - ctx->hot_wf_pba + 1;
	printk(KERN_ERR "\n ctx->free_sectors_in_wf: %lld", ctx->free_sectors_in_wf);
	
	ctx->warm_gc_wf_pba = ctx->ckpt->warm_gc_frontier_pba;
	printk(KERN_ERR "\n %s %d ctx->hot_wf_pba: %llu\n", __func__, __LINE__, ctx->hot_wf_pba);
	ctx->warm_gc_wf_end = zone_end(ctx, ctx->warm_gc_wf_pba);
	printk(KERN_ERR "\n %s %d kernel wf end: %llu\n", __func__, __LINE__, ctx->hot_wf_end);
	printk(KERN_ERR "\n max_pba = %llu", ctx->max_pba);
	ctx->free_sectors_in_gc_wf = ctx->warm_gc_wf_end - ctx->warm_gc_wf_pba + 1;

	ret = read_revmap_bitmap(ctx);
	if (ret) {
		__free_pages(ctx->sb_page, 0);
		nrpages--;
		__free_pages(ctx->ckpt_page, 0);
		nrpages--;
		printk(KERN_ERR "\n %s nrpages: %ld ", __func__, nrpages);
		return -1;
	}
	printk(KERN_ERR "\n before: PBA for first revmap blk: %llu", ctx->sb->revmap_pba/NR_SECTORS_IN_BLK);
	read_revmap(ctx);
	printk(KERN_INFO "\n Reverse map Read!");
	ret = read_translation_map(ctx);
	if (0 > ret) {
		__free_pages(ctx->sb_page, 0);
		__free_pages(ctx->ckpt_page, 0);
		__free_pages(ctx->revmap_bm, 0);
		printk(KERN_ERR "\n read_extent_map failed! cannot read the metadata ");
		return ret;
	}
	printk(KERN_INFO "\n %s extent_map read!", __func__);
	ctx->nr_freezones = 0;
	ctx->bitmap_bytes = sb2->zone_count_main /BITS_IN_BYTE;
	if (sb2->zone_count_main % BITS_IN_BYTE) {
		ctx->bitmap_bytes = ctx->bitmap_bytes + 1;
		ctx->bitmap_bit = (sb2->zone_count_main % BITS_IN_BYTE);
	}
	printk(KERN_ERR "\n %s Nr of zones in main are: %llu, bitmap_bytes: %d, bitmap_bit: %d ", __func__, sb2->zone_count_main, ctx->bitmap_bytes, ctx->bitmap_bit);
	if (sb2->zone_count_main % BITS_IN_BYTE > 0)
		ctx->bitmap_bytes += 1;
	read_seg_info_table(ctx);
	printk(KERN_INFO "\n %s ctx->nr_freezones: %u, ckpt->nr_free_zones:%u", __func__, ctx->nr_freezones, ckpt->nr_free_zones);
	if (ctx->nr_freezones != ckpt->nr_free_zones) { 
		/* TODO: Do some recovery here.
		 * We do not wait for confirmation of SIT pages on the
		 * disk. we match the SIT entries to that by the
		 * translation map. we also make sure that the ckpt
		 * entries are based on the translation map
		 */
		printk(KERN_ERR "\n SIT and checkpoint does not match!");
		ckpt->nr_free_zones = ctx->nr_freezones;
		/*
		//goto out;
		do_recovery(ctx);
		__free_pages(ctx->sb_page, 0);
		nrpages--;
		__free_pages(ctx->ckpt_page, 0);
		nrpages--;
		__free_pages(ctx->revmap_bm, 0);
		nrpages--;
		//printk(KERN_ERR "\n %s nrpages: %llu", __func__, nrpages);
		return -1;
		*/
	}
//out:
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
	kmem_cache_destroy(ctx->bio_cache);
	kmem_cache_destroy(ctx->extent_cache);
	kmem_cache_destroy(ctx->rev_extent_cache);
	kmem_cache_destroy(ctx->app_read_ctx_cache);
	kmem_cache_destroy(ctx->gc_extents_cache);
	kmem_cache_destroy(ctx->subbio_ctx_cache);
	kmem_cache_destroy(ctx->tm_page_cache);
	kmem_cache_destroy(ctx->gc_cost_node_cache);
	kmem_cache_destroy(ctx->gc_zone_node_cache);
	kmem_cache_destroy(ctx->sit_page_cache);
	kmem_cache_destroy(ctx->revmap_bioctx_cache);
	kmem_cache_destroy(ctx->bioctx_cache);
}


static int create_caches(struct ctx *ctx)
{
	ctx->bioctx_cache = kmem_cache_create("bioctx_cache", sizeof(struct lsdm_bioctx), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->bioctx_cache) {
		return -1;
	}
	ctx->revmap_bioctx_cache = kmem_cache_create("revmap_bioctx_cache", sizeof(struct revmap_bioctx), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->revmap_bioctx_cache) {
		goto destroy_cache_bioctx;
	}
	ctx->sit_page_cache = kmem_cache_create("sit_page_cache", sizeof(struct sit_page), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->sit_page_cache) {
		goto destroy_revmap_bioctx_cache;
	}
	ctx->tm_page_cache = kmem_cache_create("tm_page_cache", sizeof(struct tm_page), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->tm_page_cache) {
		goto destroy_sit_page_cache;
	}
	ctx->gc_cost_node_cache = kmem_cache_create("gc_cost_node_cache", sizeof(struct gc_cost_node), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->gc_cost_node_cache) {
		goto destroy_tm_page_cache;
	}
	ctx->gc_zone_node_cache = kmem_cache_create("gc_zone_node_cache", sizeof(struct gc_zone_node), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->gc_zone_node_cache) {
		goto destroy_gc_cost_node_cache;
	}
	ctx->subbio_ctx_cache = kmem_cache_create("subbio_ctx_cache", sizeof(struct lsdm_sub_bioctx), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->subbio_ctx_cache) {
		goto destroy_gc_zone_node_cache;
	}
	ctx->gc_extents_cache = kmem_cache_create("gc_extents_cache", sizeof(struct gc_extents), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->gc_extents_cache) {
		goto destroy_subbio_ctx_cache;
	}
	//trace_printk("\n gc_extents_cache initialized to address: %p", ctx->gc_extents_cache);
	ctx->app_read_ctx_cache = kmem_cache_create("app_read_ctx_cache", sizeof(struct app_read_ctx), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->app_read_ctx_cache) {
		goto destroy_gc_extents_cache;
	}
	//trace_printk("\n app_read_ctx_cache initialized to address: %p", ctx->app_read_ctx_cache);
	ctx->extent_cache = kmem_cache_create("lsdm_extent_cache", sizeof(struct extent), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->extent_cache) {
		goto destroy_app_read_ctx_cache;
	}
	ctx->rev_extent_cache = kmem_cache_create("lsdm_rev_extent_cache", sizeof(struct rev_extent), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->extent_cache) {
		goto destroy_extent_cache;
	}
	ctx->bio_cache = kmem_cache_create("lsdm_bio_cache", sizeof(struct bio), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
	if (!ctx->bio_cache) {
		goto destroy_rev_extent_cache;
	}

	return 0;
/* failed case */
destroy_rev_extent_cache:
	kmem_cache_destroy(ctx->rev_extent_cache);
destroy_extent_cache:
	kmem_cache_destroy(ctx->extent_cache);
destroy_app_read_ctx_cache:
	kmem_cache_destroy(ctx->app_read_ctx_cache);
destroy_gc_extents_cache:
	kmem_cache_destroy(ctx->gc_extents_cache);
destroy_subbio_ctx_cache:
	kmem_cache_destroy(ctx->subbio_ctx_cache);
destroy_gc_cost_node_cache:
	kmem_cache_destroy(ctx->gc_cost_node_cache);
destroy_gc_zone_node_cache:
	kmem_cache_destroy(ctx->gc_zone_node_cache);
destroy_tm_page_cache:
	kmem_cache_destroy(ctx->tm_page_cache);
destroy_sit_page_cache:
	kmem_cache_destroy(ctx->sit_page_cache);
destroy_revmap_bioctx_cache:
	kmem_cache_destroy(ctx->revmap_bioctx_cache);
destroy_cache_bioctx:
	kmem_cache_destroy(ctx->bioctx_cache);
	return -1;
}

static int lsdm_ctr(struct dm_target *target, unsigned int argc, char **argv)
{
	int ret = -ENOMEM;
	struct ctx *ctx;
	unsigned long long max_pba;
	loff_t disk_size;
	struct request_queue *q;

	nrpages = 0;

	printk(KERN_INFO "\n argc: %d", argc);
	if (argc < 2) {
		target->error = "dm-lsdm: Invalid argument count";
		return -EINVAL;
	}

	printk(KERN_INFO "\n argc: %d, argv[0]: %s argv[1]: %s ", argc, argv[0], argv[1]);

	ctx = kzalloc(sizeof(struct ctx), GFP_KERNEL);
	if (!ctx) {
		target->error = "dm-lsdm: Memory error";
		return -ENOMEM;
	}

	target->private = ctx;
	/* 13 comes from 9 + 3, where 2^9 is the number of bytes in a sector
	 * and 2^3 is the number of sectors in a block.
	 */
	target->max_io_len = BIO_MAX_PAGES >> 1;
	target->flush_supported = true;
	target->discards_supported = true;
	/* target->per_io_data_size - set this to get per_io_data_size allocated before every standard structure that holds a bio. */

	ret = dm_get_device(target, argv[0], dm_table_get_mode(target->table), &ctx->dev);
    	if (ret) {
		target->error = "lsdm: Device lookup failed.";
		goto free_ctx;
	}

	if (!bdev_is_zoned(ctx->dev->bdev)) {
                target->error = "Not a zoned block device";
                ret = -EINVAL;
                goto free_ctx;
        }
	ret = blkdev_report_zones(ctx->dev->bdev, 1572864, 1, print_bzr, NULL);
	if (!ret) {
		printk(KERN_ERR "\n reporting zones failed! \n");
	}

	q = bdev_get_queue(ctx->dev->bdev);
	printk(KERN_ERR "\n number of sectors in a zone: %llu", bdev_zone_sectors(ctx->dev->bdev));
	printk(KERN_ERR "\n number of zones in device: %u", bdev_nr_zones(ctx->dev->bdev));
	printk(KERN_ERR "\n block size in sectors: %u ", q->limits.logical_block_size);

	ctx->q = q;

	printk(KERN_INFO "\n sc->dev->bdev->bd_part->start_sect: %llu", ctx->dev->bdev->bd_start_sect);
	//printk(KERN_INFO "\n sc->dev->bdev->bd_part->nr_sects: %llu", ctx->dev->bdev->bd_part->nr_sects);


	disk_size = i_size_read(ctx->dev->bdev->bd_inode);
	printk(KERN_INFO "\n The disk size as read from the bd_inode: %llu", disk_size);
	printk(KERN_INFO "\n max sectors: %llu", disk_size/512);
	printk(KERN_INFO "\n max blks: %llu", disk_size/4096);
	ctx->writes_wq = create_workqueue("writes_queue");
	ctx->tm_wq = create_workqueue("tm_queue");
	
	bio_list_init(&ctx->bio_list);
	init_rwsem(&ctx->wf_lock);
	mutex_init(&ctx->bm_lock);
	mutex_init(&ctx->tm_lock);
	mutex_init(&ctx->sit_kv_store_lock);
	mutex_init(&ctx->tm_kv_store_lock);

	spin_lock_init(&ctx->tm_ref_lock);
	spin_lock_init(&ctx->tm_flush_lock);
	mutex_init(&ctx->sit_flush_lock);
	spin_lock_init(&ctx->rev_flush_lock);
	spin_lock_init(&ctx->ckpt_lock);
	spin_lock_init(&ctx->gc_ref_lock);

	atomic_set(&ctx->io_count, 0);
	ctx->nr_reads = 0;
	atomic_set(&ctx->pages_alloced, 0);
	ctx->nr_app_writes = 0;
	atomic_set(&ctx->nr_failed_writes, 0);
	atomic_set(&ctx->revmap_entry_nr, 0);
	atomic_set(&ctx->revmap_sector_nr, 0);
	atomic_set(&ctx->sit_ref, 0);
	atomic_set(&ctx->tm_flush_count, 0);
	atomic_set(&ctx->sit_flush_count, 0);
	atomic_set(&ctx->ioidle, 1);
	ctx->target = 0;
	atomic_set(&ctx->nr_pending_writes, 0);
	atomic_set(&ctx->nr_tm_writes, 0);
	atomic_set(&ctx->nr_sit_pages, 0);
	atomic_set(&ctx->nr_tm_pages, 0);
	init_waitqueue_head(&ctx->refq);
	init_waitqueue_head(&ctx->sitq);
	init_waitqueue_head(&ctx->tmq);
	ctx->mounted_time = ktime_get_real_seconds();
	ctx->extent_tbl_root = RB_ROOT;
	ctx->rev_tbl_root = RB_ROOT;
	init_rwsem(&ctx->lsdm_rb_lock);
	init_rwsem(&ctx->lsdm_rev_lock);

	ctx->tm_rb_root = RB_ROOT;
	ctx->sit_rb_root = RB_ROOT;
	ctx->gc_cost_root = RB_ROOT;
	ctx->gc_zone_root = RB_ROOT;

	rwlock_init(&ctx->sit_rb_lock);

	ctx->sectors_copied = 0;
	ret = -ENOMEM;
	target->error = "dm-lsdm: No memory";


	//printk(KERN_INFO "about to call bioset_init()");
	ctx->gc_bs = kzalloc(sizeof(*(ctx->gc_bs)), GFP_KERNEL);
	if (!ctx->gc_bs)
		goto put_dev;

	if(bioset_init(ctx->gc_bs, BS_NR_POOL_PAGES, 0, BIOSET_NEED_BVECS) == -ENOMEM) {
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

	mykref_init(&ctx->ongoing_iocount);
	//trace_printk("\n About to read metadata! 5 ! \n");
	ctx->err = 0;

    	ret = read_metadata(ctx);
	if (ret < 0) 
		goto destroy_cache;

	unsigned int gc_pool_pages = 1 << (ctx->sb->log_zone_size - ctx->sb->log_block_size);
	ctx->gc_page_pool = mempool_create_page_pool(gc_pool_pages, 0);
	if (!ctx->gc_page_pool)
		goto free_metadata_pages;

	max_pba = (ctx->dev->bdev->bd_inode->i_size) / 512;
	sprintf(ctx->nodename, "lsdm/%s", argv[1]);
	ret = -EINVAL;
	if (ctx->max_pba > max_pba) {
		printk(KERN_ERR "\n formatted max_pba (ctx->max_pba): %llu", ctx->max_pba);
		printk(KERN_ERR "\n device records max_pba: %llu", max_pba);
		target->error = "dm-lsdm: Invalid max pba found on sb";
		goto destroy_gc_page_pool;
	}

	/* lower watermark is at 5 %, watermark represents nrfreezones */
	//ctx->lower_watermark = ctx->sb->zone_count / 20; 
	//ctx->higher_watermark = ctx->lower_watermark + 20;
	ctx->lower_watermark = 3;
	ctx->middle_watermark = 6;
	ctx->higher_watermark = 10;
	printk(KERN_ERR "\n zone_count: %lld lower_watermark: %d middle_watermark: %d higher_watermark: %d ", ctx->sb->zone_count, ctx->lower_watermark, ctx->middle_watermark, ctx->higher_watermark);
	//ctx->higher_watermark = ctx->lower_watermark >> 2; 
	/*
	if (ctx->sb->zone_count > SMALL_NR_ZONES) {
		ctx->higher_watermark = ctx->lower_watermark >> 4;
	}
	*/
	printk(KERN_ERR "\n Initializing gc_extents list, ctx->gc_extents_cache: %p ", ctx->gc_extents_cache);
	ctx->gc_extents = kmem_cache_alloc(ctx->gc_extents_cache, GFP_KERNEL);
	if (!ctx->gc_extents) {
		printk(KERN_ERR "\n Could not allocate gc_extent and hence could not initialized \n");
		goto free_metadata_pages;
	}
	gcextent_init(ctx->gc_extents, 0, 0 , 0);
	//trace_printk("\n Extent allocated....! ctx->gc_extents: %p", ctx->gc_extents);
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
	debugfs_create_u32("freezones", 0444, debug_dir, &ctx->ckpt->nr_free_zones);
	printk(KERN_ERR "\n ctr() done!!");
	return 0;
/* failed case */
stop_gc_thread:
	lsdm_flush_thread_stop(ctx);
	lsdm_gc_thread_stop(ctx);
destroy_gc_page_pool:
	if (ctx->gc_page_pool)
		mempool_destroy(ctx->gc_page_pool);
free_metadata_pages:
	printk(KERN_ERR "\n freeing metadata pages!");
	if (ctx->revmap_bm) {
		__free_pages(ctx->revmap_bm, 0);
		nrpages--;
	}
	if (ctx->sb_page) {
		__free_pages(ctx->sb_page, 0);
		nrpages--;
	}
	if (ctx->ckpt_page) {
		__free_pages(ctx->ckpt_page, 0);
		nrpages--;
	}
	/* TODO : free extent page
	 * and segentries page */
destroy_cache:
	destroy_caches(ctx);
uninit_bioset:
	bioset_exit(ctx->gc_bs);
free_bioset:
	kfree(ctx->gc_bs);
put_dev:
	dm_put_device(target, ctx->dev);
free_ctx:
	kfree(ctx);
	printk(KERN_ERR "\n %s nrpages: %lu", __func__, nrpages);
	return ret;
}

/* For individual device removal */
static void lsdm_dtr(struct dm_target *dm_target)
{
	struct ctx *ctx = dm_target->private;

	lsdm_flush_thread_stop(ctx);
	lsdm_gc_thread_stop(ctx);
	flush_workqueue(ctx->writes_wq);
	flush_workqueue(ctx->tm_wq);
	printk(KERN_ERR "\n nr_app_writes: %llu", ctx->nr_app_writes);
	printk(KERN_ERR "\n nr_gc_writes: %llu", ctx->nr_gc_writes);
	/* we stop the gc thread first as it will create additional
	 * tm entries, sit entries and we want all of them to be freed
	 * and flushed as well.
	 */
	sync_blockdev(ctx->dev->bdev);
	/* flush the last partial revmap page if any */
	complete_revmap_blk_flush(ctx, ctx->revmap_page);
	printk(KERN_ERR "\n %s flush_revmap_entries done!", __func__);
	//clear_revmap_bit(ctx, revmap_bio_ctx->revmap_pba);
	/* Wait for the ALL the translation pages to be flushed to the
	 * disk. The removal work is queued.
	 */
	printk(KERN_ERR "\n translation blocks flushed! ");
	do_checkpoint(ctx);
	void * ptr = page_address(ctx->revmap_bm);
	memset(ptr, 0, 4096);
	flush_revmap_bitmap(ctx);
	//trace_printk("\n checkpoint done!");
	/* If we are here, then there was no crash while writing out
	 * the disk metadata
	 */
	free_translation_pages(ctx);	
	free_sit_pages(ctx);
	destroy_workqueue(ctx->writes_wq);
	lsdm_free_rb_tree(ctx);
	remove_gc_nodes(ctx);
	kmem_cache_free(ctx->gc_extents_cache, ctx->gc_extents);
	printk(KERN_ERR "\n RB mappings freed! ");
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
	//printk(KERN_ERR "\n %s nrpages: %lu", __func__, nrpages);

	//trace_printk("\n metadata pages freed! \n");
	/* timer based gc invocation for later
	 * del_timer_sync(&ctx->timer_list);
	 */
	//trace_printk("\n caches destroyed! \n");
	bioset_exit(ctx->gc_bs);
	//trace_printk("\n exited from bioset \n");
	kfree(ctx->gc_bs);
	//trace_printk("\n bioset memory freed \n");
	mempool_destroy(ctx->gc_page_pool);
	//trace_printk("\n memory pool destroyed\n");
	//trace_printk("\n device mapper target released\n");
	printk(KERN_ERR "\n nr_app_writes: %llu", ctx->nr_app_writes);
	printk(KERN_ERR "\n nr_gc_writes: %llu", ctx->nr_gc_writes);
	printk(KERN_ERR "\n nr_failed_writes: %u", atomic_read(&ctx->nr_failed_writes));
	printk(KERN_ERR "\n gc cleaning time average: %llu", ctx->gc_average);
	printk(KERN_ERR "\n gc total nr of segments cleaned: %d", ctx->gc_count);
	printk(KERN_ERR "\n gc total time spent cleaning: %llu", ctx->gc_total);
	destroy_caches(ctx);
	dm_put_device(dm_target, ctx->dev);
	kfree(ctx);
	//trace_printk("\n ctx memory freed!\n");
	printk(KERN_ERR "\n Goodbye World!\n");
	return;
}


int lsdm_map_io(struct dm_target *dm_target, struct bio *bio)
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
			bio_endio(bio);
			break;
	}
	
	return (ret? ret: DM_MAPIO_SUBMITTED);
}

/*
 * Setup target request queue limits.
 */
static void lsdm_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
        struct ctx *ctx = ti->private;
	struct lsdm_sb *sb = ctx->sb;
        unsigned int nrblks = 1 << (sb->log_zone_size - sb->log_block_size);

        limits->logical_block_size = BLOCK_SIZE;
        limits->physical_block_size = BLOCK_SIZE;

        blk_limits_io_min(limits, BLOCK_SIZE);
        blk_limits_io_opt(limits, BLOCK_SIZE);

        limits->discard_alignment = BLOCK_SIZE;
        limits->discard_granularity = BLOCK_SIZE;
        limits->max_discard_sectors = nrblks;
        limits->max_hw_discard_sectors = nrblks;
        limits->max_write_zeroes_sectors = nrblks;

        /* FS hint to try to align to the device zone size */
        limits->chunk_sectors = nrblks;
        limits->max_sectors = nrblks;

	/* expose the device as a regular non zoned device */
        //limits->zoned = BLK_ZONED_NONE;
}



static struct target_type lsdm_target = {
	.name            = "lsdm",
	.version         = {1, 0, 0},
	.module          = THIS_MODULE,
	.ctr             = lsdm_ctr,
	.dtr             = lsdm_dtr,
	.map             = lsdm_map_io,
	.status          = 0 /*lsdm_status*/,
	.prepare_ioctl   = 0 /*lsdm_prepare_ioctl*/,
	.message         = 0 /*lsdm_message*/,
	.iterate_devices = 0 /*lsdm_iterate_devices*/,
	.io_hints	 = lsdm_io_hints,
};

/* Called on module entry (insmod) */
int __init ls_dm_init(void)
{
	debug_dir = debugfs_create_dir("lsdm", NULL);
	if (!debug_dir) {
		printk(KERN_ERR "\n Could not create directory in debugfs ");
		return -1;
	}
	
	return dm_register_target(&lsdm_target);
}

/* Called on module exit (rmmod) */
void __exit ls_dm_exit(void)
{
	debugfs_remove_recursive(debug_dir);
	dm_unregister_target(&lsdm_target);
}


module_init(ls_dm_init);
module_exit(ls_dm_exit);

MODULE_DESCRIPTION(DM_NAME " Log Structured SMR Translation Layer");
MODULE_AUTHOR("Surbhi Palande <csurbhi@gmail.com>");
MODULE_LICENSE("GPL");
