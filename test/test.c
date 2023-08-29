#include <linux/module.h>    // included for all kernel modules
#include <linux/kernel.h>    // included for KERN_INFO
#include <linux/init.h>      // included for __init and __exit macros
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/list_sort.h>


MODULE_LICENSE("GPL");
MODULE_AUTHOR("Surbhi");
MODULE_DESCRIPTION("test functions in lsdm");

enum {
	BG_GC,
	FG_GC,
};

#define GC_GREEDY 1 
#define GC_CB 2


struct ctx {
	struct rb_root    gc_zone_root;
	struct rb_root    gc_cost_root;
	struct kmem_cache *gc_cost_node_cache;
	struct kmem_cache *gc_zone_node_cache;

};

struct gc_cost_node {
        struct rb_node rb;      /* 20 bytes */
        unsigned int cost;      /* unique key! */
        struct list_head znodes_list; /* list of all the gc_zone_nodes that have the same cost */
};

struct gc_zone_node {
        struct rb_node rb;
        u32 zonenr;     /* unique key */
        u32 vblks;
        struct gc_cost_node *ptr_to_cost_node;
        struct list_head list; /* we add this node to the list maintained on gc_cost_node */
};

static int select_zone_to_clean(struct ctx *ctx, int mode, char *func)
{
        int zonenr = -1;
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


unsigned int get_cost(u32 nrblks, u64 age, char gc_mode)
{
	if (gc_mode == GC_GREEDY) {
		return nrblks;
	}
	return age;
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
	int offset = 0;
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
			printk(KERN_ERR "\n %s Deleting zone: %d from the cost node %p zone list! nrblks: %d \n", __func__, zonenr, cost_node, zone_node->vblks);
			zone_node->ptr_to_cost_node = NULL;
			list_del(&zone_node->list);
			zcount = zcount - 1;
			flag = 1;
		}
		if ((zcount > 0) && (flag)) {
			/* There are other nodes with this cost, so we dont want to remove the cost node then */
			break;
		}
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
int update_gc_tree(struct ctx *ctx, unsigned int zonenr, u32 nrblks, u64 mtime, char * caller)
{
	struct rb_root *root = &ctx->gc_cost_root;
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct gc_cost_node *cost_node = NULL, *new = NULL;
	struct gc_zone_node *znode = NULL;
	unsigned int cost = 0;

	if (nrblks == 0) {
		printk(KERN_ERR "\n %s Removing zone: %d from gc tree! \n", __func__, zonenr);
		remove_zone_from_gc_tree(ctx, zonenr);
		return 0;
	}
	cost = get_cost(nrblks, mtime, GC_GREEDY);
	znode = add_zonenr_gc_zone_tree(ctx, zonenr, nrblks);
	if (!znode) {
		printk(KERN_ERR "\n %s gc data structure allocation failed!! \n");
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
		//printk(KERN_ERR "\n Removing zone from old cost node. %s zonenr: %d vblks: %d old_cost: %d new_cost:%d ", __func__, zonenr, nrblks, new->cost, cost);
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
			printk(KERN_ERR "\n %s Adding zone to the costlist: zonenr: %d nrblks: %u, mtime: %llu caller: (%s) cost:%d \n", __func__, zonenr, nrblks, mtime, caller, cost, cost_node );
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
	printk(KERN_ERR "\n %s zone to clean: %d ", __func__, zonenr);
	return 0;
}

void print_cost_node(struct ctx *ctx, struct gc_cost_node *cost_node)
{
	struct list_head *list_head = NULL;
	struct gc_zone_node *zone_node = NULL, *next_node = NULL;

	list_head = &cost_node->znodes_list;
	list_for_each_entry_safe(zone_node, next_node, list_head, list) {
		printk(KERN_ERR "\t zone: %d, vblks: %d", zone_node->zonenr, zone_node->vblks);
		zone_node->ptr_to_cost_node = NULL;
	}

}

void print_gc_tree(struct ctx *ctx, struct rb_node *link)
{
	struct rb_root *root = &ctx->gc_cost_root;
	struct gc_cost_node *cost_node = NULL;

	if (!link)
		return;
	cost_node = container_of(link, struct gc_cost_node, rb);
	print_cost_node(ctx, cost_node);
	printk(KERN_ERR "\n");
	print_gc_tree(ctx, link->rb_left);
	print_gc_tree(ctx, link->rb_right);
	return;
}

int init_ctx(struct ctx *ctx)
{
	
	ctx->gc_cost_node_cache = kmem_cache_create("gc_cost_node_cache", sizeof(struct gc_cost_node), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
        if (!ctx->gc_cost_node_cache) {
		return -1;
        }
	ctx->gc_zone_node_cache = kmem_cache_create("gc_zone_node_cache", sizeof(struct gc_zone_node), 0, SLAB_RED_ZONE|SLAB_ACCOUNT, NULL);
        if (!ctx->gc_zone_node_cache) {
		kmem_cache_destroy(ctx->gc_cost_node_cache);
		return -1;
        }
	ctx->gc_cost_root = RB_ROOT;
        ctx->gc_zone_root = RB_ROOT;
	return 0;
}

struct ctx *ctx;
static int __init tm_init(void)
{
	ctx = kzalloc(sizeof(struct ctx), GFP_KERNEL);
        if (!ctx) {
                return -ENOMEM;
        }
	if (init_ctx(ctx))
		return -1;
	/* ctx, zonenr, nrblks, mtime, caller */
	update_gc_tree(ctx, 0, 10, 20, __func__);
	update_gc_tree(ctx, 0, 12, 20, __func__);
	update_gc_tree(ctx, 1, 12, 20, __func__);
	return 0;
}

void free_ctx(struct ctx *ctx)
{
	kmem_cache_destroy(ctx->gc_cost_node_cache);
	kmem_cache_destroy(ctx->gc_zone_node_cache);
}

static void __exit tm_exit(void)
{
	struct rb_node *link = ctx->gc_cost_root.rb_node;
	printk(KERN_ERR "\n");
	print_gc_tree(ctx, link);
	remove_zone_from_gc_tree(ctx, 0);
	remove_zone_from_gc_tree(ctx, 1);
	print_gc_tree(ctx, link);
	free_ctx(ctx);
	kfree(ctx);
	printk(KERN_ERR "\n Goodbye beautiful world!");
}

module_init(tm_init);
module_exit(tm_exit);
