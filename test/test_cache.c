#include <linux/module.h>    // included for all kernel modules
#include <linux/device-mapper.h>
#include <linux/bio.h>

#include <linux/kernel.h>    // included for KERN_INFO
#include <linux/init.h>      // included for __init and __exit macros
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/list_sort.h>
#include <linux/blkdev.h>
#include <linux/blk_types.h>

struct ctx {
	struct dm_dev    *dev;
};

static int lsdm_read_io(struct ctx *ctx, struct bio *bio)
{
	return 0;
}

int lsdm_write_io(struct ctx *ctx, struct bio *bio)
{
	return 0;
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

static int lsdm_ctr(struct dm_target *target, unsigned int argc, char **argv)
{
	struct ctx *ctx;
	int ret;
	struct request_queue *q;
	loff_t disk_size;

	ctx = kzalloc(sizeof(struct ctx), GFP_KERNEL);
        if (!ctx) {
                target->error = "dm-lsdm: Memory error";
                return -ENOMEM;
        }

        target->private = ctx;
        /* 13 comes from 9 + 3, where 2^9 is the number of bytes in a sector
         * and 2^3 is the number of sectors in a block.
         */
        target->max_io_len = 128;
        target->flush_supported = true;
        target->discards_supported = true;
        /* target->per_io_data_size - set this to get per_io_data_size allocated before every standard structure that holds a bio. */

        ret = dm_get_device(target, argv[0], dm_table_get_mode(target->table), &ctx->dev);
        if (ret) {
                target->error = "lsdm: Device lookup failed.";
                goto free_ctx;
        }

        if (bdev_zoned_model(ctx->dev->bdev) == BLK_ZONED_NONE) {
                target->error = "Not a zoned block device";
                ret = -EINVAL;
                goto free_ctx;
        }
        ret = blkdev_report_zones(ctx->dev->bdev, 1572864, 1, NULL, NULL);
        if (!ret) {
                printk(KERN_ERR "\n reporting zones failed! \n");
        }

        q = bdev_get_queue(ctx->dev->bdev);
        printk(KERN_ERR "\n number of sectors in a zone: %llu", bdev_zone_sectors(ctx->dev->bdev));
        printk(KERN_ERR "\n number of zones in device: %u", bdev_nr_zones(ctx->dev->bdev));
        printk(KERN_ERR "\n block size in sectors: %u ", q->limits.logical_block_size);

        printk(KERN_INFO "\n sc->dev->bdev->bd_part->start_sect: %llu", ctx->dev->bdev->bd_start_sect);
        //printk(KERN_INFO "\n sc->dev->bdev->bd_part->nr_sects: %llu", ctx->dev->bdev->bd_part->nr_sects);

        disk_size = i_size_read(ctx->dev->bdev->bd_inode);
        printk(KERN_INFO "\n The disk size as read from the bd_inode: %llu", disk_size);
        printk(KERN_INFO "\n max sectors: %llu", disk_size/512);
        printk(KERN_INFO "\n max blks: %llu", disk_size/4096);

        printk(KERN_ERR "\n --------------------------------- \n");
	return 0;
free_ctx:
	kfree(ctx);
	return -1;

}

static void lsdm_dtr(struct dm_target *dm_target)
{
	struct ctx *ctx = dm_target->private;
	dm_put_device(dm_target, ctx->dev);
	kfree(ctx);
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
};

/* Called on module entry (insmod) */
int __init ls_dm_init(void)
{
	return dm_register_target(&lsdm_target);
}

/* Called on module exit (rmmod) */
void __exit ls_dm_exit(void)
{
	dm_unregister_target(&lsdm_target);
}


module_init(ls_dm_init);
module_exit(ls_dm_exit);

MODULE_DESCRIPTION(DM_NAME "log structured SMR Translation Layer");
MODULE_AUTHOR("Surbhi Palande <csurbhi@gmail.com>");
MODULE_LICENSE("GPL");
