#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <linux/blkzoned.h>
#include <linux/types.h>
#include <linux/fs.h>


__le32 get_zone_count(int fd)
{
	__le32 zone_count = 0, zonesz;
	char str[400];
    	__u64 capacity = 0;

	/* get the capacity in bytes */
	if (ioctl(fd, BLKGETSIZE64, &capacity) < 0) {
		sprintf(str, "Get capacity failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		exit(errno);
	}
	printf("\n capacity: %llu", capacity);

	if (ioctl(fd, BLKGETZONESZ, &zonesz) < 0) {
		sprintf(str, "Get zone size failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		exit(errno);
	}
	printf("\n ZONE size: %d", zonesz);
	/* zonesz is number of addressable lbas */
	zonesz = zonesz << 9;

	if (ioctl(fd, BLKGETNRZONES, &zone_count) < 0) {
		sprintf(str, "Get nr of zones ioctl failed %d (%s)\n", errno, strerror(errno));
		perror(str);
		exit(errno);
	}

	printf("\n Nr of zones reported by disk :%u", zone_count);
	/* Use zone queries and find this eventually
	 * Doing this manually for now for a 20GB 
	 * harddisk and 256MB zone size.
	 */
	if (zone_count >= (capacity/zonesz)) {
		printf("\n Number of zones: %d ", zone_count);
		printf("\n capacity/ZONE_SZ: %llu ", capacity/zonesz);
		return capacity/zonesz;
	}
	printf("\n Actual zone count calculated: %llu ", (capacity/zonesz));
	return zone_count;
	//return 16500;
	//return 28200;
	//return 4500;
	//return 9000;
	//return 10000;
	// 2048 GB
	//return 8192;
	//return 29807;
	//return (capacity/zonesz);
	//return 100;
	//return 1900;
	/* we test with a disk capacity of 1 TB */
	//return 4032;
	//return 500;
	//return 256;
	//return 100;
	/* 
	 * resilvering test1 - returned this
	 * return 3000;
	 *
	 * 2000 zones is 500GB. 450GB is 90% of 500GB.
	 * We are using this to perform the test.
	 *
	 * Next we will perform the test with 1900 zones
	 * ie 475 GB
	 *
	 */
	//return 2500;
	//return 29808;
	//return 209;
}


void report_zone(unsigned int fd, unsigned long zonenr, struct blk_zone * bzone)
{
	int ret;
	long i = 0;
	struct blk_zone_report *bzr;
	unsigned long long wp, start;
	unsigned int nrblks;


	printf("\n-----------------------------------");
	printf("\n %s Zonenr: %ld ", __func__, zonenr);
	bzr = malloc(sizeof(struct blk_zone_report) + sizeof(struct blk_zone));
	bzr->sector = bzone->start;
	bzr->nr_zones = 1;

	ret = ioctl(fd, BLKREPORTZONE, bzr);
	if (ret) {
		fprintf(stderr, "\n blkreportzone for zonenr: %lu ioctl failed, ret: %d ", zonenr, ret);
		perror("\n blkreportzone failed because: ");
		printf("\n");
		return;
	}
	assert(bzr->nr_zones == 1);
	start = bzr->zones[0].start;
	wp = bzr->zones[0].wp;
	nrblks = (wp - start)/8;
	if (nrblks > 0)
		printf("\n zonenr: %lu, valid blks: %d ", zonenr, nrblks);
	return;	
}

long report_shingled_zones(int fd)
{
	int ret;
	long i = 0;
	long zone_count = 0;
	struct blk_zone_report * bzr;
	struct blk_zone_range bz_range;
	long cmr = 0;


	zone_count = get_zone_count(fd);

	printf("\n Nr of zones: %ld ", zone_count);

	bzr = malloc(sizeof(struct blk_zone_report) + sizeof(struct blk_zone) * zone_count);

	bzr->sector = 0;
	bzr->nr_zones = zone_count;

	ret = ioctl(fd, BLKREPORTZONE, bzr);
	if (ret) {
		fprintf(stdout, "\n blkreportzone for zonenr: %d ioctl failed, ret: %d ", 0, ret);
		perror("\n blkreportzone failed because: ");
		printf("\n");
		return -1;
	}

	printf("\n nr of zones reported: %d", bzr->nr_zones);
	assert(bzr->nr_zones == zone_count);

	if (zone_count < 1024) {
		bzr->nr_zones = zone_count;
	} else {
		bzr->nr_zones = 1024;
	}

	printf("\n zone_count: %d ", bzr->nr_zones);
	for (i=0; i<bzr->nr_zones; i++) {
		if ((bzr->zones[i].type == BLK_ZONE_TYPE_SEQWRITE_PREF)  || 
		   (bzr->zones[i].type == BLK_ZONE_TYPE_SEQWRITE_REQ)) {
			bz_range.sector = bzr->zones[i].start;
			bz_range.nr_sectors = bzr->zones[i].len;
			report_zone(fd, i, &bzr->zones[i]);
		} else {
			printf("\n zonenr: %ld is a non shingled zone! ", i);
			cmr++;
		}
	}
	return cmr;
}


long reset_shingled_zones(int fd)
{
	int ret;
	long i = 0;
	long zone_count = 0;
	struct blk_zone_report * bzr;
	struct blk_zone_range bz_range;
	long cmr = 0;


	zone_count = get_zone_count(fd);

	printf("\n Nr of zones: %ld ", zone_count);

	bzr = malloc(sizeof(struct blk_zone_report) + sizeof(struct blk_zone) * zone_count);

	bzr->sector = 0;
	bzr->nr_zones = zone_count;

	ret = ioctl(fd, BLKREPORTZONE, bzr);
	if (ret) {
		fprintf(stdout, "\n blkreportzone for zonenr: %d ioctl failed, ret: %d ", 0, ret);
		perror("\n blkreportzone failed because: ");
		printf("\n");
		return -1;
	}

	printf("\n nr of zones reported: %d", bzr->nr_zones);
	assert(bzr->nr_zones == zone_count);

	if (zone_count < 1024) {
		bzr->nr_zones = zone_count;
	} else {
		bzr->nr_zones = 1024;
	}

	printf("\n zone_count: %d ", bzr->nr_zones);
	for (i=0; i<bzr->nr_zones; i++) {
		if ((bzr->zones[i].type == BLK_ZONE_TYPE_SEQWRITE_PREF)  || 
		   (bzr->zones[i].type == BLK_ZONE_TYPE_SEQWRITE_REQ)) {
			bz_range.sector = bzr->zones[i].start;
			bz_range.nr_sectors = bzr->zones[i].len;
			ret = ioctl(fd, BLKRESETZONE, &bz_range); 
			if (ret) {
				fprintf(stdout, "\n Could not reset zonenr with sector: %lld", bz_range.sector);
				perror("\n blkresetzone failed because: ");
				printf("\n");
			}
			report_zone(fd, i, &bzr->zones[i]);
		} else {
			printf("\n zonenr: %ld is a non shingled zone! ", i);
			cmr++;
		}
	}
	return cmr;
}

int open_disk(char *dname)
{
        int fd;

        printf("\n Opening %s ", dname);

        fd = open(dname, O_RDWR);
        if (fd < 0) {
                perror("Could not open the disk: ");
                exit(errno);
        }
        printf("\n %s opened with fd: %d ", dname, fd);
        return fd;
}

int main(int argc, char * argv[])
{
	int nr_blks = 0, i, j, ret, fd, blknr = 0;
	char buff[4096];
	char *blks;
	int nr_zones, free_blks;
	char *blkdev;
	unsigned long long offset = 0;

	printf("\n %s argc: %d \n ", __func__, argc);
        if (argc != 4) {
                fprintf(stderr, "\n Usage: %s device-name seq-data-zones(# zones filled with seq data) freeblks (#blks in every zone freed)  \n", argv[0]);
                exit(EXIT_FAILURE);
        }
        blkdev = argv[1];
        /* size in GB */
        nr_zones = atoi(argv[2]);
        if (nr_zones == 0) {
                printf("\n");
                return -1;
        }
        if (nr_zones > 29808) {
                printf("\n Device has 29808 zones, cannot populate more!");
                printf("\n");
                return -1;
        }
        free_blks = atoi(argv[3]);
	if (free_blks > 65535) {
		printf("\n Atleast one block needs to be populated, freeblks < 65536");
                printf("\n");
		return -1;
	}

	fd = open_disk("/dev/urandom");
	ret = read(fd, buff, 4096);
	if (ret < 0) {
		perror("Read error: ");
		printf("\n");
		exit(EXIT_FAILURE);
	}
	close(fd);

	nr_blks = 65536 - free_blks;

	fd = open_disk(blkdev);
	printf("\n nr_blks: %d, free_blks: %d ", nr_blks, free_blks);

	for(j=0; j<nr_zones; j++) {
		offset = 0;
		blknr = 0;
		for(i=0; i<nr_blks; i++) {
			ret = write(fd, buff, 4096);
			if (ret < 0) {
				perror("!! write Error: \n");
				printf("\n write to disk zonenr: %u, blknr: %d ret: %d", j, i, ret);
				exit(errno);
			}
			offset += 4096;
			blknr++;
		}
		for(i=0; i<free_blks; i++) {
			ret = lseek(fd, 4096, SEEK_CUR);
			if (ret == -1) {
				perror("!! lseek Error: \n");
				printf("\n lseek to disk zonenr: %u, blknr: %d ret: %d", j, i+nr_blks, ret);
			}
			offset += 4096;
			blknr++;
		}
		//printf("\n blknr: %d \n", blknr);
		assert(blknr == 65536);
		//assert((offset % (65536 * 4096)) == 0);
	}
	report_shingled_zones(fd);
	printf("\n Success ! ");
	printf("\n");
	close(fd);
	return 0;
}
