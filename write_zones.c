#include <stdio.h>
#define _LARGEFILE64_SOURCE
#define __USE_FILE_OFFSET64 
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#define __USE_FILE_OFFSET64 
#include <fcntl.h>
#include <linux/blkzoned.h>
#include <assert.h>
#include <stdlib.h>
#include <sys/ioctl.h>


#define NR_ZONES 1
#define NR_BLKS_IN_ZONE 65536
#define BLKSZ 4096


int report_zone(unsigned long zonenr)
{
	struct blk_zone_report * bzr;
	int ret;
	long i = 0, fd = 0;

	fd = open("/dev/sdb", O_RDWR);
	if (!fd) {
		perror("Could not open the disk: ");
		return;
	}
	printf("\n %s opened %s with fd: %ld ", __func__, "/dev/sdb", fd);

	bzr = malloc(sizeof(struct blk_zone_report) + sizeof(struct blk_zone) * 256);

	bzr->sector = 244842496 + (zonenr * 65536 * 8);
	bzr->nr_zones = 1;

	ret = ioctl(fd, BLKREPORTZONE, bzr);
	if (ret) {
		fprintf(stderr, "\n blkreportzone for zonenr: %ld ioctl failed, ret: %d ", zonenr, ret);
		perror("\n blkreportzone failed because: ");
		return;
	}
	assert(bzr->nr_zones <= 256);
	for (i=0; i<bzr->nr_zones; i++) {
		printf("\n-----------------------------------");
		printf("\n Zonenr: %ld ", i);
		printf("\n start: %lld ", bzr->zones[i].start);
		printf("\n len: %lld ", bzr->zones[i].len);
		printf("\n state: %d ", bzr->zones[i].cond);
		printf("\n reset recommendation: %d ", bzr->zones[i].reset);
		printf("\n wp: %llu ", bzr->zones[i].wp);
		printf("\n non_seq: %d ", bzr->zones[i].non_seq);
		printf("\n-----------------------------------\n");
	}
	close(fd);
	return (bzr->zones[0].wp == bzr->zones[0].start);
}


int verify_read(int zonenr, int blknr, char ch)
{
	off_t offset;
	char buff[BLKSZ];
	int i, ret, fd;

	fd = open("/dev/sdb", O_RDWR);
	if (fd < 0) {
		perror("\n Could not open file because: ");
		printf("\n");
		return errno;
	}

	/*sb->zone0_pba: 244842496 */
	offset = (zonenr * 65536 * 8 * 512) + (blknr * BLKSZ) + (244842496 * 512) ;
	ret = lseek64(fd, offset, SEEK_SET);
	if (ret < 0) {
		perror("\n Could not lseek because: ");
		return ret;
	}
	ret = read(fd, buff, BLKSZ);
	if (ret < 0) {
		perror("\n Could not read because: ");
		return ret;
	}
	for(i=0; i<BLKSZ; i++) {
		if (buff[i] != ch) {
			printf("\n (raw-sdb) write could not be verified, lba: %llu content is not %c ", (zonenr*NR_BLKS_IN_ZONE * 8) + (blknr * 8), ch);
			printf("\n buff[i]: %c", buff[i]);
			return -1;
		}

	}




}

int main(int argc, char *argv[])
{
	char buff[BLKSZ], newbuff[BLKSZ];
	int fd, i, j, k, ret;
	off_t offset = 0;
	char newch = '6', origch = '2';
	int count = 0;


	printf("\n character written is: %c ", origch);
	for(i=0; i<BLKSZ; i++) {
		buff[i] = origch;
	}

	fd = open("/dev/dm-0", O_RDWR);
	//fd = open("/dev/sdb", O_RDWR);
	if (fd < 0) {
		perror("\n Could not create file because: ");
		printf("\n");
		return errno;
	}



	printf("\n Conducting write verification ....");
	//offset = (244842496 * 512);

	lseek(fd, 0, SEEK_SET);
	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
retry:
			ret = write(fd, buff, BLKSZ);
			if (ret < 0) {
				fprintf(stdout, "\n Could not write, zonenr: %d, blknr: %d", i, j);
				if (report_zone(i)) {
					/* retry the write */
					count = count + 1;
					if (count < 2)
						goto retry;
				}
				printf("\n");
				perror("\n Could not write to file because: ");
				printf("\n");
				break;
			}
			if (ret < BLKSZ) {
				printf("\n Partial write, zonenr: %d blknr: %d", i, j);
			}
		}
	}

	close(fd);
	sync();
	printf("\n Writes done!! \n");

/*
	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not open file because: ");
		printf("\n");
		return errno;
	}

	lseek(fd, 0, SEEK_SET);
	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not read from file because: ");
				printf("\n");
				return errno;
			}
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != origch) {
					printf("\n 1) write could not be verified, lba: %llu content is not %c ", (i*NR_BLKS_IN_ZONE * 8) + (j * 8), origch);
					verify_read(i, j, origch);
					printf("\n zone_nr: %d, blknr: %d k: %d buff[k]: %c \n", i, j, k, buff[k]);
					return -1;
				}
			} 
		}
	}

	printf("\n Writes verified!! \n"); 


	close(fd);
	sync();
*/
	printf("\n Conducting overwrites verification! .......");
	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not open file because: ");
		printf("\n");
		return errno;
	}
	lseek(fd, 0, SEEK_SET);

	for(i=0; i<BLKSZ; i++) {
		newbuff[i] = newch;
	}

	offset = 0;
	lseek(fd, offset, SEEK_SET);
	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j=j+2) {
			ret = write(fd, newbuff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not write to file because: ");
				printf("\n");
				return errno;
			}
			if (ret < BLKSZ) {
				printf("\n Partial write, zonenr: %d blknr: %d", i, j);
				return(-1);
			}
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not read to file because: ");
				printf("\n");
				return errno;
			}
		
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != origch) {
					printf("\n Expected val: %c and found: %c for zonenr: %d  blknr: %d, k: %d", origch, buff[k], i, j, k);
					printf("\n");
					break;
				}
			}
		}
	}

	close(fd);
	printf("\n Overwrites done ! \n");
	sync();
	
	printf("\n Read verifying the writes ......\n");
	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not open file because: ");
		printf("\n");
		return errno;
	}


	lseek(fd, 0, SEEK_SET);

	offset = 0;
	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j=j+2) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n b) Could not read to file because: ");
				printf("\n");
				return errno;
			}
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != newch) {
					printf("\n b) Expected val: %c and found: %c for zonenr: %d  blknr: %d, k: %d", newch, buff[k], i, j, k);
					printf("\n");
					break;
				}
			}
			 
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not read to file because: ");
				printf("\n");
				return errno;
			}
		
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != origch) {
					printf("\n Expected val: %c and found: %c for zonenr: %d  blknr: %d, k: %d", origch, buff[k], i, j, k);
					printf("\n");
					break;
				}
			}
		}
	}
	printf("\n");
	close(fd);
	return 0;
}
