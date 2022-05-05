#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define NR_ZONES 10
#define NR_BLKS_IN_ZONE 65536
#define BLKSZ 4096


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
	char buff[BLKSZ];
	int fd, i, j, k, ret;
	unsigned int offset = 0;
	char ch = '6';

	for(i=0; i<BLKSZ; i++) {
		buff[i] = ch;
	}

	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not create file because: ");
		printf("\n");
		return errno;
	}



	printf("\n Conducting write verification ....");

	lseek(fd, 0, SEEK_SET);
	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
			ret = write(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not write to file because: ");
				printf("\n");
				//report_zone(fd, i);
				return errno;
			}
			if (ret < BLKSZ) {
				printf("\n Partial write, zonenr: %d blknr: %d", i, j);
			}
		}
	}

	close(fd);

	sync();

	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not create file because: ");
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
				if (buff[k] != ch) {
					printf("\n 1) write could not be verified, content is not %c", ch);
					printf("\n zone_nr: %d, blknr: %d k: %d buff[k]: %d \n", i, j, k, buff[k]);
					verify_read(i, j, ch);
					break;
				}
			}
		}
	}

	printf("\n Writes verified!! ");
	close(fd);
	sync();
	return 0;

	printf("\n Conducting overwrites verification! .......");

	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not create file because: ");
		printf("\n");
		return errno;
	}


	lseek(fd, 0, SEEK_SET);


	for(i=0; i<BLKSZ; i++) {
		buff[i] = 2;
	}
	

	offset = 0;
	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j=j+2) {
			lseek(fd, offset, SEEK_SET);
			ret = write(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not write to file because: ");
				printf("\n");
				return errno;
			}
			if (ret < BLKSZ) {
				printf("\n Partial write, zonenr: %d blknr: %d", i, j);
				return(-1);
			}
			offset = offset + 8192;
		}
	}

	close(fd);
	printf("\n Overwrites done ! ");

	printf("\n Read verifying the writes ......\n");
	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not create file because: ");
		printf("\n");
		return errno;
	}


	lseek(fd, 0, SEEK_SET);

	offset = 0;
	for(i=0; i<NR_ZONES; i++) {
		for(j=2; j<NR_BLKS_IN_ZONE; j=j+2) {
			lseek(fd, offset, SEEK_SET);
			offset = offset + 8192;
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n b) Could not read to file because: ");
				printf("\n");
				return errno;
			}
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != 2) {
					printf("\n Expected val: 2 and found: %d for zonenr: %d  blknr: %d, k: %d", buff[k], i, j, k);
					printf("\n");
					return -1;
				}
			}
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not read to file because: ");
				printf("\n");
				return errno;
			}
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != 1) {
					printf("\n b) Expected val: 1 and found: %d for zonenr: %d  blknr: %d, k: %d", buff[k], i, j, k);
					printf("\n");
					return -1;
				}
			}

		}
	}
	printf("\n");
	close(fd);
	return 0;
}
