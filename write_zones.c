#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define NR_ZONES 1
#define NR_BLKS_IN_ZONE 65536
#define BLKSZ 4096

int main(int argc, char *argv[])
{
	char buff[BLKSZ];
	int fd, i, j, k, ret;
	unsigned int offset = 0;

	for(i=0; i<BLKSZ; i++) {
		buff[i] = 1;
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
				if (buff[k] != 1) {
					printf("\n 1) write could not be verified, content is not 1 ");
					printf("\n zone_nr: %d, blknr: %d k: %d buff[k]: %d \n", i, j, k, buff[k]);
					break;
				}
			}
		}
	}

	printf("\n Writes verified!! ");



	close(fd);

	sync();

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
