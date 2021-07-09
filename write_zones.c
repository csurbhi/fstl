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
	int fd, i, j, k, val, ret;
	unsigned int offset = 0;

	fd = open("/dev/dm-0", O_RDWR);
	if (fd < 0) {
		perror("\n Could not create file because: ");
		printf("\n");
		return errno;
	}


	for(i=0; i<BLKSZ; i++) {
		buff[i] = 1;
	}

	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
			ret = write(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not write to file because: ");
				printf("\n");
				return errno;
			}
		}
	}

	lseek(fd, 0, SEEK_SET);
	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not write to file because: ");
				printf("\n");
				return errno;
			}
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != 1) {
					printf("\n Read error! 1");
					printf("\n k: %d buff[k]: %d", k, buff[k]);
					return errno;
				}
			}
		}
	}

	printf("\n Written two zones of blks, and read them back! About to overwrite....");
	close(fd);

	sync();

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
	

	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j=j+2) {
			offset = 4096 * j;
			lseek(fd, offset, SEEK_SET);
			ret = write(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not write to file because: ");
				printf("\n");
				return errno;
			}
			//printf("\n written 2 at j: %d offset: %d", j, offset);
		}
	}

	lseek(fd, 0, SEEK_SET);

	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not read to file because: ");
				printf("\n");
				return errno;
			}
			if (j % 2 == 0)
				val = 2;
			else
				val = 1;
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != val) {
					printf("\n Expected val: %d and found: %d for j: %d, k: %d", val, buff[k], j, k);
					printf("\n");
					continue;
				}
			}
		}
	}
	printf("\n");
	return 0;
}
