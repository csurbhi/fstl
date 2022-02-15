#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define NR_ZONES 1
#define NR_BLKS_IN_ZONE 1000
#define BLKSZ 4096

int main(int argc, char *argv[])
{
	char buff[BLKSZ];
	int fd, i, j, k, ret;
	unsigned int offset = 0, val;

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
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n b) Could not read to file because: ");
				printf("\n");
				return errno;
			}
			if (j % 2 == 0) 
				val = 2;
			else 
				val = 1;
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != val) {
					printf("\n Expected val: %d and found: %d for zonenr: %d  blknr: %d, k: %d", val, buff[k], i, j, k);
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
