#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#define NR_BLKS_IN_ZONE 65536
#define BLKSZ 4096

char * fname = "/dev/sdb";

int main(int argc, char *argv[])
{
	char buff[BLKSZ], newch='7', origch='5';
	int fd, i, j, k, ret;
	unsigned int offset = 0, val;
	int nrzones = 0;

	if (argc < 2) {
		printf("\n Usage: %s <nrzones> \n", argv[0]);
		return -1;
	}

	nrzones = strtol(argv[1], NULL, 10);
	printf("\n %s nrzones: %d ", __func__, nrzones);

	printf("\n Read verifying the writes ......\n");
	fd = open(fname, O_RDWR);
	if (fd < 0) {
		perror("\n Could not open file because: ");
		printf("\n");
		return errno;
	}

	lseek(fd, 0, SEEK_SET);
	offset = 0;
	for(i=0; i<nrzones; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j=j+1) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n b) Could not read to file because: ");
				printf("\n");
				return errno;
			}
		}
	}
	printf("\n");
	close(fd);
	return 0;
}
