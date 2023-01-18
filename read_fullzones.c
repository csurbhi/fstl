#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#define NR_BLKS_IN_ZONE 65536
#define BLKSZ 4096

//char * fname = "/mnt/test";
char * fname;

int main(int argc, char *argv[])
{
	char buff[BLKSZ], newch='6', origch='2';
	int fd, i, j, k, ret;
	unsigned int offset = 0, val;
	int nrzones;

	if (argc < 3) {
		printf("\n Usage: %s <fname> <nrzones> \n", argv[0]);
		exit(-1);
	}

	fname = argv[1];
	nrzones = strtol(argv[2], NULL, 10);

	printf("\n Read verifying the writes in file: %s nrzones: %d ......\n", fname, nrzones);
	fd = open(fname, O_RDWR);
	if (fd < 0) {
		perror("\n Could not open file because: ");
		printf("\n");
		return errno;
	}
	offset = (65536 * 4096);
	lseek(fd, offset, SEEK_SET);
	for(i=0; i<63; i++) {
		lseek(fd, offset, SEEK_CUR);
	}

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
