#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#define NR_BLKS_IN_ZONE 65536
#define BLKSZ 4096

char * fname = "/mnt/test";

int main(int argc, char *argv[])
{
	char buff[BLKSZ], newch='6', origch='2';
	int fd, i, j, k, ret;
	unsigned int offset = 0, val;
	int nrzones;

	if (argc < 2) {
		printf("\n Usage: %s <nrzones> \n");
		return -1;
	}

	nrzones = 128;

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
		//for(j=0; j<NR_BLKS_IN_ZONE; j=j+2) {
		for(j=0; j<NR_BLKS_IN_ZONE; j=j+1) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n b) Could not read to file because: ");
				printf("\n");
				return errno;
			}
			/*
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != newch) {
					printf("\n b) Expected val: %c and found: %c for zonenr: %d  blknr: %d, k: %d", newch, buff[k], i, j, k);
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
				if (buff[k] != origch) {
					printf("\n Expected val: %c and found: %c for zonenr: %d  blknr: %d, k: %d", origch, buff[k], i, j+1, k);
					printf("\n");
					return -1;
				}
			}
			*/
		}
	}
	printf("\n");
	close(fd);
	return 0;
}
