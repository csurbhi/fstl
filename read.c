#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define NR_ZONES 2
#define NR_BLKS_IN_ZONE 65536
#define BLKSZ 4096

int main(int argc, char *argv[])
{
	char buff[BLKSZ];
	int fd, i, j, k, val, ret;

	fd = open("test", O_RDWR);
	if (fd < 0) {
		perror("\n Could not create file because: ");
		printf("\n");
		return errno;
	}


	for(i=0; i<NR_ZONES; i++) {
		for(j=0; j<NR_BLKS_IN_ZONE; j++) {
			ret = read(fd, buff, BLKSZ);
			if (ret < 0) {
				perror("\n Could not write to file because: ");
				printf("\n");
				return errno;
			}
			if (j % 2 == 0)
				val = 2;
			else
				val = 1;
			for(k=0; k<BLKSZ; k++) {
				if (buff[k] != val) {
					perror("\n Read error! ");
					printf("\n");
					return errno;
				}
			}
		}
	}

	return 0;
}
