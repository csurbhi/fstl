#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <search.h>

static void *root = NULL;
int fd;

struct item {
    off_t lba;
    int   seq;
};

static int compare(const void *pa, const void *pb)
{
    if (*(off_t *) pa < *(off_t *) pb)
        return -1;
    if (*(off_t *) pa > *(off_t *) pb)
        return 1;
    return 0;
}

char *page;

int failed;

static void check(struct item *p)
{
    lseek(fd, p->lba*512, SEEK_SET);
    if (read(fd, page, 4096) != 4096)
        perror("read check error"), exit(1);
    off_t lba;
    int seq;
    if (sscanf(page, "%ld %d", &lba, &seq) != 2 || lba != p->lba || seq != p->seq) {
        printf("ERROR: lba %ld %d: %02x %02x %02x %02x\n", p->lba, p->seq, page[0], page[1], page[2], page[3]);
        failed = 1;
    }
}

static void test_action(const void *nodep, const VISIT which, const int depth)
{
    switch (which) {
    case preorder:
        break;
    case postorder:
        check(*(struct item **) nodep);
        break;
    case endorder:
        break;
    case leaf:
        check(*(struct item **) nodep);
        break;
    }
}

static void print(struct item *p)
{
    printf("%ld %d\n", p->lba, p->seq);
}

static void print_action(const void *nodep, const VISIT which, const int depth)
{
    switch (which) {
    case preorder:
        break;
    case postorder:
        print(*(struct item **) nodep);
        break;
    case endorder:
        break;
    case leaf:
        print(*(struct item **) nodep);
        break;
    }
}

int main(int argc, char **argv)
{
    char buf[128];
    off_t lba;
    int i, len, x = 0, n = 1000;

    if (argv[1][0] == '-') {
        n = atoi(argv[1]+1);
        argv++; argc--;
    }
    FILE *fp = fopen(argv[1], "r");
    page = valloc(512*2048);
    
    fd = open(argv[2], O_RDWR | O_DIRECT);
        
    while (fgets(buf, sizeof(buf), fp)) {
        if (2 != sscanf(buf, "%ld %d", &lba, &len))
            printf("bad line: %s\n", buf), exit(1);
        for (i = 0; i < len/8; i++) {
            struct item *ptr = malloc(sizeof(*ptr));
            ptr->lba = lba + i*8;
            ptr->seq = 0;
            struct item *val = tsearch(ptr, &root, compare);
            if (val->lba == ptr->lba) {
                val->seq++;
                free(ptr);
                ptr = val;
            }
            sprintf(page+i*4096, "%ld %d", ptr->lba, ptr->seq);
        }
        lseek(fd, lba * 512, SEEK_SET);
        if (write(fd, page, len*512) < 0)
                perror("write"), exit(1);
        if ((++x) % n == 0) {
            printf("testing ... %d ...\n", x);
            sync();
            twalk(root, test_action);
            if (failed) {
                //printf("table:\n");
                //twalk(root, print_action);
                exit(1);
            }
            printf("done.\n");
        }
    }

    sync();

    twalk(root, test_action);
    return 0;
}
