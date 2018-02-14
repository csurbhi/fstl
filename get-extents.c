#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>

#include "stl-u.h"

typedef unsigned long long u64;

int main(int argc, char **argv)
{
    int fd = open(argv[1], O_RDWR);
    if (fd < 0)
        perror("failed to open dev file"), exit(1);

    struct stl_msg m = {.cmd = STL_GET_WF, .flags=0, .lba=0, .len=0, .pba=0};
    if (argc > 2 && !strcmp(argv[2], "start")) {
        m.cmd = STL_CMD_START;
        write(fd, &m, sizeof(m));
        exit(0);
    }

    if (write(fd, &m, sizeof(m)) != sizeof(m))
        perror("write failed"), exit(1);

    int i, n, buf[4096], len = sizeof(buf);
    struct stl_msg *p = (void*)buf;
    if ((n = read(fd, p, len)) < 0)
        perror("read failed"), exit(1);
    if (n == 0)
        printf("0???\n"), exit(1);
    printf("WF%s %llu\n", (p[0].cmd == STL_PUT_WF) ? "" : " *BAD CMD*", (u64)p[0].pba);
        
    struct stl_msg m2 = {.cmd = STL_GET_EXT, .flags=0, .lba=0, .len=0, .pba=0};
    if (write(fd, &m2, sizeof(m)) != sizeof(m))
        perror("write failed"), exit(1);
    if ((n = read(fd, p, len)) < 0)
        perror("read failed"), exit(1);

    while (1) {
        for (i = 0; i < n/sizeof(m); i++) {
            if (p[i].cmd == STL_PUT_EXT)
                printf("ext %llu +%d %llu\n", (u64)p[i].lba, (int)p[i].len, (u64)p[i].pba);
            else if (p[i].cmd == STL_END) {
                printf("DONE\n");
                goto done;
            }
            else if (p[i].cmd != STL_NO_OP)
                printf("unknown response: %d\n", p[i].cmd);
        }
        if ((n = read(fd, p, len)) < 0)
            perror("read failed"), exit(1);
        if (n < 16)
            break;
    }
done:
    close(fd);
}
   
