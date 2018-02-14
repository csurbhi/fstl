#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>

#include "nstl-u.h"


int64_t generic_write(int fd, enum stl_cmd cmd, uint64_t lba, uint64_t pba, uint32_t len)
{
    struct stl_msg m = {.cmd = cmd, .flags = 0, .lba = lba, .len = len, .pba = pba};
    if (write(fd, &m, sizeof(m)) < 0)
        perror("error sending PUT to kernel"), exit(1);
    return 0;
}

int64_t set_write_frontier(int fd, int64_t wf) {
    generic_write(fd, STL_PUT_WF, 0, wf, 0);
    return 0;
}

int64_t put_space(int fd, int64_t space) {
    generic_write(fd, STL_PUT_SPACE, space, space, space);
    return 0;
}
int64_t start_ios(int fd) {
    generic_write(fd, STL_CMD_START, 0, 0, 0);
    return 0;
}

int64_t get_generic(int fd, enum stl_cmd cmd, enum stl_cmd rsp, uint64_t *pba)
{
    struct stl_msg m = {.cmd = cmd, .flags = 0, .lba = 0, .len = 0, .pba = 0};
    if (write(fd, &m, sizeof(m)) < 0)
        perror("error sending GET to kernel"), exit(1);
    if (read(fd, &m, sizeof(m)) < 0)
        perror("error getting response from kernel"), exit(1);
    if (m.cmd != rsp)
        printf("invalid response to %d: %d (%d)\n", cmd, m.cmd, rsp), exit(1);
    if (pba != NULL)
        *pba = m.pba;
    return m.lba;
}
int64_t get_generic_pba(int fd, enum stl_cmd cmd, enum stl_cmd rsp) {
    uint64_t pba;
    get_generic(fd, cmd, rsp, &pba);
    return pba;
}

int64_t get_sequence(int fd) { 
    return get_generic(fd, STL_GET_SEQ, STL_PUT_SEQ, NULL);
}
int64_t get_read_count(int fd) {
    return get_generic(fd, STL_GET_READS, STL_VAL_READS, NULL);
}
int64_t get_write_frontier(int fd) {
    return get_generic_pba(fd, STL_GET_WF, STL_PUT_WF);
}
int64_t get_free_sectors(int fd) {
    return get_generic(fd, STL_GET_SPACE, STL_PUT_SPACE, NULL);
}

int64_t get_free_zones(int fd)
{
    generic_write(fd, STL_GET_FREEZONE, 0, 0, 0);
    int len = 128 * sizeof(struct stl_msg);
    struct stl_msg *m = malloc(len);

    if ((len = read(fd, m, len)) < 0)
        perror("read failed"), exit(1);
    int i, nmsgs = len / sizeof(struct stl_msg);
    for (i = 0; i < nmsgs; i++)
        if (m[i].cmd == STL_PUT_FREEZONE)
            printf("zone %ld .. %ld\n", (int64_t)m[i].lba, (int64_t)m[i].pba);
    free(m);
    return 0;
}

int64_t cmd_doit(int fd) {
    uint64_t end;
    int64_t nsectors = get_generic(fd, STL_CMD_DOIT, STL_VAL_COPIED, &end);
    printf("%ld sectors copied, ending at %ld\n", nsectors, end);
    return 0;
}

int64_t cmd_kill(int fd) {
    generic_write(fd, STL_KILL, 0, 0, 0);
    printf("killed\n");
    return 0;
}

int64_t get_extents(int fd) {
    generic_write(fd, STL_GET_EXT, 0, 0, 0);
    int len = 128 * 1024 * sizeof(struct stl_msg);
    struct stl_msg *m = malloc(len);

    if ((len = read(fd, m, len)) < 0)
        perror("read failed"), exit(1);
    int i, nmsgs = len / sizeof(struct stl_msg);
    for (i = 0; i < nmsgs; i++)
        if (m[i].cmd == STL_PUT_EXT)
            printf("ext %ld +%d %ld\n", (int64_t)m[i].lba, (int)m[i].len, (int64_t)m[i].pba);
    free(m);
    return 0;
}

int64_t help(int fd);

struct {
    char *name;
    int64_t (*fn)(int);
    int result;
} no_arg_cmds[] = {
    {.name = "get_sequence", .fn = get_sequence, .result=1},
    {.name = "get_read_count", .fn = get_read_count, .result=1},
    {.name = "get_write_frontier", .fn = get_write_frontier, .result=1},
    {.name = "get_free_sectors", .fn = get_free_sectors, .result=1},
    {.name = "start_ios", .fn = start_ios, .result=0},
    {.name = "help", .fn = help, .result = 0},
    {.name = "get_free_zones", .fn = get_free_zones, .result=0},
    {.name = "get_extents", .fn = get_extents, .result=0},
    {.name = "doit", .fn = cmd_doit, .result=0},
    {.name = "kill", .fn = cmd_kill, .result=0},
};
#define N_0ARG_CMDS sizeof(no_arg_cmds)/sizeof(no_arg_cmds[0])

struct {
    char *name;
    char *help;
    int64_t (*fn)(int, int64_t);
    int result;
} one_arg_cmds[] = {
    {.name = "set_write_frontier", .help = "<wf>", .fn = set_write_frontier, .result=0},
    {.name = "put_space", .help = "<sectors>", .fn = put_space, .result=0},
};
#define N_1ARG_CMDS sizeof(one_arg_cmds)/sizeof(one_arg_cmds[0])

int64_t set_target(int fd, int64_t lba, int64_t pba) {
    generic_write(fd, STL_PUT_TGT, lba, pba, 0);
    return 0;
}

int64_t put_freezone(int fd, int64_t start, int64_t end)
{
    generic_write(fd, STL_PUT_FREEZONE, start, end, 0);
    return 0;
}

struct {
    char *name;
    char *help;
    int64_t (*fn)(int, int64_t, int64_t);
    int result;
} two_arg_cmds[] = {
    {.name = "set_target", .help = "<seq#> <pba_dst>", .fn = set_target, .result=0},
    {.name = "put_zone", .help = "<start_pba> <end_pba>", .fn = put_freezone, .result=0},
};
#define N_2ARG_CMDS sizeof(two_arg_cmds)/sizeof(two_arg_cmds[0])

int64_t put_extent(int fd, int64_t lba, int64_t pba, int64_t len)
{
    generic_write(fd, STL_PUT_EXT, lba, pba, len);
    return 0;
}
int64_t put_move(int fd, int64_t lba, int64_t pba, int64_t len)
{
    generic_write(fd, STL_PUT_COPY, lba, pba, len);
    return 0;
}


struct {
    char *name;
    char *help;
    int64_t (*fn)(int, int64_t, int64_t, int64_t);
    int result;
} three_arg_cmds[] = {
    {.name = "put_ext", .help = "<lba> <pba> <len>", .fn = put_extent, .result=0},
    {.name = "put_move", .help = "<lba> <pba> <len>", .fn = put_move, .result=0},
};
#define N_3ARG_CMDS sizeof(three_arg_cmds)/sizeof(three_arg_cmds[0])

int64_t help(int fd)
{
    int i;
    printf("commands:\n");
    for (i = 0; i < N_0ARG_CMDS; i++)
        printf(" %s\n", no_arg_cmds[i].name);
    for (i = 0; i < N_1ARG_CMDS; i++)
        printf(" %s %s\n", one_arg_cmds[i].name, one_arg_cmds[i].help);
    for (i = 0; i < N_2ARG_CMDS; i++)
        printf(" %s %s\n", two_arg_cmds[i].name, two_arg_cmds[i].help);
    for (i = 0; i < N_3ARG_CMDS; i++)
        printf(" %s %s\n", three_arg_cmds[i].name, three_arg_cmds[i].help);
}

    
void run_cmd(int fd, int argc, char **argv)
{
    int i;
    if (argc == 1) {
        for (i = 0; i < N_0ARG_CMDS; i++)
            if (!strcmp(argv[0], no_arg_cmds[i].name)) {
                int64_t val = no_arg_cmds[i].fn(fd);
                if (no_arg_cmds[i].result)
                    printf("%ld\n", val);
                return;
            }
    }

    if (argc == 2) {
        int64_t arg1 = atoll(argv[1]);
        for (i = 0; i < N_1ARG_CMDS; i++)
            if (!strcmp(argv[0], one_arg_cmds[i].name)) {
                int64_t val = one_arg_cmds[i].fn(fd, arg1);
                if (one_arg_cmds[i].result)
                    printf("%ld\n", val);
                return;
            }
    }

    if (argc == 3) {
        int64_t arg1 = atoll(argv[1]);
        int64_t arg2 = atoll(argv[2]);
        for (i = 0; i < N_2ARG_CMDS; i++)
            if (!strcmp(argv[0], two_arg_cmds[i].name)) {
                int64_t val = two_arg_cmds[i].fn(fd, arg1, arg2);
                if (two_arg_cmds[i].result)
                    printf("%ld\n", val);
                return;
            }
    }

    if (argc == 4) {
        int64_t arg1 = atoll(argv[1]);
        int64_t arg2 = atoll(argv[2]);
        int64_t arg3 = atoll(argv[3]);
        for (i = 0; i < N_3ARG_CMDS; i++)
            if (!strcmp(argv[0], three_arg_cmds[i].name)) {
                int64_t val = three_arg_cmds[i].fn(fd, arg1, arg2, arg3);
                if (three_arg_cmds[i].result)
                    printf("%ld\n", val);
                return;
            }
    }

    printf("bad command: %s\n", argv[0]);
    help(0);
}

int main(int argc, char **argv)
{
    if (argc == 1 || !strcmp(argv[1], "help")) {
        help(0);
        exit(1);
    }
        
    int fd = open("/dev/stl/stl0", O_RDWR);
    if (fd < 0)
        perror("failed to open /dev/stl/stl0"), exit(1);

    run_cmd(fd, argc-1, argv+1);
    close(fd);
    return 0;
}
