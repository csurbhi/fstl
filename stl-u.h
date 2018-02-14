/*
 * file: stl-u.h, user/kernel communications for Sxxgate STL
 */
#ifndef __STL_U_H__
#define __STL_U_H__

/* journal format on disk
 */
#define STL_HDR_MAGIC 0X5353544C /* 'SSTL' */
#define STL_HDR_SIZE 2048

struct stl_header {
	uint32_t magic;
	uint32_t nonce;
	uint32_t crc32;
	uint16_t flags;
	uint16_t len;
	uint64_t prev_pba;
	uint64_t next_pba;
	uint64_t lba;
	uint64_t pba;
	uint32_t seq;
} __attribute__((packed));
struct _stl_header {
	struct stl_header h;
	char pad[2048-sizeof(struct stl_header)];
};


/* the structure used for user/kernel communication
 */
struct stl_msg {
	unsigned long long cmd:8;
	unsigned long long flags:8;
	unsigned long long lba:48;
	unsigned long long len:16;
	unsigned long long pba:48;
};

enum stl_cmd {
	STL_NO_OP = 0,
	STL_GET_EXT,            /* W: list extents */
	STL_PUT_EXT,            /* W: set, R: value */
	STL_GET_WF,             /* W: get write frontier */
	STL_PUT_WF,             /* W: set, R: value */
	STL_GET_TAIL,           /* W: get media cache tail pointer */
	STL_VAL_TAIL,           /* W: set, R: value */
	STL_GET_WC,             /* W: get count of writes */
	STL_VAL_WC,             /* R: value (in lba) */
	STL_GET_RC,             /* W: get count of reads */
	STL_VAL_RC,             /* R: value (in lba) */
	STL_CMD_START,          /* W: start processing user I/Os */
	STL_CMD_GATHER,         /* W: cleaning step, LBAs 'lba' through 'pba' */
	STL_CMD_REWRITE,        /* W: cleaning step */
	STL_END                 /* R: sentinel for end of list */
};

#endif
