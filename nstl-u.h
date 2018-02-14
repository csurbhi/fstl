/*
 * file: stl-u.h, user/kernel communications for Sxxgate STL
 */
#ifndef __STL_U_H__
#define __STL_U_H__

/* journal format on disk
 */
#define STL_HDR_MAGIC 0x4c545353 /* 'SSTL' */
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


/* the structure used for user/kernel communication. Needs to fit
 * evenly into a 4K page.
 */
#if 0
struct stl_msg {		/* alternate structure */
	uint32_t cmd;
	uint32_t flags;
	uint64_t lba;
	uint64_t pba;
	uint64_t len;
} __attribute__((packed));
#endif

struct stl_msg {
	unsigned long long cmd:8;
	unsigned long long flags:8;
	unsigned long long lba:48;
	unsigned long long len:24; /* max extent = 16M sectors = 8GB */
	unsigned long long pba:40;
};

enum stl_cmd {
	STL_NO_OP = 0,
	STL_GET_EXT,            /* W: list extents */
	STL_PUT_EXT,            /* W: set, R: value */
	STL_GET_WF,             /* W: get write frontier */
	STL_PUT_WF,             /* W: set, R: value (pba) */
	STL_GET_SEQ,            /* W: write sequence # (lba) */
	STL_PUT_SEQ,            /* W: set, R: value (lba) */
	STL_GET_READS,          /* W: count of reads (lba) */
	STL_VAL_READS,          /* R: value (lba) */
	STL_CMD_START,          /* W: start processing user I/Os */
        STL_PUT_TGT,
        STL_PUT_COPY,
        STL_CMD_DOIT,
        STL_VAL_COPIED,
        STL_GET_FREEZONE,       /* W, return is list of PUT_FREEZONE */
        STL_PUT_FREEZONE,       /* lba = zone start, pba = zone_end */
        STL_GET_SPACE,
        STL_PUT_SPACE,          /* lba = free sectors, pba = free zones */
        STL_KILL,               /* unblock and fail all I/Os */
	STL_END                 /* R: sentinel for end of list */
};

/* Note that adding a free zone doesn't add free space - you have to
 * add it separately. (this allows progressive cleaning)
 */

#define FLAG_TGT_PBA 0          /* copy starting at tgt.pba */
#define FLAG_TGT_WF  1          /* copy to current WF (not supported yet) */

/*
 * STL_SET_TGT:  lba = sequence number, pba = target to copy to
 * STL_PUT_COPY: lba/pba/len
 * STL_CMD_DOIT
 * STL_VAL_COPIED: lba = count of sectors copied
 *                 pba = end of copied data
 */
#endif
