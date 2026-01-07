#ifndef __KV_STORE_H__
#define __KV_STORE_H__



#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include <sys/time.h>


#define NETWORK_REACTOR 	0
#define NETWORK_PROACTOR	1
#define NETWORK_NTYCO		2

#define NETWORK_SELECT		NETWORK_PROACTOR

#define KVS_MAX_TOKENS		128


typedef int (*msg_handler)(char *msg, int length, char *response, int rsp_buf_len, int* length_r);


extern int reactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);



// void *kvs_mp_malloc(size_t size);
// void kvs_mp_free(void *ptr, size_t size);


#endif
