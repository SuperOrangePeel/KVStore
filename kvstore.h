


#ifndef __KV_STORE_H__
#define __KV_STORE_H__


#define NETWORK_REACTOR 	0
#define NETWORK_PROACTOR	1
#define NETWORK_NTYCO		2

#define NETWORK_SELECT		NETWORK_REACTOR


typedef int (*msg_handler)(char *msg, int length, char *response);


extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);



const char *command[] = {
	"SET", "GET", "DEL", "MOD", "EXIST"
};

const char *response[] = {

};



#endif



