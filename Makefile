

CC = gcc
TARGET = kvstore
SRCS = kvstore.c ntyco.c proactor.c kvs_array.c kvs_rbtree.c
INC = -I ./NtyCo/core/
LIBS = -L ./NtyCo/ -lntyco -luring


all: 
	$(CC) -o $(TARGET) $(SRCS) $(INC) $(LIBS)

clean:
	rm -rf kvstore

