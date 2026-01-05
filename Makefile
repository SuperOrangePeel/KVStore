
CC = gcc
FLAGS = -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -luring -ldl -g -O3
SRCS = kvstore.c ntyco.c proactor.c kvs_array.c kvs_rbtree.c kvs_hash.c kvs_persistence.c kvs_mempool.c

TARGET = kvstore
SUBDIRS = ./NtyCo/ ./test
OBJS = $(SRCS:.c=.o)


all: subdirs $(TARGET)

subdirs : $(SUBDIRS)
	for dir in $(SUBDIRS); do \
		make -C $$dir all; \
	done
	

$(TARGET): $(OBJS) 
	$(CC) -o $@ $^ $(FLAGS)

%.o: %.c
	$(CC) $(FLAGS) -c $^ -o $@

clean: 
	rm -rf $(OBJS) $(TARGET)
	@for dir in $(SUBDIRS); do \
        if [ -d "$$dir" ]; then \
            make -C $$dir clean; \
        fi \
    done


.PHONY: all clean