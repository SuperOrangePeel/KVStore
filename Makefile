
CC = gcc
FLAGS = -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -luring -ldl -g -O3 # -ljemalloc
SRCS = $(wildcard *.c)
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
	rm -rf ./test_slave/*
	

%.o: %.c
	$(CC) $(FLAGS) -c $^ -o $@

clean: 
	rm -rf $(OBJS) $(TARGET)
	@for dir in $(SUBDIRS); do \
        if [ -d "$$dir" ]; then \
            make -C $$dir clean; \
        fi \
    done
	rm -rf kvstore.aof
	rm -rf dump.rdb
	rm -rf ./test_slave/*

restore:
	rm -rf kvstore.aof
	cp kvstore.aof.bk kvstore.aof
	cp $(TARGET) ./test_slave/


.PHONY: all clean