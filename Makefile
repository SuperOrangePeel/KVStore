
CC = gcc
FLAGS = -I ./deps/NtyCo/core/ -L ./deps/NtyCo/ -lntyco -lpthread -luring -ldl -g -O0  -I deps/tomlc99 -lrdmacm -libverbs -Wall -Wextra # -fsanitize=address# -ljemalloc
SRCS = $(wildcard *.c) deps/tomlc99/toml.c
TARGET = kvstore
SUBDIRS = ./deps/NtyCo/ ./test ./deps/tomlc99
OBJS = $(SRCS:.c=.o)


all: subdirs $(TARGET)

subdirs : $(SUBDIRS)
	for dir in $(SUBDIRS); do \
		make -C $$dir all; \
	done
	

$(TARGET): $(OBJS) 
	$(CC) -o $@ $^ $(FLAGS)
	rm -rf ./test_slave/*
	cp $(TARGET) ./test_slave/
	

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