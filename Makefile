
CC = gcc
FLAGS = -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -luring -ldl
SRCS = kvstore.c ntyco.c proactor.c kvs_array.c kvs_rbtree.c kvs_hash.c
TESTCASE_SRCS = testcase.c
TARGET = kvstore
SUBDIR = ./NtyCo/
TESTCASE = testcase
TESTCASE2 = testcase_redis
TESTCASE_SRCS2 = testcase_redis.c

OBJS = $(SRCS:.c=.o)


all: $(SUBDIR) $(TARGET) $(TESTCASE) $(TESTCASE2)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

$(TARGET): $(OBJS) 
	$(CC) -o $@ $^ $(FLAGS)

$(TESTCASE): $(TESTCASE_SRCS)
	$(CC) -o $@ $^

$(TESTCASE2): $(TESTCASE_SRCS2)
	$(CC) -o $@ $^

%.o: %.c
	$(CC) $(FLAGS) -c $^ -o $@

clean: 
	rm -rf $(OBJS) $(TARGET) $(TESTCASE)


