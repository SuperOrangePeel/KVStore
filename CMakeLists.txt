cmake_minimum_required(VERSION 3.16)

project(kvstore C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# =========================
# Build options
# =========================

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_JEMALLOC "Link jemalloc" OFF)
option(BUILD_NTYCO "Build deps/NtyCo before kvstore" ON)
option(BUILD_TEST_DIR "Build ./test subdir if it has Makefile" ON)
option(BUILD_EBPF "Build ./ebpf programs" ON)

# =========================
# Compiler flags
# =========================

if (ENABLE_ASAN)
    message(STATUS "ASAN enabled")
    add_compile_options(
        -g
        -O0
        -fsanitize=address
        -fno-omit-frame-pointer
        -Wall
        -Wextra
    )
    add_link_options(
        -fsanitize=address
    )
else()
    add_compile_options(
        -g
        -O3
        -Wall
        -Wextra
    )
endif()

# =========================
# Source files
# =========================

set(KVSTORE_SOURCES
    src/kvstore.c

    src/base/common.c
    src/base/kvs_mempool.c
    src/base/logger.c

    src/config/kvs_config.c

    src/executor/kvs_executor.c

    src/loop/kvs_event_loop.c

    src/net/kvs_network.c
    src/net/kvs_rdma_engine.c

    src/persistence/kvs_persistence.c

    src/protocol/kvs_response.c
    src/protocol/kvs_resp_protocol.c

    src/server/echo.c
    src/server/kvs_server.c
    src/server/kvs_server_client.c
    src/server/kvs_server_db.c
    src/server/kvs_server_handler.c
    src/server/kvs_server_master.c
    src/server/kvs_server_rdb.c
    src/server/kvs_server_signal.c
    src/server/kvs_server_slave.c
    src/server/kvs_server_timer.c

    src/storage/kvs_array.c
    src/storage/kvs_hash.c
    src/storage/kvs_rbtree.c

    deps/tomlc99/toml.c
)

add_executable(kvstore ${KVSTORE_SOURCES})

# =========================
# Include directories
# =========================

target_include_directories(kvstore PRIVATE
    src
    src/base
    src/config
    src/executor
    src/loop
    src/net
    src/persistence
    src/protocol
    src/server
    src/storage
    src/core

    deps/tomlc99
    deps/NtyCo/core
)

# =========================
# Link directories
# =========================

target_link_directories(kvstore PRIVATE
    ${CMAKE_SOURCE_DIR}/deps/NtyCo
)

# =========================
# External deps built by Makefile
# =========================

if (BUILD_NTYCO)
    add_custom_target(build_ntyco ALL
        COMMAND ${CMAKE_MAKE_PROGRAM} -C ${CMAKE_SOURCE_DIR}/deps/NtyCo all
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building deps/NtyCo"
    )

    add_dependencies(kvstore build_ntyco)
endif()

if (BUILD_TEST_DIR)
    add_custom_target(build_test_dir ALL
        COMMAND ${CMAKE_MAKE_PROGRAM} -C ${CMAKE_SOURCE_DIR}/test all
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building ./test"
    )
endif()

# =========================
# Libraries
# =========================

target_link_libraries(kvstore PRIVATE
    ntyco
    pthread
    uring
    dl
    rdmacm
    ibverbs
)

if (ENABLE_JEMALLOC)
    target_link_libraries(kvstore PRIVATE jemalloc)
endif()

# =========================
# Post build: copy to test_slave
# =========================

add_custom_command(TARGET kvstore POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/src/kvs.toml
        $<TARGET_FILE_DIR:kvstore>/kvs.toml

    COMMENT "Copying kvs.toml to output directory"

)

add_custom_command(TARGET kvstore POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_SOURCE_DIR}/test_slave
    COMMAND ${CMAKE_COMMAND} -E rm -f ${CMAKE_SOURCE_DIR}/test_slave/kvstore
    COMMAND ${CMAKE_COMMAND} -E rm -f ${CMAKE_SOURCE_DIR}/test_slave/dump.rdb
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:kvstore> ${CMAKE_SOURCE_DIR}/test_slave/
    COMMENT "Copying kvstore to test_slave/"
)

# =========================
# Clean extra generated files
# =========================

add_custom_target(clean_runtime
    COMMAND ${CMAKE_COMMAND} -E rm -f ${CMAKE_SOURCE_DIR}/kvstore.aof
    COMMAND ${CMAKE_COMMAND} -E rm -f ${CMAKE_SOURCE_DIR}/dump.rdb
    COMMAND ${CMAKE_COMMAND} -E rm -f ${CMAKE_SOURCE_DIR}/test_slave/kvstore
    COMMAND ${CMAKE_COMMAND} -E rm -f ${CMAKE_SOURCE_DIR}/test_slave/dump.rdb
    COMMENT "Cleaning runtime files"
)

add_custom_target(restore
    COMMAND ${CMAKE_COMMAND} -E rm -f ${CMAKE_SOURCE_DIR}/kvstore.aof
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/kvstore.aof.bk ${CMAKE_SOURCE_DIR}/kvstore.aof
    COMMENT "Restoring kvstore.aof from kvstore.aof.bk"
)

if (BUILD_EBPF)
    add_custom_target(ebpf ALL
        COMMAND ${CMAKE_COMMAND} -S ${CMAKE_SOURCE_DIR}/ebpf -B ${CMAKE_BINARY_DIR}/ebpf
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/ebpf -j
        COMMENT "Building eBPF programs"
    )
endif()
