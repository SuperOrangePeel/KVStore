cmake_minimum_required(VERSION 3.16)

project(kvstore_ebpf C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# =========================
# Paths
# =========================

set(ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/..)

set(LIBBPF_BOOTSTRAP_DIR ${ROOT_DIR}/deps/libbpf-bootstrap)
set(LIBBPF_SRC_DIR ${LIBBPF_BOOTSTRAP_DIR}/libbpf/src)
set(LIBBPF_A ${LIBBPF_SRC_DIR}/libbpf.a)

set(BPFTOOL_SRC_DIR ${LIBBPF_BOOTSTRAP_DIR}/bpftool/src)
set(BPFTOOL_BIN ${BPFTOOL_SRC_DIR}/bpftool)

set(VMLINUX_H ${CMAKE_CURRENT_BINARY_DIR}/vmlinux.h)

# =========================
# Program name
# =========================

set(BPF_TARGET tc_forwarder)

set(BPF_SRC ${CMAKE_CURRENT_SOURCE_DIR}/${BPF_TARGET}.bpf.c)
set(USER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/${BPF_TARGET}.c)

set(BPF_OBJ ${CMAKE_CURRENT_BINARY_DIR}/${BPF_TARGET}.bpf.o)
set(BPF_SKEL ${CMAKE_CURRENT_BINARY_DIR}/${BPF_TARGET}.skel.h)

# =========================
# Architecture
# =========================

execute_process(
    COMMAND uname -m
    OUTPUT_VARIABLE UNAME_M
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (${UNAME_M} STREQUAL "x86_64")
    set(BPF_ARCH x86)
elseif (${UNAME_M} STREQUAL "aarch64")
    set(BPF_ARCH arm64)
else()
    set(BPF_ARCH ${UNAME_M})
endif()

message(STATUS "BPF target arch: ${BPF_ARCH}")

# =========================
# Build libbpf
# =========================

add_custom_command(
    OUTPUT ${LIBBPF_A}
    COMMAND make -C ${LIBBPF_SRC_DIR} BUILD_STATIC_ONLY=1
    WORKING_DIRECTORY ${ROOT_DIR}
    COMMENT "Building libbpf"
)

add_custom_target(libbpf_static DEPENDS ${LIBBPF_A})

# =========================
# Build bpftool
# =========================

add_custom_command(
    OUTPUT ${BPFTOOL_BIN}
    COMMAND make -C ${BPFTOOL_SRC_DIR}
    WORKING_DIRECTORY ${ROOT_DIR}
    DEPENDS libbpf_static
    COMMENT "Building bpftool"
)

add_custom_target(bpftool_build DEPENDS ${BPFTOOL_BIN})

# =========================
# Generate vmlinux.h
# =========================

add_custom_command(
    OUTPUT ${VMLINUX_H}
    COMMAND ${BPFTOOL_BIN} btf dump file /sys/kernel/btf/vmlinux format c > ${VMLINUX_H}
    DEPENDS bpftool_build
    COMMENT "Generating vmlinux.h"
)

add_custom_target(vmlinux_h DEPENDS ${VMLINUX_H})

# =========================
# Compile BPF object
# =========================

add_custom_command(
    OUTPUT ${BPF_OBJ}
    COMMAND clang
        -g
        -O2
        -target bpf
        -D__TARGET_ARCH_${BPF_ARCH}
        -I${CMAKE_CURRENT_BINARY_DIR}
        -I${CMAKE_CURRENT_SOURCE_DIR}
        -I${ROOT_DIR}
        -I${LIBBPF_BOOTSTRAP_DIR}/libbpf/include
        -I${LIBBPF_SRC_DIR}
        -c ${BPF_SRC}
        -o ${BPF_OBJ}
    DEPENDS ${BPF_SRC} ${VMLINUX_H}
    COMMENT "Compiling ${BPF_TARGET}.bpf.c"
)

add_custom_target(bpf_object DEPENDS ${BPF_OBJ})

# =========================
# Generate skeleton
# =========================

add_custom_command(
    OUTPUT ${BPF_SKEL}
    COMMAND ${BPFTOOL_BIN} gen skeleton ${BPF_OBJ} > ${BPF_SKEL}
    DEPENDS ${BPF_OBJ} bpftool_build
    COMMENT "Generating ${BPF_TARGET}.skel.h"
)

add_custom_target(bpf_skeleton DEPENDS ${BPF_SKEL})

# =========================
# Build userspace loader
# =========================

add_executable(${BPF_TARGET}
    ${USER_SRC}
)

add_dependencies(${BPF_TARGET}
    bpf_skeleton
    libbpf_static
)

target_include_directories(${BPF_TARGET} PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${ROOT_DIR}
    ${LIBBPF_BOOTSTRAP_DIR}/libbpf/include
    ${LIBBPF_SRC_DIR}
)

target_compile_options(${BPF_TARGET} PRIVATE
    -g
    -O2
    -Wall
    -Wextra
)

target_link_libraries(${BPF_TARGET} PRIVATE
    ${LIBBPF_A}
    elf
    z
    pthread
)