# syntax=docker/dockerfile:1

FROM debian:bookworm-slim AS builder

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libibverbs-dev \
    librdmacm-dev \
    liburing-dev \
    make \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B /build \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_EBPF=OFF \
      -DBUILD_TEST_DIR=OFF \
 && cmake --build /build --parallel

FROM debian:bookworm-slim AS runtime

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libibverbs1 \
    librdmacm1 \
    liburing2 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system kvstore \
 && useradd --system --gid kvstore --home-dir /var/lib/kvstore kvstore \
 && mkdir -p /etc/kvstore /var/lib/kvstore \
 && chown kvstore:kvstore /var/lib/kvstore

COPY --from=builder /build/kvstore /usr/local/bin/kvstore
COPY --from=builder /src/src/kvs.toml /etc/kvstore/kvs.toml

# The repository's development config targets a host RDMA IP and SQPOLL.
# Use container-safe defaults; mount a replacement config for RDMA deployments.
RUN sed -i \
    -e 's/^bind_ip = .*/bind_ip = "0.0.0.0"/' \
    -e 's/^io_uring_mode = "sqpoll"/io_uring_mode = "normal"/' \
    /etc/kvstore/kvs.toml

WORKDIR /var/lib/kvstore
USER kvstore

EXPOSE 2000 2001
VOLUME ["/var/lib/kvstore"]

ENTRYPOINT ["/usr/local/bin/kvstore"]
CMD ["/etc/kvstore/kvs.toml"]
