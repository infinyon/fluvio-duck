VERSION 0.6
FROM ubuntu:20.04

## for apt to be noninteractive
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

RUN apt-get update && apt-get install -y build-essential cmake git wget curl 
RUN wget https://github.com/Kitware/CMake/releases/download/v3.26.0-rc4/cmake-3.26.0-rc4-linux-aarch64.sh \
    -q -O /tmp/cmake-install.sh \
    && chmod u+x /tmp/cmake-install.sh \
    && mkdir /opt/cmake \
    && /tmp/cmake-install.sh --skip-license --prefix=/opt/cmake \
    && rm /tmp/cmake-install.sh \
    && ln -s /opt/cmake/bin/* /usr/local/bin

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
# RUN rustup update stable && rustup default stable
# RUN source $HOME/.cargo/env

WORKDIR /fluvio-duck

code:
    COPY --dir src duckdb ./ 
    COPY Cargo.lock Cargo.toml CMakeLists.txt Makefile ./
build:
  FROM +code
  RUN make release
  # cache cmake temp files to prevent rebuilding .o files
  # when the .cpp files don't change
  RUN --mount=type=cache,target=/code/CMakeFiles make
#   SAVE ARTIFACT fibonacci AS LOCAL "fibonacci"