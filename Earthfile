VERSION 0.6
FROM ubuntu:20.04
ARG USERPLATFORM
ARG NATIVEPLATFORM
ARG TARGETPLATFORM
ARG EARTHLY_TARGET
ARG TARGETARCH
ARG TARGETOS
ARG TARGETPLATFORM
ARG tag=$TARGETOS-$TARGETARCH
WORKDIR /fluvio-duck

all:
    BUILD \
        --platform=linux/amd64 \
        --platform=linux/arm64 \
        +docker

code:
    COPY --dir src duckdb .git ./ 
    COPY build_extension.sh .gitmodules toolchain.toml Cargo.lock Cargo.toml CMakeLists.txt Makefile ./

build:
  FROM +code
  ENV GEN=ninja
  RUN echo "The current target is $EARTHLY_TARGET"
  RUN echo "The current target arch is $TARGETARCH" 
  RUN echo "The current target os is $TARGETOS"
  RUN echo "The current target os is $TARGETPLATFORM"
  ## for apt to be noninteractive
  ENV DEBIAN_FRONTEND noninteractive
  ENV DEBCONF_NONINTERACTIVE_SEEN true
  RUN apt-get update 
  RUN DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true TZ=Etc/UTC apt-get install -yqq --no-install-recommends ninja-build build-essential git wget curl software-properties-common lsb-release apt-utils
  RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
  RUN apt-add-repository "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main"
  RUN DEBIAN_FRONTEND=noninteractive apt update && apt install -yq cmake
  #RUN cd duckdb && make release
  RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
  #RUN make release
  # cache cmake temp files to prevent rebuilding .o files
  # when the .cpp files don't change
  RUN --mount=type=cache,target=/fluvio-duck/build/release/CMakeFiles/ /usr/bin/bash build_extension.sh
  SAVE ARTIFACT ./build/release/duckdb AS LOCAL fluvio-duckdb 
  SAVE ARTIFACT ./build/release/extension/fluvio-duck/fluvioduck.duckdb_extension AS LOCAL fluvioduck.duckdb_extension

docker:
  FROM ubuntu:20.04
  COPY +build/duckdb duckdb
  COPY +build/fluvioduck.duckdb_extension fluvioduck.duckdb_extension
  RUN ./duckdb --unsigned
  CMD ["./duckdb", "--unsigned"]
  SAVE IMAGE --push fluvio-duckdb/extension:multiplatform
