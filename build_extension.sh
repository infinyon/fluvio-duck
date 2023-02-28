#!/usr/bin/env bash
source "$HOME/.cargo/env"
mkdir -p build/release
cd build/release
cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=RelWithDebInfo ${BUILD_FLAGS} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=../../fluvio-duck -B.
cmake --build . --config Release