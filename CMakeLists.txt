cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22)
set(TARGET_NAME fluvioduck)   # rust lib name
project(${TARGET_NAME} VERSION 0.0.1)


include(FetchContent)

FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
  GIT_TAG v0.2.1 # Optionally specify a commit hash, version tag or branch here
)
FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(MANIFEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml)

set(ALL_SOURCES src/wrapper.h src/wrapper.c)
build_loadable_extension(${TARGET_NAME}  ${ALL_SOURCES})

set(LIB_NAME ${TARGET_NAME}_loadable_extension)

set_target_properties(${LIB_NAME} PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(${LIB_NAME}
                      "${CMAKE_CURRENT_BINARY_DIR}/lib${TARGET_NAME}.a")

target_link_libraries(${LIB_NAME}
                      "${CMAKE_BINARY_DIR}/src/libduckdb_static.a")
