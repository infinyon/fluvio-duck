/*
 * because we link twice (once to the rust library, and once to the duckdb library) we need a bridge to export the rust symbols
 * this is that bridge
 */

#include "wrapper.h"

const char* fluvioduck_version_rust(void);
void fluvioduck_init_rust(void* db);

DUCKDB_EXTENSION_API const char* fluvioduck_version() {
    return fluvioduck_version_rust();
}

DUCKDB_EXTENSION_API void fluvioduck_init(void* db) {
    fluvioduck_init_rust(db);
}
