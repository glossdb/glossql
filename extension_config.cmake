# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(glossql
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
# json: typed JSON bodies and json_valid() checks in the store
duckdb_extension_load(json)
# autocomplete: the CLI's suggestions, which grammar rules feed
duckdb_extension_load(autocomplete)

# The workspace is a DuckLake: built here as a loadable (nothing is linked in
# unless listed with duckdb_extension_statically_link), at the commit DuckDB
# 2.0.0 pins (duckdb/.github/config/extensions/ducklake.cmake), so the tests
# find it in the build's extension repository. It needs roaring (vcpkg.json);
# the tidy configure has no vcpkg and no need for the lake, so it skips it,
# and so does a build that installs the lake from DuckDB's own repository
# instead (EXT_FLAGS=-DGLOSSQL_LAKE=OFF)
option(GLOSSQL_LAKE "Build the lake beside the extension, for its tests" ON)
if(GLOSSQL_LAKE AND NOT CLANG_TIDY)
    duckdb_extension_load(ducklake
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG 417a9117f590e8141742ac0579d02f6a90162e37
    )
endif()
