cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME glossql)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD
    "17"
    CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)
# the yyjson DuckDB ships, for the JSON Schema validator
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/yyjson/include)

set(EXTENSION_SOURCES
    src/glossql_extension.cpp
    src/glossql_grammar.cpp
    src/glossql_store.cpp
    src/glossql_runner.cpp
    src/glossql_schema.cpp
    src/glossql_lineage.cpp
    src/glossql_service.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

# A deprecated DuckDB call fails the build, not a later lint
if(NOT MSVC)
  foreach(target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
    target_compile_options(
      ${target} PRIVATE -Wall -Wextra -Wno-unused-parameter
                        -Werror=deprecated-declarations)
  endforeach()
endif()

# Link OpenSSL in both the static library as the loadable extension

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
