# ##############################################################################
# Dependencies
# ##############################################################################

need_zlib()
need_sqlite3()
need_argparse()
need_ghc_filesystem()
need_xxhash()
need_cpplogger()
need_yyjson()
find_package(Threads REQUIRED)

# ##############################################################################
# Libraries
# ##############################################################################

# -----------------------------------------
# Utils Base library
# -----------------------------------------

set(DFTRACER_UTILS_SOURCES
    # Common
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/common/constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/common/format_detector.cpp
    # Indexer
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/indexer_c.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/helpers.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/checkpoint_size.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/error.cpp
    # Indexer
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/indexer_factory.cpp
    # GZIP indexer
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/delete_file_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/insert_checkpoint_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/insert_file_metadata_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/insert_file_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_checkpoint.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_checkpoints.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_file_id.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_max_bytes.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_num_lines.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_schema_validity.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_stored_file_info.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/queries/query_checkpoint_size.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/gzip_indexer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/gzip/constants.cpp
    # TAR indexer
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/tar_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/tar_indexer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/insert_file_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/insert_archive_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/insert_archive_metadata_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/insert_tar_file_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/insert_tar_checkpoint_record.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/query_archive_id.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/query_tar_files.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/query_tar_checkpoints.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/tar/queries/query_metadata.cpp
    # Reader
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/reader_c.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/gzip_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/tar_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/reader_factory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/error.cpp
    # Utilities
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/utils/timer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/utils/string.cpp
    # Pipeline
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/error.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/pipeline.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/executor_type.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/executor_factory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/sequential_executor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/thread_executor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/scheduler/thread_scheduler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/scheduler/thread_task_queue.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/executor_context.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/tasks/task_context.cpp
    # Analyzers
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/analyzer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/pipeline/trace_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/derive_io_cat.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/bins.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/ignore_events.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/parse_trace.cpp
)

# +++++++++++++++++++++++++++++++++++++++++
# Static library
# +++++++++++++++++++++++++++++++++++++++++

add_library(dftracer_utils STATIC ${DFTRACER_UTILS_SOURCES})
add_library(dftracer_utils::static ALIAS dftracer_utils)

set_target_properties(dftracer_utils PROPERTIES OUTPUT_NAME dftracer_utils)
target_link_libraries(
  dftracer_utils
  PRIVATE 
  PUBLIC ghc_filesystem cpp-logger Threads::Threads yyjson::yyjson_static xxHash::xxhash_static)
target_add_rpath(dftracer_utils)
add_stdfs_if_needed(dftracer_utils)

# Link SQLite3, zlib, and Arrow with appropriate variants for static library
link_sqlite3(dftracer_utils STATIC)
link_zlib(dftracer_utils STATIC)
# link_arrow(dftracer_utils STATIC)

target_include_directories(
  dftracer_utils
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
         $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>)
target_set_warnings(dftracer_utils)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(dftracer_utils PRIVATE --coverage -fprofile-arcs
                                                -ftest-coverage)
  target_link_libraries(dftracer_utils PRIVATE --coverage)
endif()

# +++++++++++++++++++++++++++++++++++++++++
# Shared Library
# +++++++++++++++++++++++++++++++++++++++++

add_library(dftracer_utils_shared SHARED ${DFTRACER_UTILS_SOURCES})
add_library(dftracer_utils::shared ALIAS dftracer_utils_shared)

set_target_properties(
  dftracer_utils_shared
  PROPERTIES OUTPUT_NAME dftracer_utils
             EXPORT_NAME shared
             VERSION ${PROJECT_VERSION}
             SOVERSION ${PROJECT_VERSION_MAJOR})
target_link_libraries(
  dftracer_utils_shared
  PRIVATE 
  PUBLIC ghc_filesystem cpp-logger Threads::Threads yyjson::yyjson xxHash::xxhash)
target_add_rpath(dftracer_utils_shared)
add_stdfs_if_needed(dftracer_utils_shared)

# Link SQLite3, zlib, and Arrow with appropriate variants for shared library
link_sqlite3(dftracer_utils_shared SHARED)
link_zlib(dftracer_utils_shared SHARED)
# link_arrow(dftracer_utils_shared SHARED)

target_include_directories(
  dftracer_utils_shared
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
         $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>)
target_set_warnings(dftracer_utils_shared)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(dftracer_utils_shared
                         PRIVATE --coverage -fprofile-arcs -ftest-coverage)
  target_link_libraries(dftracer_utils_shared PRIVATE --coverage)
endif()

# ##############################################################################
# Executables
# ##############################################################################

set(DFTRACER_BINARIES
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_tar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_map.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_event_count.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_pgzip.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_merge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_split.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dftracer_info.cpp)

foreach(bin ${DFTRACER_BINARIES})
  string(REPLACE ".cpp" "" bin_exec ${bin})
  string(REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/" "" bin_exec
                 ${bin_exec})

  add_executable(${bin_exec} ${bin})
  set_target_properties(${bin_exec} PROPERTIES OUTPUT_NAME "${bin_exec}")
  target_link_libraries(
    ${bin_exec} PRIVATE dftracer_utils argparse::argparse ghc_filesystem)
  link_sqlite3(${bin_exec} STATIC)
  link_zlib(${bin_exec} STATIC)

  target_include_directories(${bin_exec}
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

  target_set_warnings(${bin_exec})

  # Apply coverage flags if enabled
  if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${bin_exec} PRIVATE --coverage -fprofile-arcs
                                               -ftest-coverage)
    target_link_libraries(${bin_exec} PRIVATE --coverage)
  endif()

  # Install executable
  install(TARGETS ${bin_exec} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endforeach()

# ##############################################################################
# Python binding
# ##############################################################################

if(CMAKE_VERSION VERSION_LESS 3.18)
  set(DEV_MODULE Development)
else()
  set(DEV_MODULE Development.Module)
endif()

find_package(
  Python 3.8
  COMPONENTS Interpreter ${DEV_MODULE}
  OPTIONAL_COMPONENTS Development.SABIModule)

if(Python_FOUND)
  # if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  # set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  # set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
  # "MinSizeRel" "RelWithDebInfo") endif()

  # Python C API binding sources
  add_library(
    dftracer_utils_ext MODULE
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/dftracer_utils_ext.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer_checkpoint.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer_checkpoint.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/reader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/json.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/json.cpp)

  target_include_directories(dftracer_utils_ext PRIVATE ${Python_INCLUDE_DIRS})

  target_link_libraries(dftracer_utils_ext PRIVATE dftracer_utils::shared
                                                   Python::Module)

  set_target_properties(
    dftracer_utils_ext
    PROPERTIES PREFIX "" SUFFIX ".${Python_SOABI}${CMAKE_SHARED_MODULE_SUFFIX}")

  target_add_rpath(dftracer_utils_ext)

  install(TARGETS dftracer_utils_ext LIBRARY DESTINATION dftracer/utils)
endif()

# ##############################################################################
# Installations
# ##############################################################################

# Install directive for libraries

set(INSTALLABLE_TARGETS dftracer_utils dftracer_utils_shared
)# dftracer_utils_analyzer)
if(TARGET sqlite3)
  list(APPEND INSTALLABLE_TARGETS sqlite3)
endif()
if(TARGET sqlite3_static)
  list(APPEND INSTALLABLE_TARGETS sqlite3_static)
endif()
if(TARGET ghc_filesystem)
  list(APPEND INSTALLABLE_TARGETS ghc_filesystem)
endif()
if(TARGET xxhash)
  list(APPEND INSTALLABLE_TARGETS xxhash)
endif()
if(TARGET xxhash_static)
  list(APPEND INSTALLABLE_TARGETS xxhash_static)
endif()
if(TARGET yyjson)
  list(APPEND INSTALLABLE_TARGETS yyjson)
endif()
if(TARGET yyjson_static)
  list(APPEND INSTALLABLE_TARGETS yyjson_static)
endif()
if(TARGET dftracer_zlib)
  list(APPEND INSTALLABLE_TARGETS dftracer_zlib)
endif()
if(TARGET dftracer_zlibstatic)
  list(APPEND INSTALLABLE_TARGETS dftracer_zlibstatic)
endif()
if(TARGET cpp-logger)
  list(APPEND INSTALLABLE_TARGETS cpp-logger)
endif()

install(TARGETS ${INSTALLABLE_TARGETS} EXPORT dftracer_utilsTargets)

# Install headers
install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../include/dftracer/utils/
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dftracer/utils
  FILES_MATCHING
  PATTERN "*.h")

install(
  DIRECTORY ${CMAKE_BINARY_DIR}/include/dftracer/utils/
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dftracer/utils
  FILES_MATCHING
  PATTERN "*.h")

message(STATUS "Creating package config for dftracer_utils")

# Determine which dependencies to require based on what was found vs built with
# CPM
set(PKG_CONFIG_REQUIRES "")
set(PKG_CONFIG_LIBS_PRIVATE "")

# Only add sqlite3 to pkg-config requirements if it was found on system
if(SQLite3_FOUND AND NOT SQLite3_CPM)
  set(PKG_CONFIG_REQUIRES "${PKG_CONFIG_REQUIRES} sqlite3")
  set(PKG_CONFIG_LIBS_PRIVATE "${PKG_CONFIG_LIBS_PRIVATE} -lsqlite3")
endif()

# Only add zlib to pkg-config requirements if it was found on system
if(ZLIB_FOUND AND NOT ZLIB_CPM)
  set(PKG_CONFIG_LIBS_PRIVATE "${PKG_CONFIG_LIBS_PRIVATE} -lz")
endif()

create_package_config(
  TARGET
  dftracer_utils
  VERSION
  ${DFTRACER_UTILS_PACKAGE_VERSION}
  DESCRIPTION
  "DFTracer utils library for processing gzipped files"
  URL
  "https://github.com/LLNL/dftracer-utils"
  REQUIRES
  "${PKG_CONFIG_REQUIRES}"
  LIBS_PRIVATE
  "${PKG_CONFIG_LIBS_PRIVATE}")
