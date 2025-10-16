set(CPM_USE_LOCAL_PACKAGES ON)
set(CPM_SOURCE_CACHE "${CMAKE_SOURCE_DIR}/.cpmsource")

find_package(Threads REQUIRED)

set(DEPENDENCY_LIBRARY_DIRS "" PARENT_SCOPE)

if(CMAKE_VERSION VERSION_LESS 3.18)
  set(DEV_MODULE Development)
else()
  set(DEV_MODULE Development.Module)
endif()

find_package(
  Python 3.8
  COMPONENTS Interpreter ${DEV_MODULE}
  OPTIONAL_COMPONENTS Development.SABIModule)

function(need_cpplogger)
  if(NOT cpplogger_ADDED)
    cpmaddpackage(
      NAME cpplogger GITHUB_REPOSITORY hariharan-devarajan/cpp-logger
      GIT_TAG 22201852471578483f3aa6b754dcb35a09d6838f
      OPTIONS
      FORCE YES)
    if(cpplogger_ADDED AND SKBUILD)
        set(DEPENDENCY_LIBRARY_DIRS "${DEPENDENCY_LIBRARY_DIRS}" ${Python_SITELIB}/${CMAKE_INSTALL_LIBDIR} PARENT_SCOPE)
    endif() 
  endif()
endfunction()

function(need_argparse)
  if(NOT argparse_ADDED)
    cpmaddpackage(
      NAME
      argparse
      GITHUB_REPOSITORY
      p-ranav/argparse
      VERSION
      3.2
      OPTIONS
      "ARGPARSE_BUILD_TESTS OFF"
      "ARGPARSE_BUILD_SAMPLES OFF"
      FORCE
      YES)
  endif()
endfunction()

function(need_ghc_filesystem)
  if(NOT ghc_filesystem_ADDED)
    cpmaddpackage(
      NAME
      ghc_filesystem
      GITHUB_REPOSITORY
      gulrak/filesystem
      VERSION
      1.5.14
      OPTIONS
      "GHC_FILESYSTEM_WITH_INSTALL ON"
      FORCE
      YES)
  endif()
endfunction()

function(need_yyjson)
  if(NOT yyjson_ADDED)
    cpmaddpackage(
      NAME
      yyjson
      GITHUB_REPOSITORY
      ibireme/yyjson
      VERSION
      0.12.0
      GIT_TAG
      0.12.0
      FORCE
      YES
      DOWNLOAD_ONLY
      YES)
  endif()

  set(YYJSON_SOVERSION 0)
  add_library(yyjson_static STATIC ${yyjson_SOURCE_DIR}/src/yyjson.h
                                   ${yyjson_SOURCE_DIR}/src/yyjson.c)
  add_library(yyjson SHARED ${yyjson_SOURCE_DIR}/src/yyjson.h
                            ${yyjson_SOURCE_DIR}/src/yyjson.c)

  target_include_directories(yyjson_static
                             PUBLIC $<BUILD_INTERFACE:${yyjson_SOURCE_DIR}/src>)
  set_target_properties(yyjson_static PROPERTIES VERSION ${PROJECT_VERSION}
                                                 SOVERSION ${YYJSON_SOVERSION})

  target_include_directories(yyjson
                             PUBLIC $<BUILD_INTERFACE:${yyjson_SOURCE_DIR}/src>)
  set_target_properties(yyjson PROPERTIES VERSION ${PROJECT_VERSION}
                                          SOVERSION ${YYJSON_SOVERSION})

  install(FILES ${yyjson_SOURCE_DIR}/src/yyjson.h
          DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  install(
    TARGETS yyjson yyjson_static
    EXPORT yyjsonTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_library(yyjson::yyjson ALIAS yyjson)
  add_library(yyjson::yyjson_static ALIAS yyjson_static)
  message(STATUS "Added yyjson library")
endfunction()

function(need_sqlite3)
  find_package(SQLite3 3.30 QUIET)

  if(SQLite3_FOUND)
    message(STATUS "Found system SQLite3: ${SQLite3_LIBRARIES}")

    # Create alias for system SQLite3 if it doesn't exist
    if(NOT TARGET SQLite::SQLite3)
      # Create imported target for system SQLite3
      add_library(SQLite::SQLite3 UNKNOWN IMPORTED)
      set_target_properties(
        SQLite::SQLite3
        PROPERTIES IMPORTED_LOCATION "${SQLite3_LIBRARIES}"
                   INTERFACE_INCLUDE_DIRECTORIES "${SQLite3_INCLUDE_DIRS}")
    endif()

    if(NOT TARGET SQLite::SQLite3_static)
      add_library(SQLite::SQLite3_static ALIAS SQLite::SQLite3)
    endif()

    # Set variables in parent scope so they persist outside the function
    set(SQLite3_FOUND
        ${SQLite3_FOUND}
        PARENT_SCOPE)
    set(SQLite3_LIBRARIES
        ${SQLite3_LIBRARIES}
        PARENT_SCOPE)
    set(SQLite3_INCLUDE_DIRS
        ${SQLite3_INCLUDE_DIRS}
        PARENT_SCOPE)
    set(SQLite3_CPM
        FALSE
        PARENT_SCOPE)
  else()
    # Build with CPM
    if(NOT SQLite3_ADDED)
      cpmaddpackage(
        NAME
        SQLite3
        URL
        https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip
        VERSION
        3.46.1
        DOWNLOAD_ONLY
        YES)
    endif()

    if(SQLite3_ADDED)
      message(STATUS "Built SQLite3 with CPM")

      # Create sqlite3 library from amalgamation
      add_library(sqlite3 SHARED ${SQLite3_SOURCE_DIR}/sqlite3.c)

      add_library(sqlite3_static STATIC ${SQLite3_SOURCE_DIR}/sqlite3.c)

      target_include_directories(
        sqlite3 PUBLIC $<BUILD_INTERFACE:${SQLite3_SOURCE_DIR}>
                       $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

      target_include_directories(
        sqlite3_static PUBLIC $<BUILD_INTERFACE:${SQLite3_SOURCE_DIR}>
                              $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

      # Enable common SQLite features
      target_compile_definitions(
        sqlite3 PUBLIC SQLITE_ENABLE_FTS5 SQLITE_ENABLE_JSON1
                       SQLITE_ENABLE_RTREE SQLITE_THREADSAFE=1)

      target_compile_definitions(
        sqlite3_static PUBLIC SQLITE_ENABLE_FTS5 SQLITE_ENABLE_JSON1
                              SQLITE_ENABLE_RTREE SQLITE_THREADSAFE=1)

      if(NOT WIN32)
        target_link_libraries(sqlite3 PRIVATE pthread dl m)
        target_link_libraries(sqlite3_static PRIVATE pthread dl m)
      endif()

      # Create alias for compatibility
      add_library(SQLite::SQLite3 ALIAS sqlite3)
      add_library(SQLite::SQLite3_static ALIAS sqlite3_static)

      # Make sqlite3 installable
      install(
        TARGETS sqlite3 sqlite3_static
        EXPORT sqlite3Targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

      # Install sqlite3 header
      install(FILES ${SQLite3_SOURCE_DIR}/sqlite3.h
              DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

      set(SQLite3_CPM
          TRUE
          PARENT_SCOPE)
    endif()
  endif()
endfunction()

# Function to link SQLite3 to a target Parameters: TARGET_NAME - name of the
# target to link SQLite3 to
function(link_sqlite3 TARGET_NAME LIBRARY_TYPE)
  # Validate parameters
  if(NOT TARGET_NAME)
    message(FATAL_ERROR "link_sqlite3: TARGET_NAME is required")
  endif()

  if(NOT TARGET ${TARGET_NAME})
    message(FATAL_ERROR "link_sqlite3: Target '${TARGET_NAME}' does not exist")
  endif()

  # Check if any SQLite3 variant is available
  set(SQLITE3_AVAILABLE FALSE)

  # Check for CPM-built SQLite3
  if(TARGET sqlite3 OR TARGET sqlite3_static)
    set(SQLITE3_AVAILABLE TRUE)
  endif()

  # Check for system SQLite3
  if(TARGET SQLite::SQLite3)
    set(SQLITE3_AVAILABLE TRUE)
  endif()

  if(NOT SQLITE3_AVAILABLE)
    message(
      FATAL_ERROR
        "link_sqlite3: No SQLite3 found! Call need_sqlite3() first or ensure system SQLite3 is available."
    )
  endif()

  # Link appropriate SQLite3 variant
  if(LIBRARY_TYPE STREQUAL "STATIC")
    # For static libraries, prefer static SQLite3
    if(TARGET sqlite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(
        STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3_static")
    elseif(TARGET SQLite::SQLite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3_static")
    elseif(TARGET sqlite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3")
    elseif(TARGET SQLite::SQLite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3")
    endif()
  else()
    # For shared libraries, prefer shared SQLite3
    if(TARGET sqlite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3")
    elseif(TARGET SQLite::SQLite3)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3")
    elseif(TARGET sqlite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(
        STATUS "Linked ${TARGET_NAME} to CPM-built SQLite::SQLite3_static")
    elseif(TARGET SQLite::SQLite3_static)
      target_link_libraries(${TARGET_NAME} PRIVATE SQLite::SQLite3_static)
      message(STATUS "Linked ${TARGET_NAME} to SQLite::SQLite3_static")
    endif()
  endif()
endfunction()

function(need_zlib)
  find_package(ZLIB 1.2 QUIET)

  if(ZLIB_FOUND)
    message(STATUS "Found system ZLIB: ${ZLIB_LIBRARIES}")

    # Set variables in parent scope so they persist outside the function
    set(ZLIB_FOUND
        ${ZLIB_FOUND}
        PARENT_SCOPE)
    set(ZLIB_LIBRARIES
        ${ZLIB_LIBRARIES}
        PARENT_SCOPE)
    set(ZLIB_INCLUDE_DIRS
        ${ZLIB_INCLUDE_DIRS}
        PARENT_SCOPE)
    set(ZLIB_CPM
        FALSE
        PARENT_SCOPE)
  else()
    set(ZLIB_CPM
        FALSE
        PARENT_SCOPE)
    # Build with CPM
    cpmaddpackage(
      NAME
      ZLIB
      GITHUB_REPOSITORY
      madler/zlib
      VERSION
      1.3.1
      OPTIONS
      "ZLIB_BUILD_STATIC ON"
      "ZLIB_BUILD_SHARED ON"
      "ZLIB_INSTALL OFF"
      "ZLIB_BUILD_EXAMPLES OFF"
      DOWNLOAD_ONLY
      YES)

    if(ZLIB_ADDED)
      message(STATUS "Built ZLIB with CPM")
      set(ZLIB_CPM
          TRUE
          PARENT_SCOPE)

      # Make sure the source and binary directories are available in parent
      # scope
      set(ZLIB_SOURCE_DIR
          ${ZLIB_SOURCE_DIR}
          PARENT_SCOPE)
      set(ZLIB_BINARY_DIR
          ${ZLIB_BINARY_DIR}
          PARENT_SCOPE)

      # Create our own zlib targets with proper install interface and fix macOS
      # issues
      if(APPLE)
        # Create patched source files for macOS to fix type conflicts
        set(ZLIB_SOURCES_PATCHED "")
        foreach(src_file adler32.c crc32.c)
          file(READ "${ZLIB_SOURCE_DIR}/${src_file}" SRC_CONTENT)
          # Replace z_off64_t parameter with z_off_t in function definitions to
          # match declarations
          string(REGEX REPLACE "z_off64_t len2\\)" "z_off_t len2)"
                               SRC_CONTENT_FIXED "${SRC_CONTENT}")
          string(REGEX REPLACE "z_off64_t len2\\s*\\{" "z_off_t len2 {"
                               SRC_CONTENT_FIXED "${SRC_CONTENT_FIXED}")
          file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/${src_file}"
               "${SRC_CONTENT_FIXED}")
          list(APPEND ZLIB_SOURCES_PATCHED
               "${CMAKE_CURRENT_BINARY_DIR}/${src_file}")
        endforeach()

        # Add the rest of the files normally
        foreach(
          src_file
          compress.c
          deflate.c
          gzclose.c
          gzlib.c
          gzread.c
          gzwrite.c
          inflate.c
          infback.c
          inftrees.c
          inffast.c
          trees.c
          uncompr.c
          zutil.c)
          list(APPEND ZLIB_SOURCES_PATCHED "${ZLIB_SOURCE_DIR}/${src_file}")
        endforeach()

        add_library(dftracer_zlib SHARED ${ZLIB_SOURCES_PATCHED})
        add_library(dftracer_zlibstatic STATIC ${ZLIB_SOURCES_PATCHED})
      else()
        add_library(
          dftracer_zlib SHARED
          ${ZLIB_SOURCE_DIR}/adler32.c
          ${ZLIB_SOURCE_DIR}/compress.c
          ${ZLIB_SOURCE_DIR}/crc32.c
          ${ZLIB_SOURCE_DIR}/deflate.c
          ${ZLIB_SOURCE_DIR}/gzclose.c
          ${ZLIB_SOURCE_DIR}/gzlib.c
          ${ZLIB_SOURCE_DIR}/gzread.c
          ${ZLIB_SOURCE_DIR}/gzwrite.c
          ${ZLIB_SOURCE_DIR}/inflate.c
          ${ZLIB_SOURCE_DIR}/infback.c
          ${ZLIB_SOURCE_DIR}/inftrees.c
          ${ZLIB_SOURCE_DIR}/inffast.c
          ${ZLIB_SOURCE_DIR}/trees.c
          ${ZLIB_SOURCE_DIR}/uncompr.c
          ${ZLIB_SOURCE_DIR}/zutil.c)
        add_library(
          dftracer_zlibstatic STATIC
          ${ZLIB_SOURCE_DIR}/adler32.c
          ${ZLIB_SOURCE_DIR}/compress.c
          ${ZLIB_SOURCE_DIR}/crc32.c
          ${ZLIB_SOURCE_DIR}/deflate.c
          ${ZLIB_SOURCE_DIR}/gzclose.c
          ${ZLIB_SOURCE_DIR}/gzlib.c
          ${ZLIB_SOURCE_DIR}/gzread.c
          ${ZLIB_SOURCE_DIR}/gzwrite.c
          ${ZLIB_SOURCE_DIR}/inflate.c
          ${ZLIB_SOURCE_DIR}/infback.c
          ${ZLIB_SOURCE_DIR}/inftrees.c
          ${ZLIB_SOURCE_DIR}/inffast.c
          ${ZLIB_SOURCE_DIR}/trees.c
          ${ZLIB_SOURCE_DIR}/uncompr.c
          ${ZLIB_SOURCE_DIR}/zutil.c)
      endif()

      # Fix type mismatch issues on macOS by ensuring consistent type
      # definitions
      if(APPLE)
        target_compile_definitions(dftracer_zlib PRIVATE _LARGEFILE64_SOURCE=1
                                                         _FILE_OFFSET_BITS=64)
        target_compile_definitions(
          dftracer_zlibstatic PRIVATE _LARGEFILE64_SOURCE=1
                                      _FILE_OFFSET_BITS=64)
      else()
        target_compile_definitions(
          dftracer_zlib PRIVATE _LARGEFILE64_SOURCE=1 _FILE_OFFSET_BITS=64
                                Z_HAVE_STDARG_H=1)
        target_compile_definitions(
          dftracer_zlibstatic PRIVATE _LARGEFILE64_SOURCE=1
                                      _FILE_OFFSET_BITS=64 Z_HAVE_STDARG_H=1)
      endif()

      # Set proper include directories for build and install
      target_include_directories(
        dftracer_zlib
        PUBLIC $<BUILD_INTERFACE:${ZLIB_SOURCE_DIR}>
               # $<BUILD_INTERFACE:${ZLIB_BINARY_DIR}>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

      target_include_directories(
        dftracer_zlibstatic
        PUBLIC $<BUILD_INTERFACE:${ZLIB_SOURCE_DIR}>
               # $<BUILD_INTERFACE:${ZLIB_BINARY_DIR}>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

      # Copy the generated zconf.h from the original zlib build
      if(EXISTS "${ZLIB_BINARY_DIR}/zconf.h")
        configure_file("${ZLIB_BINARY_DIR}/zconf.h"
                       "${CMAKE_CURRENT_BINARY_DIR}/zconf.h" COPYONLY)
        target_include_directories(
          dftracer_zlib PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
        target_include_directories(
          dftracer_zlibstatic
          PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
      endif()

      # Install our custom zlib targets
      install(
        TARGETS dftracer_zlib dftracer_zlibstatic
        EXPORT ZlibTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

      install(
        EXPORT ZlibTargets
        FILE ZlibTargets.cmake
        NAMESPACE dftracer::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/zlib)

      # Install zlib headers manually
      if(ZLIB_SOURCE_DIR AND ZLIB_BINARY_DIR)
        if(EXISTS "${ZLIB_SOURCE_DIR}/zlib.h")
          install(FILES "${ZLIB_SOURCE_DIR}/zlib.h"
                  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
        endif()

        if(EXISTS "${ZLIB_BINARY_DIR}/zconf.h")
          install(FILES "${ZLIB_BINARY_DIR}/zconf.h"
                  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
        endif()
      endif()

      # Create aliases for compatibility (avoid conflicts with Arrow)
      add_library(dftracer::zlib ALIAS dftracer_zlib)
      add_library(dftracer::zlibstatic ALIAS dftracer_zlibstatic)

      # Make zlib available in parent scope for Arrow - let Arrow build its own
      set(ZLIB_LIBRARIES
          dftracer_zlib
          PARENT_SCOPE)
      set(ZLIB_INCLUDE_DIRS
          ${ZLIB_SOURCE_DIR} ${ZLIB_BINARY_DIR}
          PARENT_SCOPE)
      # Don't set ZLIB_FOUND to let Arrow build its own zlib
      set(ZLIB_FOUND
          FALSE
          PARENT_SCOPE)
    endif()
  endif()
endfunction()

function(link_zlib TARGET_NAME LIBRARY_TYPE)
  # Validate parameters
  if(NOT TARGET_NAME)
    message(FATAL_ERROR "link_zlib: TARGET_NAME is required")
  endif()

  if(NOT LIBRARY_TYPE MATCHES "^(STATIC|SHARED)$")
    message(
      FATAL_ERROR "link_zlib: LIBRARY_TYPE must be either STATIC or SHARED")
  endif()

  if(NOT TARGET ${TARGET_NAME})
    message(FATAL_ERROR "link_zlib: Target '${TARGET_NAME}' does not exist")
  endif()

  # Check if any zlib variant is available
  set(ZLIB_AVAILABLE FALSE)
  if(TARGET dftracer_zlibstatic
     OR TARGET dftracer_zlib
     OR ZLIB_FOUND)
    set(ZLIB_AVAILABLE TRUE)
  endif()

  if(NOT ZLIB_AVAILABLE)
    message(
      FATAL_ERROR
        "link_zlib: No zlib found! Call need_zlib() first or ensure system zlib is available."
    )
  endif()

  # Link appropriate zlib variant
  if(LIBRARY_TYPE STREQUAL "STATIC")
    # For static libraries, prefer static zlib if available
    if(TARGET dftracer_zlibstatic)
      target_link_libraries(${TARGET_NAME} PRIVATE dftracer::zlibstatic)
      message(STATUS "Linked ${TARGET_NAME} to dftracer zlibstatic")
    elseif(TARGET dftracer_zlib)
      target_link_libraries(${TARGET_NAME} PRIVATE dftracer::zlib)
      message(STATUS "Linked ${TARGET_NAME} to dftracer zlib (shared)")
    elseif(ZLIB_FOUND)
      target_link_libraries(${TARGET_NAME} PRIVATE ZLIB::ZLIB)
      message(STATUS "Linked ${TARGET_NAME} to system ZLIB::ZLIB")
    endif()
  else() # SHARED
    # For shared libraries, prefer shared zlib if available
    if(TARGET dftracer_zlib)
      target_link_libraries(${TARGET_NAME} PRIVATE dftracer::zlib)
      message(STATUS "Linked ${TARGET_NAME} to dftracer zlib (shared)")
    elseif(TARGET dftracer_zlibstatic)
      target_link_libraries(${TARGET_NAME} PRIVATE dftracer::zlibstatic)
      message(STATUS "Linked ${TARGET_NAME} to dftracer zlibstatic")
    elseif(ZLIB_FOUND)
      target_link_libraries(${TARGET_NAME} PRIVATE ZLIB::ZLIB)
      message(STATUS "Linked ${TARGET_NAME} to system ZLIB::ZLIB")
    endif()
  endif()
endfunction()

function(need_xxhash)
  if(NOT xxhash_ADDED)
    cpmaddpackage(
      NAME
      xxhash
      GITHUB_REPOSITORY
      Cyan4973/xxHash
      GIT_TAG
      v0.8.3
      OPTIONS
      "XXHASH_BUILD_XXHSUM OFF"
      "XXHASH_BUNDLED_MODE ON"
      SOURCE_SUBDIR
      cmake_unofficial
      DOWNLOAD_ONLY
      YES)
    if(xxhash_ADDED)
      add_library(xxhash_static "${xxhash_SOURCE_DIR}/xxhash.c")
      add_library(xxhash SHARED "${xxhash_SOURCE_DIR}/xxhash.c")
      target_include_directories(
        xxhash PUBLIC $<BUILD_INTERFACE:${xxhash_SOURCE_DIR}>
                      $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
      target_include_directories(
        xxhash_static PUBLIC $<BUILD_INTERFACE:${xxhash_SOURCE_DIR}>
                             $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
      install(FILES ${xxhash_SOURCE_DIR}/xxhash.h
              DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
      install(
        TARGETS xxhash xxhash_static
        EXPORT xxhashTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
      add_library(xxHash::xxhash ALIAS xxhash)
      add_library(xxHash::xxhash_static ALIAS xxhash_static)
      message(STATUS "Added xxhash library")
    endif()
  endif()
endfunction()

function(need_picosha2)
  if(NOT PicoSHA2_ADDED)
    cpmaddpackage(
      NAME
      PicoSHA2
      GITHUB_REPOSITORY
      okdshin/PicoSHA2
      VERSION
      1.0.1
      GIT_TAG
      "v1.0.1"
      DOWNLOAD_ONLY
      YES)

    if(PicoSHA2_ADDED)
      add_library(picosha2 INTERFACE)
      target_include_directories(
        picosha2 INTERFACE $<BUILD_INTERFACE:${PicoSHA2_SOURCE_DIR}>
                           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
      install(FILES ${PicoSHA2_SOURCE_DIR}/picosha2.h
              DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
      message(STATUS "Added picosha2 header-only library")
    endif()
  endif()
endfunction()

function(need_arrow)
  find_package(Arrow 21.0.0 QUIET)
  find_package(Parquet 21.0.0 QUIET)

  if(Arrow_FOUND AND Parquet_FOUND)
    message(STATUS "Found system Arrow and Parquet")
    set(Arrow_ADDED
        TRUE
        PARENT_SCOPE)
  else()
    if(NOT Arrow_ADDED)
      # Use a known stable version with minimal config
      cpmaddpackage(
        NAME
        Arrow
        GITHUB_REPOSITORY
        apache/arrow
        VERSION
        21.0.0
        GIT_TAG
        "apache-arrow-21.0.0"
        SOURCE_SUBDIR
        "./cpp"
        EXCLUDE_FROM_ALL
        YES
        OPTIONS
        "ARROW_DEFINE_OPTIONS ON"
        "ARROW_BUILD_STATIC ON"
        "ARROW_BUILD_SHARED ON"
        "ARROW_PARQUET ON"
        "ARROW_BUILD_TESTS OFF"
        "ARROW_BUILD_BENCHMARKS OFF"
        "ARROW_BUILD_EXAMPLES OFF"
        "ARROW_WITH_BACKTRACE OFF"
        # "ARROW_DEPENDENCY_SOURCE SYSTEM"
        "ARROW_BOOST_USE_SHARED OFF"
        "ARROW_ZSTD_USE_SHARED OFF"
        "ARROW_JEMALLOC_USE_SHARED OFF"
        "ARROW_PROTOBUF_USE_SHARED OFF"
        "ARROW_WITH_THRIFT OFF"
        "ARROW_COMPUTE OFF"
        "ARROW_FLIGHT OFF"
        "ARROW_WITH_GRPC OFF"
        "ARROW_WITH_OPENTELEMETRY OFF"
        "ARROW_IPC OFF"
        "ARROW_DATASET OFF"
        "ARROW_BUILD_CONFIG_SUMMARY_JSON OFF"
        "ARROW_WITH_ZLIB OFF"
        "ARROW_ENABLE_TIMING_TESTS OFF"
        "ARROW_BROTLI_USE_SHARED OFF"
        "ARROW_GFLAGS_USE_SHARED OFF"
        "ARROW_GRPC_USE_SHARED OFF"
        "ARROW_JEMALLOC_USE_SHARED OFF"
        "ARROW_LLVM_USE_SHARED OFF"
        "ARROW_LZ4_USE_SHARED OFF"
        "ARROW_OPENSSL_USE_SHARED OFF"
        "ARROW_SNAPPY_USE_SHARED OFF"
        "ARROW_THRIFT_USE_SHARED OFF"
        "ARROW_UTF8PROC_USE_SHARED OFF"
        "ARROW_ZSTD_USE_SHARED OFF"
        "ARROW_INSTALL_NAME_RPATH ON"
        "ARROW_INSTALL ON"
        "PARQUET_INSTALL ON"
        "ARROW_NO_INSTALL OFF"
        "ARROW_WITH_SNAPPY ON" # compression
        FORCE
        YES)
      file(READ "${Arrow_SOURCE_DIR}/cmake_modules/ArrowTargets.cmake"
           _arrow_targets_cmake)
      string(REPLACE "install(EXPORT arrow_targets"
                     "# install(EXPORT arrow_targets" _arrow_targets_cmake
                     "${_arrow_targets_cmake}")
      file(WRITE "${Arrow_SOURCE_DIR}/cmake_modules/ArrowTargets.cmake"
           "${_arrow_targets_cmake}")
    endif()
  endif()
endfunction()

function(link_arrow TARGET_NAME LIBRARY_TYPE)
  # Validate parameters
  if(NOT TARGET_NAME)
    message(FATAL_ERROR "link_arrow: TARGET_NAME is required")
  endif()

  if(NOT LIBRARY_TYPE MATCHES "^(STATIC|SHARED)$")
    message(
      FATAL_ERROR "link_arrow: LIBRARY_TYPE must be either STATIC or SHARED")
  endif()

  if(NOT TARGET ${TARGET_NAME})
    message(FATAL_ERROR "link_arrow: Target '${TARGET_NAME}' does not exist")
  endif()

  # Check if Arrow is available
  set(ARROW_AVAILABLE FALSE)

  # Check for CPM-built Arrow
  if(TARGET arrow_shared OR TARGET arrow_static)
    set(ARROW_AVAILABLE TRUE)
  endif()

  # Check for system Arrow
  if(Arrow_FOUND)
    set(ARROW_AVAILABLE TRUE)
  endif()

  if(NOT ARROW_AVAILABLE)
    message(
      FATAL_ERROR
        "link_arrow: No Arrow found! Call need_arrow() first or ensure system Arrow is available."
    )
  endif()

  # Add Arrow include directories for CPM-built Arrow
  if(TARGET arrow_shared OR TARGET arrow_static)
    if(arrow_SOURCE_DIR)
      target_include_directories(${TARGET_NAME} PRIVATE ${arrow_SOURCE_DIR}/src
                                                        ${arrow_BINARY_DIR}/src)
      message(
        STATUS
          "Added Arrow include directories to ${TARGET_NAME}: ${arrow_SOURCE_DIR}/src, ${arrow_BINARY_DIR}/src"
      )
    endif()
  endif()

  # Link appropriate Arrow variant based on LIBRARY_TYPE
  if(LIBRARY_TYPE STREQUAL "STATIC")
    # For static libraries, prefer static Arrow if available
    if(TARGET arrow_static)
      target_link_libraries(${TARGET_NAME} PRIVATE arrow_static)
      message(STATUS "Linked ${TARGET_NAME} to arrow_static")
    elseif(TARGET arrow_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE arrow_shared)
      message(
        STATUS
          "Linked ${TARGET_NAME} to arrow_shared (static requested but not available)"
      )
    elseif(TARGET Arrow::arrow_static)
      target_link_libraries(${TARGET_NAME} PRIVATE Arrow::arrow_static)
      message(STATUS "Linked ${TARGET_NAME} to Arrow::arrow_static")
    elseif(TARGET Arrow::arrow_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE Arrow::arrow_shared)
      message(
        STATUS
          "Linked ${TARGET_NAME} to Arrow::arrow_shared (static requested but not available)"
      )
    endif()

    # Link Parquet static variant
    if(TARGET parquet_static)
      target_link_libraries(${TARGET_NAME} PRIVATE parquet_static)
      message(STATUS "Linked ${TARGET_NAME} to parquet_static")
    elseif(TARGET parquet_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE parquet_shared)
      message(
        STATUS
          "Linked ${TARGET_NAME} to parquet_shared (static requested but not available)"
      )
    elseif(TARGET Parquet::parquet_static)
      target_link_libraries(${TARGET_NAME} PRIVATE Parquet::parquet_static)
      message(STATUS "Linked ${TARGET_NAME} to Parquet::parquet_static")
    elseif(TARGET Parquet::parquet_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE Parquet::parquet_shared)
      message(
        STATUS
          "Linked ${TARGET_NAME} to Parquet::parquet_shared (static requested but not available)"
      )
    endif()
  else() # SHARED
    # For shared libraries, prefer shared Arrow if available
    if(TARGET arrow_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE arrow_shared)
      message(STATUS "Linked ${TARGET_NAME} to arrow_shared")
    elseif(TARGET arrow_static)
      target_link_libraries(${TARGET_NAME} PRIVATE arrow_static)
      message(
        STATUS
          "Linked ${TARGET_NAME} to arrow_static (shared requested but not available)"
      )
    elseif(TARGET Arrow::arrow_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE Arrow::arrow_shared)
      message(STATUS "Linked ${TARGET_NAME} to Arrow::arrow_shared")
    elseif(TARGET Arrow::arrow_static)
      target_link_libraries(${TARGET_NAME} PRIVATE Arrow::arrow_static)
      message(
        STATUS
          "Linked ${TARGET_NAME} to Arrow::arrow_static (shared requested but not available)"
      )
    endif()

    # Link Parquet shared variant
    if(TARGET parquet_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE parquet_shared)
      message(STATUS "Linked ${TARGET_NAME} to parquet_shared")
    elseif(TARGET parquet_static)
      target_link_libraries(${TARGET_NAME} PRIVATE parquet_static)
      message(
        STATUS
          "Linked ${TARGET_NAME} to parquet_static (shared requested but not available)"
      )
    elseif(TARGET Parquet::parquet_shared)
      target_link_libraries(${TARGET_NAME} PRIVATE Parquet::parquet_shared)
      message(STATUS "Linked ${TARGET_NAME} to Parquet::parquet_shared")
    elseif(TARGET Parquet::parquet_static)
      target_link_libraries(${TARGET_NAME} PRIVATE Parquet::parquet_static)
      message(
        STATUS
          "Linked ${TARGET_NAME} to Parquet::parquet_static (shared requested but not available)"
      )
    endif()
  endif()
endfunction()

function(need_test_deps)
  cpmaddpackage(NAME doctest GITHUB_REPOSITORY doctest/doctest VERSION 2.4.11)

  cpmaddpackage(
    NAME
    unity
    GITHUB_REPOSITORY
    ThrowTheSwitch/Unity
    VERSION
    2.6.0
    DOWNLOAD_ONLY
    YES)

  if(TARGET unity)
    add_library(unity_lib ALIAS unity)
  else()
    add_library(unity_lib STATIC ${unity_SOURCE_DIR}/src/unity.c)
    target_include_directories(unity_lib PUBLIC ${unity_SOURCE_DIR}/src)
  endif()
endfunction()

macro(check_std_filesystem)
  try_compile(
    DFTRACER_UTILS_HAS_STD_FILESYSTEM "${CMAKE_BINARY_DIR}/temp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/tests/has_filesystem.cpp"
    CMAKE_FLAGS ${CMAKE_CXX_FLAGS}
    LINK_LIBRARIES stdc++fs)
  if(DFTRACER_UTILS_HAS_STD_FILESYSTEM)
    message(STATUS "Compiler has std::filesystem support")
  else()
    message(
      STATUS
        "Compiler does not have std::filesystem support. Use gulrak::filesystem"
    )
  endif(DFTRACER_UTILS_HAS_STD_FILESYSTEM)
endmacro()

function(add_stdfs_if_needed TARGET)
  if(DFTRACER_UTILS_HAS_STD_FILESYSTEM)
    target_link_libraries(${TARGET} PRIVATE stdc++fs)
  endif()
endfunction()
