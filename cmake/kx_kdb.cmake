# cmake/deps/kx_kdb.cmake
# Usage:
#   include(cmake/deps/kx_kdb.cmake)
# Then:
#   target_link_libraries(your_target PRIVATE kx::capi)
#
# Output layout:
#   third_party/kx/include/k.h
#   third_party/kx/lib/l64/c.o
#   third_party/kx/lib/l32/c.o (if needed)
#
# Notes:
# - downloads https://github.com/KxSystems/kdb as a tarball (no git needed)
# - configure-time download/extract/copy (same pattern as many "deps/*.cmake")

if (TARGET kx::capi)
    # already set up
    return()
endif()

if (NOT CMAKE_SOURCE_DIR)
    message(FATAL_ERROR "[kx] CMAKE_SOURCE_DIR not set")
endif()

set(KX_ROOT "${CMAKE_SOURCE_DIR}/third_party/kx")
set(KX_DL   "${KX_ROOT}/_dl")
set(KX_SRC  "${KX_ROOT}/_src")
set(KX_STG  "${KX_ROOT}/_src/kdb-master")

file(MAKE_DIRECTORY "${KX_ROOT}/include")
file(MAKE_DIRECTORY "${KX_ROOT}/lib/l32")
file(MAKE_DIRECTORY "${KX_ROOT}/lib/l64")
file(MAKE_DIRECTORY "${KX_DL}")
file(MAKE_DIRECTORY "${KX_SRC}")

# GitHub tarball for master branch
set(KX_KDB_URL "https://codeload.github.com/KxSystems/kdb/tar.gz/refs/heads/master")
set(KX_TAR     "${KX_DL}/kdb-master.tar.gz")

# Download if missing
if (NOT EXISTS "${KX_TAR}")
    message(STATUS "[kx] downloading kdb tarball...")
    file(DOWNLOAD "${KX_KDB_URL}" "${KX_TAR}" SHOW_PROGRESS STATUS _dlstat)
    list(GET _dlstat 0 _code)
    if (NOT _code EQUAL 0)
        list(GET _dlstat 1 _msg)
        message(FATAL_ERROR "[kx] download failed: ${_msg}")
    endif()
else()
    message(STATUS "[kx] tarball cached: ${KX_TAR}")
endif()

# Extract if missing
if (NOT EXISTS "${KX_STG}/c/c/k.h")
    message(STATUS "[kx] extracting kdb tarball...")
    file(REMOVE_RECURSE "${KX_STG}")
    execute_process(
            COMMAND ${CMAKE_COMMAND} -E tar xzf "${KX_TAR}"
            WORKING_DIRECTORY "${KX_SRC}"
            RESULT_VARIABLE _tar_rc
    )
    if (NOT _tar_rc EQUAL 0)
        message(FATAL_ERROR "[kx] extract failed with code ${_tar_rc}")
    endif()

    if (NOT EXISTS "${KX_STG}/c/c/k.h")
        message(FATAL_ERROR "[kx] unexpected tar layout: cannot find ${KX_STG}/c/k.h")
    endif()
else()
    message(STATUS "[kx] already extracted: ${KX_STG}")
endif()

# Copy k.h + c.o to stable paths (always overwrite to keep up-to-date)
file(COPY "${KX_STG}/c/c/k.h" DESTINATION "${KX_ROOT}/include")
if (EXISTS "${KX_STG}/l64/c.o")
    file(COPY "${KX_STG}/l64/c.o" DESTINATION "${KX_ROOT}/lib/l64")
endif()
if (EXISTS "${KX_STG}/l32/c.o")
    file(COPY "${KX_STG}/l32/c.o" DESTINATION "${KX_ROOT}/lib/l32")
endif()

# Validate outputs
set(KX_KH "${KX_ROOT}/include/k.h")
if (NOT EXISTS "${KX_KH}")
    message(FATAL_ERROR "[kx] missing: ${KX_KH}")
endif()

# Pick c.o for current build word size (Linux)
if (CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(KX_CO "${KX_ROOT}/lib/l64/c.o")
else()
    set(KX_CO "${KX_ROOT}/lib/l32/c.o")
endif()

if (NOT EXISTS "${KX_CO}")
    message(FATAL_ERROR "[kx] missing: ${KX_CO}")
endif()

# Create imported target (single point of truth)
add_library(kx::capi INTERFACE IMPORTED)
target_include_directories(kx::capi INTERFACE "${KX_ROOT}/include")
# c.o is an object file; link it like a "library"
target_link_libraries(kx::capi INTERFACE "${KX_CO}")

message(STATUS "[kx] ready: include=${KX_ROOT}/include, co=${KX_CO}")
