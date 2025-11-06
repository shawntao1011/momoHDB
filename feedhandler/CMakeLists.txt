cmake_minimum_required(VERSION 3.15)
project(futu_feedhandler_c NONE)

# --- futu cpp api download setting---
set(FUTU_URL "https://www.futunn.com/download/fetch-lasted-link?name=openapi-cpp")
set(FUTU_ROOT "${CMAKE_SOURCE_DIR}/third_party/futu_openapi")       # unzip dest
set(FUTU_ZIP  "${CMAKE_BINARY_DIR}/_downloads/futu-openapi.zip")    # raw downloads
set(FUTU_TMP   "${CMAKE_BINARY_DIR}/_stage/futu_openapi_unpack")    # temp unzip dir
set(FUTU_STAMP "${FUTU_ROOT}/.unpacked")                            # flagfile for dependency

# --- download + unzip ---
add_custom_command(
    OUTPUT "${FUTU_STAMP}"
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${FUTU_ROOT}" "${FUTU_TMP}" "${CMAKE_BINARY_DIR}/_downloads"

    # download
    COMMAND ${CMAKE_COMMAND} -DURL=${FUTU_URL}
                             -DOUT=${FUTU_ZIP}
                             -P ${CMAKE_SOURCE_DIR}/cmake/_download.cmake

    # unzip
    COMMAND ${CMAKE_COMMAND} -E chdir "${FUTU_TMP}" ${CMAKE_COMMAND} -E tar xvf "${FUTU_ZIP}"

    COMMAND ${CMAKE_COMMAND} -DDEST=${FUTU_ROOT}
                             -DSRC=${FUTU_TMP}
                             -P ${CMAKE_SOURCE_DIR}/cmake/_flatten_topdir.cmake

    # flagfile
    COMMAND ${CMAKE_COMMAND} -E touch "${FUTU_STAMP}"
    COMMENT "Downloading & extracting Futu OpenAPI to ${FUTU_ROOT}"
    VERBATIM
)

add_custom_target(fetch_futu ALL DEPENDS "${FUTU_STAMP}")