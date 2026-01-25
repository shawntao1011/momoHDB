# cmake/desp/futu_openapi.cmake

# --- futu cpp api download setting---
set(FUTU_URL "https://www.futunn.com/download/fetch-lasted-link?name=openapi-cpp")
set(FUTU_ROOT "${CMAKE_SOURCE_DIR}/third_party/futu_openapi")       # unzip dest
set(FUTU_ZIP  "${CMAKE_BINARY_DIR}/_downloads/futu-openapi.zip")    # raw downloads
set(FUTU_TMP   "${CMAKE_BINARY_DIR}/_stage/futu_openapi_unpack")    # temp unzip dir
set(FUTU_STAMP "${CMAKE_BINARY_DIR}/_stamp/futu_openapi.stamp")     # flagfile for dependency

# --- download + unzip ---
add_custom_command(
    OUTPUT "${FUTU_STAMP}"
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    COMMAND ${CMAKE_COMMAND} -E make_directory 
            "${FUTU_ROOT}"
            "${FUTU_TMP}"
            "${CMAKE_BINARY_DIR}/_downloads"
            "${CMAKE_BINARY_DIR}/_stamp"

    # download
    COMMAND ${CMAKE_COMMAND} 
            -DURL=${FUTU_URL}
            -DOUT=${FUTU_ZIP}
            -P ${CMAKE_SOURCE_DIR}/cmake/_download.cmake

    # unzip
    COMMAND ${CMAKE_COMMAND} -E chdir "${FUTU_TMP}" 
            ${CMAKE_COMMAND} -E tar xvf "${FUTU_ZIP}"

    COMMAND ${CMAKE_COMMAND} 
            -DDEST=${FUTU_ROOT}
            -DSRC=${FUTU_TMP}
            -P ${CMAKE_SOURCE_DIR}/cmake/_flatten_topdir.cmake

    # flagfile
    COMMAND ${CMAKE_COMMAND} -E touch "${FUTU_STAMP}"
    COMMENT "Downloading & extracting Futu OpenAPI to ${FUTU_ROOT}"
    VERBATIM
)

add_custom_target(fetch_futu ALL DEPENDS "${FUTU_STAMP}")


set(FUTU_LIBDIR "${FUTU_ROOT}/Bin/Ubuntu16.04")
set(FUTU_INCDIR "${FUTU_ROOT}/Include")

if(NOT EXISTS "${FUTU_INCDIR}")
  message(FATAL_ERROR "Futu OpenAPI headers not found under ${FUTU_ROOT}. Please check unzip/flatten result.")
endif()

add_library(futu_openapi_ftapi STATIC IMPORTED GLOBAL)
set_target_properties(futu_openapi_ftapi PROPERTIES
  IMPORTED_LOCATION "${FUTU_LIBDIR}/libFTAPI.a"
  INTERFACE_INCLUDE_DIRECTORIES "${FUTU_INCDIR}"
)

add_library(futu_openapi_channel SHARED IMPORTED GLOBAL)
set_target_properties(futu_openapi_channel PROPERTIES
  IMPORTED_LOCATION "${FUTU_LIBDIR}/libFTAPIChannel.so"
  INTERFACE_INCLUDE_DIRECTORIES "${FUTU_INCDIR}"
)

add_library(futu_openapi_protobuf STATIC IMPORTED GLOBAL)
set_target_properties(futu_openapi_protobuf PROPERTIES
  IMPORTED_LOCATION "${FUTU_LIBDIR}/libprotobuf.a"
  INTERFACE_INCLUDE_DIRECTORIES "${FUTU_INCDIR}"
)

add_library(futu_openapi INTERFACE IMPORTED GLOBAL)
target_link_libraries(futu_openapi INTERFACE
  futu_openapi_ftapi
  futu_openapi_channel
  futu_openapi_protobuf
)

add_dependencies(futu_openapi_ftapi fetch_futu)
add_dependencies(futu_openapi_channel fetch_futu)
add_dependencies(futu_openapi_protobuf fetch_futu)
add_dependencies(futu_openapi fetch_futu)
