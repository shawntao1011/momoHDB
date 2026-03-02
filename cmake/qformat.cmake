# cmake/qformat_fetch.cmake
include(FetchContent)

set(QFORMAT_GIT_TAG "v1.0.0" CACHE STRING "qformat git tag/sha")

FetchContent_Declare(
        qformat_src
        GIT_REPOSITORY https://github.com/shawntao1011/qformat.git
        GIT_TAG        ${QFORMAT_GIT_TAG}
)

set(QFORMAT_BUILD_EXAMPLES OFF CACHE BOOL "Build qformat examples" FORCE)

FetchContent_MakeAvailable(qformat_src)

if(TARGET qformat AND NOT TARGET qformat::qformat)
    add_library(qformat::qformat ALIAS qformat)
endif()