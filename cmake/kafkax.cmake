# cmake/kafkax_fetch.cmake
include(FetchContent)

set(KAFKAX_GIT_TAG "v1.1.0" CACHE STRING "kafkax git tag/sha")

FetchContent_Declare(
    kafkax_src
    GIT_REPOSITORY https://github.com/shawntao1011/kafkax.git
    GIT_TAG        ${KAFKAX_GIT_TAG}
)

FetchContent_MakeAvailable(kafkax_src)

# ---- ABI header-only target ----
if(NOT TARGET kafkax::abi)
    add_library(kafkax_abi INTERFACE)
    add_library(kafkax::abi ALIAS kafkax_abi)

    target_include_directories(kafkax_abi INTERFACE
            $<BUILD_INTERFACE:${kafkax_src_SOURCE_DIR}/include>
    )
endif()

message(STATUS "kafkax source: ${kafkax_src_SOURCE_DIR}")