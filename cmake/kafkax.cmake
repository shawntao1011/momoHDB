# cmake/kafkax_fetch.cmake
include(FetchContent)

if(POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif()

set(KAFKAX_GIT_TAG "v1.0.1" CACHE STRING "kafkax git tag/sha")

FetchContent_Declare(
    kafkax_src
    GIT_REPOSITORY https://github.com/shawntao1011/kafkax.git
    GIT_TAG        ${KAFKAX_GIT_TAG}
)

FetchContent_Populate(kafkax_src)

set(KAFKAX_SOURCE_DIR "${kafkax_src_SOURCE_DIR}" CACHE PATH "kafkax source dir")
message(STATUS "kafkax source: ${KAFKAX_SOURCE_DIR}")

# ---- 1) ABI header-only target ----
add_library(kafkax_abi INTERFACE)
add_library(kafkax::abi ALIAS kafkax_abi)
target_include_directories(kafkax_abi INTERFACE
        "${KAFKAX_SOURCE_DIR}/include"
)

# ---- 2) qipc static lib target ----
add_library(kafkax_qipc STATIC
        "${KAFKAX_SOURCE_DIR}/src/qipc/qipc_c.cpp"
)
add_library(kafkax::qipc ALIAS kafkax_qipc)

target_include_directories(kafkax_qipc PUBLIC
        "${KAFKAX_SOURCE_DIR}/include"
)
set_target_properties(kafkax_qipc PROPERTIES
        POSITION_INDEPENDENT_CODE ON
)