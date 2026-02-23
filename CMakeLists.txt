cmake_minimum_required(VERSION 3.15)
project(momo_hdb LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_SOURCE_DIR}/cmake/futu_openapi.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/kx_kdb.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/kafkax.cmake)

find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(PkgConfig REQUIRED)

pkg_check_modules(RDKAFKACPP REQUIRED IMPORTED_TARGET rdkafka++)
pkg_check_modules(RDKAFKA   REQUIRED IMPORTED_TARGET rdkafka)

# subprojects
add_subdirectory(momo_utils)
add_subdirectory(momo_feed)
add_subdirectory(momo_decoders)
