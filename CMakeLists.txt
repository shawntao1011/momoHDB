cmake_minimum_required(VERSION 3.15)
project(momo_db LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_SOURCE_DIR}/cmake/futu_openapi.cmake)

find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)     # fallback to FindProtobuf.cmake
endif()
pkg_check_modules(RDKAFKACPP REQUIRED IMPORTED_TARGET rdkafka++)
pkg_check_modules(RDKAFKA   REQUIRED IMPORTED_TARGET rdkafka)

add_subdirectory(feedhandler)
add_subdirectory(kafka_pb_consumer)
