cmake_minimum_required(VERSION 3.15)
project(futu_feedhandler_c LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_SOURCE_DIR}/cmake/futu_openapi.cmake)

add_subdirectory(src)

