add_library(feedhandler_common INTERFACE)
add_library(feedhandler::common ALIAS feedhandler_common)

target_include_directories(feedhandler_common INTERFACE
        ${CMAKE_SOURCE_DIR}/include
)

find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
target_link_libraries(feedhandler_common INTERFACE spdlog::spdlog fmt::fmt)

add_subdirectory(config)
add_subdirectory(futu_quote)
add_subdirectory(runtime)
add_subdirectory(sinks)

add_executable(futu_feeder
        ${PROJECT_SOURCE_DIR}/src/main.cpp)

target_compile_options(futu_feeder PRIVATE -fno-PIE)
target_link_options(futu_feeder PRIVATE -no-pie)

target_link_libraries(futu_feeder
        PRIVATE
        feedhandler::common
        futu::quote
        feedhandler::runtime
        feedhandler::sinks
        config
)