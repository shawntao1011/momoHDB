add_library(feedhandler_common INTERFACE)
add_library(feedhandler::common ALIAS feedhandler_common)

target_include_directories(feedhandler_common INTERFACE
        ${CMAKE_SOURCE_DIR}/include
)

find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
target_link_libraries(feedhandler_common INTERFACE spdlog::spdlog fmt::fmt)

add_subdirectory(config)
add_subdirectory(futu_core)
add_subdirectory(runtime)
add_subdirectory(sinks)
add_subdirectory(apps)
