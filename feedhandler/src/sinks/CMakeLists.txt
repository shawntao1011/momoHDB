find_package(PkgConfig REQUIRED)

pkg_check_modules(RDKAFKA    REQUIRED IMPORTED_TARGET rdkafka)
pkg_check_modules(RDKAFKACPP REQUIRED IMPORTED_TARGET rdkafka++)

add_library(feedhandler_sinks
        RedPandaSink.cpp
)
add_library(feedhandler::sinks ALIAS feedhandler_sinks)

target_include_directories(feedhandler_sinks
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(feedhandler_sinks
    PUBLIC
        PkgConfig::RDKAFKACPP
        PkgConfig::RDKAFKA
)