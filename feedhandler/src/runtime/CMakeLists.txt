add_library(sinks STATIC
        Dispatcher.cpp
)

target_include_directories(sinks
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)
