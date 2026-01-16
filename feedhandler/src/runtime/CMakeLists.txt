add_library(runtime STATIC
        Dispatcher.cpp
)

target_include_directories(runtime
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)
