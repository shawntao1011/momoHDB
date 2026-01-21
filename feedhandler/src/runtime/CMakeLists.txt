add_library(feedhandler_runtime
        ${PROJECT_SOURCE_DIR}/src/runtime/Dispatcher.cpp
)

add_library(feedhandler::runtime ALIAS feedhandler_runtime)

target_include_directories(feedhandler_runtime
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)
