add_library(momo_common INTERFACE)

target_include_directories(momo_common
        INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(momo_common
        INTERFACE
        fmt::fmt
)