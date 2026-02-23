add_library(momo_utils INTERFACE)

target_include_directories(momo_utils INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(momo_utils INTERFACE
        futu_openapi_protobuf
)