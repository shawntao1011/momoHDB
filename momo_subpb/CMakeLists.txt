add_library(kfkpb_core SHARED
        src/kfkpb_core.cpp
        src/decoders.cpp
        src/print.cpp
)
target_include_directories(kfkpb_core
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(kfkpb_core
        PUBLIC
            futu_pb
            PkgConfig::RDKAFKA
            futu_openapi_protobuf
            momo::schema
            momo_common
)

add_library(kfkpb SHARED
        src/kfkpb.cpp
)
target_link_libraries(kfkpb
        PUBLIC
            kfkpb_core
        PRIVATE
            kx::capi
)
set_target_properties(kfkpb PROPERTIES OUTPUT_NAME "kfkpb")

add_subdirectory(demo)