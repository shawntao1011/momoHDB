add_library(kfkpb SHARED
        src/kfkpb.cpp
        src/kfkpb_core.cpp
        src/decoders.cpp
        src/print.cpp
)

target_include_directories(kfkpb
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(kfkpb PUBLIC futu_pb PkgConfig::RDKAFKA)
set_target_properties(kfkpb PROPERTIES OUTPUT_NAME "kfkpb")

#add_subdirectory(demo)