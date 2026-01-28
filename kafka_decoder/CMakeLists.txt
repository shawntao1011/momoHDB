add_library(kafka_decoder
        src/decoders.cpp
        src/print.cpp
)

add_library(momo::kafka_decoder ALIAS kafka_decoder)

target_include_directories(kafka_decoder
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(kafka_decoder PUBLIC futu_openapi)

add_subdirectory(demo)