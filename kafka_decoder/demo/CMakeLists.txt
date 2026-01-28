add_executable(decode_demo
        decode_message_json.cpp
)

target_include_directories(decode_demo
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_options(decode_demo PRIVATE -no-pie)

target_link_libraries(decode_demo
        PRIVATE
        momo::kafka_decoder
)