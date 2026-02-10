add_executable(demo_qipc
        demo_qipc.cpp
)

target_include_directories(demo_qipc
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(demo_qipc
        PRIVATE
        momo::qcodec
)