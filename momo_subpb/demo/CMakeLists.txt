add_executable(kfkpb_demo
        kfkpb_demo.cpp
)

target_include_directories(kfkpb_demo
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_options(kfkpb_demo PRIVATE -no-pie)

target_link_libraries(kfkpb_demo
        PRIVATE
        kfkpb_core
)