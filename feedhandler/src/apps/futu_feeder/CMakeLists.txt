add_executable(futu_feeder main.cpp)

target_compile_options(futu_feeder PRIVATE -fno-PIE)
target_link_options(futu_feeder PRIVATE -no-pie)

target_link_libraries(futu_feeder 
    PRIVATE 
        futu_core
)
