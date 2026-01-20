add_library(config STATIC
        SubscriptionLoader.cpp
        ConfigLoader.cpp
)

target_include_directories(config
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(config
    PUBLIC futu_openapi
)