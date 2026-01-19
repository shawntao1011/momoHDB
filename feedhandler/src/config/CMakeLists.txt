add_library(config STATIC
        SubscriptionLoader.cpp
)

target_include_directories(config
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)
