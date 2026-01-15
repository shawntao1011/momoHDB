add_library(futu_core STATIC
        FutuQuoteSession.cpp
        SubscriptionManager.cpp
        ../config/YamlSubscribeConfigLoader.cpp
)

find_package(yaml-cpp REQUIRED)

target_include_directories(futu_core
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(futu_core
        PUBLIC futu_openapi
        PRIVATE yaml-cpp
)
