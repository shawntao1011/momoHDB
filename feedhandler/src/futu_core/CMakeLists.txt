add_library(futu_core STATIC
        FutuQuoteSession.cpp
        SubscriptionManager.cpp
)

target_include_directories(futu_core
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(futu_core
    futu_openapi
)
