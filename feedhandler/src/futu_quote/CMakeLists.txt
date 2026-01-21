add_library(futu_quote
        ${PROJECT_SOURCE_DIR}/src/futu_quote/FutuQuoteSession.cpp
        ${PROJECT_SOURCE_DIR}/src/futu_quote/SubscriptionManager.cpp
)
add_library(futu::quote ALIAS futu_quote)

find_package(yaml-cpp REQUIRED)

target_include_directories(futu_quote
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(futu_quote
        PUBLIC futu_openapi
        PRIVATE yaml-cpp
)
