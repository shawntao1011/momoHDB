add_library(config
        ${PROJECT_SOURCE_DIR}/src/config/SubscriptionLoader.cpp
        ${PROJECT_SOURCE_DIR}/src/config/ConfigLoader.cpp
)

target_include_directories(config
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(config
    PUBLIC futu_openapi
)