add_library(feedhandler_config OBJECT
        ${CMAKE_CURRENT_SOURCE_DIR}/src/config/ConfigLoader.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/config/SubscriptionLoader.cpp
)

add_library(feedhandler_runtime OBJECT
        ${CMAKE_CURRENT_SOURCE_DIR}/src/runtime/Dispatcher.cpp
)

add_library(feedhandler_sinks OBJECT
        ${CMAKE_CURRENT_SOURCE_DIR}/src/sinks/RedPandaSink.cpp
)

add_library(feedhandler_futu_core OBJECT
        ${CMAKE_CURRENT_SOURCE_DIR}/src/futu_quote/FutuQuoteSession.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/futu_quote/SubscriptionManager.cpp
)

foreach(t feedhandler_config feedhandler_runtime feedhandler_sinks feedhandler_futu_core)
    target_include_directories(${t} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
endforeach()

target_link_libraries(feedhandler_config PUBLIC yaml-cpp futu_openapi)
target_link_libraries(feedhandler_sinks  PUBLIC PkgConfig::RDKAFKACPP PkgConfig::RDKAFKA)
target_link_libraries(feedhandler_futu_core PUBLIC futu_openapi)

add_executable(futu_feeder
        ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
        $<TARGET_OBJECTS:feedhandler_config>
        $<TARGET_OBJECTS:feedhandler_runtime>
        $<TARGET_OBJECTS:feedhandler_sinks>
        $<TARGET_OBJECTS:feedhandler_futu_core>
)

target_compile_options(futu_feeder PRIVATE -fno-PIE)
target_link_options(futu_feeder PRIVATE -no-pie)

target_link_libraries(futu_feeder PRIVATE yaml-cpp fmt futu_openapi PkgConfig::RDKAFKACPP PkgConfig::RDKAFKA)

target_include_directories(futu_feeder PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)


