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
    target_link_libraries(${t} PUBLIC momo_common)
endforeach()

target_link_libraries(feedhandler_config PUBLIC yaml-cpp futu_openapi)
target_link_libraries(feedhandler_sinks  PUBLIC PkgConfig::RDKAFKACPP PkgConfig::RDKAFKA)
target_link_libraries(feedhandler_futu_core PUBLIC futu_openapi)

add_executable(momo_feeder
        ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
        $<TARGET_OBJECTS:feedhandler_config>
        $<TARGET_OBJECTS:feedhandler_runtime>
        $<TARGET_OBJECTS:feedhandler_sinks>
        $<TARGET_OBJECTS:feedhandler_futu_core>
)

target_compile_options(momo_feeder PRIVATE -fno-PIE)
target_link_options(momo_feeder PRIVATE -no-pie)

target_link_libraries(momo_feeder PRIVATE momo_common yaml-cpp fmt futu_openapi PkgConfig::RDKAFKACPP PkgConfig::RDKAFKA)

target_include_directories(momo_feeder PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# install
include(GNUInstallDirs)
set(MOMO_LIBDIR "${CMAKE_INSTALL_LIBDIR}/momo_feedhandler")

install(TARGETS momo_feeder
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

set_target_properties(momo_feeder PROPERTIES
        BUILD_RPATH "${CMAKE_BINARY_DIR}"
        INSTALL_RPATH "\$ORIGIN/../${MOMO_LIBDIR}"
)

install(FILES
        "${FUTU_LIBDIR}/libFTAPIChannel.so"
        DESTINATION ${MOMO_LIBDIR})