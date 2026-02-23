add_executable(momo_feeder
        ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/src/config/ConfigLoader.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/config/SubscriptionLoader.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/src/runtime/Dispatcher.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/runtime/TopicRegistry.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/src/sinks/RedPandaSink.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/src/futu_quote/FutuQuoteSession.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/futu_quote/SubscriptionManager.cpp
)

target_include_directories(momo_feeder PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(momo_feeder PRIVATE
        yaml-cpp
        fmt
        momo_utils
        futu_openapi

        PkgConfig::RDKAFKACPP
        PkgConfig::RDKAFKA
)

target_compile_options(momo_feeder PRIVATE -fno-PIE)
target_link_options(momo_feeder PRIVATE -no-pie)

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
