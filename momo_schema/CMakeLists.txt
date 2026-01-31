add_library(momo_schema INTERFACE)

add_library(momo::schema ALIAS momo_schema)

target_include_directories(momo_schema
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

add_library(momo_qcodec STATIC
        src/qcodec.cpp
)
add_library(momo::qcodec ALIAS momo_qcodec)

target_include_directories(momo_qcodec
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(momo_qcodec
        PUBLIC
        momo::schema
)