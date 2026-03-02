add_library(futu_quote_decoders SHARED
        src/exports.cpp
        src/decode_basicqot.cpp
        src/decode_orderbook.cpp
        src/decode_ticker.cpp
        src/decode_kl1min.cpp
)

set_target_properties(futu_quote_decoders PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        POSITION_INDEPENDENT_CODE ON
        OUTPUT_NAME "futu_quote_decoders"
)

target_include_directories(futu_quote_decoders PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(futu_quote_decoders PRIVATE
        kafkax::abi
        qformat::qformat
        momo_utils
        futu_quote_schema
        futu_openapi_protobuf
)