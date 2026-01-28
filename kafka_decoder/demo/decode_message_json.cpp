#include <iostream>
#include <vector>
#include "decoders.hpp"
#include "print.hpp"
#include "json_extract.hpp"

enum class MsgType { Ticker, OrderBook, BasicQuote, Kline1M };
static bool parse_type(std::string_view s, MsgType& out) {
    if (s == "ticker") { out = MsgType::Ticker; return true; }
    if (s == "orderbook") { out = MsgType::OrderBook; return true; }
    if (s == "basicquote") { out = MsgType::BasicQuote; return true; }
    if (s == "kline1m") { out = MsgType::Kline1M; return true; }
    return false;
}

int main(int argc, char** argv) {
    if (argc < 4 || std::string_view(argv[1]) != "--type") {
        std::cerr << "Usage: decode_message_json --type <ticker|orderbook|basicquote|kline1m> message.json\n";
        return 1;
    }

    MsgType type;
    if (!parse_type(argv[2], type)) {
        std::cerr << "unknown type: " << argv[2] << "\n";
        return 1;
    }
    std::string json_path = argv[3];

    // 1. extract raw bytes
    std::vector<uint8_t> payload;
    std::string key, err;
    if (!json_extract::extract_kafka_message(json_path, payload, key, err)) {
        std::cerr << "failed to extract kafka message\n";
        return 1;
    }

    switch (type) {
        case MsgType::Ticker: {
            TickerBatch batch;
            if (!decode_qot_update_ticker(payload.data(), payload.size(), batch, err)) {
                std::cerr << "decode failed: " << err << "\n";
                return 1;
            }
            pretty_print(batch, std::cout, 200);
            break;
        }
        case MsgType::OrderBook: {
            OrderBookBatch batch;
            if (!decode_qot_update_orderbook(payload.data(), payload.size(), batch, err)) {
                std::cerr << "decode failed: " << err << "\n";
                return 1;
            }
            pretty_print(batch, std::cout, 200);
            break;
        }
        case MsgType::BasicQuote: {
            BasicQuoteBatch batch;
            if (!decode_qot_update_basicquote(payload.data(), payload.size(), batch, err)) {
                std::cerr << "decode failed: " << err << "\n";
                return 1;
            }
            pretty_print(batch, std::cout, 200);
            break;
        }
        case MsgType::Kline1M: {
            KL1MinBatch batch;
            if (!decode_qot_update_kl1min(payload.data(), payload.size(), batch, err)) {
                std::cerr << "decode failed: " << err << "\n";
                return 1;
            }
            pretty_print(batch, std::cout, 200);
            break;
        }
    }

    return 0;
}
