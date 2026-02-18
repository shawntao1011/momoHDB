#pragma once
#include <string>
#include <cstdint>
#include <vector>

enum class MsgKind : uint16_t {
    BasicQuote,
    OrderBook,
    Ticker,
    KL1Min,
    RT,
    Broker
};

struct Envelope {
    MsgKind kind;
    std::string symbol; // key
    std::vector<uint8_t> payload; // bytes
    int64_t ingest_time_ms; // ingest time
};
