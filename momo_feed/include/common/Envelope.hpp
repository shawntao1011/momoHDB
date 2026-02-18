#pragma once
#include <string>
#include <cstdint>
#include <vector>

enum class MsgKind : int {
    Unknown    = -1,
    BasicQuote = 0,
    OrderBook  = 1,
    Ticker     = 2,
    KL1Min     = 3,
    RT         = 4,
    Broker     = 5,
};

struct Envelope {
    MsgKind kind;
    std::string symbol; // key
    std::vector<uint8_t> payload; // bytes
    int64_t ingest_time_ms; // ingest time
};
