#pragma once
#include <string>
#include <cstdint>

struct Envelope {
    std::string topic;      // futu.quote.raw
    std::string key;        // symbol
    std::string payload;    // bytes
    std::int64_t ts_ns{0};  // ingest time
};
