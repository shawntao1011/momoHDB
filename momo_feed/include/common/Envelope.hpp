#pragma once
#include <string>
#include <cstdint>

struct Envelope {
    std::string topic;      // futu.quote.raw
    std::string key;        // symbol
    std::string payload;    // bytes
    int64_t ingest_time_ms; // ingest time
};
