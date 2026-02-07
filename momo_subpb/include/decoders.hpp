#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

// All decoders run in C++ worker threads.
// Input: Kafka key/value bytes + ts_ms
// Output: kbytes (to be wrapped as KG in q thread).
// Return: true=success, false=fail (err_msg filled)
bool decode_basicquote_kbytes(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ms,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
);

bool decode_orderbook_kbytes(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ms,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
);

bool decode_ticker_kbytes(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ms,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
);

bool decode_kl1min_kbytes(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ms,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
);
