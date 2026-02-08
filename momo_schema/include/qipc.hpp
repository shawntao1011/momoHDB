#pragma once
#include <string>
#include <unordered_map>

#include "schema.hpp"

namespace qipc {

    /**
     * Application-level message envelope.
     *
     * Transport-agnostic:
     *  - can come from Kafka
     *  - can come from IPC / SHM / replay
     */
    struct QipcEnvelope {
        // ---- routing / transport metadata ----
        std::string topic;          // e.g. "marketdata.basicquote"
        std::int32_t partition{-1}; // kafka partition (or -1 if N/A)
        std::string key;            // e.g. "HK.00700"

        // ---- protocol / semantic metadata ----
        // Typical keys:
        //   event_type = BasicQuote
        //   schema     = basicquote.v2
        //   source     = futu
        //   ingest_ts  = 1700000000123
        std::unordered_map<std::string, std::string> headers;

        // ---- payload ----
        // protobuf / flatbuffer / raw bytes
        std::vector<std::uint8_t> value;

        // ---- helpers ----
        bool empty() const noexcept {
            return value.empty();
        }

        std::string_view header(std::string_view k) const noexcept {
            auto it = headers.find(std::string(k));
            if (it == headers.end()) {
                return {};
            }
            return it->second;
        }
    };

    bool serialize_basicquote_qipc(const BasicQuoteBatch& batch, std::vector<std::uint8_t>& out_kbytes);
    bool serialize_orderbook_qipc(const OrderBookBatch& batch, std::vector<std::uint8_t>& out_kbytes);
    bool serialize_ticker_qipc(const TickerBatch& batch, std::vector<std::uint8_t>& out_kbytes);
    bool serialize_kl1min_qipc(const KL1MinBatch& batch, std::vector<std::uint8_t>& out_kbytes);
} // namespace qipc