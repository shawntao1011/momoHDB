#pragma once

#include <string>
#include "momoDB_types.hpp"

bool decode_qot_update_basicquote(const void* data, std::size_t len, int64_t ingest_time, BasicQuoteBatch& out, std::string& err);

bool decode_qot_update_orderbook(const void* data, std::size_t len, int64_t ingest_time, OrderBookBatch& out, std::string& err);

bool decode_qot_update_ticker(const void* data, std::size_t len, int64_t ingest_time, TickerBatch& out, std::string& err);

bool decode_qot_update_kl1min(const void* data, std::size_t len, int64_t ingest_time, KL1MinBatch& out, std::string& err);
