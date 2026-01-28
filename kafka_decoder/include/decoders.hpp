#pragma once

#include <string>
#include "momoDB_types.hpp"

bool decode_qot_update_basicquote(const void* data, std::size_t len, std::chrono::system_clock::time_point ingest_time, BasicQuoteBatch& out, std::string& err);

bool decode_qot_update_orderbook(const void* data, std::size_t len, std::chrono::system_clock::time_point ingest_time, OrderBookBatch& out, std::string& err);

bool decode_qot_update_ticker(const void* data, std::size_t len, std::chrono::system_clock::time_point ingest_time, TickerBatch& out, std::string& err);

bool decode_qot_update_kl1min(const void* data, std::size_t len, std::chrono::system_clock::time_point ingest_time, KL1MinBatch& out, std::string& err);
