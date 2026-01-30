#pragma once

#include <string>
#include "momoDB_types.hpp"

bool decode_qot_update_basicquote(const void* data, std::size_t len, int64_t ingest_time, BasicQuoteBatch& out, std::string& err);
std::string serialize_basicquotebatch(const BasicQuoteBatch& batch);

bool decode_qot_update_orderbook(const void* data, std::size_t len, int64_t ingest_time, OrderBookBatch& out, std::string& err);
std::string serialize_orderbookbatch(const OrderBookBatch& batch);

bool decode_qot_update_ticker(const void* data, std::size_t len, int64_t ingest_time, TickerBatch& out, std::string& err);
std::string serialize_tickerbatch(const TickerBatch& batch);

bool decode_qot_update_kl1min(const void* data, std::size_t len, int64_t ingest_time, KL1MinBatch& out, std::string& err);
std::string serialize_kl1minbatch(const KL1MinBatch& batch);