#pragma once

#include <string>
#include "momoDB_types.hpp"

bool decode_qot_update_basicquote(const void* data, std::size_t len, BasicQuoteBatch& out, std::string& err);

bool decode_qot_update_orderbook(const void* data, std::size_t len, OrderBookBatch& out, std::string& err);

bool decode_qot_update_ticker(const void* data, std::size_t len, TickerBatch& out, std::string& err);

bool decode_qot_update_kl1min(const void* data, std::size_t len, KL1MinBatch& out, std::string& err);
