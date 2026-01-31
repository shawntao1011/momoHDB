#pragma once
#include <string>

#include "schema.hpp"

std::string serialize_basicquote_to_qbytes(const BasicQuoteBatch& batch);

std::string serialize_orderbook_to_qbytes(const OrderBookBatch& batch);

std::string serialize_ticker_to_qbytes(const TickerBatch& batch);

std::string serialize_kl1min_to_qbytes(const KL1MinBatch& batch);