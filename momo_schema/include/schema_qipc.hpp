#pragma once
#include <string>

#include "schema.hpp"

namespace qipc {
    std::string serialize_basicquote_qipc(const BasicQuoteBatch& batch);

    std::string serialize_orderbook_qipc(const OrderBookBatch& batch);

    std::string serialize_ticker_qipc(const TickerBatch& batch);

    std::string serialize_kl1min_qipc(const KL1MinBatch& batch);
} //namespace qipc