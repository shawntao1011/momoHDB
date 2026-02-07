#pragma once
#include <string>

#include "schema.hpp"

namespace qipc {
    bool serialize_basicquote_qipc(const BasicQuoteBatch& batch, std::vector<std::uint8_t>& out_kbytes);

    bool serialize_orderbook_qipc(const OrderBookBatch& batch, std::vector<std::uint8_t>& out_kbytes);

    bool serialize_ticker_qipc(const TickerBatch& batch, std::vector<std::uint8_t>& out_kbytes);

    bool serialize_kl1min_qipc(const KL1MinBatch& batch, std::vector<std::uint8_t>& out_kbytes);
} //namespace qipc