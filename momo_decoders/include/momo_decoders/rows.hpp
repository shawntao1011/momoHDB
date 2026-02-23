#pragma once

#include "kafkax/qipc/encode.hpp"
#include "kafkax/qipc/reflect.hpp"
#include <array>
#include <chrono>
#include <cstdint>
#include <string>

namespace momo::schema {

struct BasicQuoteRow {
    std::string sym;
    std::chrono::system_clock::time_point time;
    double spread{};
    double high{};
    double open{};
    double low{};
    double cur{};
    double lastclose{};
    std::int64_t volume{};
    double amount{};
    double turnoverrate{};
    double amplitude{};
    std::chrono::system_clock::time_point updtime;
};

inline constexpr std::array<kafkax::qipc::ColumnSpec<BasicQuoteRow>, 13>
    BASICQOT_COLS = {{
        KAFKAX_QIPC_COL_SYM(BasicQuoteRow, "sym", sym),
        KAFKAX_QIPC_COL_TSNS(BasicQuoteRow, "time", time),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "spread", spread),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "high", high),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "open", open),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "low", low),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "cur", cur),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "lastclose", lastclose),
        KAFKAX_QIPC_COL_I64(BasicQuoteRow, "volume", volume),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "amount", amount),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "turnoverrate", turnoverrate),
        KAFKAX_QIPC_COL_F64(BasicQuoteRow, "amplitude", amplitude),
        KAFKAX_QIPC_COL_TSNS(BasicQuoteRow, "updtime", updtime),
    }};

struct OrderBookRow {
    std::string sym;
    std::chrono::system_clock::time_point time;
    std::string side;
    std::int32_t level{};
    double price{};
    std::int64_t volume{};
    std::int32_t ordercount{};
};

inline constexpr std::array<kafkax::qipc::ColumnSpec<OrderBookRow>, 7>
    ORDERBOOK_COLS = {{
        KAFKAX_QIPC_COL_SYM(OrderBookRow, "sym", sym),
        KAFKAX_QIPC_COL_TSNS(OrderBookRow, "time", time),
        KAFKAX_QIPC_COL_SYM(OrderBookRow, "side", side),
        KAFKAX_QIPC_COL_I32(OrderBookRow, "level", level),
        KAFKAX_QIPC_COL_F64(OrderBookRow, "price", price),
        KAFKAX_QIPC_COL_I64(OrderBookRow, "volume", volume),
        KAFKAX_QIPC_COL_I32(OrderBookRow, "ordercount", ordercount),
    }};

struct TickerRow {
    std::string sym;
    std::chrono::system_clock::time_point time;
    std::int32_t direction{};
    double price{};
    std::int64_t volume{};
    double amount{};
    std::chrono::system_clock::time_point msgTime;
    std::chrono::system_clock::time_point recvTime;
};

inline constexpr std::array<kafkax::qipc::ColumnSpec<TickerRow>, 8>
    TICKER_COLS = {{
        KAFKAX_QIPC_COL_SYM(TickerRow, "sym", sym),
        KAFKAX_QIPC_COL_TSNS(TickerRow, "time", time),
        KAFKAX_QIPC_COL_I32(TickerRow, "direction", direction),
        KAFKAX_QIPC_COL_F64(TickerRow, "price", price),
        KAFKAX_QIPC_COL_I64(TickerRow, "volume", volume),
        KAFKAX_QIPC_COL_F64(TickerRow, "amount", amount),
        KAFKAX_QIPC_COL_TSNS(TickerRow, "msgTime", msgTime),
        KAFKAX_QIPC_COL_TSNS(TickerRow, "recvTime", recvTime),
    }};

struct KL1MinRow {
    std::string sym;
    std::chrono::system_clock::time_point time;
    double high{};
    double open{};
    double low{};
    double close{};
    double lastclose{};
    std::int64_t volume{};
    double amount{};
    double turnoverrate{};
    double pe{};
    double changerate{};
    std::chrono::system_clock::time_point recvtime;
    std::chrono::system_clock::time_point tstime;
};

inline constexpr std::array<kafkax::qipc::ColumnSpec<KL1MinRow>, 14>
    KL1MIN_COLS = {{
        KAFKAX_QIPC_COL_SYM(KL1MinRow, "sym", sym),
        KAFKAX_QIPC_COL_TSNS(KL1MinRow, "time", time),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "high", high),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "open", open),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "low", low),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "close", close),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "lastclose", lastclose),
        KAFKAX_QIPC_COL_I64(KL1MinRow, "volume", volume),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "amount", amount),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "turnoverrate", turnoverrate),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "pe", pe),
        KAFKAX_QIPC_COL_F64(KL1MinRow, "changerate", changerate),
        KAFKAX_QIPC_COL_TSNS(KL1MinRow, "recvtime", recvtime),
        KAFKAX_QIPC_COL_TSNS(KL1MinRow, "tstime", tstime),
    }};

} // namespace momo::schema
