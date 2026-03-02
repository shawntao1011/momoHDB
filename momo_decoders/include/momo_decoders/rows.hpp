#pragma once

#include "qformat/qipc/encode.hpp"
#include "qformat/qipc//reflect.hpp"
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

inline constexpr std::array<qformat::qipc::ColumnSpec<BasicQuoteRow>, 13>
    BASICQOT_COLS = {{
        QFORMAT_QIPC_COL_SYM(BasicQuoteRow, "sym", sym),
        QFORMAT_QIPC_COL_TSNS(BasicQuoteRow, "time", time),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "spread", spread),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "high", high),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "open", open),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "low", low),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "cur", cur),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "lastclose", lastclose),
        QFORMAT_QIPC_COL_I64(BasicQuoteRow, "volume", volume),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "amount", amount),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "turnoverrate", turnoverrate),
        QFORMAT_QIPC_COL_F64(BasicQuoteRow, "amplitude", amplitude),
        QFORMAT_QIPC_COL_TSNS(BasicQuoteRow, "updtime", updtime),
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

inline constexpr std::array<qformat::qipc::ColumnSpec<OrderBookRow>, 7>
    ORDERBOOK_COLS = {{
        QFORMAT_QIPC_COL_SYM(OrderBookRow, "sym", sym),
        QFORMAT_QIPC_COL_TSNS(OrderBookRow, "time", time),
        QFORMAT_QIPC_COL_SYM(OrderBookRow, "side", side),
        QFORMAT_QIPC_COL_I32(OrderBookRow, "level", level),
        QFORMAT_QIPC_COL_F64(OrderBookRow, "price", price),
        QFORMAT_QIPC_COL_I64(OrderBookRow, "volume", volume),
        QFORMAT_QIPC_COL_I32(OrderBookRow, "ordercount", ordercount),
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

inline constexpr std::array<qformat::qipc::ColumnSpec<TickerRow>, 8>
    TICKER_COLS = {{
        QFORMAT_QIPC_COL_SYM(TickerRow, "sym", sym),
        QFORMAT_QIPC_COL_TSNS(TickerRow, "time", time),
        QFORMAT_QIPC_COL_I32(TickerRow, "direction", direction),
        QFORMAT_QIPC_COL_F64(TickerRow, "price", price),
        QFORMAT_QIPC_COL_I64(TickerRow, "volume", volume),
        QFORMAT_QIPC_COL_F64(TickerRow, "amount", amount),
        QFORMAT_QIPC_COL_TSNS(TickerRow, "msgTime", msgTime),
        QFORMAT_QIPC_COL_TSNS(TickerRow, "recvTime", recvTime),
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

inline constexpr std::array<qformat::qipc::ColumnSpec<KL1MinRow>, 14>
    KL1MIN_COLS = {{
        QFORMAT_QIPC_COL_SYM(KL1MinRow, "sym", sym),
        QFORMAT_QIPC_COL_TSNS(KL1MinRow, "time", time),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "high", high),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "open", open),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "low", low),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "close", close),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "lastclose", lastclose),
        QFORMAT_QIPC_COL_I64(KL1MinRow, "volume", volume),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "amount", amount),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "turnoverrate", turnoverrate),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "pe", pe),
        QFORMAT_QIPC_COL_F64(KL1MinRow, "changerate", changerate),
        QFORMAT_QIPC_COL_TSNS(KL1MinRow, "recvtime", recvtime),
        QFORMAT_QIPC_COL_TSNS(KL1MinRow, "tstime", tstime),
    }};

} // namespace momo::schema
