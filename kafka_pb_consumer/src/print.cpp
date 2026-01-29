#include "print.hpp"

inline std::string fmt_ts_ms(std::chrono::system_clock::time_point tp) {
    using namespace std::chrono;
    const auto tp_ms = floor<milliseconds>(tp);
    // "YYYY-mm-dd HH:MM:SS.mmm" => 23 chars
    return std::format("{:%Y-%m-%d %H:%M:%S}", tp_ms) +
           std::format(".{:03}", duration_cast<milliseconds>(tp_ms.time_since_epoch()).count() % 1000);
}
inline std::string shorten(std::string_view s, std::size_t w) {
    if (w == 0) return {};
    if (s.size() <= w) return std::string(s);
    if (w <= 1) return std::string(w, '.');
    return std::string(s.substr(0, w - 1)) + "…";
}
inline std::size_t apply_limit(std::size_t n, std::size_t limit) {
    if (limit == 0) return n;
    return std::min(n, limit);
}
struct Col {
    std::string_view name;
    int width;
    bool right;
};
inline void print_header(std::ostream& os, std::initializer_list<Col> cols) {
    // header line
    for (const auto& c : cols) {
        os << std::setw(c.width)
           << (c.right ? std::right : std::left)
           << c.name << " ";
    }
    os << "\n";

    // dash line (always left)
    for (const auto& c : cols) {
        os << std::setw(c.width) << std::left
           << std::string(std::max(1, c.width), '-') << " ";
    }
    os << "\n";
}
inline void print_footer(std::ostream& os, std::size_t shown, std::size_t total) {
    if (shown < total) {
        os << "... (" << shown << "/" << total << " rows shown)\n";
    } else {
        os << "(" << shown << " rows)\n";
    }
}

// ---------- Row single-line pretty (good for logs) ----------
inline std::ostream& pretty_row(std::ostream& os, const BasicQuoteRow& r) {
    os << "BasicQuoteRow{symbol=" << r.symbol
       << " time=" << fmt_ts_ms(r.time)
       << " curPrice=" << r.curPrice
       << " open=" << r.openPrice
       << " high=" << r.highPrice
       << " low=" << r.lowPrice
       << " lastClose=" << r.lastClosePrice
       << " spread=" << r.priceSpread
       << " volume=" << r.volume
       << " amount=" << r.amount
       << " turnoverRate=" << r.turnoverRate
       << " amplitude=" << r.amplitude
       << " updateTime=" << fmt_ts_ms(r.updateTime)
       << "}";
    return os;
}

inline std::ostream& pretty_row(std::ostream& os, const OrderBookRow& r) {
    os << "OrderBookRow{symbol=" << r.symbol
       << " time=" << fmt_ts_ms(r.time)
       << " side=" << r.side
       << " rank=" << r.rank
       << " price=" << r.price
       << " volume=" << r.volume
       << " orderCount=" << r.orderCount
       << "}";
    return os;
}

inline std::ostream& pretty_row(std::ostream& os, const TickerRow& r) {
    os << "TickerRow{symbol=" << r.symbol
       << " time=" << fmt_ts_ms(r.time)
       << " dir=" << r.direction
       << " price=" << r.price
       << " volume=" << r.volume
       << " turnover=" << r.turnover
       << " msgTime=" << fmt_ts_ms(r.msgTime)
       << " recvTime=" << fmt_ts_ms(r.recvTime)
       << "}";
    return os;
}

inline std::ostream& pretty_row(std::ostream& os, const KL1MinRow& r) {
    os << "KL1MinRow{symbol=" << r.symbol
       << " time=" << fmt_ts_ms(r.time)
       << " open=" << r.openPrice
       << " high=" << r.highPrice
       << " low=" << r.lowPrice
       << " close=" << r.closePrice
       << " lastClose=" << r.lastClosePrice
       << " volume=" << r.volume
       << " amount=" << r.amount
       << " turnoverRate=" << r.turnoverRate
       << " pe=" << r.pe
       << " changeRate=" << r.changeRate
       << " recvTime=" << fmt_ts_ms(r.recvTime)
       << " timestamp=" << fmt_ts_ms(r.timestamp)
       << "}";
    return os;
}

void pretty_print(const BasicQuoteBatch& b, std::ostream& os, std::size_t limit) {
    const auto total = b.rows.size();
    const auto shown = apply_limit(total, limit);

    os << "BasicQuoteBatch\n";
    print_header(os, {
        {"symbol", 14, false},
        {"time", 23, false},
        {"cur", 10, true},
        {"open", 10, true},
        {"high", 10, true},
        {"low", 10, true},
        {"lastClose", 10, true},
        {"spread", 10, true},
        {"volume", 12, true},
        {"amount", 12, true},
        {"turnoverRate", 12, true},
        {"amplitude", 10, true},
        {"updateTime", 23, false},
    });

    os.setf(std::ios::fixed);
    os << std::setprecision(4);

    for (std::size_t i = 0; i < shown; ++i) {
        const auto& r = b.rows[i];
        os << std::setw(14) << std::left  << shorten(r.symbol, 14) << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.time) << " "
           << std::setw(10) << std::right << r.curPrice << " "
           << std::setw(10) << std::right << r.openPrice << " "
           << std::setw(10) << std::right << r.highPrice << " "
           << std::setw(10) << std::right << r.lowPrice << " "
           << std::setw(10) << std::right << r.lastClosePrice << " "
           << std::setw(10) << std::right << r.priceSpread << " "
           << std::setw(12) << std::right << r.volume << " "
           << std::setw(12) << std::right << r.amount << " "
           << std::setw(12) << std::right << r.turnoverRate << " "
           << std::setw(10) << std::right << r.amplitude << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.updateTime)
           << "\n";
    }

    print_footer(os, shown, total);
}

void pretty_print(const OrderBookBatch& b, std::ostream& os, std::size_t limit) {
    const auto total = b.rows.size();
    const auto shown = apply_limit(total, limit);

    os << "OrderBookBatch\n";
    print_header(os, {
        {"symbol", 14, false},
        {"time", 23, false},
        {"side", 4, false},
        {"rank", 4, true},
        {"price", 12, true},
        {"volume", 12, true},
        {"orderCnt", 9, true},
    });

    os.setf(std::ios::fixed);
    os << std::setprecision(4);

    for (std::size_t i = 0; i < shown; ++i) {
        const auto& r = b.rows[i];
        os << std::setw(14) << std::left  << shorten(r.symbol, 14) << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.time) << " "
           << std::setw(4)  << std::left  << shorten(r.side, 4) << " "
           << std::setw(4)  << std::right << r.rank << " "
           << std::setw(12) << std::right << r.price << " "
           << std::setw(12) << std::right << r.volume << " "
           << std::setw(9)  << std::right << r.orderCount
           << "\n";
    }

    print_footer(os, shown, total);
}

void pretty_print(const TickerBatch& b, std::ostream& os, std::size_t limit) {
    const auto total = b.rows.size();
    const auto shown = apply_limit(total, limit);

    os << "TickerBatch\n";
    print_header(os, {
        {"symbol", 14, false},
        {"time", 23, false},
        {"dir", 3, true},
        {"price", 12, true},
        {"volume", 12, true},
        {"turnover", 12, true},
        {"msgTime", 23, false},
        {"recvTime", 23, false},
    });

    os.setf(std::ios::fixed);
    os << std::setprecision(4);

    for (std::size_t i = 0; i < shown; ++i) {
        const auto& r = b.rows[i];
        os << std::setw(14) << std::left  << shorten(r.symbol, 14) << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.time) << " "
           << std::setw(3)  << std::right << r.direction << " "
           << std::setw(12) << std::right << r.price << " "
           << std::setw(12) << std::right << r.volume << " "
           << std::setw(12) << std::right << r.turnover << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.msgTime) << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.recvTime)
           << "\n";
    }

    print_footer(os, shown, total);
}

void pretty_print(const KL1MinBatch& b, std::ostream& os, std::size_t limit) {
    const auto total = b.rows.size();
    const auto shown = apply_limit(total, limit);

    os << "KL1MinBatch\n";
    print_header(os, {
        {"symbol", 14, false},
        {"time", 23, false},
        {"open", 10, true},
        {"high", 10, true},
        {"low", 10, true},
        {"close", 10, true},
        {"lastClose", 10, true},
        {"volume", 12, true},
        {"amount", 12, true},
        {"turnoverRate", 12, true},
        {"pe", 10, true},
        {"changeRate", 12, true},
        {"recvTime", 23, false},
        {"timestamp", 23, false},
    });

    os.setf(std::ios::fixed);
    os << std::setprecision(4);

    for (std::size_t i = 0; i < shown; ++i) {
        const auto& r = b.rows[i];
        os << std::setw(14) << std::left  << shorten(r.symbol, 14) << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.time) << " "
           << std::setw(10) << std::right << r.openPrice << " "
           << std::setw(10) << std::right << r.highPrice << " "
           << std::setw(10) << std::right << r.lowPrice << " "
           << std::setw(10) << std::right << r.closePrice << " "
           << std::setw(10) << std::right << r.lastClosePrice << " "
           << std::setw(12) << std::right << r.volume << " "
           << std::setw(12) << std::right << r.amount << " "
           << std::setw(12) << std::right << r.turnoverRate << " "
           << std::setw(10) << std::right << r.pe << " "
           << std::setw(12) << std::right << r.changeRate << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.recvTime) << " "
           << std::setw(23) << std::left  << fmt_ts_ms(r.timestamp)
           << "\n";
    }

    print_footer(os, shown, total);
}