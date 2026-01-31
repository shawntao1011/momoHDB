#include "schema_qipc.hpp"
#include "schema.hpp"

#include <iostream>
#include <iomanip>
#include <sstream>

// -------- helper: print bytes as 0x.... --------
static void print_hex(const std::string& name, const std::string& bytes) {
    std::ostringstream oss;
    oss << "==== " << name << " ====\n0x";
    for (unsigned char c : bytes) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)c;
    }
    oss << "\n\n";
    std::cout << oss.str();
}

// -------- helpers for timestamps --------
static std::chrono::system_clock::time_point
tp_from_ns_since_unix(int64_t ns) {
    return std::chrono::system_clock::time_point{
        std::chrono::nanoseconds(ns)
    };
}

int main() {
    using clock = std::chrono::system_clock;

    // =========================================================
    // 1) BasicQuote
    // =========================================================
    BasicQuoteBatch quoteBatch;
    quoteBatch.rows.push_back({
        "HK.00700",
        tp_from_ns_since_unix(1769853158369900000LL), // 2026-01-31 07:12:38.3699
        168.7,
        170.0,
        170.5,
        168.2,
        173.3,
        0.1,
        19778781,
        3.350718e9,
        0.104,
        1.327,
        tp_from_ns_since_unix(1769853158369900000LL)
    });

    auto quoteBytes = qipc::serialize_basicquote_qipc(quoteBatch);
    print_hex("BasicQuote", quoteBytes);

    // =========================================================
    // 2) OrderBook
    // =========================================================
    OrderBookBatch obBatch;
    obBatch.rows.push_back({
        "HK.00700",
        tp_from_ns_since_unix(1769853158369900000LL),
        "B",        // side as symbol
        1,          // rank
        173.20,
        12000,
        8           // orderCount
    });

    auto obBytes = qipc::serialize_orderbook_qipc(obBatch);
    print_hex("OrderBook", obBytes);

    // =========================================================
    // 3) Ticker
    // =========================================================
    TickerBatch tickerBatch;
    tickerBatch.rows.push_back({
        "HK.00700",
        tp_from_ns_since_unix(1769853158369900000LL),
        1,          // direction
        173.25,
        200,
        34650.0,
        tp_from_ns_since_unix(1769853158369900000LL), // msgTime
        tp_from_ns_since_unix(1769853158370000000LL)  // recvTime
    });

    auto tickerBytes = qipc::serialize_ticker_qipc(tickerBatch);
    print_hex("Ticker", tickerBytes);

    // =========================================================
    // 4) KL1Min
    // =========================================================
    KL1MinBatch klBatch;
    klBatch.rows.push_back({
        "HK.00700",
        tp_from_ns_since_unix(1769853120000000000LL), // 07:12:00
        174.0,
        172.5,
        171.8,
        173.3,
        170.0,
        580000,
        1.002e8,
        0.032,
        25.6,
        0.018,
        tp_from_ns_since_unix(1769853159000000000LL), // recvTime
        tp_from_ns_since_unix(1769853120000000000LL)  // timestamp
    });

    auto klBytes = qipc::serialize_kl1min_qipc(klBatch);
    print_hex("KL1Min", klBytes);

    return 0;
}