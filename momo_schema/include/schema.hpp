#pragma once

#include <chrono>
#include <string>

struct BasicQuoteRow {
    std::string symbol;
    std::chrono::system_clock::time_point time;
    double priceSpread;
    double highPrice;
    double openPrice;
    double lowPrice;
    double curPrice;
    double lastClosePrice;
    std::int64_t volume;
    double amount; // message "turnover"
    double turnoverRate;
    double amplitude;
    std::chrono::system_clock::time_point updateTime;
};
struct BasicQuoteBatch {
    std::vector<BasicQuoteRow> rows;
};

struct OrderBookRow {
    std::string symbol;
    std::chrono::system_clock::time_point time;
    std::string side;
    std::int32_t rank;
    double price;
    int64_t volume;
    int32_t orderCount; // msg "oredercount" ! api typo !
};
struct OrderBookBatch {
    std::vector<OrderBookRow> rows;
};

struct TickerRow {
    std::string symbol;
    std::chrono::system_clock::time_point time;
    int32_t direction;
    double price;
    int64_t volume;
    double turnover;
    std::chrono::system_clock::time_point msgTime; // message "time"
    std::chrono::system_clock::time_point recvTime; // message "recvTime"
};
struct TickerBatch {
    std::vector<TickerRow> rows;
};

struct KL1MinRow {
    std::string symbol;
    std::chrono::system_clock::time_point time;
    double highPrice;
    double openPrice;
    double lowPrice;
    double closePrice;
    double lastClosePrice;
    int64_t volume;
    double amount; // message "turnover"
    double turnoverRate;
    double pe;
    double changeRate;
    std::chrono::system_clock::time_point recvTime; // message "time"
    std::chrono::system_clock::time_point timestamp; // message "timestamp"
};
struct KL1MinBatch {
    std::vector<KL1MinRow> rows;
};