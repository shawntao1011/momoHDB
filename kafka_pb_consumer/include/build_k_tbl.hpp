#pragma once
#include <cstdint>

struct k0;
using K = k0*;

struct TickerBatch;
struct OrderBookBatch;
struct BasicQuoteBatch;
struct KL1MinBatch;

K to_table(const TickerBatch& b);
K to_table(const OrderBookBatch& b);
K to_table(const BasicQuoteBatch& b);
K to_table(const KL1MinBatch& b);
