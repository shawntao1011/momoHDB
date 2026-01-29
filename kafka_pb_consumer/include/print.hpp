#pragma once
#include "momoDB_types.hpp"

void pretty_print(const TickerBatch&, std::ostream&,std::size_t limit=0);
void pretty_print(const OrderBookBatch&, std::ostream&,std::size_t limit=0);
void pretty_print(const BasicQuoteBatch&, std::ostream&,std::size_t limit=0);
void pretty_print(const KL1MinBatch&, std::ostream&,std::size_t limit=0);
