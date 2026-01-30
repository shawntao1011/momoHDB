#include <k.h>
#include "build_k_tbl.hpp"
#include "momoDB_types.hpp"

namespace {

    // 1970-01-01 -> 2000-01-01
    constexpr std::int64_t DAYS_1970_TO_2000 = 10957LL;
    constexpr std::int64_t NS_PER_DAY = 86400LL * 1000000000LL;
    constexpr std::int64_t NS_1970_TO_2000 = DAYS_1970_TO_2000 * NS_PER_DAY;

    inline std::int64_t ns2000_from_time_point(
        const std::chrono::system_clock::time_point& tp
    ) {
        using namespace std::chrono;

        if (tp.time_since_epoch().count() == 0)
            return nj;

        constexpr std::int64_t DAYS_1970_TO_2000 = 10957LL;
        constexpr std::int64_t NS_PER_DAY = 86400LL * 1000000000LL;
        constexpr std::int64_t NS_1970_TO_2000 = DAYS_1970_TO_2000 * NS_PER_DAY;

        const auto ns_epoch =
            duration_cast<nanoseconds>(tp.time_since_epoch()).count();

        return ns_epoch - NS_1970_TO_2000;
    }

    inline K ksym_vec(J n) { return ktn(KS, n); }
    inline K kts_vec(J n)  { return ktn(KP, n); } // timestamp vector: underlying J

} // namespace

K to_table(const TickerBatch& b) {
    const J n = (J)b.rows.size();

    K sym      = ksym_vec(n);
    K time     = kts_vec(n);
    K dir      = ktn(KI, n);
    K price    = ktn(KF, n);
    K volume   = ktn(KJ, n);
    K turnover = ktn(KF, n);
    K msgTime  = kts_vec(n);
    K recvTime = kts_vec(n);

    for (J i = 0; i < n; ++i) {
        const auto& r = b.rows[(std::size_t)i];
        kS(sym)[i]      = ss((S)r.symbol.c_str());
        kJ(time)[i]     = ns2000_from_time_point(r.time);
        kI(dir)[i]      = r.direction;
        kF(price)[i]    = r.price;
        kJ(volume)[i]   = (J)r.volume;
        kF(turnover)[i] = r.turnover;
        kJ(msgTime)[i]  = ns2000_from_time_point(r.msgTime);
        kJ(recvTime)[i] = ns2000_from_time_point(r.recvTime);
    }

    K cols = ktn(KS, 8);
    kS(cols)[0]=ss((S)"sym");
    kS(cols)[1]=ss((S)"time");
    kS(cols)[2]=ss((S)"dir");
    kS(cols)[3]=ss((S)"price");
    kS(cols)[4]=ss((S)"volume");
    kS(cols)[5]=ss((S)"turnover");
    kS(cols)[6]=ss((S)"msgTime");
    kS(cols)[7]=ss((S)"recvTime");

    K vals = knk(8, sym, time, dir, price, volume, turnover, msgTime, recvTime);
    return xT(xD(cols, vals));
}

K to_table(const OrderBookBatch& b) {
    const J n = (J)b.rows.size();

    K sym        = ksym_vec(n);
    K time       = kts_vec(n);
    K side       = ksym_vec(n);   // store as symbol
    K rank       = ktn(KI, n);
    K price      = ktn(KF, n);
    K volume     = ktn(KJ, n);
    K orderCount = ktn(KI, n);

    for (J i = 0; i < n; ++i) {
        const auto& r = b.rows[(std::size_t)i];
        kS(sym)[i]        = ss((S)r.symbol.c_str());
        kJ(time)[i]       = ns2000_from_time_point(r.time);
        kS(side)[i]       = ss((S)r.side.c_str());
        kI(rank)[i]       = r.rank;
        kF(price)[i]      = r.price;
        kJ(volume)[i]     = (J)r.volume;
        kI(orderCount)[i] = r.orderCount;
    }

    K cols = ktn(KS, 7);
    kS(cols)[0]=ss((S)"sym");
    kS(cols)[1]=ss((S)"time");
    kS(cols)[2]=ss((S)"side");
    kS(cols)[3]=ss((S)"rank");
    kS(cols)[4]=ss((S)"price");
    kS(cols)[5]=ss((S)"volume");
    kS(cols)[6]=ss((S)"orderCount");

    K vals = knk(7, sym, time, side, rank, price, volume, orderCount);
    return xT(xD(cols, vals));
}

K to_table(const BasicQuoteBatch& b) {
    const J n = (J)b.rows.size();

    K sym            = ksym_vec(n);
    K time           = kts_vec(n);
    K priceSpread    = ktn(KF, n);
    K highPrice      = ktn(KF, n);
    K openPrice      = ktn(KF, n);
    K lowPrice       = ktn(KF, n);
    K curPrice       = ktn(KF, n);
    K lastClosePrice = ktn(KF, n);
    K volume         = ktn(KJ, n);
    K amount         = ktn(KF, n);
    K turnoverRate   = ktn(KF, n);
    K amplitude      = ktn(KF, n);
    K updateTime     = kts_vec(n);

    for (J i = 0; i < n; ++i) {
        const auto& r = b.rows[(std::size_t)i];
        kS(sym)[i]            = ss((S)r.symbol.c_str());
        kJ(time)[i]           = ns2000_from_time_point(r.time);
        kF(priceSpread)[i]    = r.priceSpread;
        kF(highPrice)[i]      = r.highPrice;
        kF(openPrice)[i]      = r.openPrice;
        kF(lowPrice)[i]       = r.lowPrice;
        kF(curPrice)[i]       = r.curPrice;
        kF(lastClosePrice)[i] = r.lastClosePrice;
        kJ(volume)[i]         = (J)r.volume;
        kF(amount)[i]         = r.amount;
        kF(turnoverRate)[i]   = r.turnoverRate;
        kF(amplitude)[i]      = r.amplitude;
        kJ(updateTime)[i]     = ns2000_from_time_point(r.updateTime);
    }

    K cols = ktn(KS, 13);
    kS(cols)[0]=ss((S)"sym");
    kS(cols)[1]=ss((S)"time");
    kS(cols)[2]=ss((S)"priceSpread");
    kS(cols)[3]=ss((S)"highPrice");
    kS(cols)[4]=ss((S)"openPrice");
    kS(cols)[5]=ss((S)"lowPrice");
    kS(cols)[6]=ss((S)"curPrice");
    kS(cols)[7]=ss((S)"lastClosePrice");
    kS(cols)[8]=ss((S)"volume");
    kS(cols)[9]=ss((S)"amount");
    kS(cols)[10]=ss((S)"turnoverRate");
    kS(cols)[11]=ss((S)"amplitude");
    kS(cols)[12]=ss((S)"updateTime");

    K vals = knk(13, sym, time, priceSpread, highPrice, openPrice, lowPrice, curPrice,
                 lastClosePrice, volume, amount, turnoverRate, amplitude, updateTime);
    return xT(xD(cols, vals));
}

K to_table(const KL1MinBatch& b) {
    const J n = (J)b.rows.size();

    K sym            = ksym_vec(n);
    K time           = kts_vec(n);
    K highPrice      = ktn(KF, n);
    K openPrice      = ktn(KF, n);
    K lowPrice       = ktn(KF, n);
    K closePrice     = ktn(KF, n);
    K lastClosePrice = ktn(KF, n);
    K volume         = ktn(KJ, n);
    K amount         = ktn(KF, n);
    K turnoverRate   = ktn(KF, n);
    K pe             = ktn(KF, n);
    K changeRate     = ktn(KF, n);
    K recvTime       = kts_vec(n);
    K timestamp      = kts_vec(n);

    for (J i = 0; i < n; ++i) {
        const auto& r = b.rows[(std::size_t)i];
        kS(sym)[i]            = ss((S)r.symbol.c_str());
        kJ(time)[i]           = ns2000_from_time_point(r.time);
        kF(highPrice)[i]      = r.highPrice;
        kF(openPrice)[i]      = r.openPrice;
        kF(lowPrice)[i]       = r.lowPrice;
        kF(closePrice)[i]     = r.closePrice;
        kF(lastClosePrice)[i] = r.lastClosePrice;
        kJ(volume)[i]         = (J)r.volume;
        kF(amount)[i]         = r.amount;
        kF(turnoverRate)[i]   = r.turnoverRate;
        kF(pe)[i]             = r.pe;
        kF(changeRate)[i]     = r.changeRate;
        kJ(recvTime)[i]       = ns2000_from_time_point(r.recvTime);
        kJ(timestamp)[i]      = ns2000_from_time_point(r.timestamp);
    }

    K cols = ktn(KS, 14);
    kS(cols)[0]=ss((S)"sym");
    kS(cols)[1]=ss((S)"time");
    kS(cols)[2]=ss((S)"highPrice");
    kS(cols)[3]=ss((S)"openPrice");
    kS(cols)[4]=ss((S)"lowPrice");
    kS(cols)[5]=ss((S)"closePrice");
    kS(cols)[6]=ss((S)"lastClosePrice");
    kS(cols)[7]=ss((S)"volume");
    kS(cols)[8]=ss((S)"amount");
    kS(cols)[9]=ss((S)"turnoverRate");
    kS(cols)[10]=ss((S)"pe");
    kS(cols)[11]=ss((S)"changeRate");
    kS(cols)[12]=ss((S)"recvTime");
    kS(cols)[13]=ss((S)"timestamp");

    K vals = knk(14, sym, time, highPrice, openPrice, lowPrice, closePrice, lastClosePrice,
                 volume, amount, turnoverRate, pe, changeRate, recvTime, timestamp);
    return xT(xD(cols, vals));
}