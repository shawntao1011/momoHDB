#include "decoders.hpp"
#include "parse_helpers.hpp"
#include "Proto/Qot_UpdateBasicQot.pb.h"
#include "Proto/Qot_UpdateKL.pb.h"
#include "Proto/Qot_UpdateOrderBook.pb.h"
#include "Proto/Qot_UpdateTicker.pb.h"

bool decode_qot_update_basicquote(
    const void* data,
    std::size_t len,
    int64_t ingest_time,
    BasicQuoteBatch& out,
    std::string& err) {

    out.rows.clear();
    err.clear();

    Qot_UpdateBasicQot::Response rsp;
    if (!rsp.ParseFromArray(data, static_cast<int>(len))) {
        err = "ParseFromArray failed for Qot_UpdateBasicQot::Response (type mismatch or corrupted bytes)";
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        err = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) err += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  err += " retMsg=" + rsp.retmsg();
        return false;
    }

    if (!rsp.has_s2c()) {
        err = "missing s2c";
        return false;
    }
    const auto& s2c = rsp.s2c();

    const int n = s2c.basicqotlist_size();
    if (n <= 0) {
        err = "basicqotlist is empty";
        return false;
    }

    out.rows.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.basicqotlist(i);
        BasicQuoteRow row{};

        // security: market + code .e.g HK.00100
        if (t.has_security()) row.symbol = build_symbol_from_security(t.security());

        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ingest_time));

        if (t.has_pricespread())        row.priceSpread = t.pricespread();
        if (t.has_highprice())          row.highPrice = t.highprice();
        if (t.has_openprice())          row.openPrice = t.openprice();
        if (t.has_lowprice())           row.lowPrice = t.lowprice();
        if (t.has_curprice())           row.curPrice = t.curprice();
        if (t.has_lastcloseprice())     row.lastClosePrice = t.lastcloseprice();
        if (t.has_volume())         row.volume = (int64_t)t.volume();
        if (t.has_turnover())       row.amount = t.turnover();
        if (t.has_turnoverrate())   row.turnoverRate = t.turnoverrate();
        if (t.has_amplitude())      row.amplitude = t.amplitude();

        if (t.has_updatetime())     row.updateTime = parse_time_utc_string(t.updatetime());

        out.rows.emplace_back(std::move(row));
    }

    return true;
}

std::string serialize_basicquotebatch(const BasicQuoteBatch& batch) {

}

bool decode_qot_update_orderbook(
    const void* data,
    std::size_t len,
    int64_t ingest_time,
    OrderBookBatch& out,
    std::string& err) {

    out.rows.clear();
    err.clear();

    Qot_UpdateOrderBook::Response rsp;
    if (!rsp.ParseFromArray(data, static_cast<int>(len))) {
        err = "ParseFromArray failed for Qot_UpdateOrderBook::Response (type mismatch or corrupted bytes)";
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        err = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) err += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  err += " retMsg=" + rsp.retmsg();
        return false;
    }

    if (!rsp.has_s2c()) {
        err = "missing s2c";
        return false;
    }
    const auto& s2c = rsp.s2c();

    std::string symbol;
    if (s2c.has_security()) symbol = build_symbol_from_security(s2c.security());

    const int32_t nask = s2c.orderbookasklist_size();
    const int32_t nbid = s2c.orderbookbidlist_size();
    if (nask <= 0 && nbid <= 0) {
        err = "orderbook asklist and bidlist are both empty";
        return false;
    }
    if (nask == 0) err += "warn: asklist empty; ";
    if (nbid == 0) err += "warn: bidlist empty; ";

    for (int i = 0; i < nask; i++) {
        const Qot_Common::OrderBook &ob = s2c.orderbookasklist(i);

        OrderBookRow row{};
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ingest_time));
        row.side = "ask";
        row.rank = i;

        if (ob.has_price()) row.price = ob.price();
        if (ob.has_volume()) row.volume = ob.volume();
        if (ob.has_oredercount()) row.orderCount = ob.oredercount();

        out.rows.emplace_back(std::move(row));
    }

    for (int i = 0; i < nbid; i++) {
        const Qot_Common::OrderBook &ob = s2c.orderbookasklist(i);

        OrderBookRow row{};
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ingest_time));
        row.side = "bid";
        row.rank = i;

        if (ob.has_price()) row.price = ob.price();
        if (ob.has_volume()) row.volume = ob.volume();
        if (ob.has_oredercount()) row.orderCount = ob.oredercount();

        out.rows.emplace_back(std::move(row));
    }

    return true;
}

std::string serialize_orderbookbatch(const OrderBookBatch& batch) {

}

bool decode_qot_update_ticker(
    const void* data,
    std::size_t len,
    int64_t ingest_time,
    TickerBatch& out,
    std::string& err) {

    out.rows.clear();
    err.clear();

    Qot_UpdateTicker::Response rsp;
    if (!rsp.ParseFromArray(data, static_cast<int>(len))) {
        err = "ParseFromArray failed for Qot_UpdateTicker::Response (type mismatch or corrupted bytes)";
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        err = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) err += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  err += " retMsg=" + rsp.retmsg();
        return false;
    }

    if (!rsp.has_s2c()) {
        err = "missing s2c";
        return false;
    }
    const auto& s2c = rsp.s2c();

    const int n = s2c.tickerlist_size();
    if (n <= 0) {
        err = "tickerlist is empty";
        return false;
    }

    std::string symbol;
    if (s2c.has_security()) symbol = build_symbol_from_security(s2c.security());

    // repeated .Qot_Common.Ticker tickerList = 2;
    out.rows.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.tickerlist(i);

        TickerRow row{};

        // security: market + code .e.g HK.00100
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ingest_time));

        if (t.has_dir())        row.direction = (int32_t)t.dir();
        if (t.has_price())      row.price    = t.price();
        if (t.has_volume())     row.volume   = (int64_t)t.volume();
        if (t.has_turnover())   row.turnover = t.turnover();

        if (t.has_time())       row.msgTime = parse_time_utc_string(t.time());
        if (t.has_recvtime())   row.recvTime = parse_time_epoch_sec(t.recvtime());

        out.rows.emplace_back(std::move(row));
    }

    return true;
}

std::string serialize_tickerbatch(const TickerBatch& batch) {

}

bool decode_qot_update_kl1min(
    const void* data,
    std::size_t len,
    int64_t ingest_time,
    KL1MinBatch& out,
    std::string& err) {

    out.rows.clear();
    err.clear();

    Qot_UpdateKL::Response rsp;
    if (!rsp.ParseFromArray(data, static_cast<int>(len))) {
        err = "ParseFromArray failed for Qot_UpdateKL::Response (type mismatch or corrupted bytes)";
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        err = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) err += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  err += " retMsg=" + rsp.retmsg();
        return false;
    }

    if (!rsp.has_s2c()) {
        err = "missing s2c";
        return false;
    }
    const auto& s2c = rsp.s2c();

    const int n = s2c.kllist_size();
    if (n <= 0) {
        err = "kline list is empty";
        return false;
    }

    std::string symbol;
    if (s2c.has_security()) symbol = build_symbol_from_security(s2c.security());

    // repeated .Qot_Common.Ticker tickerList = 2;
    out.rows.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.kllist(i);

        KL1MinRow row{};

        // security: market + code .e.g HK.00100
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ingest_time));

        if (t.has_highprice())  row.highPrice = t.highprice();
        if (t.has_openprice())  row.openPrice = t.openprice();
        if (t.has_lowprice())  row.lowPrice = t.lowprice();
        if (t.has_closeprice())  row.closePrice = t.closeprice();
        if (t.has_lastcloseprice())  row.lastClosePrice = t.lastcloseprice();

        if (t.has_volume()) row.volume = t.volume();

        if (t.has_turnover())   row.volume = t.volume();
        if (t.has_turnoverrate())   row.turnoverRate = t.turnoverrate();
        if (t.has_pe())   row.pe = t.pe();
        if (t.has_changerate())   row.changeRate = t.changerate();

        if (t.has_time())   row.recvTime = parse_time_utc_string(t.time());
        if (t.has_timestamp()) row.timestamp = parse_time_epoch_sec(t.timestamp());

        out.rows.emplace_back(std::move(row));
    }

    return true;
}

std::string serialize_kl1minbatch(const KL1MinBatch& batch) {

}