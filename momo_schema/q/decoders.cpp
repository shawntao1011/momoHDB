#include "../include/decoders.hpp"
#include "parse_helpers.hpp"
#include "schema.hpp"
#include "qipc.hpp"

#include "Proto/Qot_UpdateBasicQot.pb.h"
#include "Proto/Qot_UpdateKL.pb.h"
#include "Proto/Qot_UpdateOrderBook.pb.h"
#include "Proto/Qot_UpdateTicker.pb.h"

static void set_err(char* dst, std::size_t cap, const std::string& s) {
    if (!dst || cap == 0) return;
    std::snprintf(dst, cap, "%s", s.c_str());
    dst[cap - 1] = '\0';
}

bool decode_basicquote_kbytes(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ns,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
) {
    out_kbytes.clear();
    if (err_msg && err_cap) err_msg[0] = '\0';

    // --- 1) protobuf parse ---
    Qot_UpdateBasicQot::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(payload_len))) {
        set_err(err_msg, err_cap, "ParseFromArray failed for Qot_UpdateBasicQot::Response (type mismatch or corrupted bytes)");
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        std::string e = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) e += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  e += " retMsg=" + rsp.retmsg();
        set_err(err_msg, err_cap, e);
        return false;
    }

    if (!rsp.has_s2c()) {
        set_err(err_msg, err_cap, "missing s2c");
        return false;
    }
    const auto& s2c = rsp.s2c();

    const int n = s2c.basicqotlist_size();
    if (n <= 0) {
        set_err(err_msg, err_cap, "basicqotlist is empty");
        return false;
    }

    // --- 2) fill intermediate struct ---
    BasicQuoteBatch b;
    b.rows.clear();
    b.rows.reserve(static_cast<std::size_t>(n));

    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.basicqotlist(i);
        BasicQuoteRow row{};

        // security: market + code .e.g HK.00100
        if (t.has_security()) row.symbol = build_symbol_from_security(t.security());

        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ts_ns));

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

        b.rows.emplace_back(std::move(row));
    }

    // --- 3) serialize to kbytes ---
    if (!qipc::serialize_basicquote_qipc(b, out_kbytes)) {
        set_err(err_msg, err_cap, "encode_ticker_batch failed");
        return false;
    }

    return true;
}

bool decode_orderbook_kbytes(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ns,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
) {
    out_kbytes.clear();
    if (err_msg && err_cap) err_msg[0] = '\0';

    // --- 1) protobuf parse ---
    Qot_UpdateOrderBook::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(payload_len))) {
        set_err(err_msg, err_cap, "ParseFromArray failed for Qot_UpdateOrderBook::Response (type mismatch or corrupted bytes)");
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        std::string e = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) e += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  e += " retMsg=" + rsp.retmsg();
        set_err(err_msg, err_cap, e);
        return false;
    }

    if (!rsp.has_s2c()) {
        set_err(err_msg, err_cap,"missing s2c");
        return false;
    }
    const auto& s2c = rsp.s2c();

    std::string symbol;
    if (s2c.has_security()) symbol = build_symbol_from_security(s2c.security());

    const int32_t nask = s2c.orderbookasklist_size();
    const int32_t nbid = s2c.orderbookbidlist_size();
    if (nask <= 0 && nbid <= 0) {
        set_err(err_msg, err_cap, "orderbook asklist and bidlist are both empty");
        return false;
    }
    std::string e;
    if (nask == 0) e += "warn: asklist empty; ";
    if (nbid == 0) e += "warn: bidlist empty; ";

    // --- 2) fill intermediate struct ---
    OrderBookBatch b;
    b.rows.clear();
    b.rows.reserve(static_cast<std::size_t>(nask+nbid));
    for (int i = 0; i < nask; i++) {
        const Qot_Common::OrderBook &ob = s2c.orderbookasklist(i);

        OrderBookRow row{};
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ts_ns));
        row.side = "ask";
        row.rank = i;

        if (ob.has_price()) row.price = ob.price();
        if (ob.has_volume()) row.volume = ob.volume();
        if (ob.has_oredercount()) row.orderCount = ob.oredercount();

        b.rows.emplace_back(std::move(row));
    }

    for (int i = 0; i < nbid; i++) {
        const Qot_Common::OrderBook &ob = s2c.orderbookbidlist(i);

        OrderBookRow row{};
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ts_ns));
        row.side = "bid";
        row.rank = i;

        if (ob.has_price()) row.price = ob.price();
        if (ob.has_volume()) row.volume = ob.volume();
        if (ob.has_oredercount()) row.orderCount = ob.oredercount();

        b.rows.emplace_back(std::move(row));
    }


    // --- 3) serialize to kbytes ---
    if (!qipc::serialize_orderbook_qipc(b, out_kbytes)) {
        e += "encode_ticker_batch failed";
        set_err(err_msg, err_cap, e);
        return false;
    }

    return true;
}

bool decode_ticker_kbytes(
    const std::uint8_t* /*key*/, std::size_t /*key_len*/,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ns,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
) {
    out_kbytes.clear();
    if (err_msg && err_cap) err_msg[0] = '\0';

    // --- 1) protobuf parse ---
    Qot_UpdateTicker::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(payload_len))) {
        set_err(err_msg, err_cap, "ParseFromArray failed for Qot_UpdateTicker::Response");
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        std::string e = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) e += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  e += " retMsg=" + rsp.retmsg();
        set_err(err_msg, err_cap, e);
        return false;
    }

    if (!rsp.has_s2c()) {
        set_err(err_msg, err_cap, "missing s2c");
        return false;
    }
    const auto& s2c = rsp.s2c();

    const int n = s2c.tickerlist_size();
    if (n <= 0) {
        set_err(err_msg, err_cap, "tickerlist is empty");
        return false;
    }

    std::string symbol;
    if (s2c.has_security()) symbol = build_symbol_from_security(s2c.security());

    // --- 2) fill intermediate struct ---
    TickerBatch b;
    b.rows.clear();
    b.rows.reserve(static_cast<std::size_t>(n));

    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.tickerlist(i);

        TickerRow row{};
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ts_ns));

        if (t.has_dir())        row.direction = (int32_t)t.dir();
        if (t.has_price())      row.price    = t.price();
        if (t.has_volume())     row.volume   = (int64_t)t.volume();
        if (t.has_turnover())   row.amount = t.turnover();

        if (t.has_time())       row.msgTime  = parse_time_utc_string(t.time());
        if (t.has_recvtime())   row.recvTime = parse_time_epoch_sec(t.recvtime());

        b.rows.emplace_back(std::move(row));
    }

    // --- 3) serialize to kbytes ---
    if (!qipc::serialize_ticker_qipc(b, out_kbytes)) {
        set_err(err_msg, err_cap, "encode_ticker_batch failed");
        return false;
    }

    return true;
}

bool decode_kl1min_kbytes(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ns,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
) {
    out_kbytes.clear();
    if (err_msg && err_cap) err_msg[0] = '\0';

    // --- 1) protobuf parse ---
    Qot_UpdateKL::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(payload_len))) {
        set_err(err_msg, err_cap, "ParseFromArray failed for Qot_UpdateKL::Response (type mismatch or corrupted bytes)");
        return false;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        std::string e = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) e += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  e += " retMsg=" + rsp.retmsg();
        set_err(err_msg, err_cap, e);
        return false;
    }

    if (!rsp.has_s2c()) {
        set_err(err_msg, err_cap, "missing s2c");
        return false;
    }
    const auto& s2c = rsp.s2c();

    const int n = s2c.kllist_size();
    if (n <= 0) {
        set_err(err_msg, err_cap,"kline list is empty");
        return false;
    }

    std::string symbol;
    if (s2c.has_security()) symbol = build_symbol_from_security(s2c.security());

    // --- 2) fill intermediate struct ---
    KL1MinBatch b;
    b.rows.clear();
    b.rows.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.kllist(i);

        KL1MinRow row{};

        // security: market + code .e.g HK.00100
        row.symbol = symbol;
        row.time = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ts_ns));

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

        b.rows.emplace_back(std::move(row));
    }

    // --- 3) serialize to kbytes ---
    if (!qipc::serialize_kl1min_qipc(b, out_kbytes)) {
        set_err(err_msg, err_cap, "encode_ticker_batch failed");
        return false;
    }

    return true;
}