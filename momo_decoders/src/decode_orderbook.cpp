#include <vector>
#include <string>

#include "momo_utils/market_utils.hpp"
#include "momo_utils/time_utils.hpp"

#include "Proto/Qot_Common.pb.h"
#include "Proto/Qot_UpdateOrderBook.pb.h"

#include "momo_decoders/rows.hpp"
#include "momo_decoders/util.hpp"

#include "kafkax/decoder.h"

using momo::utils::tp_from_ts_ms;

extern "C" int momo_decode_orderbook(const kafkax_envelope_t* env, kafkax_decode_out_t* out) {
    if (!env || !out) return -1;
    out->err_msg[0] = '\0';

    const auto* payload = env->payload.data;
    const auto  len = env->payload.len;
    if (!payload || len == 0) {
        momo::decoders::set_err(out, "empty payload");
        return -1;
    }

    Qot_UpdateOrderBook::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(len))) {
        momo::decoders::set_err(out, "ParseFromArray failed for Qot_UpdateOrderBook::Response");
        return -1;
    }

    if (rsp.has_rettype() && rsp.rettype() != 0) {
        std::string e = "retType!=0 retType=" + std::to_string(rsp.rettype());
        if (rsp.has_errcode()) e += " errCode=" + std::to_string(rsp.errcode());
        if (rsp.has_retmsg())  e += " retMsg=" + rsp.retmsg();
        momo::decoders::set_err(out, e);
        return -1;
    }

    if (!rsp.has_s2c()) {
        momo::decoders::set_err(out, "missing s2c");
        return -1;
    }

    const auto& s2c = rsp.s2c();

    std::string sym;
    if (s2c.has_security()) sym = momo::utils::build_symbol_from_security(s2c.security());
    else if (env->symbol.data && env->symbol.len) sym.assign(env->symbol.data, env->symbol.len);

    const int nask = s2c.orderbookasklist_size();
    const int nbid = s2c.orderbookbidlist_size();
    if (nask <= 0 && nbid <= 0) {
        out->kind = KAFKAX_DECODE_SKIP;
        out->len = 0;
        return 0;
    }

    auto base_tp = tp_from_ts_ms(env->timestamp_ms);

    std::vector<momo::schema::OrderBookRow> rows;
    rows.reserve(static_cast<size_t>(nask + nbid));

    for (int i = 0; i < nask; ++i) {
        const Qot_Common::OrderBook& ob = s2c.orderbookasklist(i);
        momo::schema::OrderBookRow r{};
        r.sym = sym;
        r.time = base_tp;
        r.side = "ask";
        r.level = i;
        if (ob.has_price()) r.price = ob.price();
        if (ob.has_volume()) r.volume = ob.volume();
        if (ob.has_oredercount()) r.ordercount = ob.oredercount();
        rows.emplace_back(std::move(r));
    }

    for (int i = 0; i < nbid; ++i) {
        const Qot_Common::OrderBook& ob = s2c.orderbookbidlist(i);
        momo::schema::OrderBookRow r{};
        r.sym = sym;
        r.time = base_tp;
        r.side = "bid";
        r.level = i;
        if (ob.has_price()) r.price = ob.price();
        if (ob.has_volume()) r.volume = ob.volume();
        if (ob.has_oredercount()) r.ordercount = ob.oredercount();
        rows.emplace_back(std::move(r));
    }

    auto bytes = qformat::qipc::encode_table<momo::schema::OrderBookRow>(rows, momo::schema::ORDERBOOK_COLS);
    return momo::decoders::write_bytes_to_out(bytes, out);
}
