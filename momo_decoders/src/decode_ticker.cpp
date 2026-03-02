#include <vector>
#include <string>

#include "../include/momo_decoders/rows.hpp"
#include "momo_utils/market_utils.hpp"
#include "momo_utils/time_utils.hpp"

#include "Proto/Qot_UpdateTicker.pb.h"

#include "momo_decoders/rows.hpp"
#include "momo_decoders/util.hpp"

#include "kafkax/decoder.h"

using momo::utils::tp_from_ts_ms;

extern "C" int momo_decode_ticker(const kafkax_envelope_t* env, kafkax_decode_out_t* out) {
    if (!env || !out) return -1;
    out->err_msg[0] = '\0';

    const auto* payload = env->payload.data;
    const auto  len = env->payload.len;
    if (!payload || len == 0) {
        momo::decoders::set_err(out, "empty payload");
        return -1;
    }

    Qot_UpdateTicker::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(len))) {
        momo::decoders::set_err(out, "ParseFromArray failed for Qot_UpdateTicker::Response");
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
    const int n = s2c.tickerlist_size();
    if (n <= 0) {
        out->kind = KAFKAX_DECODE_SKIP;
        out->len = 0;
        return 0;
    }

    std::string sym;
    if (s2c.has_security()) sym = momo::utils::build_symbol_from_security(s2c.security());
    else if (env->symbol.data && env->symbol.len) sym.assign(env->symbol.data, env->symbol.len);

    auto base_tp = tp_from_ts_ms(env->timestamp_ms);

    std::vector<momo::schema::TickerRow> rows;
    rows.reserve(static_cast<size_t>(n));

    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.tickerlist(i);
        momo::schema::TickerRow r{};
        r.sym = sym;
        r.time = base_tp;

        if (t.has_dir()) r.direction = static_cast<int32_t>(t.dir());
        if (t.has_price()) r.price = t.price();
        if (t.has_volume()) r.volume = static_cast<int64_t>(t.volume());
        if (t.has_turnover()) r.amount = t.turnover();

        if (t.has_time()) r.msgTime = momo::utils::parse_time_utc_string(t.time());
        else r.msgTime = base_tp;

        if (t.has_recvtime()) r.recvTime = momo::utils::parse_time_epoch_sec(t.recvtime());
        else r.recvTime = base_tp;

        rows.emplace_back(std::move(r));
    }

    auto bytes = qformat::qipc::encode_table<momo::schema::TickerRow>(rows, momo::schema::TICKER_COLS);
    return momo::decoders::write_bytes_to_out(bytes, out);
}
