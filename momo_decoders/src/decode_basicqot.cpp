#include <vector>
#include <string>

#include "../include/momo_decoders/rows.hpp"
#include "momo_utils/market_utils.hpp"
#include "momo_utils/time_utils.hpp"

#include "Proto/Qot_UpdateBasicQot.pb.h"

#include "momo_decoders/rows.hpp"
#include "momo_decoders/util.hpp"

#include "kafkax/decoder.h"

using momo::utils::tp_from_ts_ms;

extern "C" int momo_decode_basicqot(const kafkax_envelope_t* env, kafkax_decode_out_t* out) {
    if (!env || !out) return -1;
    out->err_msg[0] = '\0';

    const auto* payload = env->payload.data;
    const auto  len = env->payload.len;
    if (!payload || len == 0) {
        momo::decoders::set_err(out, "empty payload");
        return -1;
    }

    Qot_UpdateBasicQot::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(len))) {
        momo::decoders::set_err(out, "ParseFromArray failed for Qot_UpdateBasicQot::Response");
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
    const int n = s2c.basicqotlist_size();
    if (n <= 0) {
        out->kind = KAFKAX_DECODE_SKIP;
        out->len = 0;
        return 0;
    }

    std::vector<momo::schema::BasicQuoteRow> rows;
    rows.reserve(static_cast<size_t>(n));

    auto base_tp = tp_from_ts_ms(env->timestamp_ms);

    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.basicqotlist(i);
        momo::schema::BasicQuoteRow r{};

        if (t.has_security()) r.sym = momo::utils::build_symbol_from_security(t.security());
        else if (env->symbol.data && env->symbol.len) r.sym.assign(env->symbol.data, env->symbol.len);

        r.time = base_tp;

        if (t.has_pricespread())    r.spread = t.pricespread();
        if (t.has_highprice())      r.high = t.highprice();
        if (t.has_openprice())      r.open = t.openprice();
        if (t.has_lowprice())       r.low = t.lowprice();
        if (t.has_curprice())       r.cur = t.curprice();
        if (t.has_lastcloseprice()) r.lastclose = t.lastcloseprice();
        if (t.has_volume())         r.volume = static_cast<int64_t>(t.volume());
        if (t.has_turnover())       r.amount = t.turnover();
        if (t.has_turnoverrate())   r.turnoverrate = t.turnoverrate();
        if (t.has_amplitude())      r.amplitude = t.amplitude();

        if (t.has_updatetime()) {
            r.updtime = momo::utils::parse_time_utc_string(t.updatetime());
        } else {
            r.updtime = base_tp;
        }

        rows.emplace_back(std::move(r));
    }

    auto bytes = qformat::qipc::encode_table<momo::schema::BasicQuoteRow>(rows, momo::schema::BASICQOT_COLS);

    return momo::decoders::write_bytes_to_out(bytes, out);
}
