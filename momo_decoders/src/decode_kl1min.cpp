#include <vector>
#include <string>

#include "momo_decoders/rows.hpp"
#include "momo_utils/market_utils.hpp"
#include "momo_utils/time_utils.hpp"

#include "Proto/Qot_UpdateKL.pb.h"

#include "momo_decoders/rows.hpp"
#include "momo_decoders/util.hpp"

#include <kafkax/core/decoder.h>

using momo::utils::tp_from_ts_ms;

extern "C" int momo_decode_kl1min(const kafkax_envelope_t* env, kafkax_decode_out_t* out) {
    if (!env || !out) return -1;
    out->err_msg[0] = '\0';

    const auto* payload = env->payload.data;
    const auto  len = env->payload.len;
    if (!payload || len == 0) {
        momo::decoders::set_err(out, "empty payload");
        return -1;
    }

    Qot_UpdateKL::Response rsp;
    if (!rsp.ParseFromArray(payload, static_cast<int>(len))) {
        momo::decoders::set_err(out, "ParseFromArray failed for Qot_UpdateKL::Response");
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
    const int n = s2c.kllist_size();
    if (n <= 0) {
        out->kind = KAFKAX_DECODE_SKIP;
        out->len = 0;
        return 0;
    }

    std::string sym;
    if (s2c.has_security()) sym = momo::utils::build_symbol_from_security(s2c.security());
    else if (env->symbol.data && env->symbol.len) sym.assign(env->symbol.data, env->symbol.len);

    auto base_tp = tp_from_ts_ms(env->timestamp_ms);

    std::vector<momo::schema::KL1MinRow> rows;
    rows.reserve(static_cast<size_t>(n));

    for (int i = 0; i < n; ++i) {
        const auto& t = s2c.kllist(i);
        momo::schema::KL1MinRow r{};
        r.sym = sym;
        r.time = base_tp;

        if (t.has_highprice()) r.high = t.highprice();
        if (t.has_openprice()) r.open = t.openprice();
        if (t.has_lowprice()) r.low = t.lowprice();
        if (t.has_closeprice()) r.close = t.closeprice();
        if (t.has_lastcloseprice()) r.lastclose = t.lastcloseprice();

        if (t.has_volume()) r.volume = t.volume();
        if (t.has_turnover()) r.amount = t.turnover();
        if (t.has_turnoverrate()) r.turnoverrate = t.turnoverrate();
        if (t.has_pe()) r.pe = t.pe();
        if (t.has_changerate()) r.changerate = t.changerate();

        if (t.has_time()) r.recvtime = momo::utils::parse_time_utc_string(t.time());
        else r.recvtime = base_tp;

        if (t.has_timestamp()) r.tstime = momo::utils::parse_time_epoch_sec(t.timestamp());
        else r.tstime = base_tp;

        rows.emplace_back(std::move(r));
    }

    auto bytes = kafkax::qipc::encode_table_ipc<momo::schema::KL1MinRow>(rows, momo::schema::KL1MIN_COLS);
    return momo::decoders::write_bytes_to_out(bytes, out);
}
