#include "schema_qipc.hpp"

#include "qcodec.hpp"

namespace qipc {
    template <class RowT>
    static inline void emit_sym_col(qipc::Builder& b, uint32_t n, const std::vector<RowT>& rows,
                                    std::string_view colname_unused,
                                    std::string_view (*get_sym)(const RowT&)) = delete;

    // Overload using lambda getter returning std::string_view
    template <class RowT, class Getter>
    static inline void emit_sym_col(qipc::Builder& b, uint32_t n, const std::vector<RowT>& rows, Getter get) {
        qipc::emit_typed_list_hdr(b.buf, qipc::KS_LIST, n);
        for (const auto& r : rows) qipc::put_cstr(b.buf, std::string_view(get(r)));
    }

    template <class RowT, class Getter>
    static inline void emit_ts_col(qipc::Builder& b, uint32_t n, const std::vector<RowT>& rows, Getter get_tp) {
        qipc::emit_typed_list_hdr(b.buf, qipc::KP_TS, n);
        for (const auto& r : rows) qipc::put_i64_le(b.buf, qipc::to_kdb_timestamp_ns(get_tp(r)));
    }

    template <class RowT, class Getter>
    static inline void emit_f64_col(qipc::Builder& b, uint32_t n, const std::vector<RowT>& rows, Getter get) {
        qipc::emit_typed_list_hdr(b.buf, qipc::KF_LIST, n);
        for (const auto& r : rows) qipc::put_f64_le(b.buf, static_cast<double>(get(r)));
    }

    template <class RowT, class Getter>
    static inline void emit_i64_col(qipc::Builder& b, uint32_t n, const std::vector<RowT>& rows, Getter get) {
        qipc::emit_typed_list_hdr(b.buf, qipc::KJ_LIST, n);
        for (const auto& r : rows) qipc::put_i64_le(b.buf, static_cast<int64_t>(get(r)));
    }

    template <class RowT, class Getter>
    static inline void emit_i32_col(qipc::Builder& b, uint32_t n, const std::vector<RowT>& rows, Getter get) {
        qipc::emit_typed_list_hdr(b.buf, qipc::KI_LIST, n);
        for (const auto& r : rows) qipc::put_i32_le(b.buf, static_cast<int32_t>(get(r)));
    }

    // ---- common: begin a table with given colnames (keys) and declare general list size ----
    static inline void begin_table(qipc::Builder& b, const std::vector<std::string_view>& colnames) {
        qipc::emit_table_begin(b.buf);
        qipc::emit_dict_begin(b.buf);
        qipc::emit_sym_list(b.buf, colnames);
        qipc::emit_general_list_hdr(b.buf, static_cast<uint32_t>(colnames.size()));
    }

    // ========== 1) BasicQuoteBatch ==========
    bool serialize_basicquote_qipc(const BasicQuoteBatch& batch, std::vector<std::uint8_t>& out_kbytes) {
        static const std::vector<std::string_view> colnames = {
            "sym","time","spread","high","open","low","cur","lastclose",
            "volume","amount","turnoverrate","amplitude","updtime"
        };

        qipc::Builder b;
        b.begin_ipc();
        begin_table(b, colnames);

        const auto& rows = batch.rows;
        const uint32_t n = static_cast<uint32_t>(rows.size());

        emit_sym_col(b, n, rows, [](const BasicQuoteRow& r)->std::string_view { return r.symbol; });
        emit_ts_col (b, n, rows, [](const BasicQuoteRow& r){ return r.time; });

        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.priceSpread; });
        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.highPrice; });
        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.openPrice; });
        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.lowPrice; });
        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.curPrice; });
        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.lastClosePrice; });

        emit_i64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.volume; });

        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.amount; });
        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.turnoverRate; });
        emit_f64_col(b, n, rows, [](const BasicQuoteRow& r){ return r.amplitude; });

        emit_ts_col (b, n, rows, [](const BasicQuoteRow& r){ return r.updateTime; });

        b.finish_ipc_and_return(out_kbytes);
        return true;
    }

    // ========== 2) OrderBookBatch ==========
    bool serialize_orderbook_qipc(const OrderBookBatch& batch, std::vector<std::uint8_t>& out_kbytes) {

        static const std::vector<std::string_view> colnames = {
            "sym","time","side","level","price","volume","ordercount"
        };

        qipc::Builder b;
        b.begin_ipc();
        begin_table(b, colnames);

        const auto& rows = batch.rows;
        const uint32_t n = static_cast<uint32_t>(rows.size());

        emit_sym_col(b, n, rows, [](const OrderBookRow& r)->std::string_view { return r.symbol; });
        emit_ts_col (b, n, rows, [](const OrderBookRow& r){ return r.time; });
        emit_sym_col(b, n, rows, [](const OrderBookRow& r)->std::string_view { return r.side; });

        emit_i32_col(b, n, rows, [](const OrderBookRow& r){ return r.rank; });
        emit_f64_col(b, n, rows, [](const OrderBookRow& r){ return r.price; });
        emit_i64_col(b, n, rows, [](const OrderBookRow& r){ return r.volume; });
        emit_i32_col(b, n, rows, [](const OrderBookRow& r){ return r.orderCount; });

        b.finish_ipc_and_return(out_kbytes);
        return true;
    }

    // ========== 3) TickerBatch ==========
    bool serialize_ticker_qipc(const TickerBatch& batch, std::vector<std::uint8_t>& out_kbytes) {
        static const std::vector<std::string_view> colnames = {
            "sym","time","direction","price","volume","turnover","amount","msgTime","recvTime"
        };

        qipc::Builder b;
        b.begin_ipc();
        begin_table(b, colnames);

        const auto& rows = batch.rows;
        const uint32_t n = static_cast<uint32_t>(rows.size());

        emit_sym_col(b, n, rows, [](const TickerRow& r)->std::string_view { return r.symbol; });
        emit_ts_col (b, n, rows, [](const TickerRow& r){ return r.time; });

        emit_i32_col(b, n, rows, [](const TickerRow& r){ return r.direction; });
        emit_f64_col(b, n, rows, [](const TickerRow& r){ return r.price; });
        emit_i64_col(b, n, rows, [](const TickerRow& r){ return r.volume; });
        emit_f64_col(b, n, rows, [](const TickerRow& r){ return r.turnover; });

        emit_ts_col (b, n, rows, [](const TickerRow& r){ return r.msgTime; });
        emit_ts_col (b, n, rows, [](const TickerRow& r){ return r.recvTime; });

        b.finish_ipc_and_return(out_kbytes);
        return true;
    }

    // ========== 4) KL1MinBatch ==========
    bool serialize_kl1min_qipc(const KL1MinBatch& batch, std::vector<std::uint8_t>& out_kbytes) {
        static const std::vector<std::string_view> colnames = {
            "sym","time","high","open","low","close","lastclose","volume",
            "amount","turnoverrate","pe","changerate","recvtime","tstime"
        };

        qipc::Builder b;
        b.begin_ipc();
        begin_table(b, colnames);

        const auto& rows = batch.rows;
        const uint32_t n = static_cast<uint32_t>(rows.size());

        emit_sym_col(b, n, rows, [](const KL1MinRow& r)->std::string_view { return r.symbol; });
        emit_ts_col (b, n, rows, [](const KL1MinRow& r){ return r.time; });

        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.highPrice; });
        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.openPrice; });
        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.lowPrice; });
        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.closePrice; });
        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.lastClosePrice; });

        emit_i64_col(b, n, rows, [](const KL1MinRow& r){ return r.volume; });

        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.amount; });
        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.turnoverRate; });
        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.pe; });
        emit_f64_col(b, n, rows, [](const KL1MinRow& r){ return r.changeRate; });

        emit_ts_col (b, n, rows, [](const KL1MinRow& r){ return r.recvTime; });
        emit_ts_col (b, n, rows, [](const KL1MinRow& r){ return r.timestamp; });

        b.finish_ipc_and_return(out_kbytes);
        return true;
    }
} //namespace qipc