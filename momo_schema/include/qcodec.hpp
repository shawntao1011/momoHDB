#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <chrono>

namespace qipc {

// ---------- Little-endian writers ----------
static inline void put_u8(std::vector<uint8_t>& b, uint8_t v) { b.push_back(v); }

static inline void put_u32_le(std::vector<uint8_t>& b, uint32_t v) {
    b.push_back(uint8_t(v & 0xFF));
    b.push_back(uint8_t((v >> 8) & 0xFF));
    b.push_back(uint8_t((v >> 16) & 0xFF));
    b.push_back(uint8_t((v >> 24) & 0xFF));
}

static inline void put_u64_le(std::vector<uint8_t>& b, uint64_t v) {
    for (int i = 0; i < 8; ++i) b.push_back(uint8_t((v >> (8 * i)) & 0xFF));
}

static inline void put_i32_le(std::vector<uint8_t>& b, int32_t v) {
    put_u32_le(b, static_cast<uint32_t>(v));
}

static inline void put_i64_le(std::vector<uint8_t>& b, int64_t v) {
    put_u64_le(b, static_cast<uint64_t>(v));
}

static inline void put_f64_le(std::vector<uint8_t>& b, double d) {
    static_assert(sizeof(double) == 8, "double must be 8 bytes");
    uint64_t u = 0;
    std::memcpy(&u, &d, 8);
    put_u64_le(b, u);
}

static inline void put_cstr(std::vector<uint8_t>& b, std::string_view s) {
    b.insert(b.end(), s.begin(), s.end());
    b.push_back(0);
}

// ---------- kdb+ IPC type codes (subset) ----------
constexpr uint8_t XT_TABLE = 98;   // xT
constexpr uint8_t XD_DICT  = 99;   // xD
constexpr uint8_t KL_LIST  = 0;    // general list
constexpr uint8_t KS_LIST  = 11;   // symbol list
constexpr uint8_t KP_TS    = 12;   // timestamp list
constexpr uint8_t KF_LIST  = 9;    // float (double) list
constexpr uint8_t KJ_LIST  = 7;    // long (int64) list
constexpr uint8_t KI_LIST  = 6;    // int (int32) list

// ---------- time conversion ----------
static inline int64_t to_kdb_timestamp_ns(std::chrono::system_clock::time_point tp) {
    using namespace std::chrono;
    // 2000-01-01 00:00:00 UTC minus 1970-01-01 00:00:00 UTC
    constexpr int64_t unix_to_kdb_epoch_sec = 946684800LL;
    const auto ns_since_unix = duration_cast<nanoseconds>(tp.time_since_epoch()).count();
    const int64_t shift = unix_to_kdb_epoch_sec * 1000000000LL;
    return ns_since_unix - shift;
}

// ---------- K object emit helpers ----------
static inline void emit_typed_list_hdr(std::vector<uint8_t>& b, uint8_t t, uint32_t n) {
    put_u8(b, t);
    put_u8(b, 0);          // attr
    put_u32_le(b, n);
}

static inline void emit_general_list_hdr(std::vector<uint8_t>& b, uint32_t n) {
    put_u8(b, KL_LIST);
    put_u8(b, 0);
    put_u32_le(b, n);
}

static inline void emit_dict_begin(std::vector<uint8_t>& b) { put_u8(b, XD_DICT); }
static inline void emit_table_begin(std::vector<uint8_t>& b){ put_u8(b, XT_TABLE); put_u8(b, 0); }

static inline void emit_sym_list(std::vector<uint8_t>& b, const std::vector<std::string_view>& syms) {
    emit_typed_list_hdr(b, KS_LIST, static_cast<uint32_t>(syms.size()));
    for (auto s : syms) put_cstr(b, s);
}

struct Builder {
    std::vector<uint8_t> buf;

    void begin_ipc() {
        buf.clear();
        buf.resize(8, 0); // reserve IPC header
    }

    void finish_ipc_and_return(std::string& out) {
        // fill IPC header
        buf[0] = 1; // little-endian
        buf[1] = 0; // msgtype
        buf[2] = 0; // no compression
        buf[3] = 0;
        uint32_t total = static_cast<uint32_t>(buf.size());
        buf[4] = uint8_t(total & 0xFF);
        buf[5] = uint8_t((total >> 8) & 0xFF);
        buf[6] = uint8_t((total >> 16) & 0xFF);
        buf[7] = uint8_t((total >> 24) & 0xFF);

        out.assign(reinterpret_cast<const char*>(buf.data()), buf.size());
    }
};

} // namespace qipc