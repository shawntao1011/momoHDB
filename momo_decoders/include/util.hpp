#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <chrono>
#include <vector>

#include "kafkax/core/decoder.h"

namespace momo::util {

    inline void set_err(kafkax_decode_out_t* out, const char* msg) {
        if (!out) return;
            out->kind = KAFKAX_DECODE_ERR;
        if (msg) {
            std::snprintf(out->err_msg, sizeof(out->err_msg), "%s", msg);
            out->err_msg[sizeof(out->err_msg) - 1] = '\0';
        } else {
            out->err_msg[0] = '\0';
        }
    }

    inline void set_err(kafkax_decode_out_t* out, const std::string& msg) {
        set_err(out, msg.c_str());
    }

    inline std::chrono::system_clock::time_point tp_from_ts_ms(int64_t ts_ms) {
        if (ts_ms <= 0) {
            return std::chrono::system_clock::now();
        }
        return std::chrono::system_clock::time_point{std::chrono::milliseconds{ts_ms}};
    }

    // Futu uses integer market codes in proto; map common ones.
    inline std::string market_code_to_prefix(int32_t mkt) {
        switch (mkt) {
            case 1: return "HK";
            case 2: return "US";
            case 3: return "SH"; // guess
            case 4: return "SZ"; // guess
            default: return std::to_string(mkt);
        }
    }

    template <class SecurityT>
    inline std::string build_symbol_from_security(const SecurityT& sec) {
        // Security has fields: market(), code()
        return market_code_to_prefix(static_cast<int32_t>(sec.market())) + "." + sec.code();
    }

    namespace detail {
        inline bool is_digit(char c) { return c >= '0' && c <= '9'; }

        inline bool parse2(const char* p, int& out) {
            if (!is_digit(p[0]) || !is_digit(p[1])) return false;
            out = (p[0] - '0') * 10 + (p[1] - '0');
            return true;
        }

        inline bool parse4(const char* p, int& out) {
            if (!is_digit(p[0]) || !is_digit(p[1]) || !is_digit(p[2]) || !is_digit(p[3])) return false;
            out = (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0');
            return true;
        }

        // Howard Hinnant's civil date -> days since 1970-01-01 algorithm.
        inline int64_t days_from_civil(int y, unsigned m, unsigned d) {
            y -= m <= 2;
            const int era = (y >= 0 ? y : y - 399) / 400;
            const unsigned yoe = static_cast<unsigned>(y - era * 400);
            const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
            const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            return era * 146097 + static_cast<int>(doe) - 719468;
        }
    } // namespace detail

    // Parse "YYYY-MM-DD HH:MM:SS" / "YYYY-MM-DDTHH:MM:SS" (assumed UTC).
    // Fast path avoids iostream/locale overhead and heap allocation.
    inline std::chrono::system_clock::time_point parse_time_utc_string(std::string_view s) {
        if (s.size() < 19) return std::chrono::system_clock::now();

        const char* p = s.data();
        if (p[4] != '-' || p[7] != '-' || (p[10] != ' ' && p[10] != 'T') || p[13] != ':' || p[16] != ':') {
            return std::chrono::system_clock::now();
        }

        int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
        if (!detail::parse4(p + 0, year) ||
            !detail::parse2(p + 5, month) ||
            !detail::parse2(p + 8, day) ||
            !detail::parse2(p + 11, hour) ||
            !detail::parse2(p + 14, minute) ||
            !detail::parse2(p + 17, second)) {
            return std::chrono::system_clock::now();
            }

        if (month < 1 || month > 12 || day < 1 || day > 31 ||
            hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 60) {
            return std::chrono::system_clock::now();
            }

        const int64_t days = detail::days_from_civil(year, static_cast<unsigned>(month), static_cast<unsigned>(day));
        const int64_t sod = static_cast<int64_t>(hour) * 3600 + static_cast<int64_t>(minute) * 60 + second;
        return std::chrono::system_clock::time_point{std::chrono::seconds{days * 86400 + sod}};
    }

    inline std::chrono::system_clock::time_point parse_time_epoch_sec(int64_t epoch_sec) {
        if (epoch_sec <= 0) return std::chrono::system_clock::now();
        return std::chrono::system_clock::time_point{std::chrono::seconds{epoch_sec}};
    }

    inline int write_bytes_to_out(const std::vector<std::uint8_t>& bytes, kafkax_decode_out_t* out) {
        if (!out) return -1;
        out->err_msg[0] = '\0';

        if (bytes.size() > out->cap) {
            out->kind = KAFKAX_DECODE_NEED_MORE;
            out->need = bytes.size();
            out->len = 0;
            return 0;
        }

        if (!out->buf || out->cap == 0) {
            out->kind = KAFKAX_DECODE_ERR;
            std::snprintf(out->err_msg, sizeof(out->err_msg), "output buffer is null/empty");
            out->err_msg[sizeof(out->err_msg) - 1] = '\0';
            return -1;
        }

        std::memcpy(out->buf, bytes.data(), bytes.size());
        out->kind = KAFKAX_DECODE_OK;
        out->len = bytes.size();
        out->need = 0;
        return 0;
    }

} // namespace momo::util
