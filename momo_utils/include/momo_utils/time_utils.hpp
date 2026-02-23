#pragma once

#include <chrono>
#include <cstdint>

namespace momo::utils {

    inline std::chrono::system_clock::time_point tp_from_ts_ms(int64_t ts_ms) {
            if (ts_ms <= 0) {
                return std::chrono::system_clock::now();
            }
            return std::chrono::system_clock::time_point{std::chrono::milliseconds{ts_ms}};
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

}// namespace momo::utils