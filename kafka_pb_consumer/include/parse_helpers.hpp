#pragma once
#include <chrono>
#include <cmath>
#include <string>

#include "Proto/Qot_Common.pb.h"

inline std::string market_prefix(int market) {
    // Prefer human-friendly keys like HK.00700 / US.AAPL / SH.600519.
    const std::string name = Qot_Common::QotMarket_Name(
        static_cast<Qot_Common::QotMarket>(market)
    );
    constexpr std::string_view prefix = "QotMarket_";
    constexpr std::string_view suffix = "_Security";

    if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
        return name; //fallback
    }
    const auto begin = prefix.size();
    const auto len   = name.size() - prefix.size() - suffix.size();

    if (len == 0) {
        return name; //fallback
    }

    return name.substr(begin, len);
}

inline std::string build_symbol_from_security(const Qot_Common::Security& sec) {
    const std::string code = sec.has_code() ? sec.code() : std::string{};
    if (code.empty()) return {};
    if (sec.has_market()) {
        return market_prefix(sec.market()) + "." + code;
    }
    return code;
}

inline std::chrono::system_clock::time_point
parse_time_utc_string(const std::string& s)
{
    std::tm tm{};
    std::istringstream iss(s);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    if (iss.fail()) {
        throw std::runtime_error("invalid datetime format: " + s);
    }

    std::time_t tt = std::mktime(&tm);
    if (tt == -1) {
        throw std::runtime_error("mktime failed: " + s);
    }

    return std::chrono::system_clock::from_time_t(tt);
}

inline std::chrono::system_clock::time_point
parse_time_epoch_sec(double epoch_seconds) {
    if (!std::isfinite(epoch_seconds)) {
        throw std::invalid_argument("epoch seconds is not finite");
    }

    auto sec  = static_cast<int64_t>(epoch_seconds);
    auto frac = epoch_seconds - static_cast<double>(sec);

    auto ns = static_cast<int64_t>(frac * 1'000'000'000.0);

    return std::chrono::system_clock::time_point{
        std::chrono::seconds{sec} +
        std::chrono::nanoseconds{ns}
    };
}