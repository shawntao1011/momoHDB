#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <fmt/format.h>
#include <filesystem>

#include "config/LoggerConfig.hpp"

namespace logger {
// ------------------------------
// Public types
// ------------------------------
enum class Level : std::uint8_t { Debug, Info, Warn, Error };
Level parse_level(std::string_view s);

enum class OverflowPolicy : std::uint8_t {
    // Never block producer threads; if queue is full, drop oldest log lines.
    OverrunOldest,
    // Block producer threads when queue is full (more complete logs, can impact latency).
    Block,
};

struct Field {
    enum class Kind : std::uint8_t { String, RawJson } kind{Kind::String};
    std::string_view key;
    std::string_view value; // for Kind::String: unescaped value; for RawJson: already valid JSON fragment
};

inline Field field(std::string_view k, std::string_view v) {
    return Field{Field::Kind::String, k, v};
}

// Raw JSON fragment (use carefully): e.g. raw("latency_ms", "12") or raw("tags", R"(["a","b"])")
inline Field raw(std::string_view k, std::string_view json_fragment) {
    return Field{Field::Kind::RawJson, k, json_fragment};
}

template <class T>
requires(std::is_integral_v<T> || std::is_floating_point_v<T>)
inline Field num(std::string_view k, T v) {
    // Store the formatted number in a thread_local scratch for this call.
    // Simpler approach: let caller pass raw() if they want to avoid formatting.
    thread_local std::string tmp;
    tmp = fmt::format("{}", v);
    return raw(k, tmp);
}

inline Field b(std::string_view k, bool v) {
    return raw(k, v ? "true" : "false");
}

// ------------------------------
// Implementation helpers
// ------------------------------
namespace detail {

inline constexpr std::string_view level_str(Level lv) {
    switch (lv) {
        case Level::Debug: return "debug";
        case Level::Info:  return "info";
        case Level::Warn:  return "warn";
        case Level::Error: return "error";
    }
    return "info";
}

inline constexpr spdlog::level::level_enum to_spd(Level lv) {
    switch (lv) {
        case Level::Debug: return spdlog::level::debug;
        case Level::Info:  return spdlog::level::info;
        case Level::Warn:  return spdlog::level::warn;
        case Level::Error: return spdlog::level::err;
    }
    return spdlog::level::info;
}

// Fast JSON string escape into fmt buffer (no locks, no heap if buffer has capacity).
inline void append_json_escaped(fmt::memory_buffer& out, std::string_view s) {
    for (unsigned char c : s) {
        switch (c) {
            case '\"': fmt::format_to(std::back_inserter(out), "\\\""); break;
            case '\\': fmt::format_to(std::back_inserter(out), "\\\\"); break;
            case '\b': fmt::format_to(std::back_inserter(out), "\\b"); break;
            case '\f': fmt::format_to(std::back_inserter(out), "\\f"); break;
            case '\n': fmt::format_to(std::back_inserter(out), "\\n"); break;
            case '\r': fmt::format_to(std::back_inserter(out), "\\r"); break;
            case '\t': fmt::format_to(std::back_inserter(out), "\\t"); break;
            default:
                if (c < 0x20) {
                    // Control chars -> \u00XX
                    fmt::format_to(std::back_inserter(out), "\\u{:04x}", static_cast<unsigned>(c));
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
}

// epoch millis (cheap + stable for ELK)
inline std::int64_t now_epoch_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

inline void append_kv_string(fmt::memory_buffer& out, std::string_view k, std::string_view v) {
    out.push_back('\"');
    append_json_escaped(out, k);
    fmt::format_to(std::back_inserter(out), "\":\"");
    append_json_escaped(out, v);
    out.push_back('\"');
}

inline void append_kv_raw(fmt::memory_buffer& out, std::string_view k, std::string_view raw_json) {
    out.push_back('\"');
    append_json_escaped(out, k);
    fmt::format_to(std::back_inserter(out), "\":");
    // raw_json is expected to be valid JSON token/fragment
    fmt::format_to(std::back_inserter(out), "{}", raw_json);
}

} // namespace detail

// ------------------------------
// Logger singleton
// ------------------------------
class JsonLogger final {
public:
    struct Options {
        std::string file_path{"logs/app.jsonl"};  // JSON Lines
        bool also_console{false};                 // helpful for dev
        Level level{Level::Info};

        std::size_t queue_size{1u << 16};         // 65536
        std::size_t worker_threads{1};            // 1 is usually enough
        OverflowPolicy overflow{OverflowPolicy::OverrunOldest};

        // flush policy
        spdlog::level::level_enum flush_on{spdlog::level::warn};
    };

    static JsonLogger& instance() {
        static JsonLogger inst;
        return inst;
    }

    // Call once early in main() before spawning your hot threads.
    void init(const cfg::LoggerCfg& cfg) {
        bool expected = false;
        if (!inited_.compare_exchange_strong(expected, true)) return;

        // thread pool for async
        spdlog::init_thread_pool(cfg.queue_size, cfg.worker_threads);

        std::filesystem::create_directories(std::filesystem::path(cfg.file_path).parent_path());

        // sinks
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(cfg.file_path, /*truncate=*/false);
        sinks_.clear();
        sinks_.push_back(file_sink);

        if (cfg.also_console) {
            auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            sinks_.push_back(console_sink);
        }

        auto overflow = (cfg.overflow == cfg::OverflowPolicy::Block)
            ? spdlog::async_overflow_policy::block
            : spdlog::async_overflow_policy::overrun_oldest;

        logger_ = std::make_shared<spdlog::async_logger>(
            "jsonl",
            sinks_.begin(),
            sinks_.end(),
            spdlog::thread_pool(),
            overflow
        );

        logger_->set_level(detail::to_spd(logger::parse_level(cfg.level)));
        logger_->flush_on(detail::to_spd(parse_level(cfg.flush_on)));

        spdlog::register_logger(logger_);
    }

    void shutdown() {
        if (!inited_.load()) return;
        // flush and shutdown spdlog (also stops thread pool)
        spdlog::shutdown();
        logger_.reset();
        sinks_.clear();
        inited_.store(false);
    }

    void set_level(Level lv) {
        if (logger_) logger_->set_level(detail::to_spd(lv));
    }

    // Core log
    void log(Level lv,
             std::string_view component,
             std::string_view event,
             std::initializer_list<Field> fields = {}) {
        auto lg = logger_;
        if (!lg) return;

        // fast level check: avoid JSON building if filtered out
        const auto spdlv = detail::to_spd(lv);
        if (!lg->should_log(spdlv)) return;

        thread_local fmt::memory_buffer buf;
        buf.clear();

        // JSON object start
        buf.push_back('{');

        // ts_ms
        fmt::format_to(std::back_inserter(buf), "\"ts_ms\":{}", detail::now_epoch_ms());

        // level
        fmt::format_to(std::back_inserter(buf), ",\"level\":\"{}\"", detail::level_str(lv));

        // component
        fmt::format_to(std::back_inserter(buf), ",\"component\":\"");
        detail::append_json_escaped(buf, component);
        buf.push_back('\"');

        // event
        fmt::format_to(std::back_inserter(buf), ",\"event\":\"");
        detail::append_json_escaped(buf, event);
        buf.push_back('\"');

        // extra fields
        for (const auto& f : fields) {
            buf.push_back(',');
            if (f.kind == Field::Kind::String) {
                detail::append_kv_string(buf, f.key, f.value);
            } else {
                detail::append_kv_raw(buf, f.key, f.value);
            }
        }

        // end
        buf.push_back('}');
        // one line per record
        lg->log(spdlv, spdlog::string_view_t{buf.data(), buf.size()});
    }

    // Convenience APIs
    void info (std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { log(Level::Info,  c, e, f); }
    void warn (std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { log(Level::Warn,  c, e, f); }
    void error(std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { log(Level::Error, c, e, f); }
    void debug(std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { log(Level::Debug, c, e, f); }

private:
    JsonLogger() = default;

    std::atomic<bool> inited_{false};
    std::shared_ptr<spdlog::logger> logger_;
    std::vector<spdlog::sink_ptr> sinks_;
};

// ------------------------------
// logger options and helper
// ------------------------------
inline Level parse_level(std::string_view s) {
    auto lower = [](unsigned char c) { return static_cast<char>(std::tolower(c)); };
    std::string tmp;
    tmp.reserve(s.size());
    for (unsigned char c : std::string(s)) tmp.push_back(lower(c));

    if (tmp == "debug") return Level::Debug;
    if (tmp == "info")  return Level::Info;
    if (tmp == "warn" || tmp == "warning") return Level::Warn;
    if (tmp == "error" || tmp == "err") return Level::Error;
    return Level::Info;
}

inline bool env_bool(const char* v, bool defv) {
    if (!v) return defv;
    return !(std::strcmp(v, "0") == 0 || std::strcmp(v, "false") == 0 || std::strcmp(v, "FALSE") == 0);
}

inline JsonLogger::Options apply_env(JsonLogger::Options opt, std::string_view app_name) {
    if (const char* v = std::getenv("LOG_LEVEL")) {
        opt.level = parse_level(v);
    }
    if (const char* v = std::getenv("LOG_CONSOLE")) {
        opt.also_console = env_bool(v, opt.also_console);
    }
    if (const char* v = std::getenv("LOG_OVERFLOW")) {
        if (std::strcmp(v, "block") == 0) opt.overflow = OverflowPolicy::Block;
        if (std::strcmp(v, "drop")  == 0) opt.overflow = OverflowPolicy::OverrunOldest;
    }
    if (const char* v = std::getenv("LOG_FILE")) {
        opt.file_path = v;
    } else if (const char* dir = std::getenv("LOG_DIR")) {
        opt.file_path = (std::filesystem::path(dir) / (std::string(app_name) + ".jsonl")).string();
    }
    return opt;
}
inline JsonLogger::Options default_options_for(std::string_view app_name) {
    JsonLogger::Options opt;
    opt.file_path = fmt::format("logs/{}.jsonl", app_name);
    opt.also_console = true; // dev on; prod off
    opt.level = Level::Info;
    opt.queue_size = 1u << 16;
    opt.worker_threads = 1;
    opt.overflow = OverflowPolicy::OverrunOldest;
    opt.flush_on = spdlog::level::warn;

    return apply_env(std::move(opt), app_name);
}

// ------------------------------
// Free functions (nice call sites)
// ------------------------------
inline void init(const cfg::LoggerCfg& cfg) { JsonLogger::instance().init(cfg); }
inline void shutdown() { JsonLogger::instance().shutdown(); }
inline void set_level(Level lv) { JsonLogger::instance().set_level(lv); }

inline void info (std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { JsonLogger::instance().info(c,e,f); }
inline void warn (std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { JsonLogger::instance().warn(c,e,f); }
inline void error(std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { JsonLogger::instance().error(c,e,f); }
inline void debug(std::string_view c, std::string_view e, std::initializer_list<Field> f = {}) { JsonLogger::instance().debug(c,e,f); }

} // namespace logger
