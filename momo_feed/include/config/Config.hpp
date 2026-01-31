# pragma once
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "OverflowPolicy.hpp"

namespace cfg {

// ---------- logger ----------
struct LoggerCfg {
    std::string file_path{"logs/app.jsonl"};
    bool also_console{true};
    std::string level{"Info"};

    std::size_t queue_size{1u << 16};
    std::size_t worker_threads{1};
    OverflowPolicy overflow{OverflowPolicy::DropOldest};

    bool json{true};

    std::string flush_on{"warn"};
};

// ---------- session ----------
struct FutuSessionCfg {
    std::string opend_ip;
    uint16_t opend_port{0};
};

// ---------- subscription ----------
struct SubscriptionCfg {
    std::string path;
};

// ---------- queue ----------
struct SubscriptionManagerCfg {
    int refresh_ms{5000};
    std::size_t capacity{1024};
    OverflowPolicy overflow{OverflowPolicy::DropOldest};
    bool first_push{false};
};

// ---------- sinks ----------
struct RedpandaSinkCfg {
    std::string brokers;
    std::string client_id{"futu-feedhandler"};
    std::string compression{"lz4"};
    std::string acks{"all"};
};

struct DemoLogSinkCfg {};

using SinkCfg = std::variant<RedpandaSinkCfg, DemoLogSinkCfg>;

struct DownstreamCfg {
    std::string name;
    SinkCfg sink;
};

// ---------- app ----------
struct AppConfig {
    LoggerCfg logger;
    FutuSessionCfg futu;
    SubscriptionCfg subscription;
    SubscriptionManagerCfg submanager;
    std::vector<DownstreamCfg> downstreams;
};

} // namespace cfg