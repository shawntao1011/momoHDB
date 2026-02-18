# pragma once
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "LoggerConfig.hpp"
#include "OverflowPolicy.hpp"

namespace cfg {
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
    cfg::OverflowPolicy overflow{cfg::OverflowPolicy::Block};
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
    cfg::LoggerCfg logger;
    FutuSessionCfg futu;
    SubscriptionCfg subscription;
    SubscriptionManagerCfg submanager;
    std::vector<DownstreamCfg> downstreams;
};

} // namespace cfg