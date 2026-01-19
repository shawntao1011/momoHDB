#include <cstdint>
#include <string>
#include <variant>
#include <vector>

// ---------- session ----------
struct FutuSessionCfg {
    std::string opend_ip;
    uint16_t opend_port{0};
};

// ---------- subscription ----------
struct SubscriptionCfg {
    std::string path;
    int refresh_ms{5000};
};

// ---------- queue ----------
enum class OverflowPolicy {
    DropOldest,
    Block
};

struct QueueCfg {
    std::size_t capacity{1024};
    OverflowPolicy overflow{OverflowPolicy::DropOldest};
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
    QueueCfg queue;
    SinkCfg sink;
};

// ---------- app ----------
struct AppConfig {
    FutuSessionCfg futu;
    SubscriptionCfg subscription;
    std::vector<DownstreamCfg> downstreams;
};