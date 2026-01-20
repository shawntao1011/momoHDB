#include "config/ConfigLoader.hpp"

#include <yaml-cpp/yaml.h>
#include <filesystem>

#include "common/JsonLogger.hpp"
#include "common/SPSCRing.hpp"

namespace fs = std::filesystem;

static std::expected<std::string, cfg::ConfigError>
require_scalar(const YAML::Node& n,
               const std::string& file,
               const std::string& path) {
    if (!n || !n.IsScalar()) {
        return std::unexpected(cfg::ConfigError{file, path, "required scalar missing"});
    }
    return n.as<std::string>();
}

static logger::OverflowPolicy to_logger_overflow(cfg::OverflowPolicy p) {
    switch (p) {
        case cfg::OverflowPolicy::Block: return logger::OverflowPolicy::Block;
        case cfg::OverflowPolicy::DropOldest: return logger::OverflowPolicy::OverrunOldest;
    }
    std::abort();
}

static queue::OverflowPolicy to_queue_overflow(cfg::OverflowPolicy p) {
    switch (p) {
        case cfg::OverflowPolicy::Block: return queue::OverflowPolicy::Block;
        case cfg::OverflowPolicy::DropOldest: return queue::OverflowPolicy::DropOldest;
    }
    std::abort();
}

std::expected<cfg::AppConfig, cfg::ConfigError>
cfg::ConfigLoader::load(const std::string& path) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (...) {
        return std::unexpected(cfg::ConfigError{path, "", "failed to load yaml"});
    }

    cfg::AppConfig cfg;
    fs::path base_dir = fs::absolute(fs::path(path)).parent_path();

    // -------- json logger --------
    {
        auto n = root["logger"];
        if (!n || !n.IsMap())
            return std::unexpected(ConfigError{path, "logger", "missing logger section"});

        cfg.logger.level = n["level"].as<std::string>("info");
        cfg.logger.also_console = n["also_console"].as<bool>(true);
        cfg.logger.file_path = n["path"].as<std::string>("logs/futu_feeder.log");
    }

    // -------- futu session --------
    {
        auto n = root["futu"];
        if (!n || !n.IsMap())
            return std::unexpected(ConfigError{path, "futu", "missing futu section"});

        cfg.futu.opend_ip =
            require_scalar(n["opend_ip"], path, "futu.opend_ip").value();
        cfg.futu.opend_port =
            static_cast<uint16_t>(n["opend_port"].as<int>());
    }

    // -------- subscriptions --------
    {
        auto n = root["subscriptions"];
        if (!n || !n.IsMap())
            return std::unexpected(ConfigError{path, "subscriptions", "missing subscription section"});

        auto p = require_scalar(n["path"], path, "subscriptions.path").value();
        cfg.subscription.path = (base_dir / p).string();

    }

    // -------- subscription manager --------
    {
        auto n = root["subscriptionmanager"];
        if (!n || !n.IsMap())
            return std::unexpected(ConfigError{path, "submanager", "missing subscription manager section"});

        cfg.submanager.refresh_ms = n["refresh_ms"].as<int>();
        cfg.submanager.capacity =n["capacity"].as<std::size_t>(1024);
        cfg.submanager.overflow =
            parse_overflow(n["overflow"].as<std::string>("drop_oldest"));
    }

    // -------- downstreams --------
    auto arr = root["downstreams"];
    if (!arr || !arr.IsSequence())
        return std::unexpected(ConfigError{path, "downstreams", "must be sequence"});

    for (std::size_t i = 0; i < arr.size(); ++i) {
        const auto& d = arr[i];
        std::string base = "downstreams[" + std::to_string(i) + "]";

        DownstreamCfg ds;
        ds.name = require_scalar(d["name"], path, base + ".name").value();

        // sink
        auto type = require_scalar(d["type"], path, base + ".type").value();
        if (type == "redpanda") {
            auto r = d["redpanda"];
            RedpandaSinkCfg rc;
            rc.brokers =
                require_scalar(r["brokers"], path, base + ".redpanda.brokers").value();
            rc.client_id = r["client_id"].as<std::string>("futu-feedhandler");
            rc.compression = r["compression"].as<std::string>("lz4");
            rc.acks = r["acks"].as<std::string>("all");
            ds.sink = rc;
        } else if (type == "demo_log") {
            ds.sink = DemoLogSinkCfg{};
        } else {
            return std::unexpected(ConfigError{path, base + ".type", "unknown type"});
        }

        cfg.downstreams.emplace_back(std::move(ds));
    }

    return cfg;
}