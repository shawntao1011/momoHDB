#pragma once
#include "config/AppConfigLoader.hpp"

#include <yaml-cpp/yaml.h>
#include <filesystem>

namespace fs = std::filesystem;

static std::expected<std::string, ConfigError>
require_scalar(const YAML::Node& n,
               const std::string& file,
               const std::string& path) {
    if (!n || !n.IsScalar()) {
        return std::unexpected(ConfigError{file, path, "required scalar missing"});
    }
    return n.as<std::string>();
}

static OverflowPolicy parse_overflow(const std::string& s) {
    if (s == "block") return OverflowPolicy::Block;
    return OverflowPolicy::DropOldest;
}

std::expected<AppConfig, ConfigError>
    AppConfigLoader::load(const std::string& path) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (...) {
        return std::unexpected(ConfigError{path, "", "failed to load yaml"});
    }

    AppConfig cfg;
    fs::path base_dir = fs::absolute(fs::path(path)).parent_path();

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
            return std::unexpected(ConfigError{path, "subscriptions", "missing section"});

        auto p = require_scalar(n["path"], path, "subscriptions.path").value();
        cfg.subscription.path = (base_dir / p).string();

        if (n["refresh_ms"])
            cfg.subscription.refresh_ms = n["refresh_ms"].as<int>();
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

        // queue
        auto q = d["queue"];
        ds.queue.capacity = q["capacity"].as<std::size_t>(1024);
        ds.queue.overflow =
            parse_overflow(q["overflow"].as<std::string>("drop_oldest"));

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