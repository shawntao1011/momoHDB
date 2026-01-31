#include "config/ConfigLoader.hpp"

#include <yaml-cpp/yaml.h>
#include <filesystem>

#include "common/JsonLogger.hpp"
#include "common/SPSCRing.hpp"

namespace fs = std::filesystem;

template <typename T>
static std::expected<T, cfg::ConfigError> require_scalar_as(
    const YAML::Node& n,
    const std::string& file,
    const std::string& path) {
    if (!n || !n.IsScalar()) {
        return std::unexpected(cfg::ConfigError{file, path, "required scalar missing"});
    }
    try {
        return n.as<T>();
    } catch (const std::exception& ex) {
        return std::unexpected(cfg::ConfigError{file, path, ex.what()});
    }
}

template <typename T>
static std::expected<T, cfg::ConfigError> optional_scalar_as(
    const YAML::Node& n,
    const std::string& file,
    const std::string& path,
    T default_value) {
    if (!n) {
        return default_value;
    }
    if (!n.IsScalar()) {
        return std::unexpected(cfg::ConfigError{file, path, "expected scalar value"});
    }
    try {
        return n.as<T>();
    } catch (const std::exception& ex) {
        return std::unexpected(cfg::ConfigError{file, path, ex.what()});
    }
}

static std::expected<cfg::OverflowPolicy, cfg::ConfigError> parse_overflow_policy(
    std::string_view value,
    const std::string& file,
    const std::string& path) {
    try {
        return cfg::parse_overflow(value);
    } catch (const std::exception& ex) {
        return std::unexpected(cfg::ConfigError{file, path, ex.what()});
    }
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

        auto level = optional_scalar_as<std::string>(
            n["level"], path, "logger.level", cfg.logger.level);
        if (!level) return std::unexpected(level.error());
        cfg.logger.level = *level;

        auto also_console = optional_scalar_as<bool>(
            n["also_console"], path, "logger.also_console", cfg.logger.also_console);
        if (!also_console) return std::unexpected(also_console.error());
        cfg.logger.also_console = *also_console;

        if (n["path"]) {
            auto file_path = optional_scalar_as<std::string>(
                n["path"], path, "logger.path", cfg.logger.file_path);
            if (!file_path) return std::unexpected(file_path.error());
            cfg.logger.file_path = *file_path;
        } else {
            auto file_path = optional_scalar_as<std::string>(
                n["file"], path, "logger.file", cfg.logger.file_path);
            if (!file_path) return std::unexpected(file_path.error());
            cfg.logger.file_path = *file_path;
        }

        auto queue_size = optional_scalar_as<std::size_t>(
            n["queue_size"], path, "logger.queue_size", cfg.logger.queue_size);
        if (!queue_size) return std::unexpected(queue_size.error());
        cfg.logger.queue_size = *queue_size;

        auto worker_threads = optional_scalar_as<std::size_t>(
            n["worker_threads"], path, "logger.worker_threads", cfg.logger.worker_threads);
        if (!worker_threads) return std::unexpected(worker_threads.error());
        cfg.logger.worker_threads = *worker_threads;

        auto overflow_value = optional_scalar_as<std::string>(
            n["overflow"], path, "logger.overflow", "drop_oldest");
        if (!overflow_value) return std::unexpected(overflow_value.error());
        auto overflow = parse_overflow_policy(*overflow_value, path, "logger.overflow");
        if (!overflow) return std::unexpected(overflow.error());
        cfg.logger.overflow = *overflow;

        auto flush_on = optional_scalar_as<std::string>(
            n["flush_on"], path, "logger.flush_on", cfg.logger.flush_on);
        if (!flush_on) return std::unexpected(flush_on.error());
        cfg.logger.flush_on = *flush_on;
    }

    // -------- futu session --------
    {
        auto n = root["futu"];
        if (!n || !n.IsMap())
            return std::unexpected(ConfigError{path, "futu", "missing futu section"});

        auto opend_ip = require_scalar_as<std::string>(
            n["opend_ip"], path, "futu.opend_ip");
        if (!opend_ip) return std::unexpected(opend_ip.error());
        cfg.futu.opend_ip = *opend_ip;

        auto opend_port = require_scalar_as<int>(
            n["opend_port"], path, "futu.opend_port");
        if (!opend_port) return std::unexpected(opend_port.error());
        if (*opend_port <= 0 || *opend_port > 65535) {
            return std::unexpected(ConfigError{path, "futu.opend_port", "port out of range"});
        }
        cfg.futu.opend_port = static_cast<uint16_t>(*opend_port);
    }

    // -------- subscriptions --------
    {
        auto n = root["subscriptions"];
        if (!n || !n.IsMap())
            return std::unexpected(ConfigError{path, "subscriptions", "missing subscription section"});

        auto p = require_scalar_as<std::string>(
            n["path"], path, "subscriptions.path");
        if (!p) return std::unexpected(p.error());
        cfg.subscription.path = (base_dir / p.value()).string();
    }

    // -------- subscription manager --------
    {
        auto n = root["subscriptionmanager"];
        if (!n || !n.IsMap())
            return std::unexpected(ConfigError{path, "submanager", "missing subscription manager section"});

        auto refresh_ms = require_scalar_as<int>(
            n["refresh_ms"], path, "subscriptionmanager.refresh_ms");
        if (!refresh_ms) return std::unexpected(refresh_ms.error());
        if (*refresh_ms <= 0) {
            return std::unexpected(ConfigError{path, "subscriptionmanager.refresh_ms",
                "refresh_ms must be greater than zero"});
        }
        cfg.submanager.refresh_ms = *refresh_ms;

        auto capacity = optional_scalar_as<std::size_t>(
            n["capacity"], path, "subscriptionmanager.capacity", cfg.submanager.capacity);
        if (!capacity) return std::unexpected(capacity.error());
        cfg.submanager.capacity = *capacity;

        auto overflow_value = optional_scalar_as<std::string>(
            n["overflow"], path, "subscriptionmanager.overflow", "drop_oldest");
        if (!overflow_value) return std::unexpected(overflow_value.error());
        auto overflow = parse_overflow_policy(*overflow_value, path, "subscriptionmanager.overflow");
        if (!overflow) return std::unexpected(overflow.error());
        cfg.submanager.overflow = *overflow;

        auto first_push = optional_scalar_as<bool>(
            n["first_push"], path, "subscriptionmanager.first_push", cfg.submanager.first_push);
        if (!first_push) return std::unexpected(first_push.error());
        cfg.submanager.first_push = *first_push;
    }

    // -------- downstreams --------
    auto arr = root["downstreams"];
    if (!arr || !arr.IsSequence())
        return std::unexpected(ConfigError{path, "downstreams", "must be sequence"});

    for (std::size_t i = 0; i < arr.size(); ++i) {
        const auto& d = arr[i];
        std::string base = "downstreams[" + std::to_string(i) + "]";

        DownstreamCfg ds;
        auto name = require_scalar_as<std::string>(d["name"], path, base + ".name");
        if (!name) return std::unexpected(name.error());
        ds.name = *name;

        // sink
        auto type = require_scalar_as<std::string>(d["type"], path, base + ".type");
        if (!type) return std::unexpected(type.error());
        if (type == "redpanda") {
            auto r = d["redpanda"];
            if (!r || !r.IsMap()) {
                return std::unexpected(ConfigError{path, base + ".redpanda", "missing redpanda section"});
            }
            RedpandaSinkCfg rc;
            auto brokers = require_scalar_as<std::string>(
                 r["brokers"], path, base + ".redpanda.brokers");
            if (!brokers) return std::unexpected(brokers.error());
            rc.brokers = *brokers;

            auto client_id = optional_scalar_as<std::string>(
                r["client_id"], path, base + ".redpanda.client_id", rc.client_id);
            if (!client_id) return std::unexpected(client_id.error());
            rc.client_id = *client_id;

            auto compression = optional_scalar_as<std::string>(
                r["compression"], path, base + ".redpanda.compression", rc.compression);
            if (!compression) return std::unexpected(compression.error());
            rc.compression = *compression;

            auto acks = optional_scalar_as<std::string>(
                r["acks"], path, base + ".redpanda.acks", rc.acks);
            if (!acks) return std::unexpected(acks.error());
            rc.acks = *acks;
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