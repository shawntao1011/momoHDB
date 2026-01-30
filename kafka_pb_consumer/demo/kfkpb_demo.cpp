#include <cstring>
#include <fcntl.h>
#include <iostream>

#include "k.h"
#include "../src/print_k.cpp"
#include "kfkpb_core.hpp"

namespace {
struct KafkaSettings {
    std::string bootstrap_servers{"192.168.2.209:9092"};
    std::string group_id{"kfkpb_demo_earliest"};
    std::string auto_offset_reset{"earliest"};
};

rd_kafka_t* create_consumer(const KafkaSettings& settings) {
    char errstr[512]{0};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    auto set_conf = [&](const char* key, const std::string& value) {
        if (rd_kafka_conf_set(conf, key, value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::string msg = std::string("rd_kafka_conf_set failed for ") + key + ": " + errstr;
            rd_kafka_conf_destroy(conf);
            throw std::runtime_error(msg);
        }
    };

    set_conf("bootstrap.servers", settings.bootstrap_servers);
    set_conf("group.id", settings.group_id);
    set_conf("auto.offset.reset", settings.auto_offset_reset);
    set_conf("enable.auto.commit", "true");

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("rd_kafka_new consumer failed: ") + errstr);
    }

    rd_kafka_poll_set_consumer(rk);
    return rk;
}

void set_nonblock(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
}

void drain_notify_fd(int fd) {
    char buf[256];
    while (true) {
        const ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n > 0) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        }
        break;
    }
}

std::string indent(int depth) {
    return std::string(static_cast<std::size_t>(depth) * 2, ' ');
}
} // namespace

int main() {
    try {
        int notify_fds[2]{-1, -1};
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, notify_fds) != 0) {
            std::cerr << "socketpair failed: " << std::strerror(errno) << "\n";
            return 1;
        }
        set_nonblock(notify_fds[0]);
        set_nonblock(notify_fds[1]);

        KafkaSettings settings{};
        rd_kafka_t* rk = create_consumer(settings);

        KfkpbClient::ThreadCfg cfg;
        cfg.decode_threads = 2;
        KfkpbClient client(rk, notify_fds[0], cfg);

        std::unordered_map<std::string, KfkpbMsgType> topics{
                {"futu.ticker.pb", KfkpbMsgType::Ticker},
                {"futu.orderbook.pb", KfkpbMsgType::OrderBook},
                {"futu.basicqot.pb", KfkpbMsgType::BasicQuote},
                {"futu.kl_1m.pb", KfkpbMsgType::Kline1M},
            };
        client.subscribe(std::move(topics));

        std::cout << "kfkpb demo started. Waiting for events..." << std::endl;

        while (true) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(notify_fds[1], &rfds);

            timeval tv{};
            tv.tv_sec = 1;
            tv.tv_usec = 0;

            const int rc = select(notify_fds[1] + 1, &rfds, nullptr, nullptr, &tv);
            if (rc > 0 && FD_ISSET(notify_fds[1], &rfds)) {
                drain_notify_fd(notify_fds[1]);
            }

            std::vector<K> events;
            client.drainTo(events);
            for (K ev : events) {
                print_k(ev, std::cout, 0);
                std::cout << "\n";
                r0(ev);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "demo error: " << e.what() << "\n";
        return 1;
    }
}