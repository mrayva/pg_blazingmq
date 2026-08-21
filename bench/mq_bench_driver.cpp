// Minimal C++ nats_asio-based driver for the nats_sidecar multi-instance
// queue-group re-benchmark: publishes msgpack {"region": N} payloads (the
// shape nats_sidecar's matching_engine actually reads, via zerialize - not
// NATS headers), and separately counts/verifies matches on the output side.
// Built ad hoc to replace the nats-py harness used in the original run.
//
// Build (adjust NATS_ASIO to your nats_asio checkout with a configured
// build/ dir, for its FetchContent-populated zerialize/msgpack/vcpkg deps):
//   NATS_ASIO=~/nats_asio; VCPKG=$NATS_ASIO/build/vcpkg_installed/x64-linux/lib
//   c++ -I$NATS_ASIO/include -I$NATS_ASIO/build/_deps/zerialize-src/include \
//     -I$NATS_ASIO/build/_deps/msgpack-src/include \
//     -I$NATS_ASIO/build/_deps/msgpack-build/include \
//     -I$NATS_ASIO/build/_deps/msgpack-build/include/msgpack \
//     -isystem $NATS_ASIO/build/vcpkg_installed/x64-linux/include \
//     -O2 -std=c++23 -Wno-missing-braces -o mq_bench_driver mq_bench_driver.cpp \
//     $VCPKG/libspdlog.a $VCPKG/libfmt.a $VCPKG/libsimdjson.a $VCPKG/libssl.a \
//     $VCPKG/libcrypto.a $VCPKG/libz.a -lpthread -latomic -ldl -lrt
//
// Usage: mq_bench_driver pub <subject> <count>
//        mq_bench_driver sub <subject_pattern> <expected_count> <timeout_ms>
//        mq_bench_driver ctrl <subscribe_subject> <expression>
#include <nats_asio/nats_asio.hpp>
#include <asio/io_context.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <zerialize/zerialize.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <nlohmann/json.hpp>
#include <iostream>
#include <chrono>
#include <atomic>
#include <cstring>

namespace z = zerialize;
using json = nlohmann::json;

static nats_asio::iconnection_sptr make_conn(asio::io_context& ioc, std::atomic<bool>& connected) {
    return nats_asio::create_connection(
        ioc,
        [&connected](nats_asio::iconnection&) -> asio::awaitable<void> {
            connected = true;
            co_return;
        },
        [](nats_asio::iconnection&) -> asio::awaitable<void> { co_return; },
        [](nats_asio::iconnection&, nats_asio::string_view err) -> asio::awaitable<void> {
            std::cerr << "conn error: " << err << "\n";
            co_return;
        },
        std::nullopt); // plain TCP, no TLS - this local nats-server doesn't speak TLS
}

asio::awaitable<void> wait_connected(asio::io_context& ioc, std::atomic<bool>& connected) {
    asio::steady_timer t(ioc);
    while (!connected) {
        t.expires_after(std::chrono::milliseconds(5));
        co_await t.async_wait(asio::use_awaitable);
    }
}

// pub <subject> <count>: publish `count` msgpack {"region": 1..8} payloads, round-robin.
asio::awaitable<void> do_pub(asio::io_context& ioc, std::string subject, long count) {
    std::atomic<bool> connected{false};
    auto conn = make_conn(ioc, connected);
    nats_asio::connect_config conf;
    conf.address = "127.0.0.1";
    conf.port = 4222;
    conn->start(conf);
    co_await wait_connected(ioc, connected);

    auto t0 = std::chrono::steady_clock::now();
    for (long i = 0; i < count; ++i) {
        int region = static_cast<int>(i % 8) + 1;
        auto buf = z::serialize<z::MsgPack>(z::dyn::map({{"region", region}}));
        std::span<const char> payload(reinterpret_cast<const char*>(buf.data()), buf.size());
        co_await conn->publish(subject, payload, std::nullopt);
    }
    auto t1 = std::chrono::steady_clock::now();
    double secs = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "PUB_DONE count=" << count << " secs=" << secs
              << " rate=" << (count / secs) << "\n";
    ioc.stop();
    co_return;
}

// sub <subject_pattern> <expected_count> <timeout_ms>: count msgpack messages,
// verify region==1 in each, stop at expected_count or timeout.
asio::awaitable<void> do_sub(asio::io_context& ioc, std::string pattern, long expected, int timeout_ms) {
    std::atomic<bool> connected{false};
    auto conn = make_conn(ioc, connected);
    nats_asio::connect_config conf;
    conf.address = "127.0.0.1";
    conf.port = 4222;
    conn->start(conf);
    co_await wait_connected(ioc, connected);

    auto total = std::make_shared<std::atomic<long>>(0);
    auto wrong = std::make_shared<std::atomic<long>>(0);
    auto t0 = std::make_shared<std::chrono::steady_clock::time_point>(std::chrono::steady_clock::now());
    auto first_msg = std::make_shared<bool>(true);

    nats_asio::subscribe_options opts;
    auto [sub, status] = co_await conn->subscribe(
        pattern,
        [total, wrong, t0, first_msg](nats_asio::string_view, std::optional<nats_asio::string_view>,
                                       std::span<const char> payload) -> asio::awaitable<void> {
            if (*first_msg) { *t0 = std::chrono::steady_clock::now(); *first_msg = false; }
            try {
                std::span<const uint8_t> bytes(
                    reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
                z::MsgPackDeserializer doc(bytes);
                int64_t region = doc["region"].asInt64();
                if (region != 1) wrong->fetch_add(1);
            } catch (...) {
                wrong->fetch_add(1);
            }
            total->fetch_add(1);
            co_return;
        },
        opts);
    if (status.failed()) {
        std::cerr << "subscribe failed: " << status.error() << "\n";
        co_return;
    }

    asio::steady_timer t(ioc);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (total->load() < expected && std::chrono::steady_clock::now() < deadline) {
        t.expires_after(std::chrono::milliseconds(10));
        co_await t.async_wait(asio::use_awaitable);
    }
    // brief settle window in case more are in flight
    t.expires_after(std::chrono::milliseconds(300));
    co_await t.async_wait(asio::use_awaitable);

    auto t1 = std::chrono::steady_clock::now();
    double secs = std::chrono::duration<double>(t1 - *t0).count();
    long tot = total->load();
    std::cout << "SUB_DONE total=" << tot << " wrong=" << wrong->load()
              << " secs=" << secs << " rate=" << (secs > 0 ? tot / secs : 0.0) << "\n";
    ioc.stop();
    co_return;
}

// ctrl <subscribe_subject> <expression>: send {"expression":..., "client_id":"bench"}
// request, print the reply, exit.
asio::awaitable<void> do_ctrl(asio::io_context& ioc, std::string subject, std::string expr) {
    std::atomic<bool> connected{false};
    auto conn = make_conn(ioc, connected);
    nats_asio::connect_config conf;
    conf.address = "127.0.0.1";
    conf.port = 4222;
    conn->start(conf);
    co_await wait_connected(ioc, connected);

    json req = {{"expression", expr}, {"client_id", "bench"}};
    std::string req_str = req.dump();
    std::span<const char> payload(req_str.data(), req_str.size());
    auto [reply, status] = co_await conn->request(subject, payload, std::chrono::milliseconds(5000));
    if (status.failed()) {
        std::cerr << "CTRL_FAIL " << status.error() << "\n";
        co_return;
    }
    std::cout << "CTRL_REPLY " << std::string(reply.payload.begin(), reply.payload.end()) << "\n";
    ioc.stop();
    co_return;
}

int main(int argc, char** argv) {
    if (argc < 2) { std::cerr << "usage: mq_bench_driver <pub|sub|ctrl> ...\n"; return 1; }
    std::string mode = argv[1];
    asio::io_context ioc;
    if (mode == "pub" && argc == 4) {
        asio::co_spawn(ioc, do_pub(ioc, argv[2], std::atol(argv[3])), asio::detached);
    } else if (mode == "sub" && argc == 5) {
        asio::co_spawn(ioc, do_sub(ioc, argv[2], std::atol(argv[3]), std::atoi(argv[4])), asio::detached);
    } else if (mode == "ctrl" && argc == 4) {
        asio::co_spawn(ioc, do_ctrl(ioc, argv[2], argv[3]), asio::detached);
    } else {
        std::cerr << "bad args\n";
        return 1;
    }
    ioc.run();
    return 0;
}
