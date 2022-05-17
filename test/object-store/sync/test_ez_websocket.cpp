#include <catch2/catch.hpp>
#include "util/test_file.hpp"
#include "util/test_utils.hpp"
#include <realm/util/random.hpp>

#include <memory>

#include <realm/util/network.hpp>
#include <realm/util/websocket.hpp>
#include <realm/util/ez_websocket.hpp>
#include <realm/sync/noinst/client_impl_base.hpp>

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/schema.hpp>
#endif

//TEST_CASE("ez socket: test should fail", "[ez socket]") {
//    SECTION("test is reachable") {
//        REQUIRE(false);
//    }
//}

using namespace realm;
using namespace realm::sync;
using namespace realm::util::websocket;

// * SyncClient tests
//     * Should have m_client.socket_factory set
// * SyncClientConfig .socket_factory
// * ClientConfig .socket_factory
// * SyncConfig .socket_factory
// * ClientImpl can have custom m_socket_factory


// create SyncClientConfig
// check value of socket_factory
// call connect(_, _)
// check result
// call async_write_binary
namespace {

class TestEZSocket: public EZSocket {

    void async_write_binary(const char* data, size_t size, util::UniqueFunction<void()>&& handler) override {
        CHECK(data);
        CHECK(size > 0);
        CHECK(handler);
    }
};

class TestSocketFactory: public EZSocketFactory {
    std::unique_ptr<EZSocket> connect(EZObserver* observer, EZEndpoint&& endpoint) override {
        return std::make_unique<TestEZSocket>();
    }
};

class StubEZObserver: public EZObserver {
    void websocket_handshake_completion_handler(const std::string& protocol) override { }
    void websocket_connect_error_handler(std::error_code) override { }
    void websocket_ssl_handshake_error_handler(std::error_code) override { }
    void websocket_read_or_write_error_handler(std::error_code) override { }
    void websocket_handshake_error_handler(std::error_code, const std::string_view* body) override { }
    void websocket_protocol_error_handler(std::error_code) override { }
    bool websocket_binary_message_received(const char* data, size_t size) override { }
    bool websocket_close_message_received(std::error_code error_code, StringData message) override { }
};
}

TEST_CASE("Test platform networking") {
    TestLogger logger = TestLogger();

    StubEZObserver observer = StubEZObserver();
    EZEndpoint endpoint;
    ClientConfig config = ClientConfig();
//    ClientImpl cImp = ClientImpl(config);
    util::network::Service service = util::network::Service();
    std::mt19937_64 m_random;
    util::seed_prng_nondeterministically(m_random);

    EZConfig ezConfig = EZConfig{
        logger, // util::Logger& logger;
        m_random, // std::mt19937_64& random;
        service, // util::network::Service& service;
        "Test agent"// std::string user_agent;
    };

    SECTION("EZSocketFactory tests") {
        EZSocketFactory sf = EZSocketFactory(ezConfig);
        std::map<std::string, std::string> headers = {};
        util::Optional<std::string> str = {};
        std::function<SyncConfig::SSLVerifyCallback> ssl_verify_callback = {};
        util::Optional<SyncConfig::ProxyConfig> proxy = {};

        auto websocket = sf.connect(&observer, util::websocket::EZEndpoint{
            "localhost", //std::string address;
            1, //port_type port;
            "", //std::string path;      // Includes auth token in query.
            "realm.io", //std::string protocols; // separated with ", "
            true, //bool is_ssl;
            headers, //std::map<std::string, std::string> headers; // Only includes "custom" headers.
            false, //bool verify_servers_ssl_certificate;
            str, //util::Optional<std::string> ssl_trust_certificate_path;
            ssl_verify_callback, //std::function<SyncConfig::SSLVerifyCallback> ssl_verify_callback;
            proxy //util::Optional<SyncConfig::ProxyConfig> proxy;
        });
        REQUIRE(websocket);
        
        bool didCall = false;
        auto handler = [&] {
            didCall = true;
        };
        ClientImpl::OutputBuffer out;

        websocket->async_write_binary(out.data(), out.size(), std::move(handler));
        REQUIRE(didCall);
    }
}


