#include "util/sync/mockable_proxy_server.hpp"

#include <realm/sync/network/default_socket.hpp>
#include <realm/util/platform_info.hpp>

#include <catch2/catch_all.hpp>

namespace realm {
namespace {
class MockableProxySocketProvider : public std::enable_shared_from_this<MockableProxySocketProvider>,
                                    public sync::SyncSocketProvider {
public:
    using WebSocketEvent = sync::WebSocketEvent;
    explicit MockableProxySocketProvider(std::shared_ptr<sync::SyncSocketProvider> proxied_provider,
                                         MockableProxySocketProviderCallbacks&& callbacks)
        : m_proxied_provider(std::move(proxied_provider))
        , m_callbacks(std::move(callbacks))
    {
        if (!m_callbacks.on_websocket_event) {
            m_callbacks.on_websocket_event = [](uint64_t, WebSocketEvent&& event) -> util::Future<WebSocketEvent> {
                return event;
            };
        }

        if (!m_callbacks.on_websocket_create) {
            m_callbacks.on_websocket_create = [](uint64_t, sync::WebSocketEndpoint&& ep) {
                return ep;
            };
        }

        if (!m_callbacks.on_websocket_send) {
            m_callbacks.on_websocket_send = [](uint64_t,
                                               util::Span<const char> data) -> util::Future<util::Span<const char>> {
                return data;
            };
        }
    }


    std::unique_ptr<sync::WebSocketInterface> connect(util::UniqueFunction<void(WebSocketEvent&&)> observer,
                                                      sync::WebSocketEndpoint&& endpoint) override;

    void post(FunctionHandler&& handler) override
    {
        m_proxied_provider->post(std::move(handler));
    }

    SyncTimer create_timer(std::chrono::milliseconds delay, FunctionHandler&& handler) override
    {
        return m_proxied_provider->create_timer(delay, std::move(handler));
    }

    void stop(bool what) override
    {
        return m_proxied_provider->stop(what);
    }

private:
    struct MockableProxySocketState : public util::AtomicRefCountBase {
        MockableProxySocketState(uint64_t conn_id, std::shared_ptr<MockableProxySocketProvider> parent,
                                 util::UniqueFunction<void(WebSocketEvent&&)> proxied_observer)
            : conn_id(conn_id)
            , provider(std::move(parent))
            , proxied_observer(std::move(proxied_observer))
        {
        }
        uint64_t conn_id;
        std::shared_ptr<MockableProxySocketProvider> provider;
        util::UniqueFunction<void(WebSocketEvent&&)> proxied_observer;
        std::unique_ptr<sync::WebSocketInterface> proxied_conn;
        bool closed = false;
        uint64_t sent_msgs = 0;
    };

    class MockableProxySocket : public sync::WebSocketInterface {
    public:
        MockableProxySocket(util::bind_ptr<MockableProxySocketState> state)
            : m_state(std::move(state))
        {
        }

        ~MockableProxySocket()
        {
            close();
        }

        void async_write_binary(util::Span<const char> data,
                                sync::SyncSocketProvider::FunctionHandler&& handler) override;
        void close();

    private:
        util::bind_ptr<MockableProxySocketState> m_state;
    };

    const std::shared_ptr<sync::SyncSocketProvider> m_proxied_provider;
    MockableProxySocketProviderCallbacks m_callbacks;
    uint64_t m_conn_counter = 0;
};

std::unique_ptr<sync::WebSocketInterface>
MockableProxySocketProvider::connect(util::UniqueFunction<void(WebSocketEvent&&)> observer,
                                     sync::WebSocketEndpoint&& endpoint)
{
    auto conn_id = ++m_conn_counter;

    auto state = util::make_bind<MockableProxySocketState>(conn_id, shared_from_this(), std::move(observer));
    auto wrapped_observer = [state, self = shared_from_this()](WebSocketEvent&& event) {
        self->m_callbacks.on_websocket_event(state->conn_id, std::move(event))
            .get_async([state](StatusWith<WebSocketEvent> sw_event) {
                if (state->closed) {
                    return;
                }
                REALM_ASSERT(sw_event.is_ok());
                auto event = std::move(sw_event.get_value());
                if (auto close_event = mpark::get_if<WebSocketEvent::Close>(&event.event);
                    close_event && !close_event->was_clean) {
                    state->proxied_observer(WebSocketEvent{WebSocketEvent::Error{}});
                }
                state->proxied_observer(std::move(event));
            });
    };
    auto real_ep = m_callbacks.on_websocket_create(conn_id, std::move(endpoint));
    state->proxied_conn = m_proxied_provider->connect(std::move(wrapped_observer), std::move(real_ep));
    return std::make_unique<MockableProxySocket>(state);
}

void MockableProxySocketProvider::MockableProxySocket::async_write_binary(
    util::Span<const char> data, sync::SyncSocketProvider::FunctionHandler&& handler)
{
    auto wrapped_handler = [handler = std::move(handler), state = m_state](Status status) {
        if (state->closed) {
            return;
        }

        handler(status);
    };
    m_state->provider->m_callbacks.on_websocket_send(m_state->conn_id, data)
        .get_async([handler = std::move(wrapped_handler),
                    state = m_state](StatusWith<util::Span<const char>> sw_data) mutable {
            auto logger = util::Logger::get_default_logger();
            if (state->closed) {
                return;
            }
            if (sw_data.is_ok()) {
                state->proxied_conn->async_write_binary(sw_data.get_value(), std::move(handler));
                return;
            }

            state->provider->post([state, handler = std::move(handler), send_status = sw_data.get_status()](Status) {
                if (state->closed) {
                    return;
                }
                handler(send_status);
                state->closed = true;

                state->proxied_observer(WebSocketEvent{WebSocketEvent::Error{}});
                state->proxied_observer(WebSocketEvent{WebSocketEvent::Close{
                    false, sync::websocket::WebSocketError::websocket_read_error, send_status.reason()}});
                state->proxied_conn.reset();
            });
        });
}

void MockableProxySocketProvider::MockableProxySocket::close()
{
    if (m_state->provider->m_callbacks.on_websocket_close) {
        m_state->provider->m_callbacks.on_websocket_close(m_state->conn_id);
    }
    m_state->proxied_conn.reset();
    m_state->closed = true;
}

std::shared_ptr<sync::SyncSocketProvider>
set_and_get_global_socket_provider(std::optional<std::shared_ptr<sync::SyncSocketProvider>> update)
{
    static std::shared_ptr<sync::SyncSocketProvider> global_socket_provider = nullptr;
    if (update) {
        global_socket_provider = *update;
    }
    return global_socket_provider;
}

bool set_and_get_testing_socket_provider_disabled(std::optional<bool> update)
{
    static std::atomic<bool> disabled = false;
    if (update) {
        disabled.store(*update);
    }
    return disabled.load();
}

struct DroppingSocketsState : public util::RefCountBase {
    struct DroppingSocketState {
        std::bernoulli_distribution dist;
    };

    std::default_random_engine rand;
    std::unordered_map<uint64_t, DroppingSocketState> conn_states;
};

std::shared_ptr<sync::SyncSocketProvider> create_dropping_socket_provider(std::default_random_engine random_engine,
                                                                          std::shared_ptr<util::Logger> logger)
{
    auto default_socket_provider = std::make_shared<sync::websocket::DefaultSocketProvider>(
        logger, util::format("RealmSyncDroppy/%1 (%2)", REALM_VERSION_STRING, util::get_platform_info()));

    MockableProxySocketProviderCallbacks callbacks;
    auto state = util::make_bind<DroppingSocketsState>();
    state->rand = std::move(random_engine);
    callbacks.on_websocket_send = [state](uint64_t conn_id,
                                          util::Span<const char> data) -> util::Future<util::Span<const char>> {
        if (set_and_get_testing_socket_provider_disabled(std::nullopt)) {
            return data;
        }
        auto [it, inserted] = state->conn_states.insert({conn_id, {std::bernoulli_distribution{1 / 20.0}}});
        if (!it->second.dist(state->rand)) {
            return data;
        }

        return Status{ErrorCodes::ConnectionClosed, "connection closed by chaos during write"};
    };

    callbacks.on_websocket_event = [state](uint64_t conn_id,
                                           sync::WebSocketEvent&& event) -> util::Future<sync::WebSocketEvent> {
        if (set_and_get_testing_socket_provider_disabled(std::nullopt)) {
            return event;
        }

        if (event.is_terminal_event()) {
            return event;
        }

        auto [it, inserted] = state->conn_states.insert({conn_id, {std::bernoulli_distribution{1 / 20.0}}});
        if (!it->second.dist(state->rand)) {
            return event;
        }
        if (mpark::holds_alternative<sync::WebSocketEvent::Open>(event.event)) {
            return sync::WebSocketEvent{sync::WebSocketEvent::Close{
                false, sync::websocket::WebSocketError::websocket_connection_failed, "connection failed by chaos"}};
        }
        return sync::WebSocketEvent{sync::WebSocketEvent::Close{
            false, sync::websocket::WebSocketError::websocket_read_error, "connection closed by chaos"}};
    };

    return create_mockable_proxy_socket_provider(std::move(default_socket_provider), std::move(callbacks));
}

} // namespace

std::shared_ptr<sync::SyncSocketProvider>
create_mockable_proxy_socket_provider(std::shared_ptr<sync::SyncSocketProvider> proxied_provider,
                                      MockableProxySocketProviderCallbacks&& callbacks)
{
    return std::make_shared<MockableProxySocketProvider>(std::move(proxied_provider), std::move(callbacks));
}

class NetworkChaosManager : public Catch::EventListenerBase {
public:
    using Catch::EventListenerBase::EventListenerBase;

    void testRunStarting(Catch::TestRunInfo const&) override
    {
        auto enable_chaos = [] {
            auto enable_chaos_ptr = std::getenv("UNITTEST_ENABLE_NETWORK_CHAOS");
            return enable_chaos_ptr ? std::string_view(enable_chaos_ptr) : std::string_view{};
        }();

        std::initializer_list<std::string_view> enabled_values = {"1", "On", "on"};
        if (!std::any_of(enabled_values.begin(), enabled_values.end(), [&](const std::string_view& val) {
                return val == enable_chaos;
            })) {
            return;
        }

        auto logger = util::Logger::get_default_logger();
        logger->info("Running tests with network chaos enabled");
        std::seed_seq sseq{Catch::getSeed()};
        m_chaos_provider = create_dropping_socket_provider(std::default_random_engine(sseq), logger);
    }

    void testCaseStarting(Catch::TestCaseInfo const& testInfo) override
    {
        auto logger = util::Logger::get_default_logger();
        if (!m_chaos_provider) {
            return;
        }

        if (std::find_if(testInfo.tags.begin(), testInfo.tags.end(), [](const Catch::Tag& tag) {
                return tag.original == "no network chaos";
            }) != testInfo.tags.end()) {
            logger->info("Disabling network chaos for test %1", testInfo.name);
            m_disabled_for_current_test = true;
            set_and_get_global_socket_provider(nullptr);
        }
        else {
            m_disabled_for_current_test = false;
            set_and_get_global_socket_provider(m_chaos_provider);
        }
    }

    void testRunEnded(Catch::TestRunStats const&) override
    {
        m_chaos_provider.reset();
    }

private:
    bool m_disabled_for_current_test = false;
    std::shared_ptr<sync::SyncSocketProvider> m_chaos_provider;
};

CATCH_REGISTER_LISTENER(NetworkChaosManager)

std::shared_ptr<sync::SyncSocketProvider> get_testing_sync_socket_provider()
{
    return set_and_get_global_socket_provider(std::nullopt);
}

std::optional<sync::ResumptionDelayInfo> get_testing_resumption_delay_info()
{
    if (!get_testing_sync_socket_provider() || set_and_get_testing_socket_provider_disabled(std::nullopt)) {
        return std::nullopt;
    }
    sync::ResumptionDelayInfo delay_info;
    delay_info.max_resumption_delay_interval = std::chrono::seconds(1);
    delay_info.resumption_delay_interval = std::chrono::seconds(1);
    delay_info.resumption_delay_backoff_multiplier = 1;
    return delay_info;
}

DisableNetworkChaosGuard::DisableNetworkChaosGuard()
{
    set_and_get_testing_socket_provider_disabled(true);
}

DisableNetworkChaosGuard::~DisableNetworkChaosGuard()
{
    set_and_get_testing_socket_provider_disabled(false);
}

} // namespace realm
