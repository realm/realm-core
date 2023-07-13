#include <realm/error_codes.hpp>
#include <realm/status.hpp>
#include <realm/object-store/c_api/util.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/network/websocket.hpp>

namespace realm::c_api {
namespace {

struct CAPITimer : sync::SyncSocketProvider::Timer {
public:
    CAPITimer(realm_userdata_t userdata, int64_t delay_ms, realm_sync_socket_callback_t* handler,
              realm_sync_socket_create_timer_func_t create_timer_func,
              realm_sync_socket_timer_canceled_func_t cancel_timer_func,
              realm_sync_socket_timer_free_func_t free_timer_func)
        : m_handler(handler)
        , m_timer_create(create_timer_func)
        , m_timer_cancel(cancel_timer_func)
        , m_timer_free(free_timer_func)
    {
        m_timer = m_timer_create(userdata, delay_ms, handler);
    }

    /// Cancels the timer and destroys the timer instance.
    ~CAPITimer()
    {
        m_timer_cancel(m_userdata, m_timer);
        m_timer_free(m_userdata, m_timer);
        realm_release(m_handler);
    }

    /// Cancel the timer immediately.
    void cancel() override
    {
        m_timer_cancel(m_userdata, m_timer);
    }

private:
    realm_sync_socket_timer_t m_timer = nullptr;

    realm_userdata_t m_userdata = nullptr;
    realm_sync_socket_callback_t* m_handler = nullptr;
    realm_sync_socket_create_timer_func_t m_timer_create = nullptr;
    realm_sync_socket_timer_canceled_func_t m_timer_cancel = nullptr;
    realm_sync_socket_timer_free_func_t m_timer_free = nullptr;
};

struct CAPIWebSocket : sync::WebSocketInterface {
public:
    CAPIWebSocket(realm_userdata_t userdata, realm_sync_socket_connect_func_t websocket_connect_func,
                  realm_sync_socket_websocket_async_write_func_t websocket_write_func,
                  realm_sync_socket_websocket_free_func_t websocket_free_func, realm_websocket_observer_t* observer,
                  sync::WebSocketEndpoint&& endpoint)
        : m_observer(observer)
        , m_userdata(userdata)
        , m_websocket_connect(websocket_connect_func)
        , m_websocket_async_write(websocket_write_func)
        , m_websocket_free(websocket_free_func)
    {
        realm_websocket_endpoint_t capi_endpoint;
        capi_endpoint.address = endpoint.address.c_str();
        capi_endpoint.port = endpoint.port;
        capi_endpoint.path = endpoint.path.c_str();

        std::vector<const char*> protocols;
        for (size_t i = 0; i < endpoint.protocols.size(); ++i) {
            auto& protocol = endpoint.protocols[i];
            protocols.push_back(protocol.c_str());
        }
        capi_endpoint.protocols = protocols.data();
        capi_endpoint.num_protocols = protocols.size();
        capi_endpoint.is_ssl = endpoint.is_ssl;

        m_socket = m_websocket_connect(m_userdata, capi_endpoint, observer);
    }

    /// Destroys the web socket instance.
    ~CAPIWebSocket()
    {
        m_websocket_free(m_userdata, m_socket);
        realm_release(m_observer);
    }

    void async_write_binary(util::Span<const char> data, sync::SyncSocketProvider::FunctionHandler&& handler) final
    {
        auto shared_handler = std::make_shared<sync::SyncSocketProvider::FunctionHandler>(std::move(handler));
        m_websocket_async_write(m_userdata, m_socket, data.data(), data.size(),
                                new realm_sync_socket_callback_t(std::move(shared_handler)));
    }

private:
    realm_sync_socket_websocket_t m_socket = nullptr;
    realm_websocket_observer_t* m_observer = nullptr;
    realm_userdata_t m_userdata = nullptr;

    realm_sync_socket_connect_func_t m_websocket_connect = nullptr;
    realm_sync_socket_websocket_async_write_func_t m_websocket_async_write = nullptr;
    realm_sync_socket_websocket_free_func_t m_websocket_free = nullptr;
};

struct CAPIWebSocketObserver : sync::WebSocketObserver {
public:
    CAPIWebSocketObserver(std::unique_ptr<sync::WebSocketObserver> observer)
        : m_observer(std::move(observer))
    {
    }

    ~CAPIWebSocketObserver() = default;

    void websocket_connected_handler(const std::string& protocol) final
    {
        m_observer->websocket_connected_handler(protocol);
    }

    void websocket_error_handler() final
    {
        m_observer->websocket_error_handler();
    }

    bool websocket_binary_message_received(util::Span<const char> data) final
    {
        return m_observer->websocket_binary_message_received(data);
    }

    bool websocket_closed_handler(bool was_clean, sync::websocket::WebSocketError code, std::string_view msg) final
    {
        return m_observer->websocket_closed_handler(was_clean, code, msg);
    }

private:
    std::unique_ptr<sync::WebSocketObserver> m_observer;
};

struct CAPISyncSocketProvider : sync::SyncSocketProvider {
    realm_userdata_t m_userdata = nullptr;
    realm_free_userdata_func_t m_free = nullptr;
    realm_sync_socket_post_func_t m_post = nullptr;
    realm_sync_socket_create_timer_func_t m_timer_create = nullptr;
    realm_sync_socket_timer_canceled_func_t m_timer_cancel = nullptr;
    realm_sync_socket_timer_free_func_t m_timer_free = nullptr;
    realm_sync_socket_connect_func_t m_websocket_connect = nullptr;
    realm_sync_socket_websocket_async_write_func_t m_websocket_async_write = nullptr;
    realm_sync_socket_websocket_free_func_t m_websocket_free = nullptr;

    CAPISyncSocketProvider() = default;
    CAPISyncSocketProvider(CAPISyncSocketProvider&& other)
        : m_userdata(std::exchange(other.m_userdata, nullptr))
        , m_free(std::exchange(other.m_free, nullptr))
        , m_post(std::exchange(other.m_post, nullptr))
        , m_timer_create(std::exchange(other.m_timer_create, nullptr))
        , m_timer_cancel(std::exchange(other.m_timer_cancel, nullptr))
        , m_timer_free(std::exchange(other.m_timer_free, nullptr))
        , m_websocket_connect(std::exchange(other.m_websocket_connect, nullptr))
        , m_websocket_async_write(std::exchange(other.m_websocket_async_write, nullptr))
        , m_websocket_free(std::exchange(other.m_websocket_free, nullptr))
    {
        REALM_ASSERT(m_free);
        REALM_ASSERT(m_post);
        REALM_ASSERT(m_timer_create);
        REALM_ASSERT(m_timer_cancel);
        REALM_ASSERT(m_timer_free);
        REALM_ASSERT(m_websocket_connect);
        REALM_ASSERT(m_websocket_async_write);
        REALM_ASSERT(m_websocket_free);
    }

    ~CAPISyncSocketProvider()
    {
        m_free(m_userdata);
    }

    std::unique_ptr<sync::WebSocketInterface> connect(std::unique_ptr<sync::WebSocketObserver> observer,
                                                      sync::WebSocketEndpoint&& endpoint) final
    {
        auto capi_observer = std::make_shared<CAPIWebSocketObserver>(std::move(observer));
        return std::make_unique<CAPIWebSocket>(m_userdata, m_websocket_connect, m_websocket_async_write,
                                               m_websocket_free, new realm_websocket_observer_t(capi_observer),
                                               std::move(endpoint));
    }

    void post(FunctionHandler&& handler) final
    {
        auto shared_handler = std::make_shared<FunctionHandler>(std::move(handler));
        m_post(m_userdata, new realm_sync_socket_callback_t(std::move(shared_handler)));
    }

    SyncTimer create_timer(std::chrono::milliseconds delay, FunctionHandler&& handler) final
    {
        auto shared_handler = std::make_shared<FunctionHandler>(std::move(handler));
        return std::make_unique<CAPITimer>(m_userdata, delay.count(),
                                           new realm_sync_socket_callback_t(std::move(shared_handler)),
                                           m_timer_create, m_timer_cancel, m_timer_free);
    }
};

} // namespace

RLM_API realm_sync_socket_t* realm_sync_socket_new(
    realm_userdata_t userdata, realm_free_userdata_func_t userdata_free, realm_sync_socket_post_func_t post_func,
    realm_sync_socket_create_timer_func_t create_timer_func,
    realm_sync_socket_timer_canceled_func_t cancel_timer_func, realm_sync_socket_timer_free_func_t free_timer_func,
    realm_sync_socket_connect_func_t websocket_connect_func,
    realm_sync_socket_websocket_async_write_func_t websocket_write_func,
    realm_sync_socket_websocket_free_func_t websocket_free_func)
{
    return wrap_err([&]() {
        auto capi_socket_provider = std::make_shared<CAPISyncSocketProvider>();
        capi_socket_provider->m_userdata = userdata;
        capi_socket_provider->m_free = userdata_free;
        capi_socket_provider->m_post = post_func;
        capi_socket_provider->m_timer_create = create_timer_func;
        capi_socket_provider->m_timer_cancel = cancel_timer_func;
        capi_socket_provider->m_timer_free = free_timer_func;
        capi_socket_provider->m_websocket_connect = websocket_connect_func;
        capi_socket_provider->m_websocket_async_write = websocket_write_func;
        capi_socket_provider->m_websocket_free = websocket_free_func;
        return new realm_sync_socket_t(std::move(capi_socket_provider));
    });
}

RLM_API void realm_sync_socket_callback_complete(realm_sync_socket_callback* realm_callback, realm_errno_e code,
                                                 const char* reason)
{
    auto complete_status =
        code == realm_errno_e::RLM_ERR_NONE ? Status::OK() : Status{static_cast<ErrorCodes::Error>(code), reason};
    (*(realm_callback->get()))(complete_status);
    realm_release(realm_callback);
}

RLM_API void realm_sync_socket_websocket_connected(realm_websocket_observer_t* realm_websocket_observer,
                                                   const char* protocol)
{
    realm_websocket_observer->get()->websocket_connected_handler(protocol);
}

RLM_API void realm_sync_socket_websocket_error(realm_websocket_observer_t* realm_websocket_observer)
{
    realm_websocket_observer->get()->websocket_error_handler();
}

RLM_API void realm_sync_socket_websocket_message(realm_websocket_observer_t* realm_websocket_observer,
                                                 const char* data, size_t data_size)
{
    realm_websocket_observer->get()->websocket_binary_message_received(util::Span{data, data_size});
}

RLM_API void realm_sync_socket_websocket_closed(realm_websocket_observer_t* realm_websocket_observer, bool was_clean,
                                                realm_web_socket_errno_e code, const char* reason)
{
    realm_websocket_observer->get()->websocket_closed_handler(
        was_clean, static_cast<sync::websocket::WebSocketError>(code), reason);
}

RLM_API void realm_sync_client_config_set_sync_socket(realm_sync_client_config_t* config,
                                                      realm_sync_socket_t* sync_socket) RLM_API_NOEXCEPT
{
    config->socket_provider = *sync_socket;
}

} // namespace realm::c_api
