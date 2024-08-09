#include <realm/error_codes.hpp>
#include <realm/status.hpp>
#include <realm/object-store/c_api/util.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/network/websocket.hpp>

namespace realm::c_api {
namespace {

// THis class represents the timer resource that is returned to the sync client from the
// CAPI implementation details for canceling and deleting the timer resources.
struct CAPITimer : sync::SyncSocketProvider::Timer {
public:
    CAPITimer(realm_userdata_t userdata, int64_t delay_ms, realm_sync_socket_timer_callback_t* handler,
              realm_sync_socket_create_timer_func_t create_timer_func,
              realm_sync_socket_timer_canceled_func_t cancel_timer_func,
              realm_sync_socket_timer_free_func_t free_timer_func)
        : m_userdata(userdata)
        , m_timer_create(create_timer_func)
        , m_timer_cancel(cancel_timer_func)
        , m_timer_free(free_timer_func)
    {
        m_timer = m_timer_create(userdata, delay_ms, handler);
    }

    /// Cancels the timer and destroys the timer instance.
    ~CAPITimer()
    {
        // Make sure the timer is stopped, if not already
        m_timer_cancel(m_userdata, m_timer);
        m_timer_free(m_userdata, m_timer);
    }

    // Cancel the timer immediately - the CAPI implementation will need to call the
    // realm_sync_socket_timer_canceled function to notify the sync client that the
    // timer has been canceled and must be called in the same execution thread as
    // the timer complete.
    void cancel() override
    {
        m_timer_cancel(m_userdata, m_timer);
    }

private:
    // A pointer to the CAPI implementation's timer instance. This is provided by the
    // CAPI implementation when the create_timer_func function is called.
    realm_sync_socket_timer_t m_timer = nullptr;

    // These values were originally provided to the socket_provider instance by the CAPI
    // implementation when it was created
    realm_userdata_t m_userdata = nullptr;
    realm_sync_socket_create_timer_func_t m_timer_create = nullptr;
    realm_sync_socket_timer_canceled_func_t m_timer_cancel = nullptr;
    realm_sync_socket_timer_free_func_t m_timer_free = nullptr;
};

static void realm_sync_socket_op_complete(realm_sync_socket_callback* realm_callback,
                                          realm_sync_socket_callback_result_e result, const char* reason)
{
    if (!realm_callback)
        return;

    (*realm_callback)(result, reason);
    realm_release(realm_callback);
}

RLM_API void realm_sync_socket_timer_complete(realm_sync_socket_timer_callback_t* timer_handler,
                                              realm_sync_socket_callback_result_e result, const char* reason)
{
    realm_sync_socket_op_complete(timer_handler, result, reason);
}

RLM_API void realm_sync_socket_timer_canceled(realm_sync_socket_timer_callback_t* timer_handler)
{
    realm_sync_socket_op_complete(timer_handler, RLM_ERR_SYNC_SOCKET_OPERATION_ABORTED, "Timer canceled");
}

// This class represents a websocket instance provided by the CAPI implememtation for sending
// and receiving data and connection state from the websocket. This class is used directly by
// the sync client.
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
                                new realm_sync_socket_write_callback_t(std::move(shared_handler)));
    }

private:
    // A pointer to the CAPI implementation's websocket instance. This is provided by
    // the m_websocket_connect() function when this websocket instance is created.
    realm_sync_socket_websocket_t m_socket = nullptr;

    // A wrapped reference to the websocket observer in the sync client that receives the
    // websocket status callbacks. This is provided by the Sync Client.
    realm_websocket_observer_t* m_observer = nullptr;

    // These values were originally provided to the socket_provider instance by the CAPI
    // implementation when it was created.
    realm_userdata_t m_userdata = nullptr;
    realm_sync_socket_connect_func_t m_websocket_connect = nullptr;
    realm_sync_socket_websocket_async_write_func_t m_websocket_async_write = nullptr;
    realm_sync_socket_websocket_free_func_t m_websocket_free = nullptr;
};

// Represents the websocket observer in the sync client that receives websocket status
// callbacks and passes them along to the WebSocketObserver object.
struct CAPIWebSocketObserver : sync::WebSocketObserver {
public:
    CAPIWebSocketObserver(std::unique_ptr<sync::WebSocketObserver> observer)
        : m_observer(std::move(observer))
    {
        REALM_ASSERT_EX(m_observer, "WebSocketObserver cannot be null");
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

    void websocket_binary_message_received(util::Span<const char> data) final
    {
        m_observer->websocket_binary_message_received(data);
    }

    void websocket_closed_handler(bool was_clean, sync::websocket::WebSocketError code, std::string_view msg) final
    {
        m_observer->websocket_closed_handler(was_clean, code, msg);
    }

private:
    std::unique_ptr<sync::WebSocketObserver> m_observer;
};

// This is the primary resource for providing event loop, timer and websocket
// resources and synchronization for the Sync Client. The CAPI implementation
// needs to implement the "funct_t" functions provided to this class for connecting
// the implementation to the operations called by the Sync Client.
struct CAPISyncSocketProvider : sync::SyncSocketProvider {
    realm_userdata_t m_userdata = nullptr;
    realm_free_userdata_func_t m_userdata_free = nullptr;
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
        , m_userdata_free(std::exchange(other.m_userdata_free, nullptr))
        , m_post(std::exchange(other.m_post, nullptr))
        , m_timer_create(std::exchange(other.m_timer_create, nullptr))
        , m_timer_cancel(std::exchange(other.m_timer_cancel, nullptr))
        , m_timer_free(std::exchange(other.m_timer_free, nullptr))
        , m_websocket_connect(std::exchange(other.m_websocket_connect, nullptr))
        , m_websocket_async_write(std::exchange(other.m_websocket_async_write, nullptr))
        , m_websocket_free(std::exchange(other.m_websocket_free, nullptr))
    {
        // userdata_free can be null if userdata is not used
        if (m_userdata != nullptr) {
            REALM_ASSERT(m_userdata_free);
        }
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
        if (m_userdata_free) {
            m_userdata_free(m_userdata);
        }
    }

    // Create a websocket object that will be returned to the Sync Client, which is expected to
    // begin connecting to the endpoint as soon as the object is created. The state and any data
    // received is passed to the socket observer via the helper functions defined below this class.
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
        m_post(m_userdata, new realm_sync_socket_post_callback_t(std::move(shared_handler)));
    }

    SyncTimer create_timer(std::chrono::milliseconds delay, FunctionHandler&& handler) final
    {
        auto shared_handler = std::make_shared<FunctionHandler>(std::move(handler));
        return std::make_unique<CAPITimer>(m_userdata, delay.count(),
                                           new realm_sync_socket_timer_callback_t(std::move(shared_handler)),
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
        capi_socket_provider->m_userdata_free = userdata_free;
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

RLM_API void realm_sync_socket_post_complete(realm_sync_socket_post_callback_t* post_handler,
                                             realm_sync_socket_callback_result_e result, const char* reason)
{
    realm_sync_socket_op_complete(post_handler, result, reason);
}

RLM_API void realm_sync_socket_write_complete(realm_sync_socket_write_callback_t* write_handler,
                                              realm_sync_socket_callback_result_e result, const char* reason)
{
    realm_sync_socket_op_complete(write_handler, result, reason);
}

RLM_API void realm_sync_socket_websocket_connected(realm_websocket_observer_t* realm_websocket_observer,
                                                   const char* protocol)
{
    if (realm_websocket_observer)
        realm_websocket_observer->get()->websocket_connected_handler(protocol);
}

RLM_API void realm_sync_socket_websocket_error(realm_websocket_observer_t* realm_websocket_observer)
{
    if (realm_websocket_observer)
        realm_websocket_observer->get()->websocket_error_handler();
}

RLM_API void realm_sync_socket_websocket_message(realm_websocket_observer_t* realm_websocket_observer,
                                                 const char* data, size_t data_size)
{
    if (realm_websocket_observer)
        realm_websocket_observer->get()->websocket_binary_message_received(util::Span{data, data_size});
}

RLM_API void realm_sync_socket_websocket_closed(realm_websocket_observer_t* realm_websocket_observer, bool was_clean,
                                                realm_web_socket_errno_e code, const char* reason)
{
    if (realm_websocket_observer)
        realm_websocket_observer->get()->websocket_closed_handler(
            was_clean, static_cast<sync::websocket::WebSocketError>(code), reason);
}

RLM_API void realm_sync_client_config_set_sync_socket(realm_sync_client_config_t* config,
                                                      realm_sync_socket_t* sync_socket) RLM_API_NOEXCEPT
{
    config->socket_provider = *sync_socket;
}

} // namespace realm::c_api
