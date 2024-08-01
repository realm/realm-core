
#ifndef REALM_NOINST_CLIENT_IMPL_BASE_HPP
#define REALM_NOINST_CLIENT_IMPL_BASE_HPP

#include <realm/sync/client_base.hpp>

#include <realm/binary_data.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/network/default_socket.hpp>
#include <realm/sync/network/network_ssl.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/migration_store.hpp>
#include <realm/sync/noinst/protocol_codec.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/subscriptions.hpp>
#include <realm/sync/trigger.hpp>
#include <realm/util/buffer_stream.hpp>
#include <realm/util/checked_mutex.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/span.hpp>

#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <map>
#include <random>
#include <string>
#include <unordered_set>
#include <utility>

namespace realm::sync {

// (protocol, address, port, user_id)
//
// `protocol` is included for convenience, even though it is not strictly part
// of an endpoint.
struct ServerEndpoint {
    ProtocolEnvelope envelope;
    std::string address;
    network::Endpoint::port_type port;
    std::string user_id;
    SyncServerMode server_mode = SyncServerMode::PBS;
    bool is_verified = false;

private:
    auto to_tuple() const
    {
        // Does not include server_mode because all endpoints for a single Client
        // must have the same mode. is_verified is not part of an endpoint's identity.
        return std::make_tuple(server_mode, envelope, std::ref(address), port, std::ref(user_id));
    }

public:
    friend inline bool operator==(const ServerEndpoint& lhs, const ServerEndpoint& rhs)
    {
        return lhs.to_tuple() == rhs.to_tuple();
    }

    friend inline bool operator<(const ServerEndpoint& lhs, const ServerEndpoint& rhs)
    {
        return lhs.to_tuple() < rhs.to_tuple();
    }
};

class SessionWrapper;

class SessionWrapperStack {
public:
    bool empty() const noexcept;
    void push(util::bind_ptr<SessionWrapper>) noexcept;
    util::bind_ptr<SessionWrapper> pop() noexcept;
    void clear() noexcept;
    bool erase(SessionWrapper*) noexcept;
    SessionWrapperStack() noexcept = default;
    ~SessionWrapperStack();

private:
    SessionWrapper* m_back = nullptr;
};

template <typename ErrorType, typename RandomEngine>
struct ErrorBackoffState {
    ErrorBackoffState() = default;
    explicit ErrorBackoffState(ResumptionDelayInfo default_delay_info, RandomEngine& random_engine)
        : default_delay_info(std::move(default_delay_info))
        , m_random_engine(random_engine)
    {
    }

    void update(ErrorType new_error, std::optional<ResumptionDelayInfo> new_delay_info)
    {
        if (triggering_error && *triggering_error == new_error) {
            return;
        }

        delay_info = new_delay_info.value_or(default_delay_info);
        cur_delay_interval = util::none;
        triggering_error = new_error;
    }

    void reset()
    {
        triggering_error = util::none;
        cur_delay_interval = util::none;
        delay_info = default_delay_info;
    }

    std::chrono::milliseconds delay_interval()
    {
        if (!cur_delay_interval) {
            cur_delay_interval = delay_info.resumption_delay_interval;
            return jitter_value(*cur_delay_interval);
        }
        if (*cur_delay_interval == delay_info.max_resumption_delay_interval) {
            return jitter_value(delay_info.max_resumption_delay_interval);
        }
        auto safe_delay_interval = cur_delay_interval->count();
        if (util::int_multiply_with_overflow_detect(safe_delay_interval,
                                                    delay_info.resumption_delay_backoff_multiplier)) {
            *cur_delay_interval = delay_info.max_resumption_delay_interval;
        }
        else {
            *cur_delay_interval =
                std::min(delay_info.max_resumption_delay_interval, std::chrono::milliseconds(safe_delay_interval));
        }
        return jitter_value(*cur_delay_interval);
    }

    ResumptionDelayInfo default_delay_info;
    ResumptionDelayInfo delay_info;
    util::Optional<std::chrono::milliseconds> cur_delay_interval;
    util::Optional<ErrorType> triggering_error;

private:
    std::chrono::milliseconds jitter_value(std::chrono::milliseconds value)
    {
        if (delay_info.delay_jitter_divisor == 0) {
            return value;
        }
        const auto max_jitter = value.count() / delay_info.delay_jitter_divisor;
        auto distr = std::uniform_int_distribution<std::chrono::milliseconds::rep>(0, max_jitter);
        std::chrono::milliseconds randomized_deduction(distr(m_random_engine.get()));
        return value - randomized_deduction;
    }

    std::reference_wrapper<RandomEngine> m_random_engine;
};

class ClientImpl {
public:
    enum class ConnectionTerminationReason;
    class Connection;
    class Session;

    using port_type = network::Endpoint::port_type;
    using OutputBuffer = util::ResettableExpandableBufferOutputStream;
    using ClientProtocol = _impl::ClientProtocol;
    using RandomEngine = std::mt19937_64;

    /// Per-server endpoint information used to determine reconnect delays.
    class ReconnectInfo {
    public:
        explicit ReconnectInfo(ReconnectMode mode, ResumptionDelayInfo default_delay_info, RandomEngine& random)
            : m_reconnect_mode(mode)
            , m_backoff_state(default_delay_info, random)
        {
        }

        void reset() noexcept;
        void update(ConnectionTerminationReason reason, std::optional<ResumptionDelayInfo> new_delay_info);
        std::chrono::milliseconds delay_interval();

        const std::optional<ConnectionTerminationReason> reason() const noexcept
        {
            return m_backoff_state.triggering_error;
        }

        // Set this flag to true to schedule a postponed invocation of reset(). See
        // Connection::cancel_reconnect_delay() for details and rationale.
        //
        // Will be set back to false when a PONG message arrives, and the
        // corresponding PING message was sent while `m_scheduled_reset` was
        // true. See receive_pong().
        bool scheduled_reset = false;

    private:
        ReconnectMode m_reconnect_mode;
        ErrorBackoffState<ConnectionTerminationReason, RandomEngine> m_backoff_state;
    };

    static constexpr milliseconds_type default_connect_timeout = 120000;        // 2 minutes
    static constexpr milliseconds_type default_connection_linger_time = 30000;  // 30 seconds
    static constexpr milliseconds_type default_ping_keepalive_period = 60000;   // 1 minute
    static constexpr milliseconds_type default_pong_keepalive_timeout = 120000; // 2 minutes
    static constexpr milliseconds_type default_fast_reconnect_limit = 60000;    // 1 minute

    class ForwardingLogger : public util::Logger {
    public:
        ForwardingLogger(std::shared_ptr<util::Logger> logger)
            : Logger(logger->get_category(), *logger)
            , base_logger(std::move(logger))
        {
        }

        void do_log(const util::LogCategory& cat, Level level, const std::string& msg) final
        {
            Logger::do_log(*base_logger, cat, level, msg);
        }

        std::shared_ptr<util::Logger> base_logger;
    };

    ForwardingLogger logger;

    ClientImpl(ClientConfig);
    ~ClientImpl();

    static constexpr int get_oldest_supported_protocol_version() noexcept;

    void shutdown() noexcept REQUIRES(!m_mutex, !m_drain_mutex);

    void shutdown_and_wait() REQUIRES(!m_mutex, !m_drain_mutex);

    const std::string& get_user_agent_string() const noexcept;
    ReconnectMode get_reconnect_mode() const noexcept;
    bool is_dry_run() const noexcept;

    // Functions to post onto the event loop and create an event loop timer using the
    // SyncSocketProvider
    void post(SyncSocketProvider::FunctionHandler&& handler) REQUIRES(!m_drain_mutex);
    void post(util::UniqueFunction<void()>&& handler) REQUIRES(!m_drain_mutex);
    SyncSocketProvider::SyncTimer create_timer(std::chrono::milliseconds delay,
                                               SyncSocketProvider::FunctionHandler&& handler)
        REQUIRES(!m_drain_mutex);
    using SyncTrigger = std::unique_ptr<Trigger<ClientImpl>>;
    SyncTrigger create_trigger(SyncSocketProvider::FunctionHandler&& handler);

    RandomEngine& get_random() noexcept;

    /// Returns false if the specified URL is invalid.
    bool decompose_server_url(const std::string& url, ProtocolEnvelope& protocol, std::string& address,
                              port_type& port, std::string& path) const;

    void cancel_reconnect_delay() REQUIRES(!m_drain_mutex);
    bool wait_for_session_terminations_or_client_stopped() REQUIRES(!m_mutex, !m_drain_mutex);
    // Async version of wait_for_session_terminations_or_client_stopped().
    util::Future<void> notify_session_terminated() REQUIRES(!m_drain_mutex);
    void voluntary_disconnect_all_connections() REQUIRES(!m_drain_mutex);

private:
    using connection_ident_type = std::int_fast64_t;

    const ReconnectMode m_reconnect_mode; // For testing purposes only
    const milliseconds_type m_connect_timeout;
    const milliseconds_type m_connection_linger_time;
    const milliseconds_type m_ping_keepalive_period;
    const milliseconds_type m_pong_keepalive_timeout;
    const milliseconds_type m_fast_reconnect_limit;
    const ResumptionDelayInfo m_reconnect_backoff_info;
    const bool m_disable_upload_activation_delay;
    const bool m_dry_run; // For testing purposes only
    const bool m_enable_default_port_hack;
    const bool m_fix_up_object_ids;
    const std::function<RoundtripTimeHandler> m_roundtrip_time_handler;
    const std::string m_user_agent_string;
    std::shared_ptr<SyncSocketProvider> m_socket_provider;
    ClientProtocol m_client_protocol;
    session_ident_type m_prev_session_ident = 0;
    const bool m_one_connection_per_session;

    RandomEngine m_random;
    SyncTrigger m_actualize_and_finalize;

    // Note: There is one server slot per server endpoint (hostname, port,
    // user_id), and it survives from one connection object to the next, which
    // is important because it carries information about a possible reconnect
    // delay applying to the new connection object (server hammering
    // protection).
    struct ServerSlot {
        explicit ServerSlot(ReconnectInfo reconnect_info);
        ~ServerSlot();

        ReconnectInfo reconnect_info; // Applies exclusively to `connection`.
        std::unique_ptr<ClientImpl::Connection> connection;

        // Used instead of `connection` when `m_one_connection_per_session` is
        // true.
        std::map<connection_ident_type, std::unique_ptr<ClientImpl::Connection>> alt_connections;
    };

    // Must be accessed only by event loop thread
    std::map<ServerEndpoint, ServerSlot> m_server_slots;

    // Must be accessed only by event loop thread
    connection_ident_type m_prev_connection_ident = 0;

    util::CheckedMutex m_drain_mutex;
    std::condition_variable m_drain_cv;
    bool m_drained GUARDED_BY(m_drain_mutex) = false;
    uint64_t m_outstanding_posts GUARDED_BY(m_drain_mutex) = 0;
    uint64_t m_num_connections GUARDED_BY(m_drain_mutex) = 0;

    util::CheckedMutex m_mutex;

    bool m_stopped GUARDED_BY(m_mutex) = false;
    bool m_sessions_terminated GUARDED_BY(m_mutex) = false;

    // The set of session wrappers that are not yet wrapping a session object,
    // and are not yet abandoned (still referenced by the application).
    SessionWrapperStack m_unactualized_session_wrappers GUARDED_BY(m_mutex);

    // The set of session wrappers that were successfully actualized, but are
    // now abandoned (no longer referenced by the application), and have not yet
    // been finalized. Order in queue is immaterial.
    SessionWrapperStack m_abandoned_session_wrappers GUARDED_BY(m_mutex);

    // Used with m_mutex
    std::condition_variable m_wait_or_client_stopped_cond;

    void register_unactualized_session_wrapper(SessionWrapper*) REQUIRES(!m_mutex);
    void register_abandoned_session_wrapper(util::bind_ptr<SessionWrapper>) noexcept REQUIRES(!m_mutex);
    void actualize_and_finalize_session_wrappers() REQUIRES(!m_mutex);

    // Get or create a connection. If a connection exists for the specified
    // endpoint, it will be returned, otherwise a new connection will be
    // created. If `m_one_connection_per_session` is true (testing only), a new
    // connection will be created every time.
    //
    // Must only be accessed from event loop thread.
    //
    // FIXME: Passing these SSL parameters here is confusing at best, since they
    // are ignored if a connection is already available for the specified
    // endpoint. Also, there is no way to check that all the specified SSL
    // parameters are in agreement with a preexisting connection. A better
    // approach would be to allow for per-endpoint SSL parameters to be
    // specifiable through public member functions of ClientImpl from where they
    // could then be picked up as new connections are created on demand.
    ClientImpl::Connection& get_connection(ServerEndpoint, const std::string& authorization_header_name,
                                           const std::map<std::string, std::string>& custom_http_headers,
                                           bool verify_servers_ssl_certificate,
                                           util::Optional<std::string> ssl_trust_certificate_path,
                                           std::function<SyncConfig::SSLVerifyCallback>,
                                           util::Optional<SyncConfig::ProxyConfig>, bool& was_created)
        REQUIRES(!m_drain_mutex);

    // Destroys the specified connection.
    void remove_connection(ClientImpl::Connection&) noexcept REQUIRES(!m_drain_mutex);

    void drain_connections();
    void drain_connections_on_loop() REQUIRES(!m_drain_mutex);

    void incr_outstanding_posts() REQUIRES(!m_drain_mutex);
    void decr_outstanding_posts() REQUIRES(!m_drain_mutex);

    session_ident_type get_next_session_ident() noexcept;

    friend class ClientImpl::Connection;
    friend class SessionWrapper;
};

constexpr int ClientImpl::get_oldest_supported_protocol_version() noexcept
{
    // See get_current_protocol_version() for information about the
    // individual protocol versions.
    return 2;
}

static_assert(ClientImpl::get_oldest_supported_protocol_version() >= 1, "");
static_assert(ClientImpl::get_oldest_supported_protocol_version() <= get_current_protocol_version(), "");


/// Information about why a connection (or connection initiation attempt) was
/// terminated. This is used to determinte the delay until the next connection
/// initiation attempt.
enum class ClientImpl::ConnectionTerminationReason {
    connect_operation_failed,          ///< Failure during connect operation
    closed_voluntarily,                ///< Voluntarily closed or connection operation canceled
    read_or_write_error,               ///< Read/write error after successful TCP connect operation
    ssl_certificate_rejected,          ///< Client rejected the SSL certificate of the server
    ssl_protocol_violation,            ///< A violation of the SSL protocol
    websocket_protocol_violation,      ///< A violation of the WebSocket protocol
    http_response_says_fatal_error,    ///< Status code in HTTP response says "fatal error"
    http_response_says_nonfatal_error, ///< Status code in HTTP response says "nonfatal error"
    bad_headers_in_http_response,      ///< Missing or bad headers in HTTP response
    sync_protocol_violation,           ///< Client received a bad message from the server
    sync_connect_timeout,              ///< Sync connection was not fully established in time
    server_said_try_again_later,       ///< Client received ERROR message with try_again=yes
    server_said_do_not_reconnect,      ///< Client received ERROR message with try_again=no
    pong_timeout,                      ///< Client did not receive PONG after PING

    /// The application requested a feature that is unavailable in the
    /// negotiated protocol version.
    missing_protocol_feature,
};

/// All use of connection objects, including construction and destruction, must
/// occur on behalf of the event loop thread of the associated client object.

// TODO: The parent will be updated to WebSocketObserver once the WebSocket integration is complete
class ClientImpl::Connection {
public:
    using connection_ident_type = std::int_fast64_t;
    using SSLVerifyCallback = SyncConfig::SSLVerifyCallback;
    using ProxyConfig = SyncConfig::ProxyConfig;
    using ReconnectInfo = ClientImpl::ReconnectInfo;
    using DownloadMessage = ClientProtocol::DownloadMessage;

    ForwardingLogger logger;

    ClientImpl& get_client() noexcept;
    ReconnectInfo get_reconnect_info() const noexcept;
    ClientProtocol& get_client_protocol() noexcept;

    /// Activate this connection object. No attempt is made to establish a
    /// connection before the connection object is activated.
    void activate();

    /// Activate the specified session.
    ///
    /// Prior to being activated, no messages will be sent or received on behalf
    /// of this session, and the associated Realm file will not be accessed,
    /// i.e., `Session::get_db()` will not be called.
    ///
    /// If activation is successful, the connection keeps the session alive
    /// until the application calls initiated_session_deactivation() or until
    /// the application destroys the connection object, whichever comes first.
    void activate_session(std::unique_ptr<Session>);

    /// Initiate the deactivation process which eventually (or immediately)
    /// leads to destruction of this session object.
    ///
    /// IMPORTANT: The session object may get destroyed before this function
    /// returns.
    ///
    /// The deactivation process must be considered initiated even if this
    /// function throws.
    ///
    /// The deactivation process is guaranteed to not be initiated until the
    /// application calls this function. So from the point of view of the
    /// application, after successful activation, a pointer to a session object
    /// remains valid until the application calls
    /// initiate_session_deactivation().
    ///
    /// After the initiation of the deactivation process, the associated Realm
    /// file will no longer be accessed, i.e., `get_db()` will not be called
    /// again, and a previously returned reference will also not be accessed
    /// again.
    ///
    /// The initiation of the deactivation process must be preceded by a
    /// successful invocation of activate_session(). It is an error to call
    /// initiate_session_deactivation() twice.
    void initiate_session_deactivation(Session*);

    /// Cancel the reconnect delay for this connection, if one is currently in
    /// effect. If a reconnect delay is not currently in effect, ensure that the
    /// delay before the next reconnection attempt will be canceled. This is
    /// necessary as an apparently established connection, or ongoing connection
    /// attempt can be about to fail for a reason that precedes the invocation
    /// of this function.
    ///
    /// It is an error to call this function before the connection has been
    /// activated.
    void cancel_reconnect_delay();

    void force_close();

    /// Returns zero until the HTTP response is received. After that point in
    /// time, it returns the negotiated protocol version, which is based on the
    /// contents of the `Sec-WebSocket-Protocol` header in the HTTP
    /// response. The negotiated protocol version is guaranteed to be greater
    /// than or equal to get_oldest_supported_protocol_version(), and be less
    /// than or equal to get_current_protocol_version().
    int get_negotiated_protocol_version() noexcept;

    // Methods from WebSocketObserver interface for websockets from the Socket Provider
    void websocket_connected_handler(const std::string& protocol);
    bool websocket_binary_message_received(util::Span<const char> data);
    void websocket_error_handler();
    bool websocket_closed_handler(bool, websocket::WebSocketError, std::string_view msg);

    connection_ident_type get_ident() const noexcept;
    const ServerEndpoint& get_server_endpoint() const noexcept;
    ConnectionState get_state() const noexcept;
    SyncServerMode get_sync_server_mode() const noexcept;
    bool is_flx_sync_connection() const noexcept;

    void update_connect_info(const std::string& http_request_path_prefix, const std::string& signed_access_token);

    void resume_active_sessions();

    void voluntary_disconnect();

    std::string get_active_appservices_connection_id();

    Connection(ClientImpl&, connection_ident_type, ServerEndpoint, const std::string& authorization_header_name,
               const std::map<std::string, std::string>& custom_http_headers, bool verify_servers_ssl_certificate,
               util::Optional<std::string> ssl_trust_certificate_path, std::function<SSLVerifyCallback>,
               util::Optional<ProxyConfig>, ReconnectInfo);

    ~Connection();

private:
    struct LifecycleSentinel : public util::AtomicRefCountBase {
        bool destroyed = false;
    };
    struct WebSocketObserverShim;

    using ReceivedChangesets = ClientProtocol::ReceivedChangesets;

    template <class H>
    void for_each_active_session(H handler);

    /// \brief Called when the connection becomes idle.
    ///
    /// The connection is considered idle when all of the following conditions
    /// are true:
    ///
    /// - The connection is activated.
    ///
    /// - The connection has no sessions in the Active state.
    ///
    /// - The connection is closed (in the disconnected state).
    ///
    /// From the point of view of this class, an overriding function is allowed
    /// to commit suicide (`delete this`).
    ///
    /// The default implementation of this function does nothing.
    ///
    /// This function is always called by the event loop thread of the
    /// associated client object.
    void on_idle();

    std::string get_http_request_path() const;

    void initiate_reconnect_wait();
    void handle_reconnect_wait(Status status);
    void initiate_reconnect();
    void initiate_connect_wait();
    void handle_connect_wait(Status status);

    void handle_connection_established();
    void schedule_urgent_ping();
    void initiate_ping_delay(milliseconds_type now);
    void handle_ping_delay();
    void initiate_pong_timeout();
    void handle_pong_timeout();
    void initiate_write_message(const OutputBuffer&, Session*);
    void handle_write_message();
    void send_next_message();
    void send_ping();
    void initiate_write_ping(const OutputBuffer&);
    void handle_write_ping();
    void handle_message_received(util::Span<const char> data);
    void initiate_disconnect_wait();
    void handle_disconnect_wait(Status status);
    void close_due_to_protocol_error(Status status);
    void close_due_to_client_side_error(Status, IsFatal is_fatal, ConnectionTerminationReason reason);
    void close_due_to_transient_error(Status status, ConnectionTerminationReason reason);
    void close_due_to_server_side_error(ProtocolError, const ProtocolErrorInfo& info);
    void involuntary_disconnect(const SessionErrorInfo& info, ConnectionTerminationReason reason);
    void disconnect(const SessionErrorInfo& info);
    void change_state_to_disconnected() noexcept;
    // These are only called from ClientProtocol class.
    void receive_pong(milliseconds_type timestamp);
    void receive_error_message(const ProtocolErrorInfo& info, session_ident_type);
    void receive_query_error_message(int error_code, std::string_view message, int64_t query_version,
                                     session_ident_type);
    void receive_ident_message(session_ident_type, SaltedFileIdent);
    void receive_download_message(session_ident_type, const DownloadMessage& message);

    void receive_mark_message(session_ident_type, request_ident_type);
    void receive_unbound_message(session_ident_type);
    void receive_test_command_response(session_ident_type, request_ident_type, std::string_view body);
    void receive_server_log_message(session_ident_type, util::Logger::Level, std::string_view body);
    void receive_appservices_request_id(std::string_view coid);
    void handle_protocol_error(Status status);

    // These are only called from Session class.
    void enlist_to_send(Session*);
    void one_more_active_unsuspended_session();
    void one_less_active_unsuspended_session();
    void finish_session_deactivation(Session* sess);

    OutputBuffer& get_output_buffer() noexcept;
    Session* get_session(session_ident_type) const noexcept;
    Session* find_and_validate_session(session_ident_type session_ident, std::string_view message) noexcept;
    static bool was_voluntary(ConnectionTerminationReason) noexcept;

    static std::shared_ptr<util::Logger> make_logger(connection_ident_type ident,
                                                     std::optional<std::string_view> coid,
                                                     std::shared_ptr<util::Logger> base_logger);

    void report_connection_state_change(ConnectionState, util::Optional<SessionErrorInfo> error_info = util::none);

    friend ClientProtocol;
    friend class Session;

    ClientImpl& m_client;
    util::bind_ptr<LifecycleSentinel> m_websocket_sentinel;
    std::unique_ptr<WebSocketInterface> m_websocket;

    /// DEPRECATED - These will be removed in a future release
    const bool m_verify_servers_ssl_certificate;
    const util::Optional<std::string> m_ssl_trust_certificate_path;
    const std::function<SSLVerifyCallback> m_ssl_verify_callback;
    const util::Optional<ProxyConfig> m_proxy_config;

    ReconnectInfo m_reconnect_info;
    int m_negotiated_protocol_version = 0;

    ConnectionState m_state = ConnectionState::disconnected;

    std::size_t m_num_active_unsuspended_sessions = 0;
    std::size_t m_num_active_sessions = 0;
    ClientImpl::SyncTrigger m_on_idle;

    // activate() has been called
    bool m_activated = false;

    // A reconnect delay is in progress
    bool m_reconnect_delay_in_progress = false;

    // Has no meaning when m_reconnect_delay_in_progress is false.
    bool m_nonzero_reconnect_delay = false;

    // A disconnect (linger) delay is in progress. This is for keeping the
    // connection open for a while after there are no more active unsuspended
    // sessions.
    bool m_disconnect_delay_in_progress = false;

    bool m_disconnect_has_occurred = false;

    // A message is currently being sent, i.e., the sending of a message has
    // been initiated, but not yet completed.
    bool m_sending = false;

    bool m_ping_delay_in_progress = false;
    bool m_waiting_for_pong = false;
    bool m_send_ping = false;
    bool m_minimize_next_ping_delay = false;
    bool m_ping_after_scheduled_reset_of_reconnect_info = false;

    // At least one PING message was sent since connection was established
    bool m_ping_sent = false;

    bool m_websocket_error_received = false;

    bool m_force_closed = false;

    // The timer will be constructed on demand, and will only be destroyed when
    // canceling a reconnect or disconnect delay.
    //
    // It is necessary to destroy and recreate the timer when canceling a wait
    // operation, because the next wait operation might need to be initiated
    // before the completion handler of the previous canceled wait operation
    // starts executing. Such an overlap is not allowed for wait operations on
    // the same timer instance.
    SyncSocketProvider::SyncTimer m_reconnect_disconnect_timer;

    // Timer for connect operation watchdog. For why this timer is optional, see
    // `m_reconnect_disconnect_timer`.
    SyncSocketProvider::SyncTimer m_connect_timer;

    // This timer is used to schedule the sending of PING messages, and as a
    // watchdog for timely reception of PONG messages. For why this timer is
    // optional, see `m_reconnect_disconnect_timer`.
    SyncSocketProvider::SyncTimer m_heartbeat_timer;

    milliseconds_type m_pong_wait_started_at = 0;
    milliseconds_type m_last_ping_sent_at = 0;

    // Round-trip time, in milliseconds, for last PING message for which a PONG
    // message has been received, or zero if no PONG message has been received.
    milliseconds_type m_previous_ping_rtt = 0;

    // Only valid when `m_disconnect_has_occurred` is true.
    milliseconds_type m_disconnect_time = 0;

    // The set of sessions associated with this connection. A session becomes
    // associated with a connection when it is activated.
    std::map<session_ident_type, std::unique_ptr<Session>> m_sessions;
    // Keep track of previously used sessions idents to see if a stale message was
    // received for a closed session
    std::unordered_set<session_ident_type> m_session_history;

    // A queue of sessions that have enlisted for an opportunity to send a
    // message to the server. Sessions will be served in the order that they
    // enlist. A session is only allowed to occur once in this queue. If the
    // connection is open, and the queue is not empty, and no message is
    // currently being written, the first session is taken out of the queue, and
    // then granted an opportunity to send a message.
    std::deque<Session*> m_sessions_enlisted_to_send;

    Session* m_sending_session = nullptr;

    std::unique_ptr<char[]> m_input_body_buffer;
    OutputBuffer m_output_buffer;

    const connection_ident_type m_ident;
    ServerEndpoint m_server_endpoint;
    std::string m_appservices_coid;

    /// DEPRECATED - These will be removed in a future release
    const std::string m_authorization_header_name;
    const std::map<std::string, std::string> m_custom_http_headers;

    std::string m_http_request_path_prefix;
    std::string m_signed_access_token;
};

/// A synchronization session between a local and a remote Realm file.
///
/// All use of session objects, including construction and destruction, must
/// occur on the event loop thread of the associated client object.
class ClientImpl::Session {
public:
    using ReceivedChangesets = ClientProtocol::ReceivedChangesets;
    using DownloadMessage = ClientProtocol::DownloadMessage;

    ForwardingLogger logger;

    ClientImpl& get_client() noexcept;
    Connection& get_connection() noexcept;
    session_ident_type get_ident() const noexcept;

    /// Inform this client about new changesets in the history.
    ///
    /// The type of the version specified here is the one that identifies an
    /// entry in the sync history. Whether this is the same as the snapshot
    /// version of the Realm depends on the history implementation.
    ///
    /// The application is supposed to call this function to inform the client
    /// about a new version produced by a transaction that was not performed on
    /// behalf of this client. If the application does not call this function,
    /// the client will not discover and upload new changesets in a timely
    /// manner.
    ///
    /// It is an error to call this function before activation of the session,
    /// or after initiation of deactivation.
    void recognize_sync_version(version_type);

    /// \brief Request notification when all changesets currently avaialble on
    /// the server have been downloaded.
    ///
    /// When downloading completes, on_download_completion() will be called by
    /// the thread that processes the event loop (as long as such a thread
    /// exists).
    ///
    /// If request_download_completion_notification() is called while a
    /// previously requested completion notification has not yet occurred, the
    /// previous request is canceled and the corresponding notification will
    /// never occur. This ensure that there is no ambiguity about the meaning of
    /// each completion notification.
    ///
    /// The application must be prepared for "spurious" invocations of
    /// on_download_completion() before the client's first invocation of
    /// request_download_completion_notification(), or after a previous
    /// invocation of on_download_completion(), as long as it is before the
    /// subsequent invocation by the client of
    /// request_download_completion_notification(). This is possible because the
    /// client reserves the right to request download completion notifications
    /// internally.
    ///
    /// Download is considered complete when all changesets in the server-side
    /// history, that are supposed to be downloaded, and that precede
    /// `current_server_version`, have been downloaded and integrated into the
    /// local history. `current_server_version` is the version that refers to
    /// the last changeset in the server-side history at the time the server
    /// receives the first MARK message that is sent by the client after the
    /// invocation of request_download_completion_notification().
    ///
    /// Every invocation of request_download_completion_notification() will
    /// cause a new MARK message to be sent to the server, to redetermine
    /// `current_server_version`.
    ///
    /// It is an error to call this function before activation of the session,
    /// or after initiation of deactivation.
    void request_download_completion_notification();

    /// \brief Gets the subscription store associated with this Session.
    SubscriptionStore* get_flx_subscription_store();

    /// \brief Gets the migration store associated with this Session.
    MigrationStore* get_migration_store();

    /// If this session is currently suspended, resume it immediately.
    ///
    /// It is an error to call this function before activation of the session,
    /// or after initiation of deactivation.
    void cancel_resumption_delay();

    /// To be used in connection with implementations of
    /// initiate_integrate_changesets().
    void integrate_changesets(const SyncProgress&, std::uint_fast64_t downloadable_bytes, const ReceivedChangesets&,
                              VersionInfo&, DownloadBatchState batch_state);

    /// It is an error to call this function before activation of the session
    /// (Connection::activate_session()), or after initiation of deactivation
    /// (Connection::initiate_session_deactivation()).
    void on_changesets_integrated(version_type client_version, const SyncProgress& progress);

    void on_integration_failure(const IntegrationException& e);

    void on_connection_state_changed(ConnectionState, const util::Optional<SessionErrorInfo>&);

    /// The application must ensure that the new session object is either
    /// activated (Connection::activate_session()) or destroyed before the
    /// specified connection object is destroyed.
    ///
    /// The specified transaction reporter (via the config object) is guaranteed
    /// to not be called before activation, and also not after initiation of
    /// deactivation.
    Session(SessionWrapper&, ClientImpl::Connection&);
    ~Session();

    void force_close();

    util::Future<std::string> send_test_command(std::string body);

private:
    struct PendingTestCommand {
        request_ident_type id;
        std::string body;
        util::Promise<std::string> promise;
        bool pending = true;
    };

    /// Fetch a reference to the remote virtual path of the Realm associated
    /// with this session.
    ///
    /// This function is always called by the event loop thread of the
    /// associated client object.
    ///
    /// This function is guaranteed to not be called before activation, and also
    /// not after initiation of deactivation.
    const std::string& get_virt_path() const noexcept;

    const std::string& get_realm_path() const noexcept;

    // Can only be called if the session is active or being activated
    DBRef get_db() const noexcept;
    ClientReplication& get_repl() const noexcept;
    ClientHistory& get_history() const noexcept;

    // client_reset_config() returns the config for client
    // reset. If it returns none, ordinary sync is used. If it returns a
    // Config::ClientReset, the session will be initiated with a state Realm
    // transfer from the server.
    util::Optional<ClientReset>& get_client_reset_config() noexcept;

    // Get the reason a synchronization session is used for (regular sync or client reset)
    // - Client reset state means the session is going to be used to download a fresh realm.
    SessionReason get_session_reason() noexcept;

    /// Returns the schema version the synchronization session connects with to the server.
    uint64_t get_schema_version() noexcept;

    // Returns false if this session is not allowed to send UPLOAD messages to the server to
    // update the cursor info, such as during a client reset fresh realm download
    bool upload_messages_allowed() noexcept;

    /// \brief Initiate the integration of downloaded changesets.
    ///
    /// This function must provide for the passed changesets (if any) to
    /// eventually be integrated, and without unnecessary delay. If no
    /// changesets are passed, the purpose of this function reduces to causing
    /// the current synchronization progress (SyncProgress) to be persisted.
    ///
    /// When all changesets have been integrated, and the synchronization
    /// progress has been persisted, this function must provide for
    /// on_changesets_integrated() to be called without unnecessary delay,
    /// although never after initiation of session deactivation.
    ///
    /// The implementation is allowed, but not obliged to aggregate changesets
    /// from multiple invocations of initiate_integrate_changesets() and pass
    /// them to ClientReplication::integrate_server_changesets() at once.
    ///
    /// The synchronization progress passed to
    /// ClientReplication::integrate_server_changesets() must be obtained
    /// by calling get_status(), and that call must occur after the last
    /// invocation of initiate_integrate_changesets() whose changesets are
    /// included in what is passed to
    /// ClientReplication::integrate_server_changesets().
    ///
    /// The download cursor passed to on_changesets_integrated() must be
    /// SyncProgress::download of the synchronization progress passed to the
    /// last invocation of
    /// ClientReplication::integrate_server_changesets().
    ///
    /// The default implementation integrates the specified changesets and calls
    /// on_changesets_integrated() immediately (i.e., from the event loop thread
    /// of the associated client object, and before
    /// initiate_integrate_changesets() returns), and via the history accessor
    /// made available by access_realm().
    ///
    /// This function is always called by the event loop thread of the
    /// associated client object, and on_changesets_integrated() must always be
    /// called by that thread too.
    ///
    /// This function is guaranteed to not be called before activation, and also
    /// not after initiation of deactivation.
    void initiate_integrate_changesets(std::uint_fast64_t downloadable_bytes, DownloadBatchState batch_state,
                                       const SyncProgress& progress, const ReceivedChangesets&);

    /// See request_download_completion_notification().
    void on_download_completion();

    //@{
    /// These are called as the state of the session changes between
    /// "suspended" and "resumed". The initial state is
    /// always "resumed".
    ///
    /// A switch to the suspended state only happens when an error occurs,
    /// and information about that error is passed to on_suspended().
    ///
    /// These functions are always called by the event loop thread of the
    /// associated client object.
    ///
    /// These functions are guaranteed to not be called before activation, and also
    /// not after initiation of deactivation.
    void on_suspended(const SessionErrorInfo& error_info);
    void on_resumed();
    //@}

    void on_flx_sync_error(int64_t version, std::string_view err_msg);

    // Processes an FLX download message, if it's a bootstrap message. If it's not a bootstrap
    // message then this is a noop and will return false. Otherwise this will return true
    // and no further processing of the download message should take place.
    bool process_flx_bootstrap_message(const DownloadMessage& message);

    // Processes any pending FLX bootstraps, if one exists. Otherwise this is a noop.
    void process_pending_flx_bootstrap();

    bool client_reset_if_needed();
    void handle_pending_client_reset_acknowledgement();

    void gather_pending_compensating_writes(util::Span<Changeset> changesets, std::vector<ProtocolErrorInfo>* out);

    void begin_resumption_delay(const ProtocolErrorInfo& error_info);
    void clear_resumption_delay_state();

private:
    Connection& m_conn;
    const session_ident_type m_ident;

    // The states only transition in one direction, from left to right.
    // The transition to Active happens very soon after construction, as soon as
    // it is registered with the Connection.
    // The transition from Deactivating to Deactivated state happens when the
    // unbinding process completes (unbind_process_complete()).
    enum State { Unactivated, Active, Deactivating, Deactivated };
    State m_state = Unactivated;

    bool m_suspended = false;

    SyncSocketProvider::SyncTimer m_try_again_activation_timer;
    ErrorBackoffState<sync::ProtocolError, RandomEngine> m_try_again_delay_info;

    // Set to false when download completion is reached. Set to true after a
    // slow reconnect, such that UPLOAD and QUERY messages will not be sent until
    // download completion is reached again. This feature can be disabled (always
    // false) if ClientConfig::disable_upload_activation_delay is true.
    bool m_delay_uploads = true;

    bool m_is_flx_sync_session = false;

    bool m_fix_up_object_ids = false;

    // These are reset when the session is activated, and again whenever the
    // connection is lost or the rebinding process is initiated.
    bool m_enlisted_to_send;
    bool m_bind_message_sent;            // Sending of BIND message has been initiated
    bool m_ident_message_sent;           // Sending of IDENT message has been initiated
    bool m_unbind_message_sent;          // Sending of UNBIND message has been initiated
    bool m_unbind_message_send_complete; // Sending of UNBIND message has been completed
    bool m_error_message_received;       // Session specific ERROR message received
    bool m_unbound_message_received;     // UNBOUND message received
    bool m_error_to_send;

    // True when there is a new FLX sync query we need to send to the server.
    util::Optional<SubscriptionStore::PendingSubscription> m_pending_flx_sub_set;
    int64_t m_last_sent_flx_query_version = 0;

    std::deque<ProtocolErrorInfo> m_pending_compensating_write_errors;

    util::Optional<IntegrationException> m_client_error;

    // `ident == 0` means unassigned.
    SaltedFileIdent m_client_file_ident = {0, 0};

    // The latest sync progress reported by the server via a DOWNLOAD
    // message. See struct SyncProgress for a description. The values stored in
    // `m_progress` either are persisted, or are about to be.
    //
    // Initialized by way of ClientHistory::get_status() at session
    // activation time.
    //
    // `m_progress.upload.client_version` is the client-side sync version
    // produced by the latest local changeset that has been acknowledged as
    // integrated by the server.
    SyncProgress m_progress;

    // In general, the local version produced by the last changeset in the local
    // history. The changeset that produced this version may, or may not
    // contain changes of local origin.
    //
    // It is set to the current version of the local Realm at session activation
    // time (although always zero for the initial empty Realm
    // state). Thereafter, it is updated when the application calls
    // recognize_sync_version(), when changesets are received from the server
    // and integrated locally, and when the uploading process discovers newer
    // versions.
    //
    // INVARIANT: m_progress.upload.client_version <= m_last_version_available
    version_type m_last_version_available = 0;

    // In general, this is the position in the history reached while scanning
    // for changesets to be uploaded.
    //
    // Set to `m_progress.upload` at session activation time and whenever the
    // connection to the server is lost. When the connection is established, the
    // scanning for changesets to be uploaded then progresses from there towards
    // `m_last_version_available`.
    //
    // INVARIANT: m_progress.upload.client_version <= m_upload_progress.client_version
    UploadCursor m_upload_progress = {0, 0};

    // Same as `m_progress.download` but is updated only as the progress gets
    // persisted.
    DownloadCursor m_download_progress = {0, 0};

    // Used to implement download completion notifications. Set equal to
    // `m_progress.download.server_version` when a MARK message is received. Set
    // back to zero when `m_download_progress.server_version` becomes greater
    // than, or equal to `m_server_version_at_last_download_mark`. For further
    // details, see check_for_download_completion().
    version_type m_server_version_at_last_download_mark = 0;

    // The serial number to attach to the next download MARK message. A new MARK
    // message will be sent when `m_target_download_mark >
    // m_last_download_mark_sent`. To cause a new MARK message to be sent,
    // simply increment `m_target_download_mark`.
    request_ident_type m_target_download_mark = 0;

    // Set equal to `m_target_download_mark` as the sending of each MARK message
    // is initiated. Must be set equal to `m_last_download_mark_received` when
    // the connection to the server is lost.
    request_ident_type m_last_download_mark_sent = 0;

    // Updated when a MARK message is received. See see
    // check_for_download_completion() for how details on how it participates in
    // the detection of download completion.
    request_ident_type m_last_download_mark_received = 0;

    // Updated when a download completion is detected, to avoid multiple
    // triggerings after reception of a single MARK message. See see
    // check_for_download_completion() for how details on how it participates in
    // the detection of download completion.
    request_ident_type m_last_triggering_download_mark = 0;

    SessionWrapper& m_wrapper;

    request_ident_type m_last_pending_test_command_ident = 0;
    std::list<PendingTestCommand> m_pending_test_commands;

    static std::shared_ptr<util::Logger> make_logger(session_ident_type, std::shared_ptr<util::Logger> base_logger);

    Session(SessionWrapper& wrapper, Connection&, session_ident_type);

    bool do_recognize_sync_version(version_type) noexcept;

    bool have_client_file_ident() const noexcept;

    // The unbinding process completes when both of the following become true:
    //
    //  - The sending of the UNBIND message has been completed
    //    (m_unbind_message_sent_2).
    //
    //  - A session specific ERROR, or the UNBOUND message has been received
    //    (m_error_message_received || m_unbond_message_received).
    //
    // Rebinding (sending of a new BIND message) can only be initiated while the
    // session is in the Active state, and the unbinding process has completed
    // (unbind_process_complete()).
    bool unbind_process_complete() const noexcept;

    void activate();
    void initiate_deactivation();
    void complete_deactivation();
    void connection_established(bool fast_reconnect);
    void suspend(const SessionErrorInfo& session_error);
    void connection_lost();
    void send_message();
    void message_sent();
    void send_bind_message();
    void send_ident_message();
    void send_upload_message();
    void send_mark_message();
    void send_alloc_message();
    void send_unbind_message();
    void send_query_change_message();
    void send_json_error_message();
    void send_test_command_message();
    Status receive_ident_message(SaltedFileIdent);
    Status receive_download_message(const DownloadMessage& message);
    Status receive_mark_message(request_ident_type);
    Status receive_unbound_message();
    Status receive_error_message(const ProtocolErrorInfo& info);
    void receive_query_error_message(int error_code, std::string_view message, int64_t query_version);
    Status receive_test_command_response(request_ident_type, std::string_view body);

    void initiate_rebind();
    void reset_protocol_state() noexcept;
    void ensure_enlisted_to_send();
    void enlist_to_send();
    Status check_received_sync_progress(const SyncProgress&) noexcept;
    void check_for_download_completion();

    SyncClientHookAction call_debug_hook(SyncClientHookEvent event, const SyncProgress&, int64_t, DownloadBatchState,
                                         size_t);
    SyncClientHookAction call_debug_hook(SyncClientHookEvent event, const ProtocolErrorInfo* = nullptr);
    SyncClientHookAction call_debug_hook(const SyncClientHookData& data);

    void init_progress_handler();
    void enable_progress_notifications();

    friend class Connection;
};


// Implementation

inline const std::string& ClientImpl::get_user_agent_string() const noexcept
{
    return m_user_agent_string;
}

inline auto ClientImpl::get_reconnect_mode() const noexcept -> ReconnectMode
{
    return m_reconnect_mode;
}

inline bool ClientImpl::is_dry_run() const noexcept
{
    return m_dry_run;
}

inline ClientImpl::RandomEngine& ClientImpl::get_random() noexcept
{
    return m_random;
}

inline auto ClientImpl::get_next_session_ident() noexcept -> session_ident_type
{
    return ++m_prev_session_ident;
}


inline ClientImpl& ClientImpl::Connection::get_client() noexcept
{
    return m_client;
}

inline ConnectionState ClientImpl::Connection::get_state() const noexcept
{
    return m_state;
}

inline ClientImpl::ServerSlot::ServerSlot(ReconnectInfo reconnect_info)
    : reconnect_info(std::move(reconnect_info))
{
}

inline ClientImpl::ServerSlot::~ServerSlot() = default;

inline SyncServerMode ClientImpl::Connection::get_sync_server_mode() const noexcept
{
    return m_server_endpoint.server_mode;
}

inline auto ClientImpl::Connection::get_reconnect_info() const noexcept -> ReconnectInfo
{
    return m_reconnect_info;
}

inline auto ClientImpl::Connection::get_client_protocol() noexcept -> ClientProtocol&
{
    return m_client.m_client_protocol;
}

inline int ClientImpl::Connection::get_negotiated_protocol_version() noexcept
{
    return m_negotiated_protocol_version;
}

template <class H>
void ClientImpl::Connection::for_each_active_session(H handler)
{
    for (auto& p : m_sessions) {
        Session& sess = *p.second;
        if (sess.m_state == Session::Active)
            handler(sess); // Throws
    }
}

inline void ClientImpl::Connection::voluntary_disconnect()
{
    if (m_state == ConnectionState::disconnected) {
        return;
    }
    m_reconnect_info.update(ConnectionTerminationReason::closed_voluntarily, std::nullopt);
    SessionErrorInfo error_info{Status{ErrorCodes::ConnectionClosed, "Connection closed"}, IsFatal{false}};
    error_info.server_requests_action = ProtocolErrorInfo::Action::Transient;

    disconnect(std::move(error_info)); // Throws
}

inline void ClientImpl::Connection::involuntary_disconnect(const SessionErrorInfo& info,
                                                           ConnectionTerminationReason reason)
{
    REALM_ASSERT(!was_voluntary(reason));
    m_reconnect_info.update(reason, info.resumption_delay_interval);
    disconnect(info); // Throws
}

inline void ClientImpl::Connection::change_state_to_disconnected() noexcept
{
    REALM_ASSERT(m_on_idle);
    REALM_ASSERT(m_state != ConnectionState::disconnected);
    m_state = ConnectionState::disconnected;

    if (m_num_active_sessions == 0)
        m_on_idle->trigger();

    REALM_ASSERT(!m_reconnect_delay_in_progress);
    if (m_disconnect_delay_in_progress) {
        m_reconnect_disconnect_timer.reset();
        m_disconnect_delay_in_progress = false;
    }
}

inline void ClientImpl::Connection::one_more_active_unsuspended_session()
{
    if (m_num_active_unsuspended_sessions++ != 0)
        return;
    // Rose from zero to one
    if (m_state == ConnectionState::disconnected && !m_reconnect_delay_in_progress && m_activated)
        initiate_reconnect(); // Throws
}

inline void ClientImpl::Connection::one_less_active_unsuspended_session()
{
    REALM_ASSERT(m_num_active_unsuspended_sessions);
    if (--m_num_active_unsuspended_sessions != 0)
        return;

    // Dropped from one to zero
    if (m_state != ConnectionState::disconnected)
        initiate_disconnect_wait(); // Throws
}

// Sessions, and the connection, should get the output_buffer and insert a message,
// after which they call initiate_write_output_buffer(Session* sess).
inline auto ClientImpl::Connection::get_output_buffer() noexcept -> OutputBuffer&
{
    m_output_buffer.reset();
    return m_output_buffer;
}

inline auto ClientImpl::Connection::get_session(session_ident_type ident) const noexcept -> Session*
{
    auto i = m_sessions.find(ident);
    bool found = (i != m_sessions.end());
    return found ? i->second.get() : nullptr;
}

inline bool ClientImpl::Connection::was_voluntary(ConnectionTerminationReason reason) noexcept
{
    switch (reason) {
        case ConnectionTerminationReason::closed_voluntarily:
            return true;
        case ConnectionTerminationReason::connect_operation_failed:
        case ConnectionTerminationReason::read_or_write_error:
        case ConnectionTerminationReason::ssl_certificate_rejected:
        case ConnectionTerminationReason::ssl_protocol_violation:
        case ConnectionTerminationReason::websocket_protocol_violation:
        case ConnectionTerminationReason::http_response_says_fatal_error:
        case ConnectionTerminationReason::http_response_says_nonfatal_error:
        case ConnectionTerminationReason::bad_headers_in_http_response:
        case ConnectionTerminationReason::sync_protocol_violation:
        case ConnectionTerminationReason::sync_connect_timeout:
        case ConnectionTerminationReason::server_said_try_again_later:
        case ConnectionTerminationReason::server_said_do_not_reconnect:
        case ConnectionTerminationReason::pong_timeout:
        case ConnectionTerminationReason::missing_protocol_feature:
            break;
    }
    return false;
}

inline ClientImpl& ClientImpl::Session::get_client() noexcept
{
    return m_conn.get_client();
}

inline auto ClientImpl::Session::get_connection() noexcept -> Connection&
{
    return m_conn;
}

inline auto ClientImpl::Session::get_ident() const noexcept -> session_ident_type
{
    return m_ident;
}

inline void ClientImpl::Session::recognize_sync_version(version_type version)
{
    REALM_ASSERT(m_state == Active);

    bool resume_upload = do_recognize_sync_version(version);
    if (REALM_LIKELY(resume_upload)) {
        // Don't attempt to send any updates before the IDENT message has been
        // sent or after the UNBIND message has been sent or an error message
        // was received.
        if (m_ident_message_sent && !m_error_message_received && !m_unbind_message_sent)
            ensure_enlisted_to_send(); // Throws
    }
}

inline void ClientImpl::Session::request_download_completion_notification()
{
    REALM_ASSERT(m_state == Active);

    ++m_target_download_mark;

    // Since the deactivation process has not been initiated, the UNBIND message
    // cannot have been sent unless an ERROR message was received.
    REALM_ASSERT(m_error_message_received || !m_unbind_message_sent);
    if (m_ident_message_sent && !m_error_message_received)
        ensure_enlisted_to_send(); // Throws
}

inline ClientImpl::Session::Session(SessionWrapper& wrapper, Connection& conn)
    : Session{wrapper, conn, conn.get_client().get_next_session_ident()} // Throws
{
}

inline ClientImpl::Session::Session(SessionWrapper& wrapper, Connection& conn, session_ident_type ident)
    : logger{make_logger(ident, conn.logger.base_logger)} // Throws
    , m_conn{conn}
    , m_ident{ident}
    , m_try_again_delay_info(conn.get_client().m_reconnect_backoff_info, conn.get_client().get_random())
    , m_is_flx_sync_session(conn.is_flx_sync_connection())
    , m_fix_up_object_ids(get_client().m_fix_up_object_ids)
    , m_wrapper{wrapper}
{
    if (get_client().m_disable_upload_activation_delay)
        m_delay_uploads = false;
}

inline bool ClientImpl::Session::do_recognize_sync_version(version_type version) noexcept
{
    if (REALM_LIKELY(version > m_last_version_available)) {
        m_last_version_available = version;
        return true;
    }
    return false;
}

inline bool ClientImpl::Session::have_client_file_ident() const noexcept
{
    return (m_client_file_ident.ident != 0);
}

inline bool ClientImpl::Session::unbind_process_complete() const noexcept
{
    return (m_unbind_message_send_complete && (m_error_message_received || m_unbound_message_received));
}

inline void ClientImpl::Session::connection_established(bool fast_reconnect)
{
    REALM_ASSERT(m_state == Active);

    if (!fast_reconnect && !get_client().m_disable_upload_activation_delay) {
        // Disallow immediate activation of the upload process, even if download
        // completion was reached during an earlier period of connectivity.
        m_delay_uploads = true;
    }

    if (m_delay_uploads) {
        // Request download completion notification
        ++m_target_download_mark;
    }

    // Notify the debug hook of the SessionConnected event before sending
    // the bind messsage
    call_debug_hook(SyncClientHookEvent::SessionConnected);

    if (!m_suspended) {
        // Ready to send BIND message
        enlist_to_send(); // Throws
    }
}

// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
inline void ClientImpl::Session::connection_lost()
{
    REALM_ASSERT(m_state == Active || m_state == Deactivating);
    // If the deactivation process has been initiated, it can now be immediately
    // completed.
    if (m_state == Deactivating) {
        complete_deactivation(); // Throws
        REALM_ASSERT(m_state == Deactivated);
        return;
    }
    reset_protocol_state();
}

// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
inline void ClientImpl::Session::message_sent()
{
    // Note that it is possible for this function to get called after the client
    // has received a message sent by the server in reposnse to the message that
    // the client has just finished sending.

    REALM_ASSERT(m_state == Active || m_state == Deactivating);

    // No message will be sent after the UNBIND message
    REALM_ASSERT(!m_unbind_message_send_complete);

    // If the client reset config structure is populated, then try to perform
    // the client reset diff once the BIND message has been sent successfully
    if (m_bind_message_sent && m_state == Active && get_client_reset_config()) {
        client_reset_if_needed();
        // Ready to send the IDENT message
        ensure_enlisted_to_send(); // Throws
    }

    if (m_unbind_message_sent) {
        REALM_ASSERT(!m_enlisted_to_send);

        // If the sending of the UNBIND message has been initiated, this must be
        // the time when the sending of that message completes.
        m_unbind_message_send_complete = true;

        // Detect the completion of the unbinding process
        if (m_error_message_received || m_unbound_message_received) {
            // If the deactivation process has been initiated, it can now be
            // immediately completed.
            if (m_state == Deactivating) {
                // Life cycle state is Deactivating
                complete_deactivation(); // Throws
                // Life cycle state is now Deactivated
                return;
            }

            // The session is still in the Active state, so initiate the
            // rebinding process if the session is no longer suspended.
            if (!m_suspended)
                initiate_rebind(); // Throws
        }
    }
}

inline void ClientImpl::Session::initiate_rebind()
{
    // Life cycle state must be Active
    REALM_ASSERT(m_state == Active);

    REALM_ASSERT(!m_suspended);
    REALM_ASSERT(!m_enlisted_to_send);

    reset_protocol_state();

    // Notify the debug hook of the SessionResumed event before sending
    // the bind messsage
    call_debug_hook(SyncClientHookEvent::SessionResumed);

    // Ready to send BIND message
    enlist_to_send(); // Throws
}

inline void ClientImpl::Session::reset_protocol_state() noexcept
{
    m_enlisted_to_send = false;
    m_bind_message_sent = false;
    m_error_to_send = false;
    m_ident_message_sent = false;
    m_unbind_message_sent = false;
    m_unbind_message_send_complete = false;
    m_error_message_received = false;
    m_unbound_message_received = false;
    m_client_error = util::none;
    m_pending_compensating_write_errors.clear();

    m_upload_progress = m_progress.upload;
    m_last_download_mark_sent = m_last_download_mark_received;
}

inline void ClientImpl::Session::ensure_enlisted_to_send()
{
    if (!m_enlisted_to_send)
        enlist_to_send(); // Throws
}

// This function will never "commit suicide" despite the fact that it may
// involve an invocation of send_message(), which in certain cases can lead to
// the completion of the deactivation process, and if that did happen, it would
// cause Connection::send_next_message() to destroy this session, but it does
// not happen.
//
// If the session is already in the Deactivating state, send_message() will
// complete the deactivation process immediately when, and only when the BIND
// message has not already been sent.
//
// Note however, that this function gets called when the establishment of the
// connection completes, but at that time, the session cannot be in the
// Deactivating state, because until the BIND message is sent, the deactivation
// process will complete immediately. So the first invocation of this function
// after establishemnt of the connection will not commit suicide.
//
// Note then, that the session will stay enlisted to send, until it gets to send
// the BIND message, and since the and enlist_to_send() must not be called while
// the session is enlisted, the next invocation of this function will be after
// the BIND message has been sent, but then the deactivation process will no
// longer be completed by send_message().
inline void ClientImpl::Session::enlist_to_send()
{
    REALM_ASSERT(m_state == Active || m_state == Deactivating);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(!m_enlisted_to_send);
    m_enlisted_to_send = true;
    m_conn.enlist_to_send(this); // Throws
}

} // namespace realm::sync

#endif // REALM_NOINST_CLIENT_IMPL_BASE_HPP
