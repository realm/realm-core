
#ifndef REALM_NOINST_CLIENT_IMPL_BASE_HPP
#define REALM_NOINST_CLIENT_IMPL_BASE_HPP

#include <cstdint>
#include <utility>
#include <functional>
#include <deque>
#include <map>
#include <string>
#include <random>

#include <realm/binary_data.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/buffer_stream.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/network_ssl.hpp>
#include <realm/util/websocket.hpp>
#include <realm/sync/noinst/protocol_codec.hpp>
#include <realm/sync/noinst/client_state_download.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/client.hpp>


namespace realm {
namespace _impl {

class ClientImplBase {
public:
    class Config;
    enum class ConnectionTerminationReason;
    class ReconnectInfo;
    class Connection;
    class Session;

    // clang-format off
    using ReconnectMode        = sync::Client::ReconnectMode;
    using RoundtripTimeHandler = sync::Client::RoundtripTimeHandler;
    using ProtocolEnvelope     = sync::ProtocolEnvelope;
    using ProtocolError        = sync::ProtocolError;
    using port_type            = sync::Session::port_type;
    using version_type         = sync::version_type;
    using timestamp_type       = sync::timestamp_type;
    using SaltedVersion        = sync::SaltedVersion;
    using milliseconds_type    = sync::milliseconds_type;
    using OutputBuffer         = util::ResettableExpandableBufferOutputStream;
    // clang-format on

    using EventLoopMetricsHandler = util::network::Service::EventLoopMetricsHandler;

    util::Logger& logger;

    static constexpr int get_oldest_supported_protocol_version() noexcept;

    // @{
    /// These call stop(), run(), and report_event_loop_metrics() on the service
    /// object (get_service()) respectively.
    void stop() noexcept;
    void run();
    void report_event_loop_metrics(std::function<EventLoopMetricsHandler>);
    // @}

    const std::string& get_user_agent_string() const noexcept;
    ReconnectMode get_reconnect_mode() const noexcept;
    bool is_dry_run() const noexcept;
    bool get_tcp_no_delay() const noexcept;
    util::network::Service& get_service() noexcept;
    std::mt19937_64& get_random() noexcept;

    /// Returns false if the specified URL is invalid.
    bool decompose_server_url(const std::string& url, ProtocolEnvelope& protocol, std::string& address,
                              port_type& port, std::string& path) const;

protected:
    ClientImplBase(Config);
    ~ClientImplBase();

private:
    using file_ident_type = sync::file_ident_type;
    using salt_type = sync::salt_type;
    using session_ident_type = sync::session_ident_type;
    using request_ident_type = sync::request_ident_type;
    using SaltedFileIdent = sync::SaltedFileIdent;
    using ClientError = sync::Client::Error;

    const ReconnectMode m_reconnect_mode; // For testing purposes only
    const milliseconds_type m_connect_timeout;
    const milliseconds_type m_connection_linger_time;
    const milliseconds_type m_ping_keepalive_period;
    const milliseconds_type m_pong_keepalive_timeout;
    const milliseconds_type m_fast_reconnect_limit;
    const bool m_disable_upload_activation_delay;
    const bool m_dry_run; // For testing purposes only
    const bool m_tcp_no_delay;
    const bool m_enable_default_port_hack;
    const bool m_disable_upload_compaction;
    const std::function<RoundtripTimeHandler> m_roundtrip_time_handler;
    const std::string m_user_agent_string;
    util::network::Service m_service;
    std::mt19937_64 m_random;
    ClientProtocol m_client_protocol;
    session_ident_type m_prev_session_ident = 0;

    static std::string make_user_agent_string(Config&);

    session_ident_type get_next_session_ident() noexcept;
};

constexpr int ClientImplBase::get_oldest_supported_protocol_version() noexcept
{
    // See sync::get_current_protocol_version() for information about the
    // individual protocol versions.
    return 1;
}

static_assert(ClientImplBase::get_oldest_supported_protocol_version() >= 1, "");
static_assert(ClientImplBase::get_oldest_supported_protocol_version() <= sync::get_current_protocol_version(), "");


/// See sync::Client::Config for the meaning of the individual properties.
class ClientImplBase::Config {
public:
    std::string user_agent_platform_info;
    std::string user_agent_application_info;
    util::Logger* logger = nullptr;
    ReconnectMode reconnect_mode = ReconnectMode::normal;
    milliseconds_type connect_timeout = sync::Client::default_connect_timeout;
    milliseconds_type connection_linger_time = sync::Client::default_connection_linger_time;
    milliseconds_type ping_keepalive_period = sync::Client::default_ping_keepalive_period;
    milliseconds_type pong_keepalive_timeout = sync::Client::default_pong_keepalive_timeout;
    milliseconds_type fast_reconnect_limit = sync::Client::default_fast_reconnect_limit;
    bool disable_upload_activation_delay = false;
    bool dry_run = false;
    bool tcp_no_delay = false;
    bool enable_default_port_hack = false;
    bool disable_upload_compaction = false;
    std::function<RoundtripTimeHandler> roundtrip_time_handler;
};


/// Information about why a connection (or connection initiation attempt) was
/// terminated. This is used to determinte the delay until the next connection
/// initiation attempt.
enum class ClientImplBase::ConnectionTerminationReason {
    resolve_operation_canceled,        ///< Resolve operation (DNS) aborted by client
    resolve_operation_failed,          ///< Failure during resolve operation (DNS)
    connect_operation_canceled,        ///< TCP connect operation aborted by client
    connect_operation_failed,          ///< Failure during TCP connect operation
    closed_voluntarily,                ///< Voluntarily closed after successful connect operation
    premature_end_of_input,            ///< Premature end of input (before ERROR message was received)
    read_or_write_error,               ///< Read/write error after successful TCP connect operation
    http_tunnel_failed,                ///< Failure to establish HTTP tunnel with proxy
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


/// Per-server endpoint information used to determine reconnect delays.
class ClientImplBase::ReconnectInfo {
public:
    void reset() noexcept;

private:
    using milliseconds_lim = std::numeric_limits<milliseconds_type>;

    // When `m_reason` is present, it indicates that a connection attempt was
    // initiated, and that a new reconnect delay must be computed before
    // initiating another connection attempt. In this case, `m_time_point` is
    // the point in time from which the next delay should count. It will
    // generally be the time at which the last connection attempt was initiated,
    // but for certain connection termination reasons, it will instead be the
    // time at which the connection was closed. `m_delay` will generally be the
    // duration of the delay that preceded the last connection attempt, and can
    // be used as a basis for computing the next delay.
    //
    // When `m_reason` is absent, it indicates that a new reconnect delay has
    // been computed, and `m_time_point` will be the time at which the delay
    // expires (if equal to `milliseconds_lim::max()`, the delay is
    // indefinite). `m_delay` will generally be the duration of the computed
    // delay.
    //
    // Since `m_reason` is absent, and `m_timepoint` is zero initially, the
    // first reconnect delay will already have expired, so the effective delay
    // will be zero.
    util::Optional<ConnectionTerminationReason> m_reason;
    milliseconds_type m_time_point = 0;
    milliseconds_type m_delay = 0;

    // Set this flag to true to schedule a postponed invocation of reset(). See
    // Connection::cancel_reconnect_delay() for details and rationale.
    //
    // Will be set back to false when a PONG message arrives, and the
    // corresponding PING message was sent while `m_scheduled_reset` was
    // true. See receive_pong().
    bool m_scheduled_reset = false;

    friend class Connection;
};


/// All use of connection objects, including construction and destruction, must
/// occur on behalf of the event loop thread of the associated client object.
class ClientImplBase::Connection : public util::websocket::Config {
public:
    using SSLVerifyCallback = sync::Session::SSLVerifyCallback;
    using ProxyConfig = sync::Session::Config::ProxyConfig;
    using ReconnectInfo = ClientImplBase::ReconnectInfo;
    using port_type = ClientImplBase::port_type;
    using ReadCompletionHandler = util::websocket::ReadCompletionHandler;
    using WriteCompletionHandler = util::websocket::WriteCompletionHandler;

    util::PrefixLogger logger;

    ClientImplBase& get_client() noexcept;
    ReconnectInfo get_reconnect_info() const noexcept;
    ClientProtocol& get_client_protocol() noexcept;

    /// Activate this connection object. No attempt is made to establish a
    /// connection before the connection object is activated.
    void activate();

    /// Activate the specified session.
    ///
    /// Prior to being activated, no messages will be sent or received on behalf
    /// of this session, and the associated Realm file will not be accessed,
    /// i.e., Session::access_realm() will not be called.
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
    /// file will no longer be accessed, i.e., access_realm() will not be called
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

    /// Returns zero until the HTTP response is received. After that point in
    /// time, it returns the negotiated protocol version, which is based on the
    /// contents of the `Sec-WebSocket-Protocol` header in the HTTP
    /// response. The negotiated protocol version is guaranteed to be greater
    /// than or equal to get_oldest_supported_protocol_version(), and be less
    /// than or equal to sync::get_current_protocol_version().
    int get_negotiated_protocol_version() noexcept;

    // Overriding methods in util::websocket::Config
    util::Logger& websocket_get_logger() noexcept override;
    std::mt19937_64& websocket_get_random() noexcept override;
    void async_read(char*, std::size_t, ReadCompletionHandler) override;
    void async_read_until(char*, std::size_t, char, ReadCompletionHandler) override;
    void async_write(const char*, std::size_t, WriteCompletionHandler) override;
    void websocket_handshake_completion_handler(const util::HTTPHeaders&) override;
    void websocket_read_error_handler(std::error_code) override;
    void websocket_write_error_handler(std::error_code) override;
    void websocket_handshake_error_handler(std::error_code, const util::HTTPHeaders*,
                                           const util::StringView*) override;
    void websocket_protocol_error_handler(std::error_code) override;
    bool websocket_binary_message_received(const char*, std::size_t) override;
    bool websocket_pong_message_received(const char*, std::size_t) override;

protected:
    /// The application must ensure that the specified client object is kept
    /// alive at least until the connection object is destroyed.
    Connection(ClientImplBase&, std::string logger_prefix, ProtocolEnvelope, std::string address, port_type port,
               bool verify_servers_ssl_certificate, util::Optional<std::string> ssl_trust_certificate_path,
               std::function<SSLVerifyCallback>, util::Optional<ProxyConfig>, ReconnectInfo);
    virtual ~Connection();

    template <class H>
    void for_each_active_session(H handler);

    //@{
    /// These are called as the state of the connection changes between
    /// "disconnected", "connecting", and "connected". The initial state is
    /// always "disconnected". The next state after "disconnected" is always
    /// "connecting". The next state after "connecting" is either "connected" or
    /// "disconnected". The next state after "connected" is always
    /// "disconnected".
    ///
    /// A switch to the disconnected state only happens when an error occurs,
    /// and information about that error is passed to on_disconnected(). If \a
    /// custom_message is null, there is no custom message, and the message is
    /// whatever is returned by `ec.message()`.
    ///
    /// The default implementations of these functions do nothing.
    ///
    /// These functions are always called by the event loop thread of the
    /// associated client object.
    virtual void on_connecting();
    virtual void on_connected();
    virtual void on_disconnected(std::error_code, bool is_fatal, const StringData* custom_message);
    //@}

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
    virtual void on_idle();

    virtual std::string get_http_request_path() const = 0;

    /// The application can override this function to set custom headers. The
    /// default implementation sets no headers.
    virtual void set_http_request_headers(util::HTTPHeaders&);

private:
    using SyncProgress = sync::SyncProgress;
    using ReceivedChangesets = ClientProtocol::ReceivedChangesets;

    ClientImplBase& m_client;
    util::Optional<util::network::Resolver> m_resolver;
    util::Optional<util::network::Socket> m_socket;
    util::Optional<util::network::ssl::Context> m_ssl_context;
    util::Optional<util::network::ssl::Stream> m_ssl_stream;
    util::network::ReadAheadBuffer m_read_ahead_buffer;
    util::websocket::Socket m_websocket;
    const ProtocolEnvelope m_protocol_envelope;
    const std::string m_address;
    const port_type m_port;
    const std::string m_http_host; // Contents of `Host:` request header
    const bool m_verify_servers_ssl_certificate;
    const util::Optional<std::string> m_ssl_trust_certificate_path;
    const std::function<SSLVerifyCallback> m_ssl_verify_callback;
    const util::Optional<ProxyConfig> m_proxy_config;
    util::Optional<util::HTTPClient<Connection>> m_proxy_client;
    ReconnectInfo m_reconnect_info;
    int m_negotiated_protocol_version = 0;

    enum class State { disconnected, connecting, connected };
    State m_state = State::disconnected;

    std::size_t m_num_active_unsuspended_sessions = 0;
    std::size_t m_num_active_sessions = 0;
    util::network::Trigger m_on_idle;

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

    // The timer will be constructed on demand, and will only be destroyed when
    // canceling a reconnect or disconnect delay.
    //
    // It is necessary to destroy and recreate the timer when canceling a wait
    // operation, because the next wait operation might need to be initiated
    // before the completion handler of the previous canceled wait operation
    // starts executing. Such an overlap is not allowed for wait operations on
    // the same timer instance.
    util::Optional<util::network::DeadlineTimer> m_reconnect_disconnect_timer;

    // Timer for connect operation watchdog. For why this timer is optional, see
    // `m_reconnect_disconnect_timer`.
    util::Optional<util::network::DeadlineTimer> m_connect_timer;

    // This timer is used to schedule the sending of PING messages, and as a
    // watchdog for timely reception of PONG messages. For why this timer is
    // optional, see `m_reconnect_disconnect_timer`.
    util::Optional<util::network::DeadlineTimer> m_heartbeat_timer;

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

    void initiate_reconnect_wait();
    void handle_reconnect_wait(std::error_code);
    void initiate_reconnect();
    void initiate_connect_wait();
    void handle_connect_wait(std::error_code);
    void initiate_resolve();
    void handle_resolve(std::error_code, util::network::Endpoint::List);
    void initiate_tcp_connect(util::network::Endpoint::List, std::size_t);
    void handle_tcp_connect(std::error_code, util::network::Endpoint::List, std::size_t);
    void initiate_http_tunnel();
    void handle_http_tunnel(std::error_code);
    void initiate_websocket_or_ssl_handshake();
    void initiate_ssl_handshake();
    void handle_ssl_handshake(std::error_code);
    void initiate_websocket_handshake();
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
    void handle_message_received(const char* data, std::size_t size);
    void handle_pong_received(const char* data, std::size_t size);
    void initiate_disconnect_wait();
    void handle_disconnect_wait(std::error_code);
    void resolve_error(std::error_code);
    void tcp_connect_error(std::error_code);
    void http_tunnel_error(std::error_code);
    void ssl_handshake_error(std::error_code);
    void read_error(std::error_code);
    void write_error(std::error_code);
    void close_due_to_protocol_error(std::error_code);
    void close_due_to_missing_protocol_feature();
    void close_due_to_client_side_error(std::error_code, bool is_fatal);
    void close_due_to_server_side_error(ProtocolError, StringData message, bool try_again);
    void voluntary_disconnect();
    void involuntary_disconnect(std::error_code ec, bool is_fatal, StringData* custom_message);
    void disconnect(std::error_code ec, bool is_fatal, StringData* custom_message);
    void change_state_to_disconnected() noexcept;

    // These are only called from ClientProtocol class.
    void receive_pong(milliseconds_type timestamp);
    void receive_error_message(int error_code, StringData message, bool try_again, session_ident_type);
    void receive_ident_message(session_ident_type, SaltedFileIdent);
    void receive_client_version_message(session_ident_type session_ident, version_type client_version);
    void receive_state_message(session_ident_type session_ident, version_type server_version,
                               salt_type server_version_salt, uint_fast64_t begin_offset, uint_fast64_t end_offset,
                               uint_fast64_t max_offset, BinaryData chunk);
    void receive_download_message(session_ident_type, const SyncProgress&, std::uint_fast64_t downloadable_bytes,
                                  const ReceivedChangesets&);
    void receive_mark_message(session_ident_type, request_ident_type);
    void receive_alloc_message(session_ident_type, file_ident_type file_ident);
    void receive_unbound_message(session_ident_type);
    void handle_protocol_error(ClientProtocol::Error);

    // These are only called from Session class.
    void enlist_to_send(Session*);
    void one_more_active_unsuspended_session();
    void one_less_active_unsuspended_session();

    OutputBuffer& get_output_buffer() noexcept;
    ConnectionTerminationReason determine_connection_termination_reason(std::error_code) noexcept;
    Session* get_session(session_ident_type) const noexcept;
    static bool was_voluntary(ConnectionTerminationReason) noexcept;

    friend class ClientProtocol;
    friend class Session;
};


/// A synchronization session between a local and a remote Realm file.
///
/// All use of session objects, including construction and destruction, must
/// occur on the event loop thread of the associated client object.
class ClientImplBase::Session {
public:
    class Config;

    using SyncProgress = sync::SyncProgress;
    using VersionInfo = sync::VersionInfo;
    using ClientHistoryBase = sync::ClientReplicationBase;
    using ReceivedChangesets = ClientProtocol::ReceivedChangesets;
    using IntegrationError = ClientHistoryBase::IntegrationError;

    util::PrefixLogger logger;

    ClientImplBase& get_client() noexcept;
    Connection& get_connection() noexcept;
    session_ident_type get_ident() const noexcept;
    SyncProgress get_sync_progress() const noexcept;

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

    /// \brief Request notification when all changesets in the local history
    /// have been uploaded to the server.
    ///
    /// When uploading completes, on_upload_completion() will be called by the
    /// thread that processes the event loop (as long as such a thread exists).
    ///
    /// IMPORTANT: on_upload_completion() may get called before
    /// request_upload_completion_notification() returns (reentrant callback).
    ///
    /// If request_upload_completion_notification() is called while a previously
    /// requested completion notification has not yet occurred, the previous
    /// request is canceled and the corresponding notification will never
    /// occur. This ensure that there is no ambiguity about the meaning of each
    /// completion notification.
    ///
    /// The application must be prepared for "spurious" invocations of
    /// on_upload_completion() before the client's first invocation of
    /// request_upload_completion_notification(), or after a previous invocation
    /// of on_upload_completion(), as long as it is before the subsequent
    /// invocation by the client of
    /// request_upload_completion_notification(). This is possible because the
    /// client reserves the right to request upload completion notifications
    /// internally.
    ///
    /// Upload is considered complete when all changesets in the history, that
    /// are supposed to be uploaded, and that precede `current_client_version`,
    /// have been uploaded and acknowledged by the
    /// server. `current_client_version` is generally the version that refers to
    /// the last changeset in the history, but more precisely, it may be any
    /// version between the last version reported by the application through
    /// recognize_sync_version() and the version referring to the last history
    /// entry (both ends inclusive).
    ///
    /// If new changesets are added to the history while a previously requested
    /// completion notification has not yet occurred, it is unspecified whether
    /// the addition of those changesets will cause `current_client_version` to
    /// be bumped or stay fixed, regardless of whether they are advertised via
    /// recognize_sync_version().
    ///
    /// It is an error to call this function before activation of the session,
    /// or after initiation of deactivation.
    void request_upload_completion_notification();

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

    /// \brief Make this client request a new file identifier from the server
    /// for a subordinate client.
    ///
    /// The application is allowed to request additional file identifiers while
    /// it is waiting to receive others.
    ///
    /// The requested file identifiers will be passed back to the application as
    /// they become available. This happens through the virtual callback
    /// function on_subtier_file_ident(), which the application will need to
    /// override. on_subtier_file_ident() will be called once for each requested
    /// identifier as it becomes available.
    ///
    /// The callback function is guaranteed to not be called until after
    /// request_subtier_file_ident() returns (no callback reentrance).
    ///
    /// It is an error to call this function before activation of the session,
    /// or after initiation of deactivation.
    void request_subtier_file_ident();

    /// \brief Announce that a new access token is available.
    ///
    /// By calling this function, the application announces to the session
    /// object that a new access token has been made available, and that it can
    /// be fetched by calling get_signed_access_token().
    ///
    /// This function will not resume a session that has already been suspended
    /// by an error (e.g., `ProtocolError::token_expired`). If the application
    /// wishes to resume such a session, it should follow up with a call to
    /// cancel_resumption_delay().
    ///
    /// Even if the session is not suspended when this function is called, it
    /// may end up becoming suspended before the new access token is delivered
    /// to the server. For example, the prior access token may expire before the
    /// new access token is received by the server, but the ERROR message may
    /// not arrive on the client until after the new token is made available by
    /// the application. This means that the application must be prepared to
    /// receive `ProtocolError::token_expired` after making a new access token
    /// available, even when the new token has not expired. Fortunately, this
    /// should be a rare event, so the application can choose to handle this by
    /// "blindly" renewing the token again, even though such a renewal is
    /// technically redundant.
    ///
    /// FIXME: Improve the implementation of new_access_token_available() such
    /// that there is no risk of getting the session suspended by
    /// `ProtocolError::token_expired` after a new access token has been made
    /// available. Doing this right, requires protocol changes: Add sequence
    /// number to REFRESH messages sent by client, and introduce a REFRESH
    /// response message telling the client that a particular token has been
    /// received by the server.
    ///
    /// IMPORTANT: get_signed_access_token() may get called before
    /// new_access_token_available() returns (reentrant callback).
    ///
    /// It is an error to call this function before activation of the session,
    /// or after initiation of deactivation.
    void new_access_token_available();

    /// If this session is currently suspended, resume it immediately.
    ///
    /// It is an error to call this function before activation of the session,
    /// or after initiation of deactivation.
    void cancel_resumption_delay();

    /// To be used in connection with implementations of
    /// initiate_integrate_changesets().
    ///
    /// This function is thread-safe, but if called from a thread other than the
    /// event loop thread of the associated client object, the specified history
    /// accessor must **not** be the one made available by access_realm().
    bool integrate_changesets(ClientHistoryBase&, const SyncProgress&, std::uint_fast64_t downloadable_bytes,
                              const ReceivedChangesets&, VersionInfo&, IntegrationError&);

    /// To be used in connection with implementations of
    /// initiate_integrate_changesets().
    ///
    /// If \a success is true, the value of \a error does not matter. If \a
    /// success is false, the values of \a client_version and \a
    /// download_progress do not matter.
    ///
    /// It is an error to call this function before activation of the session
    /// (Connection::activate_session()), or after initiation of deactivation
    /// (Connection::initiate_session_deactivation()).
    void on_changesets_integrated(bool success, version_type client_version, sync::DownloadCursor download_progress,
                                  IntegrationError error);

    virtual ~Session();

protected:
    using SyncTransactReporter = ClientHistoryBase::SyncTransactReporter;

    /// The application must ensure that the new session object is either
    /// activated (Connection::activate_session()) or destroyed before the
    /// specified connection object is destroyed.
    ///
    /// The specified transaction reporter (via the config object) is guaranteed
    /// to not be called before activation, and also not after initiation of
    /// deactivation.
    Session(Connection&, Config);

    /// Fetch a reference to the remote virtual path of the Realm associated
    /// with this session.
    ///
    /// This function is always called by the event loop thread of the
    /// associated client object.
    ///
    /// This function is guaranteed to not be called before activation, and also
    /// not after initiation of deactivation.
    virtual const std::string& get_virt_path() const noexcept = 0;

    /// Fetch a reference to the signed access token.
    ///
    /// This function is always called by the event loop thread of the
    /// associated client object.
    ///
    /// This function is guaranteed to not be called before activation, and also
    /// not after initiation of deactivation.
    ///
    /// FIXME: For the upstream client of a 2nd tier server it is not ideal that
    /// the admin token needs to be uploaded for every session.
    virtual const std::string& get_signed_access_token() const noexcept = 0;

    virtual const std::string& get_realm_path() const noexcept = 0;

    /// The implementation need only ensure that the returned reference stays valid
    /// until the next invocation of access_realm() on one of the session
    /// objects associated with the same client object.
    ///
    /// This function is always called by the event loop thread of the
    /// associated client object.
    ///
    /// This function is guaranteed to not be called before activation, and also
    /// not after initiation of deactivation.
    virtual ClientHistoryBase& access_realm() = 0;

    /// Gets the encryption key used for Realm file encryption. The default
    /// implementation returns none.
    virtual util::Optional<std::array<char, 64>> get_encryption_key() const noexcept;

    // client_reset_config() returns the config for async open and client
    // reset. If it returns none, ordinary sync is used. If it returns a
    // Config::ClientReset, the session will be initiated with a state Realm
    // transfer from the server.
    virtual const util::Optional<sync::Session::Config::ClientReset>& get_client_reset_config() const noexcept;

    /// on_state_download_progress() is called with progress information if
    /// state download is employed. The default implementation does nothing.
    virtual void on_state_download_progress(std::uint_fast64_t downloaded_bytes,
                                            std::uint_fast64_t downloadable_bytes);

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
    /// The integration of the specified changesets must happen by means of an
    /// invocation of integrate_changesets(), but not necessarily using the
    /// history accessor made available by access_realm().
    ///
    /// The implementation is allowed, but not obliged to aggregate changesets
    /// from multiple invocations of initiate_integrate_changesets() and pass
    /// them to sync::ClientHistoryBase::integrate_server_changesets() at once.
    ///
    /// The synchronization progress passed to
    /// sync::ClientHistoryBase::integrate_server_changesets() must be obtained
    /// by calling get_sync_progress(), and that call must occur after the last
    /// invocation of initiate_integrate_changesets() whose changesets are
    /// included in what is passed to
    /// sync::ClientHistoryBase::integrate_server_changesets().
    ///
    /// The download cursor passed to on_changesets_integrated() must be
    /// SyncProgress::download of the synchronization progress passed to the
    /// last invocation of
    /// sync::ClientHistoryBase::integrate_server_changesets().
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
    virtual void initiate_integrate_changesets(std::uint_fast64_t downloadable_bytes, const ReceivedChangesets&);

    /// See request_upload_completion_notification().
    ///
    /// The default implementation does nothing.
    virtual void on_upload_completion();

    /// See request_download_completion_notification().
    ///
    /// The default implementation does nothing.
    virtual void on_download_completion();

    /// By returning true, this function indicates to the session that the
    /// received file identifier is valid. If the identfier is invald, this
    /// function should return false.
    ///
    /// For more, see request_subtier_file_ident().
    ///
    /// The default implementation returns false, so it must be overridden if
    /// request_subtier_file_ident() is ever called.
    virtual bool on_subtier_file_ident(file_ident_type);

    //@{
    /// These are called as the state of the session changes between
    /// "suspended" and "resumed". The initial state is
    /// always "resumed".
    ///
    /// A switch to the suspended state only happens when an error occurs,
    /// and information about that error is passed to on_suspended().
    ///
    /// The default implementations of these functions do nothing.
    ///
    /// These functions are always called by the event loop thread of the
    /// associated client object.
    ///
    /// These functions are guaranteed to not be called before activation, and also
    /// not after initiation of deactivation.
    virtual void on_suspended(std::error_code ec, StringData message, bool is_fatal);
    virtual void on_resumed();
    //@}

private:
    using UploadCursor = sync::UploadCursor;
    using DownloadCursor = sync::DownloadCursor;

    Connection& m_conn;
    const session_ident_type m_ident;
    SyncTransactReporter* const m_sync_transact_reporter;
    const bool m_disable_upload;
    const bool m_disable_empty_upload;
    const bool m_is_subserver;

    // Session life cycle state:
    //
    //   State          m_deactivation_initiated  m_active_or_deactivating
    //   -----------------------------------------------------------------
    //   Unactivated    false                     false
    //   Active         false                     TRUE
    //   Deactivating   TRUE                      TRUE
    //   Deactivated    TRUE                      false
    //
    // The transition from Deactivating to Deactivated state happens when the
    // unbinding process completes (unbind_process_complete()).
    bool m_deactivation_initiated = false;
    bool m_active_or_deactivating = false;

    bool m_suspended = false;

    // Set to false when a new access token is available and needs to be
    // uploaded to the server. Set to true when uploading of the token has been
    // initiated via a BIND or a REFRESH message.
    bool m_access_token_sent = false;

    // Set to true when download completion is reached. Set to false after a
    // slow reconnect, such that the upload process will become suspended until
    // download completion is reached again.
    bool m_allow_upload = false;

    bool m_upload_completion_notification_requested = false;

    // These are reset when the session is activated, and again whenever the
    // connection is lost or the rebinding process is initiated.
    bool m_enlisted_to_send;
    bool m_bind_message_sent;                   // Sending of BIND message has been initiated
    bool m_client_version_request_message_sent; // Sending of CLIENT_VERSION_REQUEST has been initiated
    bool m_state_request_message_sent;          // Sending of STATE_REQUEST message has been initiated
    bool m_ident_message_sent;                  // Sending of IDENT message has been initiated
    bool m_alloc_message_sent;                  // See send_alloc_message()
    bool m_unbind_message_sent;                 // Sending of UNBIND message has been initiated
    bool m_unbind_message_sent_2;               // Sending of UNBIND message has been completed
    bool m_error_message_received;              // Session specific ERROR message received
    bool m_unbound_message_received;            // UNBOUND message received

    // True if and only if state download is in progress.
    bool m_state_download_in_progress = false;

    // True if and only if the session is performing a client reset.
    bool m_client_reset = false;

    // A client reset Config parameter.
    bool m_client_reset_recover_local_changes = true;

    // `ident == 0` means unassigned.
    SaltedFileIdent m_client_file_ident = {0, 0};

    // m_client_state_download controls state download and is used for
    // async open and client reset.
    std::unique_ptr<ClientStateDownload> m_client_state_download;

    // The latest sync progress reported by the server via a DOWNLOAD
    // message. See struct SyncProgress for a description. The values stored in
    // `m_progress` either are persisted, or are about to be.
    //
    // Initialized by way of ClientHistoryBase::get_status() at session
    // activation time.
    //
    // `m_progress.upload.client_version` is the client-side sync version
    // produced by the latest local changeset that has been acknowledged as
    // integrated by the server.
    SyncProgress m_progress;

    // In general, the local version produced by the last changeset in the local
    // history. The uploading process will never advance beyond this point. The
    // changeset that produced this version may, or may not contain changes of
    // local origin.
    //
    // It is set to the current version of the local Realm at session activation
    // time (although always zero for the initial empty Realm
    // state). Thereafter, it is generally updated when the application calls
    // recognize_sync_version() and when changesets are received from the server
    // and integrated locally.
    //
    // INVARIANT: m_progress.upload.client_version <= m_last_version_available
    version_type m_last_version_available = 0;

    // The target version for the upload process. When the upload cursor
    // (`m_upload_progress`) reaches `m_upload_target_version`, uploading stops.
    //
    // In general, `m_upload_target_version` follows `m_last_version_available`
    // as it is increased, but in some cases, `m_upload_target_version` will be
    // kept fixed for a while in order to constrain the uploading process.
    //
    // Is set equal to `m_last_version_available` at session activation time.
    //
    // INVARIANT: m_upload_target_version <= m_last_version_available
    version_type m_upload_target_version = 0;

    // In general, this is the position in the history reached while scanning
    // for changesets to be uploaded.
    //
    // Set to `m_progress.upload` at session activation time and whenever the
    // connection to the server is lost. When the connection is established, the
    // scanning for changesets to be uploaded then progresses from there towards
    // `m_upload_target_version`.
    //
    // INVARIANT: m_progress.upload.client_version <= m_upload_progress.client_version
    // INVARIANT: m_upload_progress.client_version <= m_upload_target_version
    UploadCursor m_upload_progress = {0, 0};

    // Set to `m_progress.upload.client_version` at session activation time and
    // whenever the connection to the server is lost. Otherwise it is the
    // version of the latest changeset that has been selected for upload while
    // scanning the history.
    //
    // INVARIANT: m_progress.upload.client_version <= m_last_version_selected_for_upload
    // INVARIANT: m_last_version_selected_for_upload <= m_upload_progress.client_version
    version_type m_last_version_selected_for_upload = 0;

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

    std::int_fast32_t m_num_outstanding_subtier_allocations = 0;

    util::Optional<sync::Session::Config::ClientReset> m_client_reset_config = util::none;

    static std::string make_logger_prefix(session_ident_type);

    Session(Connection&, session_ident_type, Config);

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
    void connection_lost();
    void send_message();
    void message_sent();
    void send_bind_message();
    void send_ident_message();
    void send_client_version_request_message();
    void send_state_request_message();
    void send_upload_message();
    void send_mark_message();
    void send_alloc_message();
    void send_refresh_message();
    void send_unbind_message();
    std::error_code receive_ident_message(SaltedFileIdent);
    void receive_client_version_message(version_type client_version);
    void receive_state_message(version_type server_version, salt_type server_version_salt, uint_fast64_t begin_offset,
                               uint_fast64_t end_offset, uint_fast64_t max_offset, BinaryData chunk);
    void receive_download_message(const SyncProgress&, std::uint_fast64_t downloadable_bytes,
                                  const ReceivedChangesets&);
    std::error_code receive_mark_message(request_ident_type);
    std::error_code receive_alloc_message(file_ident_type file_ident);
    std::error_code receive_unbound_message();
    std::error_code receive_error_message(int error_code, StringData message, bool try_again);

    void initiate_rebind();
    void reset_protocol_state() noexcept;
    void ensure_enlisted_to_send();
    void enlist_to_send();
    void update_progress(const SyncProgress&);
    bool check_received_sync_progress(const SyncProgress&) noexcept;
    bool check_received_sync_progress(const SyncProgress&, int&) noexcept;
    void check_for_upload_completion();
    void check_for_download_completion();

    friend class Connection;
};


/// See sync::Client::Session for the meaning of the individual properties
/// (other than `sync_transact_reporter`).
class ClientImplBase::Session::Config {
public:
    SyncTransactReporter* sync_transact_reporter = nullptr;
    bool disable_upload = false;
    bool disable_empty_upload = false;
    bool is_subserver = false;
};


// Implementation

inline void ClientImplBase::stop() noexcept
{
    m_service.stop();
}

inline void ClientImplBase::run()
{
    m_service.run(); // Throws
}

inline void ClientImplBase::report_event_loop_metrics(std::function<EventLoopMetricsHandler> handler)
{
    m_service.report_event_loop_metrics(std::move(handler)); // Throws
}

inline const std::string& ClientImplBase::get_user_agent_string() const noexcept
{
    return m_user_agent_string;
}

inline auto ClientImplBase::get_reconnect_mode() const noexcept -> ReconnectMode
{
    return m_reconnect_mode;
}

inline bool ClientImplBase::is_dry_run() const noexcept
{
    return m_dry_run;
}

inline bool ClientImplBase::get_tcp_no_delay() const noexcept
{
    return m_tcp_no_delay;
}

inline util::network::Service& ClientImplBase::get_service() noexcept
{
    return m_service;
}

inline std::mt19937_64& ClientImplBase::get_random() noexcept
{
    return m_random;
}

inline ClientImplBase::~ClientImplBase() {}

inline auto ClientImplBase::get_next_session_ident() noexcept -> session_ident_type
{
    return ++m_prev_session_ident;
}

inline void ClientImplBase::ReconnectInfo::reset() noexcept
{
    m_reason = util::none;
    m_time_point = 0;
    m_delay = 0;
    m_scheduled_reset = false;
}

inline ClientImplBase& ClientImplBase::Connection::get_client() noexcept
{
    return m_client;
}

inline auto ClientImplBase::Connection::get_reconnect_info() const noexcept -> ReconnectInfo
{
    return m_reconnect_info;
}

inline auto ClientImplBase::Connection::get_client_protocol() noexcept -> ClientProtocol&
{
    return m_client.m_client_protocol;
}

inline int ClientImplBase::Connection::get_negotiated_protocol_version() noexcept
{
    return m_negotiated_protocol_version;
}

inline ClientImplBase::Connection::~Connection() {}

template <class H>
void ClientImplBase::Connection::for_each_active_session(H handler)
{
    for (auto& p : m_sessions) {
        Session& sess = *p.second;
        if (!sess.m_deactivation_initiated)
            handler(sess); // Throws
    }
}

inline void ClientImplBase::Connection::voluntary_disconnect()
{
    REALM_ASSERT(m_reconnect_info.m_reason && was_voluntary(*m_reconnect_info.m_reason));
    std::error_code ec = sync::Client::Error::connection_closed;
    bool is_fatal = false;
    StringData* custom_message = nullptr;
    disconnect(ec, is_fatal, custom_message); // Throws
}

inline void ClientImplBase::Connection::involuntary_disconnect(std::error_code ec, bool is_fatal,
                                                               StringData* custom_message)
{
    REALM_ASSERT(m_reconnect_info.m_reason && !was_voluntary(*m_reconnect_info.m_reason));
    disconnect(ec, is_fatal, custom_message); // Throws
}

inline void ClientImplBase::Connection::change_state_to_disconnected() noexcept
{
    REALM_ASSERT(m_state != State::disconnected);
    m_state = State::disconnected;

    if (m_num_active_sessions == 0)
        m_on_idle.trigger();

    REALM_ASSERT(!m_reconnect_delay_in_progress);
    if (m_disconnect_delay_in_progress) {
        m_reconnect_disconnect_timer = util::none;
        m_disconnect_delay_in_progress = false;
    }
}

inline void ClientImplBase::Connection::one_more_active_unsuspended_session()
{
    if (m_num_active_unsuspended_sessions++ != 0)
        return;
    // Rose from zero to one
    if (m_state == State::disconnected && !m_reconnect_delay_in_progress && m_activated)
        initiate_reconnect(); // Throws
}

inline void ClientImplBase::Connection::one_less_active_unsuspended_session()
{
    if (--m_num_active_unsuspended_sessions != 0)
        return;
    // Dropped from one to zero
    if (m_state != State::disconnected)
        initiate_disconnect_wait(); // Throws
}

// Sessions, and the connection, should get the output_buffer and insert a message,
// after which they call initiate_write_output_buffer(Session* sess).
inline auto ClientImplBase::Connection::get_output_buffer() noexcept -> OutputBuffer&
{
    m_output_buffer.reset();
    return m_output_buffer;
}

inline auto ClientImplBase::Connection::get_session(session_ident_type ident) const noexcept -> Session*
{
    auto i = m_sessions.find(ident);
    bool found = (i != m_sessions.end());
    return found ? i->second.get() : nullptr;
}

inline bool ClientImplBase::Connection::was_voluntary(ConnectionTerminationReason reason) noexcept
{
    switch (reason) {
        case ConnectionTerminationReason::resolve_operation_canceled:
        case ConnectionTerminationReason::connect_operation_canceled:
        case ConnectionTerminationReason::closed_voluntarily:
            return true;
        case ConnectionTerminationReason::resolve_operation_failed:
        case ConnectionTerminationReason::connect_operation_failed:
        case ConnectionTerminationReason::premature_end_of_input:
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
        case ConnectionTerminationReason::http_tunnel_failed:
            break;
    }
    return false;
}

inline ClientImplBase& ClientImplBase::Session::get_client() noexcept
{
    return m_conn.get_client();
}

inline auto ClientImplBase::Session::get_connection() noexcept -> Connection&
{
    return m_conn;
}

inline auto ClientImplBase::Session::get_ident() const noexcept -> session_ident_type
{
    return m_ident;
}

inline auto ClientImplBase::Session::get_sync_progress() const noexcept -> SyncProgress
{
    return m_progress;
}

inline void ClientImplBase::Session::recognize_sync_version(version_type version)
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    bool resume_upload = do_recognize_sync_version(version);
    if (REALM_LIKELY(resume_upload)) {
        // Since the deactivation process has not been initiated, the UNBIND
        // message cannot have been sent unless an ERROR message was received.
        REALM_ASSERT(m_error_message_received || !m_unbind_message_sent);
        if (m_ident_message_sent && !m_error_message_received)
            ensure_enlisted_to_send(); // Throws
    }
}

inline void ClientImplBase::Session::request_upload_completion_notification()
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    m_upload_completion_notification_requested = true;
    check_for_upload_completion(); // Throws
}

inline void ClientImplBase::Session::request_download_completion_notification()
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    ++m_target_download_mark;

    // Since the deactivation process has not been initiated, the UNBIND message
    // cannot have been sent unless an ERROR message was received.
    REALM_ASSERT(m_error_message_received || !m_unbind_message_sent);
    if (m_ident_message_sent && !m_error_message_received)
        ensure_enlisted_to_send(); // Throws
}

inline void ClientImplBase::Session::request_subtier_file_ident()
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    bool was_zero = (m_num_outstanding_subtier_allocations == 0);
    ++m_num_outstanding_subtier_allocations;

    // Since the deactivation process has not been initiated, the UNBIND message
    // cannot have been sent unless an ERROR message was received.
    REALM_ASSERT(m_error_message_received || !m_unbind_message_sent);
    if (was_zero && m_ident_message_sent && !m_error_message_received) {
        if (!m_alloc_message_sent)
            ensure_enlisted_to_send(); // Throws
    }
}

inline void ClientImplBase::Session::new_access_token_available()
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    m_access_token_sent = false;

    // Since the deactivation process has not been initiated, the UNBIND message
    // cannot have been sent unless an ERROR message was received.
    REALM_ASSERT(m_error_message_received || !m_unbind_message_sent);
    if (m_bind_message_sent && !m_error_message_received)
        ensure_enlisted_to_send(); // Throws
}

inline ClientImplBase::Session::Session(Connection& conn, Config config)
    : Session{conn, conn.get_client().get_next_session_ident(), std::move(config)} // Throws
{
}

inline ClientImplBase::Session::Session(Connection& conn, session_ident_type ident, Config config)
    : logger{make_logger_prefix(ident), conn.logger} // Throws
    , m_conn{conn}
    , m_ident{ident}
    , m_sync_transact_reporter{config.sync_transact_reporter}
    , m_disable_upload{config.disable_upload}
    , m_disable_empty_upload{config.disable_empty_upload}
    , m_is_subserver{config.is_subserver}
{
    if (get_client().m_disable_upload_activation_delay)
        m_allow_upload = true;
}

inline bool ClientImplBase::Session::do_recognize_sync_version(version_type version) noexcept
{
    if (REALM_LIKELY(version > m_last_version_available)) {
        m_last_version_available = version;
        m_upload_target_version = version;
        return true;
    }
    return false;
}

inline bool ClientImplBase::Session::have_client_file_ident() const noexcept
{
    return (m_client_file_ident.ident != 0);
}

inline bool ClientImplBase::Session::unbind_process_complete() const noexcept
{
    return (m_unbind_message_sent_2 && (m_error_message_received || m_unbound_message_received));
}

inline void ClientImplBase::Session::connection_established(bool fast_reconnect)
{
    // This function must only be called for sessions in the Active state.
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_active_or_deactivating);


    if (!fast_reconnect && !get_client().m_disable_upload_activation_delay) {
        // Disallow immediate activation of the upload process, even if download
        // completion was reached during an earlier period of connectivity.
        m_allow_upload = false;
    }

    if (!m_allow_upload) {
        // Request download completion notification
        ++m_target_download_mark;
    }

    if (!m_suspended) {
        // Ready to send BIND message
        enlist_to_send(); // Throws
    }
}

// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
inline void ClientImplBase::Session::connection_lost()
{
    REALM_ASSERT(m_active_or_deactivating);
    // If the deactivation process has been initiated, it can now be immediately
    // completed.
    if (m_deactivation_initiated) {
        // Life cycle state is Deactivating
        complete_deactivation(); // Throws
        // Life cycle state is now Deactivated
        return;
    }
    reset_protocol_state();
}

// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
inline void ClientImplBase::Session::message_sent()
{
    // Note that it is possible for this function to get called after the client
    // has received a message sent by the server in reposnse to the message that
    // the client has just finished sending.

    // Session life cycle state is Active or Deactivating
    REALM_ASSERT(m_active_or_deactivating);

    // No message will be sent after the UNBIND message
    REALM_ASSERT(!m_unbind_message_sent_2);

    if (m_unbind_message_sent) {
        REALM_ASSERT(!m_enlisted_to_send);

        // If the sending of the UNBIND message has been initiated, this must be
        // the time when the sending of that message completes.
        m_unbind_message_sent_2 = true;

        // Detect the completion of the unbinding process
        if (m_error_message_received || m_unbound_message_received) {
            // If the deactivation process has been initiated, it can now be
            // immediately completed.
            if (m_deactivation_initiated) {
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

inline void ClientImplBase::Session::initiate_rebind()
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    REALM_ASSERT(!m_suspended);
    REALM_ASSERT(!m_enlisted_to_send);

    reset_protocol_state();

    // Ready to send BIND message
    enlist_to_send(); // Throws
}

inline void ClientImplBase::Session::reset_protocol_state() noexcept
{
    // clang-format off
    m_enlisted_to_send                    = false;
    m_bind_message_sent                   = false;
    m_client_version_request_message_sent = false;
    m_state_request_message_sent = false;
    m_ident_message_sent = false;
    m_alloc_message_sent = false;
    m_unbind_message_sent = false;
    m_unbind_message_sent_2 = false;
    m_error_message_received = false;
    m_unbound_message_received = false;

    m_upload_progress = m_progress.upload;
    m_last_version_selected_for_upload = m_upload_progress.client_version;
    m_last_download_mark_sent          = m_last_download_mark_received;
    // clang-format on
}

inline void ClientImplBase::Session::ensure_enlisted_to_send()
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
inline void ClientImplBase::Session::enlist_to_send()
{
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(!m_enlisted_to_send);
    m_enlisted_to_send = true;
    m_conn.enlist_to_send(this); // Throws
}

inline bool ClientImplBase::Session::check_received_sync_progress(const SyncProgress& progress) noexcept
{
    int error_code = 0; // Dummy
    return check_received_sync_progress(progress, error_code);
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_IMPL_BASE_HPP
