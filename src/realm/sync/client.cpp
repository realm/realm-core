#include <tuple>
#include <atomic>

#include <realm/util/value_reset_guard.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/util/circular_buffer.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_impl_base.hpp>
#include <realm/version.hpp>
#include <realm/version.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/config.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::util;


// clang-format off
using ClientImplBase                  = _impl::ClientImplBase;
using SyncTransactReporter            = ClientReplication::SyncTransactReporter;
using SyncTransactCallback            = Session::SyncTransactCallback;
using ProgressHandler                 = Session::ProgressHandler;
using WaitOperCompletionHandler       = Session::WaitOperCompletionHandler;
using ConnectionState                 = Session::ConnectionState;
using ErrorInfo                       = Session::ErrorInfo;
using ConnectionStateChangeListener   = Session::ConnectionStateChangeListener;
using port_type                       = Session::port_type;
using connection_ident_type           = std::int_fast64_t;
using ProxyConfig                     = SyncConfig::ProxyConfig;
// clang-format on


namespace {

class ConnectionImpl;
class SessionImpl;
class SessionWrapper;


// (protocol, address, port, session_multiplex_ident)
//
// `protocol` is included for convenience, even though it is not strictly part
// of an endpoint.
using ServerEndpoint = std::tuple<ProtocolEnvelope, std::string, port_type, std::string>;


class SessionWrapperStack {
public:
    bool empty() const noexcept;
    void push(util::bind_ptr<SessionWrapper>) noexcept;
    util::bind_ptr<SessionWrapper> pop() noexcept;
    void clear() noexcept;
    SessionWrapperStack() noexcept = default;
    SessionWrapperStack(SessionWrapperStack&&) noexcept;
    ~SessionWrapperStack();
    friend void swap(SessionWrapperStack& q_1, SessionWrapperStack& q_2) noexcept
    {
        std::swap(q_1.m_back, q_2.m_back);
    }

private:
    SessionWrapper* m_back = nullptr;
};


class ClientImpl : public ClientImplBase {
public:
    ClientImpl(Client::Config);
    ~ClientImpl();

    void cancel_reconnect_delay();
    bool wait_for_session_terminations_or_client_stopped();

    void stop() noexcept;
    void run();

private:
    const bool m_one_connection_per_session;
    util::network::Trigger m_actualize_and_finalize;
    util::network::DeadlineTimer m_keep_running_timer;

    // Note: There is one server slot per server endpoint (hostname, port,
    // session_multiplex_ident), and it survives from one connection object to
    // the next, which is important because it carries information about a
    // possible reconnect delay applying to the new connection object (server
    // hammering protection).
    //
    // Note: Due to a particular load balancing scheme that is currently in use,
    // every session is forced to open a seperate connection (via abuse of
    // `m_one_connection_per_session`, which is only intended for testing
    // purposes). This disables part of the hammering protection scheme built in
    // to the client.
    struct ServerSlot {
        ReconnectInfo reconnect_info; // Applies exclusively to `connection`.
        std::unique_ptr<ConnectionImpl> connection;

        // Used instead of `connection` when `m_one_connection_per_session` is
        // true.
        std::map<connection_ident_type, std::unique_ptr<ConnectionImpl>> alt_connections;
    };

    // Must be accessed only by event loop thread
    std::map<ServerEndpoint, ServerSlot> m_server_slots;

    // Must be accessed only by event loop thread
    connection_ident_type m_prev_connection_ident = 0;

    util::Mutex m_mutex;

    bool m_stopped = false;                       // Protected by `m_mutex`
    bool m_sessions_terminated = false;           // Protected by `m_mutex`
    bool m_actualize_and_finalize_needed = false; // Protected by `m_mutex`

    std::atomic<bool> m_running{false}; // Debugging facility

    // The set of session wrappers that are not yet wrapping a session object,
    // and are not yet abandoned (still referenced by the application).
    //
    // Protected by `m_mutex`.
    std::map<SessionWrapper*, ServerEndpoint> m_unactualized_session_wrappers;

    // The set of session wrappers that were successfully actualized, but are
    // now abandoned (no longer referenced by the application), and have not yet
    // been finalized. Order in queue is immaterial.
    //
    // Protected by `m_mutex`.
    SessionWrapperStack m_abandoned_session_wrappers;

    // Protected by `m_mutex`.
    util::CondVar m_wait_or_client_stopped_cond;

    static ClientImplBase::Config make_client_impl_base_config(Client::Config&);

    void start_keep_running_timer();
    void register_unactualized_session_wrapper(SessionWrapper*, ServerEndpoint);
    void register_abandoned_session_wrapper(util::bind_ptr<SessionWrapper>) noexcept;
    void actualize_and_finalize_session_wrappers();

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
    //
    // FIXME: `session_multiplex_ident` should be eliminated from ServerEndpoint
    // as it effectively disables part of the hammering protection scheme if it
    // is used to ensure that each session gets a separate connection. With the
    // alternative approach outlined in the previous FIXME (specify per endpoint
    // SSL parameters at the client object level), there seems to be no more use
    // for `session_multiplex_ident`.
    ConnectionImpl& get_connection(ServerEndpoint, const std::string& authorization_header_name,
                                   const std::map<std::string, std::string>& custom_http_headers,
                                   bool verify_servers_ssl_certificate,
                                   Optional<std::string> ssl_trust_certificate_path,
                                   std::function<SyncConfig::SSLVerifyCallback>, Optional<ProxyConfig>,
                                   bool& was_created);

    // Destroys the specified connection.
    void remove_connection(ConnectionImpl&) noexcept;

    friend class ConnectionImpl;
    friend class SessionWrapper;
};


class ConnectionImpl : public ClientImplBase::Connection {
public:
    ConnectionImpl(ClientImpl&, connection_ident_type, ServerEndpoint, const std::string& authorization_header_name,
                   const std::map<std::string, std::string>& custom_http_headers, bool verify_servers_ssl_certificate,
                   Optional<std::string> ssl_trust_certificate_path, std::function<SSLVerifyCallback>,
                   Optional<ProxyConfig>, ReconnectInfo);

    ClientImpl& get_client() noexcept;
    connection_ident_type get_ident() const noexcept;
    const ServerEndpoint& get_server_endpoint() const noexcept;
    ConnectionState get_state() const noexcept;

    void update_connect_info(const std::string& http_request_path_prefix, const std::string& realm_virt_path,
                             const std::string& signed_access_token);

    void resume_active_sessions();

    // Overriding member function in ClientImplBase::Connection
    void on_connecting() override final;
    void on_connected() override final;
    void on_disconnected(std::error_code, bool, const StringData*) override final;
    void on_idle() override final;
    std::string get_http_request_path() const override final;
    void set_http_request_headers(HTTPHeaders&) override final;

private:
    const connection_ident_type m_ident;
    const ServerEndpoint m_server_endpoint;
    const std::string m_authorization_header_name;
    const std::map<std::string, std::string> m_custom_http_headers;

    std::string m_http_request_path_prefix;
    std::string m_realm_virt_path;
    std::string m_signed_access_token;

    ConnectionState m_state = ConnectionState::disconnected;

    static std::string make_logger_prefix(connection_ident_type);

    void report_connection_state_change(ConnectionState, const ErrorInfo*);
};


class SessionImpl : public ClientImplBase::Session {
public:
    SessionImpl(SessionWrapper&, ConnectionImpl&, Config);

    ClientImpl& get_client() noexcept;
    ConnectionImpl& get_connection() noexcept;

    void on_connection_state_changed(ConnectionState, const ErrorInfo*);

    // Overriding member function in ClientImplBase::Session
    const std::string& get_virt_path() const noexcept override final;
    const std::string& get_signed_access_token() const noexcept override final;
    const std::string& get_realm_path() const noexcept override final;
    DB& get_db() const noexcept override final;
    ClientHistoryBase& access_realm() override final;
    util::Optional<sync::Session::Config::ClientReset>& get_client_reset_config() noexcept override final;
    void initiate_integrate_changesets(std::uint_fast64_t, const ReceivedChangesets&) override final;
    void on_upload_completion() override final;
    void on_download_completion() override final;
    void on_suspended(std::error_code, StringData, bool) override final;
    void on_resumed() override final;

private:
    // Becomes dangling after initiation of deactivation. Note that this is not
    // a problem as callbacks are guaranteed to not occur after initiation of
    // deactivation.
    SessionWrapper& m_wrapper;
};


// Life cycle states of a session wrapper:
//
//  - Uninitiated
//  - Unactualized
//  - Actualized
//  - Finalized
//
// The session wrapper moves from the Uninitiated to the Unactualized state when
// it is initiated, i.e., when initiate() is called. This may happen on any
// thread.
//
// The session wrapper moves from the Unactualized to the Actualized state when
// it is associated with a session object, i.e., when `m_sess` is made to refer
// to an object of type SessionImpl. This always happens on the event loop
// thread.
//
// The session wrapper moves from the Actualized to the Finalized state when it
// is dissociated from the session object. This happens in response to the
// session wrapper having been abandoned by the application. This always happens
// on the event loop thread.
//
// The session wrapper will exist in the Finalized state only while referenced
// from a post handler waiting to be executed.
//
// If the session wrapper is abandoned by the application while in the
// Uninitiated state, it will be destroyed immediately, since no post handlers
// can have been scheduled prior to initiation.
//
// If the session wrapper is abandoned while in the Unactivated state, it will
// move immediately to the Finalized state. This may happen on any thread.
//
// The moving of a session wrapper to, or from the Actualized state always
// happen on the event loop thread. All other state transitions may happen on
// any thread.
//
// NOTE: Activation of the session happens no later than during actualization,
// and initiation of deactivation happens no earlier than during
// finalization. See also activate_session() and initiate_session_deactivation()
// in _impl::ClientImplBase::Connection.
class SessionWrapper : public util::AtomicRefCountBase, public SyncTransactReporter {
public:
    SessionWrapper(ClientImpl&, DBRef db, Session::Config);
    ~SessionWrapper() noexcept;

    ClientReplication& get_history() noexcept;
    ClientImpl& get_client() noexcept;

    void set_sync_transact_handler(std::function<SyncTransactCallback>);
    void set_progress_handler(std::function<ProgressHandler>);
    void set_connection_state_change_listener(std::function<ConnectionStateChangeListener>);

    void initiate();
    void initiate(ProtocolEnvelope, std::string server_address, port_type server_port, std::string virt_path,
                  std::string signed_access_token);

    void nonsync_transact_notify(version_type new_version);
    void cancel_reconnect_delay();

    void async_wait_for(bool upload_completion, bool download_completion, WaitOperCompletionHandler);
    bool wait_for_upload_complete_or_client_stopped();
    bool wait_for_download_complete_or_client_stopped();

    void refresh(std::string signed_access_token);
    void override_server(std::string address, port_type port);

    static void abandon(util::bind_ptr<SessionWrapper>) noexcept;

    // These are called from ClientImpl
    void actualize(ServerEndpoint);
    void finalize();
    void finalize_before_actualization() noexcept;

    // Overriding member function in SyncTransactReporter
    void report_sync_transact(VersionID, VersionID) override final;

private:
    ClientImpl& m_client;
    DBRef m_db;
    Replication* m_history;

    const ProtocolEnvelope m_protocol_envelope;
    const std::string m_server_address;
    const port_type m_server_port;
    const std::string m_multiplex_ident;
    const std::string m_authorization_header_name;
    const std::map<std::string, std::string> m_custom_http_headers;
    const bool m_verify_servers_ssl_certificate;
    const bool m_simulate_integration_error;
    const Optional<std::string> m_ssl_trust_certificate_path;
    const std::function<SyncConfig::SSLVerifyCallback> m_ssl_verify_callback;
    const ClientImplBase::Session::Config m_session_impl_config;

    // This one is different from null when, and only when the session wrapper
    // is in ClientImpl::m_abandoned_session_wrappers.
    SessionWrapper* m_next = nullptr;

    // After initiation, these may only be accessed by the event loop thread.
    std::string m_http_request_path_prefix;
    std::string m_virt_path;
    std::string m_signed_access_token;

    util::Optional<Session::Config::ClientReset> m_client_reset_config;

    util::Optional<ProxyConfig> m_proxy_config;

    std::function<SyncTransactCallback> m_sync_transact_handler;
    std::function<ProgressHandler> m_progress_handler;
    std::function<ConnectionStateChangeListener> m_connection_state_change_listener;

    bool m_initiated = false;

    // Set to true when this session wrapper is actualized (or when it is
    // finalized before proper actualization). It is then never modified again.
    //
    // A session specific post handler submitted after the initiation of the
    // session wrapper (initiate()) will always find that `m_actualized` is
    // true. This is the case, because the scheduling of such a post handler
    // will have been preceded by the triggering of
    // `ClientImpl::m_actualize_and_finalize` (in
    // ClientImpl::register_unactualized_session_wrapper()), which ensures that
    // ClientImpl::actualize_and_finalize_session_wrappers() gets to execute
    // before the post handler. If the session wrapper is no longer in
    // `ClientImpl::m_unactualized_session_wrappers` when
    // ClientImpl::actualize_and_finalize_session_wrappers() executes, it must
    // have been abandoned already, but in that case,
    // finalize_before_actualization() has already been called.
    bool m_actualized = false;

    bool m_suspended = false;

    // Set to true when the first DOWNLOAD message is received to indicate that
    // the byte-level download progress parameters can be considered reasonable
    // reliable. Before that, a lot of time may have passed, so our record of
    // the download progress is likely completely out of date.
    bool m_reliable_download_progress = false;

    // Set to point to an activated session object during actualization of the
    // session wrapper. Set to null during finalization of the session
    // wrapper. Both modifications are guaranteed to be performed by the event
    // loop thread.
    //
    // If a session specific post handler, that is submitted after the
    // initiation of the session wrapper, sees that `m_sess` is null, it can
    // conclude that the session wrapper has been both abandoned and
    // finalized. This is true, because the scheduling of such a post handler
    // will have been preceded by the triggering of
    // `ClientImpl::m_actualize_and_finalize` (in
    // ClientImpl::register_unactualized_session_wrapper()), which ensures that
    // ClientImpl::actualize_and_finalize_session_wrappers() gets to execute
    // before the post handler, so the session wrapper must have been actualized
    // unless it was already abandoned by the application. If it was abandoned
    // before it was actualized, it will already have been finalized by
    // finalize_before_actualization().
    //
    // Must only be accessed from the event loop thread.
    SessionImpl* m_sess = nullptr;

    // These must only be accessed from the event loop thread.
    std::vector<WaitOperCompletionHandler> m_upload_completion_handlers;
    std::vector<WaitOperCompletionHandler> m_download_completion_handlers;
    std::vector<WaitOperCompletionHandler> m_sync_completion_handlers;

    // `m_target_*load_mark` and `m_reached_*load_mark` are protected by
    // `m_client.m_mutex`. `m_staged_*load_mark` must only be accessed by the
    // event loop thread.
    std::int_fast64_t m_target_upload_mark = 0, m_target_download_mark = 0;
    std::int_fast64_t m_staged_upload_mark = 0, m_staged_download_mark = 0;
    std::int_fast64_t m_reached_upload_mark = 0, m_reached_download_mark = 0;

    static ClientImplBase::Session::Config make_session_impl_config(SyncTransactReporter&, Session::Config&);

    void do_initiate(ProtocolEnvelope, std::string server_address, port_type server_port,
                     std::string multiplex_ident);

    void on_sync_progress();
    void on_upload_completion();
    void on_download_completion();
    void on_suspended(std::error_code ec, StringData message, bool is_fatal);
    void on_resumed();
    void on_connection_state_changed(ConnectionState, const ErrorInfo*);

    void report_progress();
    void change_server_endpoint(ServerEndpoint);

    friend class SessionWrapperStack;
    friend class SessionImpl;
};


// ################ SessionWrapperStack ################

inline bool SessionWrapperStack::empty() const noexcept
{
    return !m_back;
}


inline void SessionWrapperStack::push(util::bind_ptr<SessionWrapper> w) noexcept
{
    REALM_ASSERT(!w->m_next);
    w->m_next = m_back;
    m_back = w.release();
}


inline util::bind_ptr<SessionWrapper> SessionWrapperStack::pop() noexcept
{
    util::bind_ptr<SessionWrapper> w{m_back, util::bind_ptr_base::adopt_tag{}};
    if (m_back) {
        m_back = m_back->m_next;
        w->m_next = nullptr;
    }
    return w;
}


inline void SessionWrapperStack::clear() noexcept
{
    while (m_back) {
        util::bind_ptr<SessionWrapper> w{m_back, util::bind_ptr_base::adopt_tag{}};
        m_back = w->m_next;
    }
}


inline SessionWrapperStack::SessionWrapperStack(SessionWrapperStack&& q) noexcept
    : m_back{q.m_back}
{
    q.m_back = nullptr;
}


inline SessionWrapperStack::~SessionWrapperStack()
{
    clear();
}


// ################ ClientImpl ################

inline ClientImpl::ClientImpl(Client::Config config)
    : ClientImplBase{make_client_impl_base_config(config)}                            // Throws
    , m_one_connection_per_session{config.one_connection_per_session}
    , m_keep_running_timer{get_service()} // Throws
{
    logger.debug("Realm sync client (%1)", REALM_VER_CHUNK); // Throws
    logger.debug("Supported protocol versions: %1-%2", get_oldest_supported_protocol_version(),
                 get_current_protocol_version()); // Throws
    logger.debug("Platform: %1", util::get_platform_info());
    const char* build_mode;
#if REALM_DEBUG
    build_mode = "Debug";
#else
    build_mode = "Release";
#endif
    logger.debug("Build mode: %1", build_mode);
    logger.debug("Config param: one_connection_per_session = %1",
                 config.one_connection_per_session); // Throws
    logger.debug("Config param: connect_timeout = %1 ms",
                 config.connect_timeout); // Throws
    logger.debug("Config param: connection_linger_time = %1 ms",
                 config.connection_linger_time); // Throws
    logger.debug("Config param: ping_keepalive_period = %1 ms",
                 config.ping_keepalive_period); // Throws
    logger.debug("Config param: pong_keepalive_timeout = %1 ms",
                 config.pong_keepalive_timeout); // Throws
    logger.debug("Config param: fast_reconnect_limit = %1 ms",
                 config.fast_reconnect_limit); // Throws
    logger.debug("Config param: disable_upload_compaction = %1",
                 config.disable_upload_compaction); // Throws
    logger.debug("Config param: tcp_no_delay = %1",
                 config.tcp_no_delay); // Throws
    logger.debug("Config param: disable_sync_to_disk = %1",
                 config.disable_sync_to_disk); // Throws
    logger.debug("User agent string: '%1'", get_user_agent_string());

    if (config.reconnect_mode != ReconnectMode::normal) {
        logger.warn("Testing/debugging feature 'nonnormal reconnect mode' enabled - "
                    "never do this in production!");
    }

    if (config.dry_run) {
        logger.warn("Testing/debugging feature 'dry run' enabled - "
                    "never do this in production!");
    }

    if (m_one_connection_per_session) {
        // FIXME: Re-enable this warning when the load balancer is able to handle
        // multiplexing.
        //        logger.warn("Testing/debugging feature 'one connection per session' enabled - "
        //            "never do this in production");
    }

    if (config.disable_upload_activation_delay) {
        logger.warn("Testing/debugging feature 'disable_upload_activation_delay' enabled - "
                    "never do this in production");
    }

    if (config.disable_sync_to_disk) {
        logger.warn("Testing/debugging feature 'disable_sync_to_disk' enabled - "
                    "never do this in production");
    }

    auto handler = [this] {
        actualize_and_finalize_session_wrappers(); // Throws
    };
    m_actualize_and_finalize = util::network::Trigger{get_service(), std::move(handler)}; // Throws

    start_keep_running_timer(); // Throws
}


ClientImpl::~ClientImpl()
{
    bool client_destroyed_while_still_running = m_running;
    REALM_ASSERT_RELEASE(!client_destroyed_while_still_running);

    // Since no other thread is allowed to be accessing this client or any of
    // its subobjects at this time, no mutex locking is necessary.

    // Session wrappers are removed from m_unactualized_session_wrappers as they
    // are abandoned.
    REALM_ASSERT(m_unactualized_session_wrappers.empty());
}


void ClientImpl::cancel_reconnect_delay()
{
    // Thread safety required
    auto handler = [this] {
        for (auto& p : m_server_slots) {
            ServerSlot& slot = p.second;
            if (m_one_connection_per_session) {
                REALM_ASSERT(!slot.connection);
                for (const auto& p : slot.alt_connections) {
                    ConnectionImpl& conn = *p.second;
                    conn.resume_active_sessions(); // Throws
                    conn.cancel_reconnect_delay(); // Throws
                }
            }
            else {
                REALM_ASSERT(slot.alt_connections.empty());
                if (slot.connection) {
                    ConnectionImpl& conn = *slot.connection;
                    conn.resume_active_sessions(); // Throws
                    conn.cancel_reconnect_delay(); // Throws
                }
                else {
                    slot.reconnect_info.reset();
                }
            }
        }
    };
    get_service().post(std::move(handler)); // Throws
}


bool ClientImpl::wait_for_session_terminations_or_client_stopped()
{
    // Thread safety required

    {
        util::LockGuard lock{m_mutex};
        m_sessions_terminated = false;
    }

    // The technique employed here relies on the fact that
    // actualize_and_finalize_session_wrappers() must get to execute at least
    // once before the post handler submitted below gets to execute, but still
    // at a time where all session wrappers, that are abandoned prior to the
    // execution of wait_for_session_terminations_or_client_stopped(), have been
    // added to `m_abandoned_session_wrappers`.
    //
    // To see that this is the case, consider a session wrapper that was
    // abandoned before wait_for_session_terminations_or_client_stopped() was
    // invoked. Then the session wrapper will have been added to
    // `m_abandoned_session_wrappers`, and an invocation of
    // actualize_and_finalize_session_wrappers() will have been scheduled. The
    // guarantees mentioned in the documentation of network::Trigger then ensure
    // that at least one execution of actualize_and_finalize_session_wrappers()
    // will happen after the session wrapper has been added to
    // `m_abandoned_session_wrappers`, but before the post handler submitted
    // below gets to execute.
    auto handler = [this] {
        util::LockGuard lock{m_mutex};
        m_sessions_terminated = true;
        m_wait_or_client_stopped_cond.notify_all();
    };
    get_service().post(std::move(handler)); // Throws

    bool completion_condition_was_satisfied;
    {
        util::LockGuard lock{m_mutex};
        while (!m_sessions_terminated && !m_stopped)
            m_wait_or_client_stopped_cond.wait(lock);
        completion_condition_was_satisfied = !m_stopped;
    }
    return completion_condition_was_satisfied;
}


void ClientImpl::stop() noexcept
{
    util::LockGuard lock{m_mutex};
    if (m_stopped)
        return;
    m_stopped = true;
    m_wait_or_client_stopped_cond.notify_all();
    ClientImplBase::stop();
}


void ClientImpl::run()
{
    auto ta = util::make_temp_assign(m_running, true);
    ClientImplBase::run(); // Throws
}


ClientImplBase::Config ClientImpl::make_client_impl_base_config(Client::Config& config)
{
    ClientImplBase::Config config_2;

    // clang-format off
    config_2.user_agent_platform_info        = std::move(config.user_agent_platform_info);
    config_2.user_agent_application_info     = std::move(config.user_agent_application_info);
    config_2.logger                          = config.logger;
    config_2.reconnect_mode                  = config.reconnect_mode;
    config_2.connect_timeout                 = config.connect_timeout;
    config_2.connection_linger_time          = (config.one_connection_per_session ? 0 :
                                                config.connection_linger_time);
    config_2.ping_keepalive_period           = config.ping_keepalive_period;
    config_2.pong_keepalive_timeout          = config.pong_keepalive_timeout;
    config_2.fast_reconnect_limit            = config.fast_reconnect_limit;
    config_2.disable_upload_activation_delay = config.disable_upload_activation_delay;
    config_2.dry_run                         = config.dry_run;
    config_2.tcp_no_delay                    = config.tcp_no_delay;
    config_2.enable_default_port_hack        = config.enable_default_port_hack;
    config_2.disable_upload_compaction       = config.disable_upload_compaction;
    config_2.roundtrip_time_handler          = std::move(config.roundtrip_time_handler);
    // clang-format on

    return config_2;
}


void ClientImpl::start_keep_running_timer()
{
    auto handler = [this](std::error_code ec) {
        if (ec != util::error::operation_aborted)
            start_keep_running_timer();
    };
    m_keep_running_timer.async_wait(std::chrono::hours(1000), handler); // Throws
}


void ClientImpl::register_unactualized_session_wrapper(SessionWrapper* wrapper, ServerEndpoint endpoint)
{
    // Thread safety required.

    util::LockGuard lock{m_mutex};
    m_unactualized_session_wrappers.emplace(wrapper, std::move(endpoint)); // Throws
    bool retrigger = !m_actualize_and_finalize_needed;
    m_actualize_and_finalize_needed = true;
    // The conditional triggering needs to happen before releasing the mutex,
    // because if two threads call register_unactualized_session_wrapper()
    // roughly concurrently, then only the first one is guaranteed to be asked
    // to retrigger, but that retriggering must have happened before the other
    // thread returns from register_unactualized_session_wrapper().
    //
    // Note that a similar argument applies when two threads call
    // register_abandoned_session_wrapper(), and when one thread calls one of
    // them and another thread call the other.
    if (retrigger)
        m_actualize_and_finalize.trigger();
}


void ClientImpl::register_abandoned_session_wrapper(util::bind_ptr<SessionWrapper> wrapper) noexcept
{
    // Thread safety required.

    util::LockGuard lock{m_mutex};

    // If the session wrapper has not yet been actualized (on the event loop
    // thread), it can be immediately finalized. This ensures that we will
    // generally not actualize a session wrapper that has already been
    // abandoned.
    auto i = m_unactualized_session_wrappers.find(wrapper.get());
    if (i != m_unactualized_session_wrappers.end()) {
        m_unactualized_session_wrappers.erase(i);
        wrapper->finalize_before_actualization();
        return;
    }
    m_abandoned_session_wrappers.push(std::move(wrapper));
    bool retrigger = !m_actualize_and_finalize_needed;
    m_actualize_and_finalize_needed = true;
    // The conditional triggering needs to happen before releasing the
    // mutex. See implementation of register_unactualized_session_wrapper() for
    // details.
    if (retrigger)
        m_actualize_and_finalize.trigger();
}


// Must be called from the event loop thread.
void ClientImpl::actualize_and_finalize_session_wrappers()
{
    std::map<SessionWrapper*, ServerEndpoint> unactualized_session_wrappers;
    SessionWrapperStack abandoned_session_wrappers;
    {
        util::LockGuard lock{m_mutex};
        m_actualize_and_finalize_needed = false;
        swap(m_unactualized_session_wrappers, unactualized_session_wrappers);
        swap(m_abandoned_session_wrappers, abandoned_session_wrappers);
    }
    // Note, we need to finalize old session wrappers before we actualize new
    // ones. This ensures that deactivation of old sessions is initiated before
    // new session are activated. This, in turn, ensures that the server does
    // not see two overlapping sessions for the same local Realm file.
    while (util::bind_ptr<SessionWrapper> wrapper = abandoned_session_wrappers.pop())
        wrapper->finalize(); // Throws
    for (auto& p : unactualized_session_wrappers) {
        SessionWrapper& wrapper = *p.first;
        ServerEndpoint server_endpoint = std::move(p.second);
        wrapper.actualize(std::move(server_endpoint)); // Throws
    }
}


ConnectionImpl& ClientImpl::get_connection(ServerEndpoint endpoint, const std::string& authorization_header_name,
                                           const std::map<std::string, std::string>& custom_http_headers,
                                           bool verify_servers_ssl_certificate,
                                           Optional<std::string> ssl_trust_certificate_path,
                                           std::function<SyncConfig::SSLVerifyCallback> ssl_verify_callback,
                                           Optional<ProxyConfig> proxy_config, bool& was_created)
{
    ServerSlot& server_slot = m_server_slots[endpoint]; // Throws

    // TODO: enable multiplexing with proxies
    if (server_slot.connection && !m_one_connection_per_session && !proxy_config) {
        // Use preexisting connection
        REALM_ASSERT(server_slot.alt_connections.empty());
        return *server_slot.connection;
    }

    // Create a new connection
    REALM_ASSERT(!server_slot.connection);
    connection_ident_type ident = m_prev_connection_ident + 1;
    std::unique_ptr<ConnectionImpl> conn_2 = std::make_unique<ConnectionImpl>(
        *this, ident, std::move(endpoint), authorization_header_name, custom_http_headers,
        verify_servers_ssl_certificate, std::move(ssl_trust_certificate_path), std::move(ssl_verify_callback),
        std::move(proxy_config),
        server_slot.reconnect_info); // Throws
    ConnectionImpl& conn = *conn_2;
    if (!m_one_connection_per_session) {
        server_slot.connection = std::move(conn_2);
    }
    else {
        server_slot.alt_connections[ident] = std::move(conn_2); // Throws
    }
    m_prev_connection_ident = ident;
    was_created = true;
    return conn;
}


void ClientImpl::remove_connection(ConnectionImpl& conn) noexcept
{
    const ServerEndpoint& endpoint = conn.get_server_endpoint();
    auto i = m_server_slots.find(endpoint);
    REALM_ASSERT(i != m_server_slots.end()); // Must be found
    ServerSlot& server_slot = i->second;
    if (!m_one_connection_per_session) {
        REALM_ASSERT(server_slot.alt_connections.empty());
        REALM_ASSERT(&*server_slot.connection == &conn);
        server_slot.reconnect_info = conn.get_reconnect_info();
        server_slot.connection.reset();
    }
    else {
        REALM_ASSERT(!server_slot.connection);
        connection_ident_type ident = conn.get_ident();
        auto j = server_slot.alt_connections.find(ident);
        REALM_ASSERT(j != server_slot.alt_connections.end()); // Must be found
        REALM_ASSERT(&*j->second == &conn);
        server_slot.alt_connections.erase(j);
    }
}


// ################ ConnectionImpl ################

ConnectionImpl::ConnectionImpl(ClientImpl& client, connection_ident_type ident, ServerEndpoint endpoint,
                               const std::string& authorization_header_name,
                               const std::map<std::string, std::string>& custom_http_headers,
                               bool verify_servers_ssl_certificate, Optional<std::string> ssl_trust_certificate_path,
                               std::function<SSLVerifyCallback> ssl_verify_callback,
                               Optional<ProxyConfig> proxy_config, ReconnectInfo reconnect_info)
    : ClientImplBase::Connection{client,
                                 make_logger_prefix(ident),
                                 std::get<0>(endpoint),
                                 std::get<1>(endpoint),
                                 std::get<2>(endpoint),
                                 verify_servers_ssl_certificate,
                                 ssl_trust_certificate_path,
                                 ssl_verify_callback,
                                 proxy_config,
                                 reconnect_info} // Throws
    , m_ident{ident}
    , m_server_endpoint{std::move(endpoint)}
    , m_authorization_header_name{authorization_header_name}
    , m_custom_http_headers{custom_http_headers}
{
}


inline ClientImpl& ConnectionImpl::get_client() noexcept
{
    return static_cast<ClientImpl&>(Connection::get_client());
}


inline connection_ident_type ConnectionImpl::get_ident() const noexcept
{
    return m_ident;
}


inline const ServerEndpoint& ConnectionImpl::get_server_endpoint() const noexcept
{
    return m_server_endpoint;
}


inline ConnectionState ConnectionImpl::get_state() const noexcept
{
    return m_state;
}


inline void ConnectionImpl::update_connect_info(const std::string& http_request_path_prefix,
                                                const std::string& realm_virt_path,
                                                const std::string& signed_access_token)
{
    m_http_request_path_prefix = http_request_path_prefix; // Throws (copy)
    m_realm_virt_path = realm_virt_path;                   // Throws (copy)
    m_signed_access_token = signed_access_token;           // Throws (copy)
}


void ConnectionImpl::resume_active_sessions()
{
    auto handler = [=](ClientImplBase::Session& sess) {
        sess.cancel_resumption_delay(); // Throws
    };
    for_each_active_session(std::move(handler)); // Throws
}


void ConnectionImpl::on_connecting()
{
    m_state = ConnectionState::connecting;
    report_connection_state_change(ConnectionState::connecting, nullptr); // Throws
}


void ConnectionImpl::on_connected()
{
    m_state = ConnectionState::connected;
    report_connection_state_change(ConnectionState::connected, nullptr); // Throws
}


void ConnectionImpl::on_disconnected(std::error_code ec, bool is_fatal, const StringData* custom_message)
{
    m_state = ConnectionState::disconnected;
    std::string detailed_message = (custom_message ? std::string(*custom_message) : ec.message()); // Throws
    ErrorInfo error_info{ec, is_fatal, detailed_message};
    report_connection_state_change(ConnectionState::disconnected, &error_info); // Throws
}


void ConnectionImpl::on_idle()
{
    logger.debug("Destroying connection object");
    ClientImpl& client = get_client();
    client.remove_connection(*this);
    // NOTE: This connection object is now destroyed!
}


std::string ConnectionImpl::get_http_request_path() const
{
    std::string path = m_http_request_path_prefix; // Throws (copy)
    return path;
}


void ConnectionImpl::set_http_request_headers(HTTPHeaders& headers)
{
    headers[m_authorization_header_name] = _impl::make_authorization_header(m_signed_access_token); // Throws

    for (auto const& header : m_custom_http_headers)
        headers[header.first] = header.second;
}


std::string ConnectionImpl::make_logger_prefix(connection_ident_type ident)
{
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << "Connection[" << ident << "]: "; // Throws
    return out.str();                       // Throws
}


inline void ConnectionImpl::report_connection_state_change(ConnectionState state, const ErrorInfo* error_info)
{
    auto handler = [=](ClientImplBase::Session& sess) {
        SessionImpl& sess_2 = static_cast<SessionImpl&>(sess);
        sess_2.on_connection_state_changed(state, error_info); // Throws
    };
    for_each_active_session(std::move(handler)); // Throws
}


// ################ SessionImpl ################

inline SessionImpl::SessionImpl(SessionWrapper& wrapper, ConnectionImpl& conn, Config config)
    : ClientImplBase::Session{conn, std::move(config)} // Throws
    , m_wrapper{wrapper}
{
}


inline ClientImpl& SessionImpl::get_client() noexcept
{
    return m_wrapper.m_client;
}


inline ConnectionImpl& SessionImpl::get_connection() noexcept
{
    return static_cast<ConnectionImpl&>(Session::get_connection());
}


inline void SessionImpl::on_connection_state_changed(ConnectionState state, const ErrorInfo* error_info)
{
    m_wrapper.on_connection_state_changed(state, error_info); // Throws
}


const std::string& SessionImpl::get_virt_path() const noexcept
{
    return m_wrapper.m_virt_path;
}


const std::string& SessionImpl::get_signed_access_token() const noexcept
{
    return m_wrapper.m_signed_access_token;
}

const std::string& SessionImpl::get_realm_path() const noexcept
{
    return m_wrapper.m_db->get_path();
}

DB& SessionImpl::get_db() const noexcept
{
    return *m_wrapper.m_db;
}

ClientReplicationBase& SessionImpl::access_realm()
{
    return m_wrapper.get_history();
}

util::Optional<sync::Session::Config::ClientReset>& SessionImpl::get_client_reset_config() noexcept
{
    return m_wrapper.m_client_reset_config;
}

inline void SessionImpl::initiate_integrate_changesets(std::uint_fast64_t downloadable_bytes,
                                                       const ReceivedChangesets& changesets)
{
    bool simulate_integration_error = (m_wrapper.m_simulate_integration_error && !changesets.empty());
    if (REALM_LIKELY(!simulate_integration_error)) {
        ClientImplBase::Session::initiate_integrate_changesets(downloadable_bytes,
                                                               changesets); // Throws
    }
    else {
        bool success = false;
        version_type client_version = 0;                 // Dummy
        sync::DownloadCursor download_progress = {0, 0}; // Dummy
        IntegrationError error = IntegrationError::bad_changeset;
        on_changesets_integrated(success, client_version, download_progress, error); // Throws
    }
    m_wrapper.on_sync_progress(); // Throws
}


void SessionImpl::on_upload_completion()
{
    m_wrapper.on_upload_completion(); // Throws
}


void SessionImpl::on_download_completion()
{
    m_wrapper.on_download_completion(); // Throws
}


void SessionImpl::on_suspended(std::error_code ec, StringData message, bool is_fatal)
{
    m_wrapper.on_suspended(ec, message, is_fatal); // Throws
}


void SessionImpl::on_resumed()
{
    m_wrapper.on_resumed(); // Throws
}


// ################ SessionWrapper ################

SessionWrapper::SessionWrapper(ClientImpl& client, DBRef db, Session::Config config)
    : m_client{client}
    , m_db(std::move(db))
    , m_history(m_db->get_replication())
    , m_protocol_envelope{config.protocol_envelope}
    , m_server_address{std::move(config.server_address)}
    , m_server_port{config.server_port}
    , m_multiplex_ident{std::move(config.multiplex_ident)}
    , m_authorization_header_name{config.authorization_header_name}
    , m_custom_http_headers{config.custom_http_headers}
    , m_verify_servers_ssl_certificate{config.verify_servers_ssl_certificate}
    , m_simulate_integration_error{config.simulate_integration_error}
    , m_ssl_trust_certificate_path{std::move(config.ssl_trust_certificate_path)}
    , m_ssl_verify_callback{std::move(config.ssl_verify_callback)}
    , m_session_impl_config{make_session_impl_config(*this, config)}
    , m_http_request_path_prefix{std::move(config.service_identifier)}
    , m_virt_path{std::move(config.realm_identifier)}
    , m_signed_access_token{std::move(config.signed_user_token)}
    , m_client_reset_config{std::move(config.client_reset_config)}
    , m_proxy_config{config.proxy_config} // Throws
{
    REALM_ASSERT(m_db);
    REALM_ASSERT(m_db->get_replication());
    REALM_ASSERT(dynamic_cast<ClientReplication*>(m_db->get_replication()));
}

SessionWrapper::~SessionWrapper() noexcept
{
    if (m_db && m_actualized)
        m_db->release_sync_agent();
}


inline ClientReplication& SessionWrapper::get_history() noexcept
{
    return static_cast<ClientReplication&>(*m_history);
}


inline ClientImpl& SessionWrapper::get_client() noexcept
{
    return m_client;
}


inline void SessionWrapper::set_sync_transact_handler(std::function<SyncTransactCallback> handler)
{
    REALM_ASSERT(!m_initiated);
    m_sync_transact_handler = std::move(handler); // Throws
}


inline void SessionWrapper::set_progress_handler(std::function<ProgressHandler> handler)
{
    REALM_ASSERT(!m_initiated);
    m_progress_handler = std::move(handler);
}


inline void
SessionWrapper::set_connection_state_change_listener(std::function<ConnectionStateChangeListener> listener)
{
    REALM_ASSERT(!m_initiated);
    m_connection_state_change_listener = std::move(listener);
}


inline void SessionWrapper::initiate()
{
    // FIXME: Storing connection related information in the session object seems
    // unnecessary and goes against the idea that a session should be truely
    // lightweight (when many share a single connection). The original idea was
    // that all connection related information is passed directly from the
    // caller of initiate() to the connection constructor.
    do_initiate(m_protocol_envelope, m_server_address, m_server_port, m_multiplex_ident); // Throws
}


inline void SessionWrapper::initiate(ProtocolEnvelope protocol, std::string server_address, port_type server_port,
                                     std::string virt_path, std::string signed_access_token)
{
    m_virt_path = std::move(virt_path);
    m_signed_access_token = std::move(signed_access_token);
    do_initiate(protocol, std::move(server_address), server_port, m_multiplex_ident); // Throws
}


void SessionWrapper::nonsync_transact_notify(version_type new_version)
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    auto handler = [self = std::move(self), new_version] {
        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(!self->m_sess))
            return; // Already finalized
        SessionImpl& sess = *self->m_sess;
        sess.recognize_sync_version(new_version); // Throws
        self->report_progress();                  // Throws
    };
    m_client.get_service().post(std::move(handler)); // Throws
}


void SessionWrapper::cancel_reconnect_delay()
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    auto handler = [self = std::move(self)] {
        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(!self->m_sess))
            return; // Already finalized
        SessionImpl& sess = *self->m_sess;
        sess.cancel_resumption_delay(); // Throws
        ConnectionImpl& conn = sess.get_connection();
        conn.cancel_reconnect_delay(); // Throws
    };
    m_client.get_service().post(std::move(handler)); // Throws
}


void SessionWrapper::async_wait_for(bool upload_completion, bool download_completion,
                                    WaitOperCompletionHandler handler)
{
    REALM_ASSERT(upload_completion || download_completion);
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    auto handler_2 = [self = std::move(self), handler = std::move(handler), upload_completion, download_completion] {
        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(!self->m_sess)) {
            // Already finalized
            handler(util::error::operation_aborted); // Throws
            return;
        }
        if (upload_completion) {
            if (download_completion) {
                // Wait for upload and download completion
                self->m_sync_completion_handlers.push_back(std::move(handler)); // Throws
            }
            else {
                // Wait for upload completion only
                self->m_upload_completion_handlers.push_back(std::move(handler)); // Throws
            }
        }
        else {
            // Wait for download completion only
            self->m_download_completion_handlers.push_back(std::move(handler)); // Throws
        }
        SessionImpl& sess = *self->m_sess;
        if (upload_completion)
            sess.request_upload_completion_notification(); // Throws
        if (download_completion)
            sess.request_download_completion_notification(); // Throws
    };
    m_client.get_service().post(std::move(handler_2)); // Throws
}


bool SessionWrapper::wait_for_upload_complete_or_client_stopped()
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    std::int_fast64_t target_mark;
    {
        util::LockGuard lock{m_client.m_mutex};
        target_mark = ++m_target_upload_mark;
    }

    util::bind_ptr<SessionWrapper> self{this};
    auto handler = [self = std::move(self), target_mark] {
        REALM_ASSERT(self->m_actualized);
        // The session wrapper may already have been finalized. This can only
        // happen if it was abandoned, but in that case, the call of
        // wait_for_upload_complete_or_client_stopped() must have returned
        // already.
        if (REALM_UNLIKELY(!self->m_sess))
            return;
        if (target_mark > self->m_staged_upload_mark) {
            self->m_staged_upload_mark = target_mark;
            SessionImpl& sess = *self->m_sess;
            sess.request_upload_completion_notification(); // Throws
        }
    };
    m_client.get_service().post(std::move(handler)); // Throws

    bool completion_condition_was_satisfied;
    {
        util::LockGuard lock{m_client.m_mutex};
        while (m_reached_upload_mark < target_mark && !m_client.m_stopped)
            m_client.m_wait_or_client_stopped_cond.wait(lock);
        completion_condition_was_satisfied = !m_client.m_stopped;
    }
    return completion_condition_was_satisfied;
}


bool SessionWrapper::wait_for_download_complete_or_client_stopped()
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    std::int_fast64_t target_mark;
    {
        util::LockGuard lock{m_client.m_mutex};
        target_mark = ++m_target_download_mark;
    }

    util::bind_ptr<SessionWrapper> self{this};
    auto handler = [self = std::move(self), target_mark] {
        REALM_ASSERT(self->m_actualized);
        // The session wrapper may already have been finalized. This can only
        // happen if it was abandoned, but in that case, the call of
        // wait_for_download_complete_or_client_stopped() must have returned
        // already.
        if (REALM_UNLIKELY(!self->m_sess))
            return;
        if (target_mark > self->m_staged_download_mark) {
            self->m_staged_download_mark = target_mark;
            SessionImpl& sess = *self->m_sess;
            sess.request_download_completion_notification(); // Throws
        }
    };
    m_client.get_service().post(std::move(handler)); // Throws

    bool completion_condition_was_satisfied;
    {
        util::LockGuard lock{m_client.m_mutex};
        while (m_reached_download_mark < target_mark && !m_client.m_stopped)
            m_client.m_wait_or_client_stopped_cond.wait(lock);
        completion_condition_was_satisfied = !m_client.m_stopped;
    }
    return completion_condition_was_satisfied;
}


void SessionWrapper::refresh(std::string signed_access_token)
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    auto handler = [self = std::move(self), token = std::move(signed_access_token)] {
        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(!self->m_sess))
            return; // Already finalized
        self->m_signed_access_token = std::move(token);
        SessionImpl& sess = *self->m_sess;
        ConnectionImpl& conn = sess.get_connection();
        // FIXME: This only makes sense when each session uses a separate connection.
        conn.update_connect_info(self->m_http_request_path_prefix, self->m_virt_path,
                                 self->m_signed_access_token); // Throws
        sess.new_access_token_available();                     // Throws
        sess.cancel_resumption_delay();                        // Throws
        conn.cancel_reconnect_delay();                         // Throws
    };
    m_client.get_service().post(std::move(handler)); // Throws
}


void SessionWrapper::override_server(std::string address, port_type port)
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    auto handler = [self = std::move(self), address = std::move(address), port] {
        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(!self->m_sess))
            return; // Already finalized
        SessionImpl& sess = *self->m_sess;
        ConnectionImpl& conn = sess.get_connection();
        ServerEndpoint endpoint = conn.get_server_endpoint(); // Throws (copy)
        std::get<1>(endpoint) = std::move(address);
        std::get<2>(endpoint) = port;
        self->change_server_endpoint(std::move(endpoint)); // Throws
    };
    m_client.get_service().post(std::move(handler)); // Throws
}


inline void SessionWrapper::abandon(util::bind_ptr<SessionWrapper> wrapper) noexcept
{
    if (wrapper->m_initiated) {
        ClientImpl& client = wrapper->m_client;
        client.register_abandoned_session_wrapper(std::move(wrapper));
    }
}


// Must be called from event loop thread
void SessionWrapper::actualize(ServerEndpoint endpoint)
{
    REALM_ASSERT(!m_actualized);
    REALM_ASSERT(!m_sess);
    m_db->claim_sync_agent();

    bool was_created = false;
    ConnectionImpl& conn = m_client.get_connection(
        std::move(endpoint), m_authorization_header_name, m_custom_http_headers, m_verify_servers_ssl_certificate,
        m_ssl_trust_certificate_path, m_ssl_verify_callback, m_proxy_config,
        was_created); // Throws
    try {
        // FIXME: This only makes sense when each session uses a separate connection.
        conn.update_connect_info(m_http_request_path_prefix, m_virt_path,
                                 m_signed_access_token); // Throws
        std::unique_ptr<SessionImpl> sess_2 =
            std::make_unique<SessionImpl>(*this, conn, m_session_impl_config); // Throws
        SessionImpl& sess = *sess_2;
        sess.logger.detail("Binding '%1' to '%2'", m_db->get_path(), m_virt_path);       // Throws
        conn.activate_session(std::move(sess_2));                                        // Throws

        m_actualized = true;
        m_sess = &sess;
    }
    catch (...) {
        if (was_created)
            m_client.remove_connection(conn);
        throw;
    }

    if (was_created)
        conn.activate(); // Throws

    if (m_connection_state_change_listener) {
        ConnectionState state = conn.get_state();
        if (state != ConnectionState::disconnected) {
            m_connection_state_change_listener(ConnectionState::connecting, nullptr); // Throws
            if (state == ConnectionState::connected)
                m_connection_state_change_listener(ConnectionState::connected, nullptr); // Throws
        }
    }

    if (!m_client_reset_config)
        report_progress(); // Throws
}


// Must be called from event loop thread
void SessionWrapper::finalize()
{
    REALM_ASSERT(m_actualized);
    REALM_ASSERT(m_sess);

    ConnectionImpl& conn = m_sess->get_connection();
    conn.initiate_session_deactivation(m_sess); // Throws

    m_sess = nullptr;

    // The Realm file can be closed now, as no access to the Realm file is
    // supposed to happen on behalf of a session after initiation of
    // deactivation.
    m_db->release_sync_agent();
    m_db = nullptr;

    // All outstanding wait operations must be canceled
    while (!m_upload_completion_handlers.empty()) {
        auto handler = std::move(m_upload_completion_handlers.back());
        m_upload_completion_handlers.pop_back();
        std::error_code ec = error::operation_aborted;
        handler(ec); // Throws
    }
    while (!m_download_completion_handlers.empty()) {
        auto handler = std::move(m_download_completion_handlers.back());
        m_download_completion_handlers.pop_back();
        std::error_code ec = error::operation_aborted;
        handler(ec); // Throws
    }
    while (!m_sync_completion_handlers.empty()) {
        auto handler = std::move(m_sync_completion_handlers.back());
        m_sync_completion_handlers.pop_back();
        std::error_code ec = error::operation_aborted;
        handler(ec); // Throws
    }
}


// Must be called only when an unactualized session wrapper becomes abandoned.
//
// Called with a lock on `m_client.m_mutex`.
inline void SessionWrapper::finalize_before_actualization() noexcept
{
    m_actualized = true;
}


inline void SessionWrapper::report_sync_transact(VersionID old_version, VersionID new_version)
{
    if (m_sync_transact_handler)
        m_sync_transact_handler(old_version, new_version); // Throws
}

auto SessionWrapper::make_session_impl_config(SyncTransactReporter& transact_reporter, Session::Config& config)
    -> ClientImplBase::Session::Config
{
    ClientImplBase::Session::Config config_2;
    config_2.sync_transact_reporter = &transact_reporter;
    config_2.disable_upload = config.disable_upload;
    config_2.disable_empty_upload = config.disable_empty_upload;
    return config_2;
}


void SessionWrapper::do_initiate(ProtocolEnvelope protocol, std::string server_address, port_type server_port,
                                 std::string multiplex_ident)
{
    REALM_ASSERT(!m_initiated);
    ServerEndpoint server_endpoint{protocol, std::move(server_address), server_port, std::move(multiplex_ident)};
    m_client.register_unactualized_session_wrapper(this, std::move(server_endpoint)); // Throws
    m_initiated = true;
}

inline void SessionWrapper::on_sync_progress()
{
    m_reliable_download_progress = true;
    report_progress(); // Throws
}


void SessionWrapper::on_upload_completion()
{
    while (!m_upload_completion_handlers.empty()) {
        auto handler = std::move(m_upload_completion_handlers.back());
        m_upload_completion_handlers.pop_back();
        std::error_code ec; // Success
        handler(ec);        // Throws
    }
    while (!m_sync_completion_handlers.empty()) {
        auto handler = std::move(m_sync_completion_handlers.back());
        m_download_completion_handlers.push_back(std::move(handler)); // Throws
        m_sync_completion_handlers.pop_back();
    }
    util::LockGuard lock{m_client.m_mutex};
    if (m_staged_upload_mark > m_reached_upload_mark) {
        m_reached_upload_mark = m_staged_upload_mark;
        m_client.m_wait_or_client_stopped_cond.notify_all();
    }
}


void SessionWrapper::on_download_completion()
{
    while (!m_download_completion_handlers.empty()) {
        auto handler = std::move(m_download_completion_handlers.back());
        m_download_completion_handlers.pop_back();
        std::error_code ec; // Success
        handler(ec);        // Throws
    }
    while (!m_sync_completion_handlers.empty()) {
        auto handler = std::move(m_sync_completion_handlers.back());
        m_upload_completion_handlers.push_back(std::move(handler)); // Throws
        m_sync_completion_handlers.pop_back();
    }
    util::LockGuard lock{m_client.m_mutex};
    if (m_staged_download_mark > m_reached_download_mark) {
        m_reached_download_mark = m_staged_download_mark;
        m_client.m_wait_or_client_stopped_cond.notify_all();
    }
}


void SessionWrapper::on_suspended(std::error_code ec, StringData message, bool is_fatal)
{
    m_suspended = true;
    if (m_connection_state_change_listener) {
        ConnectionImpl& conn = m_sess->get_connection();
        if (conn.get_state() != ConnectionState::disconnected) {
            std::string message_2{message}; // Throws (copy)
            ConnectionState state = ConnectionState::disconnected;
            ErrorInfo error_info{ec, is_fatal, message_2};
            m_connection_state_change_listener(state, &error_info); // Throws
        }
    }
}


void SessionWrapper::on_resumed()
{
    m_suspended = false;
    if (m_connection_state_change_listener) {
        ConnectionImpl& conn = m_sess->get_connection();
        if (conn.get_state() != ConnectionState::disconnected) {
            m_connection_state_change_listener(ConnectionState::connecting, nullptr); // Throws
            if (conn.get_state() == ConnectionState::connected)
                m_connection_state_change_listener(ConnectionState::connected, nullptr); // Throws
        }
    }
}


void SessionWrapper::on_connection_state_changed(ConnectionState state, const ErrorInfo* error_info)
{
    if (m_connection_state_change_listener) {
        if (!m_suspended)
            m_connection_state_change_listener(state, error_info); // Throws
    }
}


void SessionWrapper::report_progress()
{
    REALM_ASSERT(m_sess);

    if (!m_progress_handler)
        return;

    std::uint_fast64_t downloaded_bytes = 0;
    std::uint_fast64_t downloadable_bytes = 0;
    std::uint_fast64_t uploaded_bytes = 0;
    std::uint_fast64_t uploadable_bytes = 0;
    std::uint_fast64_t snapshot_version = 0;
    _impl::ClientHistoryImpl::get_upload_download_bytes(m_db.get(), downloaded_bytes, downloadable_bytes,
                                                        uploaded_bytes, uploadable_bytes, snapshot_version);

    // In protocol versions 25 and earlier, downloadable_bytes was the total
    // size of the history. From protocol version 26, downloadable_bytes
    // represent the non-downloaded bytes on the server. Since the user supplied
    // progress handler interprets downloadable_bytes as the total size of
    // downloadable bytes, this number must be calculated.  We could change the
    // meaning of downloadable_bytes for the progress handler, but that would be
    // a breaking change. Note that protocol version 25 (and earlier) is no
    // longer supported by clients.
    std::uint_fast64_t total_bytes = downloaded_bytes + downloadable_bytes;

    m_sess->logger.debug("Progress handler called, downloaded = %1, "
                         "downloadable(total) = %2, uploaded = %3, "
                         "uploadable = %4, reliable_download_progress = %5, "
                         "snapshot version = %6",
                         downloaded_bytes, total_bytes, uploaded_bytes, uploadable_bytes,
                         m_reliable_download_progress, snapshot_version);

    // FIXME: Why is this boolean status communicated to the application as
    // a 64-bit integer? Also, the name `progress_version` is confusing.
    std::uint_fast64_t progress_version = (m_reliable_download_progress ? 1 : 0);
    m_progress_handler(downloaded_bytes, total_bytes, uploaded_bytes, uploadable_bytes, progress_version,
                       snapshot_version);
}


void SessionWrapper::change_server_endpoint(ServerEndpoint endpoint)
{
    REALM_ASSERT(m_actualized);
    REALM_ASSERT(m_sess);

    SessionImpl& old_sess = *m_sess;
    ConnectionImpl& old_conn = old_sess.get_connection();

    bool was_created = false;
    ConnectionImpl& new_conn = m_client.get_connection(
        std::move(endpoint), m_authorization_header_name, m_custom_http_headers, m_verify_servers_ssl_certificate,
        m_ssl_trust_certificate_path, m_ssl_verify_callback, m_proxy_config,
        was_created); // Throws
    try {
        if (&new_conn == &old_conn) {
            REALM_ASSERT(!was_created);
            return;
        }

        if (m_connection_state_change_listener) {
            ConnectionState state = old_conn.get_state();
            if (state != ConnectionState::disconnected) {
                std::error_code ec = Client::Error::connection_closed;
                bool is_fatal = false;
                std::string detailed_message = ec.message(); // Throws (copy)
                ErrorInfo error_info{ec, is_fatal, detailed_message};
                m_connection_state_change_listener(ConnectionState::disconnected,
                                                   &error_info); // Throws
            }
        }

        // FIXME: This only makes sense when each session uses a separate connection.
        new_conn.update_connect_info(m_http_request_path_prefix, m_virt_path,
                                     m_signed_access_token); // Throws
        std::unique_ptr<SessionImpl> new_sess_2 =
            std::make_unique<SessionImpl>(*this, new_conn, m_session_impl_config); // Throws
        SessionImpl& new_sess = *new_sess_2;
        new_sess.logger.detail("Rebinding '%1' to '%2'", m_db->get_path(),
                               m_virt_path);              // Throws
        new_conn.activate_session(std::move(new_sess_2)); // Throws

        m_sess = &new_sess;
    }
    catch (...) {
        if (was_created)
            m_client.remove_connection(new_conn);
        throw;
    }

    old_conn.initiate_session_deactivation(&old_sess); // Throws

    if (was_created)
        new_conn.activate(); // Throws

    if (m_connection_state_change_listener) {
        ConnectionState state = new_conn.get_state();
        if (state != ConnectionState::disconnected) {
            m_connection_state_change_listener(ConnectionState::connecting, nullptr); // Throws
            if (state == ConnectionState::connected)
                m_connection_state_change_listener(ConnectionState::connected, nullptr); // Throws
        }
    }
}


// ################ miscellaneous ################

const char* get_error_message(Client::Error error_code)
{
    switch (error_code) {
        case Client::Error::connection_closed:
            return "Connection closed (no error)";
        case Client::Error::unknown_message:
            return "Unknown type of input message";
        case Client::Error::bad_syntax:
            return "Bad syntax in input message head";
        case Client::Error::limits_exceeded:
            return "Limits exceeded in input message";
        case Client::Error::bad_session_ident:
            return "Bad session identifier in input message";
        case Client::Error::bad_message_order:
            return "Bad input message order";
        case Client::Error::bad_client_file_ident:
            return "Bad client file identifier (IDENT)";
        case Client::Error::bad_progress:
            return "Bad progress information (DOWNLOAD)";
        case Client::Error::bad_changeset_header_syntax:
            return "Bad progress information (DOWNLOAD)";
        case Client::Error::bad_changeset_size:
            return "Bad changeset size in changeset header (DOWNLOAD)";
        case Client::Error::bad_origin_file_ident:
            return "Bad origin file identifier in changeset header (DOWNLOAD)";
        case Client::Error::bad_server_version:
            return "Bad server version in changeset header (DOWNLOAD)";
        case Client::Error::bad_changeset:
            return "Bad changeset (DOWNLOAD)";
        case Client::Error::bad_request_ident:
            return "Bad request identifier (MARK)";
        case Client::Error::bad_error_code:
            return "Bad error code (ERROR)";
        case Client::Error::bad_compression:
            return "Bad compression (DOWNLOAD)";
        case Client::Error::bad_client_version:
            return "Bad last integrated client version in changeset header (DOWNLOAD)";
        case Client::Error::ssl_server_cert_rejected:
            return "SSL server certificate rejected";
        case Client::Error::pong_timeout:
            return "Timeout on reception of PONG respone message";
        case Client::Error::bad_client_file_ident_salt:
            return "Bad client file identifier salt (IDENT)";
        case Client::Error::bad_file_ident:
            return "Bad file identifier (ALLOC)";
        case Client::Error::connect_timeout:
            return "Sync connection was not fully established in time";
        case Client::Error::bad_timestamp:
            return "Bad timestamp (PONG)";
        case Client::Error::bad_protocol_from_server:
            return "Bad or missing protocol version information from server";
        case Client::Error::client_too_old_for_server:
            return "Protocol version negotiation failed: Client is too old for server";
        case Client::Error::client_too_new_for_server:
            return "Protocol version negotiation failed: Client is too new for server";
        case Client::Error::protocol_mismatch:
            return ("Protocol version negotiation failed: No version supported by both "
                    "client and server");
        case Client::Error::bad_state_message:
            return "Bad state message (STATE)";
        case Client::Error::missing_protocol_feature:
            return "Requested feature missing in negotiated protocol version";
        case Client::Error::http_tunnel_failed:
            return "Failure to establish HTTP tunnel with configured proxy";
    }
    return nullptr;
}


class ErrorCategoryImpl : public std::error_category {
public:
    const char* name() const noexcept override final
    {
        return "realm::sync::Client::Error";
    }
    std::string message(int error_code) const override final
    {
        const char* msg = get_error_message(Client::Error(error_code));
        if (!msg)
            msg = "Unknown error";
        std::string msg_2{msg}; // Throws (copy)
        return msg_2;
    }
};

ErrorCategoryImpl g_error_category;

} // unnamed namespace


class Client::Impl : public ClientImpl {
public:
    Impl(Client::Config config)
        : ClientImpl{std::move(config)} // Throws
    {
    }
};


class Session::Impl : public SessionWrapper {
public:
    Impl(ClientImpl& client, DBRef db, Config config)
        : SessionWrapper{client, std::move(db), std::move(config)} // Throws
    {
    }

    static Impl* make_session(ClientImpl& client, DBRef db, Config config)
    {
        util::bind_ptr<Impl> sess;
        sess.reset(new Impl{client, std::move(db), std::move(config)}); // Throws
        // The reference count passed back to the application is implicitly
        // owned by a naked pointer. This is done to avoid exposing
        // implementation details through the header file (that is, through the
        // Session object).
        return sess.release();
    }
};


Client::Client(Config config)
    : m_impl{new Impl{std::move(config)}} // Throws
{
}


Client::Client(Client&& client) noexcept
    : m_impl{std::move(client.m_impl)}
{
}


Client::~Client() noexcept {}


void Client::run()
{
    m_impl->run(); // Throws
}


void Client::stop() noexcept
{
    m_impl->stop();
}


void Client::cancel_reconnect_delay()
{
    m_impl->cancel_reconnect_delay();
}


bool Client::wait_for_session_terminations_or_client_stopped()
{
    return m_impl->wait_for_session_terminations_or_client_stopped();
}


bool Client::decompose_server_url(const std::string& url, ProtocolEnvelope& protocol, std::string& address,
                                  port_type& port, std::string& path) const
{
    return m_impl->decompose_server_url(url, protocol, address, port, path); // Throws
}


Session::Session(Client& client, DBRef db, Config config)
    : m_impl{Impl::make_session(*client.m_impl, std::move(db), std::move(config))} // Throws
{
}


void Session::set_sync_transact_callback(std::function<SyncTransactCallback> handler)
{
    m_impl->set_sync_transact_handler(std::move(handler)); // Throws
}


void Session::set_progress_handler(std::function<ProgressHandler> handler)
{
    m_impl->set_progress_handler(std::move(handler)); // Throws
}


void Session::set_connection_state_change_listener(std::function<ConnectionStateChangeListener> listener)
{
    m_impl->set_connection_state_change_listener(std::move(listener)); // Throws
}


void Session::bind()
{
    m_impl->initiate(); // Throws
}


void Session::bind(std::string server_url, std::string signed_access_token)
{
    ClientImpl& client = m_impl->get_client();
    ProtocolEnvelope protocol = {};
    std::string address;
    port_type port = {};
    std::string path;
    if (!client.decompose_server_url(server_url, protocol, address, port, path)) // Throws
        throw BadServerUrl();
    bind(std::move(address), std::move(path), std::move(signed_access_token), port, protocol); // Throws
}


void Session::bind(std::string server_address, std::string realm_identifier, std::string signed_access_token,
                   port_type server_port, ProtocolEnvelope protocol)
{
    m_impl->initiate(protocol, std::move(server_address), server_port, std::move(realm_identifier),
                     std::move(signed_access_token)); // Throws
}


void Session::nonsync_transact_notify(version_type new_version)
{
    m_impl->nonsync_transact_notify(new_version); // Throws
}


void Session::cancel_reconnect_delay()
{
    m_impl->cancel_reconnect_delay(); // Throws
}


void Session::async_wait_for(bool upload_completion, bool download_completion, WaitOperCompletionHandler handler)
{
    m_impl->async_wait_for(upload_completion, download_completion, std::move(handler)); // Throws
}


bool Session::wait_for_upload_complete_or_client_stopped()
{
    return m_impl->wait_for_upload_complete_or_client_stopped(); // Throws
}


bool Session::wait_for_download_complete_or_client_stopped()
{
    return m_impl->wait_for_download_complete_or_client_stopped(); // Throws
}


void Session::refresh(std::string signed_access_token)
{
    m_impl->refresh(signed_access_token); // Throws
}


void Session::override_server(std::string address, port_type port)
{
    m_impl->override_server(address, port); // Throws
}


void Session::abandon() noexcept
{
    REALM_ASSERT(m_impl);
    // Reabsorb the ownership assigned to the applications naked pointer by
    // Session::Impl::make_session().
    util::bind_ptr<SessionWrapper> wrapper{m_impl, util::bind_ptr_base::adopt_tag{}};
    SessionWrapper::abandon(std::move(wrapper));
}


const std::error_category& sync::client_error_category() noexcept
{
    return g_error_category;
}


std::error_code sync::make_error_code(Client::Error error_code) noexcept
{
    return std::error_code{int(error_code), g_error_category};
}

namespace realm {
namespace sync {

std::ostream& operator<<(std::ostream& os, ProxyConfig::Type proxyType)
{
    switch (proxyType) {
        case ProxyConfig::Type::HTTP:
            return os << "HTTP";
        case ProxyConfig::Type::HTTPS:
            return os << "HTTPS";
    }
    REALM_TERMINATE("Invalid Proxy Type object.");
}

} // namespace sync
} // namespace realm
