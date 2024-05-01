
#include <memory>
#include <tuple>
#include <atomic>

#include "realm/sync/client_base.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/util/optional.hpp"
#include <realm/sync/client.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_impl_base.hpp>
#include <realm/sync/noinst/pending_bootstrap_store.hpp>
#include <realm/sync/subscriptions.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/util/circular_buffer.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/value_reset_guard.hpp>
#include <realm/version.hpp>

namespace realm {
namespace sync {

namespace {
using namespace realm::util;


// clang-format off
using SessionImpl                     = ClientImpl::Session;
using SyncTransactCallback            = Session::SyncTransactCallback;
using ProgressHandler                 = Session::ProgressHandler;
using WaitOperCompletionHandler       = Session::WaitOperCompletionHandler;
using ConnectionStateChangeListener   = Session::ConnectionStateChangeListener;
using port_type                       = Session::port_type;
using connection_ident_type           = std::int_fast64_t;
using ProxyConfig                     = SyncConfig::ProxyConfig;
// clang-format on

} // unnamed namespace


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
// in ClientImpl::Connection.
class SessionWrapper final : public util::AtomicRefCountBase, DB::CommitListener {
public:
    SessionWrapper(ClientImpl&, DBRef db, std::shared_ptr<SubscriptionStore>, std::shared_ptr<MigrationStore>,
                   Session::Config);
    ~SessionWrapper() noexcept;

    ClientReplication& get_replication() noexcept;
    ClientImpl& get_client() noexcept;

    bool has_flx_subscription_store() const;
    SubscriptionStore* get_flx_subscription_store();
    PendingBootstrapStore* get_flx_pending_bootstrap_store();

    MigrationStore* get_migration_store();

    void set_progress_handler(util::UniqueFunction<ProgressHandler>);
    void set_connection_state_change_listener(util::UniqueFunction<ConnectionStateChangeListener>);

    void initiate();

    void force_close();

    void on_commit(version_type new_version) override;
    void cancel_reconnect_delay();

    void async_wait_for(bool upload_completion, bool download_completion, WaitOperCompletionHandler);
    bool wait_for_upload_complete_or_client_stopped();
    bool wait_for_download_complete_or_client_stopped();

    void refresh(std::string_view signed_access_token);

    static void abandon(util::bind_ptr<SessionWrapper>) noexcept;

    // These are called from ClientImpl
    void actualize(ServerEndpoint);
    void finalize();
    void finalize_before_actualization() noexcept;

    util::Future<std::string> send_test_command(std::string body);

    void handle_pending_client_reset_acknowledgement();

    void update_subscription_version_info();

    std::string get_appservices_connection_id();

protected:
    friend class ClientImpl;

    // m_initiated/m_abandoned is used to check that we aren't trying to update immutable properties like the progress
    // handler or connection state listener after we've bound the session. We read the variable a bunch in
    // REALM_ASSERTS on the event loop and on the user's thread, but we only set it once and while we're registering
    // the session wrapper to be actualized. This function gets called from
    // ClientImpl::register_unactualized_session_wrapper() to synchronize updating this variable on the main thread
    // with reading the variable on the event loop.
    void mark_initiated();
    void mark_abandoned();

private:
    ClientImpl& m_client;
    DBRef m_db;
    Replication* m_replication;

    const ProtocolEnvelope m_protocol_envelope;
    const std::string m_server_address;
    const port_type m_server_port;
    const bool m_server_verified;
    const std::string m_user_id;
    const SyncServerMode m_sync_mode;
    const std::string m_authorization_header_name;
    const std::map<std::string, std::string> m_custom_http_headers;
    const bool m_verify_servers_ssl_certificate;
    const bool m_simulate_integration_error;
    const Optional<std::string> m_ssl_trust_certificate_path;
    const std::function<SyncConfig::SSLVerifyCallback> m_ssl_verify_callback;
    const size_t m_flx_bootstrap_batch_size_bytes;

    // This one is different from null when, and only when the session wrapper
    // is in ClientImpl::m_abandoned_session_wrappers.
    SessionWrapper* m_next = nullptr;

    // After initiation, these may only be accessed by the event loop thread.
    std::string m_http_request_path_prefix;
    std::string m_virt_path;
    std::string m_signed_access_token;

    util::Optional<ClientReset> m_client_reset_config;

    util::Optional<ProxyConfig> m_proxy_config;

    struct ReportedProgress {
        uint64_t snapshot = 0;
        uint64_t uploaded = 0;
        uint64_t uploadable = 0;
        uint64_t downloaded = 0;
        uint64_t downloadable = 0;
        uint64_t final_uploaded = 0;
        uint64_t final_downloaded = 0;
    } m_reported_progress;

    util::UniqueFunction<ProgressHandler> m_progress_handler;
    util::UniqueFunction<ConnectionStateChangeListener> m_connection_state_change_listener;

    std::function<SyncClientHookAction(SyncClientHookData data)> m_debug_hook;
    bool m_in_debug_hook = false;

    SessionReason m_session_reason;

    const uint64_t m_schema_version;

    std::shared_ptr<SubscriptionStore> m_flx_subscription_store;
    int64_t m_flx_active_version = 0;
    int64_t m_flx_last_seen_version = 0;
    int64_t m_flx_pending_mark_version = 0;
    std::unique_ptr<PendingBootstrapStore> m_flx_pending_bootstrap_store;

    std::shared_ptr<MigrationStore> m_migration_store;

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

    bool m_force_closed = false;

    bool m_suspended = false;

    // Set when the session has been abandoned, but before it's been finalized.
    bool m_abandoned = false;
    // Has the SessionWrapper been finalized?
    bool m_finalized = false;

    // Set to true when the first DOWNLOAD message is received to indicate that
    // the byte-level download progress parameters can be considered reasonable
    // reliable. Before that, a lot of time may have passed, so our record of
    // the download progress is likely completely out of date.
    bool m_reliable_download_progress = false;

    std::optional<double> m_download_estimate;
    std::optional<uint64_t> m_bootstrap_store_bytes;

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

    void on_upload_progress(bool only_if_new_uploadable_data = false);
    void on_download_progress(const std::optional<uint64_t>& bootstrap_store_bytes = {});
    void on_upload_completion();
    void on_download_completion();
    void on_suspended(const SessionErrorInfo& error_info);
    void on_resumed();
    void on_connection_state_changed(ConnectionState, const util::Optional<SessionErrorInfo>&);
    void on_flx_sync_progress(int64_t new_version, DownloadBatchState batch_state);
    void on_flx_sync_error(int64_t version, std::string_view err_msg);
    void on_flx_sync_version_complete(int64_t version);

    void init_progress_handler();
    // only_if_new_uploadable_data can be true only if is_download is false
    void report_progress(bool is_download, bool only_if_new_uploadable_data = false);

    friend class SessionWrapperStack;
    friend class ClientImpl::Session;
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


SessionWrapperStack::~SessionWrapperStack()
{
    clear();
}


// ################ ClientImpl ################

ClientImpl::~ClientImpl()
{
    // Since no other thread is allowed to be accessing this client or any of
    // its subobjects at this time, no mutex locking is necessary.

    shutdown_and_wait();
    // Session wrappers are removed from m_unactualized_session_wrappers as they
    // are abandoned.
    REALM_ASSERT(m_stopped);
    REALM_ASSERT(m_unactualized_session_wrappers.empty());
}


void ClientImpl::cancel_reconnect_delay()
{
    // Thread safety required
    post([this](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        for (auto& p : m_server_slots) {
            ServerSlot& slot = p.second;
            if (m_one_connection_per_session) {
                REALM_ASSERT(!slot.connection);
                for (const auto& p : slot.alt_connections) {
                    ClientImpl::Connection& conn = *p.second;
                    conn.resume_active_sessions(); // Throws
                    conn.cancel_reconnect_delay(); // Throws
                }
            }
            else {
                REALM_ASSERT(slot.alt_connections.empty());
                if (slot.connection) {
                    ClientImpl::Connection& conn = *slot.connection;
                    conn.resume_active_sessions(); // Throws
                    conn.cancel_reconnect_delay(); // Throws
                }
                else {
                    slot.reconnect_info.reset();
                }
            }
        }
    }); // Throws
}


void ClientImpl::voluntary_disconnect_all_connections()
{
    auto done_pf = util::make_promise_future<void>();
    post([this, promise = std::move(done_pf.promise)](Status status) mutable {
        if (status == ErrorCodes::OperationAborted) {
            return;
        }

        REALM_ASSERT(status.is_ok());

        try {
            for (auto& p : m_server_slots) {
                ServerSlot& slot = p.second;
                if (m_one_connection_per_session) {
                    REALM_ASSERT(!slot.connection);
                    for (const auto& p : slot.alt_connections) {
                        ClientImpl::Connection& conn = *p.second;
                        if (conn.get_state() == ConnectionState::disconnected) {
                            continue;
                        }
                        conn.voluntary_disconnect();
                    }
                }
                else {
                    REALM_ASSERT(slot.alt_connections.empty());
                    if (!slot.connection) {
                        continue;
                    }
                    ClientImpl::Connection& conn = *slot.connection;
                    if (conn.get_state() == ConnectionState::disconnected) {
                        continue;
                    }
                    conn.voluntary_disconnect();
                }
            }
        }
        catch (...) {
            promise.set_error(exception_to_status());
            return;
        }
        promise.emplace_value();
    });
    done_pf.future.get();
}


bool ClientImpl::wait_for_session_terminations_or_client_stopped()
{
    // Thread safety required

    {
        std::lock_guard lock{m_mutex};
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
    // guarantees mentioned in the documentation of Trigger then ensure
    // that at least one execution of actualize_and_finalize_session_wrappers()
    // will happen after the session wrapper has been added to
    // `m_abandoned_session_wrappers`, but before the post handler submitted
    // below gets to execute.
    post([this](Status status) mutable {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        std::lock_guard lock{m_mutex};
        m_sessions_terminated = true;
        m_wait_or_client_stopped_cond.notify_all();
    }); // Throws

    bool completion_condition_was_satisfied;
    {
        std::unique_lock lock{m_mutex};
        while (!m_sessions_terminated && !m_stopped)
            m_wait_or_client_stopped_cond.wait(lock);
        completion_condition_was_satisfied = !m_stopped;
    }
    return completion_condition_was_satisfied;
}


// This relies on the same assumptions and guarantees as wait_for_session_terminations_or_client_stopped().
util::Future<void> ClientImpl::notify_session_terminated()
{
    auto pf = util::make_promise_future<void>();
    post([promise = std::move(pf.promise)](Status status) mutable {
        // Includes operation_aborted
        if (!status.is_ok()) {
            promise.set_error(status);
            return;
        }

        promise.emplace_value();
    });

    return std::move(pf.future);
}

void ClientImpl::drain_connections_on_loop()
{
    post([this](Status status) mutable {
        REALM_ASSERT(status.is_ok());
        drain_connections();
    });
}

void ClientImpl::shutdown_and_wait()
{
    shutdown();
    std::unique_lock lock{m_drain_mutex};
    if (m_drained) {
        return;
    }

    logger.debug("Waiting for %1 connections to drain", m_num_connections);
    m_drain_cv.wait(lock, [&] {
        return m_num_connections == 0 && m_outstanding_posts == 0;
    });

    m_drained = true;
}

void ClientImpl::shutdown() noexcept
{
    {
        std::lock_guard lock{m_mutex};
        if (m_stopped)
            return;
        m_stopped = true;
        m_wait_or_client_stopped_cond.notify_all();
    }

    drain_connections_on_loop();
}


void ClientImpl::register_unactualized_session_wrapper(SessionWrapper* wrapper, ServerEndpoint endpoint)
{
    // Thread safety required.
    {
        std::lock_guard lock{m_mutex};
        REALM_ASSERT(m_actualize_and_finalize);
        wrapper->mark_initiated();
        m_unactualized_session_wrappers.emplace(wrapper, std::move(endpoint)); // Throws
    }
    m_actualize_and_finalize->trigger();
}


void ClientImpl::register_abandoned_session_wrapper(util::bind_ptr<SessionWrapper> wrapper) noexcept
{
    // Thread safety required.
    {
        std::lock_guard lock{m_mutex};
        REALM_ASSERT(m_actualize_and_finalize);
        wrapper->mark_abandoned();

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
    }
    m_actualize_and_finalize->trigger();
}


// Must be called from the event loop thread.
void ClientImpl::actualize_and_finalize_session_wrappers()
{
    std::map<SessionWrapper*, ServerEndpoint> unactualized_session_wrappers;
    SessionWrapperStack abandoned_session_wrappers;
    bool stopped;
    {
        std::lock_guard lock{m_mutex};
        swap(m_unactualized_session_wrappers, unactualized_session_wrappers);
        swap(m_abandoned_session_wrappers, abandoned_session_wrappers);
        stopped = m_stopped;
    }
    // Note, we need to finalize old session wrappers before we actualize new
    // ones. This ensures that deactivation of old sessions is initiated before
    // new session are activated. This, in turn, ensures that the server does
    // not see two overlapping sessions for the same local Realm file.
    while (util::bind_ptr<SessionWrapper> wrapper = abandoned_session_wrappers.pop())
        wrapper->finalize(); // Throws
    if (stopped) {
        for (auto& p : unactualized_session_wrappers) {
            SessionWrapper& wrapper = *p.first;
            wrapper.finalize_before_actualization();
        }
        return;
    }
    for (auto& p : unactualized_session_wrappers) {
        SessionWrapper& wrapper = *p.first;
        ServerEndpoint server_endpoint = std::move(p.second);
        wrapper.actualize(std::move(server_endpoint)); // Throws
    }
}


ClientImpl::Connection& ClientImpl::get_connection(ServerEndpoint endpoint,
                                                   const std::string& authorization_header_name,
                                                   const std::map<std::string, std::string>& custom_http_headers,
                                                   bool verify_servers_ssl_certificate,
                                                   Optional<std::string> ssl_trust_certificate_path,
                                                   std::function<SyncConfig::SSLVerifyCallback> ssl_verify_callback,
                                                   Optional<ProxyConfig> proxy_config, bool& was_created)
{
    auto&& [server_slot_it, inserted] =
        m_server_slots.try_emplace(endpoint, ReconnectInfo(m_reconnect_mode, m_reconnect_backoff_info, get_random()));
    ServerSlot& server_slot = server_slot_it->second; // Throws

    // TODO: enable multiplexing with proxies
    if (server_slot.connection && !m_one_connection_per_session && !proxy_config) {
        // Use preexisting connection
        REALM_ASSERT(server_slot.alt_connections.empty());
        return *server_slot.connection;
    }

    // Create a new connection
    REALM_ASSERT(!server_slot.connection);
    connection_ident_type ident = m_prev_connection_ident + 1;
    std::unique_ptr<ClientImpl::Connection> conn_2 = std::make_unique<ClientImpl::Connection>(
        *this, ident, std::move(endpoint), authorization_header_name, custom_http_headers,
        verify_servers_ssl_certificate, std::move(ssl_trust_certificate_path), std::move(ssl_verify_callback),
        std::move(proxy_config), server_slot.reconnect_info); // Throws
    ClientImpl::Connection& conn = *conn_2;
    if (!m_one_connection_per_session) {
        server_slot.connection = std::move(conn_2);
    }
    else {
        server_slot.alt_connections[ident] = std::move(conn_2); // Throws
    }
    m_prev_connection_ident = ident;
    was_created = true;
    {
        std::lock_guard lk(m_drain_mutex);
        ++m_num_connections;
    }
    return conn;
}


void ClientImpl::remove_connection(ClientImpl::Connection& conn) noexcept
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

    {
        std::lock_guard lk(m_drain_mutex);
        REALM_ASSERT(m_num_connections);
        --m_num_connections;
        m_drain_cv.notify_all();
    }
}


// ################ SessionImpl ################

void SessionImpl::force_close()
{
    // Allow force_close() if session is active or hasn't been activated yet.
    if (m_state == SessionImpl::Active || m_state == SessionImpl::Unactivated) {
        m_wrapper.force_close();
    }
}

void SessionImpl::on_connection_state_changed(ConnectionState state,
                                              const util::Optional<SessionErrorInfo>& error_info)
{
    // Only used to report errors back to the SyncSession while the Session is active
    if (m_state == SessionImpl::Active) {
        m_wrapper.on_connection_state_changed(state, error_info); // Throws
    }
}


const std::string& SessionImpl::get_virt_path() const noexcept
{
    // Can only be called if the session is active or being activated
    REALM_ASSERT_EX(m_state == State::Active || m_state == State::Unactivated, m_state);
    return m_wrapper.m_virt_path;
}

const std::string& SessionImpl::get_realm_path() const noexcept
{
    // Can only be called if the session is active or being activated
    REALM_ASSERT_EX(m_state == State::Active || m_state == State::Unactivated, m_state);
    return m_wrapper.m_db->get_path();
}

DBRef SessionImpl::get_db() const noexcept
{
    // Can only be called if the session is active or being activated
    REALM_ASSERT_EX(m_state == State::Active || m_state == State::Unactivated, m_state);
    return m_wrapper.m_db;
}

ClientReplication& SessionImpl::get_repl() const noexcept
{
    // Can only be called if the session is active or being activated
    REALM_ASSERT_EX(m_state == State::Active || m_state == State::Unactivated, m_state);
    return m_wrapper.get_replication();
}

ClientHistory& SessionImpl::get_history() const noexcept
{
    return get_repl().get_history();
}

util::Optional<ClientReset>& SessionImpl::get_client_reset_config() noexcept
{
    // Can only be called if the session is active or being activated
    REALM_ASSERT_EX(m_state == State::Active || m_state == State::Unactivated, m_state);
    return m_wrapper.m_client_reset_config;
}

SessionReason SessionImpl::get_session_reason() noexcept
{
    // Can only be called if the session is active or being activated
    REALM_ASSERT_EX(m_state == State::Active || m_state == State::Unactivated, m_state);
    return m_wrapper.m_session_reason;
}

uint64_t SessionImpl::get_schema_version() noexcept
{
    // Can only be called if the session is active or being activated
    REALM_ASSERT_EX(m_state == State::Active || m_state == State::Unactivated, m_state);
    return m_wrapper.m_schema_version;
}

void SessionImpl::initiate_integrate_changesets(std::uint_fast64_t downloadable_bytes, DownloadBatchState batch_state,
                                                const SyncProgress& progress, const ReceivedChangesets& changesets)
{
    // Ignore the call if the session is not active
    if (m_state != State::Active) {
        return;
    }

    try {
        bool simulate_integration_error = (m_wrapper.m_simulate_integration_error && !changesets.empty());
        if (simulate_integration_error) {
            throw IntegrationException(ErrorCodes::BadChangeset, "simulated failure", ProtocolError::bad_changeset);
        }
        version_type client_version;
        if (REALM_LIKELY(!get_client().is_dry_run())) {
            VersionInfo version_info;
            integrate_changesets(progress, downloadable_bytes, changesets, version_info, batch_state); // Throws
            client_version = version_info.realm_version;
        }
        else {
            // Fake it for "dry run" mode
            client_version = m_last_version_available + 1;
        }
        on_changesets_integrated(client_version, progress, !changesets.empty()); // Throws
    }
    catch (const IntegrationException& e) {
        on_integration_failure(e);
    }
}


void SessionImpl::on_upload_completion()
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.on_upload_completion(); // Throws
    }
}


void SessionImpl::on_download_completion()
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.on_download_completion(); // Throws
    }
}


void SessionImpl::on_suspended(const SessionErrorInfo& error_info)
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.on_suspended(error_info); // Throws
    }
}


void SessionImpl::on_resumed()
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.on_resumed(); // Throws
    }
}

void SessionImpl::handle_pending_client_reset_acknowledgement()
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.handle_pending_client_reset_acknowledgement();
    }
}

void SessionImpl::update_subscription_version_info()
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.update_subscription_version_info();
    }
}

bool SessionImpl::process_flx_bootstrap_message(const SyncProgress& progress, DownloadBatchState batch_state,
                                                int64_t query_version, const ReceivedChangesets& received_changesets)
{
    // Ignore the call if the session is not active
    if (m_state != State::Active) {
        return false;
    }

    if (is_steady_state_download_message(batch_state, query_version)) {
        return false;
    }

    auto bootstrap_store = m_wrapper.get_flx_pending_bootstrap_store();
    util::Optional<SyncProgress> maybe_progress;
    if (batch_state == DownloadBatchState::LastInBatch) {
        maybe_progress = progress;
    }

    bool new_batch = false;
    try {
        bootstrap_store->add_batch(query_version, std::move(maybe_progress), received_changesets, &new_batch);
    }
    catch (const LogicError& ex) {
        if (ex.code() == ErrorCodes::LimitExceeded) {
            IntegrationException ex(ErrorCodes::LimitExceeded,
                                    "bootstrap changeset too large to store in pending bootstrap store",
                                    ProtocolError::bad_changeset_size);
            on_integration_failure(ex);
            return true;
        }
        throw;
    }

    // If we've started a new batch and there is more to come, call on_flx_sync_progress to mark the subscription as
    // bootstrapping.
    if (new_batch && batch_state == DownloadBatchState::MoreToCome) {
        on_flx_sync_progress(query_version, DownloadBatchState::MoreToCome);
    }

    auto hook_action = call_debug_hook(SyncClientHookEvent::BootstrapMessageProcessed, progress, query_version,
                                       batch_state, received_changesets.size());
    if (hook_action == SyncClientHookAction::EarlyReturn) {
        return true;
    }
    REALM_ASSERT_EX(hook_action == SyncClientHookAction::NoAction, hook_action);

    if (batch_state == DownloadBatchState::MoreToCome) {
        notify_download_progress(bootstrap_store->pending_stats().pending_changeset_bytes);
        return true;
    }
    else {
        // FIXME (#7451) this variable is not needed in principle, and bootstrap store bytes could be passed just
        // through notify_download_progress, but since it is needed in report_progress, and it is also called on
        // upload progress for now until progress is reported separately. As soon as we understand here that there
        // are no more changesets for bootstrap store, and we want to process bootstrap, we don't need to notify
        // intermediate progress - so reset these bytes to not accidentally double report them.
        m_wrapper.m_bootstrap_store_bytes.reset();
    }

    try {
        process_pending_flx_bootstrap();
    }
    catch (const IntegrationException& e) {
        on_integration_failure(e);
    }
    catch (...) {
        on_integration_failure(IntegrationException(exception_to_status()));
    }

    return true;
}


void SessionImpl::process_pending_flx_bootstrap()
{
    // Ignore the call if not a flx session or session is not active
    if (!m_is_flx_sync_session || m_state != State::Active) {
        return;
    }
    // Should never be called if session is not active
    REALM_ASSERT_EX(m_state == SessionImpl::Active, m_state);
    auto bootstrap_store = m_wrapper.get_flx_pending_bootstrap_store();
    if (!bootstrap_store->has_pending()) {
        return;
    }

    auto pending_batch_stats = bootstrap_store->pending_stats();
    logger.info("Begin processing pending FLX bootstrap for query version %1. (changesets: %2, original total "
                "changeset size: %3)",
                pending_batch_stats.query_version, pending_batch_stats.pending_changesets,
                pending_batch_stats.pending_changeset_bytes);
    auto& history = get_repl().get_history();
    VersionInfo new_version;
    SyncProgress progress;
    int64_t query_version = -1;
    size_t changesets_processed = 0;

    // Used to commit each batch after it was transformed.
    TransactionRef transact = get_db()->start_write();
    while (bootstrap_store->has_pending()) {
        auto start_time = std::chrono::steady_clock::now();
        auto pending_batch = bootstrap_store->peek_pending(m_wrapper.m_flx_bootstrap_batch_size_bytes);
        if (!pending_batch.progress) {
            logger.info("Incomplete pending bootstrap found for query version %1", pending_batch.query_version);
            // Close the write transation before clearing the bootstrap store to avoid a deadlock because the
            // bootstrap store requires a write transaction itself.
            transact->close();
            bootstrap_store->clear();
            return;
        }

        auto batch_state =
            pending_batch.remaining_changesets > 0 ? DownloadBatchState::MoreToCome : DownloadBatchState::LastInBatch;
        uint64_t downloadable_bytes = 0;
        query_version = pending_batch.query_version;
        bool simulate_integration_error =
            (m_wrapper.m_simulate_integration_error && !pending_batch.changesets.empty());
        if (simulate_integration_error) {
            throw IntegrationException(ErrorCodes::BadChangeset, "simulated failure", ProtocolError::bad_changeset);
        }

        call_debug_hook(SyncClientHookEvent::BootstrapBatchAboutToProcess, *pending_batch.progress, query_version,
                        batch_state, pending_batch.changesets.size());

        history.integrate_server_changesets(
            *pending_batch.progress, &downloadable_bytes, pending_batch.changesets, new_version, batch_state, logger,
            transact, [&](const TransactionRef& tr, util::Span<Changeset> changesets_applied) {
                REALM_ASSERT_3(changesets_applied.size(), <=, pending_batch.changesets.size());
                bootstrap_store->pop_front_pending(tr, changesets_applied.size());
            });
        progress = *pending_batch.progress;
        changesets_processed += pending_batch.changesets.size();
        auto duration = std::chrono::steady_clock::now() - start_time;

        auto action = call_debug_hook(SyncClientHookEvent::DownloadMessageIntegrated, progress, query_version,
                                      batch_state, pending_batch.changesets.size());
        REALM_ASSERT_EX(action == SyncClientHookAction::NoAction, action);

        logger.info("Integrated %1 changesets from pending bootstrap for query version %2, producing client version "
                    "%3 in %4 ms. %5 changesets remaining in bootstrap",
                    pending_batch.changesets.size(), pending_batch.query_version, new_version.realm_version,
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(),
                    pending_batch.remaining_changesets);
    }
    on_changesets_integrated(new_version.realm_version, progress, changesets_processed > 0);

    REALM_ASSERT_3(query_version, !=, -1);
    on_flx_sync_progress(query_version, DownloadBatchState::LastInBatch);

    auto action = call_debug_hook(SyncClientHookEvent::BootstrapProcessed, progress, query_version,
                                  DownloadBatchState::LastInBatch, changesets_processed);
    // NoAction/EarlyReturn are both valid no-op actions to take here.
    REALM_ASSERT_EX(action == SyncClientHookAction::NoAction || action == SyncClientHookAction::EarlyReturn, action);
}

void SessionImpl::on_flx_sync_error(int64_t version, std::string_view err_msg)
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.on_flx_sync_error(version, err_msg);
    }
}

void SessionImpl::on_flx_sync_progress(int64_t version, DownloadBatchState batch_state)
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.on_flx_sync_progress(version, batch_state);
    }
}

SubscriptionStore* SessionImpl::get_flx_subscription_store()
{
    // Should never be called if session is not active
    REALM_ASSERT_EX(m_state == State::Active, m_state);
    return m_wrapper.get_flx_subscription_store();
}

MigrationStore* SessionImpl::get_migration_store()
{
    // Should never be called if session is not active
    REALM_ASSERT_EX(m_state == State::Active, m_state);
    return m_wrapper.get_migration_store();
}

void SessionImpl::on_flx_sync_version_complete(int64_t version)
{
    // Ignore the call if the session is not active
    if (m_state == State::Active) {
        m_wrapper.on_flx_sync_version_complete(version);
    }
}

SyncClientHookAction SessionImpl::call_debug_hook(const SyncClientHookData& data)
{
    // Should never be called if session is not active
    REALM_ASSERT_EX(m_state == State::Active, m_state);

    // Make sure we don't call the debug hook recursively.
    if (m_wrapper.m_in_debug_hook) {
        return SyncClientHookAction::NoAction;
    }
    m_wrapper.m_in_debug_hook = true;
    auto in_hook_guard = util::make_scope_exit([&]() noexcept {
        m_wrapper.m_in_debug_hook = false;
    });

    auto action = m_wrapper.m_debug_hook(data);
    switch (action) {
        case realm::SyncClientHookAction::SuspendWithRetryableError: {
            SessionErrorInfo err_info(Status{ErrorCodes::RuntimeError, "hook requested error"}, IsFatal{false});
            err_info.server_requests_action = ProtocolErrorInfo::Action::Transient;

            auto err_processing_err = receive_error_message(err_info);
            REALM_ASSERT_EX(err_processing_err.is_ok(), err_processing_err);
            return SyncClientHookAction::EarlyReturn;
        }
        case realm::SyncClientHookAction::TriggerReconnect: {
            get_connection().voluntary_disconnect();
            return SyncClientHookAction::EarlyReturn;
        }
        default:
            return action;
    }
}

SyncClientHookAction SessionImpl::call_debug_hook(SyncClientHookEvent event, const SyncProgress& progress,
                                                  int64_t query_version, DownloadBatchState batch_state,
                                                  size_t num_changesets)
{
    if (REALM_LIKELY(!m_wrapper.m_debug_hook)) {
        return SyncClientHookAction::NoAction;
    }
    if (REALM_UNLIKELY(m_state != State::Active)) {
        return SyncClientHookAction::NoAction;
    }

    SyncClientHookData data;
    data.event = event;
    data.batch_state = batch_state;
    data.progress = progress;
    data.num_changesets = num_changesets;
    data.query_version = query_version;

    return call_debug_hook(data);
}

SyncClientHookAction SessionImpl::call_debug_hook(SyncClientHookEvent event, const ProtocolErrorInfo& error_info)
{
    if (REALM_LIKELY(!m_wrapper.m_debug_hook)) {
        return SyncClientHookAction::NoAction;
    }
    if (REALM_UNLIKELY(m_state != State::Active)) {
        return SyncClientHookAction::NoAction;
    }

    SyncClientHookData data;
    data.event = event;
    data.batch_state = DownloadBatchState::SteadyState;
    data.progress = m_progress;
    data.num_changesets = 0;
    data.query_version = 0;
    data.error_info = &error_info;

    return call_debug_hook(data);
}

bool SessionImpl::is_steady_state_download_message(DownloadBatchState batch_state, int64_t query_version)
{
    // Should never be called if session is not active
    REALM_ASSERT_EX(m_state == State::Active, m_state);
    if (batch_state == DownloadBatchState::SteadyState) {
        return true;
    }

    if (!m_is_flx_sync_session) {
        return true;
    }

    // If this is a steady state DOWNLOAD, no need for special handling.
    if (batch_state == DownloadBatchState::LastInBatch && query_version == m_wrapper.m_flx_active_version) {
        return true;
    }

    return false;
}

void SessionImpl::init_progress_handler()
{
    if (m_state != State::Unactivated && m_state != State::Active)
        return;

    m_wrapper.init_progress_handler();
}

void SessionImpl::enable_progress_notifications()
{
    m_wrapper.m_reliable_download_progress = true;
}

void SessionImpl::notify_upload_progress()
{
    if (m_state != State::Active)
        return;

    m_wrapper.on_upload_progress();
}

void SessionImpl::update_download_estimate(double download_estimate)
{
    if (m_state != State::Active)
        return;

    m_wrapper.m_download_estimate = download_estimate;
}

void SessionImpl::notify_download_progress(const std::optional<uint64_t>& bootstrap_store_bytes)
{
    if (m_state != State::Active)
        return;

    m_wrapper.on_download_progress(bootstrap_store_bytes); // Throws
}

util::Future<std::string> SessionImpl::send_test_command(std::string body)
{
    if (m_state != State::Active) {
        return Status{ErrorCodes::RuntimeError, "Cannot send a test command for a session that is not active"};
    }

    try {
        auto json_body = nlohmann::json::parse(body.begin(), body.end());
        if (auto it = json_body.find("command"); it == json_body.end() || !it->is_string()) {
            return Status{ErrorCodes::LogicError,
                          "Must supply command name in \"command\" field of test command json object"};
        }
        if (json_body.size() > 1 && json_body.find("args") == json_body.end()) {
            return Status{ErrorCodes::LogicError, "Only valid fields in a test command are \"command\" and \"args\""};
        }
    }
    catch (const nlohmann::json::parse_error& e) {
        return Status{ErrorCodes::LogicError, util::format("Invalid json input to send_test_command: %1", e.what())};
    }

    auto pf = util::make_promise_future<std::string>();

    get_client().post([this, promise = std::move(pf.promise), body = std::move(body)](Status status) mutable {
        // Includes operation_aborted
        if (!status.is_ok()) {
            promise.set_error(status);
            return;
        }

        auto id = ++m_last_pending_test_command_ident;
        m_pending_test_commands.push_back(PendingTestCommand{id, std::move(body), std::move(promise)});
        ensure_enlisted_to_send();
    });

    return std::move(pf.future);
}

// ################ SessionWrapper ################

// The SessionWrapper class is held by a sync::Session (which is owned by the SyncSession instance) and
// provides a link to the ClientImpl::Session that creates and receives messages with the server with
// the ClientImpl::Connection that owns the ClientImpl::Session.
SessionWrapper::SessionWrapper(ClientImpl& client, DBRef db, std::shared_ptr<SubscriptionStore> flx_sub_store,
                               std::shared_ptr<MigrationStore> migration_store, Session::Config config)
    : m_client{client}
    , m_db(std::move(db))
    , m_replication(m_db->get_replication())
    , m_protocol_envelope{config.protocol_envelope}
    , m_server_address{std::move(config.server_address)}
    , m_server_port{config.server_port}
    , m_server_verified{config.server_verified}
    , m_user_id(std::move(config.user_id))
    , m_sync_mode(flx_sub_store ? SyncServerMode::FLX : SyncServerMode::PBS)
    , m_authorization_header_name{config.authorization_header_name}
    , m_custom_http_headers{config.custom_http_headers}
    , m_verify_servers_ssl_certificate{config.verify_servers_ssl_certificate}
    , m_simulate_integration_error{config.simulate_integration_error}
    , m_ssl_trust_certificate_path{std::move(config.ssl_trust_certificate_path)}
    , m_ssl_verify_callback{std::move(config.ssl_verify_callback)}
    , m_flx_bootstrap_batch_size_bytes(config.flx_bootstrap_batch_size_bytes)
    , m_http_request_path_prefix{std::move(config.service_identifier)}
    , m_virt_path{std::move(config.realm_identifier)}
    , m_signed_access_token{std::move(config.signed_user_token)}
    , m_client_reset_config{std::move(config.client_reset_config)}
    , m_proxy_config{config.proxy_config} // Throws
    , m_debug_hook(std::move(config.on_sync_client_event_hook))
    , m_session_reason(config.session_reason)
    , m_schema_version(config.schema_version)
    , m_flx_subscription_store(std::move(flx_sub_store))
    , m_migration_store(std::move(migration_store))
{
    REALM_ASSERT(m_db);
    REALM_ASSERT(m_db->get_replication());
    REALM_ASSERT(dynamic_cast<ClientReplication*>(m_db->get_replication()));
    if (m_client_reset_config) {
        m_session_reason = SessionReason::ClientReset;
    }
}

SessionWrapper::~SessionWrapper() noexcept
{
    if (m_db && m_actualized) {
        m_db->remove_commit_listener(this);
        m_db->release_sync_agent();
    }
}


inline ClientReplication& SessionWrapper::get_replication() noexcept
{
    REALM_ASSERT(m_db);
    return static_cast<ClientReplication&>(*m_replication);
}


inline ClientImpl& SessionWrapper::get_client() noexcept
{
    return m_client;
}

bool SessionWrapper::has_flx_subscription_store() const
{
    return static_cast<bool>(m_flx_subscription_store);
}

void SessionWrapper::on_flx_sync_error(int64_t version, std::string_view err_msg)
{
    REALM_ASSERT(!m_finalized);
    get_flx_subscription_store()->update_state(version, SubscriptionSet::State::Error, err_msg);
}

void SessionWrapper::on_flx_sync_version_complete(int64_t version)
{
    REALM_ASSERT(!m_finalized);
    m_flx_last_seen_version = version;
    m_flx_active_version = version;
}

void SessionWrapper::on_flx_sync_progress(int64_t new_version, DownloadBatchState batch_state)
{
    if (!has_flx_subscription_store()) {
        return;
    }
    REALM_ASSERT(!m_finalized);
    REALM_ASSERT(new_version >= m_flx_last_seen_version);
    REALM_ASSERT(new_version >= m_flx_active_version);
    REALM_ASSERT(batch_state != DownloadBatchState::SteadyState);

    SubscriptionSet::State new_state = SubscriptionSet::State::Uncommitted; // Initialize to make compiler happy

    switch (batch_state) {
        case DownloadBatchState::SteadyState:
            // Cannot be called with this value.
            REALM_UNREACHABLE();
        case DownloadBatchState::LastInBatch:
            if (m_flx_active_version == new_version) {
                return;
            }
            on_flx_sync_version_complete(new_version);
            if (new_version == 0) {
                new_state = SubscriptionSet::State::Complete;
            }
            else {
                new_state = SubscriptionSet::State::AwaitingMark;
                m_flx_pending_mark_version = new_version;
            }
            break;
        case DownloadBatchState::MoreToCome:
            if (m_flx_last_seen_version == new_version) {
                return;
            }

            m_flx_last_seen_version = new_version;
            new_state = SubscriptionSet::State::Bootstrapping;
            break;
    }

    get_flx_subscription_store()->update_state(new_version, new_state);
}

SubscriptionStore* SessionWrapper::get_flx_subscription_store()
{
    REALM_ASSERT(!m_finalized);
    return m_flx_subscription_store.get();
}

PendingBootstrapStore* SessionWrapper::get_flx_pending_bootstrap_store()
{
    REALM_ASSERT(!m_finalized);
    return m_flx_pending_bootstrap_store.get();
}

MigrationStore* SessionWrapper::get_migration_store()
{
    REALM_ASSERT(!m_finalized);
    return m_migration_store.get();
}

inline void SessionWrapper::mark_initiated()
{
    REALM_ASSERT(!m_initiated);
    REALM_ASSERT(!m_abandoned);
    m_initiated = true;
}


inline void SessionWrapper::mark_abandoned()
{
    REALM_ASSERT(!m_abandoned);
    m_abandoned = true;
}


inline void SessionWrapper::set_progress_handler(util::UniqueFunction<ProgressHandler> handler)
{
    REALM_ASSERT(!m_initiated);
    m_progress_handler = std::move(handler);
}


inline void
SessionWrapper::set_connection_state_change_listener(util::UniqueFunction<ConnectionStateChangeListener> listener)
{
    REALM_ASSERT(!m_initiated);
    m_connection_state_change_listener = std::move(listener);
}


void SessionWrapper::initiate()
{
    ServerEndpoint server_endpoint{m_protocol_envelope, m_server_address, m_server_port,
                                   m_user_id,           m_sync_mode,      m_server_verified};
    m_client.register_unactualized_session_wrapper(this, std::move(server_endpoint)); // Throws
    m_db->add_commit_listener(this);
}


void SessionWrapper::on_commit(version_type new_version)
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    m_client.post([self = std::move(self), new_version](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(self->m_finalized || self->m_force_closed)) {
            return;
        }

        if (REALM_UNLIKELY(!self->m_sess))
            return; // Already finalized
        SessionImpl& sess = *self->m_sess;
        sess.recognize_sync_version(new_version);                           // Throws
        self->on_upload_progress(/* only_if_new_uploadable_data = */ true); // Throws
    });
}


void SessionWrapper::cancel_reconnect_delay()
{
    // Thread safety required
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    m_client.post([self = std::move(self)](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(self->m_finalized || self->m_force_closed)) {
            return;
        }

        if (REALM_UNLIKELY(!self->m_sess))
            return; // Already finalized
        SessionImpl& sess = *self->m_sess;
        sess.cancel_resumption_delay(); // Throws
        ClientImpl::Connection& conn = sess.get_connection();
        conn.cancel_reconnect_delay(); // Throws
    });                                // Throws
}

void SessionWrapper::async_wait_for(bool upload_completion, bool download_completion,
                                    WaitOperCompletionHandler handler)
{
    REALM_ASSERT(upload_completion || download_completion);
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self{this};
    m_client.post([self = std::move(self), handler = std::move(handler), upload_completion,
                   download_completion](Status status) mutable {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(!self->m_sess)) {
            // Already finalized
            handler({ErrorCodes::OperationAborted, "Session finalized before callback could run"}); // Throws
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
    });                                                      // Throws
}


bool SessionWrapper::wait_for_upload_complete_or_client_stopped()
{
    // Thread safety required
    REALM_ASSERT(m_initiated);
    REALM_ASSERT(!m_abandoned);

    std::int_fast64_t target_mark;
    {
        std::lock_guard lock{m_client.m_mutex};
        target_mark = ++m_target_upload_mark;
    }

    util::bind_ptr<SessionWrapper> self{this};
    m_client.post([self = std::move(self), target_mark](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        REALM_ASSERT(self->m_actualized);
        REALM_ASSERT(!self->m_finalized);
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
    }); // Throws

    bool completion_condition_was_satisfied;
    {
        std::unique_lock lock{m_client.m_mutex};
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
    REALM_ASSERT(!m_abandoned);

    std::int_fast64_t target_mark;
    {
        std::lock_guard lock{m_client.m_mutex};
        target_mark = ++m_target_download_mark;
    }

    util::bind_ptr<SessionWrapper> self{this};
    m_client.post([self = std::move(self), target_mark](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        REALM_ASSERT(self->m_actualized);
        REALM_ASSERT(!self->m_finalized);
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
    }); // Throws

    bool completion_condition_was_satisfied;
    {
        std::unique_lock lock{m_client.m_mutex};
        while (m_reached_download_mark < target_mark && !m_client.m_stopped)
            m_client.m_wait_or_client_stopped_cond.wait(lock);
        completion_condition_was_satisfied = !m_client.m_stopped;
    }
    return completion_condition_was_satisfied;
}


void SessionWrapper::refresh(std::string_view signed_access_token)
{
    // Thread safety required
    REALM_ASSERT(m_initiated);
    REALM_ASSERT(!m_abandoned);

    m_client.post([self = util::bind_ptr(this), token = std::string(signed_access_token)](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        REALM_ASSERT(self->m_actualized);
        if (REALM_UNLIKELY(!self->m_sess))
            return; // Already finalized
        self->m_signed_access_token = std::move(token);
        SessionImpl& sess = *self->m_sess;
        ClientImpl::Connection& conn = sess.get_connection();
        // FIXME: This only makes sense when each session uses a separate connection.
        conn.update_connect_info(self->m_http_request_path_prefix, self->m_signed_access_token); // Throws
        sess.cancel_resumption_delay();                                                          // Throws
        conn.cancel_reconnect_delay();                                                           // Throws
    });
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
    REALM_ASSERT_DEBUG(m_initiated);
    REALM_ASSERT(!m_actualized);
    REALM_ASSERT(!m_sess);
    // Cannot be actualized if it's already been finalized or force closed
    REALM_ASSERT(!m_finalized);
    REALM_ASSERT(!m_force_closed);
    try {
        m_db->claim_sync_agent();
    }
    catch (const MultipleSyncAgents&) {
        finalize_before_actualization();
        throw;
    }
    auto sync_mode = endpoint.server_mode;

    bool was_created = false;
    ClientImpl::Connection& conn = m_client.get_connection(
        std::move(endpoint), m_authorization_header_name, m_custom_http_headers, m_verify_servers_ssl_certificate,
        m_ssl_trust_certificate_path, m_ssl_verify_callback, m_proxy_config,
        was_created); // Throws
    try {
        // FIXME: This only makes sense when each session uses a separate connection.
        conn.update_connect_info(m_http_request_path_prefix, m_signed_access_token);    // Throws
        std::unique_ptr<SessionImpl> sess = std::make_unique<SessionImpl>(*this, conn); // Throws
        if (sync_mode == SyncServerMode::FLX) {
            m_flx_pending_bootstrap_store = std::make_unique<PendingBootstrapStore>(m_db, sess->logger);
        }

        sess->logger.info("Binding '%1' to '%2'", m_db->get_path(), m_virt_path); // Throws
        m_sess = sess.get();
        conn.activate_session(std::move(sess)); // Throws
    }
    catch (...) {
        if (was_created)
            m_client.remove_connection(conn);

        // finalize_before_actualization() expects m_sess to be nullptr, but it's possible that we
        // reached its assignment above before throwing. Unset it here so we get a clean unhandled
        // exception failure instead of a REALM_ASSERT in finalize_before_actualization().
        m_sess = nullptr;
        finalize_before_actualization();
        throw;
    }

    // Initialize the variables relying on the bootstrap store from the event loop to guarantee that a previous
    // session cannot change the state of the bootstrap store at the same time.
    update_subscription_version_info();

    m_actualized = true;
    if (was_created)
        conn.activate(); // Throws

    if (m_connection_state_change_listener) {
        ConnectionState state = conn.get_state();
        if (state != ConnectionState::disconnected) {
            m_connection_state_change_listener(ConnectionState::connecting, util::none); // Throws
            if (state == ConnectionState::connected)
                m_connection_state_change_listener(ConnectionState::connected, util::none); // Throws
        }
    }

    if (!m_client_reset_config)
        on_upload_progress(/* only_if_new_uploadable_data = */ true); // Throws
}

void SessionWrapper::force_close()
{
    if (m_force_closed || m_finalized) {
        return;
    }
    REALM_ASSERT(m_actualized);
    REALM_ASSERT(m_sess);
    m_force_closed = true;

    ClientImpl::Connection& conn = m_sess->get_connection();
    conn.initiate_session_deactivation(m_sess); // Throws

    // Delete the pending bootstrap store since it uses a reference to the logger in m_sess
    m_flx_pending_bootstrap_store.reset();
    // Clear the subscription and migration store refs since they are owned by SyncSession
    m_flx_subscription_store.reset();
    m_migration_store.reset();
    m_sess = nullptr;
    // Everything is being torn down, no need to report connection state anymore
    m_connection_state_change_listener = {};
}

// Must be called from event loop thread
void SessionWrapper::finalize()
{
    REALM_ASSERT(m_actualized);
    REALM_ASSERT(m_abandoned);

    // Already finalized?
    if (m_finalized) {
        return;
    }

    // Must be before marking as finalized as we expect m_finalized == false in on_change()
    m_db->remove_commit_listener(this);

    m_finalized = true;

    if (!m_force_closed) {
        REALM_ASSERT(m_sess);
        ClientImpl::Connection& conn = m_sess->get_connection();
        conn.initiate_session_deactivation(m_sess); // Throws

        // Delete the pending bootstrap store since it uses a reference to the logger in m_sess
        m_flx_pending_bootstrap_store.reset();
        // Clear the subscription and migration store refs since they are owned by SyncSession
        m_flx_subscription_store.reset();
        m_migration_store.reset();
        m_sess = nullptr;
    }

    // The Realm file can be closed now, as no access to the Realm file is
    // supposed to happen on behalf of a session after initiation of
    // deactivation.
    m_db->release_sync_agent();
    m_db = nullptr;

    // All outstanding wait operations must be canceled
    while (!m_upload_completion_handlers.empty()) {
        auto handler = std::move(m_upload_completion_handlers.back());
        m_upload_completion_handlers.pop_back();
        handler(
            {ErrorCodes::OperationAborted, "Sync session is being finalized before upload was complete"}); // Throws
    }
    while (!m_download_completion_handlers.empty()) {
        auto handler = std::move(m_download_completion_handlers.back());
        m_download_completion_handlers.pop_back();
        handler(
            {ErrorCodes::OperationAborted, "Sync session is being finalized before download was complete"}); // Throws
    }
    while (!m_sync_completion_handlers.empty()) {
        auto handler = std::move(m_sync_completion_handlers.back());
        m_sync_completion_handlers.pop_back();
        handler({ErrorCodes::OperationAborted, "Sync session is being finalized before sync was complete"}); // Throws
    }
}


// Must be called only when an unactualized session wrapper becomes abandoned.
//
// Called with a lock on `m_client.m_mutex`.
inline void SessionWrapper::finalize_before_actualization() noexcept
{
    REALM_ASSERT(!m_sess);
    m_actualized = true;
    m_force_closed = true;
}

inline void SessionWrapper::on_upload_progress(bool only_if_new_uploadable_data)
{
    REALM_ASSERT(!m_finalized);
    report_progress(/* is_download = */ false, only_if_new_uploadable_data); // Throws
}

inline void SessionWrapper::on_download_progress(const std::optional<uint64_t>& bootstrap_store_bytes)
{
    REALM_ASSERT(!m_finalized);
    m_bootstrap_store_bytes = bootstrap_store_bytes;
    report_progress(/* is_download = */ true); // Throws
}


void SessionWrapper::on_upload_completion()
{
    REALM_ASSERT(!m_finalized);
    while (!m_upload_completion_handlers.empty()) {
        auto handler = std::move(m_upload_completion_handlers.back());
        m_upload_completion_handlers.pop_back();
        handler(Status::OK()); // Throws
    }
    while (!m_sync_completion_handlers.empty()) {
        auto handler = std::move(m_sync_completion_handlers.back());
        m_download_completion_handlers.push_back(std::move(handler)); // Throws
        m_sync_completion_handlers.pop_back();
    }
    std::lock_guard lock{m_client.m_mutex};
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
        handler(Status::OK()); // Throws
    }
    while (!m_sync_completion_handlers.empty()) {
        auto handler = std::move(m_sync_completion_handlers.back());
        m_upload_completion_handlers.push_back(std::move(handler)); // Throws
        m_sync_completion_handlers.pop_back();
    }

    if (m_flx_subscription_store && m_flx_pending_mark_version != SubscriptionSet::EmptyVersion) {
        m_sess->logger.debug("Marking query version %1 as complete after receiving MARK message",
                             m_flx_pending_mark_version);
        m_flx_subscription_store->update_state(m_flx_pending_mark_version, SubscriptionSet::State::Complete);
        m_flx_pending_mark_version = SubscriptionSet::EmptyVersion;
    }

    std::lock_guard lock{m_client.m_mutex};
    if (m_staged_download_mark > m_reached_download_mark) {
        m_reached_download_mark = m_staged_download_mark;
        m_client.m_wait_or_client_stopped_cond.notify_all();
    }
}


void SessionWrapper::on_suspended(const SessionErrorInfo& error_info)
{
    REALM_ASSERT(!m_finalized);
    m_suspended = true;
    if (m_connection_state_change_listener) {
        m_connection_state_change_listener(ConnectionState::disconnected, error_info); // Throws
    }
}


void SessionWrapper::on_resumed()
{
    REALM_ASSERT(!m_finalized);
    m_suspended = false;
    if (m_connection_state_change_listener) {
        ClientImpl::Connection& conn = m_sess->get_connection();
        if (conn.get_state() != ConnectionState::disconnected) {
            m_connection_state_change_listener(ConnectionState::connecting, util::none); // Throws
            if (conn.get_state() == ConnectionState::connected)
                m_connection_state_change_listener(ConnectionState::connected, util::none); // Throws
        }
    }
}


void SessionWrapper::on_connection_state_changed(ConnectionState state,
                                                 const util::Optional<SessionErrorInfo>& error_info)
{
    if (m_connection_state_change_listener) {
        if (!m_suspended)
            m_connection_state_change_listener(state, error_info); // Throws
    }
}

void SessionWrapper::init_progress_handler()
{
    uint64_t unused = 0;
    ClientHistory::get_upload_download_bytes(m_db.get(), m_reported_progress.final_downloaded, unused,
                                             m_reported_progress.final_uploaded, unused, unused);
}

void SessionWrapper::report_progress(bool is_download, bool only_if_new_uploadable_data)
{
    REALM_ASSERT(!m_finalized);
    REALM_ASSERT(m_sess);
    REALM_ASSERT(!(only_if_new_uploadable_data && is_download));

    if (!m_progress_handler)
        return;

    // Ignore progress messages from before we first receive a DOWNLOAD message
    if (!m_reliable_download_progress)
        return;

    ReportedProgress p = m_reported_progress;
    ClientHistory::get_upload_download_bytes(m_db.get(), p.downloaded, p.downloadable, p.uploaded, p.uploadable,
                                             p.snapshot);

    // If this progress notification was triggered by a commit being made we
    // only want to send it if the uploadable bytes has actually increased,
    // and not if it was an empty commit.
    if (only_if_new_uploadable_data && m_reported_progress.uploadable == p.uploadable)
        return;

    // uploadable_bytes is uploaded + remaining to upload, while downloadable_bytes
    // is only the remaining to download. This is confusing, so make them use
    // the same units.
    p.downloadable += p.downloaded;

    bool is_completed = false;
    if (is_download) {
        if (m_download_estimate)
            is_completed = *m_download_estimate >= 1.0;
        else
            is_completed = p.downloaded == p.downloadable;
    }
    else {
        is_completed = p.uploaded == p.uploadable;
    }

    auto calculate_progress = [](uint64_t transferred, uint64_t transferable, uint64_t final_transferred) {
        REALM_ASSERT_DEBUG_EX(final_transferred <= transferred, final_transferred, transferred, transferable);
        REALM_ASSERT_DEBUG_EX(transferred <= transferable, final_transferred, transferred, transferable);

        // The effect of this calculation is that if new bytes are added for download/upload,
        // the progress estimate doesn't go back to zero, but it goes back to some non-zero percentage.
        // This calculation allows a clean progression from 0 to 1.0 even if the new data is added for the sync
        // before progress has reached 1.0.
        // Then once it is at 1.0 the next batch of changes will restart the estimate at 0.
        // Example for upload progress reported:
        // 0 -> 1.0 -> new data added -> 0.0 -> 0.1 ...sync... -> 0.4 -> new data added -> 0.3 ...sync.. -> 1.0

        double progress_estimate = 1.0;
        if (final_transferred < transferable && transferred < transferable)
            progress_estimate = (transferred - final_transferred) / double(transferable - final_transferred);
        return progress_estimate;
    };

    double upload_estimate = 1.0, download_estimate = 1.0;

    // calculate estimate for both download/upload since the progress is reported all at once
    if (!is_completed || is_download)
        upload_estimate = calculate_progress(p.uploaded, p.uploadable, p.final_uploaded);

    // download estimate only known for flx
    if (m_download_estimate) {
        download_estimate = *m_download_estimate;

        // ... bootstrap store bytes should be null after initial sync when every changeset integrated immediately
        if (m_bootstrap_store_bytes)
            p.downloaded += *m_bootstrap_store_bytes;

        // FIXME for flx with download estimate these bytes are not known
        // provide some sensible value for non-streaming version of object-store callbacks
        // until these field are completely removed from the api after pbs deprecation
        p.downloadable = p.downloaded;
        if (0.01 <= download_estimate && download_estimate <= 0.99)
            if (p.downloaded > p.final_downloaded)
                p.downloadable =
                    p.final_downloaded + uint64_t((p.downloaded - p.final_downloaded) / download_estimate);
    }
    else {
        if (!is_completed || !is_download)
            download_estimate = calculate_progress(p.downloaded, p.downloadable, p.final_downloaded);
    }

    if (is_completed) {
        if (is_download)
            p.final_downloaded = p.downloaded;
        else
            p.final_uploaded = p.uploaded;
    }

    m_reported_progress = p;

    if (m_sess->logger.would_log(Logger::Level::debug)) {
        auto to_str = [](double d) {
            std::ostringstream ss;
            // progress estimate string in the DOWNLOAD message isn't expected to have more than 4 digits of precision
            ss << std::fixed << std::setprecision(4) << d;
            return ss.str();
        };
        m_sess->logger.debug("Progress handler called, downloaded = %1, downloadable = %2, estimate = %3, "
                             "uploaded = %4, uploadable = %5, estimate = %6, snapshot version = %7",
                             p.downloaded, p.downloadable, to_str(download_estimate), p.uploaded, p.uploadable,
                             to_str(upload_estimate), p.snapshot);
    }

    m_progress_handler(p.downloaded, p.downloadable, p.uploaded, p.uploadable, p.snapshot, download_estimate,
                       upload_estimate);
}

util::Future<std::string> SessionWrapper::send_test_command(std::string body)
{
    if (!m_sess) {
        return Status{ErrorCodes::RuntimeError, "session must be activated to send a test command"};
    }

    return m_sess->send_test_command(std::move(body));
}

void SessionWrapper::handle_pending_client_reset_acknowledgement()
{
    REALM_ASSERT(!m_finalized);

    auto pending_reset = _impl::client_reset::has_pending_reset(*m_db->start_frozen());
    REALM_ASSERT(pending_reset);
    m_sess->logger.info("Tracking pending client reset of type \"%1\" from %2", pending_reset->type,
                        pending_reset->time);
    async_wait_for(true, true, [self = util::bind_ptr(this), pending_reset = *pending_reset](Status status) {
        if (status == ErrorCodes::OperationAborted) {
            return;
        }
        auto& logger = self->m_sess->logger;
        if (!status.is_ok()) {
            logger.error("Error while tracking client reset acknowledgement: %1", status);
            return;
        }

        auto wt = self->m_db->start_write();
        auto cur_pending_reset = _impl::client_reset::has_pending_reset(*wt);
        if (!cur_pending_reset) {
            logger.debug(
                "Was going to remove client reset tracker for type \"%1\" from %2, but it was already removed",
                pending_reset.type, pending_reset.time);
            return;
        }
        else if (cur_pending_reset->type != pending_reset.type || cur_pending_reset->time != pending_reset.time) {
            logger.debug(
                "Was going to remove client reset tracker for type \"%1\" from %2, but found type \"%3\" from %4.",
                pending_reset.type, pending_reset.time, cur_pending_reset->type, cur_pending_reset->time);
        }
        else {
            logger.debug("Client reset of type \"%1\" from %2 has been acknowledged by the server. "
                         "Removing cycle detection tracker.",
                         pending_reset.type, pending_reset.time);
        }
        _impl::client_reset::remove_pending_client_resets(*wt);
        wt->commit();
    });
}

void SessionWrapper::update_subscription_version_info()
{
    if (!m_flx_subscription_store)
        return;
    auto versions_info = m_flx_subscription_store->get_version_info();
    m_flx_active_version = versions_info.active;
    m_flx_pending_mark_version = versions_info.pending_mark;
}

std::string SessionWrapper::get_appservices_connection_id()
{
    auto pf = util::make_promise_future<std::string>();
    REALM_ASSERT(m_initiated);

    util::bind_ptr<SessionWrapper> self(this);
    get_client().post([self, promise = std::move(pf.promise)](Status status) mutable {
        if (!status.is_ok()) {
            promise.set_error(status);
            return;
        }

        if (!self->m_sess) {
            promise.set_error({ErrorCodes::RuntimeError, "session already finalized"});
            return;
        }

        promise.emplace_value(self->m_sess->get_connection().get_active_appservices_connection_id());
    });

    return pf.future.get();
}

// ################ ClientImpl::Connection ################

ClientImpl::Connection::Connection(ClientImpl& client, connection_ident_type ident, ServerEndpoint endpoint,
                                   const std::string& authorization_header_name,
                                   const std::map<std::string, std::string>& custom_http_headers,
                                   bool verify_servers_ssl_certificate,
                                   Optional<std::string> ssl_trust_certificate_path,
                                   std::function<SSLVerifyCallback> ssl_verify_callback,
                                   Optional<ProxyConfig> proxy_config, ReconnectInfo reconnect_info)
    : logger_ptr{std::make_shared<util::PrefixLogger>(util::LogCategory::session, make_logger_prefix(ident),
                                                      client.logger_ptr)} // Throws
    , logger{*logger_ptr}
    , m_client{client}
    , m_verify_servers_ssl_certificate{verify_servers_ssl_certificate}    // DEPRECATED
    , m_ssl_trust_certificate_path{std::move(ssl_trust_certificate_path)} // DEPRECATED
    , m_ssl_verify_callback{std::move(ssl_verify_callback)}               // DEPRECATED
    , m_proxy_config{std::move(proxy_config)}                             // DEPRECATED
    , m_reconnect_info{reconnect_info}
    , m_session_history{}
    , m_ident{ident}
    , m_server_endpoint{std::move(endpoint)}
    , m_authorization_header_name{authorization_header_name} // DEPRECATED
    , m_custom_http_headers{custom_http_headers}             // DEPRECATED
{
    m_on_idle = m_client.create_trigger([this](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        else if (!status.is_ok())
            throw Exception(status);

        REALM_ASSERT(m_activated);
        if (m_state == ConnectionState::disconnected && m_num_active_sessions == 0) {
            on_idle(); // Throws
            // Connection object may be destroyed now.
        }
    });
}

inline connection_ident_type ClientImpl::Connection::get_ident() const noexcept
{
    return m_ident;
}


inline const ServerEndpoint& ClientImpl::Connection::get_server_endpoint() const noexcept
{
    return m_server_endpoint;
}

inline void ClientImpl::Connection::update_connect_info(const std::string& http_request_path_prefix,
                                                        const std::string& signed_access_token)
{
    m_http_request_path_prefix = http_request_path_prefix; // Throws (copy)
    m_signed_access_token = signed_access_token;           // Throws (copy)
}


void ClientImpl::Connection::resume_active_sessions()
{
    auto handler = [=](ClientImpl::Session& sess) {
        sess.cancel_resumption_delay(); // Throws
    };
    for_each_active_session(std::move(handler)); // Throws
}

void ClientImpl::Connection::on_idle()
{
    logger.debug(util::LogCategory::session, "Destroying connection object");
    ClientImpl& client = get_client();
    client.remove_connection(*this);
    // NOTE: This connection object is now destroyed!
}


std::string ClientImpl::Connection::get_http_request_path() const
{
    using namespace std::string_view_literals;
    const auto param = m_http_request_path_prefix.find('?') == std::string::npos ? "?baas_at="sv : "&baas_at="sv;

    std::string path;
    path.reserve(m_http_request_path_prefix.size() + param.size() + m_signed_access_token.size());
    path += m_http_request_path_prefix;
    path += param;
    path += m_signed_access_token;

    return path;
}


std::string ClientImpl::Connection::make_logger_prefix(connection_ident_type ident)
{
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << "Connection[" << ident << "]: "; // Throws
    return out.str();                       // Throws
}


void ClientImpl::Connection::report_connection_state_change(ConnectionState state,
                                                            util::Optional<SessionErrorInfo> error_info)
{
    if (m_force_closed) {
        return;
    }
    auto handler = [=](ClientImpl::Session& sess) {
        SessionImpl& sess_2 = static_cast<SessionImpl&>(sess);
        sess_2.on_connection_state_changed(state, error_info); // Throws
    };
    for_each_active_session(std::move(handler)); // Throws
}


Client::Client(Config config)
    : m_impl{new ClientImpl{std::move(config)}} // Throws
{
}


Client::Client(Client&& client) noexcept
    : m_impl{std::move(client.m_impl)}
{
}


Client::~Client() noexcept {}


void Client::shutdown() noexcept
{
    m_impl->shutdown();
}

void Client::shutdown_and_wait()
{
    m_impl->shutdown_and_wait();
}

void Client::cancel_reconnect_delay()
{
    m_impl->cancel_reconnect_delay();
}

void Client::voluntary_disconnect_all_connections()
{
    m_impl->voluntary_disconnect_all_connections();
}

bool Client::wait_for_session_terminations_or_client_stopped()
{
    return m_impl->wait_for_session_terminations_or_client_stopped();
}

util::Future<void> Client::notify_session_terminated()
{
    return m_impl->notify_session_terminated();
}

bool Client::decompose_server_url(const std::string& url, ProtocolEnvelope& protocol, std::string& address,
                                  port_type& port, std::string& path) const
{
    return m_impl->decompose_server_url(url, protocol, address, port, path); // Throws
}


Session::Session(Client& client, DBRef db, std::shared_ptr<SubscriptionStore> flx_sub_store,
                 std::shared_ptr<MigrationStore> migration_store, Config&& config)
{
    util::bind_ptr<SessionWrapper> sess;
    sess.reset(new SessionWrapper{*client.m_impl, std::move(db), std::move(flx_sub_store), std::move(migration_store),
                                  std::move(config)}); // Throws
    // The reference count passed back to the application is implicitly
    // owned by a naked pointer. This is done to avoid exposing
    // implementation details through the header file (that is, through the
    // Session object).
    m_impl = sess.release();
}


void Session::set_progress_handler(util::UniqueFunction<ProgressHandler> handler)
{
    m_impl->set_progress_handler(std::move(handler)); // Throws
}


void Session::set_connection_state_change_listener(util::UniqueFunction<ConnectionStateChangeListener> listener)
{
    m_impl->set_connection_state_change_listener(std::move(listener)); // Throws
}


void Session::bind()
{
    m_impl->initiate(); // Throws
}


void Session::nonsync_transact_notify(version_type new_version)
{
    m_impl->on_commit(new_version); // Throws
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


void Session::refresh(std::string_view signed_access_token)
{
    m_impl->refresh(signed_access_token); // Throws
}


void Session::abandon() noexcept
{
    REALM_ASSERT(m_impl);
    // Reabsorb the ownership assigned to the applications naked pointer by
    // Session constructor
    util::bind_ptr<SessionWrapper> wrapper{m_impl, util::bind_ptr_base::adopt_tag{}};
    SessionWrapper::abandon(std::move(wrapper));
}

util::Future<std::string> Session::send_test_command(std::string body)
{
    return m_impl->send_test_command(std::move(body));
}

std::string Session::get_appservices_connection_id()
{
    return m_impl->get_appservices_connection_id();
}

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
