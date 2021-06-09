#include <system_error>
#include <sstream>

#include <realm/util/assert.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/random.hpp>
#include <realm/util/memory_stream.hpp>
#include <realm/util/basic_system_errors.hpp>
#include <realm/util/to_string.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/http.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/sync/impl/clock.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_impl_base.hpp>
#include <realm/sync/noinst/compact_changesets.hpp>
#include <realm/version.hpp>
#include <realm/sync/changeset_parser.hpp>

// NOTE: The protocol specification is in `/doc/protocol.md`


using namespace realm;
using namespace _impl;
using namespace realm::util;

// clang-format off
using Connection      = ClientImplBase::Connection;
using Session         = ClientImplBase::Session;
using UploadChangeset = sync::ClientReplicationBase::UploadChangeset;

// These are a work-around for a bug in MSVC. It cannot find in-class types
// mentioned in signature of out-of-line member function definitions.
using file_ident_type = sync::file_ident_type;
using version_type = sync::version_type;
using salt_type = sync::salt_type;
using session_ident_type = sync::session_ident_type;
using request_ident_type = sync::request_ident_type;
using SyncProgress = sync::SyncProgress;
using ConnectionTerminationReason = ClientImplBase::ConnectionTerminationReason;
using ProtocolEnvelope            = ClientImplBase::ProtocolEnvelope;
using OutputBuffer                = ClientImplBase::OutputBuffer;
using ProtocolError               = ClientImplBase::ProtocolError;
using ReceivedChangesets          = ClientProtocol::ReceivedChangesets;
// clang-format on


namespace {

util::StderrLogger g_fallback_logger;

} // unnamed namespace


bool ClientImplBase::decompose_server_url(const std::string& url, ProtocolEnvelope& protocol, std::string& address,
                                          port_type& port, std::string& path) const
{
    util::Uri uri(url); // Throws
    uri.canonicalize(); // Throws
    std::string userinfo, address_2, port_2;
    bool realm_scheme = (uri.get_scheme() == "realm:" || uri.get_scheme() == "realms:");
    bool ws_scheme = (uri.get_scheme() == "ws:" || uri.get_scheme() == "wss:");
    bool good = ((realm_scheme || ws_scheme) && uri.get_auth(userinfo, address_2, port_2) && userinfo.empty() &&
                 !address_2.empty() && uri.get_query().empty() && uri.get_frag().empty()); // Throws
    if (REALM_UNLIKELY(!good))
        return false;
    ProtocolEnvelope protocol_2;
    port_type port_3;
    if (realm_scheme) {
        if (uri.get_scheme() == "realm:") {
            protocol_2 = ProtocolEnvelope::realm;
            port_3 = (m_enable_default_port_hack ? 80 : 7800);
        }
        else {
            protocol_2 = ProtocolEnvelope::realms;
            port_3 = (m_enable_default_port_hack ? 443 : 7801);
        }
    }
    else {
        REALM_ASSERT(ws_scheme);
        if (uri.get_scheme() == "ws:") {
            protocol_2 = ProtocolEnvelope::ws;
            port_3 = 80;
        }
        else {
            protocol_2 = ProtocolEnvelope::wss;
            port_3 = 443;
        }
    }
    if (!port_2.empty()) {
        std::istringstream in(port_2);    // Throws
        in.imbue(std::locale::classic()); // Throws
        in >> port_3;
        if (REALM_UNLIKELY(!in || !in.eof() || port_3 < 1))
            return false;
    }
    std::string path_2 = uri.get_path(); // Throws (copy)

    protocol = protocol_2;
    address = std::move(address_2);
    port = port_3;
    path = std::move(path_2);
    return true;
}


ClientImplBase::ClientImplBase(Config config)
    : logger{config.logger ? *config.logger : g_fallback_logger}
    , m_reconnect_mode{config.reconnect_mode}
    , m_connect_timeout{config.connect_timeout}
    , m_connection_linger_time{config.connection_linger_time}
    , m_ping_keepalive_period{config.ping_keepalive_period}
    , m_pong_keepalive_timeout{config.pong_keepalive_timeout}
    , m_fast_reconnect_limit{config.fast_reconnect_limit}
    , m_disable_upload_activation_delay{config.disable_upload_activation_delay}
    , m_dry_run{config.dry_run}
    , m_tcp_no_delay{config.tcp_no_delay}
    , m_enable_default_port_hack{config.enable_default_port_hack}
    , m_disable_upload_compaction{config.disable_upload_compaction}
    , m_roundtrip_time_handler{std::move(config.roundtrip_time_handler)}
    , m_user_agent_string{make_user_agent_string(config)} // Throws
    , m_service{}                                         // Throws
    , m_client_protocol{}                                 // Throws
{
    // FIXME: Would be better if seeding was up to the application.
    util::seed_prng_nondeterministically(m_random); // Throws
}


std::string ClientImplBase::make_user_agent_string(Config& config)
{
    std::string platform_info = std::move(config.user_agent_platform_info);
    if (platform_info.empty())
        platform_info = util::get_platform_info(); // Throws
    std::ostringstream out;
    out << "RealmSync/" REALM_VERSION_STRING " (" << platform_info << ")"; // Throws
    if (!config.user_agent_application_info.empty())
        out << " " << config.user_agent_application_info; // Throws
    return out.str();                                     // Throws
}


void Connection::activate()
{
    m_activated = true;
    if (m_num_active_sessions == 0)
        m_on_idle.trigger();
    // We cannot in general connect immediately, because a prior failure to
    // connect may require a delay before reconnecting (see `m_reconnect_info`).
    initiate_reconnect_wait(); // Throws
}


void Connection::activate_session(std::unique_ptr<Session> sess)
{
    REALM_ASSERT(&sess->m_conn == this);
    Session& sess_2 = *sess;
    session_ident_type ident = sess->m_ident;
    auto p = m_sessions.emplace(ident, std::move(sess)); // Throws
    bool was_inserted = p.second;
    REALM_ASSERT(was_inserted);
    sess_2.activate(); // Throws
    if (m_state == State::connected) {
        bool fast_reconnect = false;
        sess_2.connection_established(fast_reconnect); // Throws
    }
    ++m_num_active_sessions;
}


void Connection::initiate_session_deactivation(Session* sess)
{
    REALM_ASSERT(&sess->m_conn == this);
    if (REALM_UNLIKELY(--m_num_active_sessions == 0)) {
        if (m_activated && m_state == State::disconnected)
            m_on_idle.trigger();
    }
    sess->initiate_deactivation(); // Throws
    if (!sess->m_active_or_deactivating) {
        // Session is now deactivated, so remove and destroy it.
        session_ident_type ident = sess->m_ident;
        m_sessions.erase(ident);
    }
}


void Connection::cancel_reconnect_delay()
{
    REALM_ASSERT(m_activated);

    if (m_reconnect_delay_in_progress) {
        if (m_nonzero_reconnect_delay)
            logger.detail("Canceling reconnect delay"); // Throws

        // Cancel the in-progress wait operation by destroying the timer
        // object. Destruction is needed in this case, because a new wait
        // operation might have to be initiated before the previous one
        // completes (its completion handler starts to execute), so the new wait
        // operation must be done on a new timer object.
        m_reconnect_disconnect_timer = util::none;
        m_reconnect_delay_in_progress = false;
        m_reconnect_info.reset();
        initiate_reconnect_wait(); // Throws
        return;
    }
    if (m_state != State::disconnected) {
        // A currently established connection, or an in-progress attempt to
        // establish the connection may be about to fail for a reason that
        // precedes the invocation of Session::cancel_reconnect_delay(). For
        // that reason, it is important that at least one new reconnect attempt
        // is initiated without delay after the invocation of
        // Session::cancel_reconnect_delay(). The operation that resets the
        // reconnect delay (ReconnectInfo::reset()) needs to be postponed,
        // because some parts of `m_reconnect_info` may get clobbered before
        // initiate_reconnect_wait() is called again.
        //
        // If a PONG message arrives, and it is a response to the urgent PING
        // message sent below, `m_reconnect_info.m_scheduled_reset` will be
        // reset back to false, because in that case, we know that the
        // connection was not about to fail for a reason that preceded the
        // invocation of cancel_reconnect_delay().
        m_reconnect_info.m_scheduled_reset = true;
        m_ping_after_scheduled_reset_of_reconnect_info = false;

        schedule_urgent_ping(); // Throws
        return;
    }
    // Nothing to do in this case. The next reconnect attemp will be made as
    // soon as there are any sessions that are both active and unsuspended.
}


Connection::Connection(ClientImplBase& client, std::string logger_prefix, ProtocolEnvelope protocol,
                       std::string address, port_type port, bool verify_servers_ssl_certificate,
                       Optional<std::string> ssl_trust_certificate_path,
                       std::function<SSLVerifyCallback> ssl_verify_callback,
                       Optional<SyncConfig::ProxyConfig> proxy_config, ReconnectInfo reconnect_info)
    : logger{std::move(logger_prefix), client.logger} // Throws
    , m_client{client}
    , m_read_ahead_buffer{} // Throws
    , m_websocket{*this}    // Throws
    , m_protocol_envelope{protocol}
    , m_address{std::move(address)}
    , m_port{port}
    , m_http_host{util::make_http_host(sync::is_ssl(protocol), m_address, m_port)} // Throws
    , m_verify_servers_ssl_certificate{verify_servers_ssl_certificate}
    , m_ssl_trust_certificate_path{std::move(ssl_trust_certificate_path)}
    , m_ssl_verify_callback{std::move(ssl_verify_callback)}
    , m_proxy_config{std::move(proxy_config)}
    , m_reconnect_info{reconnect_info}
{
    auto handler = [this] {
        REALM_ASSERT(m_activated);
        if (m_state == State::disconnected && m_num_active_sessions == 0) {
            on_idle(); // Throws
            // Connection object may be destroyed now.
        }
    };
    m_on_idle = util::network::Trigger{client.get_service(), std::move(handler)}; // Throws
}


void Connection::on_connecting()
{
    // No-op unless overridden.
}


void Connection::on_connected()
{
    // No-op unless overridden.
}


void Connection::on_disconnected(std::error_code, bool, const StringData*)
{
    // No-op unless overridden.
}


void Connection::on_idle()
{
    // No-op unless overridden.
}


void Connection::set_http_request_headers(HTTPHeaders&)
{
    // No-op unless overridden.
}


util::Logger& Connection::websocket_get_logger() noexcept
{
    return logger;
}


std::mt19937_64& Connection::websocket_get_random() noexcept
{
    return m_client.m_random;
}


void Connection::async_read(char* buffer, std::size_t size, ReadCompletionHandler handler)
{
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_read(buffer, size, m_read_ahead_buffer, handler); // Throws
    }
    else {
        m_socket->async_read(buffer, size, m_read_ahead_buffer, handler); // Throws
    }
}


void Connection::async_read_until(char* buffer, std::size_t size, char delim, ReadCompletionHandler handler)
{
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_read_until(buffer, size, delim, m_read_ahead_buffer, handler); // Throws
    }
    else {
        m_socket->async_read_until(buffer, size, delim, m_read_ahead_buffer, handler); // Throws
    }
}


void Connection::async_write(const char* data, std::size_t size, WriteCompletionHandler handler)
{
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_write(data, size, handler); // Throws
    }
    else {
        m_socket->async_write(data, size, handler); // Throws
    }
}


void Connection::websocket_handshake_completion_handler(const HTTPHeaders& headers)
{
    auto i = headers.find("Sec-WebSocket-Protocol");
    if (i != headers.end()) {
        util::StringView value = i->second;
        util::StringView prefix = sync::get_websocket_protocol_prefix();
        // FIXME: Use std::string_view::begins_with() in C++20.
        bool begins_with =
            (value.size() >= prefix.size() && std::equal(value.data(), value.data() + prefix.size(), prefix.data()));
        if (begins_with) {
            util::MemoryInputStream in;
            in.set_buffer(value.data() + prefix.size(), value.data() + value.size());
            in.imbue(std::locale::classic());
            in.unsetf(std::ios_base::skipws);
            int value_2 = 0;
            in >> value_2;
            if (in && in.eof() && value_2 >= 0) {
                bool good_version = (value_2 >= get_oldest_supported_protocol_version() &&
                                     value_2 <= sync::get_current_protocol_version());
                if (good_version) {
                    logger.detail("Negotiated protocol version: %1", value_2);
                    m_negotiated_protocol_version = value_2;
                    handle_connection_established(); // Throws
                    return;
                }
            }
        }
        logger.error("Bad protocol info from server: '%1'", value); // Throws
    }
    else {
        logger.error("Missing protocol info from server"); // Throws
    }
    m_reconnect_info.m_reason = ConnectionTerminationReason::bad_headers_in_http_response;
    bool is_fatal = true;
    close_due_to_client_side_error(ClientError::bad_protocol_from_server, is_fatal); // Throws
}


void Connection::websocket_read_error_handler(std::error_code ec)
{
    read_error(ec); // Throws
}


void Connection::websocket_write_error_handler(std::error_code ec)
{
    write_error(ec); // Throws
}


void Connection::websocket_handshake_error_handler(std::error_code ec, const HTTPHeaders*,
                                                   const util::StringView* body)
{
    bool is_fatal;
    if (ec == util::websocket::Error::bad_response_3xx_redirection ||
        ec == util::websocket::Error::bad_response_301_moved_permanently ||
        ec == util::websocket::Error::bad_response_5xx_server_error ||
        ec == util::websocket::Error::bad_response_500_internal_server_error ||
        ec == util::websocket::Error::bad_response_502_bad_gateway ||
        ec == util::websocket::Error::bad_response_503_service_unavailable ||
        ec == util::websocket::Error::bad_response_504_gateway_timeout) {
        is_fatal = false;
        m_reconnect_info.m_reason = ConnectionTerminationReason::http_response_says_nonfatal_error;
    }
    else {
        is_fatal = true;
        m_reconnect_info.m_reason = ConnectionTerminationReason::http_response_says_fatal_error;
        if (body) {
            util::StringView identifier = "REALM_SYNC_PROTOCOL_MISMATCH";
            auto i = body->find(identifier);
            if (i != util::StringView::npos) {
                util::StringView rest = body->substr(i + identifier.size());
                // FIXME: Use std::string_view::begins_with() in C++20.
                auto begins_with = [](util::StringView string, util::StringView prefix) {
                    return (string.size() >= prefix.size() &&
                            std::equal(string.data(), string.data() + prefix.size(), prefix.data()));
                };
                if (begins_with(rest, ":CLIENT_TOO_OLD")) {
                    ec = ClientError::client_too_old_for_server;
                }
                else if (begins_with(rest, ":CLIENT_TOO_NEW")) {
                    ec = ClientError::client_too_new_for_server;
                }
                else {
                    // Other more complicated forms of mismatch
                    ec = ClientError::protocol_mismatch;
                }
            }
        }
    }

    close_due_to_client_side_error(ec, is_fatal); // Throws
}


void Connection::websocket_protocol_error_handler(std::error_code ec)
{
    m_reconnect_info.m_reason = ConnectionTerminationReason::websocket_protocol_violation;
    bool is_fatal = true;                         // A WebSocket protocol violation is a fatal error
    close_due_to_client_side_error(ec, is_fatal); // Throws
}


bool Connection::websocket_binary_message_received(const char* data, std::size_t size)
{
    std::error_code ec;
    using sf = SimulatedFailure;
    if (sf::trigger(sf::sync_client__read_head, ec)) {
        read_error(ec);
        return true;
    }

    handle_message_received(data, size);
    return true;
}


bool Connection::websocket_pong_message_received(const char* data, std::size_t size)
{
    handle_pong_received(data, size);
    return true;
}


bool Connection::websocket_close_message_received(std::error_code error_code, StringData message)
{
    if (error_code.category() == websocket::websocket_close_status_category() && error_code.value() != 1005 &&
        error_code.value() != 1000) {
        m_reconnect_info.m_reason = ConnectionTerminationReason::websocket_protocol_violation;
        involuntary_disconnect(error_code, false, &message);
    }

    return true;
}

// Guarantees that handle_reconnect_wait() is never called from within the
// execution of initiate_reconnect_wait() (no callback reentrance).
void Connection::initiate_reconnect_wait()
{
    REALM_ASSERT(m_activated);
    REALM_ASSERT(!m_reconnect_delay_in_progress);
    REALM_ASSERT(!m_disconnect_delay_in_progress);

    using milliseconds_lim = ReconnectInfo::milliseconds_lim;

    constexpr milliseconds_type min_delay = 1000;   // 1 second (barring deductions)
    constexpr milliseconds_type max_delay = 300000; // 5 minutes

    // Delay must increase when scaled by a facter grater than 1.
    static_assert(min_delay > 0, "");
    static_assert(max_delay >= min_delay, "");

    if (m_reconnect_info.m_scheduled_reset)
        m_reconnect_info.reset();

    bool infinite_delay = false;
    milliseconds_type remaining_delay = 0;
    if (!m_reconnect_info.m_reason) {
        // Delay in progress. `m_time_point` specifies when the delay expires.
        if (m_reconnect_info.m_time_point == milliseconds_lim::max()) {
            infinite_delay = true;
        }
        else {
            milliseconds_type now = monotonic_clock_now();
            if (now < m_reconnect_info.m_time_point)
                remaining_delay = m_reconnect_info.m_time_point - now;
        }
    }
    else {
        // Compute a new reconnect delay

        bool zero_delay = false;
        switch (m_client.get_reconnect_mode()) {
            case ReconnectMode::normal:
                break;
            case ReconnectMode::testing:
                if (was_voluntary(*m_reconnect_info.m_reason)) {
                    zero_delay = true;
                }
                else {
                    infinite_delay = true;
                }
                break;
        }

        // Calculate delay
        milliseconds_type delay = 0;
        bool record_delay_as_zero = false;
        if (!zero_delay && !infinite_delay) {
            switch (*m_reconnect_info.m_reason) {
                case ConnectionTerminationReason::resolve_operation_canceled:
                case ConnectionTerminationReason::connect_operation_canceled:
                case ConnectionTerminationReason::closed_voluntarily:
                case ConnectionTerminationReason::read_or_write_error:
                case ConnectionTerminationReason::premature_end_of_input:
                case ConnectionTerminationReason::pong_timeout:
                    // Minimum delay after successful connect operation
                    delay = min_delay;
                    break;
                case ConnectionTerminationReason::resolve_operation_failed:
                case ConnectionTerminationReason::connect_operation_failed:
                case ConnectionTerminationReason::http_response_says_nonfatal_error:
                case ConnectionTerminationReason::sync_connect_timeout:
                case ConnectionTerminationReason::http_tunnel_failed:
                    // The last attempt at establishing a connection failed. In
                    // this case, the reconnect delay will increase with the
                    // number of consecutive failures.
                    delay = m_reconnect_info.m_delay;
                    // Double the previous delay
                    if (util::int_multiply_with_overflow_detect(delay, 2))
                        delay = milliseconds_lim::max();
                    // Raise to minimum delay in case last delay was zero
                    if (delay < min_delay)
                        delay = min_delay;
                    // Cut off at a fixed maximum delay
                    if (delay > max_delay)
                        delay = max_delay;
                    break;
                case ConnectionTerminationReason::server_said_try_again_later:
                    delay = max_delay;
                    record_delay_as_zero = true;
                    break;
                case ConnectionTerminationReason::ssl_certificate_rejected:
                case ConnectionTerminationReason::ssl_protocol_violation:
                case ConnectionTerminationReason::websocket_protocol_violation:
                case ConnectionTerminationReason::http_response_says_fatal_error:
                case ConnectionTerminationReason::bad_headers_in_http_response:
                case ConnectionTerminationReason::sync_protocol_violation:
                case ConnectionTerminationReason::server_said_do_not_reconnect:
                case ConnectionTerminationReason::missing_protocol_feature:
                    // Use a significantly longer delay in this case to avoid
                    // disturbing the server too much. It does make sense to try
                    // again eventually, because the server may get restarted in
                    // such a way the that problem goes away.
                    delay = 3600000L; // 1 hour
                    record_delay_as_zero = true;
                    break;
            }

            // Make a randomized deduction of up to 25% to prevent a large
            // number of clients from trying to reconnect in synchronicity.
            auto distr = std::uniform_int_distribution<milliseconds_type>(0, delay / 4);
            milliseconds_type randomized_deduction = distr(m_client.get_random());
            delay -= randomized_deduction;

            // Finally, deduct the time that has already passed since the last
            // connection attempt.
            milliseconds_type now = monotonic_clock_now();
            REALM_ASSERT(now >= m_reconnect_info.m_time_point);
            milliseconds_type time_since_delay_start = now - m_reconnect_info.m_time_point;
            if (time_since_delay_start < delay)
                remaining_delay = delay - time_since_delay_start;
        }

        // Calculate expiration time for delay
        milliseconds_type time_point;
        if (infinite_delay) {
            time_point = milliseconds_lim::max();
        }
        else {
            time_point = m_reconnect_info.m_time_point;
            if (util::int_add_with_overflow_detect(time_point, delay))
                time_point = milliseconds_lim::max();
        }

        // Indicate that a new delay is now in progress
        m_reconnect_info.m_reason = util::none;
        m_reconnect_info.m_time_point = time_point;
        if (record_delay_as_zero) {
            m_reconnect_info.m_delay = 0;
        }
        else {
            m_reconnect_info.m_delay = delay;
        }
    }

    if (infinite_delay) {
        logger.detail("Reconnection delayed indefinitely"); // Throws
        // Not actually starting a timer corresponds to an infinite wait
        m_reconnect_delay_in_progress = true;
        m_nonzero_reconnect_delay = true;
        return;
    }

    if (remaining_delay > 0) {
        logger.detail("Allowing reconnection in %1 milliseconds",
                      remaining_delay); // Throws
    }

    if (!m_reconnect_disconnect_timer)
        m_reconnect_disconnect_timer.emplace(m_client.get_service()); // Throws
    auto handler = [this](std::error_code ec) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            handle_reconnect_wait(ec); // Throws
    };
    m_reconnect_disconnect_timer->async_wait(std::chrono::milliseconds(remaining_delay),
                                             std::move(handler)); // Throws
    m_reconnect_delay_in_progress = true;
    m_nonzero_reconnect_delay = (remaining_delay > 0);
}


void Connection::handle_reconnect_wait(std::error_code ec)
{
    if (ec) {
        REALM_ASSERT(ec != util::error::operation_aborted);
        throw std::system_error(ec);
    }

    m_reconnect_delay_in_progress = false;

    if (m_num_active_unsuspended_sessions > 0)
        initiate_reconnect(); // Throws
}


void Connection::initiate_reconnect()
{
    REALM_ASSERT(m_activated);

    on_connecting(); // Throws

    m_state = State::connecting;
    m_read_ahead_buffer.clear();
    m_ssl_stream = util::none;
    m_socket = util::none;
    m_resolver = util::none;

    // In most cases, the reconnect delay will be counting from the point in
    // time of the initiation of the last reconnect operation (the initiation of
    // the DNS resolve operation). It may also be counting from the point in
    // time of the reception of an ERROR message, but in that case we can simply
    // update `m_reconnect_info.m_time_point`.
    m_reconnect_info.m_time_point = monotonic_clock_now();

    // Watchdog
    initiate_connect_wait(); // Throws

    initiate_resolve(); // Throws
}


void Connection::initiate_connect_wait()
{
    // Deploy a watchdog to enforce an upper bound on the time it can take to
    // fully establish the connection (including SSL and WebSocket
    // handshakes). Without such a watchdog, connect operations could take very
    // long, or even indefinite time.
    m_connect_timer.emplace(m_client.get_service()); // Throws

    milliseconds_type time = m_client.m_connect_timeout;

    auto handler = [this](std::error_code ec) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            handle_connect_wait(ec); // Throws
    };
    m_connect_timer->async_wait(std::chrono::milliseconds(time), std::move(handler)); // Throws
}


void Connection::handle_connect_wait(std::error_code ec)
{
    if (ec) {
        REALM_ASSERT(ec != util::error::operation_aborted);
        throw std::system_error(ec);
    }

    REALM_ASSERT(m_state == State::connecting);
    m_reconnect_info.m_reason = ConnectionTerminationReason::sync_connect_timeout;
    logger.info("Connect timeout"); // Throws
    std::error_code ec_2 = ClientError::connect_timeout;
    bool is_fatal = false;
    StringData* custom_message = nullptr;
    involuntary_disconnect(ec_2, is_fatal, custom_message); // Throws
}


void Connection::initiate_resolve()
{
    // There are three outcomes of the DNS resolve operation; success, failure,
    // or cancellation. Since it is complicated to update the connection
    // termination reason on cancellation, we mark it as canceled now, and then
    // change it if the outcome ends up being success or failure.
    m_reconnect_info.m_reason = ConnectionTerminationReason::resolve_operation_canceled;

    const std::string& address = m_proxy_config ? m_proxy_config->address : m_address;
    const port_type& port = m_proxy_config ? m_proxy_config->port : m_port;

    if (m_proxy_config) {
        // logger.detail("Using %1 proxy", m_proxy_config->type); // Throws
    }

    logger.detail("Resolving '%1:%2'", address, port); // Throws

    util::network::Resolver::Query query(address, util::to_string(port)); // Throws
    auto handler = [this](std::error_code ec, util::network::Endpoint::List endpoints) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            handle_resolve(ec, std::move(endpoints)); // Throws
    };
    m_resolver.emplace(m_client.get_service());                      // Throws
    m_resolver->async_resolve(std::move(query), std::move(handler)); // Throws
}


void Connection::handle_resolve(std::error_code ec, util::network::Endpoint::List endpoints)
{
    if (ec) {
        resolve_error(ec); // Throws
        return;
    }

    initiate_tcp_connect(std::move(endpoints), 0); // Throws
}


void Connection::initiate_tcp_connect(util::network::Endpoint::List endpoints, std::size_t i)
{
    REALM_ASSERT(i < endpoints.size());

    // There are three outcomes of the TCP connect operation; success, failure,
    // or cancellation. Since it is complicated to update the connection
    // termination reason on cancellation, we mark it as canceled now, and then
    // change it if the outcome ends up being success or failure.
    m_reconnect_info.m_reason = ConnectionTerminationReason::connect_operation_canceled;

    util::network::Endpoint ep = *(endpoints.begin() + i);
    std::size_t n = endpoints.size();
    auto handler = [this, endpoints = std::move(endpoints), i](std::error_code ec) mutable {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            handle_tcp_connect(ec, std::move(endpoints), i); // Throws
    };
    m_socket.emplace(m_client.get_service());                                                     // Throws
    m_socket->async_connect(ep, std::move(handler));                                              // Throws
    logger.detail("Connecting to endpoint '%1:%2' (%3/%4)", ep.address(), ep.port(), (i + 1), n); // Throws
}


void Connection::handle_tcp_connect(std::error_code ec, util::network::Endpoint::List endpoints, std::size_t i)
{
    REALM_ASSERT(i < endpoints.size());
    const util::network::Endpoint& ep = *(endpoints.begin() + i);
    if (ec) {
        logger.error("Failed to connect to endpoint '%1:%2': %3", ep.address(), ep.port(),
                     ec.message()); // Throws
        std::size_t i_2 = i + 1;
        if (i_2 < endpoints.size()) {
            initiate_tcp_connect(std::move(endpoints), i_2); // Throws
            return;
        }
        // All endpoints failed
        tcp_connect_error(ec); // Throws
        return;
    }

    REALM_ASSERT(m_socket);
    if (m_client.get_tcp_no_delay())
        m_socket->set_option(util::network::SocketBase::no_delay(true)); // Throws
    util::network::Endpoint ep_2 = m_socket->local_endpoint();
    logger.info("Connected to endpoint '%1:%2' (from '%3:%4')", ep.address(), ep.port(), ep_2.address(),
                ep_2.port()); // Throws

    // At this point, when the connection is ultimately closed, it will either
    // be because of an error, or because the client closes it voluntarily. For
    // technical reasons, the most robust way to get the connection termination
    // reason correctly recorded, is to mark it as 'closed voluntarily' at this
    // time, and then update the reason later if the connection ends up being
    // terminated by an error (nonvoluntarily).
    m_reconnect_info.m_reason = ConnectionTerminationReason::closed_voluntarily;

    // TODO: Handle HTTPS proxies
    if (m_proxy_config) {
        initiate_http_tunnel(); // Throws
        return;
    }

    initiate_websocket_or_ssl_handshake(); // Throws
}

void Connection::initiate_websocket_or_ssl_handshake()
{
    bool ssl_mode = false;
    {
        switch (m_protocol_envelope) {
            case ProtocolEnvelope::realm:
            case ProtocolEnvelope::ws:
                break;
            case ProtocolEnvelope::realms:
            case ProtocolEnvelope::wss:
                ssl_mode = true;
                break;
        }
    }

    if (ssl_mode) {
        initiate_ssl_handshake(); // Throws
    }
    else {
        initiate_websocket_handshake(); // Throws
    }
}

void Connection::initiate_http_tunnel()
{
    HTTPRequest req;
    req.method = HTTPMethod::Connect;
    req.headers.emplace("Host", util::format("%1:%2", m_address, m_port));
    // TODO handle proxy authorization

    m_proxy_client.emplace(*this, logger);
    auto handler = [this](HTTPResponse response, std::error_code ec) {
        if (ec && ec != util::error::operation_aborted) {
            http_tunnel_error(ec); // Throws
            return;
        }

        if (response.status != HTTPStatus::Ok) {
            logger.error("Proxy server returned response '%1 %2'", response.status, response.reason); // Throws
            std::error_code ec2 =
                util::websocket::Error::bad_response_unexpected_status_code; // FIXME: is this the right error?
            http_tunnel_error(ec2);                                          // Throws
            return;
        }

        initiate_websocket_or_ssl_handshake(); // Throws
    };

    m_proxy_client->async_request(req, std::move(handler)); // Throws
}

void Connection::initiate_ssl_handshake()
{
    using namespace util::network::ssl;

    if (!m_ssl_context) {
        m_ssl_context.emplace(); // Throws
        if (m_verify_servers_ssl_certificate) {
            if (m_ssl_trust_certificate_path) {
                m_ssl_context->use_verify_file(*m_ssl_trust_certificate_path); // Throws
            }
            else if (!m_ssl_verify_callback) {
                m_ssl_context->use_default_verify(); // Throws
            }
        }
    }

    m_ssl_stream.emplace(*m_socket, *m_ssl_context, Stream::client); // Throws
    m_ssl_stream->set_logger(&logger);
    m_ssl_stream->set_host_name(m_address); // Throws
    if (m_verify_servers_ssl_certificate) {
        m_ssl_stream->set_verify_mode(VerifyMode::peer); // Throws
        m_ssl_stream->set_server_port(m_port);
        if (!m_ssl_trust_certificate_path) {
            if (m_ssl_verify_callback) {
                m_ssl_stream->use_verify_callback(m_ssl_verify_callback);
            }
            else {
                // The included certificates are used if neither the trust
                // certificate nor the callback function is set.
#if REALM_INCLUDE_CERTS
                m_ssl_stream->use_included_certificates(); // Throws
#endif
            }
        }
    }

    auto handler = [this](std::error_code ec) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            handle_ssl_handshake(ec); // Throws
    };
    m_ssl_stream->async_handshake(std::move(handler)); // Throws

    // FIXME: We also need to perform the SSL shutdown operation somewhere
}


void Connection::handle_ssl_handshake(std::error_code ec)
{
    if (ec) {
        REALM_ASSERT(ec != util::error::operation_aborted);
        ssl_handshake_error(ec); // Throws
        return;
    }

    initiate_websocket_handshake(); // Throws
}


void Connection::initiate_websocket_handshake()
{
    std::string path = get_http_request_path(); // Throws

    std::string sec_websocket_protocol;
    {
        std::ostringstream out;
        out.exceptions(std::ios_base::failbit | std::ios_base::badbit);
        out.imbue(std::locale::classic());
        const char* protocol_prefix = sync::get_websocket_protocol_prefix();
        int min = get_oldest_supported_protocol_version();
        int max = sync::get_current_protocol_version();
        REALM_ASSERT(min <= max);
        // List protocol version in descending order to ensure that the server
        // selects the highest possible version.
        int version = max;
        for (;;) {
            out << protocol_prefix << version; // Throws
            if (version == min)
                break;
            out << ", "; // Throws
            --version;
        }
        sec_websocket_protocol = std::move(out).str();
    }

    HTTPHeaders headers;
    ClientImplBase& client = get_client();
    headers["User-Agent"] = client.get_user_agent_string(); // Throws
    set_http_request_headers(headers);                      // Throws

    m_websocket.initiate_client_handshake(path, m_http_host, sec_websocket_protocol,
                                          std::move(headers)); // Throws
}


void Connection::handle_connection_established()
{
    // Cancel connect timeout watchdog
    m_connect_timer = util::none;

    m_state = State::connected;

    milliseconds_type now = monotonic_clock_now();
    m_pong_wait_started_at = now; // Initially, no time was spent waiting for a PONG message
    initiate_ping_delay(now);     // Throws

    bool fast_reconnect = false;
    if (m_disconnect_has_occurred) {
        milliseconds_type time = now - m_disconnect_time;
        if (time <= m_client.m_fast_reconnect_limit)
            fast_reconnect = true;
    }

    for (auto& p : m_sessions) {
        Session& sess = *p.second;
        sess.connection_established(fast_reconnect); // Throws
    }

    on_connected(); // Throws
}


void Connection::schedule_urgent_ping()
{
    REALM_ASSERT(m_state != State::disconnected);
    if (m_ping_delay_in_progress) {
        m_heartbeat_timer = util::none;
        m_ping_delay_in_progress = false;
        m_minimize_next_ping_delay = true;
        milliseconds_type now = monotonic_clock_now();
        initiate_ping_delay(now); // Throws
        return;
    }
    REALM_ASSERT(m_state == State::connecting || m_waiting_for_pong);
    if (!m_send_ping)
        m_minimize_next_ping_delay = true;
}


void Connection::initiate_ping_delay(milliseconds_type now)
{
    REALM_ASSERT(!m_ping_delay_in_progress);
    REALM_ASSERT(!m_waiting_for_pong);
    REALM_ASSERT(!m_send_ping);

    milliseconds_type delay = 0;
    if (!m_minimize_next_ping_delay) {
        delay = m_client.m_ping_keepalive_period;
        // Make a randomized deduction of up to 10%, or up to 100% if this is
        // the first PING message to be sent since the connection was
        // established. The purpose of this randomized deduction is to reduce
        // the risk of many connections sending PING messages simultaneously to
        // the server.
        milliseconds_type max_deduction = (m_ping_sent ? delay / 10 : delay);
        auto distr = std::uniform_int_distribution<milliseconds_type>(0, max_deduction);
        milliseconds_type randomized_deduction = distr(m_client.get_random());
        delay -= randomized_deduction;
        // Deduct the time spent waiting for PONG
        REALM_ASSERT(now >= m_pong_wait_started_at);
        milliseconds_type spent_time = now - m_pong_wait_started_at;
        if (spent_time < delay) {
            delay -= spent_time;
        }
        else {
            delay = 0;
        }
    }
    else {
        m_minimize_next_ping_delay = false;
    }


    m_ping_delay_in_progress = true;

    auto handler = [this](std::error_code ec) {
        if (ec != util::error::operation_aborted)
            handle_ping_delay(); // Throws
    };
    m_heartbeat_timer.emplace(m_client.get_service());                                   // Throws
    m_heartbeat_timer->async_wait(std::chrono::milliseconds(delay), std::move(handler)); // Throws
    logger.debug("Will emit a ping in %1 milliseconds", delay);                          // Throws
}


void Connection::handle_ping_delay()
{
    REALM_ASSERT(m_ping_delay_in_progress);
    m_ping_delay_in_progress = false;
    m_send_ping = true;

    initiate_pong_timeout(); // Throws

    if (m_state == State::connected && !m_sending)
        send_next_message(); // Throws
}


void Connection::initiate_pong_timeout()
{
    REALM_ASSERT(!m_ping_delay_in_progress);
    REALM_ASSERT(!m_waiting_for_pong);
    REALM_ASSERT(m_send_ping);

    m_waiting_for_pong = true;
    m_pong_wait_started_at = monotonic_clock_now();

    milliseconds_type time = m_client.m_pong_keepalive_timeout;
    auto handler = [this](std::error_code ec) {
        if (ec != util::error::operation_aborted)
            handle_pong_timeout(); // Throws
    };
    m_heartbeat_timer->async_wait(std::chrono::milliseconds(time), std::move(handler)); // Throws
}


void Connection::handle_pong_timeout()
{
    REALM_ASSERT(m_waiting_for_pong);
    logger.debug("Timeout on reception of PONG message"); // Throws
    m_reconnect_info.m_reason = ConnectionTerminationReason::pong_timeout;
    close_due_to_client_side_error(ClientError::pong_timeout, false);
}


void Connection::initiate_write_message(const OutputBuffer& out, Session* sess)
{
    auto handler = [=] {
        handle_write_message(); // Throws
    };
    m_websocket.async_write_binary(out.data(), out.size(), std::move(handler)); // Throws
    m_sending_session = sess;
    m_sending = true;
}


void Connection::handle_write_message()
{
    m_sending_session->message_sent(); // Throws
    if (!m_sending_session->m_active_or_deactivating) {
        // Session is now deactivated, so remove and destroy it.
        session_ident_type ident = m_sending_session->m_ident;
        m_sessions.erase(ident);
    }
    m_sending_session = nullptr;
    m_sending = false;
    send_next_message(); // Throws
}


void Connection::send_next_message()
{
    REALM_ASSERT(m_state == State::connected);
    REALM_ASSERT(!m_sending_session);
    REALM_ASSERT(!m_sending);
    if (m_send_ping) {
        send_ping(); // Throws
        return;
    }
    while (!m_sessions_enlisted_to_send.empty()) {
        // The state of being connected is not supposed to be able to change
        // across this loop thanks to the "no callback reentrance" guarantee
        // provided by util::Websocket::async_write_text(), and friends.
        REALM_ASSERT(m_state == State::connected);

        Session& sess = *m_sessions_enlisted_to_send.front();
        m_sessions_enlisted_to_send.pop_front();
        sess.send_message(); // Throws

        if (!sess.m_active_or_deactivating) {
            // Session is now deactivated, so remove and destroy it.
            session_ident_type ident = sess.m_ident;
            m_sessions.erase(ident);
        }

        // An enlisted session may choose to not send a message. In that case,
        // we should pass the opportunity to the next enlisted session.
        if (m_sending)
            break;
    }
}


void Connection::send_ping()
{
    REALM_ASSERT(!m_ping_delay_in_progress);
    REALM_ASSERT(m_waiting_for_pong);
    REALM_ASSERT(m_send_ping);

    m_send_ping = false;
    if (m_reconnect_info.m_scheduled_reset)
        m_ping_after_scheduled_reset_of_reconnect_info = true;

    m_last_ping_sent_at = monotonic_clock_now();
    logger.debug("Sending: PING(timestamp=%1, rtt=%2)", m_last_ping_sent_at,
                 m_previous_ping_rtt); // Throws

    ClientProtocol& protocol = get_client_protocol();
    OutputBuffer& out = get_output_buffer();
    protocol.make_ping(out, m_last_ping_sent_at, m_previous_ping_rtt); // Throws
    initiate_write_ping(out);                                          // Throws
    m_ping_sent = true;
}


void Connection::initiate_write_ping(const OutputBuffer& out)
{
    auto handler = [this] {
        handle_write_ping(); // Throws
    };
    m_websocket.async_write_binary(out.data(), out.size(), std::move(handler)); // Throws
    m_sending = true;
}


void Connection::handle_write_ping()
{
    REALM_ASSERT(m_sending);
    REALM_ASSERT(!m_sending_session);
    m_sending = false;
    send_next_message(); // Throws
}


void Connection::handle_message_received(const char* data, std::size_t size)
{
    // parse_message_received() parses the message and calls the proper handler
    // on the Connection object (this).
    get_client_protocol().parse_message_received<Connection>(*this, data, size);
}


void Connection::handle_pong_received(const char* data, std::size_t size)
{
    // parse_pong_received() parses the pong and calls the proper handler on the
    // Connection object (this).
    get_client_protocol().parse_pong_received<Connection>(*this, data, size);
}


void Connection::initiate_disconnect_wait()
{
    REALM_ASSERT(!m_reconnect_delay_in_progress);

    if (m_disconnect_delay_in_progress) {
        m_reconnect_disconnect_timer = util::none;
        m_disconnect_delay_in_progress = false;
    }

    milliseconds_type time = m_client.m_connection_linger_time;

    if (!m_reconnect_disconnect_timer)
        m_reconnect_disconnect_timer.emplace(m_client.get_service()); // Throws
    auto handler = [this](std::error_code ec) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            handle_disconnect_wait(ec); // Throws
    };
    m_reconnect_disconnect_timer->async_wait(std::chrono::milliseconds(time),
                                             std::move(handler)); // Throws
    m_disconnect_delay_in_progress = true;
}


void Connection::handle_disconnect_wait(std::error_code ec)
{
    if (ec) {
        REALM_ASSERT(ec != util::error::operation_aborted);
        throw std::system_error(ec);
    }

    m_disconnect_delay_in_progress = false;

    REALM_ASSERT(m_state != State::disconnected);
    if (m_num_active_unsuspended_sessions == 0) {
        if (m_client.m_connection_linger_time > 0)
            logger.detail("Linger time expired"); // Throws
        voluntary_disconnect();                   // Throws
        logger.info("Disconnected");              // Throws
    }
}


void Connection::resolve_error(std::error_code ec)
{
    m_reconnect_info.m_reason = ConnectionTerminationReason::resolve_operation_failed;
    logger.error("Failed to resolve '%1:%2': %3", m_address, m_port, ec.message()); // Throws
    // FIXME: Should some DNS lookup errors be considered fatal (persistent)?
    bool is_fatal = false;
    StringData* custom_message = nullptr;
    involuntary_disconnect(ec, is_fatal, custom_message); // Throws
}


void Connection::tcp_connect_error(std::error_code ec)
{
    m_reconnect_info.m_reason = ConnectionTerminationReason::connect_operation_failed;
    logger.error("Failed to connect to '%1:%2': All endpoints failed", m_address, m_port); // Throws
    // FIXME: Should some TCP connect errors be considered fatal (persistent)?
    bool is_fatal = false;
    StringData* custom_message = nullptr;
    involuntary_disconnect(ec, is_fatal, custom_message); // Throws
}

void Connection::http_tunnel_error(std::error_code ec)
{
    logger.error("Failed to establish HTTP tunnel: %1", ec.message());
    m_reconnect_info.m_reason = ConnectionTerminationReason::http_tunnel_failed;
    close_due_to_client_side_error(ClientError::http_tunnel_failed, true); // Throws
}

void Connection::ssl_handshake_error(std::error_code ec)
{
    logger.error("SSL handshake failed: %1", ec.message()); // Throws
    // FIXME: Some error codes (those from OpenSSL) most likely indicate a
    // fatal error (SSL protocol violation), but other errors codes
    // (read/write error from underlying socket) most likely indicate a
    // nonfatal error.
    bool is_fatal = false;
    std::error_code ec2;
    if (ec == network::ssl::Errors::certificate_rejected) {
        m_reconnect_info.m_reason = ConnectionTerminationReason::ssl_certificate_rejected;
        ec2 = ClientError::ssl_server_cert_rejected;
        is_fatal = true;
    }
    else {
        m_reconnect_info.m_reason = determine_connection_termination_reason(ec);
        ec2 = ec;
        is_fatal = false;
    }
    close_due_to_client_side_error(ec2, is_fatal); // Throws
}


void Connection::read_error(std::error_code ec)
{
    m_reconnect_info.m_reason = determine_connection_termination_reason(ec);
    logger.error("Reading failed: %1", ec.message()); // Throws
    bool is_fatal = false;                            // A read error is most likely not a persistent problem
    close_due_to_client_side_error(ec, is_fatal);     // Throws
}


void Connection::write_error(std::error_code ec)
{
    m_reconnect_info.m_reason = determine_connection_termination_reason(ec);
    logger.error("Writing failed: %1", ec.message()); // Throws
    bool is_fatal = false;                            // A write error is most likely not a persistent problem
    close_due_to_client_side_error(ec, is_fatal);     // Throws
}


void Connection::close_due_to_protocol_error(std::error_code ec)
{
    m_reconnect_info.m_reason = ConnectionTerminationReason::sync_protocol_violation;
    bool is_fatal = true;                         // A sync protocol violation is a fatal error
    close_due_to_client_side_error(ec, is_fatal); // Throws
}


void Connection::close_due_to_missing_protocol_feature()
{
    m_reconnect_info.m_reason = ConnectionTerminationReason::missing_protocol_feature;
    std::error_code ec = ClientError::missing_protocol_feature;
    bool is_fatal = true;                         // A missing protocol feature is a fatal error
    close_due_to_client_side_error(ec, is_fatal); // Throws
}


// Close connection due to error discovered on the client-side.
void Connection::close_due_to_client_side_error(std::error_code ec, bool is_fatal)
{
    logger.info("Connection closed due to error"); // Throws
    StringData* custom_message = nullptr;
    involuntary_disconnect(ec, is_fatal, custom_message); // Throws
}


// Close connection due to error discovered on the server-side, and then
// reported to the client by way of a connection-level ERROR message.
void Connection::close_due_to_server_side_error(ProtocolError error_code, StringData message, bool try_again)
{
    if (try_again) {
        m_reconnect_info.m_reason = ConnectionTerminationReason::server_said_try_again_later;
    }
    else {
        m_reconnect_info.m_reason = ConnectionTerminationReason::server_said_do_not_reconnect;
    }

    // When the server asks us to reconnect later, it is important to make the
    // reconnect delay start at the time of the reception of the ERROR message,
    // rather than at the initiation of the connection, as is usually the
    // case. This is because the message may arrive at a point in time where the
    // connection has been open for a long time, so if we let the delay count
    // from the initiation of the connection, it could easly end up as no delay
    // at all.
    m_reconnect_info.m_time_point = monotonic_clock_now();

    logger.info("Connection closed due to error reported by server: %1 (%2)", message, int(error_code)); // Throws

    std::error_code ec = make_error_code(error_code);
    bool is_fatal = !try_again;
    involuntary_disconnect(ec, is_fatal, &message); // Throws
}


void Connection::disconnect(std::error_code ec, bool is_fatal, StringData* custom_message)
{
    // Cancel connect timeout watchdog
    m_connect_timer = util::none;

    if (m_state == State::connected) {
        m_disconnect_time = monotonic_clock_now();
        m_disconnect_has_occurred = true;

        // Sessions that are in the Deactivating state at this time can be
        // immediately discarded, in part because they are no longer enlisted to
        // send. Such sessions will be taken to the Deactivated state by
        // Session::connection_lost(), and then they will be removed from
        // `m_sessions`.
        auto i = m_sessions.begin(), end = m_sessions.end();
        while (i != end) {
            // Prevent invalidation of the main iterator when erasing elements
            auto j = i++;
            Session& sess = *j->second;
            sess.connection_lost(); // Throws
            if (!sess.m_active_or_deactivating)
                m_sessions.erase(j);
        }
    }

    change_state_to_disconnected();

    m_ping_delay_in_progress = false;
    m_waiting_for_pong = false;
    m_send_ping = false;
    m_minimize_next_ping_delay = false;
    m_ping_after_scheduled_reset_of_reconnect_info = false;
    m_ping_sent = false;
    m_heartbeat_timer = util::none;
    m_previous_ping_rtt = 0;

    m_websocket.stop();
    m_ssl_stream = util::none;
    m_socket = util::none;
    m_resolver = util::none;
    m_input_body_buffer.reset();
    m_sending_session = nullptr;
    m_sessions_enlisted_to_send.clear();
    m_sending = false;

    on_disconnected(ec, is_fatal, custom_message); // Throws
    initiate_reconnect_wait();                     // Throws
}


void Connection::receive_pong(milliseconds_type timestamp)
{
    logger.debug("Received: PONG(timestamp=%1)", timestamp);

    bool legal_at_this_time = (m_waiting_for_pong && !m_send_ping);
    if (REALM_UNLIKELY(!legal_at_this_time)) {
        logger.error("Illegal message at this time");
        std::error_code ec = ClientError::bad_message_order;
        close_due_to_protocol_error(ec); // Throws
        return;
    }

    if (REALM_UNLIKELY(timestamp != m_last_ping_sent_at)) {
        logger.error("Bad timestamp in PONG message");
        std::error_code ec = ClientError::bad_timestamp;
        close_due_to_protocol_error(ec); // Throws
        return;
    }

    milliseconds_type now = monotonic_clock_now();
    milliseconds_type round_trip_time = now - timestamp;
    logger.debug("Round trip time was %1 milliseconds", round_trip_time);
    m_previous_ping_rtt = round_trip_time;

    // If this PONG message is a response to a PING mesage that was sent after
    // the last invocation of cancel_reconnect_delay(), then the connection is
    // still good, and we do not have to skip the next reconnect delay.
    if (m_ping_after_scheduled_reset_of_reconnect_info) {
        REALM_ASSERT(m_reconnect_info.m_scheduled_reset);
        m_ping_after_scheduled_reset_of_reconnect_info = false;
        m_reconnect_info.m_scheduled_reset = false;
    }

    m_heartbeat_timer = util::none;
    m_waiting_for_pong = false;

    initiate_ping_delay(now); // Throws

    if (m_client.m_roundtrip_time_handler)
        m_client.m_roundtrip_time_handler(m_previous_ping_rtt); // Throws
}


void Connection::receive_error_message(int error_code, StringData message, bool try_again,
                                       session_ident_type session_ident)
{
    Session* sess = nullptr;
    if (session_ident != 0) {
        sess = get_session(session_ident);
        if (REALM_UNLIKELY(!sess)) {
            logger.error("Bad session identifier in ERROR message, session_ident = %1",
                         session_ident);                                 // Throws
            close_due_to_protocol_error(ClientError::bad_session_ident); // Throws
            return;
        }
        std::error_code ec = sess->receive_error_message(error_code, message, try_again); // Throws
        if (ec) {
            close_due_to_protocol_error(ec); // Throws
            return;
        }

        if (!sess->m_active_or_deactivating) {
            // Session is now deactivated, so remove and destroy it.
            session_ident_type ident = sess->m_ident;
            m_sessions.erase(ident);
        }
        return;
    }

    logger.info("Received: ERROR \"%1\" (error_code=%2, try_again=%3, session_ident=%4)", message, error_code,
                try_again, session_ident); // Throws

    bool known_error_code = bool(sync::get_protocol_error_message(error_code));
    if (REALM_LIKELY(known_error_code)) {
        ProtocolError error_code_2 = ProtocolError(error_code);
        if (REALM_LIKELY(!is_session_level_error(error_code_2))) {
            close_due_to_server_side_error(ProtocolError(error_code), message,
                                           try_again); // Throws
            return;
        }
        logger.error("Not a connection-level error code"); // Throws
    }
    else {
        logger.error("Unknown error code"); // Throws
    }
    close_due_to_protocol_error(ClientError::bad_error_code); // Throws
}


void Connection::receive_ident_message(session_ident_type session_ident, SaltedFileIdent client_file_ident)
{
    Session* sess = get_session(session_ident);
    if (REALM_UNLIKELY(!sess)) {
        logger.error("Bad session identifier in IDENT message, session_ident = %1",
                     session_ident);                                 // Throws
        close_due_to_protocol_error(ClientError::bad_session_ident); // Throws
        return;
    }

    std::error_code ec = sess->receive_ident_message(client_file_ident); // Throws
    if (ec)
        close_due_to_protocol_error(ec); // Throws
}

void Connection::receive_client_version_message(session_ident_type session_ident, version_type client_version)
{
    Session* sess = get_session(session_ident);
    if (REALM_UNLIKELY(!sess)) {
        logger.error("Bad session identifier in CLIENT_VERSION message, "
                     "session_ident=%1",
                     session_ident);                                 // Throws
        close_due_to_protocol_error(ClientError::bad_session_ident); // Throws
        return;
    }

    sess->receive_client_version_message(client_version);
}

void Connection::receive_state_message(session_ident_type session_ident, version_type server_version,
                                       salt_type server_version_salt, uint_fast64_t begin_offset,
                                       uint_fast64_t end_offset, uint_fast64_t max_offset, BinaryData chunk)
{
    Session* sess = get_session(session_ident);
    if (REALM_UNLIKELY(!sess)) {
        logger.error("Bad session identifier in STATE message, session_ident = %1",
                     session_ident);                                 // Throws
        close_due_to_protocol_error(ClientError::bad_session_ident); // Throws
        return;
    }

    sess->receive_state_message(server_version, server_version_salt, begin_offset, end_offset, max_offset, chunk);
}


void Connection::receive_download_message(session_ident_type session_ident, const SyncProgress& progress,
                                          std::uint_fast64_t downloadable_bytes,
                                          const ReceivedChangesets& received_changesets)
{
    Session* sess = get_session(session_ident);
    if (REALM_UNLIKELY(!sess)) {
        logger.error("Bad session identifier in DOWNLOAD message, session_ident = %1",
                     session_ident);                                 // Throws
        close_due_to_protocol_error(ClientError::bad_session_ident); // Throws
        return;
    }

    sess->receive_download_message(progress, downloadable_bytes, received_changesets); // Throws
}


void Connection::receive_mark_message(session_ident_type session_ident, request_ident_type request_ident)
{
    Session* sess = get_session(session_ident);
    if (REALM_UNLIKELY(!sess)) {
        logger.error("Bad session identifier (%1) in MARK message", session_ident); // Throws
        close_due_to_protocol_error(ClientError::bad_session_ident);                // Throws
        return;
    }

    std::error_code ec = sess->receive_mark_message(request_ident); // Throws
    if (ec)
        close_due_to_protocol_error(ec); // Throws
}


void Connection::receive_alloc_message(session_ident_type session_ident, file_ident_type file_ident)
{
    Session* sess = get_session(session_ident);
    if (REALM_UNLIKELY(!sess)) {
        logger.error("Bad session identifier in ALLOC message, session_ident = %1",
                     session_ident);                                 // Throws
        close_due_to_protocol_error(ClientError::bad_session_ident); // Throws
        return;
    }

    std::error_code ec = sess->receive_alloc_message(file_ident); // Throws
    if (ec)
        close_due_to_protocol_error(ec); // Throws
}


void Connection::receive_unbound_message(session_ident_type session_ident)
{
    Session* sess = get_session(session_ident);
    if (REALM_UNLIKELY(!sess)) {
        logger.error("Bad session identifier in UNBOUND message, session_ident = %1",
                     session_ident);                                 // Throws
        close_due_to_protocol_error(ClientError::bad_session_ident); // Throws
        return;
    }

    std::error_code ec = sess->receive_unbound_message(); // Throws
    if (ec) {
        close_due_to_protocol_error(ec); // Throws
        return;
    }

    if (!sess->m_active_or_deactivating) {
        // Session is now deactivated, so remove and destroy it.
        session_ident_type ident = sess->m_ident;
        m_sessions.erase(ident);
    }
}


void Connection::handle_protocol_error(ClientProtocol::Error error)
{
    switch (error) {
        case ClientProtocol::Error::unknown_message:
            close_due_to_protocol_error(ClientError::unknown_message); // Throws
            break;
        case ClientProtocol::Error::bad_syntax:
            close_due_to_protocol_error(ClientError::bad_syntax); // Throws
            break;
        case ClientProtocol::Error::limits_exceeded:
            close_due_to_protocol_error(ClientError::limits_exceeded); // Throws
            break;
        case ClientProtocol::Error::bad_decompression:
            close_due_to_protocol_error(ClientError::bad_compression); // Throws
            break;
        case ClientProtocol::Error::bad_changeset_header_syntax:
            close_due_to_protocol_error(ClientError::bad_changeset_header_syntax); // Throws
            break;
        case ClientProtocol::Error::bad_changeset_size:
            close_due_to_protocol_error(ClientError::bad_changeset_size); // Throws
            break;
        case ClientProtocol::Error::bad_server_version:
            close_due_to_protocol_error(ClientError::bad_server_version); // Throws
            break;
        case ClientProtocol::Error::bad_error_code:
            close_due_to_protocol_error(ClientError::bad_error_code); // Throws
            break;
    }
}


// Sessions are guaranteed to be granted the opportunity to send a message in
// the order that they enlist. Note that this is important to ensure
// nonoverlapping communication with the server for consecutive sessions
// associated with the same Realm file.
//
// CAUTION: The specified session may get destroyed before this function
// returns, but only if its Session::send_message() puts it into the Deactivated
// state.
void Connection::enlist_to_send(Session* sess)
{
    REALM_ASSERT(m_state == State::connected);
    m_sessions_enlisted_to_send.push_back(sess); // Throws
    if (!m_sending)
        send_next_message(); // Throws
}


auto Connection::determine_connection_termination_reason(std::error_code ec) noexcept -> ConnectionTerminationReason
{
    if (ec == util::MiscExtErrors::premature_end_of_input)
        return ConnectionTerminationReason::premature_end_of_input;

    // FIXME: We need to identify SSL protocol violations here (by inspecting
    // the error code), and return
    // ConnectionTerminationReason::ssl_protocol_violation in those cases.

    return ConnectionTerminationReason::read_or_write_error;
}


void Session::cancel_resumption_delay()
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    if (!m_suspended)
        return;

    m_suspended = false;

    logger.debug("Resumed"); // Throws

    if (unbind_process_complete())
        initiate_rebind(); // Throws

    m_conn.one_more_active_unsuspended_session(); // Throws

    on_resumed(); // Throws
}


bool Session::integrate_changesets(ClientHistoryBase& history, const SyncProgress& progress,
                                   std::uint_fast64_t downloadable_bytes,
                                   const ReceivedChangesets& received_changesets, VersionInfo& version_info,
                                   IntegrationError& error)
{
    if (received_changesets.empty()) {
        history.set_sync_progress(progress, &downloadable_bytes, version_info); // Throws
        return true;
    }
    const sync::Transformer::RemoteChangeset* changesets = received_changesets.data();
    std::size_t num_changesets = received_changesets.size();
    bool success = history.integrate_server_changesets(progress, &downloadable_bytes, changesets, num_changesets,
                                                       version_info, error, logger,
                                                       m_sync_transact_reporter); // Throws
    if (REALM_LIKELY(success)) {
        if (num_changesets == 1) {
            logger.debug("1 remote changeset integrated, producing client version %1",
                         version_info.sync_version.version); // Throws
        }
        else {
            logger.debug("%2 remote changesets integrated, producing client version %1",
                         version_info.sync_version.version, num_changesets); // Throws
        }
    }
    return success;
}


void Session::on_changesets_integrated(bool success, version_type client_version, DownloadCursor download_progress,
                                       IntegrationError error)
{
    // Life cycle state must be Active
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(!m_deactivation_initiated);

    if (REALM_LIKELY(success)) {
        REALM_ASSERT(download_progress.server_version >= m_download_progress.server_version);
        m_download_progress = download_progress;
        do_recognize_sync_version(client_version); // Allows upload process to resume
        check_for_download_completion();           // Throws

        // Since the deactivation process has not been initiated, the UNBIND
        // message cannot have been sent unless an ERROR message was received.
        REALM_ASSERT(m_error_message_received || !m_unbind_message_sent);
        if (m_ident_message_sent && !m_error_message_received)
            ensure_enlisted_to_send(); // Throws
        return;
    }
    m_progress.download = m_download_progress;
    switch (error) {
        case IntegrationError::bad_origin_file_ident:
            m_conn.close_due_to_protocol_error(ClientError::bad_origin_file_ident);
            return;
        case IntegrationError::bad_changeset:
            break;
    }
    m_conn.close_due_to_protocol_error(ClientError::bad_changeset);
}


Session::~Session()
{
    //    REALM_ASSERT(!m_active_or_deactivating);
}


void Session::initiate_integrate_changesets(std::uint_fast64_t downloadable_bytes,
                                            const ReceivedChangesets& received_changesets)
{
    bool success;
    version_type client_version;
    IntegrationError error = {};
    if (REALM_LIKELY(!get_client().is_dry_run())) {
        VersionInfo version_info;
        ClientHistoryBase& history = access_realm(); // Throws
        success = integrate_changesets(history, m_progress, downloadable_bytes, received_changesets, version_info,
                                       error); // Throws
        client_version = version_info.realm_version;
    }
    else {
        // Fake it for "dry run" mode
        success = true;
        client_version = m_last_version_available + 1;
    }
    on_changesets_integrated(success, client_version, m_progress.download, error); // Throws
}


util::Optional<std::array<char, 64>> Session::get_encryption_key() const noexcept
{
    return none;
}


const util::Optional<sync::Session::Config::ClientReset>& Session::get_client_reset_config() const noexcept
{
    return m_client_reset_config;
}


void Session::on_state_download_progress(uint_fast64_t, uint_fast64_t)
{
    // No-op unless overridden.
}


void Session::on_upload_completion()
{
    // No-op unless overridden.
}


void Session::on_download_completion()
{
    // No-op unless overridden.
}


bool Session::on_subtier_file_ident(file_ident_type)
{
    return false; // By default, do not accept the received file identifier.
}


void Session::on_suspended(std::error_code, StringData, bool)
{
    // No-op unless overridden.
}


void Session::on_resumed()
{
    // No-op unless overridden.
}


std::string Session::make_logger_prefix(session_ident_type ident)
{
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << "Session[" << ident << "]: "; // Throws
    return out.str();                    // Throws
}


void Session::activate()
{
    // Session life cycle state must be Unactivated
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(!m_active_or_deactivating);

    logger.debug("Activating"); // Throws

    REALM_ASSERT(!m_client_state_download);

    if (REALM_LIKELY(!get_client().is_dry_run())) {
        const util::Optional<sync::Session::Config::ClientReset>& client_reset_config = get_client_reset_config();

        bool file_exists = util::File::exists(get_realm_path());
        if (client_reset_config && file_exists) {
            m_client_reset = true;
            m_client_reset_recover_local_changes = client_reset_config->recover_local_changes;
        }

        logger.info("client_reset_config = %1, Realm exists = %2, "
                    "async open = %3, client reset = %4",
                    client_reset_config ? "true" : "false", file_exists ? "true" : "false",
                    (client_reset_config && !file_exists) ? "true" : "false",
                    m_client_reset ? "true" : "false"); // Throws
        if (client_reset_config) {
            if (!util::File::exists(client_reset_config->metadata_dir)) {
                logger.error("Client reset config requires an existing metadata directory"); // Throws
                REALM_TERMINATE("No metadata directory");
            }
            logger.info("Client reset config, metadata_dir = '%1', "
                        "recover_local_changes = %2, "
                        "require_recent_state_realm = %3",
                        client_reset_config->metadata_dir,
                        client_reset_config->recover_local_changes ? "true" : "false",
                        client_reset_config->require_recent_state_realm ? "true" : "false"); // Throws
            m_state_download_in_progress = true;
            m_client_state_download.reset(new _impl::ClientStateDownload(logger, get_realm_path(),
                                                                         client_reset_config->metadata_dir,
                                                                         client_reset_config->recover_local_changes,
                                                                         get_encryption_key())); // Throws
        }

        if (!m_state_download_in_progress) {
            const ClientHistoryBase& history = access_realm();                             // Throws
            history.get_status(m_last_version_available, m_client_file_ident, m_progress); // Throws
        }
    }
    logger.debug("client_file_ident = %1, client_file_ident_salt = %2", m_client_file_ident.ident,
                 m_client_file_ident.salt); // Throws
    m_upload_target_version = m_last_version_available;
    m_upload_progress = m_progress.upload;
    m_last_version_selected_for_upload = m_upload_progress.client_version;
    m_download_progress = m_progress.download;
    REALM_ASSERT(m_last_version_available >= m_progress.upload.client_version);

    logger.trace("last_version_available  = %1", m_last_version_available);           // Throws
    logger.trace("progress_server_version = %1", m_progress.download.server_version); // Throws
    logger.trace("progress_client_version = %1",
                 m_progress.download.last_integrated_client_version); // Throws

    reset_protocol_state();
    m_active_or_deactivating = true;
    // Life cycle state is now Active

    REALM_ASSERT(!m_suspended);
    m_conn.one_more_active_unsuspended_session(); // Throws
}


// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
void Session::initiate_deactivation()
{
    // Life cycle state must be Active
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_active_or_deactivating);

    logger.debug("Initiating deactivation"); // Throws

    m_deactivation_initiated = true;
    // Life cycle state is now Deactivating

    if (!m_suspended)
        m_conn.one_less_active_unsuspended_session(); // Throws

    if (m_enlisted_to_send) {
        REALM_ASSERT(!unbind_process_complete());
        return;
    }

    // Deactivate immediately if the BIND message has not yet been sent and the
    // session is not enlisted to send, or if the unbinding process has already
    // completed.
    if (!m_bind_message_sent || unbind_process_complete()) {
        complete_deactivation(); // Throws
        // Life cycle state is now Deactivated
        return;
    }

    // Ready to send the UNBIND message, if it has not already been sent
    if (!m_unbind_message_sent) {
        enlist_to_send(); // Throws
        return;
    }
}


void Session::complete_deactivation()
{
    m_active_or_deactivating = false;

    logger.debug("Deactivation completed"); // Throws
}


// Called by the associated Connection object when this session is granted an
// opportunity to send a message.
//
// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
void Session::send_message()
{
    // Session life cycle state must be Active or Deactivating
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(m_enlisted_to_send);
    m_enlisted_to_send = false;
    if (REALM_LIKELY(!m_deactivation_initiated)) {
        // Session life cycle state is Active
        if (REALM_LIKELY(!m_error_message_received)) {
            // Session life cycle state is Active and the unbinding process has
            // not been initiated
            REALM_ASSERT(!m_unbind_message_sent);
            if (REALM_LIKELY(m_bind_message_sent)) {
                if (REALM_LIKELY(m_access_token_sent)) {
                    if (REALM_LIKELY(m_ident_message_sent)) {
                        bool send_alloc = (m_num_outstanding_subtier_allocations > 0 && !m_alloc_message_sent);
                        if (REALM_LIKELY(!send_alloc)) {
                            bool send_mark = (m_target_download_mark > m_last_download_mark_sent);
                            if (REALM_LIKELY(!send_mark)) {
                                REALM_ASSERT(m_upload_progress.client_version <= m_upload_target_version);
                                REALM_ASSERT(m_upload_target_version <= m_last_version_available);
                                bool need_upload = (m_upload_target_version > m_upload_progress.client_version);
                                if (REALM_LIKELY(!need_upload)) {
                                    return;
                                }
                                if (REALM_LIKELY(m_allow_upload))
                                    send_upload_message(); // Throws
                                return;
                            }
                            send_mark_message(); // Throws
                            return;
                        }
                        send_alloc_message(); // Throws
                        return;
                    }
                    if (have_client_file_ident()) {
                        bool should_send_client_version_request_message =
                            (m_client_reset && m_client_reset_recover_local_changes &&
                             !m_client_version_request_message_sent);
                        if (should_send_client_version_request_message) {
                            send_client_version_request_message(); // Throws
                        }
                        else if (m_state_download_in_progress) {
                            REALM_ASSERT(m_client_state_download);
                            if (!m_state_request_message_sent)
                                send_state_request_message(); // Throws
                        }
                        else {
                            send_ident_message(); // Throws
                        }
                    }
                    return;
                }
                send_refresh_message(); // Throws
                return;
            }
            send_bind_message(); // Throws
            return;
        }
    }
    else {
        // Deactivation has been initiated. If the UNBIND message has not been
        // sent yet, there is no point in sending it. Instead, we can let the
        // deactivation process complete.
        if (!m_bind_message_sent) {
            complete_deactivation(); // Throws
            // Life cycle state is now Deactivated
            return;
        }
    }
    // Session life cycle state is Deactivating or the unbinding process has
    // been initiated by a session specific ERROR message
    REALM_ASSERT(m_bind_message_sent);
    if (!m_unbind_message_sent)
        send_unbind_message(); // Throws
}


void Session::send_bind_message()
{
    REALM_ASSERT(!m_deactivation_initiated);

    session_ident_type session_ident = m_ident;
    const std::string& path = get_virt_path();
    const std::string& signed_access_token = get_signed_access_token();
    bool need_client_file_ident = !have_client_file_ident();

    logger.debug("Sending: BIND(path='%1', signed_user_token_size=%2, "
                 "need_client_file_ident=%3, is_subserver=%4)",
                 path, signed_access_token.size(), int(need_client_file_ident), int(m_is_subserver)); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    int protocol_version = m_conn.get_negotiated_protocol_version();
    OutputBuffer& out = m_conn.get_output_buffer();
    protocol.make_bind_message(protocol_version, out, session_ident, path, signed_access_token,
                               need_client_file_ident, m_is_subserver); // Throws
    m_conn.initiate_write_message(out, this);                           // Throws

    m_bind_message_sent = true;
    m_access_token_sent = true;

    // Ready to send the IDENT message if the file identifier pair is already
    // available.
    if (!need_client_file_ident)
        enlist_to_send(); // Throws
}


void Session::send_ident_message()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_bind_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(have_client_file_ident());

    logger.debug("Sending: IDENT(client_file_ident=%1, client_file_ident_salt=%2, "
                 "scan_server_version=%3, scan_client_version=%4, latest_server_version=%5, "
                 "latest_server_version_salt=%6)",
                 m_client_file_ident.ident, m_client_file_ident.salt, m_progress.download.server_version,
                 m_progress.download.last_integrated_client_version, m_progress.latest_server_version.version,
                 m_progress.latest_server_version.salt); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    OutputBuffer& out = m_conn.get_output_buffer();
    session_ident_type session_ident = m_ident;
    protocol.make_ident_message(out, session_ident, m_client_file_ident, m_progress); // Throws
    m_conn.initiate_write_message(out, this);                                         // Throws

    m_ident_message_sent = true;

    // Other messages may be waiting to be sent
    enlist_to_send(); // Throws
}

void Session::send_client_version_request_message()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_bind_message_sent);
    REALM_ASSERT(!m_state_request_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(have_client_file_ident());
    REALM_ASSERT(m_client_state_download);
    REALM_ASSERT(util::File::exists(get_realm_path()));

    SaltedFileIdent client_file_ident;
    {
        version_type current_client_version;                                     // Dummy
        SyncProgress progress;                                                   // Dummy
        const ClientHistoryBase& history = access_realm();                       // Throws
        history.get_status(current_client_version, client_file_ident, progress); // Throws
    }

    m_client_version_request_message_sent = true;

    if (client_file_ident.ident == 0) {
        // The response would always be client_version = 0.
        logger.debug("Skipping the CLIENT_VERSION_REQUEST since client_file_ident=0");
        enlist_to_send();
    }
    else {
        logger.debug("Sending: CLIENT_VERSION_REQUEST(client_file_ident=%1, "
                     "client_file_ident_salt=%2)",
                     client_file_ident.ident,
                     client_file_ident.salt); // Throws

        ClientProtocol& protocol = m_conn.get_client_protocol();
        OutputBuffer& out = m_conn.get_output_buffer();
        session_ident_type session_ident = m_ident;
        protocol.make_client_version_request_message(out, session_ident, client_file_ident); // Throws
        m_conn.initiate_write_message(out, this);                                            // Throws
        // No enlist_to_send() since the client must wait for the server.
    }
}

void Session::send_state_request_message()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_bind_message_sent);
    REALM_ASSERT(!m_state_request_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(have_client_file_ident());
    REALM_ASSERT(m_client_state_download);
    REALM_ASSERT(get_client_reset_config());

    session_ident_type session_ident = m_ident;

    SaltedVersion partial_transfer_server_version = {m_client_state_download->get_server_version(),
                                                     m_client_state_download->get_server_version_salt()};

    uint_fast64_t end_offset = m_client_state_download->get_end_offset();
    bool need_recent = m_client_reset || get_client_reset_config()->require_recent_state_realm;

    std::int_fast32_t min_file_format_version = 0;
    std::int_fast32_t max_file_format_version = 0;
    std::int_fast32_t min_history_schema_version = 0;
    std::int_fast32_t max_history_schema_version = 0;
    {
        using gf = GroupFriend;
        int current_file_format_version = 0;
        int history_type = Replication::hist_SyncClient;
        min_file_format_version =
            gf::get_target_file_format_version_for_session(current_file_format_version, history_type);
        min_history_schema_version = get_client_history_schema_version();
    }

    logger.debug("Sending: STATE_REQUEST(partial_transfer_server_version=%1, "
                 "partial_transfer_server_version_salt=%2, end_offset=%3, "
                 "need_recent=%4, min_file_format_version=%5, max_file_format_version=%6, "
                 "min_history_schema_version=%7, max_history_schema_version=%8)",
                 partial_transfer_server_version.version, partial_transfer_server_version.salt, end_offset,
                 need_recent, min_file_format_version, max_file_format_version, min_history_schema_version,
                 max_history_schema_version); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    int protocol_version = m_conn.get_negotiated_protocol_version();
    OutputBuffer& out = m_conn.get_output_buffer();
    protocol.make_state_request_message(protocol_version, out, session_ident, partial_transfer_server_version,
                                        end_offset, need_recent, min_file_format_version, max_file_format_version,
                                        min_history_schema_version, max_history_schema_version);
    m_conn.initiate_write_message(out, this); // Throws

    m_state_request_message_sent = true;
    // No enlist_to_send() since the client must wait for the server.
}

void Session::send_upload_message()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_ident_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(m_upload_target_version > m_upload_progress.client_version);

    if (REALM_UNLIKELY(m_disable_upload))
        return;

    if (REALM_UNLIKELY(get_client().is_dry_run()))
        return;

    const ClientHistoryBase& history = access_realm(); // Throws

    std::vector<UploadChangeset> uploadable_changesets;
    version_type locked_server_version = 0;
    history.find_uploadable_changesets(m_upload_progress, m_upload_target_version, uploadable_changesets,
                                       locked_server_version); // Throws

    if (uploadable_changesets.empty()) {
        if (REALM_UNLIKELY(m_disable_empty_upload))
            return;
        // Nothing more to upload right now
        if (m_upload_completion_notification_requested)
            check_for_upload_completion(); // Throws
    }
    else {
        m_last_version_selected_for_upload = uploadable_changesets.back().progress.client_version;
    }

    version_type progress_client_version = m_upload_progress.client_version;
    version_type progress_server_version = m_upload_progress.last_integrated_server_version;

    logger.debug("Sending: UPLOAD(progress_client_version=%1, progress_server_version=%2, "
                 "locked_server_version=%3, num_changesets=%4)",
                 progress_client_version, progress_server_version, locked_server_version,
                 uploadable_changesets.size()); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    ClientProtocol::UploadMessageBuilder upload_message_builder =
        protocol.make_upload_message_builder(logger); // Throws

    for (const UploadChangeset& uc : uploadable_changesets) {
        logger.trace("Fetching changeset for upload (client_version=%1, server_version=%2, "
                     "changeset_size=%3, origin_timestamp=%4, origin_file_ident=%5)",
                     uc.progress.client_version, uc.progress.last_integrated_server_version, uc.changeset.size(),
                     uc.origin_timestamp, uc.origin_file_ident); // Throws
        if (logger.would_log(util::Logger::Level::trace)) {
            BinaryData changeset_data = uc.changeset.get_first_chunk();
            if (changeset_data.size() < 1024) {
                logger.trace("Changeset: %1",
                             _impl::clamped_hex_dump(changeset_data)); // Throws
            }
            else {
                logger.trace("Changeset(comp): %1 %2", changeset_data.size(),
                             protocol.compressed_hex_dump(changeset_data));
            }
        }

        if (!get_client().m_disable_upload_compaction) {
            // Upload compaction only takes place within single changesets to
            // avoid another client seeing inconsistent snapshots.
            ChunkedBinaryInputStream stream{uc.changeset};
            sync::Changeset changeset;
            sync::parse_changeset(stream, changeset); // Throws
            // FIXME: What is the point of setting these? How can compaction care about them?
            changeset.version = uc.progress.client_version;
            changeset.last_integrated_remote_version = uc.progress.last_integrated_server_version;
            changeset.origin_timestamp = uc.origin_timestamp;
            changeset.origin_file_ident = uc.origin_file_ident;

            compact_changesets(&changeset, 1);

            util::AppendBuffer<char> encode_buffer;
            encode_changeset(changeset, encode_buffer);

            logger.debug("Upload compaction: original size = %1, compacted size = %2", uc.changeset.size(),
                         encode_buffer.size()); // Throws

            upload_message_builder.add_changeset(
                uc.progress.client_version, uc.progress.last_integrated_server_version, uc.origin_timestamp,
                uc.origin_file_ident, BinaryData{encode_buffer.data(), encode_buffer.size()}); // Throws
        }
        else {
            upload_message_builder.add_changeset(uc.progress.client_version,
                                                 uc.progress.last_integrated_server_version, uc.origin_timestamp,
                                                 uc.origin_file_ident,
                                                 uc.changeset); // Throws
        }
    }

    int protocol_version = m_conn.get_negotiated_protocol_version();
    OutputBuffer& out = m_conn.get_output_buffer();
    session_ident_type session_ident = get_ident();
    upload_message_builder.make_upload_message(protocol_version, out, session_ident, progress_client_version,
                                               progress_server_version,
                                               locked_server_version); // Throws
    m_conn.initiate_write_message(out, this);                          // Throws

    // Other messages may be waiting to be sent
    enlist_to_send(); // Throws
}


void Session::send_mark_message()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_ident_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(m_target_download_mark > m_last_download_mark_sent);

    request_ident_type request_ident = m_target_download_mark;
    logger.debug("Sending: MARK(request_ident=%1)", request_ident); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    OutputBuffer& out = m_conn.get_output_buffer();
    session_ident_type session_ident = get_ident();
    protocol.make_mark_message(out, session_ident, request_ident); // Throws
    m_conn.initiate_write_message(out, this);                      // Throws

    m_last_download_mark_sent = request_ident;

    // Other messages may be waiting to be sent
    enlist_to_send(); // Throws
}


void Session::send_alloc_message()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_ident_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(!m_alloc_message_sent);

    logger.debug("Sending: ALLOC"); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    OutputBuffer& out = m_conn.get_output_buffer();
    session_ident_type session_ident = get_ident();
    protocol.make_alloc_message(out, session_ident); // Throws
    m_conn.initiate_write_message(out, this);        // Throws

    m_alloc_message_sent = true;

    // Other messages may be waiting to be sent
    enlist_to_send(); // Throws
}


void Session::send_refresh_message()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_bind_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);
    REALM_ASSERT(!m_access_token_sent);

    const std::string& signed_access_token = get_signed_access_token();
    std::size_t signed_access_token_size = signed_access_token.size();

    logger.debug("Sending: REFRESH(signed_user_token_size=%1)",
                 signed_access_token_size); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    OutputBuffer& out = m_conn.get_output_buffer();
    session_ident_type session_ident = get_ident();
    protocol.make_refresh_message(out, session_ident, signed_access_token); // Throws
    m_conn.initiate_write_message(out, this);                               // Throws

    m_access_token_sent = true;

    // If the IDENT message has not yet been sent, is now ready to be sent if the
    // file identifier pair has become available. If IDENT message has been
    // sent, various other messages may be waiting to be sent, but in that case,
    // we also have the file identifier pair.
    if (have_client_file_ident())
        enlist_to_send(); // Throws
}


void Session::send_unbind_message()
{
    REALM_ASSERT(m_deactivation_initiated || m_error_message_received);
    REALM_ASSERT(m_bind_message_sent);
    REALM_ASSERT(!m_unbind_message_sent);

    logger.debug("Sending: UNBIND"); // Throws

    ClientProtocol& protocol = m_conn.get_client_protocol();
    OutputBuffer& out = m_conn.get_output_buffer();
    session_ident_type session_ident = get_ident();
    protocol.make_unbind_message(out, session_ident); // Throws
    m_conn.initiate_write_message(out, this);         // Throws

    m_unbind_message_sent = true;
}


std::error_code Session::receive_ident_message(SaltedFileIdent client_file_ident)
{
    logger.debug("Received: IDENT(client_file_ident=%1, client_file_ident_salt=%2)", client_file_ident.ident,
                 client_file_ident.salt); // Throws

    // Ignore the message if the deactivation process has been initiated,
    // because in that case, the associated Realm must not be accessed any
    // longer.
    if (m_deactivation_initiated)
        return std::error_code{}; // Success

    bool legal_at_this_time = (m_bind_message_sent && !have_client_file_ident() && !m_error_message_received &&
                               !m_unbound_message_received);
    if (REALM_UNLIKELY(!legal_at_this_time)) {
        logger.error("Illegal message at this time");
        return ClientError::bad_message_order;
    }
    if (REALM_UNLIKELY(client_file_ident.ident < 1)) {
        logger.error("Bad client file identifier in IDENT message");
        return ClientError::bad_client_file_ident;
    }
    if (REALM_UNLIKELY(client_file_ident.salt == 0)) {
        logger.error("Bad client file identifier salt in IDENT message");
        return ClientError::bad_client_file_ident_salt;
    }
    if (REALM_LIKELY(!get_client().is_dry_run())) {
        if (m_state_download_in_progress) {
            REALM_ASSERT(m_client_state_download);
            m_client_state_download->set_salted_file_ident(client_file_ident);
        }
        else {
            ClientHistoryBase& history = access_realm(); // Throws
            bool fix_up_object_ids = true;
            history.set_client_file_ident(client_file_ident, fix_up_object_ids); // Throws
        }
    }

    m_client_file_ident = client_file_ident;
    // Ready to send the STATE_REQUEST, IDENT (or REFRESH) message
    ensure_enlisted_to_send(); // Throws
    return std::error_code{};  // Success
}


void Session::receive_client_version_message(version_type client_version)
{
    logger.debug("Received: CLIENT_VERSION(client_version=%1)", client_version); // Throws

    if (m_deactivation_initiated)
        return;

    if (!m_client_version_request_message_sent) {
        logger.error("Illegal: CLIENT_VERSION message received "
                     "before CLIENT_VERSION_REQUEST message sent."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (m_state_request_message_sent) {
        logger.error("Illegal: CLIENT_VERSION message received after STATE_REQUEST message sent."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (m_ident_message_sent) {
        logger.error("Illegal: CLIENT_VERSION message received after IDENT message sent."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (m_error_message_received) {
        logger.error("Illegal: CLIENT_VERSION message received after ERROR message."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (m_unbound_message_received) {
        logger.error("Illegal: CLIENT_VERSION message received after UNBOUND message."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    REALM_ASSERT(m_client_state_download);

    m_client_state_download->set_client_reset_client_version(client_version);

    enlist_to_send();
}


void Session::receive_state_message(version_type server_version, salt_type server_version_salt,
                                    uint_fast64_t begin_offset, uint_fast64_t end_offset, uint_fast64_t max_offset,
                                    BinaryData chunk)
{
    logger.debug("Received: STATE(server_version=%1, server_version_salt=%2, "
                 "begin_offset=%3, end_offset=%4, max_offset=%5, chunk size=%6)",
                 server_version, server_version_salt, begin_offset, end_offset, max_offset, chunk.size()); // Throws

    if (m_deactivation_initiated)
        return;

    if (!m_state_request_message_sent) {
        logger.error("Illegal: STATE message received before STATE_REQUEST message sent."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (!m_state_download_in_progress) {
        logger.error("Illegal: STATE message received without state download in progress."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (m_ident_message_sent) {
        logger.error("Illegal: STATE message received after IDENT message sent."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (m_error_message_received) {
        logger.error("Illegal: STATE message received after ERROR message."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    if (m_unbound_message_received) {
        logger.error("Illegal: STATE message received after UNBOUND message."); // Throws
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }

    REALM_ASSERT(m_client_state_download);

    bool legal_state_info = m_client_state_download->receive_state(server_version, server_version_salt, begin_offset,
                                                                   end_offset, max_offset, chunk);

    if (REALM_UNLIKELY(!legal_state_info)) {
        logger.error("Illegal state message content");
        m_conn.close_due_to_protocol_error(ClientError::bad_state_message);
        return;
    }

    bool client_reset_is_complete = false;
    realm::VersionID client_reset_old_version;
    realm::VersionID client_reset_new_version;

    if (m_client_state_download->is_complete()) {
        // The State Realm is complete and can be used.
        logger.debug("Async open or client reset is completed, path=%1",
                     get_realm_path()); // Throws
        m_state_download_in_progress = false;

        SaltedFileIdent client_file_ident;
        const ClientHistoryBase& history = access_realm();                           // Throws
        history.get_status(m_last_version_available, client_file_ident, m_progress); // Throws
        REALM_ASSERT(m_client_file_ident.ident == client_file_ident.ident);
        REALM_ASSERT(m_client_file_ident.salt == client_file_ident.salt);
        REALM_ASSERT(m_progress.latest_server_version.version == server_version);
        REALM_ASSERT(m_progress.latest_server_version.salt == server_version_salt);
        REALM_ASSERT(m_progress.download.server_version == server_version);
        REALM_ASSERT(m_progress.download.last_integrated_client_version == 0);
        REALM_ASSERT(m_progress.upload.client_version == 0);
        REALM_ASSERT(m_progress.upload.last_integrated_server_version == 0);
        logger.trace("last_version_available  = %1", m_last_version_available); // Throws

        m_upload_target_version = m_last_version_available;
        m_upload_progress = m_progress.upload;
        REALM_ASSERT(m_last_version_selected_for_upload == 0);
        m_download_progress = {server_version, 0};

        if (m_client_state_download->is_client_reset()) {
            client_reset_is_complete = true;
            client_reset_old_version = m_client_state_download->get_client_reset_old_version();
            client_reset_new_version = m_client_state_download->get_client_reset_new_version();
        }

        m_client_state_download.reset();
        enlist_to_send();
    }

    uint_fast64_t downloaded_bytes = end_offset;
    uint_fast64_t downloadable_bytes = max_offset;
    on_state_download_progress(downloaded_bytes, downloadable_bytes);

    if (client_reset_is_complete && m_sync_transact_reporter) {
        m_sync_transact_reporter->report_sync_transact(client_reset_old_version, client_reset_new_version);
    }
}


void Session::receive_download_message(const SyncProgress& progress, std::uint_fast64_t downloadable_bytes,
                                       const ReceivedChangesets& received_changesets)
{
    logger.debug("Received: DOWNLOAD(download_server_version=%1, download_client_version=%2, "
                 "latest_server_version=%3, latest_server_version_salt=%4, "
                 "upload_client_version=%5, upload_server_version=%6, downloadable_bytes=%7, "
                 "num_changesets=%8, ...)",
                 progress.download.server_version, progress.download.last_integrated_client_version,
                 progress.latest_server_version.version, progress.latest_server_version.salt,
                 progress.upload.client_version, progress.upload.last_integrated_server_version, downloadable_bytes,
                 received_changesets.size()); // Throws

    // Ignore the message if the deactivation process has been initiated,
    // because in that case, the associated Realm must not be accessed any
    // longer.
    if (m_deactivation_initiated)
        return;

    bool legal_at_this_time = (m_ident_message_sent && !m_error_message_received && !m_unbound_message_received);
    if (REALM_UNLIKELY(!legal_at_this_time)) {
        logger.error("Illegal message at this time");
        m_conn.close_due_to_protocol_error(ClientError::bad_message_order);
        return;
    }
    int error_code = 0;
    if (REALM_UNLIKELY(!check_received_sync_progress(progress, error_code))) {
        logger.error("Bad sync progress received (%1)", error_code);
        m_conn.close_due_to_protocol_error(ClientError::bad_progress);
        return;
    }

    version_type server_version = m_progress.download.server_version;
    version_type last_integrated_client_version = m_progress.download.last_integrated_client_version;
    for (const sync::Transformer::RemoteChangeset& changeset : received_changesets) {
        // Check that per-changeset server version is strictly increasing.
        bool good_server_version = (changeset.remote_version > server_version &&
                                    changeset.remote_version <= progress.download.server_version);
        if (!good_server_version) {
            logger.error("Bad server version in changeset header (DOWNLOAD) (%1, %2, %3)", changeset.remote_version,
                         server_version, progress.download.server_version);
            m_conn.close_due_to_protocol_error(ClientError::bad_server_version);
            return;
        }
        server_version = changeset.remote_version;
        // Check that per-changeset last integrated client version is "weakly"
        // increasing.
        bool good_client_version =
            (changeset.last_integrated_local_version >= last_integrated_client_version &&
             changeset.last_integrated_local_version <= progress.download.last_integrated_client_version);
        if (!good_client_version) {
            logger.error("Bad last integrated client version in changeset header (DOWNLOAD) "
                         "(%1, %2, %3)",
                         changeset.last_integrated_local_version, last_integrated_client_version,
                         progress.download.last_integrated_client_version);
            m_conn.close_due_to_protocol_error(ClientError::bad_client_version);
            return;
        }
        last_integrated_client_version = changeset.last_integrated_local_version;
        // Server shouldn't send our own changes, and zero is not a valid client
        // file identifier.
        bool good_file_ident =
            (changeset.origin_file_ident > 0 && changeset.origin_file_ident != m_client_file_ident.ident);
        if (!good_file_ident) {
            logger.error("Bad origin file identifier");
            m_conn.close_due_to_protocol_error(ClientError::bad_origin_file_ident);
            return;
        }
    }

    update_progress(progress); // Throws

    initiate_integrate_changesets(downloadable_bytes, received_changesets); // Throws
}


std::error_code Session::receive_mark_message(request_ident_type request_ident)
{
    logger.debug("Received: MARK(request_ident=%1)", request_ident); // Throws

    // Ignore the message if the deactivation process has been initiated,
    // because in that case, the associated Realm must not be accessed any
    // longer.
    if (m_deactivation_initiated)
        return std::error_code{}; // Success

    bool legal_at_this_time = (m_ident_message_sent && !m_error_message_received && !m_unbound_message_received);
    if (REALM_UNLIKELY(!legal_at_this_time)) {
        logger.error("Illegal message at this time");
        return ClientError::bad_message_order;
    }
    bool good_request_ident =
        (request_ident <= m_last_download_mark_sent && request_ident > m_last_download_mark_received);
    if (REALM_UNLIKELY(!good_request_ident)) {
        logger.error("Bad request identifier in MARK message");
        return ClientError::bad_request_ident;
    }

    m_server_version_at_last_download_mark = m_progress.download.server_version;
    m_last_download_mark_received = request_ident;
    check_for_download_completion(); // Throws

    return std::error_code{}; // Success
}


std::error_code Session::receive_alloc_message(file_ident_type file_ident)
{
    logger.debug("Received: ALLOC(file_ident=%1)", file_ident); // Throws

    // Ignore the message if the deactivation process has been initiated,
    // because in that case, the associated Realm must not be accessed any
    // longer.
    if (m_deactivation_initiated)
        return std::error_code{}; // Success

    bool legal_at_this_time = (m_alloc_message_sent && !m_error_message_received && !m_unbound_message_received);
    if (REALM_UNLIKELY(!legal_at_this_time)) {
        logger.error("Illegal message at this time");
        return ClientError::bad_message_order;
    }
    if (REALM_UNLIKELY(file_ident < 1)) {
        logger.error("Bad file identifier in ALLOC message");
        return ClientError::bad_file_ident;
    }

    REALM_ASSERT(have_client_file_ident());
    REALM_ASSERT(m_ident_message_sent);
    REALM_ASSERT(m_num_outstanding_subtier_allocations > 0);
    if (REALM_UNLIKELY(!on_subtier_file_ident(file_ident))) { // Throws
        logger.error("Bad file identifier in ALLOC message");
        return ClientError::bad_file_ident;
    }
    m_alloc_message_sent = false;
    --m_num_outstanding_subtier_allocations;
    // Ready to send the next ALLOC request message
    if (m_num_outstanding_subtier_allocations > 0)
        ensure_enlisted_to_send(); // Throws
    return std::error_code{};      // Success
}


// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
std::error_code Session::receive_unbound_message()
{
    logger.debug("Received: UNBOUND");

    bool legal_at_this_time = (m_unbind_message_sent && !m_error_message_received && !m_unbound_message_received);
    if (REALM_UNLIKELY(!legal_at_this_time)) {
        logger.error("Illegal message at this time");
        return ClientError::bad_message_order;
    }

    // The fact that the UNBIND message has been sent, but an ERROR message has
    // not been received, implies that the deactivation process must have been
    // initiated, so this session must be in the Deactivating state.
    REALM_ASSERT(m_active_or_deactivating);
    REALM_ASSERT(m_deactivation_initiated);

    m_unbound_message_received = true;

    // Detect completion of the unbinding process
    if (m_unbind_message_sent_2) {
        // The deactivation process completes when the unbinding process
        // completes.
        complete_deactivation(); // Throws
        // Life cycle state is now Deactivated
    }

    return std::error_code{}; // Success
}


// The caller (Connection) must discard the session if the session has become
// deactivated upon return.
std::error_code Session::receive_error_message(int error_code, StringData message, bool try_again)
{
    logger.info("Received: ERROR \"%1\" (error_code=%2, try_again=%3)", message, error_code, try_again); // Throws

    bool legal_at_this_time = (m_bind_message_sent && !m_error_message_received && !m_unbound_message_received);
    if (REALM_UNLIKELY(!legal_at_this_time)) {
        logger.error("Illegal message at this time");
        return ClientError::bad_message_order;
    }

    bool known_error_code = bool(sync::get_protocol_error_message(error_code));
    if (REALM_UNLIKELY(!known_error_code)) {
        logger.error("Unknown error code"); // Throws
        return ClientError::bad_error_code;
    }
    ProtocolError error_code_2 = ProtocolError(error_code);
    if (REALM_UNLIKELY(!is_session_level_error(error_code_2))) {
        logger.error("Not a session level error code"); // Throws
        return ClientError::bad_error_code;
    }

    REALM_ASSERT(!m_suspended);
    REALM_ASSERT(m_active_or_deactivating);

    logger.debug("Suspended"); // Throws

    m_error_message_received = true;
    m_suspended = true;

    // Detect completion of the unbinding process
    if (m_unbind_message_sent_2) {
        // The fact that the UNBIND message has been sent, but an ERROR message
        // has not been received, implies that the deactivation process must
        // have been initiated, so this session must be in the Deactivating
        // state.
        REALM_ASSERT(m_deactivation_initiated);

        // The deactivation process completes when the unbinding process
        // completes.
        complete_deactivation(); // Throws
        // Life cycle state is now Deactivated
        return std::error_code{}; // Success
    }

    // Notify the application of the suspension of the session if the session is
    // still in the Active state
    if (!m_deactivation_initiated) {
        // Life cycle state is Active
        m_conn.one_less_active_unsuspended_session(); // Throws
        std::error_code ec = make_error_code(error_code_2);
        bool is_fatal = !try_again;
        on_suspended(ec, message, is_fatal); // Throws
    }

    // FIXME: If `try_again` is true, find a way to automatically resume the session after a delay.

    // Ready to send the UNBIND message, if it has not been sent already
    if (!m_unbind_message_sent)
        ensure_enlisted_to_send(); // Throws

    return std::error_code{}; // Success
}


void Session::update_progress(const SyncProgress& progress)
{
    REALM_ASSERT(check_received_sync_progress(progress));

    bool upload_progressed = (progress.upload.client_version > m_progress.upload.client_version);

    m_progress = progress;

    if (upload_progressed) {
        if (progress.upload.client_version > m_last_version_selected_for_upload) {
            if (progress.upload.client_version > m_upload_progress.client_version)
                m_upload_progress = progress.upload;
            m_last_version_selected_for_upload = progress.upload.client_version;
        }
        if (m_upload_completion_notification_requested)
            check_for_upload_completion(); // Throws
    }
}


bool ClientImplBase::Session::check_received_sync_progress(const SyncProgress& progress, int& error_code) noexcept
{
    const SyncProgress& a = m_progress;
    const SyncProgress& b = progress;
    // Latest server version must be weakly increasing throughout a session.
    if (b.latest_server_version.version < a.latest_server_version.version) {
        error_code = 1;
        return false;
    }
    // Last integrated client version on server must be weakly increasing
    // throughout a session.
    if (b.upload.client_version < a.upload.client_version) {
        error_code = 2;
        return false;
    }
    // Last integrated client version on server cannot be greater than the
    // latest client version in existence.
    if (b.upload.client_version > m_last_version_available) {
        error_code = 3;
        return false;
    }
    // Download cursor must be weakly increasing throughout a session
    if (b.download.server_version < a.download.server_version) {
        error_code = 4;
        return false;
    }
    // Download cursor cannot be greater than the latest server version in
    // existence.
    if (b.download.server_version > b.latest_server_version.version) {
        error_code = 5;
        return false;
    }
    // The last integrated client version on the server at the position in the
    // server's history of the download cursor must be weakly increasing
    // throughout a session.
    if (b.download.last_integrated_client_version < a.download.last_integrated_client_version) {
        error_code = 6;
        return false;
    }
    // The last integrated client version on the server at the position in the
    // server's history of the download cursor cannot be greater than the latest
    // client version integrated on the server.
    if (b.download.last_integrated_client_version > b.upload.client_version) {
        error_code = 7;
        return false;
    }
    return true;
}


void Session::check_for_upload_completion()
{
    REALM_ASSERT(!m_deactivation_initiated);
    REALM_ASSERT(m_upload_completion_notification_requested);

    if (m_state_download_in_progress)
        return;

    // Upload process must have reached end of history
    REALM_ASSERT(m_upload_progress.client_version <= m_last_version_available);
    bool scan_complete = (m_upload_progress.client_version == m_last_version_available);
    if (!scan_complete)
        return;

    // All uploaded changesets must have been acknowledged by the server
    REALM_ASSERT(m_progress.upload.client_version <= m_last_version_selected_for_upload);
    bool all_uploads_accepted = (m_progress.upload.client_version == m_last_version_selected_for_upload);
    if (!all_uploads_accepted)
        return;

    m_upload_completion_notification_requested = false;
    on_upload_completion(); // Throws
}


void Session::check_for_download_completion()
{
    REALM_ASSERT(m_target_download_mark >= m_last_download_mark_received);
    REALM_ASSERT(m_last_download_mark_received >= m_last_triggering_download_mark);
    if (m_last_download_mark_received == m_last_triggering_download_mark)
        return;
    if (m_last_download_mark_received < m_target_download_mark)
        return;
    if (m_download_progress.server_version < m_server_version_at_last_download_mark)
        return;
    m_last_triggering_download_mark = m_target_download_mark;
    if (REALM_UNLIKELY(!m_allow_upload)) {
        // Activate the upload process now, and enable immediate reactivation
        // after a subsequent fast reconnect.
        m_allow_upload = true;
        ensure_enlisted_to_send(); // Throws
    }
    on_download_completion(); // Throws
}
