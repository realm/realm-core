/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_TEST_CLIENT_PEER_HPP
#define REALM_TEST_CLIENT_PEER_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <map>
#include <string>
#include <system_error>
#include <mutex>
#include <random>

#include <realm/util/optional.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/network.hpp>
#include <realm/sync/impl/clock.hpp>
#include <realm/db.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/client.hpp>

#include "auth.hpp"
#include "metrics.hpp"


namespace realm {
namespace test_client {

/// All peers share a single sync Client object.
///
/// The Client::run() of the passed client object must not be executing when
/// Peer objects are destroyed.
class Peer {
public:
    class Context;
    enum class Error;
    using ProtocolEnvelope = sync::ProtocolEnvelope;
    using port_type = sync::Client::port_type;
    using milliseconds_type = sync::milliseconds_type;
    using BindCompletionHandler = void();
    using LevelDistr = std::uniform_int_distribution<int>;

    Peer(Context&, std::string http_request_path, std::string realm_path, util::Logger&,
         std::int_fast64_t originator_ident, bool verify_ssl_cert,
         util::Optional<std::string> ssl_trust_certificate_path,
         util::Optional<sync::Session::Config::ClientReset> client_reset_config,
         std::function<void(bool is_fatal)> on_sync_error);

    sync::Session& get_session() noexcept;

    void prepare_receive_ptime_requests();

    /// If a refresh token is specified (nonempty), the access token will be
    /// periodically refreshed.
    void bind(ProtocolEnvelope protocol, const std::string& address, port_type port, const std::string& realm_name,
              const std::string& access_token, const std::string& refresh_token);

    struct TransactSpec {
        int num_blobs = 0;
        StringData blob_label;
        int blob_kind = 0;
        LevelDistr blob_level_distr{0, 0};
        bool replace_blobs = false;
        bool send_ptime_request = false;
    };

    void perform_transaction(BinaryData blob, TransactSpec&);

    void enable_receive_ptime_requests();

    void ensure_blob_class();

    void ensure_ptime_class();

    void ensure_query_class(const std::string& class_name);

    void generate_queryable(const std::string& class_name, int n, LevelDistr level_distr,
                            const std::string& queryable_text);

    void add_query(const std::string& class_name, const std::string& query);

    void dump_result_sets();

private:
    using ConnectionState = sync::ConnectionState;

    Context& m_context;
    const std::string m_realm_path;
    util::Logger& m_logger;
    const std::int_fast64_t m_originator_ident;
    const std::function<void(bool is_fatal)> m_on_sync_error;
    sync::Session m_session;
    std::unique_ptr<Replication> m_history;
    DBRef m_shared_group;
    std::unique_ptr<Replication> m_receive_history;
    DBRef m_receive_shared_group;
    TransactionRef m_receive_group = nullptr;
    std::string m_refresh_token;
    util::Optional<util::network::DeadlineTimer> m_access_token_refresh_timer;
    std::atomic<bool> m_receive_enabled{false}; // Release-Acquire ordering
    milliseconds_type m_start_time = 0;
    ConnectionState m_connection_state = ConnectionState::disconnected;
    bool m_session_is_bound = false;
    bool m_error_seen = false;
    bool m_fatal_error_seen = false;

    void open_realm();

    void open_realm_for_receive();

    void receive(VersionID new_version);

    void intitiate_access_token_refresh_wait();
    void refresh_access_token();

    void do_send_ptime_request(WriteTransaction&);
    TableRef do_ensure_blob_class(WriteTransaction&, StringData table_name);
    TableRef do_ensure_ptime_class(WriteTransaction&);
};


class Peer::Context {
public:
    sync::Client& client;
    sync::auth::Client& auth;
    util::network::Service& test_proc_service;
    std::mt19937_64& test_proc_random;
    Metrics& metrics;
    const bool disable_sync_to_disk;
    const bool report_roundtrip_times;
    const bool reset_on_reconnect;

    Context(sync::Client&, sync::auth::Client&, util::network::Service&, std::mt19937_64&, Metrics&, bool dstd,
            bool rrt, bool ror) noexcept;

    void init_metrics_gauges(bool report_propagation_time);
    void on_new_session();
    void on_session_connection_state_change(ConnectionState old_state, ConnectionState new_state);
    void on_first_session_error();
    void on_first_fatal_session_error();
    void on_error(Error, bool is_fatal);

    void add_roundtrip_time(milliseconds_type time);
    void add_propagation_time(milliseconds_type time);

private:
    int m_num_sessions = 0;
    int m_num_sessions_connecting = 0;
    int m_num_sessions_connected = 0;
    int m_num_sessions_with_error = 0;
    int m_num_sessions_with_fatal_error = 0;
    int m_num_errors = 0;
    int m_num_fatal_errors = 0;

    std::mutex m_metrics_aggregation_mutex;
    std::vector<milliseconds_type> m_roundtrip_times;   // Protected by m_metrics_aggregation_mutex
    std::vector<milliseconds_type> m_propagation_times; // Protected by m_metrics_aggregation_mutex

    util::network::DeadlineTimer m_metrics_aggregation_timer{test_proc_service};

    std::map<Error, int> m_error_counters;

    void sched_metrics_aggregation_flush();
    void metrics_aggregation_flush();
};


// Implementation

enum class Peer::Error {
    system_connection_reset,
    system_broken_pipe,
    system_connect_timeout,
    system_host_unreachable,
    system_other,
    network_end_of_input,
    network_premature_end_of_input,
    network_host_not_found,
    network_other,
    ssl,
    websocket_malformed_response,
    websocket_3xx,
    websocket_4xx,
    websocket_5xx,
    websocket_other,
    client_pong_timeout,
    client_connect_timeout,
    client_other,
    protocol_connection,
    protocol_session,
    unexpected_category
};

inline sync::Session& Peer::get_session() noexcept
{
    return m_session;
}

inline Peer::Context::Context(sync::Client& c, sync::auth::Client& a, util::network::Service& tps,
                              std::mt19937_64& tpr, Metrics& m, bool dstd, bool rrt, bool ror) noexcept
    : client{c}
    , auth{a}
    , test_proc_service{tps}
    , test_proc_random{tpr}
    , metrics{m}
    , disable_sync_to_disk{dstd}
    , report_roundtrip_times{rrt}
    , reset_on_reconnect{ror}
{
}

} // namespace test_client
} // namespace realm

#endif // REALM_TEST_CLIENT_PEER_HPP
