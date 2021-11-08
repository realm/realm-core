#include <cstring>
#include <vector>

#include <realm/util/features.h>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/network.hpp>
#include <realm/util/network_ssl.hpp>
#include <realm/util/websocket.hpp>
#include <realm/list.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/history.hpp>

#include "statistics.hpp"
#include "object_observer.hpp"
#include "peer.hpp"

using namespace realm;
using namespace realm::test_client;

using version_type = sync::version_type;


namespace {

std::string g_blob_class_name = "Blob";
std::string g_ptime_class_name = "PropagationTime";
std::string g_result_sets_class_name = "__ResultSets";

std::string& class_to_table_name(const std::string& class_name, std::string& buffer)
{
    buffer.assign("class_");   // Throws
    buffer.append(class_name); // Throws
    return buffer;
}

std::string& class_to_matches_column_name(const std::string& class_name, std::string& buffer)
{
    buffer.assign(class_name); // Throws
    buffer.append("_matches"); // Throws
    return buffer;
}

const char* get_result_set_status_text(int status)
{
    switch (status) {
        case 0:
            return "Uninitialized";
        case 1:
            return "Initialized";
        case -1:
            return "Query parsing failed";
    }
    return "(unexpected value)";
}

Peer::Error map_error(std::error_code ec) noexcept
{
    const std::error_category& category = ec.category();
    if (category == util::misc_ext_error_category) {
        using util::MiscExtErrors;
        switch (MiscExtErrors(ec.value())) {
            case MiscExtErrors::end_of_input:
                return Peer::Error::network_end_of_input;
            case MiscExtErrors::premature_end_of_input:
                return Peer::Error::network_premature_end_of_input;
            case MiscExtErrors::delim_not_found:
            case MiscExtErrors::operation_not_supported:
                break;
        }
        return Peer::Error::network_other;
    }
    const char* category_name = category.name();
    if (std::strcmp(category_name, "realm.basic_system") == 0) {
        switch (ec.value()) {
            case ECONNRESET:
                return Peer::Error::system_connection_reset;
            case EPIPE:
                return Peer::Error::system_broken_pipe;
            case ETIMEDOUT:
                return Peer::Error::system_connect_timeout;
            case EHOSTUNREACH:
                return Peer::Error::system_host_unreachable;
        }
        return Peer::Error::system_other;
    }
    if (category == util::network::resolve_error_category) {
        using util::network::ResolveErrors;
        switch (ResolveErrors(ec.value())) {
            case ResolveErrors::host_not_found:
            case ResolveErrors::host_not_found_try_again:
                return Peer::Error::network_host_not_found;
            case ResolveErrors::no_data:
            case ResolveErrors::no_recovery:
            case ResolveErrors::service_not_found:
            case ResolveErrors::socket_type_not_supported:
                break;
        }
        return Peer::Error::network_other;
    }
    bool is_ssl_related =
        (category == util::network::ssl::error_category || category == util::network::openssl_error_category ||
         category == util::network::secure_transport_error_category);
    if (is_ssl_related)
        return Peer::Error::ssl;
    if (std::strcmp(category_name, "realm::util::websocket::Error") == 0) {
        switch (util::websocket::Error(ec.value())) {
            case util::websocket::Error::bad_request_malformed_http:
            case util::websocket::Error::bad_request_header_upgrade:
            case util::websocket::Error::bad_request_header_connection:
            case util::websocket::Error::bad_request_header_websocket_version:
            case util::websocket::Error::bad_request_header_websocket_key:
                break;
            case util::websocket::Error::bad_response_invalid_http:
                return Peer::Error::websocket_malformed_response;
            case util::websocket::Error::bad_response_2xx_successful:
            case util::websocket::Error::bad_response_200_ok:
                break;
            case util::websocket::Error::bad_response_3xx_redirection:
            case util::websocket::Error::bad_response_301_moved_permanently:
                return Peer::Error::websocket_3xx;
            case util::websocket::Error::bad_response_4xx_client_errors:
            case util::websocket::Error::bad_response_401_unauthorized:
            case util::websocket::Error::bad_response_403_forbidden:
            case util::websocket::Error::bad_response_404_not_found:
            case util::websocket::Error::bad_response_410_gone:
                return Peer::Error::websocket_4xx;
            case util::websocket::Error::bad_response_5xx_server_error:
            case util::websocket::Error::bad_response_500_internal_server_error:
            case util::websocket::Error::bad_response_502_bad_gateway:
            case util::websocket::Error::bad_response_503_service_unavailable:
            case util::websocket::Error::bad_response_504_gateway_timeout:
                return Peer::Error::websocket_5xx;
            case util::websocket::Error::bad_response_unexpected_status_code:
            case util::websocket::Error::bad_response_header_protocol_violation:
            case util::websocket::Error::bad_message:
                break;
        }
        return Peer::Error::websocket_other;
    }
    if (std::strcmp(category_name, "realm::sync::Client::Error") == 0) {
        switch (sync::Client::Error(ec.value())) {
            case sync::Client::Error::connection_closed:
            case sync::Client::Error::unknown_message:
            case sync::Client::Error::bad_syntax:
            case sync::Client::Error::limits_exceeded:
            case sync::Client::Error::bad_session_ident:
            case sync::Client::Error::bad_message_order:
            case sync::Client::Error::bad_client_file_ident:
            case sync::Client::Error::bad_progress:
            case sync::Client::Error::bad_changeset_header_syntax:
            case sync::Client::Error::bad_changeset_size:
            case sync::Client::Error::bad_origin_file_ident:
            case sync::Client::Error::bad_server_version:
            case sync::Client::Error::bad_changeset:
            case sync::Client::Error::bad_request_ident:
            case sync::Client::Error::bad_error_code:
            case sync::Client::Error::bad_compression:
            case sync::Client::Error::bad_client_version:
            case sync::Client::Error::ssl_server_cert_rejected:
            case sync::Client::Error::bad_timestamp:
            case sync::Client::Error::bad_state_message:
            case sync::Client::Error::bad_client_file_ident_salt:
            case sync::Client::Error::bad_file_ident:
            case sync::Client::Error::bad_protocol_from_server:
            case sync::Client::Error::client_too_old_for_server:
            case sync::Client::Error::client_too_new_for_server:
            case sync::Client::Error::protocol_mismatch:
            case sync::Client::Error::missing_protocol_feature:
            case sync::Client::Error::http_tunnel_failed:
                break;
            case sync::Client::Error::pong_timeout:
                return Peer::Error::client_pong_timeout;
            case sync::Client::Error::connect_timeout:
                return Peer::Error::client_connect_timeout;
        }
        return Peer::Error::client_other;
    }
    if (std::strcmp(category_name, "realm::sync::ProtocolError") == 0) {
        if (sync::is_session_level_error(sync::ProtocolError(ec.value())))
            return Peer::Error::protocol_session;
        return Peer::Error::protocol_connection;
    }
    return Peer::Error::unexpected_category;
}

const char* get_error_metric(Peer::Error error)
{
    switch (error) {
        case Peer::Error::system_connection_reset:
            return "client.errors_system_connection_reset";
        case Peer::Error::system_broken_pipe:
            return "client.errors_system_proken_pipe";
        case Peer::Error::system_connect_timeout:
            return "client.errors_system_connect_timeout";
        case Peer::Error::system_host_unreachable:
            return "client.errors_system_host_unreachable";
        case Peer::Error::system_other:
            return "client.errors_system_other";
        case Peer::Error::network_end_of_input:
            return "client.errors_network_end_of_input";
        case Peer::Error::network_premature_end_of_input:
            return "client.errors_network_premature_end_of_input";
        case Peer::Error::network_host_not_found:
            return "client.errors_network_host_not_found";
        case Peer::Error::network_other:
            return "client.errors_network_other";
        case Peer::Error::ssl:
            return "client.errors_ssl";
        case Peer::Error::websocket_malformed_response:
            return "client.errors_websocket_malformed_response";
        case Peer::Error::websocket_3xx:
            return "client.errors_websocket_3xx";
        case Peer::Error::websocket_4xx:
            return "client.errors_websocket_4xx";
        case Peer::Error::websocket_5xx:
            return "client.errors_websocket_5xx";
        case Peer::Error::websocket_other:
            return "client.errors_websocket_other";
        case Peer::Error::client_pong_timeout:
            return "client.errors_client_pong_timeout";
        case Peer::Error::client_connect_timeout:
            return "client.errors_client_connect_timeout";
        case Peer::Error::client_other:
            return "client.errors_client_other";
        case Peer::Error::protocol_connection:
            return "client.errors_protocol_connection";
        case Peer::Error::protocol_session:
            return "client.errors_protocol_session";
        case Peer::Error::unexpected_category:
            return "client.errors_unexpected_category";
    }
    return nullptr;
}

} // unnamed namespace


Peer::Peer(Context& context, std::string http_request_path, std::string realm_path, util::Logger& logger,
           std::int_fast64_t originator_ident, bool verify_ssl_cert,
           util::Optional<std::string> ssl_trust_certificate_path,
           util::Optional<sync::Session::Config::ClientReset> client_reset_config,
           std::function<void(bool is_fatal)> on_sync_error)
    : m_context{context}
    , m_realm_path{std::move(realm_path)}
    , m_logger{logger}
    , m_originator_ident{originator_ident}
    , m_on_sync_error{std::move(on_sync_error)}
{
    using ErrorInfo = sync::Session::ErrorInfo;
    auto listener = [this](ConnectionState state, const ErrorInfo* error_info) {
        m_context.on_session_connection_state_change(m_connection_state, state);
        m_connection_state = state;
        if (state == ConnectionState::disconnected) {
            REALM_ASSERT(error_info);
            std::error_code error_code = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            Error error = map_error(error_code);
            m_context.on_error(error, is_fatal);
            if (!m_error_seen) {
                m_context.on_first_session_error();
                m_error_seen = true;
            }
            if (is_fatal && !m_fatal_error_seen) {
                m_context.on_first_fatal_session_error();
                m_fatal_error_seen = true;
            }
            if (error_code == sync::ProtocolError::token_expired) {
                m_access_token_refresh_timer = util::none;
                // FIXME: This scheme is prone to cause, or contribute to server
                // hammering. Ideally, the client should manage the
                // authentication protocol internally, and use the same level of
                // hammering protection as is used for the sync protocol.
                refresh_access_token();
                is_fatal = false;
            }
            const std::string& detailed_message = error_info->detailed_message;
            using Level = util::Logger::Level;
            Level level = (is_fatal ? Level::fatal : Level::error);
            m_logger.log(level, "%1 (error_code=%2)", detailed_message, error_code);
            if (m_on_sync_error)
                m_on_sync_error(is_fatal);
        }
        else if (state == ConnectionState::connected) {
            if (m_context.reset_on_reconnect)
                m_start_time = _impl::realtime_clock_now();
        }
    };

    sync::Session::Config session_config;
    session_config.verify_servers_ssl_certificate = verify_ssl_cert;
    session_config.ssl_trust_certificate_path = ssl_trust_certificate_path;
    session_config.client_reset_config = client_reset_config;
    session_config.service_identifier = http_request_path;
    m_session = sync::Session{m_context.client, m_realm_path, session_config};
    m_session.set_connection_state_change_listener(listener);
}


void Peer::prepare_receive_ptime_requests()
{
    open_realm_for_receive();
    std::string buffer;
    auto callback = [this](VersionID, VersionID new_version) {
        if (m_receive_enabled.load(std::memory_order_acquire))
            receive(new_version);
    };
    m_session.set_sync_transact_callback(std::move(callback));
    m_start_time = _impl::realtime_clock_now();
}


void Peer::bind(ProtocolEnvelope protocol, const std::string& address, port_type port, const std::string& realm_name,
                const std::string& access_token, const std::string& refresh_token)
{
    m_refresh_token = refresh_token;
    if (!m_refresh_token.empty())
        intitiate_access_token_refresh_wait();
    m_context.on_new_session();
    m_session.bind(address, realm_name, access_token, port, protocol);
    m_session_is_bound = true;
}


void Peer::perform_transaction(BinaryData blob, TransactSpec& spec)
{
    open_realm();
    std::string buffer;
    const std::string& table_name = class_to_table_name(g_blob_class_name, buffer);
    version_type new_version;
    {
        WriteTransaction wt{m_shared_group};
        if (spec.num_blobs > 0) {
            TableRef table = do_ensure_blob_class(wt, table_name);
            ColKey col_key_blob = table->get_column_key("blob");
            ColKey col_key_label = table->get_column_key("label");
            ColKey col_key_kind = table->get_column_key("kind");
            ColKey col_key_level = table->get_column_key("level");
            std::size_t num_blobs = 0;
            if (spec.num_blobs >= 0) {
                if (util::int_cast_with_overflow_detect(spec.num_blobs, num_blobs))
                    throw std::overflow_error("Number of blobs");
            }
            std::vector<Obj> objects;
            if (spec.replace_blobs) {
                std::size_t n = table->size();
                if (num_blobs < n)
                    n = num_blobs;
                for (std::size_t i = 0; i < n; ++i) {
                    Obj obj = table->get_object(i);
                    objects.push_back(std::move(obj));
                }
            }
            std::size_t n = num_blobs - objects.size();
            for (std::size_t i = 0; i < n; ++i) {
                ObjectId pkey = ObjectId::gen(); // Globally unique ???
                Obj obj = table->create_object_with_primary_key(Mixed(pkey));
                objects.push_back(std::move(obj));
            }
            BinaryData blob_2 = blob;
            if (blob_2.is_null())
                blob_2 = BinaryData("", 0); // Due to quirky behavior of Table::set_binary()
            for (Obj obj : objects) {
                obj.set(col_key_blob, blob_2);
                obj.set(col_key_label, spec.blob_label);
                obj.set(col_key_kind, spec.blob_kind);
                int level = spec.blob_level_distr(m_context.test_proc_random);
                obj.set(col_key_level, level);
            }
        }
        if (spec.send_ptime_request)
            do_send_ptime_request(wt);
        new_version = wt.commit();
    }
    if (m_session_is_bound)
        m_session.nonsync_transact_notify(new_version);
    if (spec.send_ptime_request)
        m_context.metrics.increment("client.ptime_request_sent");
}


void Peer::enable_receive_ptime_requests()
{
    m_start_time = _impl::realtime_clock_now();
    m_receive_enabled.store(true, std::memory_order_release);
}


void Peer::ensure_blob_class()
{
    open_realm();
    version_type new_version;
    {
        std::string buffer;
        const std::string& table_name = class_to_table_name(g_blob_class_name, buffer);
        WriteTransaction wt{m_shared_group};
        do_ensure_blob_class(wt, table_name);
        new_version = wt.commit();
    }
    if (m_session_is_bound)
        m_session.nonsync_transact_notify(new_version);
}


void Peer::ensure_ptime_class()
{
    open_realm();
    version_type new_version;
    {
        WriteTransaction wt{m_shared_group};
        do_ensure_ptime_class(wt);
        new_version = wt.commit();
    }
    if (m_session_is_bound)
        m_session.nonsync_transact_notify(new_version);
}


void Peer::ensure_query_class(const std::string& class_name)
{
    open_realm();
    std::string buffer;
    const std::string& table_name = class_to_table_name(class_name, buffer);
    version_type new_version;
    {
        WriteTransaction wt{m_shared_group};
        TableRef queryable = wt.get_table(table_name);
        if (!queryable)
            queryable = sync::create_table(wt, table_name);
        ColKey level_ndx = queryable->get_column_key("level");
        if (!level_ndx) {
            queryable->add_column(type_Int, "level");
        }
        else if (queryable->get_column_type(level_ndx) != type_Int) {
            m_logger.error("Wrong data type for property 'level' in queryable class '%1'", class_name);
            return;
        }
        ColKey text_ndx = queryable->get_column_key("text");
        if (!text_ndx) {
            queryable->add_column(type_String, "text");
        }
        else if (queryable->get_column_type(text_ndx) != type_String) {
            m_logger.error("Wrong data type for property 'text' in queryable class '%1'", class_name);
            return;
        }
        new_version = wt.commit();
    }
    if (m_session_is_bound)
        m_session.nonsync_transact_notify(new_version);
}


void Peer::generate_queryable(const std::string& class_name, int n, LevelDistr level_distr, const std::string& text)
{
    open_realm();
    std::string buffer;
    const std::string& table_name = class_to_table_name(class_name, buffer);
    version_type new_version;
    {
        WriteTransaction wt{m_shared_group};
        TableRef queryable = wt.get_table(table_name);
        if (!queryable) {
            m_logger.error("Queryable class '%1' not found", class_name);
            return;
        }
        ColKey level_ndx = queryable->get_column_key("level");
        if (!level_ndx) {
            m_logger.error("Property 'level' not found in queryable class '%1'", class_name);
            return;
        }
        if (queryable->get_column_type(level_ndx) != type_Int) {
            m_logger.error("Wrong type of property 'level' in queryable class '%1'", class_name);
            return;
        }
        ColKey text_ndx = queryable->get_column_key("text");
        if (!text_ndx) {
            m_logger.error("Property 'text' not found in queryable class '%1'", class_name);
            return;
        }
        if (queryable->get_column_type(text_ndx) != type_String) {
            m_logger.error("Wrong type of property `text` in queryable class '%1'", class_name);
            return;
        }
        for (int i = 0; i < n; ++i) {
            int level_2 = level_distr(m_context.test_proc_random);
            queryable->create_object().set(level_ndx, level_2).set(text_ndx, text);
        }
        new_version = wt.commit();
    }
    if (m_session_is_bound)
        m_session.nonsync_transact_notify(new_version);
}


void Peer::add_query(const std::string& class_name, const std::string& query)
{
    open_realm();
    version_type new_version;
    {
        WriteTransaction wt{m_shared_group};
        std::string buffer;
        TableRef queryable = wt.get_table(class_to_table_name(class_name, buffer));
        if (!queryable) {
            m_logger.error("Query target class '%1' not found", class_name);
            return;
        }
        const std::string& result_sets_table_name = class_to_table_name(g_result_sets_class_name, buffer);
        TableRef result_sets = wt.get_table(result_sets_table_name);
        if (!result_sets) {
            result_sets = sync::create_table(wt, result_sets_table_name);
            result_sets->add_column(type_String, "query");
            result_sets->add_column(type_String, "matches_property");
            // 0 = uninitialized, 1 = initialized, -1 = query parsing failed
            result_sets->add_column(type_Int, "status");
            result_sets->add_column(type_String, "error_message");
            result_sets->add_column(type_Int, "query_parse_counter");
        }
        const std::string& matches_column_name = class_to_matches_column_name(class_name, buffer);
        ColKey col_ndx_matches = result_sets->get_column_key(matches_column_name);
        if (!col_ndx_matches) {
            result_sets->add_column_list(*queryable, matches_column_name);
        }
        else {
            if (result_sets->get_column_type(col_ndx_matches) != type_LinkList) {
                m_logger.error("Matches column '%1' of result sets table has wrong type", matches_column_name);
                return;
            }
            if (result_sets->get_link_target(col_ndx_matches) != queryable) {
                m_logger.error("Matches column '%1' of result sets table has wrong target table",
                               matches_column_name);
            }
        }
        ColKey col_ndx_query = result_sets->get_column_key("query");
        ColKey col_ndx_matches_property = result_sets->get_column_key("matches_property");
        result_sets->create_object().set(col_ndx_query, query).set(col_ndx_matches_property, matches_column_name);
        new_version = wt.commit();
    }
    if (m_session_is_bound)
        m_session.nonsync_transact_notify(new_version);
}


void Peer::dump_result_sets()
{
    open_realm();
    std::string buffer;
    const std::string& result_sets_table_name = class_to_table_name(g_result_sets_class_name, buffer);
    ReadTransaction wt{m_shared_group};
    ConstTableRef result_sets = wt.get_table(result_sets_table_name);
    if (!result_sets) {
        m_logger.error("dump_result_sets(): Table '%1' missing", result_sets_table_name);
        return;
    }
    ColKey col_ndx_oid = result_sets->get_column_key("!OID");
    if (!col_ndx_oid) {
        m_logger.error("dump_result_sets(): Column '!OID' not found in table '%1'", result_sets_table_name);
        return;
    }
    ColKey col_ndx_matches_property = result_sets->get_column_key("matches_property");
    if (!col_ndx_matches_property) {
        m_logger.error("dump_result_sets(): Column 'matches_property' not found in table '%1'",
                       result_sets_table_name);
        return;
    }
    if (result_sets->get_column_type(col_ndx_matches_property) != type_String) {
        m_logger.error("dump_result_sets(): Wrong data type for column 'matches_property' "
                       "in table '%1'",
                       result_sets_table_name);
        return;
    }
    ColKey col_ndx_query = result_sets->get_column_key("query");
    if (!col_ndx_query) {
        m_logger.error("dump_result_sets(): Column 'query' not found in table '%1'", result_sets_table_name);
        return;
    }
    if (result_sets->get_column_type(col_ndx_query) != type_String) {
        m_logger.error("dump_result_sets(): Wrong data type for column 'query' in table '%1'",
                       result_sets_table_name);
        return;
    }
    ColKey col_ndx_status = result_sets->get_column_key("status");
    if (!col_ndx_status) {
        m_logger.error("dump_result_sets(): Column 'status' not found in table '%1'", result_sets_table_name);
        return;
    }
    if (result_sets->get_column_type(col_ndx_status) != type_Int) {
        m_logger.error("dump_result_sets(): Wrong data type for column 'status' in table '%1'",
                       result_sets_table_name);
        return;
    }
    ColKey col_ndx_error_message = result_sets->get_column_key("error_message");
    if (!col_ndx_error_message) {
        m_logger.error("dump_result_sets(): Column 'error_message' not found in table '%1'", result_sets_table_name);
        return;
    }
    if (result_sets->get_column_type(col_ndx_error_message) != type_String) {
        m_logger.error("dump_result_sets(): Wrong data type for column 'error_message' "
                       "in table '%1'",
                       result_sets_table_name);
        return;
    }

    for (Obj result_set : *result_sets) {
        StringData col_name_matches = result_set.get<StringData>(col_ndx_matches_property);
        ColKey col_ndx_matches = result_sets->get_column_key(col_name_matches);
        if (!col_ndx_matches) {
            m_logger.error("dump_result_sets(): No matches column '%1' in result sets table "
                           "'%1'",
                           col_name_matches, result_sets_table_name);
            std::cout << "-------------------------------------\n";
            continue;
        }
        if (result_sets->get_column_type(col_ndx_matches) != type_LinkList) {
            m_logger.error("dump_result_sets(): Wrong data type for matches column '%1' "
                           "in result sets table '%1'",
                           col_name_matches, result_sets_table_name);
            return;
        }
        LnkLst link_list = result_set.get_linklist(col_ndx_matches);
        const Table& target_table = *result_sets->get_link_target(col_ndx_matches);
        std::int_fast64_t status = result_set.get<int64_t>(col_ndx_status);
        auto id = result_set.get_key().value;
        std::cout << "\n"
                     "RESULT SET (ID = "
                  << id
                  << "):\n"
                     "-------------------------------------\n"
                     "Table:  "
                  << target_table.get_name()
                  << "\n"
                     "Query:  "
                  << result_set.get<StringData>(col_ndx_query)
                  << "\n"
                     "Status: "
                  << get_result_set_status_text(int(status)) << "\n";
        if (status < 0) { // Query parsing failed
            StringData error_message = result_set.get<StringData>(col_ndx_error_message);
            std::cout << error_message; // Already contains line terminators
            // FIXME: Current parser implementation fails to add final newline
            if (error_message.size() != 0 && error_message[error_message.size() - 1] != '\n')
                std::cout << "\n";
        }
        else if (status > 0) { // Initialized
            std::size_t num_links = link_list.size();
            for (std::size_t j = 0; j < num_links; ++j) {
                const Obj row = link_list.get_object(j);
                size_t col_i = 0;
                for (ColKey k : target_table.get_column_keys()) {
                    if (col_i++ > 0)
                        std::cout << ", ";
                    StringData name = target_table.get_column_name(k);
                    std::cout << name << ": ";
                    DataType type = target_table.get_column_type(k);
                    switch (type) {
                        case type_Int:
                            std::cout << row.get<int64_t>(k);
                            break;
                        case type_String:
                            std::cout << "'" << row.get<StringData>(k) << "'";
                            break;
                        default:
                            std::cout << "(unexpected value type)";
                            break;
                    }
                }
                std::cout << "\n";
            }
        }
        std::cout << "-------------------------------------\n";
    }
}


void Peer::open_realm()
{
    if (!m_shared_group) {
        m_history = sync::make_client_replication();
        DBOptions options;
        if (m_context.disable_sync_to_disk)
            options.durability = DBOptions::Durability::Unsafe;
        m_shared_group = DB::create(*m_history, m_realm_path, std::move(options));
    }
}


void Peer::open_realm_for_receive()
{
    if (!m_receive_group) {
        m_receive_history = sync::make_client_replication();
        DBOptions options;
        if (m_context.disable_sync_to_disk)
            options.durability = DBOptions::Durability::Unsafe;
        m_receive_shared_group = DB::create(*m_receive_history, m_realm_path, std::move(options));
        m_receive_group = m_receive_shared_group->start_read();
    }
}


void Peer::receive(VersionID)
{
    std::map<TableKey, std::set<ObjKey>> new_objects;
    ObjectObserver observer{new_objects};
    m_receive_group->advance_read(&observer);
    std::string buffer;
    const std::string& table_name = class_to_table_name(g_ptime_class_name, buffer);
    for (const auto& table_entry : new_objects) {
        const TableKey& table_key = table_entry.first;
        StringData table_name_2 = m_receive_group->get_table_name(table_key);
        if (table_name_2 == table_name) {
            m_logger.debug("Processing changeset_propagation_time_measurement_request_received");
            ConstTableRef table = m_receive_group->get_table(table_name);
            ColKey col_originator = table->get_column_key("originator");
            ColKey col_timestamp = table->get_column_key("timestamp");
            const std::set<ObjKey>& objects = table_entry.second;
            for (ObjKey obj_key : objects) {
                Obj obj = table->get_object(obj_key);
                std::int_fast64_t originator_ident = obj.get<int64_t>(col_originator);
                if (originator_ident != m_originator_ident)
                    continue;
                milliseconds_type timestamp = milliseconds_type(obj.get<int64_t>(col_timestamp));
                if (timestamp < m_start_time)
                    continue;
                milliseconds_type now = _impl::realtime_clock_now();
                milliseconds_type propagation_time = now - timestamp;
                m_logger.detail("Propagation time was %1 milliseconds", propagation_time);
                m_context.add_propagation_time(propagation_time);
                m_context.metrics.increment("client.ptime_request_received");
            }
        }
    }
}


void Peer::intitiate_access_token_refresh_wait()
{
    if (!m_access_token_refresh_timer)
        m_access_token_refresh_timer.emplace(m_context.test_proc_service);
    auto handler = [this](std::error_code ec) {
        if (ec == util::error::operation_aborted)
            return;
        REALM_ASSERT(!ec);
        refresh_access_token();
    };
    milliseconds_type min_delay = 1200000; // 20 minutes
    milliseconds_type max_delay = 1440000; // 24 minutes
    // Randomize delay in an attempt to avoid having a large number of clients
    // trying to refresh simultaneously.
    auto distr = std::uniform_int_distribution<milliseconds_type>(min_delay, max_delay);
    milliseconds_type delay = distr(m_context.test_proc_random);
    m_access_token_refresh_timer->async_wait(std::chrono::milliseconds(delay), handler);
}


void Peer::refresh_access_token()
{
    m_logger.detail("Refreshing access token");
    auto handler = [this](std::error_code ec, std::string access_token) {
        if (ec == util::error::operation_aborted)
            return;
        if (ec) {
            m_logger.error("Failed to refresh access token: %1 (error_code=%2)", ec.message(), ec);
            if (m_on_sync_error) {
                bool is_fatal = true;
                m_on_sync_error(is_fatal);
            }
            return;
        }
        m_session.refresh(access_token);
        auto func = [&] {
            intitiate_access_token_refresh_wait();
        };
        m_context.test_proc_service.post(func);
    };
    m_context.auth.refresh(m_refresh_token, handler);
}


void Peer::do_send_ptime_request(WriteTransaction& wt)
{
    TableRef table = do_ensure_ptime_class(wt);
    milliseconds_type timestamp = _impl::realtime_clock_now();
    ObjectId pkey = ObjectId::gen(); // Globally unique ???
    Obj obj = table->create_object_with_primary_key(Mixed(pkey));
    obj.set("originator", m_originator_ident);
    obj.set("timestamp", std::int64_t(timestamp));
}


TableRef Peer::do_ensure_blob_class(WriteTransaction& wt, StringData table_name)
{
    TableRef table = wt.get_table(table_name);
    if (!table) {
        DataType pk_type = type_ObjectId;
        StringData pk_column_name = "_id";
        table = sync::create_table_with_primary_key(wt, table_name, pk_type, pk_column_name);
        table->add_column(type_Binary, "blob");
        table->add_column(type_String, "label");
        table->add_column(type_Int, "kind");
        table->add_column(type_Int, "level");
    }
    return table;
}


TableRef Peer::do_ensure_ptime_class(WriteTransaction& wt)
{
    std::string buffer;
    const std::string& table_name = class_to_table_name(g_ptime_class_name, buffer);
    TableRef table = wt.get_table(table_name);
    if (!table) {
        DataType pk_type = type_ObjectId;
        StringData pk_column_name = "_id";
        table = sync::create_table_with_primary_key(wt, table_name, pk_type, pk_column_name);
        table->add_column(type_Int, "originator");
        table->add_column(type_Int, "timestamp");
    }
    return table;
}


void Peer::Context::init_metrics_gauges(bool report_propagation_times)
{
    metrics.gauge("client.sessions", 0);
    metrics.gauge("client.sessions_connecting", 0);
    metrics.gauge("client.sessions_connected", 0);
    metrics.gauge("client.sessions_with_error", 0);
    metrics.gauge("client.sessions_with_fatal_error", 0);
    metrics.gauge("client.errors", 0);
    metrics.gauge("client.fatal_errors", 0);
    bool aggregate = false;
    if (report_roundtrip_times) {
        metrics.gauge("client.roundtrip_times.n", 0);
        metrics.gauge("client.roundtrip_times.f50", 0);
        metrics.gauge("client.roundtrip_times.f90", 0);
        metrics.gauge("client.roundtrip_times.f99", 0);
        metrics.gauge("client.roundtrip_times.max", 0);
        aggregate = true;
    }
    if (report_propagation_times) {
        metrics.gauge("client.propagation_times.n", 0);
        metrics.gauge("client.propagation_times.f50", 0);
        metrics.gauge("client.propagation_times.f90", 0);
        metrics.gauge("client.propagation_times.f99", 0);
        metrics.gauge("client.propagation_times.max", 0);
        aggregate = true;
    }
    if (aggregate)
        sched_metrics_aggregation_flush();
}


void Peer::Context::on_new_session()
{
    int n = ++m_num_sessions;
    metrics.gauge("client.sessions", double(n));
}


void Peer::Context::on_session_connection_state_change(ConnectionState old_state, ConnectionState new_state)
{
    int n;
    switch (old_state) {
        case ConnectionState::disconnected:
            break;
        case ConnectionState::connecting:
            n = --m_num_sessions_connecting;
            metrics.gauge("client.sessions_connecting", double(n));
            break;
        case ConnectionState::connected:
            n = --m_num_sessions_connected;
            metrics.gauge("client.sessions_connected", double(n));
            break;
    }
    switch (new_state) {
        case ConnectionState::disconnected:
            break;
        case ConnectionState::connecting:
            n = ++m_num_sessions_connecting;
            metrics.gauge("client.sessions_connecting", double(n));
            break;
        case ConnectionState::connected:
            n = ++m_num_sessions_connected;
            metrics.gauge("client.sessions_connected", double(n));
            break;
    }
}


void Peer::Context::on_error(Error error, bool is_fatal)
{
    int n = ++m_num_errors;
    metrics.gauge("client.errors", double(n));
    if (is_fatal) {
        n = ++m_num_fatal_errors;
        metrics.gauge("client.fatal_errors", double(n));
    }
    int& num = m_error_counters[error];
    ++num;
    const char* metric = get_error_metric(error);
    metrics.gauge(metric, double(num));
}


void Peer::Context::on_first_session_error()
{
    int n = ++m_num_sessions_with_error;
    metrics.gauge("client.sessions_with_error", double(n));
}


void Peer::Context::on_first_fatal_session_error()
{
    int n = ++m_num_sessions_with_fatal_error;
    metrics.gauge("client.sessions_with_fatal_error", double(n));
}


void Peer::Context::add_roundtrip_time(milliseconds_type time)
{
    std::lock_guard<std::mutex> lock{m_metrics_aggregation_mutex};
    m_roundtrip_times.push_back(time);
}


void Peer::Context::add_propagation_time(milliseconds_type time)
{
    std::lock_guard<std::mutex> lock{m_metrics_aggregation_mutex};
    m_propagation_times.push_back(time);
}


void Peer::Context::sched_metrics_aggregation_flush()
{
    auto handler = [this](std::error_code ec) {
        if (ec == util::error::operation_aborted)
            return;
        metrics_aggregation_flush();
        sched_metrics_aggregation_flush();
    };
    m_metrics_aggregation_timer.async_wait(std::chrono::seconds(30), std::move(handler));
}


void Peer::Context::metrics_aggregation_flush()
{
    std::size_t n;
    double f50, f90, f99, max; // 50% (median), 90%, 99%, and 100% fractiles respectively
    {
        std::lock_guard<std::mutex> lock{m_metrics_aggregation_mutex};
        std::sort(m_roundtrip_times.begin(), m_roundtrip_times.end());
        n = m_roundtrip_times.size();
        f50 = fractile(m_roundtrip_times.begin(), m_roundtrip_times.end(), 0.50);
        f90 = fractile(m_roundtrip_times.begin(), m_roundtrip_times.end(), 0.90);
        f99 = fractile(m_roundtrip_times.begin(), m_roundtrip_times.end(), 0.99);
        max = fractile(m_roundtrip_times.begin(), m_roundtrip_times.end(), 1.00);
        m_roundtrip_times.clear();
    }
    metrics.gauge("client.roundtrip_times.n", double(n));
    metrics.gauge("client.roundtrip_times.f50", f50);
    metrics.gauge("client.roundtrip_times.f90", f90);
    metrics.gauge("client.roundtrip_times.f99", f99);
    metrics.gauge("client.roundtrip_times.max", max);
    {
        std::lock_guard<std::mutex> lock{m_metrics_aggregation_mutex};
        std::sort(m_propagation_times.begin(), m_propagation_times.end());
        n = m_propagation_times.size();
        f50 = fractile(m_propagation_times.begin(), m_propagation_times.end(), 0.50);
        f90 = fractile(m_propagation_times.begin(), m_propagation_times.end(), 0.90);
        f99 = fractile(m_propagation_times.begin(), m_propagation_times.end(), 0.99);
        max = fractile(m_propagation_times.begin(), m_propagation_times.end(), 1.00);
        m_propagation_times.clear();
    }
    metrics.gauge("client.propagation_times.n", double(n));
    metrics.gauge("client.propagation_times.f50", f50);
    metrics.gauge("client.propagation_times.f90", f90);
    metrics.gauge("client.propagation_times.f99", f99);
    metrics.gauge("client.propagation_times.max", max);
}
