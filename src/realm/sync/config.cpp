////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/sync/config.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/object-store/util/bson/bson.hpp>
#include <realm/util/network.hpp>

#include <ostream>

namespace realm {

// sync defines its own copy of port_type to avoid depending on network.hpp, but they should be the same.
static_assert(std::is_same_v<sync::port_type, util::network::Endpoint::port_type>);

using ProtocolError = realm::sync::ProtocolError;

SyncError::SyncError(std::error_code error_code, std::string msg, bool is_fatal,
                     util::Optional<std::string> serverLog,
                     std::vector<sync::CompensatingWriteErrorInfo> compensating_writes)
    : error_code(std::move(error_code))
    , is_fatal(is_fatal)
    , message(std::move(msg))
    , compensating_writes_info(std::move(compensating_writes))
{
    if (serverLog) {
        size_t msg_length = message.size();
        static constexpr std::string_view middle(" Logs: ");
        message = util::format("%1%2%3", message, middle, *serverLog);
        simple_message = std::string_view(message.data(), msg_length);
        logURL = std::string_view(message.data() + msg_length + middle.size(), serverLog->size());
    }
    else {
        simple_message = message;
    }
}

bool SyncError::is_client_error() const
{
    return error_code.category() == realm::sync::client_error_category();
}

/// The error is a protocol error, which may either be connection-level or session-level.
bool SyncError::is_connection_level_protocol_error() const
{
    if (error_code.category() != realm::sync::protocol_error_category()) {
        return false;
    }
    return !realm::sync::is_session_level_error(static_cast<ProtocolError>(error_code.value()));
}

/// The error is a connection-level protocol error.
bool SyncError::is_session_level_protocol_error() const
{
    if (error_code.category() != realm::sync::protocol_error_category()) {
        return false;
    }
    return realm::sync::is_session_level_error(static_cast<ProtocolError>(error_code.value()));
}

/// The error indicates a client reset situation.
bool SyncError::is_client_reset_requested() const
{
    if (server_requests_client_reset) {
        return *server_requests_client_reset != SyncError::ClientResetModeAllowed::DoNotClientReset;
    }
    if (error_code == make_error_code(sync::Client::Error::auto_client_reset_failure)) {
        return true;
    }
    if (error_code.category() != realm::sync::protocol_error_category()) {
        return false;
    }
    return get_simplified_error(static_cast<sync::ProtocolError>(error_code.value())) ==
           SimplifiedProtocolError::ClientResetRequested;
}

SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, bson::Bson partition)
    : user(std::move(user))
    , partition_value(partition.to_string())
{
}
SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, std::string partition)
    : user(std::move(user))
    , partition_value(std::move(partition))
{
}
SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, const char* partition)
    : user(std::move(user))
    , partition_value(partition)
{
}

SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, FLXSyncEnabled)
    : user(std::move(user))
    , partition_value()
    , flx_sync_requested(true)
{
}

SimplifiedProtocolError get_simplified_error(sync::ProtocolError err)
{
    switch (err) {
        // Connection level errors
        case ProtocolError::connection_closed:
        case ProtocolError::other_error:
            // Not real errors, don't need to be reported to the binding.
            return SimplifiedProtocolError::ConnectionIssue;
        case ProtocolError::unknown_message:
        case ProtocolError::bad_syntax:
        case ProtocolError::limits_exceeded:
        case ProtocolError::wrong_protocol_version:
        case ProtocolError::bad_session_ident:
        case ProtocolError::reuse_of_session_ident:
        case ProtocolError::bound_in_other_session:
        case ProtocolError::bad_message_order:
        case ProtocolError::bad_client_version:
        case ProtocolError::illegal_realm_path:
        case ProtocolError::no_such_realm:
        case ProtocolError::bad_changeset:
        case ProtocolError::bad_changeset_header_syntax:
        case ProtocolError::bad_changeset_size:
        case ProtocolError::bad_decompression:
        case ProtocolError::unsupported_session_feature:
        case ProtocolError::transact_before_upload:
        case ProtocolError::partial_sync_disabled:
        case ProtocolError::user_mismatch:
        case ProtocolError::too_many_sessions:
        case ProtocolError::bad_query:
        case ProtocolError::switch_to_pbs:
        case ProtocolError::switch_to_flx_sync:
            return SimplifiedProtocolError::UnexpectedInternalIssue;
        // Session errors
        case ProtocolError::session_closed:
        case ProtocolError::other_session_error:
        case ProtocolError::initial_sync_not_completed:
            // The binding doesn't need to be aware of these because they are strictly informational, and do not
            // represent actual errors.
            return SimplifiedProtocolError::SessionIssue;
        case ProtocolError::token_expired: {
            REALM_UNREACHABLE(); // This is not sent by the MongoDB server
        }
        case ProtocolError::bad_authentication:
            return SimplifiedProtocolError::BadAuthentication;
        case ProtocolError::permission_denied:
            return SimplifiedProtocolError::PermissionDenied;
        case ProtocolError::bad_client_file:
        case ProtocolError::bad_client_file_ident:
        case ProtocolError::bad_origin_file_ident:
        case ProtocolError::bad_server_file_ident:
        case ProtocolError::bad_server_version:
        case ProtocolError::client_file_blacklisted:
        case ProtocolError::client_file_expired:
        case ProtocolError::diverging_histories:
        case ProtocolError::invalid_schema_change:
        case ProtocolError::server_file_deleted:
        case ProtocolError::user_blacklisted:
        case ProtocolError::object_already_exists:
        case ProtocolError::server_permissions_changed:
        case ProtocolError::write_not_allowed:
            return SimplifiedProtocolError::ClientResetRequested;
        case ProtocolError::compensating_write:
            return SimplifiedProtocolError::CompensatingWrite;
    }
    return SimplifiedProtocolError::UnexpectedInternalIssue; // always return a value to appease MSVC.
}

} // namespace realm
