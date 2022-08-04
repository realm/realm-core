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
    if (server_requests_action == sync::ProtocolErrorInfo::Action::ClientReset ||
        server_requests_action == sync::ProtocolErrorInfo::Action::ClientResetNoRecovery) {
        return true;
    }
    if (error_code == make_error_code(sync::Client::Error::auto_client_reset_failure)) {
        return true;
    }
    return false;
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

} // namespace realm
