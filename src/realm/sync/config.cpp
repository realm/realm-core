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

namespace realm {

using ProtocolError = realm::sync::ProtocolError;

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
    if (error_code.category() != realm::sync::protocol_error_category()) {
        return false;
    }
    // Documented here: https://realm.io/docs/realm-object-server/#client-recovery-from-a-backup
    return (error_code == ProtocolError::bad_server_file_ident ||
            error_code == ProtocolError::bad_client_file_ident || error_code == ProtocolError::bad_server_version ||
            error_code == ProtocolError::diverging_histories || error_code == ProtocolError::client_file_expired ||
            error_code == ProtocolError::invalid_schema_change);
}

} // namespace realm
