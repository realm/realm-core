////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#ifndef REALM_OS_SYNC_CONFIG_HPP
#define REALM_OS_SYNC_CONFIG_HPP

#include "sync_user.hpp"
#include "sync_manager.hpp"
#include "util/bson/bson.hpp"

#include <realm/util/assert.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/protocol.hpp>

#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>

#include <realm/sync/history.hpp>

namespace realm {

class SyncUser;
class SyncSession;

using ChangesetTransformer = sync::ClientReplication::ChangesetCooker;

enum class SyncSessionStopPolicy;

struct SyncConfig;

struct SyncError;
using SyncSessionErrorHandler = void(std::shared_ptr<SyncSession>, SyncError);

struct SyncError {
    using ProtocolError = realm::sync::ProtocolError;

    std::error_code error_code;
    std::string message;
    bool is_fatal;
    std::unordered_map<std::string, std::string> user_info;
    /// The sync server may send down an error that the client does not recognize,
    /// whether because of a version mismatch or an oversight. It is still valuable
    /// to expose these errors so that users can do something about them.
    bool is_unrecognized_by_client = false;

    SyncError(std::error_code error_code, std::string message, bool is_fatal)
        : error_code(std::move(error_code))
        , message(std::move(message))
        , is_fatal(is_fatal)
    {
    }

    static constexpr const char c_original_file_path_key[] = "ORIGINAL_FILE_PATH";
    static constexpr const char c_recovery_file_path_key[] = "RECOVERY_FILE_PATH";

    /// The error is a client error, which applies to the client and all its sessions.
    bool is_client_error() const
    {
        return error_code.category() == realm::sync::client_error_category();
    }

    /// The error is a protocol error, which may either be connection-level or session-level.
    bool is_connection_level_protocol_error() const
    {
        if (error_code.category() != realm::sync::protocol_error_category()) {
            return false;
        }
        return !realm::sync::is_session_level_error(static_cast<ProtocolError>(error_code.value()));
    }

    /// The error is a connection-level protocol error.
    bool is_session_level_protocol_error() const
    {
        if (error_code.category() != realm::sync::protocol_error_category()) {
            return false;
        }
        return realm::sync::is_session_level_error(static_cast<ProtocolError>(error_code.value()));
    }

    /// The error indicates a client reset situation.
    bool is_client_reset_requested() const
    {
        if (error_code.category() != realm::sync::protocol_error_category()) {
            return false;
        }
        // Documented here: https://realm.io/docs/realm-object-server/#client-recovery-from-a-backup
        return (error_code == ProtocolError::bad_server_file_ident
                || error_code == ProtocolError::bad_client_file_ident
                || error_code == ProtocolError::bad_server_version
                || error_code == ProtocolError::diverging_histories
                || error_code == ProtocolError::client_file_expired
                || error_code == ProtocolError::invalid_schema_change);
    }
};

enum class ClientResyncMode : unsigned char {
    // Enable automatic client resync with local transaction recovery
    Recover = 0,
    // Enable automatic client resync without local transaction recovery
    DiscardLocal = 1,
    // Fire a client reset error
    Manual = 2,
};

struct SyncConfig {
    using ProxyConfig = sync::Session::Config::ProxyConfig;

    std::shared_ptr<SyncUser> user;
    std::string partition_value;
    SyncSessionStopPolicy stop_policy = SyncSessionStopPolicy::AfterChangesUploaded;
    std::function<SyncSessionErrorHandler> error_handler;
    std::shared_ptr<ChangesetTransformer> transformer;
    util::Optional<std::array<char, 64>> realm_encryption_key;
    bool client_validate_ssl = true;
    util::Optional<std::string> ssl_trust_certificate_path;
    std::function<sync::Session::SSLVerifyCallback> ssl_verify_callback;
    util::Optional<ProxyConfig> proxy_config;

    // If true, upload/download waits are canceled on any sync error and not just fatal ones
    bool cancel_waits_on_nonfatal_error = false;

    util::Optional<std::string> authorization_header_name;
    std::map<std::string, std::string> custom_http_headers;

    // The name of the directory which Realms should be backed up to following
    // a client reset
    util::Optional<std::string> recovery_directory;
    ClientResyncMode client_resync_mode = ClientResyncMode::Recover;

    explicit SyncConfig(std::shared_ptr<SyncUser> user, bson::Bson partition)
    : user(std::move(user))
    , partition_value(partition.to_string())
    {
    }
    explicit SyncConfig(std::shared_ptr<SyncUser> user, std::string partition)
    : user(std::move(user))
    , partition_value(std::move(partition))
    {
    }
    explicit SyncConfig(std::shared_ptr<SyncUser> user, const char* partition)
    : user(std::move(user))
    , partition_value(partition)
    {
    }


};

} // namespace realm

#endif // REALM_OS_SYNC_CONFIG_HPP
