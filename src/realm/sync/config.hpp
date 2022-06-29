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

#ifndef REALM_SYNC_CONFIG_HPP
#define REALM_SYNC_CONFIG_HPP

#include <realm/db.hpp>
#include <realm/util/assert.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/protocol.hpp>

#include <functional>
#include <memory>
#include <string>
#include <map>
#include <system_error>
#include <unordered_map>

namespace realm {

class SyncUser;
class SyncSession;
class Realm;
class ThreadSafeReference;

namespace bson {
class Bson;
}

enum class SimplifiedProtocolError {
    ConnectionIssue,
    UnexpectedInternalIssue,
    SessionIssue,
    BadAuthentication,
    PermissionDenied,
    ClientResetRequested,
    CompensatingWrite,
};

namespace sync {
using port_type = std::uint_fast16_t;
enum class ProtocolError;
} // namespace sync

SimplifiedProtocolError get_simplified_error(sync::ProtocolError err);

struct SyncError {
    enum class ClientResetModeAllowed { DoNotClientReset, RecoveryPermitted, RecoveryNotPermitted };

    std::error_code error_code;
    bool is_fatal;
    /// A consolidated explanation of the error, including a link to the server logs if applicable.
    std::string message;
    // Just the minimal error message, without any log URL.
    std::string_view simple_message;
    // The URL to the associated server log if any. If not supplied by the server, this will be `empty()`.
    std::string_view logURL;
    /// A dictionary of extra user information associated with this error.
    /// If this is a client reset error, the keys for c_original_file_path_key and c_recovery_file_path_key will be
    /// populated with the relevant filesystem paths.
    std::unordered_map<std::string, std::string> user_info;
    /// The sync server may send down an error that the client does not recognize,
    /// whether because of a version mismatch or an oversight. It is still valuable
    /// to expose these errors so that users can do something about them.
    bool is_unrecognized_by_client = false;
    // the server may explicitly send down "IsClientReset" as part of an error
    // if this is set, it overrides the clients interpretation of the error
    util::Optional<ClientResetModeAllowed> server_requests_client_reset = util::none;
    // If this error resulted from a compensating write, this vector will contain information about each object
    // that caused a compensating write and why the write was illegal.
    std::vector<sync::CompensatingWriteErrorInfo> compensating_writes_info;

    SyncError(std::error_code error_code, std::string msg, bool is_fatal,
              util::Optional<std::string> serverLog = util::none,
              std::vector<sync::CompensatingWriteErrorInfo> compensating_writes = {});

    static constexpr const char c_original_file_path_key[] = "ORIGINAL_FILE_PATH";
    static constexpr const char c_recovery_file_path_key[] = "RECOVERY_FILE_PATH";

    /// The error is a client error, which applies to the client and all its sessions.
    bool is_client_error() const;

    /// The error is a protocol error, which may either be connection-level or session-level.
    bool is_connection_level_protocol_error() const;

    /// The error is a connection-level protocol error.
    bool is_session_level_protocol_error() const;

    /// The error indicates a client reset situation.
    bool is_client_reset_requested() const;
};

using SyncSessionErrorHandler = void(std::shared_ptr<SyncSession>, SyncError);

enum class ReconnectMode {
    /// This is the mode that should always be used in production. In this
    /// mode the client uses a scheme for determining a reconnect delay that
    /// prevents it from creating too many connection requests in a short
    /// amount of time (i.e., a server hammering protection mechanism).
    normal,

    /// For testing purposes only.
    ///
    /// Never reconnect automatically after the connection is closed due to
    /// an error. Allow immediate reconnect if the connection was closed
    /// voluntarily (e.g., due to sessions being abandoned).
    ///
    /// In this mode, Client::cancel_reconnect_delay() and
    /// Session::cancel_reconnect_delay() can still be used to trigger
    /// another reconnection attempt (with no delay) after an error has
    /// caused the connection to be closed.
    testing
};

enum class SyncSessionStopPolicy {
    Immediately,          // Immediately stop the session as soon as all Realms/Sessions go out of scope.
    LiveIndefinitely,     // Never stop the session.
    AfterChangesUploaded, // Once all Realms/Sessions go out of scope, wait for uploads to complete and stop.
};

enum class ClientResyncMode : unsigned char {
    // Fire a client reset error
    Manual,
    // Discard local changes, without disrupting accessors or closing the Realm
    DiscardLocal,
    // Attempt to recover unsynchronized but committed changes.
    Recover,
    // Attempt recovery and if that fails, discard local.
    RecoverOrDiscard,
};

struct SyncConfig {
    struct FLXSyncEnabled {
    };

    struct ProxyConfig {
        using port_type = sync::port_type;
        enum class Type { HTTP, HTTPS } type;
        std::string address;
        port_type port;
    };
    using SSLVerifyCallback = bool(const std::string& server_address, ProxyConfig::port_type server_port,
                                   const char* pem_data, size_t pem_size, int preverify_ok, int depth);

    std::shared_ptr<SyncUser> user;
    std::string partition_value;
    SyncSessionStopPolicy stop_policy = SyncSessionStopPolicy::AfterChangesUploaded;
    std::function<SyncSessionErrorHandler> error_handler;
    util::Optional<std::string> ssl_trust_certificate_path;
    std::function<SSLVerifyCallback> ssl_verify_callback;
    util::Optional<ProxyConfig> proxy_config;
    bool flx_sync_requested = false;
    bool client_validate_ssl = true;

    // If true, upload/download waits are canceled on any sync error and not just fatal ones
    bool cancel_waits_on_nonfatal_error = false;

    // If false, changesets incoming from the server are discarded without
    // applying them to the Realm file. This is required when writing objects
    // directly to replication, and will break horribly otherwise
    bool apply_server_changes = true;

    util::Optional<std::string> authorization_header_name;
    std::map<std::string, std::string> custom_http_headers;

    // The name of the directory which Realms should be backed up to following
    // a client reset in ClientResyncMode::Manual mode
    util::Optional<std::string> recovery_directory;
    ClientResyncMode client_resync_mode = ClientResyncMode::Manual;
    std::function<void(std::shared_ptr<Realm> before_frozen)> notify_before_client_reset;
    std::function<void(std::shared_ptr<Realm> before_frozen, ThreadSafeReference after, bool did_recover)>
        notify_after_client_reset;

    // Will be called after a download message is received and validated by the client but befefore it's been
    // transformed or applied. To be used in testing only.
    std::function<void(std::weak_ptr<SyncSession>, const sync::SyncProgress&, int64_t, sync::DownloadBatchState)>
        on_download_message_received_hook;
    // Will be called after each bootstrap message is added to the pending bootstrap store, but before
    // processing a finalized bootstrap. For testing only.
    std::function<bool(std::weak_ptr<SyncSession>, const sync::SyncProgress&, int64_t, sync::DownloadBatchState)>
        on_bootstrap_message_processed_hook;

    explicit SyncConfig(std::shared_ptr<SyncUser> user, bson::Bson partition);
    explicit SyncConfig(std::shared_ptr<SyncUser> user, std::string partition);
    explicit SyncConfig(std::shared_ptr<SyncUser> user, const char* partition);
    explicit SyncConfig(std::shared_ptr<SyncUser> user, FLXSyncEnabled);
};

} // namespace realm

#endif // REALM_SYNC_CONFIG_HPP
