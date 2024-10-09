#include <realm/sync/protocol.hpp>


namespace realm::sync {

const char* get_protocol_error_message(int error_code) noexcept
{
    // FIXME: These human-readable messages are phrased from the perspective of the client, but they may occur on the
    // server side as well.

    switch (ProtocolError(error_code)) {
        case ProtocolError::connection_closed:
            return "Connection closed (no error)";
        case ProtocolError::other_error:
            return "Other connection level error";
        case ProtocolError::unknown_message:
            return "Unknown type of input message";
        case ProtocolError::bad_syntax:
            return "Bad syntax in input message head";
        case ProtocolError::limits_exceeded:
            return "Limits exceeded in input message";
        case ProtocolError::wrong_protocol_version:
            return "Wrong protocol version (CLIENT)";
        case ProtocolError::bad_session_ident:
            return "The server has forgotten about this session (Bad session identifier in input message). "
                   "Restart the client to resume synchronization";
        case ProtocolError::reuse_of_session_ident:
            return "An existing synchronization session exists with this session identifier (Overlapping reuse of "
                   "session identifier (BIND)).";
        case ProtocolError::bound_in_other_session:
            return "An existing synchronization session exists for this client-side file (Client file bound in other "
                   "session (IDENT))";
        case ProtocolError::bad_message_order:
            return "Bad input message order";
        case ProtocolError::bad_decompression:
            return "The server sent an invalid DOWNLOAD message (Bad decompression of message)";
        case ProtocolError::bad_changeset_header_syntax:
            return "The server sent an invalid DOWNLOAD message (Bad changeset header syntax)";
        case ProtocolError::bad_changeset_size:
            return "The server sent an invalid DOWNLOAD message (Bad changeset size)";
        case ProtocolError::switch_to_flx_sync:
            return "Wrong wire protocol, switch to the flexible sync wire protocol";
        case ProtocolError::switch_to_pbs:
            return "Wrong wire protocol, switch to the partition-based sync wire protocol";

        case ProtocolError::session_closed:
            return "Session closed (no error)";
        case ProtocolError::other_session_error:
            return "Other session level error";
        case ProtocolError::token_expired:
            return "Access token expired";
        case ProtocolError::bad_authentication:
            return "Bad user authentication (BIND)";
        case ProtocolError::illegal_realm_path:
            return "Illegal Realm path (BIND)";
        case ProtocolError::no_such_realm:
            return "No such Realm (BIND)";
        case ProtocolError::permission_denied:
            return "Permission denied (BIND)";
        case ProtocolError::bad_server_file_ident:
            return "The server sent an obsolete error code (Bad server file identifier (IDENT))";
        case ProtocolError::bad_client_file_ident:
            return "The server has forgotten about this client-side file (Bad client file identifier (IDENT)). "
                   "Please wipe the file on the client to resume synchronization";
        case ProtocolError::bad_server_version:
            return "The client is ahead of the server (Bad server version (IDENT, UPLOAD)). Please wipe the file on "
                   "the client to resume synchronization";
        case ProtocolError::bad_client_version:
            return "The server claimed to have received changesets from this client that the client has not produced "
                   "yet (Bad client version (IDENT, UPLOAD)). Please wipe the file on the client to resume "
                   "synchronization";
        case ProtocolError::diverging_histories:
            return "The client and server disagree about the history (Diverging histories (IDENT)). Please wipe the "
                   "file on the client to resume synchronization";
        case ProtocolError::bad_changeset:
            return "The server sent a changeset that could not be integrated (Bad changeset (UPLOAD, ERROR)). This "
                   "is likely due to corruption of the client-side file. Please restore the file on the client by "
                   "wiping it and resuming synchronization";
        case ProtocolError::partial_sync_disabled:
            return "Query-based sync is disabled";
        case ProtocolError::unsupported_session_feature:
            return "Unsupported session-level feature";
        case ProtocolError::bad_origin_file_ident:
            return "The server sent an obsolete error code (Bad origin file identifier (UPLOAD))";
        case ProtocolError::bad_client_file:
            return "Synchronization no longer possible for client-side file. Please wipe the file on the client to "
                   "resume synchronization";
        case ProtocolError::server_file_deleted:
            return "Server file was deleted while a session was bound to it";
        case ProtocolError::client_file_blacklisted:
            return "Client file has been blacklisted (IDENT)";
        case ProtocolError::user_blacklisted:
            return "User has been blacklisted (BIND)";
        case ProtocolError::transact_before_upload:
            return "The server sent an obsolete error code (Serialized transaction before upload completion)";
        case ProtocolError::client_file_expired:
            return "Client file has expired due to log compaction. Please wipe the file on the client to resume "
                   "synchronization";
        case ProtocolError::user_mismatch:
            return "User mismatch for client file identifier (IDENT)";
        case ProtocolError::too_many_sessions:
            return "Too many sessions in connection (BIND)";
        case ProtocolError::invalid_schema_change:
            return "Invalid schema change (UPLOAD)";
        case ProtocolError::bad_query:
            return "Client query is invalid/malformed (IDENT, QUERY)";
        case ProtocolError::object_already_exists:
            return "Client tried to create an object that already exists outside their view (UPLOAD)";
        case ProtocolError::server_permissions_changed:
            return "Server permissions for this file ident have changed since the last time it was used (IDENT)";
        case ProtocolError::initial_sync_not_completed:
            return "Client tried to open a session before initial sync is complete (BIND)";
        case ProtocolError::write_not_allowed:
            return "Client attempted a write that is disallowed by permissions, or modifies an object outside the "
                   "current query - requires client reset";
        case ProtocolError::compensating_write:
            return "Client attempted a write that is disallowed by permissions, or modifies an object outside the "
                   "current query, and the server undid the change";
        case ProtocolError::migrate_to_flx:
            return "Server migrated to flexible sync - migrating client to use flexible sync";
        case ProtocolError::bad_progress:
            return "Bad progress information (DOWNLOAD)";
        case ProtocolError::revert_to_pbs:
            return "Server rolled back after flexible sync migration - reverting client to partition based "
                   "sync";
        case ProtocolError::bad_schema_version:
            return "Client tried to open a session with an invalid schema version (BIND)";
        case ProtocolError::schema_version_changed:
            return "Client opened a session with a new valid schema version - migrating client to use new schema "
                   "version (BIND)";
        case ProtocolError::schema_version_force_upgrade:
            return "Server has forcefully bumped client's schema version because it does not support schema "
                   "versioning";
    }
    return nullptr;
}

std::ostream& operator<<(std::ostream& os, ProtocolError error)
{
    if (auto str = get_protocol_error_message(static_cast<int>(error))) {
        return os << str;
    }
    return os << "Unknown protocol error " << static_cast<int>(error);
}

Status protocol_error_to_status(ProtocolError error_code, std::string_view msg)
{
    auto translated_error_code = [&] {
        switch (error_code) {
            case ProtocolError::connection_closed:
                return ErrorCodes::ConnectionClosed;
            case ProtocolError::other_error:
                return ErrorCodes::RuntimeError;
            case ProtocolError::unknown_message:
                [[fallthrough]];
            case ProtocolError::bad_syntax:
                [[fallthrough]];
            case ProtocolError::wrong_protocol_version:
                [[fallthrough]];
            case ProtocolError::bad_session_ident:
                [[fallthrough]];
            case ProtocolError::reuse_of_session_ident:
                [[fallthrough]];
            case ProtocolError::bound_in_other_session:
                [[fallthrough]];
            case ProtocolError::bad_changeset_header_syntax:
                [[fallthrough]];
            case ProtocolError::bad_changeset_size:
                [[fallthrough]];
            case ProtocolError::bad_message_order:
                return ErrorCodes::SyncProtocolInvariantFailed;
            case ProtocolError::bad_decompression:
                return ErrorCodes::RuntimeError;
            case ProtocolError::switch_to_flx_sync:
                [[fallthrough]];
            case ProtocolError::switch_to_pbs:
                return ErrorCodes::WrongSyncType;

            case ProtocolError::session_closed:
                return ErrorCodes::ConnectionClosed;
            case ProtocolError::other_session_error:
                return ErrorCodes::RuntimeError;
            case ProtocolError::illegal_realm_path:
                return ErrorCodes::BadSyncPartitionValue;
            case ProtocolError::permission_denied:
                return ErrorCodes::SyncPermissionDenied;
            case ProtocolError::bad_client_file_ident:
                [[fallthrough]];
            case ProtocolError::bad_server_version:
                [[fallthrough]];
            case ProtocolError::bad_client_version:
                [[fallthrough]];
            case ProtocolError::diverging_histories:
                [[fallthrough]];
            case ProtocolError::client_file_expired:
                [[fallthrough]];
            case ProtocolError::bad_client_file:
                return ErrorCodes::SyncClientResetRequired;
            case ProtocolError::bad_changeset:
                return ErrorCodes::BadChangeset;
            case ProtocolError::bad_origin_file_ident:
                return ErrorCodes::SyncProtocolInvariantFailed;
            case ProtocolError::user_mismatch:
                return ErrorCodes::SyncUserMismatch;
            case ProtocolError::invalid_schema_change:
                return ErrorCodes::InvalidSchemaChange;
            case ProtocolError::bad_query:
                return ErrorCodes::InvalidSubscriptionQuery;
            case ProtocolError::object_already_exists:
                return ErrorCodes::ObjectAlreadyExists;
            case ProtocolError::server_permissions_changed:
                return ErrorCodes::SyncServerPermissionsChanged;
            case ProtocolError::initial_sync_not_completed:
                return ErrorCodes::ConnectionClosed;
            case ProtocolError::write_not_allowed:
                return ErrorCodes::SyncWriteNotAllowed;
            case ProtocolError::compensating_write:
                return ErrorCodes::SyncCompensatingWrite;
            case ProtocolError::bad_progress:
                return ErrorCodes::SyncProtocolInvariantFailed;
            case ProtocolError::migrate_to_flx:
                [[fallthrough]];
            case ProtocolError::revert_to_pbs:
                return ErrorCodes::WrongSyncType;
            case ProtocolError::bad_schema_version:
                [[fallthrough]];
            case ProtocolError::schema_version_changed:
                [[fallthrough]];
            case ProtocolError::schema_version_force_upgrade:
                return ErrorCodes::SyncSchemaMigrationError;

            case ProtocolError::limits_exceeded:
                [[fallthrough]];
            case ProtocolError::token_expired:
                [[fallthrough]];
            case ProtocolError::bad_authentication:
                [[fallthrough]];
            case ProtocolError::no_such_realm:
                [[fallthrough]];
            case ProtocolError::bad_server_file_ident:
                [[fallthrough]];
            case ProtocolError::partial_sync_disabled:
                [[fallthrough]];
            case ProtocolError::unsupported_session_feature:
                [[fallthrough]];
            case ProtocolError::too_many_sessions:
                [[fallthrough]];
            case ProtocolError::server_file_deleted:
                [[fallthrough]];
            case ProtocolError::client_file_blacklisted:
                [[fallthrough]];
            case ProtocolError::user_blacklisted:
                [[fallthrough]];
            case ProtocolError::transact_before_upload:
                REALM_UNREACHABLE();
        }
        return ErrorCodes::UnknownError;
    }();

    if (translated_error_code == ErrorCodes::UnknownError) {
        return {ErrorCodes::UnknownError,
                util::format("Unknown sync protocol error code %1: %2", static_cast<int>(error_code), msg)};
    }
    return {translated_error_code, msg};
}

} // namespace realm::sync
