#include <realm/sync/protocol.hpp>


namespace {

class ErrorCategoryImpl : public std::error_category {
public:
    const char* name() const noexcept override final
    {
        return "realm::sync::ProtocolError";
    }
    std::string message(int error_code) const override final
    {
        const char* msg = realm::sync::get_protocol_error_message(error_code);
        if (!msg)
            msg = "Unknown error";
        std::string msg_2{msg}; // Throws (copy)
        return msg_2;
    }
};

ErrorCategoryImpl g_error_category;

} // unnamed namespace


namespace realm {
namespace sync {

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
            return "The server sent a changeset that could not be integrated (Bad changeset (UPLOAD)). This is "
                   "likely due to corruption of the client-side file. Please restore the file on the client by "
                   "wiping it and resuming synchronization";
        case ProtocolError::superseded:
            return "The server sent an obsolete error code (Superseded by new session for same client-side file)";
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
    }
    return nullptr;
}

const std::error_category& protocol_error_category() noexcept
{
    return g_error_category;
}

std::error_code make_error_code(ProtocolError error_code) noexcept
{
    return std::error_code(int(error_code), g_error_category);
}

} // namespace sync
} // namespace realm
