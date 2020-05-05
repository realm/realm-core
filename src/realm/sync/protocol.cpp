#include <realm/sync/protocol.hpp>


namespace {

class ErrorCategoryImpl: public std::error_category {
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
            return "Bad session identifier in input message";
        case ProtocolError::reuse_of_session_ident:
            return "Overlapping reuse of session identifier (BIND)";
        case ProtocolError::bound_in_other_session:
            return "Client file bound in other session (IDENT)";
        case ProtocolError::bad_message_order:
            return "Bad input message order";
        case ProtocolError::bad_decompression:
            return "Bad decompression of message";
        case ProtocolError::bad_changeset_header_syntax:
            return "Bad changeset header syntax";
        case ProtocolError::bad_changeset_size:
            return "Bad changeset size";
        case ProtocolError::bad_changesets:
            return "Bad changesets";

        case ProtocolError::session_closed:
            return "Session closed (no error)";
        case ProtocolError::other_session_error:
            return "Other session level error";
        case ProtocolError::token_expired:
            return "Access token expired";
        case ProtocolError::bad_authentication:
            return "Bad user authentication (BIND, REFRESH)";
        case ProtocolError::illegal_realm_path:
            return "Illegal Realm path (BIND)";
        case ProtocolError::no_such_realm:
            return "No such Realm (BIND)";
        case ProtocolError::permission_denied:
            return "Permission denied (BIND, REFRESH)";
        case ProtocolError::bad_server_file_ident:
            return "Bad server file identifier (IDENT)";
        case ProtocolError::bad_client_file_ident:
            return "Bad client file identifier (IDENT)";
        case ProtocolError::bad_server_version:
            return "Bad server version (IDENT, UPLOAD)";
        case ProtocolError::bad_client_version:
            return "Bad client version (IDENT, UPLOAD)";
        case ProtocolError::diverging_histories:
            return "Diverging histories (IDENT)";
        case ProtocolError::bad_changeset:
            return "Bad changeset (UPLOAD)";
        case ProtocolError::superseded:
            return "Superseded by new session for same client-side file";
        case ProtocolError::partial_sync_disabled:
            return "Partial sync disabled";
        case ProtocolError::unsupported_session_feature:
            return "Unsupported session-level feature";
        case ProtocolError::bad_origin_file_ident:
            return "Bad origin file identifier (UPLOAD)";
        case ProtocolError::bad_client_file:
            return "Synchronization no longer possible for client-side file";
        case ProtocolError::server_file_deleted:
            return "Server file was deleted while session was bound to it";
        case ProtocolError::client_file_blacklisted:
            return "Client file has been blacklisted (IDENT)";
        case ProtocolError::user_blacklisted:
            return "User has been blacklisted (BIND)";
        case ProtocolError::transact_before_upload:
            return "Serialized transaction before upload completion";
        case ProtocolError::client_file_expired:
            return "Client file has expired";
        case ProtocolError::user_mismatch:
            return "User mismatch for client file identifier (IDENT)";
        case ProtocolError::too_many_sessions:
            return "Too many sessions in connection (BIND)";
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
