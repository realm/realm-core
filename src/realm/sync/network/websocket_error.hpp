#pragma once

#include <ostream>

#include <realm/error_codes.h>

namespace realm::sync::websocket {
enum class WebSocketError {
    websocket_ok = RLM_ERR_WEBSOCKET_OK,
    websocket_going_away = RLM_ERR_WEBSOCKET_GOINGAWAY,
    websocket_protocol_error = RLM_ERR_WEBSOCKET_PROTOCOLERROR,
    websocket_unsupported_data = RLM_ERR_WEBSOCKET_UNSUPPORTEDDATA,
    websocket_reserved = RLM_ERR_WEBSOCKET_RESERVED,
    websocket_no_status_received = RLM_ERR_WEBSOCKET_NOSTATUSRECEIVED,
    websocket_abnormal_closure = RLM_ERR_WEBSOCKET_ABNORMALCLOSURE,
    websocket_invalid_payload_data = RLM_ERR_WEBSOCKET_INVALIDPAYLOADDATA,
    websocket_policy_violation = RLM_ERR_WEBSOCKET_POLICYVIOLATION,
    websocket_message_too_big = RLM_ERR_WEBSOCKET_MESSAGETOOBIG,
    websocket_invalid_extension = RLM_ERR_WEBSOCKET_INAVALIDEXTENSION,
    websocket_internal_server_error = RLM_ERR_WEBSOCKET_INTERNALSERVERERROR,
    websocket_tls_handshake_failed = RLM_ERR_WEBSOCKET_TLSHANDSHAKEFAILED, // Used by default WebSocket

    // WebSocket Errors - reported by server
    websocket_unauthorized = RLM_ERR_WEBSOCKET_UNAUTHORIZED,
    websocket_forbidden = RLM_ERR_WEBSOCKET_FORBIDDEN,
    websocket_moved_permanently = RLM_ERR_WEBSOCKET_MOVEDPERMANENTLY,
    websocket_client_too_old = RLM_ERR_WEBSOCKET_CLIENT_TOO_OLD,
    websocket_client_too_new = RLM_ERR_WEBSOCKET_CLIENT_TOO_NEW,
    websocket_protocol_mismatch = RLM_ERR_WEBSOCKET_PROTOCOL_MISMATCH,

    websocket_resolve_failed = RLM_ERR_WEBSOCKET_RESOLVE_FAILED,
    websocket_connection_failed = RLM_ERR_WEBSOCKET_CONNECTION_FAILED,
    websocket_read_error = RLM_ERR_WEBSOCKET_READ_ERROR,
    websocket_write_error = RLM_ERR_WEBSOCKET_WRITE_ERROR,
    websocket_retry_error = RLM_ERR_WEBSOCKET_RETRY_ERROR,
    websocket_fatal_error = RLM_ERR_WEBSOCKET_FATAL_ERROR,
};

std::ostream& operator<<(std::ostream&, WebSocketError);

} // namespace realm::sync::websocket
