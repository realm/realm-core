/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "realm/error_codes.hpp"

namespace realm {

StringData ErrorCodes::error_string(Error code)
{
    switch (code) {
        case ErrorCodes::OK:
            return "OK";
        case ErrorCodes::RuntimeError:
            return "RuntimeError";
        case ErrorCodes::LogicError:
            return "LogicError";
        case ErrorCodes::BrokenPromise:
            return "BrokenPromise";
        case ErrorCodes::OperationAborted:
            return "OperationAborted";
        case ErrorCodes::ReadError:
            return "ReadError";
        case ErrorCodes::WriteError:
            return "WriteError";
        case ErrorCodes::ResolveFailed:
            return "ResolveFailed";
        case ErrorCodes::ConnectionFailed:
            return "ConnectionFailed";

        /// WebSocket error codes
        case ErrorCodes::WebSocket_GoingAway:
            return "WebSocket: Going Away";
        case ErrorCodes::WebSocket_ProtocolError:
            return "WebSocket: Protocol Error";
        case ErrorCodes::WebSocket_UnsupportedData:
            return "WebSocket: Unsupported Data";
        case ErrorCodes::WebSocket_Reserved:
            return "WebSocket: Reserved";
        case ErrorCodes::WebSocket_NoStatusReceived:
            return "WebSocket: No Status Received";
        case ErrorCodes::WebSocket_AbnormalClosure:
            return "WebSocket: Abnormal Closure";
        case ErrorCodes::WebSocket_InvalidPayloadData:
            return "WebSocket: Invalid Payload Data";
        case ErrorCodes::WebSocket_PolicyViolation:
            return "WebSocket: Policy Violation";
        case ErrorCodes::WebSocket_MessageTooBig:
            return "WebSocket: Message Too Big";
        case ErrorCodes::WebSocket_InavalidExtension:
            return "WebSocket: Invalid Extension";
        case ErrorCodes::WebSocket_InternalServerError:
            return "WebSocket: Internal Server Error";
        case ErrorCodes::WebSocket_TLSHandshakeFailed:
            return "WebSocket: TLS Handshake Failed";

        /// WebSocket Errors - reported by server
        case ErrorCodes::WebSocket_Unauthorized:
            return "WebSocket: Unauthorized";
        case ErrorCodes::WebSocket_Forbidden:
            return "WebSocket: Forbidden";
        case ErrorCodes::WebSocket_MovedPermanently:
            return "WebSocket: Moved Permanently";
        case ErrorCodes::WebSocket_Client_Too_Old:
            return "WebSocket: Client Too Old";
        case ErrorCodes::WebSocket_Client_Too_New:
            return "WebSocket: Client Too New";
        case ErrorCodes::WebSocket_Protocol_Mismatch:
            return "WebSocket: Protocol Mismatch";

        case ErrorCodes::UnknownError:
            [[fallthrough]];
        default:
            return "UnknownError";
    }
}

namespace {

class StatusErrorCategory : public std::error_category {
    const char* name() const noexcept final
    {
        return "realm::sync::websocket::CloseStatus";
    }
    std::string message(int error_code) const final
    {
        // Converts an error_code to one of the pre-defined status codes in
        // https://tools.ietf.org/html/rfc6455#section-7.4.1
        if (error_code == 1000 || error_code == 0) {
            return ErrorCodes::error_string(ErrorCodes::OK);
        }
        return ErrorCodes::error_string(static_cast<ErrorCodes::Error>(error_code));
    }
};

} // unnamed namespace

const std::error_category& close_status_category() noexcept
{
    static const StatusErrorCategory category = {};
    return category;
}

std::error_code make_error_code(ErrorCodes::Error error) noexcept
{
    return std::error_code{error, close_status_category()};
}

} // namespace realm
