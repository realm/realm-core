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

#pragma once

#include <realm/string_data.hpp>

namespace realm {

/*
 * TODO As part of RCORE-720, we'll move all the exception types and error codes we want to expose in the
 * public API here so that each one has a unique error code. For now though, this is here to complete the
 * API of Status/StatusWith.
 */
class ErrorCodes {
public:
    enum Error : int32_t {
        OK = 0,
        UnknownError = 1,
        RuntimeError = 2,
        LogicError = 3,
        BrokenPromise = 4,
        OperationAborted = 5,
        ReadError = 7,
        WriteError = 8,
        ResolveFailed = 9,
        ConnectionFailed = 10,
        WebSocket_Retry_Error = 11,
        WebSocket_Fatal_Error = 12,

        /// WebSocket Errors
        // WebSocket_OK = 1000 is not used, just use OK instead
        WebSocket_GoingAway = 1001,
        WebSocket_ProtocolError = 1002,
        WebSocket_UnsupportedData = 1003,
        WebSocket_Reserved = 1004,
        WebSocket_NoStatusReceived = 1005,
        WebSocket_AbnormalClosure = 1006,
        WebSocket_InvalidPayloadData = 1007,
        WebSocket_PolicyViolation = 1008,
        WebSocket_MessageTooBig = 1009,
        WebSocket_InavalidExtension = 1010,
        WebSocket_InternalServerError = 1011,
        WebSocket_TLSHandshakeFailed = 1015, // Used by default WebSocket

        /// WebSocket Errors - reported by server
        WebSocket_Unauthorized = 4001,
        WebSocket_Forbidden = 4002,
        WebSocket_MovedPermanently = 4003,
        WebSocket_Client_Too_Old = 4004,
        WebSocket_Client_Too_New = 4005,
        WebSocket_Protocol_Mismatch = 4006,
    };

    static StringData error_string(Error code);
};

} // namespace realm
