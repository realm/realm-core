/*************************************************************************
 *
 * Copyright 2024 Realm Inc.
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

#include "realm/sync/network/network_error.hpp"

#include "realm/util/basic_system_errors.hpp"
#include "realm/util/misc_ext_errors.hpp"
#include "realm/sync/network/network_ssl.hpp"

namespace realm::sync::network {

Status get_status_from_network_error(std::error_code ec)
{
    if (!ec) {
        return Status::OK();
    }
    if (ec == make_error_code(util::MiscExtErrors::end_of_input)) {
        return {ErrorCodes::ConnectionClosed, ec.message()};
    }
    if (ec == ssl::Errors::tls_handshake_failed) {
        return {ErrorCodes::TlsHandshakeFailed, ec.message()};
    }
    switch (ec.value()) {
        case util::error::operation_aborted:
            return {ErrorCodes::Error::OperationAborted, "Write operation cancelled"};
        case util::error::address_family_not_supported:
            [[fallthrough]];
        case util::error::invalid_argument:
            return {ErrorCodes::Error::InvalidArgument, ec.message()};
        case util::error::no_memory:
            return {ErrorCodes::Error::OutOfMemory, ec.message()};
        case util::error::connection_aborted:
            [[fallthrough]];
        case util::error::connection_reset:
            [[fallthrough]];
        case util::error::broken_pipe:
            [[fallthrough]];
        case util::error::resource_unavailable_try_again:
            return {ErrorCodes::Error::ConnectionClosed, ec.message()};
        default:
            return {ErrorCodes::Error::UnknownError, ec.message()};
    }
}

} // namespace realm::sync::network
