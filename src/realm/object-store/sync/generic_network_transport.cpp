////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/sync/generic_network_transport.hpp>

#include <ostream>

namespace realm::app {

namespace {
std::string http_message(const std::string& prefix, int code)
{
    if (code >= 100 && code < 200) {
        return util::format("%1. Informational: %2", prefix, code);
    }
    else if (code >= 200 && code < 300) {
        return util::format("%1. Success: %2", prefix, code);
    }
    else if (code >= 300 && code < 400) {
        return util::format("%1. Redirection: %2", prefix, code);
    }
    else if (code >= 400 && code < 500) {
        return util::format("%1. Client Error: %2", prefix, code);
    }
    else if (code >= 500 && code < 600) {
        return util::format("%1. Server Error: %2", prefix, code);
    }
    return util::format("%1. Unknown HTTP Error: %2", prefix, code);
}
} // anonymous namespace

const char* httpmethod_to_string(HttpMethod method)
{
    switch (method) {
        case HttpMethod::get:
            return "GET";
        case HttpMethod::post:
            return "POST";
        case HttpMethod::patch:
            return "PATCH";
        case HttpMethod::put:
            return "PUT";
        case HttpMethod::del:
            return "DEL";
    }
    return "UNKNOWN";
}

AppError::AppError(ErrorCodes::Error ec, std::string message, std::string link,
                   std::optional<int> additional_error_code, std::optional<std::string> server_err)
    : Exception(ec, ec == ErrorCodes::HTTPError ? http_message(message, *additional_error_code) : message)
    , additional_status_code(additional_error_code)
    , link_to_server_logs(link)
    , server_error(server_err ? *server_err : "")
{
    // For these errors, the server_error string is empty
    REALM_ASSERT(ErrorCodes::error_categories(ec).test(ErrorCategory::app_error));
}

std::ostream& operator<<(std::ostream& os, AppError error)
{
    return os << error.server_error << ": " << error.what();
}

} // namespace realm::app
