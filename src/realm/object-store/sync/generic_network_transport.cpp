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
#include <iostream>
namespace realm::app {

static std::string http_message(int code)
{
    if (code >= 100 && code < 200) {
        return util::format(". Informational: %1", code);
    }
    else if (code >= 200 && code < 300) {
        return util::format(". Success: %1", code);
    }
    else if (code >= 300 && code < 400) {
        return util::format(". Redirection: %1", code);
    }
    else if (code >= 400 && code < 500) {
        return util::format(". Client Error: %1", code);
    }
    else if (code >= 500 && code < 600) {
        return util::format(". Server Error: %1", code);
    }
    return util::format(". Unknown HTTP Error: %1", code);
}

AppError::AppError(ErrorCodes::Error error_code, std::string message, std::string link,
                   util::Optional<int> additional_error_code)
    : Exception(error_code,
                message + (error_code == ErrorCodes::HTTPError ? http_message(*additional_error_code) : ""))
    , additional_status_code(additional_error_code)
    , link_to_server_logs(link)
{
    REALM_ASSERT(ErrorCodes::error_categories(error_code).test(ErrorCategory::app_error));
}

std::ostream& operator<<(std::ostream& os, AppError error)
{
    return os << ErrorCodes::error_string(error.code()) << ": " << error.what();
}

} // namespace realm::app
