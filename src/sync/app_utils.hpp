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


#ifndef APP_UTILS_HPP
#define APP_UTILS_HPP

#include "sync/generic_network_transport.hpp"
#include "sync/auth_request_client.hpp"
#include <realm/util/optional.hpp>
#include <string>

namespace realm {
namespace app {
static util::Optional<AppError> check_for_errors(const Response& response)
{
    bool http_status_code_is_fatal = response.http_status_code >= 300 ||
        (response.http_status_code < 200 && response.http_status_code != 0);

    auto find_case_insensitive_header = [&response](const std::string& needle) {
        for (auto it = response.headers.begin(); it != response.headers.end(); ++it) {
            if (std::equal(it->first.begin(), it->first.end(),
                              needle.begin(), needle.end(),
                              [](char a, char b) {
                                  return tolower(a) == tolower(b);
            })) {
                return it;
            }
        }
        return response.headers.end();
    };

    try {
        auto ct = find_case_insensitive_header("content-type");
        if (ct != response.headers.end() && ct->second == "application/json") {
            auto body = nlohmann::json::parse(response.body);
            auto message = body.find("error");
            if (auto error_code = body.find("error_code"); error_code != body.end() &&
                !error_code->get<std::string>().empty())
            {
                return AppError(make_error_code(service_error_code_from_string(body["error_code"].get<std::string>())),
                                message != body.end() ? message->get<std::string>() : "no error message");
            } else if (message != body.end()) {
                return AppError(make_error_code(ServiceErrorCode::unknown), message->get<std::string>());
            }
        }
    } catch (const std::exception&) {
        // ignore parse errors from our attempt to read the error from json
    }

    if (response.custom_status_code != 0) {
        std::string error_msg = (!response.body.empty()) ? response.body : "non-zero custom status code considered fatal";
        return AppError(make_custom_error_code(response.custom_status_code), error_msg);
    }

    if (http_status_code_is_fatal)
    {
        return AppError(make_http_error_code(response.http_status_code), "http error code considered fatal");
    }

    return {};
}
} // namespace app
} // namespace realm

#endif /* APP_UTILS_HPP */
