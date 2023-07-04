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

#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>

#include <external/json/json.hpp>

#include <algorithm>

namespace realm {
namespace app {

// Performs a case insensitive search to see of key_name is in the search_map
// Returns a pointer to the first key/value pair item found in search map
const std::pair<const std::string, std::string>*
AppUtils::find_header(const std::string& key_name, const std::map<std::string, std::string>& search_map)
{
    for (auto&& current : search_map) {
#ifdef _MSC_VER
        if (key_name.size() == current.first.size() && _stricmp(key_name.c_str(), current.first.c_str()) == 0) {
            return &current;
        }
#else
        if (key_name.size() == current.first.size() && strcasecmp(key_name.c_str(), current.first.c_str()) == 0) {
            return &current;
        }
#endif
    }
    return nullptr;
}

// Performs a case insensitive search to determine if needle is in haystack
size_t AppUtils::ifind_substr(const std::string_view haystack, const std::string_view needle)
{
    char first_char = std::tolower(needle[0]);
    size_t needle_len = needle.length();
    // Only need to check for the first character of needle through haystack minus the
    // length of needle plus one.
    size_t max_start_idx = haystack.length() - needle_len + 1;
    for (size_t idx_a = 0; idx_a < max_start_idx; idx_a++) {
        if (std::tolower(haystack[idx_a]) == first_char) {
            size_t cmp = 1;
            while (cmp < needle_len && std::tolower(haystack[idx_a + cmp]) == std::tolower(needle[cmp])) {
                cmp++;
            }
            // All characters matched
            if (cmp == needle_len) {
                return idx_a;
            }
        }
    }
    return std::string_view::npos;
}

util::Optional<AppError> AppUtils::check_for_errors(const Response& response)
{
    std::string error_msg;
    bool http_status_code_is_fatal =
        response.http_status_code >= 300 || (response.http_status_code < 200 && response.http_status_code != 0);

    auto ct = AppUtils::find_header("content-type", response.headers);
    if (ct && AppUtils::ifind_substr(ct->second, "application/json") != std::string_view::npos) {
        try {
            auto body = nlohmann::json::parse(response.body);
            auto message = body.find("error");
            auto link = body.find("link");
            std::string parsed_link = link == body.end() ? "" : link->get<std::string>();

            if (auto error_code = body.find("error_code");
                error_code != body.end() && !error_code->get<std::string>().empty()) {
                return AppError(ErrorCodes::from_string(body["error_code"].get<std::string>()),
                                message != body.end() ? message->get<std::string>() : "no error message",
                                std::move(parsed_link), response.http_status_code);
            }
            else if (message != body.end()) {
                return AppError(ErrorCodes::AppUnknownError, message->get<std::string>(), std::move(parsed_link),
                                response.http_status_code);
            }
        }
        catch (const std::exception&) {
            // ignore parse errors from our attempt to read the error from json
        }
    }

    if (response.client_error_code) {
        error_msg = response.body.empty() ? "client error code value considered fatal" : response.body;
        return AppError(*(response.client_error_code), error_msg, "", response.http_status_code);
    }

    if (response.custom_status_code != 0) {
        error_msg = response.body.empty() ? "non-zero custom status code considered fatal" : response.body;
        return AppError(ErrorCodes::CustomError, error_msg, "", response.custom_status_code);
    }

    if (http_status_code_is_fatal) {
        error_msg = response.body.empty() ? "http error code considered fatal"
                                          : "http error code considered fatal: " + response.body;
        return AppError(ErrorCodes::HTTPError, error_msg, "", response.http_status_code);
    }

    return {};
}

} // namespace app
} // namespace realm
