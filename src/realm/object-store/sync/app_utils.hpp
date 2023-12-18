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

#include <realm/util/optional.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>

#include <map>

namespace realm::app {

class AppUtils {
public:
    static util::Optional<AppError> check_for_errors(const Response& response);
    static Response get_response_from_apperror(const AppError& error);
    static const std::pair<const std::string, std::string>*
    find_header(const std::string& key_name, const std::map<std::string, std::string>& search_map);
    static bool split_url(std::string url, std::string& scheme, std::string& dest, std::string& request);
    static bool is_success_status_code(int status_code);
    static bool is_redirect_status_code(int status_code);
    static util::Optional<std::string> extract_redir_location(const Response& response);
};

// Internal class for passing a response and/or app error between callbacks
struct AppResponse {
    const Response& response; // minimize copies of response data
    std::optional<AppError> error;

    explicit AppResponse(const Response& resp)
        : response(resp)
        , has_response(true)
    {
        error = AppUtils::check_for_errors(response);
    }

    explicit AppResponse(AppError&& error)
        : response(local_response) // dummy empty response
        , error(std::move(error))
        , has_response(false)
    {
    }

    bool is_ok() const
    {
        return !error && has_response;
    }

    std::string_view body() const
    {
        if (has_response) {
            return response.body;
        }
        return {};
    }

    int status_code() const
    {
        if (has_response) {
            return response.http_status_code;
        }
        if (error && error->additional_status_code) {
            return *error->additional_status_code;
        }
        return 0;
    }

private:
    Response local_response;
    bool has_response;
};

} // namespace realm::app

#endif /* APP_UTILS_HPP */
