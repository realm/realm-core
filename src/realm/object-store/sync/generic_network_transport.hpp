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

#ifndef REALM_GENERIC_NETWORK_TRANSPORT_HPP
#define REALM_GENERIC_NETWORK_TRANSPORT_HPP

#include <realm/exceptions.hpp>
#include <realm/util/functional.hpp>
#include <realm/util/optional.hpp>

#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace realm::app {

struct AppError : public Exception {
    util::Optional<int> additional_status_code;

    std::string link_to_server_logs;

    AppError(ErrorCodes::Error error_code, std::string message, std::string link = "",
             util::Optional<int> additional_error_code = util::none);

    bool is_json_error() const
    {
        return ErrorCodes::error_categories(code()).test(ErrorCategory::json_error);
    }

    bool is_service_error() const
    {
        return ErrorCodes::error_categories(code()).test(ErrorCategory::service_error);
    }

    bool is_http_error() const
    {
        return ErrorCodes::error_categories(code()).test(ErrorCategory::http_error);
    }

    bool is_custom_error() const
    {
        return ErrorCodes::error_categories(code()).test(ErrorCategory::custom_error);
    }

    bool is_client_error() const
    {
        return ErrorCodes::error_categories(code()).test(ErrorCategory::client_error);
    }
};

std::ostream& operator<<(std::ostream& os, AppError error);

/**
 * An HTTP method type.
 */
enum class HttpMethod { get, post, patch, put, del };

/**
 * An HTTP request that can be made to an arbitrary server.
 */
struct Request {
    /**
     * The HTTP method of this request.
     */
    HttpMethod method = HttpMethod::get;

    /**
     * The URL to which this request will be made.
     */
    std::string url;

    /**
     * The number of milliseconds that the underlying transport should spend on an HTTP round trip before failing with
     * an error.
     */
    uint64_t timeout_ms = 0;

    /**
     * The HTTP headers of this request.
     */
    std::map<std::string, std::string> headers;

    /**
     * The body of the request.
     */
    std::string body;

    /// Indicates if the request uses the refresh token or the access token
    bool uses_refresh_token = false;
};

/**
 * The contents of an HTTP response.
 */
struct Response {
    /**
     * The status code of the HTTP response.
     */
    int http_status_code;

    /**
     * A custom status code provided by the language binding.
     */
    int custom_status_code;

    /**
     * The headers of the HTTP response.
     */
    std::map<std::string, std::string> headers;

    /**
     * The body of the HTTP response.
     */
    std::string body;
};

/// Generic network transport for foreign interfaces.
struct GenericNetworkTransport {
    virtual void send_request_to_server(Request&& request,
                                        util::UniqueFunction<void(const Response&)>&& completionBlock) = 0;
    virtual ~GenericNetworkTransport() = default;
};

} // namespace realm::app

#endif /* REALM_GENERIC_NETWORK_TRANSPORT_HPP */
