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

#include "sync/generic_network_transport.hpp"

#include <string>

namespace realm {
namespace app {

namespace {

std::string get_error_message(JSONErrorCode error)
{
    switch (error) {
        case JSONErrorCode::bad_token:
            return "bad token";
        case JSONErrorCode::malformed_json:
            return "malformed json";
        case JSONErrorCode::missing_json_key:
            return "missing json key";
        case JSONErrorCode::bad_bson_parse:
            return "bad bson parse";
    }
    return "unknown";
}

struct JSONErrorCategory : public std::error_category {
    const char* name() const noexcept final override
    {
        return "realm::app::JSONError";
    }

    std::string message(int error_code) const override final
    {
        return get_error_message(JSONErrorCode(error_code));
    }
};

JSONErrorCategory g_json_error_category;

static const std::map<std::string, ServiceErrorCode> service_error_map = {
    {"MissingAuthReq", ServiceErrorCode::missing_auth_req},
    {"InvalidSession", ServiceErrorCode::invalid_session},
    {"UserAppDomainMismatch", ServiceErrorCode::user_app_domain_mismatch},
    {"DomainNotAllowed", ServiceErrorCode::domain_not_allowed},
    {"ReadSizeLimitExceeded", ServiceErrorCode::read_size_limit_exceeded},
    {"InvalidParameter", ServiceErrorCode::invalid_parameter},
    {"MissingParameter", ServiceErrorCode::missing_parameter},
    {"TwilioError", ServiceErrorCode::twilio_error},
    {"GCMError", ServiceErrorCode::gcm_error},
    {"HTTPError", ServiceErrorCode::http_error},
    {"AWSError", ServiceErrorCode::aws_error},
    {"MongoDBError", ServiceErrorCode::mongodb_error},
    {"ArgumentsNotAllowed", ServiceErrorCode::arguments_not_allowed},
    {"FunctionExecutionError", ServiceErrorCode::function_execution_error},
    {"NoMatchingRule", ServiceErrorCode::no_matching_rule_found},
    {"InternalServerError", ServiceErrorCode::internal_server_error},
    {"AuthProviderNotFound", ServiceErrorCode::auth_provider_not_found},
    {"AuthProviderAlreadyExists", ServiceErrorCode::auth_provider_already_exists},
    {"ServiceNotFound", ServiceErrorCode::service_not_found},
    {"ServiceTypeNotFound", ServiceErrorCode::service_type_not_found},
    {"ServiceAlreadyExists", ServiceErrorCode::service_already_exists},
    {"ServiceCommandNotFound", ServiceErrorCode::service_command_not_found},
    {"ValueNotFound", ServiceErrorCode::value_not_found},
    {"ValueAlreadyExists", ServiceErrorCode::value_already_exists},
    {"ValueDuplicateName", ServiceErrorCode::value_duplicate_name},
    {"FunctionNotFound", ServiceErrorCode::function_not_found},
    {"FunctionAlreadyExists", ServiceErrorCode::function_already_exists},
    {"FunctionDuplicateName", ServiceErrorCode::function_duplicate_name},
    {"FunctionSyntaxError", ServiceErrorCode::function_syntax_error},
    {"FunctionInvalid", ServiceErrorCode::function_invalid},
    {"IncomingWebhookNotFound", ServiceErrorCode::incoming_webhook_not_found},
    {"IncomingWebhookAlreadyExists", ServiceErrorCode::incoming_webhook_already_exists},
    {"IncomingWebhookDuplicateName", ServiceErrorCode::incoming_webhook_duplicate_name},
    {"RuleNotFound", ServiceErrorCode::rule_not_found},
    {"APIKeyNotFound", ServiceErrorCode::api_key_not_found},
    {"RuleAlreadyExists", ServiceErrorCode::rule_already_exists},
    {"AuthProviderDuplicateName", ServiceErrorCode::auth_provider_duplicate_name},
    {"RestrictedHost", ServiceErrorCode::restricted_host},
    {"APIKeyAlreadyExists", ServiceErrorCode::api_key_already_exists},
    {"IncomingWebhookAuthFailed", ServiceErrorCode::incoming_webhook_auth_failed},
    {"ExecutionTimeLimitExceeded", ServiceErrorCode::execution_time_limit_exceeded},
    {"NotCallable", ServiceErrorCode::not_callable},
    {"UserAlreadyConfirmed", ServiceErrorCode::user_already_confirmed},
    {"UserNotFound", ServiceErrorCode::user_not_found},
    {"UserDisabled", ServiceErrorCode::user_disabled},
    {"AuthError", ServiceErrorCode::auth_error},
    {"BadRequest", ServiceErrorCode::bad_request},
    {"AccountNameInUse", ServiceErrorCode::account_name_in_use},
    {"Unknown", ServiceErrorCode::unknown},
};

std::string get_error_message(ServiceErrorCode error)
{
    for (auto it : service_error_map) {
        if (it.second == error) {
            return it.first;
        }
    }
    return "unknown";
}

struct ServiceErrorCategory : public std::error_category {
    const char* name() const noexcept final override
    {
        return "realm::app::ServiceError";
    }

    std::string message(int error_code) const override final
    {
        return get_error_message(ServiceErrorCode(error_code));
    }
};

ServiceErrorCategory g_service_error_category;


struct HttpErrorCategory : public std::error_category {
    const char* name() const noexcept final override
    {
        return "realm::app::HttpError";
    }

    std::string message(int code) const override final
    {
        if (code >= 100 && code < 200) {
            return util::format("Informational: %1", code);
        }
        else if (code >= 200 && code < 300) {
            return util::format("Success: %1", code);
        }
        else if (code >= 300 && code < 400) {
            return util::format("Redirection: %1", code);
        }
        else if (code >= 400 && code < 500) {
            return util::format("Client Error: %1", code);
        }
        else if (code >= 500 && code < 600) {
            return util::format("Server Error: %1", code);
        }
        return util::format("Unknown HTTP Error: %1", code);
    }
};

HttpErrorCategory g_http_error_category;

struct CustomErrorCategory : public std::error_category {
    const char* name() const noexcept final override
    {
        return "realm::app::CustomError";
    }

    std::string message(int code) const override final
    {
        return util::format("code %1", code);
    }
};

CustomErrorCategory g_custom_error_category;

struct ClientErrorCategory : public std::error_category {
    const char* name() const noexcept final override
    {
        return "realm::app::ClientError";
    }

    std::string message(int code) const override final
    {
        return util::format("code %1", code);
    }
};

ClientErrorCategory g_client_error_category;

} // unnamed namespace

std::ostream& operator<<(std::ostream& os, AppError error)
{
    return os << error.error_code.message() << ": " << error.message;
}

const std::error_category& json_error_category() noexcept
{
    return g_json_error_category;
}

std::error_code make_error_code(JSONErrorCode error) noexcept
{
    return std::error_code{int(error), g_json_error_category};
}

const std::error_category& service_error_category() noexcept
{
    return g_service_error_category;
}

std::error_code make_error_code(ServiceErrorCode error) noexcept
{
    return std::error_code{int(error), g_service_error_category};
}

ServiceErrorCode service_error_code_from_string(const std::string& code)
{
    auto search = service_error_map.find(code);
    if (search != service_error_map.end()) {
        return search->second;
    }
    return ServiceErrorCode::unknown;
}

const std::error_category& http_error_category() noexcept
{
    return g_http_error_category;
}

std::error_code make_http_error_code(int http_code) noexcept
{
    return std::error_code{http_code, g_http_error_category};
}

const std::error_category& custom_error_category() noexcept
{
    return g_custom_error_category;
}

std::error_code make_custom_error_code(int code) noexcept
{
    return std::error_code{code, g_custom_error_category};
}

const std::error_category& client_error_category() noexcept
{
    return g_client_error_category;
}

std::error_code make_client_error_code(ClientErrorCode error) noexcept
{
    return std::error_code{int(error), g_client_error_category};
}

} // namespace app
} // namespace realm
