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

#include "generic_network_transport.hpp"
#include <string>

namespace realm {
namespace app {

ServiceErrorCode ServiceError::error_code_for_string(const std::string& code)
{
    if (code == "MissingAuthReq")
        return ServiceErrorCode::missing_auth_req;
    else if (code == "InvalidSession")
        return ServiceErrorCode::invalid_session;
    else if (code == "UserAppDomainMismatch")
        return ServiceErrorCode::user_app_domain_mismatch;
    else if (code == "DomainNotAllowed")
        return ServiceErrorCode::domain_not_allowed;
    else if (code == "ReadSizeLimitExceeded")
        return ServiceErrorCode::read_size_limit_exceeded;
    else if (code == "InvalidParameter")
        return ServiceErrorCode::invalid_parameter;
    else if (code == "MissingParameter")
        return ServiceErrorCode::missing_parameter;
    else if (code == "TwilioError")
        return ServiceErrorCode::twilio_error;
    else if (code == "GCMError")
        return ServiceErrorCode::gcm_error;
    else if (code == "HTTPError")
        return ServiceErrorCode::http_error;
    else if (code == "AWSError")
        return ServiceErrorCode::aws_error;
    else if (code == "MongoDBError")
        return ServiceErrorCode::mongodb_error;
    else if (code == "ArgumentsNotAllowed")
        return ServiceErrorCode::arguments_not_allowed;
    else if (code == "FunctionExecutionError")
        return ServiceErrorCode::function_execution_error;
    else if (code == "NoMatchingRule")
        return ServiceErrorCode::no_matching_rule_found;
    else if (code == "InternalServerError")
        return ServiceErrorCode::internal_server_error;
    else if (code == "AuthProviderNotFound")
        return ServiceErrorCode::auth_provider_not_found;
    else if (code == "AuthProviderAlreadyExists")
        return ServiceErrorCode::auth_provider_already_exists;
    else if (code == "ServiceNotFound")
        return ServiceErrorCode::service_not_found;
    else if (code == "ServiceTypeNotFound")
        return ServiceErrorCode::service_type_not_found;
    else if (code == "ServiceAlreadyExists")
        return ServiceErrorCode::service_already_exists;
    else if (code == "ServiceCommandNotFound")
        return ServiceErrorCode::service_command_not_found;
    else if (code == "ValueNotFound")
        return ServiceErrorCode::value_not_found;
    else if (code == "ValueAlreadyExists")
        return ServiceErrorCode::value_already_exists;
    else if (code == "ValueDuplicateName")
        return ServiceErrorCode::value_duplicate_name;
    else if (code == "FunctionNotFound")
        return ServiceErrorCode::function_not_found;
    else if (code == "FunctionAlreadyExists")
        return ServiceErrorCode::function_already_exists;
    else if (code == "FunctionDuplicateName")
        return ServiceErrorCode::function_duplicate_name;
    else if (code == "FunctionSyntaxError")
        return ServiceErrorCode::function_syntax_error;
    else if (code == "FunctionInvalid")
        return ServiceErrorCode::function_invalid;
    else if (code == "IncomingWebhookNotFound")
        return ServiceErrorCode::incoming_webhook_not_found;
    else if (code == "IncomingWebhookAlreadyExists")
        return ServiceErrorCode::incoming_webhook_already_exists;
    else if (code == "IncomingWebhookDuplicateName")
        return ServiceErrorCode::incoming_webhook_duplicate_name;
    else if (code == "RuleNotFound")
        return ServiceErrorCode::rule_not_found;
    else if (code == "APIKeyNotFound")
        return ServiceErrorCode::api_key_not_found;
    else if (code == "RuleAlreadyExists")
        return ServiceErrorCode::rule_already_exists;
    else if (code == "AuthProviderDuplicateName")
        return ServiceErrorCode::auth_provider_duplicate_name;
    else if (code == "RestrictedHost")
        return ServiceErrorCode::restricted_host;
    else if (code == "APIKeyAlreadyExists")
        return ServiceErrorCode::api_key_already_exists;
    else if (code == "IncomingWebhookAuthFailed")
        return ServiceErrorCode::incoming_webhook_auth_failed;
    else if (code == "ExecutionTimeLimitExceeded")
        return ServiceErrorCode::execution_time_limit_exceeded;
    else if (code == "NotCallable")
        return ServiceErrorCode::not_callable;
    else if (code == "UserAlreadyConfirmed")
        return ServiceErrorCode::user_already_confirmed;
    else if (code == "UserNotFound")
        return ServiceErrorCode::user_not_found;
    else if (code == "UserDisabled")
        return ServiceErrorCode::user_disabled;
    else
        return ServiceErrorCode::unknown;
}

} // namespace app
} // namespace realm
