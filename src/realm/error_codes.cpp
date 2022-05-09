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

#include "realm/error_codes.hpp"
#include <string_view>
#include <ostream>
#include <map>

namespace realm {

ErrorCategory ErrorCodes::error_categories(Error code)
{
    switch (code) {
        case FileOperationFailed:
        case PermissionDenied:
        case FileNotFound:
        case FileAlreadyExists:
        case InvalidDatabase:
        case DecryptionFailed:
        case IncompatibleHistories:
        case FileFormatUpgradeRequired:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::file_access);
        case SystemError:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::system_error);
        case RuntimeError:
        case RangeError:
        case IncompatibleSession:
        case IncompatibleLockFile:
        case InvalidQuery:
        case BrokenInvariant:
        case DuplicatePrimaryKeyValue:
        case OutOfMemory:
        case UnsupportedFileFormatVersion:
        case MultipleSyncAgents:
        case AddressSpaceExhausted:
        case ObjectAlreadyExists:
        case OutOfDiskSpace:
        case CallbackFailed:
        case NotCloneable:
        case MaximumFileSizeExceeded:
        case BadChangeset:
        case SubscriptionFailed:
            return ErrorCategory().set(ErrorCategory::runtime_error);
        case InvalidArgument:
        case PropertyNotNullable:
        case InvalidProperty:
        case InvalidName:
        case InvalidDictionaryValue:
        case InvalidSortDescriptor:
        case SyntaxError:
        case InvalidQueryArg:
        case KeyNotFound:
        case OutOfBounds:
        case LimitExceeded:
        case UnexpectedPrimaryKey:
        case ModifyPrimaryKey:
        case ReadOnlyProperty:
        case TypeMismatch:
        case ObjectTypeMismatch:
        case MissingPrimaryKey:
        case MissingPropertyValue:
        case NoSuchTable:
        case TableNameInUse:
        case InvalidDictionaryKey:
        case InvalidEncryptionKey:
        case IllegalCombination:
            return ErrorCategory().set(ErrorCategory::invalid_argument).set(ErrorCategory::logic_error);
        case LogicError:
        case BrokenPromise:
        case CrossTableLinkTarget:
        case KeyAlreadyUsed:
        case WrongTransactionState:
        case SerializationError:
        case IllegalOperation:
        case StaleAccessor:
        case ReadOnly:
        case InvalidatedObject:
        case NotSupported:
        case WrongThread:
        case InvalidTableRef:
        case DeleteOnOpenRealm:
        case MismatchedConfig:
        case ClosedRealm:
        case InvalidSchemaVersion:
        case SchemaValidationFailed:
        case SchemaMismatch:
        case InvalidAdditiveSchemaChange:
        case InvalidReadOnlySchemaChange:
        case InvalidExternalSchemaChange:
            return ErrorCategory().set(ErrorCategory::logic_error);
        case CustomError:
            return ErrorCategory().set(ErrorCategory::app_error).set(ErrorCategory::custom_error);
        case HTTPError:
            return ErrorCategory().set(ErrorCategory::app_error).set(ErrorCategory::http_error);
        case ClientUserNotFound:
        case ClientUserNotLoggedIn:
        case ClientAppDeallocated:
            return ErrorCategory().set(ErrorCategory::app_error).set(ErrorCategory::client_error);
        case BadToken:
        case MalformedJson:
        case MissingJsonKey:
        case BadBsonParse:
            return ErrorCategory().set(ErrorCategory::app_error).set(ErrorCategory::json_error);
        case MissingAuthReq:
        case InvalidSession:
        case UserAppDomainMismatch:
        case DomainNotAllowed:
        case ReadSizeLimitExceeded:
        case InvalidParameter:
        case MissingParameter:
        case TwilioError:
        case GCMError:
        case AWSError:
        case MongoDBError:
        case ArgumentsNotAllowed:
        case FunctionExecutionError:
        case NoMatchingRule:
        case InternalServerError:
        case AuthProviderNotFound:
        case AuthProviderAlreadyExists:
        case ServiceNotFound:
        case ServiceTypeNotFound:
        case ServiceAlreadyExists:
        case ServiceCommandNotFound:
        case ValueNotFound:
        case ValueAlreadyExists:
        case ValueDuplicateName:
        case FunctionNotFound:
        case FunctionAlreadyExists:
        case FunctionDuplicateName:
        case FunctionSyntaxError:
        case FunctionInvalid:
        case IncomingWebhookNotFound:
        case IncomingWebhookAlreadyExists:
        case IncomingWebhookDuplicateName:
        case RuleNotFound:
        case APIKeyNotFound:
        case RuleAlreadyExists:
        case RuleDuplicateName:
        case AuthProviderDuplicateName:
        case RestrictedHost:
        case APIKeyAlreadyExists:
        case IncomingWebhookAuthFailed:
        case ExecutionTimeLimitExceeded:
        case NotCallable:
        case UserAlreadyConfirmed:
        case UserNotFound:
        case UserDisabled:
        case AuthError:
        case BadRequest:
        case AccountNameInUse:
        case InvalidPassword:
        case SchemaValidationFailedWrite:
        case UnknownError:
            return ErrorCategory().set(ErrorCategory::app_error).set(ErrorCategory::service_error);
        case OK:
        case GenericError:
        case MaxError:
            break;
    }
    return {};
}

static const std::map<std::string_view, ErrorCodes::Error> error_codes_map = {
    {"OK", ErrorCodes::OK},
    {"UnknownError", ErrorCodes::UnknownError},
    {"GenericError", ErrorCodes::GenericError},
    {"RuntimeError", ErrorCodes::RuntimeError},
    {"LogicError", ErrorCodes::LogicError},
    {"InvalidArgument", ErrorCodes::InvalidArgument},
    {"BrokenPromise", ErrorCodes::BrokenPromise},
    {"InvalidName", ErrorCodes::InvalidName},
    {"OutOfMemory", ErrorCodes::OutOfMemory},
    {"NoSuchTable", ErrorCodes::NoSuchTable},
    {"CrossTableLinkTarget", ErrorCodes::CrossTableLinkTarget},
    {"UnsupportedFileFormatVersion", ErrorCodes::UnsupportedFileFormatVersion},
    {"FileFormatUpgradeRequired", ErrorCodes::FileFormatUpgradeRequired},
    {"MultipleSyncAgents", ErrorCodes::MultipleSyncAgents},
    {"AddressSpaceExhausted", ErrorCodes::AddressSpaceExhausted},
    {"OutOfDiskSpace", ErrorCodes::OutOfDiskSpace},
    {"MaximumFileSizeExceeded", ErrorCodes::MaximumFileSizeExceeded},
    {"KeyNotFound", ErrorCodes::KeyNotFound},
    {"OutOfBounds", ErrorCodes::OutOfBounds},
    {"IllegalOperation", ErrorCodes::IllegalOperation},
    {"KeyAlreadyUsed", ErrorCodes::KeyAlreadyUsed},
    {"SerializationError", ErrorCodes::SerializationError},
    {"DuplicatePrimaryKeyValue", ErrorCodes::DuplicatePrimaryKeyValue},
    {"SyntaxError", ErrorCodes::SyntaxError},
    {"InvalidQueryArg", ErrorCodes::InvalidQueryArg},
    {"InvalidQuery", ErrorCodes::InvalidQuery},
    {"WrongTransactionState", ErrorCodes::WrongTransactionState},
    {"WrongThread", ErrorCodes::WrongThread},
    {"InvalidatedObject", ErrorCodes::InvalidatedObject},
    {"InvalidProperty", ErrorCodes::InvalidProperty},
    {"MissingPrimaryKey", ErrorCodes::MissingPrimaryKey},
    {"UnexpectedPrimaryKey", ErrorCodes::UnexpectedPrimaryKey},
    {"ObjectAlreadyExists", ErrorCodes::ObjectAlreadyExists},
    {"ModifyPrimaryKey", ErrorCodes::ModifyPrimaryKey},
    {"ReadOnly", ErrorCodes::ReadOnly},
    {"PropertyNotNullable", ErrorCodes::PropertyNotNullable},
    {"TableNameInUse", ErrorCodes::TableNameInUse},
    {"InvalidTableRef", ErrorCodes::InvalidTableRef},
    {"BadChangeset", ErrorCodes::BadChangeset},
    {"InvalidDictionaryKey", ErrorCodes::InvalidDictionaryKey},
    {"InvalidDictionaryValue", ErrorCodes::InvalidDictionaryValue},
    {"StaleAccessor", ErrorCodes::StaleAccessor},
    {"IncompatibleLockFile", ErrorCodes::IncompatibleLockFile},
    {"InvalidSortDescriptor", ErrorCodes::InvalidSortDescriptor},
    {"DecryptionFailed", ErrorCodes::DecryptionFailed},
    {"IncompatibleSession", ErrorCodes::IncompatibleSession},
    {"BrokenInvariant", ErrorCodes::BrokenInvariant},
    {"SubscriptionFailed", ErrorCodes::SubscriptionFailed},
    {"RangeError", ErrorCodes::RangeError},
    {"TypeMismatch", ErrorCodes::TypeMismatch},
    {"ObjectTypeMismatch", ErrorCodes::ObjectTypeMismatch},
    {"LimitExceeded", ErrorCodes::LimitExceeded},
    {"MissingPropertyValue", ErrorCodes::MissingPropertyValue},
    {"ReadOnlyProperty", ErrorCodes::ReadOnlyProperty},
    {"CallbackFailed", ErrorCodes::CallbackFailed},
    {"NotCloneable", ErrorCodes::NotCloneable},
    {"PermissionDenied", ErrorCodes::PermissionDenied},
    {"FileOperationFailed", ErrorCodes::FileOperationFailed},
    {"FileNotFound", ErrorCodes::FileNotFound},
    {"FileAlreadyExists", ErrorCodes::FileAlreadyExists},
    {"SystemError", ErrorCodes::SystemError},
    {"InvalidDatabase", ErrorCodes::InvalidDatabase},
    {"IncompatibleHistories", ErrorCodes::IncompatibleHistories},
    {"InvalidEncryptionKey", ErrorCodes::InvalidEncryptionKey},
    {"InvalidCombination", ErrorCodes::IllegalCombination},
    {"MismatchedConfig", ErrorCodes::MismatchedConfig},
    {"ClosedRealm", ErrorCodes::ClosedRealm},
    {"InvalidSchemaVersion", ErrorCodes::InvalidSchemaVersion},
    {"SchemaValidationFailed", ErrorCodes::SchemaValidationFailed},
    {"SchemaMismatch", ErrorCodes::SchemaMismatch},
    {"InvalidAdditiveSchemaChange", ErrorCodes::InvalidAdditiveSchemaChange},
    {"InvalidReadOnlySchemaChange", ErrorCodes::InvalidReadOnlySchemaChange},
    {"InvalidExternalSchemaChange", ErrorCodes::InvalidExternalSchemaChange},
    {"DeleteOnOpenRealm", ErrorCodes::DeleteOnOpenRealm},
    {"NotSupported", ErrorCodes::NotSupported},
    {"ClientUserNotFound", ErrorCodes::ClientUserNotFound},
    {"ClientUserNotLogged_in", ErrorCodes::ClientUserNotLoggedIn},
    {"ClientAppDeallocated", ErrorCodes::ClientAppDeallocated},
    {"BadToken", ErrorCodes::BadToken},
    {"MalformedJson", ErrorCodes::MalformedJson},
    {"MissingJsonKey", ErrorCodes::MissingJsonKey},
    {"BadBsonParse", ErrorCodes::BadBsonParse},

    {"CustomError", ErrorCodes::CustomError},
    {"MissingAuthReq", ErrorCodes::MissingAuthReq},
    {"InvalidSession", ErrorCodes::InvalidSession},
    {"UserAppDomainMismatch", ErrorCodes::UserAppDomainMismatch},
    {"DomainNotAllowed", ErrorCodes::DomainNotAllowed},
    {"ReadSizeLimitExceeded", ErrorCodes::ReadSizeLimitExceeded},
    {"InvalidParameter", ErrorCodes::InvalidParameter},
    {"MissingParameter", ErrorCodes::MissingParameter},
    {"TwilioError", ErrorCodes::TwilioError},
    {"GCMError", ErrorCodes::GCMError},
    {"HTTPError", ErrorCodes::HTTPError},
    {"AWSError", ErrorCodes::AWSError},
    {"MongoDBError", ErrorCodes::MongoDBError},
    {"ArgumentsNotAllowed", ErrorCodes::ArgumentsNotAllowed},
    {"FunctionExecutionError", ErrorCodes::FunctionExecutionError},
    {"NoMatchingRule", ErrorCodes::NoMatchingRule},
    {"InternalServerError", ErrorCodes::InternalServerError},
    {"AuthProviderNotFound", ErrorCodes::AuthProviderNotFound},
    {"AuthProviderAlreadyExists", ErrorCodes::AuthProviderAlreadyExists},
    {"ServiceNotFound", ErrorCodes::ServiceNotFound},
    {"ServiceTypeNotFound", ErrorCodes::ServiceTypeNotFound},
    {"ServiceAlreadyExists", ErrorCodes::ServiceAlreadyExists},
    {"ServiceCommandNotFound", ErrorCodes::ServiceCommandNotFound},
    {"ValueNotFound", ErrorCodes::ValueNotFound},
    {"ValueAlreadyExists", ErrorCodes::ValueAlreadyExists},
    {"ValueDuplicateName", ErrorCodes::ValueDuplicateName},
    {"FunctionNotFound", ErrorCodes::FunctionNotFound},
    {"FunctionAlreadyExists", ErrorCodes::FunctionAlreadyExists},
    {"FunctionDuplicateName", ErrorCodes::FunctionDuplicateName},
    {"FunctionSyntaxError", ErrorCodes::FunctionSyntaxError},
    {"FunctionInvalid", ErrorCodes::FunctionInvalid},
    {"IncomingWebhookNotFound", ErrorCodes::IncomingWebhookNotFound},
    {"IncomingWebhookAlreadyExists", ErrorCodes::IncomingWebhookAlreadyExists},
    {"IncomingWebhookDuplicateName", ErrorCodes::IncomingWebhookDuplicateName},
    {"RuleNotFound", ErrorCodes::RuleNotFound},
    {"APIKeyNotFound", ErrorCodes::APIKeyNotFound},
    {"RuleAlreadyExists", ErrorCodes::RuleAlreadyExists},
    {"RuleDuplicateName", ErrorCodes::RuleDuplicateName},
    {"AuthProviderDuplicateName", ErrorCodes::AuthProviderDuplicateName},
    {"RestrictedHost", ErrorCodes::RestrictedHost},
    {"APIKeyAlreadyExists", ErrorCodes::APIKeyAlreadyExists},
    {"IncomingWebhookAuthFailed", ErrorCodes::IncomingWebhookAuthFailed},
    {"ExecutionTimeLimitExceeded", ErrorCodes::ExecutionTimeLimitExceeded},
    {"NotCallable", ErrorCodes::NotCallable},
    {"UserAlreadyConfirmed", ErrorCodes::UserAlreadyConfirmed},
    {"UserNotFound", ErrorCodes::UserNotFound},
    {"UserDisabled", ErrorCodes::UserDisabled},
    {"AuthError", ErrorCodes::AuthError},
    {"BadRequest", ErrorCodes::BadRequest},
    {"AccountNameInUse", ErrorCodes::AccountNameInUse},
    {"InvalidPassword", ErrorCodes::InvalidPassword},
    {"SchemaValidationFailedWrite", ErrorCodes::SchemaValidationFailedWrite},
};

std::string_view ErrorCodes::error_string(Error code)
{
    for (auto it : error_codes_map) {
        if (it.second == code) {
            return it.first;
        }
    }
    return "unknown";
}

ErrorCodes::Error ErrorCodes::from_string(std::string_view name)
{
    auto it = error_codes_map.find(name);
    if (it != error_codes_map.end()) {
        return it->second;
    }
    return UnknownError;
}

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code)
{
    return stream << ErrorCodes::error_string(code);
}

} // namespace realm
