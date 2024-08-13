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

#include <realm/error_codes.hpp>
#include <realm/util/assert.hpp>

#include <iostream>
#include <string_view>
#include <algorithm>
#include <cstring>

namespace realm {

ErrorCategory ErrorCodes::error_categories(Error code)
{
    switch (code) {
        case OK:
            break;

        case AddressSpaceExhausted:
        case BadVersion:
        case BrokenInvariant:
        case CallbackFailed:
        case MaximumFileSizeExceeded:
        case MultipleSyncAgents:
        case NoSubscriptionForWrite:
        case NotCloneable:
        case ObjectAlreadyExists:
        case OutOfDiskSpace:
        case OutOfMemory:
        case RangeError:
        case RuntimeError:
        case SchemaVersionMismatch:
        case UnsupportedFileFormatVersion:
        case OperationAborted:
            return ErrorCategory().set(ErrorCategory::runtime_error);

        case AutoClientResetFailed:
        case BadChangeset:
        case ConnectionClosed:
        case SubscriptionFailed:
        case SyncClientResetRequired:
        case SyncCompensatingWrite:
        case SyncPermissionDenied:
        case SyncProtocolInvariantFailed:
        case SyncServerPermissionsChanged:
        case SyncUserMismatch:
        case SyncWriteNotAllowed:
        case SyncLocalClockBeforeEpoch:
        case SyncSchemaMigrationError:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::sync_error);

        case SyncConnectFailed:
        case SyncConnectTimeout:
        case SyncProtocolNegotiationFailed:
        case TlsHandshakeFailed:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::websocket_error)
                .set(ErrorCategory::sync_error);

        case DecryptionFailed:
        case DeleteOnOpenRealm:
        case FileAlreadyExists:
        case FileFormatUpgradeRequired:
        case FileNotFound:
        case FileOperationFailed:
        case IncompatibleHistories:
        case IncompatibleLockFile:
        case IncompatibleSession:
        case InvalidDatabase:
        case PermissionDenied:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::file_access);

        case SystemError:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::system_error);

        case BrokenPromise:
        case ClosedRealm:
        case CrossTableLinkTarget:
        case IllegalOperation:
        case InvalidSchemaChange:
        case InvalidSchemaVersion:
        case InvalidTableRef:
        case InvalidatedObject:
        case KeyAlreadyUsed:
        case LogicError:
        case MigrationFailed:
        case MismatchedConfig:
        case NotSupported:
        case ReadOnlyDB:
        case SchemaMismatch:
        case SchemaValidationFailed:
        case SerializationError:
        case StaleAccessor:
        case WrongThread:
        case WrongTransactionState:
            return ErrorCategory().set(ErrorCategory::logic_error);

        case BadServerUrl:
        case IllegalCombination:
        case InvalidArgument:
        case InvalidDictionaryKey:
        case InvalidDictionaryValue:
        case InvalidEncryptionKey:
        case InvalidName:
        case InvalidProperty:
        case InvalidQuery:
        case InvalidQueryArg:
        case InvalidSortDescriptor:
        case KeyNotFound:
        case LimitExceeded:
        case MissingPrimaryKey:
        case MissingPropertyValue:
        case ModifyPrimaryKey:
        case NoSuchTable:
        case ObjectTypeMismatch:
        case OutOfBounds:
        case PropertyNotNullable:
        case ReadOnlyProperty:
        case SyntaxError:
        case TableNameInUse:
        case TopLevelObject:
        case TypeMismatch:
        case UnexpectedPrimaryKey:
            return ErrorCategory().set(ErrorCategory::invalid_argument).set(ErrorCategory::logic_error);

        case BadSyncPartitionValue:
        case InvalidSubscriptionQuery:
        case SyncInvalidSchemaChange:
        case WrongSyncType:
            return ErrorCategory()
                .set(ErrorCategory::invalid_argument)
                .set(ErrorCategory::logic_error)
                .set(ErrorCategory::sync_error);

        case CustomError:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::custom_error);

        case HTTPError:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::http_error);

        case ClientRedirectError:
        case ClientTooManyRedirects:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::client_error);

        case ClientUserNotFound:
        case ClientUserNotLoggedIn:
        case ClientUserAlreadyNamed:
            return ErrorCategory()
                .set(ErrorCategory::logic_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::client_error);

        case BadBsonParse:
        case BadToken:
        case MalformedJson:
        case MissingJsonKey:
            return ErrorCategory()
                .set(ErrorCategory::logic_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::json_error);

        case APIKeyAlreadyExists:
        case APIKeyNotFound:
        case AWSError:
        case AccountNameInUse:
        case AppServerError:
        case AppUnknownError:
        case ArgumentsNotAllowed:
        case AuthError:
        case AuthProviderAlreadyExists:
        case AuthProviderDuplicateName:
        case AuthProviderNotFound:
        case BadRequest:
        case DomainNotAllowed:
        case ExecutionTimeLimitExceeded:
        case FunctionAlreadyExists:
        case FunctionDuplicateName:
        case FunctionExecutionError:
        case FunctionInvalid:
        case FunctionNotFound:
        case FunctionSyntaxError:
        case GCMError:
        case IncomingWebhookAlreadyExists:
        case IncomingWebhookAuthFailed:
        case IncomingWebhookDuplicateName:
        case IncomingWebhookNotFound:
        case InternalServerError:
        case InvalidParameter:
        case InvalidPassword:
        case InvalidSession:
        case MaintenanceInProgress:
        case MissingAuthReq:
        case MissingParameter:
        case MongoDBError:
        case NoMatchingRuleFound:
        case NotCallable:
        case ReadSizeLimitExceeded:
        case RestrictedHost:
        case RuleAlreadyExists:
        case RuleDuplicateName:
        case RuleNotFound:
        case SchemaValidationFailedWrite:
        case ServiceAlreadyExists:
        case ServiceCommandNotFound:
        case ServiceNotFound:
        case ServiceTypeNotFound:
        case TwilioError:
        case UserAlreadyConfirmed:
        case UserAppDomainMismatch:
        case UserDisabled:
        case UserNotFound:
        case UserpassTokenInvalid:
        case InvalidServerResponse:
        case ValueAlreadyExists:
        case ValueDuplicateName:
        case ValueNotFound:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::service_error);

        case UnknownError:
            break;
    }
    return {};
}


struct MapElem {
    const char* name;
    ErrorCodes::Error code;
};

// Note: this array must be kept in sorted order
static const constexpr MapElem string_to_error_code[] = {
    {"APIKeyAlreadyExists", ErrorCodes::APIKeyAlreadyExists},
    {"APIKeyNotFound", ErrorCodes::APIKeyNotFound},
    {"AWSError", ErrorCodes::AWSError},
    {"AccountNameInUse", ErrorCodes::AccountNameInUse},
    {"AddressSpaceExhausted", ErrorCodes::AddressSpaceExhausted},
    {"AppServerError", ErrorCodes::AppServerError},
    {"AppUnknownError", ErrorCodes::AppUnknownError},
    {"ArgumentsNotAllowed", ErrorCodes::ArgumentsNotAllowed},
    {"AuthError", ErrorCodes::AuthError},
    {"AuthProviderAlreadyExists", ErrorCodes::AuthProviderAlreadyExists},
    {"AuthProviderDuplicateName", ErrorCodes::AuthProviderDuplicateName},
    {"AuthProviderNotFound", ErrorCodes::AuthProviderNotFound},
    {"AutoClientResetFailed", ErrorCodes::AutoClientResetFailed},
    {"BadBsonParse", ErrorCodes::BadBsonParse},
    {"BadChangeset", ErrorCodes::BadChangeset},
    {"BadRequest", ErrorCodes::BadRequest},
    {"BadServerUrl", ErrorCodes::BadServerUrl},
    {"BadSyncPartitionValue", ErrorCodes::BadSyncPartitionValue},
    {"BadToken", ErrorCodes::BadToken},
    {"BadVersion", ErrorCodes::BadVersion},
    {"BrokenInvariant", ErrorCodes::BrokenInvariant},
    {"BrokenPromise", ErrorCodes::BrokenPromise},
    {"CallbackFailed", ErrorCodes::CallbackFailed},
    {"ClientRedirectError", ErrorCodes::ClientRedirectError},
    {"ClientTooManyRedirects", ErrorCodes::ClientTooManyRedirects},
    {"ClientUserAlreadyNamed", ErrorCodes::ClientUserAlreadyNamed},
    {"ClientUserNotFound", ErrorCodes::ClientUserNotFound},
    {"ClientUserNotLoggedIn", ErrorCodes::ClientUserNotLoggedIn},
    {"ClosedRealm", ErrorCodes::ClosedRealm},
    {"ConnectionClosed", ErrorCodes::ConnectionClosed},
    {"CrossTableLinkTarget", ErrorCodes::CrossTableLinkTarget},
    {"CustomError", ErrorCodes::CustomError},
    {"DecryptionFailed", ErrorCodes::DecryptionFailed},
    {"DeleteOnOpenRealm", ErrorCodes::DeleteOnOpenRealm},
    {"DomainNotAllowed", ErrorCodes::DomainNotAllowed},
    {"ExecutionTimeLimitExceeded", ErrorCodes::ExecutionTimeLimitExceeded},
    {"FileAlreadyExists", ErrorCodes::FileAlreadyExists},
    {"FileFormatUpgradeRequired", ErrorCodes::FileFormatUpgradeRequired},
    {"FileNotFound", ErrorCodes::FileNotFound},
    {"FileOperationFailed", ErrorCodes::FileOperationFailed},
    {"FunctionAlreadyExists", ErrorCodes::FunctionAlreadyExists},
    {"FunctionDuplicateName", ErrorCodes::FunctionDuplicateName},
    {"FunctionExecutionError", ErrorCodes::FunctionExecutionError},
    {"FunctionInvalid", ErrorCodes::FunctionInvalid},
    {"FunctionNotFound", ErrorCodes::FunctionNotFound},
    {"FunctionSyntaxError", ErrorCodes::FunctionSyntaxError},
    {"GCMError", ErrorCodes::GCMError},
    {"HTTPError", ErrorCodes::HTTPError},
    {"IllegalOperation", ErrorCodes::IllegalOperation},
    {"IncomingWebhookAlreadyExists", ErrorCodes::IncomingWebhookAlreadyExists},
    {"IncomingWebhookAuthFailed", ErrorCodes::IncomingWebhookAuthFailed},
    {"IncomingWebhookDuplicateName", ErrorCodes::IncomingWebhookDuplicateName},
    {"IncomingWebhookNotFound", ErrorCodes::IncomingWebhookNotFound},
    {"IncompatibleHistories", ErrorCodes::IncompatibleHistories},
    {"IncompatibleLockFile", ErrorCodes::IncompatibleLockFile},
    {"IncompatibleSession", ErrorCodes::IncompatibleSession},
    {"InternalServerError", ErrorCodes::InternalServerError},
    {"InvalidArgument", ErrorCodes::InvalidArgument},
    {"InvalidCombination", ErrorCodes::IllegalCombination},
    {"InvalidDatabase", ErrorCodes::InvalidDatabase},
    {"InvalidDictionaryKey", ErrorCodes::InvalidDictionaryKey},
    {"InvalidDictionaryValue", ErrorCodes::InvalidDictionaryValue},
    {"InvalidEncryptionKey", ErrorCodes::InvalidEncryptionKey},
    {"InvalidName", ErrorCodes::InvalidName},
    {"InvalidParameter", ErrorCodes::InvalidParameter},
    {"InvalidPassword", ErrorCodes::InvalidPassword},
    {"InvalidProperty", ErrorCodes::InvalidProperty},
    {"InvalidQuery", ErrorCodes::InvalidQuery},
    {"InvalidQueryArg", ErrorCodes::InvalidQueryArg},
    {"InvalidSchemaChange", ErrorCodes::InvalidSchemaChange},
    {"InvalidSchemaVersion", ErrorCodes::InvalidSchemaVersion},
    {"InvalidServerResponse", ErrorCodes::InvalidServerResponse},
    {"InvalidSession", ErrorCodes::InvalidSession},
    {"InvalidSortDescriptor", ErrorCodes::InvalidSortDescriptor},
    {"InvalidSubscriptionQuery", ErrorCodes::InvalidSubscriptionQuery},
    {"InvalidTableRef", ErrorCodes::InvalidTableRef},
    {"InvalidatedObject", ErrorCodes::InvalidatedObject},
    {"KeyAlreadyUsed", ErrorCodes::KeyAlreadyUsed},
    {"KeyNotFound", ErrorCodes::KeyNotFound},
    {"LimitExceeded", ErrorCodes::LimitExceeded},
    {"LogicError", ErrorCodes::LogicError},
    {"MaintenanceInProgress", ErrorCodes::MaintenanceInProgress},
    {"MalformedJson", ErrorCodes::MalformedJson},
    {"MaximumFileSizeExceeded", ErrorCodes::MaximumFileSizeExceeded},
    {"MigrationFailed", ErrorCodes::MigrationFailed},
    {"MismatchedConfig", ErrorCodes::MismatchedConfig},
    {"MissingAuthReq", ErrorCodes::MissingAuthReq},
    {"MissingJsonKey", ErrorCodes::MissingJsonKey},
    {"MissingParameter", ErrorCodes::MissingParameter},
    {"MissingPrimaryKey", ErrorCodes::MissingPrimaryKey},
    {"MissingPropertyValue", ErrorCodes::MissingPropertyValue},
    {"ModifyPrimaryKey", ErrorCodes::ModifyPrimaryKey},
    {"MongoDBError", ErrorCodes::MongoDBError},
    {"MultipleSyncAgents", ErrorCodes::MultipleSyncAgents},
    {"NoMatchingRuleFound", ErrorCodes::NoMatchingRuleFound},
    {"NoSubscriptionForWrite", ErrorCodes::NoSubscriptionForWrite},
    {"NoSuchTable", ErrorCodes::NoSuchTable},
    {"NotCallable", ErrorCodes::NotCallable},
    {"NotCloneable", ErrorCodes::NotCloneable},
    {"NotSupported", ErrorCodes::NotSupported},
    {"OK", ErrorCodes::OK},
    {"ObjectAlreadyExists", ErrorCodes::ObjectAlreadyExists},
    {"ObjectTypeMismatch", ErrorCodes::ObjectTypeMismatch},
    {"OperationAborted", ErrorCodes::OperationAborted},
    {"OutOfBounds", ErrorCodes::OutOfBounds},
    {"OutOfDiskSpace", ErrorCodes::OutOfDiskSpace},
    {"OutOfMemory", ErrorCodes::OutOfMemory},
    {"PermissionDenied", ErrorCodes::PermissionDenied},
    {"PropertyNotNullable", ErrorCodes::PropertyNotNullable},
    {"RangeError", ErrorCodes::RangeError},
    {"ReadOnlyDB", ErrorCodes::ReadOnlyDB},
    {"ReadOnlyProperty", ErrorCodes::ReadOnlyProperty},
    {"ReadSizeLimitExceeded", ErrorCodes::ReadSizeLimitExceeded},
    {"RestrictedHost", ErrorCodes::RestrictedHost},
    {"RuleAlreadyExists", ErrorCodes::RuleAlreadyExists},
    {"RuleDuplicateName", ErrorCodes::RuleDuplicateName},
    {"RuleNotFound", ErrorCodes::RuleNotFound},
    {"RuntimeError", ErrorCodes::RuntimeError},
    {"SchemaMismatch", ErrorCodes::SchemaMismatch},
    {"SchemaValidationFailed", ErrorCodes::SchemaValidationFailed},
    {"SchemaValidationFailedWrite", ErrorCodes::SchemaValidationFailedWrite},
    {"SchemaVersionMismatch", ErrorCodes::SchemaVersionMismatch},
    {"SerializationError", ErrorCodes::SerializationError},
    {"ServiceAlreadyExists", ErrorCodes::ServiceAlreadyExists},
    {"ServiceCommandNotFound", ErrorCodes::ServiceCommandNotFound},
    {"ServiceNotFound", ErrorCodes::ServiceNotFound},
    {"ServiceTypeNotFound", ErrorCodes::ServiceTypeNotFound},
    {"StaleAccessor", ErrorCodes::StaleAccessor},
    {"SubscriptionFailed", ErrorCodes::SubscriptionFailed},
    {"SyncClientResetRequired", ErrorCodes::SyncClientResetRequired},
    {"SyncCompensatingWrite", ErrorCodes::SyncCompensatingWrite},
    {"SyncConnectFailed", ErrorCodes::SyncConnectFailed},
    {"SyncConnectTimeout", ErrorCodes::SyncConnectTimeout},
    {"SyncInvalidSchemaChange", ErrorCodes::SyncInvalidSchemaChange},
    {"SyncLocalClockBeforeEpoch", ErrorCodes::SyncLocalClockBeforeEpoch},
    {"SyncPermissionDenied", ErrorCodes::SyncPermissionDenied},
    {"SyncProtocolInvariantFailed", ErrorCodes::SyncProtocolInvariantFailed},
    {"SyncProtocolNegotiationFailed", ErrorCodes::SyncProtocolNegotiationFailed},
    {"SyncSchemaMigrationError", ErrorCodes::SyncSchemaMigrationError},
    {"SyncServerPermissionsChanged", ErrorCodes::SyncServerPermissionsChanged},
    {"SyncUserMismatch", ErrorCodes::SyncUserMismatch},
    {"SyncWriteNotAllowed", ErrorCodes::SyncWriteNotAllowed},
    {"SyntaxError", ErrorCodes::SyntaxError},
    {"SystemError", ErrorCodes::SystemError},
    {"TableNameInUse", ErrorCodes::TableNameInUse},
    {"TlsHandshakeFailed", ErrorCodes::TlsHandshakeFailed},
    {"TopLevelObject", ErrorCodes::TopLevelObject},
    {"TwilioError", ErrorCodes::TwilioError},
    {"TypeMismatch", ErrorCodes::TypeMismatch},
    {"UnexpectedPrimaryKey", ErrorCodes::UnexpectedPrimaryKey},
    // UnknownError intentionally left out of list
    {"UnsupportedFileFormatVersion", ErrorCodes::UnsupportedFileFormatVersion},
    {"UserAlreadyConfirmed", ErrorCodes::UserAlreadyConfirmed},
    {"UserAppDomainMismatch", ErrorCodes::UserAppDomainMismatch},
    {"UserDisabled", ErrorCodes::UserDisabled},
    {"UserNotFound", ErrorCodes::UserNotFound},
    {"UserpassTokenInvalid", ErrorCodes::UserpassTokenInvalid},
    {"ValueAlreadyExists", ErrorCodes::ValueAlreadyExists},
    {"ValueDuplicateName", ErrorCodes::ValueDuplicateName},
    {"ValueNotFound", ErrorCodes::ValueNotFound},
    {"WrongSyncType", ErrorCodes::WrongSyncType},
    {"WrongThread", ErrorCodes::WrongThread},
    {"WrongTransactionState", ErrorCodes::WrongTransactionState},
};

namespace {

class ErrorCodesMap {
public:
    ErrorCodesMap()
    {
#if REALM_DEBUG
        bool is_valid =
            std::is_sorted(std::begin(string_to_error_code), std::end(string_to_error_code), [](auto& a, auto& b) {
                if (strcmp(a.name, b.name) < 0) {
                    std::cout << a.name << " comes before " << b.name << std::endl;
                    return true;
                }
                return false;
            });

        REALM_ASSERT_DEBUG(is_valid);
#endif
    }

    ErrorCodes::Error operator[](std::string_view name)
    {
        auto it = std::lower_bound(std::begin(string_to_error_code), std::end(string_to_error_code), name,
                                   [](auto& ec_pair, auto name) {
                                       return strncmp(ec_pair.name, name.data(), name.size()) < 0;
                                   });
        if (it != std::end(string_to_error_code) && it->name == name) {
            return it->code;
        }
        return ErrorCodes::UnknownError;
    }

    std::string_view operator[](ErrorCodes::Error code)
    {
        for (auto [name, c] : string_to_error_code) {
            if (code == c) {
                return name;
            }
        }
        return "unknown";
    }
} error_codes_map;

} // namespace

std::string_view ErrorCodes::error_string(Error code)
{
    return error_codes_map[code];
}

ErrorCodes::Error ErrorCodes::from_string(std::string_view name)
{
    return error_codes_map[name];
}

std::vector<ErrorCodes::Error> ErrorCodes::get_all_codes()
{
    std::vector<ErrorCodes::Error> ret;
    for (auto it : string_to_error_code) {
        ret.push_back(it.code);
    }
    return ret;
}

std::vector<std::string_view> ErrorCodes::get_all_names()
{
    std::vector<std::string_view> ret;
    for (auto it : string_to_error_code) {
        ret.emplace_back(it.name);
    }
    return ret;
}

std::vector<std::pair<std::string_view, ErrorCodes::Error>> ErrorCodes::get_error_list()
{
    std::vector<std::pair<std::string_view, ErrorCodes::Error>> ret;
    for (auto it : string_to_error_code) {
        ret.emplace_back(std::make_pair(it.name, it.code));
    }
    return ret;
}

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code)
{
    return stream << ErrorCodes::error_string(code);
}


} // namespace realm
