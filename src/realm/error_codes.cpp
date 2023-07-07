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
        case BadChangeset:
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
        case SubscriptionFailed:
        case UnsupportedFileFormatVersion:
        case OperationAborted:
            return ErrorCategory().set(ErrorCategory::runtime_error);

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
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::logic_error);

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
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::invalid_argument)
                .set(ErrorCategory::logic_error);

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

        case ClientAppDeallocated:
        case ClientRedirectError:
        case ClientTooManyRedirects:
        case ClientUserNotFound:
        case ClientUserNotLoggedIn:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::client_error);

        case BadBsonParse:
        case BadToken:
        case MalformedJson:
        case MissingJsonKey:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::json_error);

        case InvalidJSON:
        case InvalidJSONFieldType:
        case APIKeyAlreadyExists:
        case APIKeyNotFound:
        case AWSError:
        case ArgumentsNotAllowed:
        case AuthProviderAlreadyExists:
        case AuthProviderDuplicateName:
        case AuthProviderNotFound:
        case DomainNotAllowed:
        case EndpointAlreadyExists:
        case EndpointDuplicateKey:
        case EndpointNotFound:
        case EventSubscriptionAlreadyExists:
        case EventSubscriptionDuplicateName:
        case EventSubscriptionNotFound:
        case EventSubscriptionTypeChange:
        case ErrCodeEventSubscriptionMaxCount:
        case ExecutionMemoryLimitExceeded:
        case ExecutionTimeLimitExceeded:
        case FileHashDoesNotMatch:
        case FileSizeGivenIncorrect:
        case FunctionAlreadyExists:
        case FunctionDuplicateName:
        case FunctionExecutionError:
        case FunctionInvalid:
        case FunctionNotFound:
        case FunctionSyntaxError:
        case FunctionMaxStackDepthError:
        case GCMError:
        case HostingInvalidDomain:
        case HostingChangesInProgress:
        case HostingFileLimitExceeded:
        case HostingNumFilesLimitExceeded:
        case HostingTotalSizeLimitExceeded:
        case HostingDirectoryDepthLimitExceeded:
        case HostingNotEnabled:
        case ImportAppError:
        case IncomingWebhookAlreadyExists:
        case IncomingWebhookAuthFailed:
        case IncomingWebhookDuplicateName:
        case IncomingWebhookNotFound:
        case NoHTTPWebhooksToConvert:
        case InternalServerError:
        case InvalidParameter:
        case InvalidPassword:
        case AuthError:
        case InvalidSession:
        case RestrictedUIIP:
        case InvalidFile:
        case InvalidMove:
        case InvalidDirectory:
        case DirectoryAlreadyExists:
        case AssetMetadataNotFound:
        case InvalidAuthReq:
        case MissingAuthReq:
        case InvalidRedirectURI:
        case LinkingNotSupported:
        case MissingParameter:
        case MongoDBError:
        case NoFilesToPush:
        case DeletedFileNotFound:
        case NoMatchingRoleFound:
        case NoMatchingRuleFound:
        case FunctionNotCallable:
        case PromiseConversionError:
        case PromiseValueError:
        case ReadSizeLimitExceeded:
        case PromiseRejected:
        case RestrictedHost:
        case DefaultRuleAlreadyExists:
        case RuleAlreadyExists:
        case DefaultRuleIDMismatch:
        case RuleDuplicateName:
        case DefaultRuleNotFound:
        case RuleNotFound:
        case RulesNotFound:
        case DatabaseRulesNotFound:
        case ServiceAlreadyExists:
        case ServiceCommandNotFound:
        case ServiceNotFound:
        case ServiceTypeNotFound:
        case ServiceAssociatedWithTrigger:
        case ServiceAssociatedWithLogForwarder:
        case TwilioError:
        case UserAlreadyConfirmed:
        case UserAppDomainMismatch:
        case UserDisabled:
        case UserNotFound:
        case ValueAlreadyExists:
        case ValueDuplicateName:
        case ValueNotFound:
        case SecretAlreadyExists:
        case SecretDuplicateName:
        case SecretNotFound:
        case EnvironmentTagInvalid:
        case EnvironmentValueAlreadyExists:
        case EnvironmentValueDuplicateName:
        case EnvironmentValueNotFound:
        case DraftAlreadyExists:
        case DraftNotFound:
        case DeploymentNotFound:
        case DeploymentStateNotFound:
        case DeploymentNotRedeployable:
        case CounterNotFound:
        case InstallationNotFound:
        case AccountNameInUse:
        case BadRequest:
        case AppDependenciesNotFound:
        case AppDependenciesDraftNotFound:
        case UserpassTokenInvalid:
        case BehindCurrentVersion:
        case SchemaValidationFailedRead:
        case SchemaValidationFailedWrite:
        case UncaughtPromiseRejection:
        case DependencyHashNotFound:
        case OperationCanceled:
        case CustomResolverAlreadyExists:
        case DestructiveChangeNotAllowed:
        case DependencyInstallationNotFound:
        case DependencyNotFound:
        case CollectionAlreadyExists:
        case MaintenanceInProgress:
        case MaintenanceInProgressAdmin:
        case MatchingCurrentDeploymentModelAndProviderRegion:
        case SchemaAlreadyExists:
        case SchemaMetadataExists:
        case SchemaNotFound:
        case InvalidJSONSchema:
        case AllowedIPAlreadyExists:
        case AllowedIPDuplicateAddress:
        case AllowedIPNotFound:
        case ProviderPrivateEndpointAlreadyExists:
        case ProviderPrivateEndpointDuplicateEndpointID:
        case ProviderPrivateEndpointNotFound:
        case RelationshipMissingSourceProperty:
        case RelationshipMissingSourceType:
        case RelationshipHasProperties:
        case RelationshipInvalidAdditionalProperties:
        case RelationshipInvalidItems:
        case RelationshipUnrelatableSourceType:
        case RelationshipMissingRefSchema:
        case RelationshipTypeMismatch:
        case NoMongoDBRulesFound:
        case NoMongoDBServiceFound:
        case NoSchemasFound:
        case InvalidSchema:
        case UnsupportedTableType:
        case InvalidType:
        case UnsupportedType:
        case MissingSchemaType:
        case InvalidItems:
        case InvalidDefault:
        case MissingRelationshipRuleDefinition:
        case MissingRelationshipSchemaDefinition:
        case InvalidSyncSchema:
        case SchemaRootMissingUnderscoreID:
        case SchemaRootOptionalUnderscoreID:
        case SyncAlreadyEnabled:
        case SyncFailedToPatchSchemas:
        case SyncInvalidPartitionKey:
        case SyncLifecycleError:
        case UnsupportedRealmClientLanguage:
        case SyncDeploymentSuccessWithWarning:
        case SyncFailedToCreateIndexOnPartitionKey:
        case SyncProtocolVersionIncrease:
        case DestructiveSyncProtocolVersionIncrease:
        case SyncIncompatibleRole:
        case IncompatibleMongoDBVersion:
        case NoRunAsSystem:
        case EmptyDataAPIConfig:
        case DatasourceAlreadyExists:
        case NotCallable:
        case InvalidServerResponse:
        case AppUnknownError:
            return ErrorCategory()
                .set(ErrorCategory::runtime_error)
                .set(ErrorCategory::app_error)
                .set(ErrorCategory::service_error);

        case WebSocketResolveFailedError:
        case WebSocketConnectionClosedClientError:
        case WebSocketConnectionClosedServerError:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::websocket_error);

        case UnknownError:
            return ErrorCategory().set(ErrorCategory::runtime_error);
    }
    return {};
}


struct MapElem {
    const char* name;
    ErrorCodes::Error code;
};

// Note: this array must be kept in sorted order
static const MapElem string_to_error_code[] = {
    {"APIKeyAlreadyExists", ErrorCodes::APIKeyAlreadyExists},
    {"APIKeyNotFound", ErrorCodes::APIKeyNotFound},
    {"AWSError", ErrorCodes::AWSError},
    {"AccountNameInUse", ErrorCodes::AccountNameInUse},
    {"AddressSpaceExhausted", ErrorCodes::AddressSpaceExhausted},
    {"AllowedIPAlreadyExists", ErrorCodes::AllowedIPAlreadyExists},
    {"AllowedIPDuplicateAddress", ErrorCodes::AllowedIPDuplicateAddress},
    {"AllowedIPNotFound", ErrorCodes::AllowedIPNotFound},
    {"AppDependenciesDraftNotFound", ErrorCodes::AppDependenciesDraftNotFound},
    {"AppDependenciesNotFound", ErrorCodes::AppDependenciesNotFound},
    {"AppUnknownError", ErrorCodes::AppUnknownError},
    {"ArgumentsNotAllowed", ErrorCodes::ArgumentsNotAllowed},
    {"AssetMetadataNotFound", ErrorCodes::AssetMetadataNotFound},
    {"AuthError", ErrorCodes::AuthError},
    {"AuthProviderAlreadyExists", ErrorCodes::AuthProviderAlreadyExists},
    {"AuthProviderDuplicateName", ErrorCodes::AuthProviderDuplicateName},
    {"AuthProviderNotFound", ErrorCodes::AuthProviderNotFound},
    {"BadBsonParse", ErrorCodes::BadBsonParse},
    {"BadChangeset", ErrorCodes::BadChangeset},
    {"BadRequest", ErrorCodes::BadRequest},
    {"BadServerUrl", ErrorCodes::BadServerUrl},
    {"BadToken", ErrorCodes::BadToken},
    {"BadVersion", ErrorCodes::BadVersion},
    {"BehindCurrentVersion", ErrorCodes::BehindCurrentVersion},
    {"BrokenInvariant", ErrorCodes::BrokenInvariant},
    {"BrokenPromise", ErrorCodes::BrokenPromise},
    {"CallbackFailed", ErrorCodes::CallbackFailed},
    {"ClientAppDeallocated", ErrorCodes::ClientAppDeallocated},
    {"ClientRedirectError", ErrorCodes::ClientRedirectError},
    {"ClientTooManyRedirects", ErrorCodes::ClientTooManyRedirects},
    {"ClientUserNotFound", ErrorCodes::ClientUserNotFound},
    {"ClientUserNotLoggedIn", ErrorCodes::ClientUserNotLoggedIn},
    {"ClosedRealm", ErrorCodes::ClosedRealm},
    {"CollectionAlreadyExists", ErrorCodes::CollectionAlreadyExists},
    {"CounterNotFound", ErrorCodes::CounterNotFound},
    {"CrossTableLinkTarget", ErrorCodes::CrossTableLinkTarget},
    {"CustomError", ErrorCodes::CustomError},
    {"CustomResolverAlreadyExists", ErrorCodes::CustomResolverAlreadyExists},
    {"DatabaseRulesNotFound", ErrorCodes::DatabaseRulesNotFound},
    {"DatasourceAlreadyExists", ErrorCodes::DatasourceAlreadyExists},
    {"DecryptionFailed", ErrorCodes::DecryptionFailed},
    {"DefaultRuleAlreadyExists", ErrorCodes::DefaultRuleAlreadyExists},
    {"DefaultRuleIDMismatch", ErrorCodes::DefaultRuleIDMismatch},
    {"DefaultRuleNotFound", ErrorCodes::DefaultRuleNotFound},
    {"DeleteOnOpenRealm", ErrorCodes::DeleteOnOpenRealm},
    {"DeletedFileNotFound", ErrorCodes::DeletedFileNotFound},
    {"DependencyHashNotFound", ErrorCodes::DependencyHashNotFound},
    {"DependencyInstallationNotFound", ErrorCodes::DependencyInstallationNotFound},
    {"DependencyNotFound", ErrorCodes::DependencyNotFound},
    {"DeploymentNotFound", ErrorCodes::DeploymentNotFound},
    {"DeploymentNotRedeployable", ErrorCodes::DeploymentNotRedeployable},
    {"DeploymentStateNotFound", ErrorCodes::DeploymentStateNotFound},
    {"DestructiveChangeNotAllowed", ErrorCodes::DestructiveChangeNotAllowed},
    {"DestructiveSyncProtocolVersionIncrease", ErrorCodes::DestructiveSyncProtocolVersionIncrease},
    {"DirectoryAlreadyExists", ErrorCodes::DirectoryAlreadyExists},
    {"DomainNotAllowed", ErrorCodes::DomainNotAllowed},
    {"DraftAlreadyExists", ErrorCodes::DraftAlreadyExists},
    {"DraftNotFound", ErrorCodes::DraftNotFound},
    {"EmptyDataAPIConfig", ErrorCodes::EmptyDataAPIConfig},
    {"EndpointAlreadyExists", ErrorCodes::EndpointAlreadyExists},
    {"EndpointDuplicateKey", ErrorCodes::EndpointDuplicateKey},
    {"EndpointNotFound", ErrorCodes::EndpointNotFound},
    {"EnvironmentTagInvalid", ErrorCodes::EnvironmentTagInvalid},
    {"EnvironmentValueAlreadyExists", ErrorCodes::EnvironmentValueAlreadyExists},
    {"EnvironmentValueDuplicateName", ErrorCodes::EnvironmentValueDuplicateName},
    {"EnvironmentValueNotFound", ErrorCodes::EnvironmentValueNotFound},
    {"ErrCodeEventSubscriptionMaxCount", ErrorCodes::ErrCodeEventSubscriptionMaxCount},
    {"EventSubscriptionAlreadyExists", ErrorCodes::EventSubscriptionAlreadyExists},
    {"EventSubscriptionDuplicateName", ErrorCodes::EventSubscriptionDuplicateName},
    {"EventSubscriptionNotFound", ErrorCodes::EventSubscriptionNotFound},
    {"EventSubscriptionTypeChange", ErrorCodes::EventSubscriptionTypeChange},
    {"ExecutionMemoryLimitExceeded", ErrorCodes::ExecutionMemoryLimitExceeded},
    {"ExecutionTimeLimitExceeded", ErrorCodes::ExecutionTimeLimitExceeded},
    {"FileAlreadyExists", ErrorCodes::FileAlreadyExists},
    {"FileFormatUpgradeRequired", ErrorCodes::FileFormatUpgradeRequired},
    {"FileHashDoesNotMatch", ErrorCodes::FileHashDoesNotMatch},
    {"FileNotFound", ErrorCodes::FileNotFound},
    {"FileOperationFailed", ErrorCodes::FileOperationFailed},
    {"FileSizeGivenIncorrect", ErrorCodes::FileSizeGivenIncorrect},
    {"FunctionAlreadyExists", ErrorCodes::FunctionAlreadyExists},
    {"FunctionDuplicateName", ErrorCodes::FunctionDuplicateName},
    {"FunctionExecutionError", ErrorCodes::FunctionExecutionError},
    {"FunctionInvalid", ErrorCodes::FunctionInvalid},
    {"FunctionMaxStackDepthError", ErrorCodes::FunctionMaxStackDepthError},
    {"FunctionNotCallable", ErrorCodes::FunctionNotCallable},
    {"FunctionNotFound", ErrorCodes::FunctionNotFound},
    {"FunctionSyntaxError", ErrorCodes::FunctionSyntaxError},
    {"GCMError", ErrorCodes::GCMError},
    {"HTTPError", ErrorCodes::HTTPError},
    {"HostingChangesInProgress", ErrorCodes::HostingChangesInProgress},
    {"HostingDirectoryDepthLimitExceeded", ErrorCodes::HostingDirectoryDepthLimitExceeded},
    {"HostingFileLimitExceeded", ErrorCodes::HostingFileLimitExceeded},
    {"HostingInvalidDomain", ErrorCodes::HostingInvalidDomain},
    {"HostingNotEnabled", ErrorCodes::HostingNotEnabled},
    {"HostingNumFilesLimitExceeded", ErrorCodes::HostingNumFilesLimitExceeded},
    {"HostingTotalSizeLimitExceeded", ErrorCodes::HostingTotalSizeLimitExceeded},
    {"IllegalOperation", ErrorCodes::IllegalOperation},
    {"ImportAppError", ErrorCodes::ImportAppError},
    {"IncomingWebhookAlreadyExists", ErrorCodes::IncomingWebhookAlreadyExists},
    {"IncomingWebhookAuthFailed", ErrorCodes::IncomingWebhookAuthFailed},
    {"IncomingWebhookDuplicateName", ErrorCodes::IncomingWebhookDuplicateName},
    {"IncomingWebhookNotFound", ErrorCodes::IncomingWebhookNotFound},
    {"IncompatibleHistories", ErrorCodes::IncompatibleHistories},
    {"IncompatibleLockFile", ErrorCodes::IncompatibleLockFile},
    {"IncompatibleMongoDBVersion", ErrorCodes::IncompatibleMongoDBVersion},
    {"IncompatibleSession", ErrorCodes::IncompatibleSession},
    {"InstallationNotFound", ErrorCodes::InstallationNotFound},
    {"InternalServerError", ErrorCodes::InternalServerError},
    {"InvalidArgument", ErrorCodes::InvalidArgument},
    {"InvalidAuthReq", ErrorCodes::InvalidAuthReq},
    {"InvalidCombination", ErrorCodes::IllegalCombination},
    {"InvalidDatabase", ErrorCodes::InvalidDatabase},
    {"InvalidDefault", ErrorCodes::InvalidDefault},
    {"InvalidDictionaryKey", ErrorCodes::InvalidDictionaryKey},
    {"InvalidDictionaryValue", ErrorCodes::InvalidDictionaryValue},
    {"InvalidDirectory", ErrorCodes::InvalidDirectory},
    {"InvalidEncryptionKey", ErrorCodes::InvalidEncryptionKey},
    {"InvalidFile", ErrorCodes::InvalidFile},
    {"InvalidItems", ErrorCodes::InvalidItems},
    {"InvalidJSON", ErrorCodes::InvalidJSON},
    {"InvalidJSONFieldType", ErrorCodes::InvalidJSONFieldType},
    {"InvalidJSONSchema", ErrorCodes::InvalidJSONSchema},
    {"InvalidMove", ErrorCodes::InvalidMove},
    {"InvalidName", ErrorCodes::InvalidName},
    {"InvalidParameter", ErrorCodes::InvalidParameter},
    {"InvalidPassword", ErrorCodes::InvalidPassword},
    {"InvalidProperty", ErrorCodes::InvalidProperty},
    {"InvalidQuery", ErrorCodes::InvalidQuery},
    {"InvalidQueryArg", ErrorCodes::InvalidQueryArg},
    {"InvalidRedirectURI", ErrorCodes::InvalidRedirectURI},
    {"InvalidSchema", ErrorCodes::InvalidSchema},
    {"InvalidSchemaChange", ErrorCodes::InvalidSchemaChange},
    {"InvalidSchemaVersion", ErrorCodes::InvalidSchemaVersion},
    {"InvalidServerResponse", ErrorCodes::InvalidServerResponse},
    {"InvalidSession", ErrorCodes::InvalidSession},
    {"InvalidSortDescriptor", ErrorCodes::InvalidSortDescriptor},
    {"InvalidSyncSchema", ErrorCodes::InvalidSyncSchema},
    {"InvalidTableRef", ErrorCodes::InvalidTableRef},
    {"InvalidType", ErrorCodes::InvalidType},
    {"InvalidatedObject", ErrorCodes::InvalidatedObject},
    {"KeyAlreadyUsed", ErrorCodes::KeyAlreadyUsed},
    {"KeyNotFound", ErrorCodes::KeyNotFound},
    {"LimitExceeded", ErrorCodes::LimitExceeded},
    {"LinkingNotSupported", ErrorCodes::LinkingNotSupported},
    {"LogicError", ErrorCodes::LogicError},
    {"MaintenanceInProgress", ErrorCodes::MaintenanceInProgress},
    {"MaintenanceInProgressAdmin", ErrorCodes::MaintenanceInProgressAdmin},
    {"MalformedJson", ErrorCodes::MalformedJson},
    {"MatchingCurrentDeploymentModelAndProviderRegion", ErrorCodes::MatchingCurrentDeploymentModelAndProviderRegion},
    {"MaximumFileSizeExceeded", ErrorCodes::MaximumFileSizeExceeded},
    {"MigrationFailed", ErrorCodes::MigrationFailed},
    {"MismatchedConfig", ErrorCodes::MismatchedConfig},
    {"MissingAuthReq", ErrorCodes::MissingAuthReq},
    {"MissingJsonKey", ErrorCodes::MissingJsonKey},
    {"MissingParameter", ErrorCodes::MissingParameter},
    {"MissingPrimaryKey", ErrorCodes::MissingPrimaryKey},
    {"MissingPropertyValue", ErrorCodes::MissingPropertyValue},
    {"MissingRelationshipRuleDefinition", ErrorCodes::MissingRelationshipRuleDefinition},
    {"MissingRelationshipSchemaDefinition", ErrorCodes::MissingRelationshipSchemaDefinition},
    {"MissingSchemaType", ErrorCodes::MissingSchemaType},
    {"ModifyPrimaryKey", ErrorCodes::ModifyPrimaryKey},
    {"MongoDBError", ErrorCodes::MongoDBError},
    {"MultipleSyncAgents", ErrorCodes::MultipleSyncAgents},
    {"NoFilesToPush", ErrorCodes::NoFilesToPush},
    {"NoHTTPWebhooksToConvert", ErrorCodes::NoHTTPWebhooksToConvert},
    {"NoMatchingRoleFound", ErrorCodes::NoMatchingRoleFound},
    {"NoMatchingRuleFound", ErrorCodes::NoMatchingRuleFound},
    {"NoMongoDBRulesFound", ErrorCodes::NoMongoDBRulesFound},
    {"NoMongoDBServiceFound", ErrorCodes::NoMongoDBServiceFound},
    {"NoRunAsSystem", ErrorCodes::NoRunAsSystem},
    {"NoSchemasFound", ErrorCodes::NoSchemasFound},
    {"NoSubscriptionForWrite", ErrorCodes::NoSubscriptionForWrite},
    {"NoSuchTable", ErrorCodes::NoSuchTable},
    {"NotCallable", ErrorCodes::NotCallable},
    {"NotCloneable", ErrorCodes::NotCloneable},
    {"NotSupported", ErrorCodes::NotSupported},
    {"OK", ErrorCodes::OK},
    {"ObjectAlreadyExists", ErrorCodes::ObjectAlreadyExists},
    {"ObjectTypeMismatch", ErrorCodes::ObjectTypeMismatch},
    {"OperationAborted", ErrorCodes::OperationAborted},
    {"OperationCanceled", ErrorCodes::OperationCanceled},
    {"OutOfBounds", ErrorCodes::OutOfBounds},
    {"OutOfDiskSpace", ErrorCodes::OutOfDiskSpace},
    {"OutOfMemory", ErrorCodes::OutOfMemory},
    {"PermissionDenied", ErrorCodes::PermissionDenied},
    {"PromiseConversionError", ErrorCodes::PromiseConversionError},
    {"PromiseRejected", ErrorCodes::PromiseRejected},
    {"PromiseValueError", ErrorCodes::PromiseValueError},
    {"PropertyNotNullable", ErrorCodes::PropertyNotNullable},
    {"ProviderPrivateEndpointAlreadyExists", ErrorCodes::ProviderPrivateEndpointAlreadyExists},
    {"ProviderPrivateEndpointDuplicateEndpointID", ErrorCodes::ProviderPrivateEndpointDuplicateEndpointID},
    {"ProviderPrivateEndpointNotFound", ErrorCodes::ProviderPrivateEndpointNotFound},
    {"RangeError", ErrorCodes::RangeError},
    {"ReadOnlyDB", ErrorCodes::ReadOnlyDB},
    {"ReadOnlyProperty", ErrorCodes::ReadOnlyProperty},
    {"ReadSizeLimitExceeded", ErrorCodes::ReadSizeLimitExceeded},
    {"RelationshipHasProperties", ErrorCodes::RelationshipHasProperties},
    {"RelationshipInvalidAdditionalProperties", ErrorCodes::RelationshipInvalidAdditionalProperties},
    {"RelationshipInvalidItems", ErrorCodes::RelationshipInvalidItems},
    {"RelationshipMissingRefSchema", ErrorCodes::RelationshipMissingRefSchema},
    {"RelationshipMissingSourceProperty", ErrorCodes::RelationshipMissingSourceProperty},
    {"RelationshipMissingSourceType", ErrorCodes::RelationshipMissingSourceType},
    {"RelationshipTypeMismatch", ErrorCodes::RelationshipTypeMismatch},
    {"RelationshipUnrelatableSourceType", ErrorCodes::RelationshipUnrelatableSourceType},
    {"RestrictedHost", ErrorCodes::RestrictedHost},
    {"RestrictedUIIP", ErrorCodes::RestrictedUIIP},
    {"RuleAlreadyExists", ErrorCodes::RuleAlreadyExists},
    {"RuleDuplicateName", ErrorCodes::RuleDuplicateName},
    {"RuleNotFound", ErrorCodes::RuleNotFound},
    {"RulesNotFound", ErrorCodes::RulesNotFound},
    {"RuntimeError", ErrorCodes::RuntimeError},
    {"SchemaAlreadyExists", ErrorCodes::SchemaAlreadyExists},
    {"SchemaMetadataExists", ErrorCodes::SchemaMetadataExists},
    {"SchemaMismatch", ErrorCodes::SchemaMismatch},
    {"SchemaNotFound", ErrorCodes::SchemaNotFound},
    {"SchemaRootMissingUnderscoreID", ErrorCodes::SchemaRootMissingUnderscoreID},
    {"SchemaRootOptionalUnderscoreID", ErrorCodes::SchemaRootOptionalUnderscoreID},
    {"SchemaValidationFailed", ErrorCodes::SchemaValidationFailed},
    {"SchemaValidationFailedRead", ErrorCodes::SchemaValidationFailedRead},
    {"SchemaValidationFailedWrite", ErrorCodes::SchemaValidationFailedWrite},
    {"SchemaVersionMismatch", ErrorCodes::SchemaVersionMismatch},
    {"SecretAlreadyExists", ErrorCodes::SecretAlreadyExists},
    {"SecretDuplicateName", ErrorCodes::SecretDuplicateName},
    {"SecretNotFound", ErrorCodes::SecretNotFound},
    {"SerializationError", ErrorCodes::SerializationError},
    {"ServiceAlreadyExists", ErrorCodes::ServiceAlreadyExists},
    {"ServiceAssociatedWithLogForwarder", ErrorCodes::ServiceAssociatedWithLogForwarder},
    {"ServiceAssociatedWithTrigger", ErrorCodes::ServiceAssociatedWithTrigger},
    {"ServiceCommandNotFound", ErrorCodes::ServiceCommandNotFound},
    {"ServiceNotFound", ErrorCodes::ServiceNotFound},
    {"ServiceTypeNotFound", ErrorCodes::ServiceTypeNotFound},
    {"StaleAccessor", ErrorCodes::StaleAccessor},
    {"SubscriptionFailed", ErrorCodes::SubscriptionFailed},
    {"SyncAlreadyEnabled", ErrorCodes::SyncAlreadyEnabled},
    {"SyncDeploymentSuccessWithWarning", ErrorCodes::SyncDeploymentSuccessWithWarning},
    {"SyncFailedToCreateIndexOnPartitionKey", ErrorCodes::SyncFailedToCreateIndexOnPartitionKey},
    {"SyncFailedToPatchSchemas", ErrorCodes::SyncFailedToPatchSchemas},
    {"SyncIncompatibleRole", ErrorCodes::SyncIncompatibleRole},
    {"SyncInvalidPartitionKey", ErrorCodes::SyncInvalidPartitionKey},
    {"SyncLifecycleError", ErrorCodes::SyncLifecycleError},
    {"SyncProtocolVersionIncrease", ErrorCodes::SyncProtocolVersionIncrease},
    {"SyntaxError", ErrorCodes::SyntaxError},
    {"SystemError", ErrorCodes::SystemError},
    {"TableNameInUse", ErrorCodes::TableNameInUse},
    {"TopLevelObject", ErrorCodes::TopLevelObject},
    {"TwilioError", ErrorCodes::TwilioError},
    {"TypeMismatch", ErrorCodes::TypeMismatch},
    {"UncaughtPromiseRejection", ErrorCodes::UncaughtPromiseRejection},
    {"UnexpectedPrimaryKey", ErrorCodes::UnexpectedPrimaryKey},
    {"UnsupportedFileFormatVersion", ErrorCodes::UnsupportedFileFormatVersion},
    {"UnsupportedRealmClientLanguage", ErrorCodes::UnsupportedRealmClientLanguage},
    {"UnsupportedTableType", ErrorCodes::UnsupportedTableType},
    {"UnsupportedType", ErrorCodes::UnsupportedType},
    {"UserAlreadyConfirmed", ErrorCodes::UserAlreadyConfirmed},
    {"UserAppDomainMismatch", ErrorCodes::UserAppDomainMismatch},
    {"UserDisabled", ErrorCodes::UserDisabled},
    {"UserNotFound", ErrorCodes::UserNotFound},
    {"UserpassTokenInvalid", ErrorCodes::UserpassTokenInvalid},
    {"ValueAlreadyExists", ErrorCodes::ValueAlreadyExists},
    {"ValueDuplicateName", ErrorCodes::ValueDuplicateName},
    {"ValueNotFound", ErrorCodes::ValueNotFound},
    {"WebSocketConnectionClosedClientError", ErrorCodes::WebSocketConnectionClosedClientError},
    {"WebSocketConnectionClosedServerError", ErrorCodes::WebSocketConnectionClosedServerError},
    {"WebSocketResolveFailedError", ErrorCodes::WebSocketResolveFailedError},
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
