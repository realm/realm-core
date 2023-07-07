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

#pragma once

#include <cstdint>
#include <type_traits>
#include <string>
#include <vector>
#include <realm/error_codes.h>

namespace realm {

// ErrorExtraInfo subclasses:

struct ErrorCategory {
    enum Type {
        logic_error = RLM_ERR_CAT_LOGIC,
        runtime_error = RLM_ERR_CAT_RUNTIME,
        invalid_argument = RLM_ERR_CAT_INVALID_ARG,
        file_access = RLM_ERR_CAT_FILE_ACCESS,
        system_error = RLM_ERR_CAT_SYSTEM_ERROR,
        app_error = RLM_ERR_CAT_APP_ERROR,
        client_error = RLM_ERR_CAT_CLIENT_ERROR,
        json_error = RLM_ERR_CAT_JSON_ERROR,
        service_error = RLM_ERR_CAT_SERVICE_ERROR,
        http_error = RLM_ERR_CAT_HTTP_ERROR,
        custom_error = RLM_ERR_CAT_CUSTOM_ERROR,
        websocket_error = RLM_ERR_CAT_WEBSOCKET_ERROR,
    };
    constexpr ErrorCategory() = default;
    constexpr bool test(Type cat)
    {
        return (m_value & cat) != 0;
    }
    constexpr ErrorCategory& set(Type cat)
    {
        m_value |= cat;
        return *this;
    }
    constexpr void reset(Type cat)
    {
        m_value &= ~cat;
    }
    constexpr bool operator==(const ErrorCategory& other) const
    {
        return m_value == other.m_value;
    }
    constexpr bool operator!=(const ErrorCategory& other) const
    {
        return m_value != other.m_value;
    }
    constexpr int value() const
    {
        return m_value;
    }

private:
    unsigned m_value = 0;
};

class ErrorCodes {
public:
    // Explicitly 32-bits wide so that non-symbolic values,
    // like uassert codes, are valid.
    enum Error : std::int32_t {
        OK = RLM_ERR_NONE,
        RuntimeError = RLM_ERR_RUNTIME,
        RangeError = RLM_ERR_RANGE_ERROR,
        BrokenInvariant = RLM_ERR_BROKEN_INVARIANT,
        OutOfMemory = RLM_ERR_OUT_OF_MEMORY,
        OutOfDiskSpace = RLM_ERR_OUT_OF_DISK_SPACE,
        AddressSpaceExhausted = RLM_ERR_ADDRESS_SPACE_EXHAUSTED,
        MaximumFileSizeExceeded = RLM_ERR_MAXIMUM_FILE_SIZE_EXCEEDED,
        IncompatibleSession = RLM_ERR_INCOMPATIBLE_SESSION,
        IncompatibleLockFile = RLM_ERR_INCOMPATIBLE_LOCK_FILE,
        UnsupportedFileFormatVersion = RLM_ERR_UNSUPPORTED_FILE_FORMAT_VERSION,
        MultipleSyncAgents = RLM_ERR_MULTIPLE_SYNC_AGENTS,
        ObjectAlreadyExists = RLM_ERR_OBJECT_ALREADY_EXISTS,
        NotCloneable = RLM_ERR_NOT_CLONABLE,
        BadChangeset = RLM_ERR_BAD_CHANGESET,
        SubscriptionFailed = RLM_ERR_SUBSCRIPTION_FAILED,
        FileOperationFailed = RLM_ERR_FILE_OPERATION_FAILED,
        PermissionDenied = RLM_ERR_FILE_PERMISSION_DENIED,
        FileNotFound = RLM_ERR_FILE_NOT_FOUND,
        FileAlreadyExists = RLM_ERR_FILE_ALREADY_EXISTS,
        InvalidDatabase = RLM_ERR_INVALID_DATABASE,
        DecryptionFailed = RLM_ERR_DECRYPTION_FAILED,
        IncompatibleHistories = RLM_ERR_INCOMPATIBLE_HISTORIES,
        FileFormatUpgradeRequired = RLM_ERR_FILE_FORMAT_UPGRADE_REQUIRED,
        SchemaVersionMismatch = RLM_ERR_SCHEMA_VERSION_MISMATCH,
        NoSubscriptionForWrite = RLM_ERR_NO_SUBSCRIPTION_FOR_WRITE,
        BadVersion = RLM_ERR_BAD_VERSION,
        OperationAborted = RLM_ERR_OPERATION_ABORTED,

        SystemError = RLM_ERR_SYSTEM_ERROR,

        LogicError = RLM_ERR_LOGIC,
        NotSupported = RLM_ERR_NOT_SUPPORTED,
        BrokenPromise = RLM_ERR_BROKEN_PROMISE,
        CrossTableLinkTarget = RLM_ERR_CROSS_TABLE_LINK_TARGET,
        KeyAlreadyUsed = RLM_ERR_KEY_ALREADY_USED,
        WrongTransactionState = RLM_ERR_WRONG_TRANSACTION_STATE,
        WrongThread = RLM_ERR_WRONG_THREAD,
        IllegalOperation = RLM_ERR_ILLEGAL_OPERATION,
        SerializationError = RLM_ERR_SERIALIZATION_ERROR,
        StaleAccessor = RLM_ERR_STALE_ACCESSOR,
        InvalidatedObject = RLM_ERR_INVALIDATED_OBJECT,
        ReadOnlyDB = RLM_ERR_READ_ONLY_DB,
        DeleteOnOpenRealm = RLM_ERR_DELETE_OPENED_REALM,
        MismatchedConfig = RLM_ERR_MISMATCHED_CONFIG,
        ClosedRealm = RLM_ERR_CLOSED_REALM,
        InvalidTableRef = RLM_ERR_INVALID_TABLE_REF,
        SchemaValidationFailed = RLM_ERR_SCHEMA_VALIDATION_FAILED,
        SchemaMismatch = RLM_ERR_SCHEMA_MISMATCH,
        InvalidSchemaVersion = RLM_ERR_INVALID_SCHEMA_VERSION,
        InvalidSchemaChange = RLM_ERR_INVALID_SCHEMA_CHANGE,
        MigrationFailed = RLM_ERR_MIGRATION_FAILED,
        InvalidQuery = RLM_ERR_INVALID_QUERY,

        BadServerUrl = RLM_ERR_BAD_SERVER_URL,
        InvalidArgument = RLM_ERR_INVALID_ARGUMENT,
        TypeMismatch = RLM_ERR_PROPERTY_TYPE_MISMATCH,
        PropertyNotNullable = RLM_ERR_PROPERTY_NOT_NULLABLE,
        ReadOnlyProperty = RLM_ERR_READ_ONLY_PROPERTY,
        MissingPropertyValue = RLM_ERR_MISSING_PROPERTY_VALUE,
        MissingPrimaryKey = RLM_ERR_MISSING_PRIMARY_KEY,
        UnexpectedPrimaryKey = RLM_ERR_UNEXPECTED_PRIMARY_KEY,
        ModifyPrimaryKey = RLM_ERR_MODIFY_PRIMARY_KEY,
        SyntaxError = RLM_ERR_INVALID_QUERY_STRING,
        InvalidProperty = RLM_ERR_INVALID_PROPERTY,
        InvalidName = RLM_ERR_INVALID_NAME,
        InvalidDictionaryKey = RLM_ERR_INVALID_DICTIONARY_KEY,
        InvalidDictionaryValue = RLM_ERR_INVALID_DICTIONARY_VALUE,
        InvalidSortDescriptor = RLM_ERR_INVALID_SORT_DESCRIPTOR,
        InvalidEncryptionKey = RLM_ERR_INVALID_ENCRYPTION_KEY,
        InvalidQueryArg = RLM_ERR_INVALID_QUERY_ARG,
        KeyNotFound = RLM_ERR_NO_SUCH_OBJECT,
        OutOfBounds = RLM_ERR_INDEX_OUT_OF_BOUNDS,
        LimitExceeded = RLM_ERR_LIMIT_EXCEEDED,
        ObjectTypeMismatch = RLM_ERR_OBJECT_TYPE_MISMATCH,
        NoSuchTable = RLM_ERR_NO_SUCH_TABLE,
        TableNameInUse = RLM_ERR_TABLE_NAME_IN_USE,
        IllegalCombination = RLM_ERR_ILLEGAL_COMBINATION,
        TopLevelObject = RLM_ERR_TOP_LEVEL_OBJECT,

        CustomError = RLM_ERR_CUSTOM_ERROR,

        ClientUserNotFound = RLM_ERR_CLIENT_USER_NOT_FOUND,
        ClientUserNotLoggedIn = RLM_ERR_CLIENT_USER_NOT_LOGGED_IN,
        ClientAppDeallocated = RLM_ERR_CLIENT_APP_DEALLOCATED,
        ClientRedirectError = RLM_ERR_CLIENT_REDIRECT_ERROR,
        ClientTooManyRedirects = RLM_ERR_CLIENT_TOO_MANY_REDIRECTS,

        BadToken = RLM_ERR_BAD_TOKEN,
        MalformedJson = RLM_ERR_MALFORMED_JSON,
        MissingJsonKey = RLM_ERR_MISSING_JSON_KEY,
        BadBsonParse = RLM_ERR_BAD_BSON_PARSE,

        WebSocketResolveFailedError = RLM_ERR_WEBSOCKET_RESOLVE_FAILED_ERROR,
        WebSocketConnectionClosedClientError = RLM_ERR_WEBSOCKET_CONNECTION_CLOSED_CLIENT_ERROR,
        WebSocketConnectionClosedServerError = RLM_ERR_WEBSOCKET_CONNECTION_CLOSED_SERVER_ERROR,

        InvalidJSON = RLM_ERR_INVALID_JSON,
        InvalidJSONFieldType = RLM_ERR_INVALID_JSON_FIELD_TYPE,
        APIKeyAlreadyExists = RLM_ERR_API_KEY_ALREADY_EXISTS,
        APIKeyNotFound = RLM_ERR_API_KEY_NOT_FOUND,
        AWSError = RLM_ERR_AWS_ERROR,
        ArgumentsNotAllowed = RLM_ERR_ARGUMENTS_NOT_ALLOWED,
        AuthProviderAlreadyExists = RLM_ERR_AUTH_PROVIDER_ALREADY_EXISTS,
        AuthProviderDuplicateName = RLM_ERR_AUTH_PROVIDER_DUPLICATE_NAME,
        AuthProviderNotFound = RLM_ERR_AUTH_PROVIDER_NOT_FOUND,
        DomainNotAllowed = RLM_ERR_DOMAIN_NOT_ALLOWED,
        EndpointAlreadyExists = RLM_ERR_ENDPOINT_ALREADY_EXISTS,
        EndpointDuplicateKey = RLM_ERR_ENDPOINT_DUPLICATE_KEY,
        EndpointNotFound = RLM_ERR_ENDPOINT_NOT_FOUND,
        EventSubscriptionAlreadyExists = RLM_ERR_EVENT_SUBSCRIPTION_ALREADY_EXISTS,
        EventSubscriptionDuplicateName = RLM_ERR_EVENT_SUBSCRIPTION_DUPLICATE_NAME,
        EventSubscriptionNotFound = RLM_ERR_EVENT_SUBSCRIPTION_NOT_FOUND,
        EventSubscriptionTypeChange = RLM_ERR_EVENT_SUBSCRIPTION_TYPE_CHANGE,
        ErrCodeEventSubscriptionMaxCount = RLM_ERR_EVENT_SUBSCRIPTION_MAX_COUNT,
        ExecutionMemoryLimitExceeded = RLM_ERR_EXECUTION_MEMORY_LIMIT_EXCEEDED,
        ExecutionTimeLimitExceeded = RLM_ERR_EXECUTION_TIME_LIMIT_EXCEEDED,
        FileHashDoesNotMatch = RLM_ERR_FILE_HASH_DOES_NOT_MATCH,
        FileSizeGivenIncorrect = RLM_ERR_FILE_SIZE_GIVEN_INCORRECT,
        FunctionAlreadyExists = RLM_ERR_FUNCTION_ALREADY_EXISTS,
        FunctionDuplicateName = RLM_ERR_FUNCTION_DUPLICATE_NAME,
        FunctionExecutionError = RLM_ERR_FUNCTION_EXECUTION_ERROR,
        FunctionInvalid = RLM_ERR_FUNCTION_INVALID,
        FunctionNotFound = RLM_ERR_FUNCTION_NOT_FOUND,
        FunctionSyntaxError = RLM_ERR_FUNCTION_SYNTAX_ERROR,
        FunctionMaxStackDepthError = RLM_ERR_FUNCTION_MAX_STACK_DEPTH_ERROR,
        GCMError = RLM_ERR_GCM_ERROR,
        HostingInvalidDomain = RLM_ERR_HOSTING_INVALID_DOMAIN,
        HostingChangesInProgress = RLM_ERR_HOSTING_CHANGES_IN_PROGRESS,
        HostingFileLimitExceeded = RLM_ERR_HOSTING_FILE_LIMIT_EXCEEDED,
        HostingNumFilesLimitExceeded = RLM_ERR_HOSTING_NUM_FILES_LIMIT_EXCEEDED,
        HostingTotalSizeLimitExceeded = RLM_ERR_HOSTING_TOTAL_SIZE_LIMIT_EXCEEDED,
        HostingDirectoryDepthLimitExceeded = RLM_ERR_HOSTING_DIRECTORY_DEPTH_LIMIT_EXCEEDED,
        HostingNotEnabled = RLM_ERR_HOSTING_NOT_ENABLED,
        HTTPError = RLM_ERR_HTTP_ERROR,
        ImportAppError = RLM_ERR_IMPORT_APP_ERROR,
        IncomingWebhookAlreadyExists = RLM_ERR_INCOMING_WEBHOOK_ALREADY_EXISTS,
        IncomingWebhookAuthFailed = RLM_ERR_INCOMING_WEBHOOK_AUTH_FAILED,
        IncomingWebhookDuplicateName = RLM_ERR_INCOMING_WEBHOOK_DUPLICATE_NAME,
        IncomingWebhookNotFound = RLM_ERR_INCOMING_WEBHOOK_NOT_FOUND,
        NoHTTPWebhooksToConvert = RLM_ERR_NO_HTTP_WEBHOOKS_TO_CONVERT,
        InternalServerError = RLM_ERR_INTERNAL_SERVER_ERROR,
        InvalidParameter = RLM_ERR_INVALID_PARAMETER,
        InvalidPassword = RLM_ERR_INVALID_PASSWORD,
        AuthError = RLM_ERR_AUTH_ERROR,
        InvalidSession = RLM_ERR_INVALID_SESSION,
        RestrictedUIIP = RLM_ERR_RESTRICTED_UIIP,
        InvalidFile = RLM_ERR_INVALID_FILE,
        // File not found already defined
        InvalidMove = RLM_ERR_INVALID_MOVE,
        InvalidDirectory = RLM_ERR_INVALID_DIRECTORY,
        DirectoryAlreadyExists = RLM_ERR_DIRECTORY_ALREADY_EXISTS,
        AssetMetadataNotFound = RLM_ERR_ASSET_METADATA_NOT_FOUND,
        InvalidAuthReq = RLM_ERR_INVALID_AUTH_REQ,
        MissingAuthReq = RLM_ERR_MISSING_AUTH_REQ,
        InvalidRedirectURI = RLM_ERR_INVALID_REDIRECT_URI,
        LinkingNotSupported = RLM_ERR_LINKING_NOT_SUPPORTED,
        MissingParameter = RLM_ERR_MISSING_PARAMETER,
        MongoDBError = RLM_ERR_MONGODB_ERROR,
        NoFilesToPush = RLM_ERR_NO_FILES_TO_PUSH,
        DeletedFileNotFound = RLM_ERR_DELETED_FILE_NOT_FOUND,
        NoMatchingRoleFound = RLM_ERR_NO_MATCHING_ROLE_FOUND,
        NoMatchingRuleFound = RLM_ERR_NO_MATCHING_RULE_FOUND,
        FunctionNotCallable = RLM_ERR_FUNCTION_NOT_CALLABLE,
        PromiseConversionError = RLM_ERR_PROMISE_CONVERSION_ERROR,
        PromiseValueError = RLM_ERR_PROMISE_VALUE_ERROR,
        ReadSizeLimitExceeded = RLM_ERR_READ_SIZE_LIMIT_EXCEEDED,
        PromiseRejected = RLM_ERR_PROMISE_REJECTED,
        RestrictedHost = RLM_ERR_RESTRICTED_HOST,
        DefaultRuleAlreadyExists = RLM_ERR_DEFAULT_RULE_ALREADY_EXISTS,
        RuleAlreadyExists = RLM_ERR_RULE_ALREADY_EXISTS,
        DefaultRuleIDMismatch = RLM_ERR_DEFAULT_RULE_ID_MISMATCH,
        RuleDuplicateName = RLM_ERR_RULE_DUPLICATE_NAME,
        DefaultRuleNotFound = RLM_ERR_DEFAULT_RULE_NOT_FOUND,
        RuleNotFound = RLM_ERR_RULE_NOT_FOUND,
        RulesNotFound = RLM_ERR_RULES_NOT_FOUND,
        DatabaseRulesNotFound = RLM_ERR_DATABASE_RULES_NOT_FOUND,
        ServiceAlreadyExists = RLM_ERR_SERVICE_ALREADY_EXISTS,
        ServiceCommandNotFound = RLM_ERR_SERVICE_COMMAND_NOT_FOUND,
        ServiceNotFound = RLM_ERR_SERVICE_NOT_FOUND,
        ServiceTypeNotFound = RLM_ERR_SERVICE_TYPE_NOT_FOUND,
        ServiceAssociatedWithTrigger = RLM_ERR_SERVICE_ASSOCIATED_WITH_TRIGGER,
        ServiceAssociatedWithLogForwarder = RLM_ERR_SERVICE_ASSOCIATED_WITH_LOG_FORWARDER,
        TwilioError = RLM_ERR_TWILIO_ERROR,
        UserAlreadyConfirmed = RLM_ERR_USER_ALREADY_CONFIRMED,
        UserAppDomainMismatch = RLM_ERR_USER_APP_DOMAIN_MISMATCH,
        UserDisabled = RLM_ERR_USER_DISABLED,
        UserNotFound = RLM_ERR_USER_NOT_FOUND,
        ValueAlreadyExists = RLM_ERR_VALUE_ALREADY_EXISTS,
        ValueDuplicateName = RLM_ERR_VALUE_DUPLICATE_NAME,
        ValueNotFound = RLM_ERR_VALUE_NOT_FOUND,
        SecretAlreadyExists = RLM_ERR_SECRET_ALREADY_EXISTS,
        SecretDuplicateName = RLM_ERR_SECRET_DUPLICATE_NAME,
        SecretNotFound = RLM_ERR_SECRET_NOT_FOUND,
        EnvironmentTagInvalid = RLM_ERR_ENVIRONMENT_TAG_INVALID,
        EnvironmentValueAlreadyExists = RLM_ERR_ENVIRONMENT_VALUE_ALREADY_EXISTS,
        EnvironmentValueDuplicateName = RLM_ERR_ENVIRONMENT_VALUE_DUPLICATE_NAME,
        EnvironmentValueNotFound = RLM_ERR_ENVIRONMENT_VALUE_NOT_FOUND,
        DraftAlreadyExists = RLM_ERR_DRAFT_ALREADY_EXISTS,
        DraftNotFound = RLM_ERR_DRAFT_NOT_FOUND,
        DeploymentNotFound = RLM_ERR_DEPLOYMENT_NOT_FOUND,
        DeploymentStateNotFound = RLM_ERR_DEPLOYMENT_STATE_NOT_FOUND,
        DeploymentNotRedeployable = RLM_ERR_DEPLOYMENT_NOT_REDEPLOYABLE,
        CounterNotFound = RLM_ERR_COUNTER_NOT_FOUND,
        InstallationNotFound = RLM_ERR_INSTALLATION_NOT_FOUND,
        AccountNameInUse = RLM_ERR_ACCOUNT_NAME_IN_USE,
        BadRequest = RLM_ERR_BAD_REQUEST,
        AppDependenciesNotFound = RLM_ERR_APP_DEPENDENCIES_NOT_FOUND,
        AppDependenciesDraftNotFound = RLM_ERR_APP_DEPENDENCIES_DRAFT_NOT_FOUND,
        UserpassTokenInvalid = RLM_ERR_USERPASS_TOKEN_INVALID,
        BehindCurrentVersion = RLM_ERR_BEHIND_CURRENT_VERSION,
        SchemaValidationFailedRead = RLM_ERR_SCHEMA_VALIDATION_FAILED_READ,
        SchemaValidationFailedWrite = RLM_ERR_SCHEMA_VALIDATION_FAILED_WRITE,
        UncaughtPromiseRejection = RLM_ERR_UNCAUGHT_PROMISE_REJECTION,
        DependencyHashNotFound = RLM_ERR_DEPENDENCY_HASH_NOT_FOUND,
        OperationCanceled = RLM_ERR_OPERATION_CANCELED,
        CustomResolverAlreadyExists = RLM_ERR_CUSTOM_RESOLVER_ALREADY_EXISTS,
        DestructiveChangeNotAllowed = RLM_ERR_DESTRUCTIVE_CHANGE_NOT_ALLOWED,
        DependencyInstallationNotFound = RLM_ERR_DEPENDENCY_INSTALLATION_NOT_FOUND,
        DependencyNotFound = RLM_ERR_DEPENDENCY_NOT_FOUND,
        CollectionAlreadyExists = RLM_ERR_COLLECTION_ALREADY_EXISTS,
        MaintenanceInProgress = RLM_ERR_MAINTENANCE_IN_PROGRESS,
        MaintenanceInProgressAdmin = RLM_ERR_MAINTENANCE_IN_PROGRESS_ADMIN,
        MatchingCurrentDeploymentModelAndProviderRegion =
            RLM_ERR_MATCHING_CURRENT_DEPLOYMENT_MODEL_AND_PROVIDER_REGION,
        SchemaAlreadyExists = RLM_ERR_SCHEMA_ALREADY_EXISTS,
        SchemaMetadataExists = RLM_ERR_SCHEMA_METADATA_EXISTS,
        SchemaNotFound = RLM_ERR_SCHEMA_NOT_FOUND,
        InvalidJSONSchema = RLM_ERR_INVALID_JSON_SCHEMA,
        AllowedIPAlreadyExists = RLM_ERR_ALLOWED_IP_ALREADY_EXISTS,
        AllowedIPDuplicateAddress = RLM_ERR_ALLOWED_IP_DUPLICATE_ADDRESS,
        AllowedIPNotFound = RLM_ERR_ALLOWED_IP_NOT_FOUND,
        ProviderPrivateEndpointAlreadyExists = RLM_ERR_PROVIDER_PRIVATE_ENDPOINT_ALREADY_EXISTS,
        ProviderPrivateEndpointDuplicateEndpointID = RLM_ERR_PROVIDER_PRIVATE_ENDPOINT_DUPLICATE_ENDPOINT_ID,
        ProviderPrivateEndpointNotFound = RLM_ERR_PROVIDER_PRIVATE_ENDPOINT_NOT_FOUND,
        RelationshipMissingSourceProperty = RLM_ERR_RELATIONSHIP_MISSING_SOURCE_PROPERTY,
        RelationshipMissingSourceType = RLM_ERR_RELATIONSHIP_MISSING_SOURCE_TYPE,
        RelationshipHasProperties = RLM_ERR_RELATIONSHIP_HAS_PROPERTIES,
        RelationshipInvalidAdditionalProperties = RLM_ERR_RELATIONSHIP_INVALID_ADDITIONAL_PROPERTIES,
        RelationshipInvalidItems = RLM_ERR_RELATIONSHIP_INVALID_ITEMS,
        RelationshipUnrelatableSourceType = RLM_ERR_RELATIONSHIP_UNRELATABLE_SOURCE_TYPE,
        RelationshipMissingRefSchema = RLM_ERR_RELATIONSHIP_MISSING_REF_SCHEMA,
        RelationshipTypeMismatch = RLM_ERR_RELATIONSHIP_TYPE_MISMATCH,
        NoMongoDBRulesFound = RLM_ERR_NO_MONGODB_RULES_FOUND,
        NoMongoDBServiceFound = RLM_ERR_NO_MONGODB_SERVICE_FOUND,
        NoSchemasFound = RLM_ERR_NO_SCHEMAS_FOUND,
        InvalidSchema = RLM_ERR_INVALID_SCHEMA,
        UnsupportedTableType = RLM_ERR_UNSUPPORTED_TABLE_TYPE,
        InvalidType = RLM_ERR_INVALID_TYPE,
        UnsupportedType = RLM_ERR_UNSUPPORTED_TYPE,
        MissingSchemaType = RLM_ERR_MISSING_SCHEMA_TYPE,
        InvalidItems = RLM_ERR_INVALID_ITEMS,
        InvalidDefault = RLM_ERR_INVALID_DEFAULT,
        MissingRelationshipRuleDefinition = RLM_ERR_MISSING_RELATIONSHIP_RULE_DEFINITION,
        MissingRelationshipSchemaDefinition = RLM_ERR_MISSING_RELATIONSHIP_SCHEMA_DEFINITION,
        InvalidSyncSchema = RLM_ERR_INVALID_SYNC_SCHEMA,
        SchemaRootMissingUnderscoreID = RLM_ERR_SCHEMA_ROOT_MISSING_UNDERSCORE_ID,
        SchemaRootOptionalUnderscoreID = RLM_ERR_SCHEMA_ROOT_OPTIONAL_UNDERSCORE_ID,
        SyncAlreadyEnabled = RLM_ERR_SYNC_ALREADY_ENABLED,
        SyncFailedToPatchSchemas = RLM_ERR_SYNC_FAILED_TO_PATCH_SCHEMAS,
        SyncInvalidPartitionKey = RLM_ERR_SYNC_INVALID_PARTITION_KEY,
        SyncLifecycleError = RLM_ERR_SYNC_LIFE_CYCLE_ERROR,
        UnsupportedRealmClientLanguage = RLM_ERR_UNSUPPORTED_REALM_CLIENT_LANGUAGE,
        SyncDeploymentSuccessWithWarning = RLM_ERR_SYNC_DEPLOYMENT_SUCCESS_WITH_WARNING,
        SyncFailedToCreateIndexOnPartitionKey = RLM_ERR_SYNC_FAILED_TO_CREATE_INDEX_ON_PARTITION_KEY,
        SyncProtocolVersionIncrease = RLM_ERR_SYNC_PROTOCOL_VERSION_INCREASE,
        DestructiveSyncProtocolVersionIncrease = RLM_ERR_DESTRUCTIVE_SYNC_PROTOCOL_VERSION_INCREASE,
        SyncIncompatibleRole = RLM_ERR_SYNC_INCOMPATIBLE_ROLE,
        IncompatibleMongoDBVersion = RLM_ERR_INCOMPATIBLE_MONGODB_VERSION,
        NoRunAsSystem = RLM_ERR_NO_RUN_AS_SYSTEM,
        EmptyDataAPIConfig = RLM_ERR_EMPTY_DATA_API_CONFIG,
        DatasourceAlreadyExists = RLM_ERR_DATA_SOURCE_ALREADY_EXISTS,

        NotCallable = RLM_ERR_NOT_CALLABLE,
        InvalidServerResponse = RLM_ERR_INVALID_SERVER_RESPONSE,
        AppUnknownError = RLM_ERR_APP_UNKNOWN,

        CallbackFailed = RLM_ERR_CALLBACK,
        UnknownError = RLM_ERR_UNKNOWN,
    };

    static ErrorCategory error_categories(Error code);
    static std::string_view error_string(Error code);
    static Error from_string(std::string_view str);
    static std::vector<Error> get_all_codes();
    static std::vector<std::string_view> get_all_names();
    static std::vector<std::pair<std::string_view, ErrorCodes::Error>> get_error_list();
};

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code);

} // namespace realm
