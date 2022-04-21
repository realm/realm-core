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

#include <type_traits>
#include <string>
#include <realm/error_codes.h>

namespace realm {

// ErrorExtraInfo subclasses:

struct ErrorCategory {
    enum Type {
        generic_error = 1,
        logic_error = RLM_ERR_CAT_LOGIC,
        runtime_error = RLM_ERR_CAT_RUNTIME,
        invalid_argument = RLM_ERR_CAT_INVALID_ARG,
        file_access = RLM_ERR_CAT_FILE_ACCESS,
        system_error = RLM_ERR_CAT_SYSTEM_ERROR,
    };
    constexpr ErrorCategory()
        : m_value(0)
    {
    }
    bool test(Type cat)
    {
        return (m_value & cat) != 0;
    }
    constexpr ErrorCategory& set(Type cat)
    {
        m_value |= cat;
        return *this;
    }
    void reset(Type cat)
    {
        m_value &= ~cat;
    }
    bool operator==(const ErrorCategory& other) const
    {
        return m_value == other.m_value;
    }
    bool operator!=(const ErrorCategory& other) const
    {
        return m_value != other.m_value;
    }
    int value() const
    {
        return m_value;
    }

private:
    unsigned m_value;
};

/*
 * This is a generated class containing a table of error codes and their corresponding error
 * strings. The class is derived from the definitions in src/realm/error_codes.yml file and the
 * src/realm/error_codes.tpl.hpp template.
 *
 * Do not update this file directly. Update src/realm/error_codes.yml instead.
 */
class ErrorCodes {
public:
    // Explicitly 32-bits wide so that non-symbolic values,
    // like uassert codes, are valid.
    enum Error : std::int32_t {
        OK = RLM_ERR_NONE,
        UnknownError = RLM_ERR_UNKNOWN,
        GenericError = RLM_ERR_OTHER_EXCEPTION,
        RuntimeError = RLM_ERR_RUNTIME,
        LogicError = RLM_ERR_LOGIC,
        InvalidArgument = RLM_ERR_INVALID_ARGUMENT,
        BrokenPromise = RLM_ERR_BROKEN_PROMISE,
        InvalidName = RLM_ERR_INVALID_NAME,
        OutOfMemory = RLM_ERR_OUT_OF_MEMORY,
        NoSuchTable = RLM_ERR_NO_SUCH_TABLE,
        CrossTableLinkTarget = RLM_ERR_CROSS_TABLE_LINK_TARGET,
        UnsupportedFileFormatVersion = RLM_ERR_UNSUPPORTED_FILE_FORMAT_VERSION,
        FileFormatUpgradeRequired = RLM_ERR_FILE_FORMAT_UPGRADE_REQUIRED,
        MultipleSyncAgents = RLM_ERR_MULTIPLE_SYNC_AGENTS,
        AddressSpaceExhausted = RLM_ERR_ADDRESS_SPACE_EXHAUSTED,
        OutOfDiskSpace = RLM_ERR_OUT_OF_DISK_SPACE,
        MaximumFileSizeExceeded = RLM_ERR_MAXIMUM_FILE_SIZE_EXCEEDED,
        KeyNotFound = RLM_ERR_NO_SUCH_OBJECT,
        OutOfBounds = RLM_ERR_INDEX_OUT_OF_BOUNDS,
        IllegalOperation = RLM_ERR_ILLEGAL_OPERATION,
        KeyAlreadyUsed = RLM_ERR_KEY_ALREADY_USED,
        SerializationError = RLM_ERR_SERIALIZATION_ERROR,
        InvalidPath = RLM_ERR_INVALID_PATH_ERROR,
        DuplicatePrimaryKeyValue = RLM_ERR_DUPLICATE_PRIMARY_KEY_VALUE,
        SyntaxError = RLM_ERR_INVALID_QUERY_STRING,
        InvalidQueryArg = RLM_ERR_INVALID_QUERY_ARG,
        InvalidQuery = RLM_ERR_INVALID_QUERY,
        WrongTransactionState = RLM_ERR_WRONG_TRANSACTION_STATE,
        WrongThread = RLM_ERR_WRONG_THREAD,
        InvalidatedObject = RLM_ERR_INVALIDATED_OBJECT,
        InvalidProperty = RLM_ERR_INVALID_PROPERTY,
        MissingPrimaryKey = RLM_ERR_MISSING_PRIMARY_KEY,
        UnexpectedPrimaryKey = RLM_ERR_UNEXPECTED_PRIMARY_KEY,
        ObjectAlreadyExists = RLM_ERR_OBJECT_ALREADY_EXISTS,
        ModifyPrimaryKey = RLM_ERR_MODIFY_PRIMARY_KEY,
        ReadOnly = RLM_ERR_READ_ONLY,
        PropertyNotNullable = RLM_ERR_PROPERTY_NOT_NULLABLE,
        TableNameInUse = RLM_ERR_TABLE_NAME_IN_USE,
        InvalidTableRef = RLM_ERR_INVALID_TABLE_REF,
        BadChangeset = RLM_ERR_BAD_CHANGESET,
        InvalidDictionaryKey = RLM_ERR_INVALID_DICTIONARY_KEY,
        InvalidDictionaryValue = RLM_ERR_INVALID_DICTIONARY_VALUE,
        StaleAccessor = RLM_ERR_STALE_ACCESSOR,
        IncompatibleLockFile = RLM_ERR_INCOMPATIBLE_LOCK_FILE,
        InvalidSortDescriptor = RLM_ERR_INVALID_SORT_DESCRIPTOR,
        DecryptionFailed = RLM_ERR_DECRYPTION_FAILED,
        IncompatibleSession = RLM_ERR_INCOMPATIBLE_SESSION,
        BrokenInvariant = RLM_ERR_BROKEN_INVARIANT,
        SubscriptionFailed = RLM_ERR_SUBSCRIPTION_FAILED,
        RangeError = RLM_ERR_RANGE_ERROR,
        TypeMismatch = RLM_ERR_PROPERTY_TYPE_MISMATCH,
        LimitExceeded = RLM_ERR_LIMIT_EXCEEDED,
        MissingPropertyValue = RLM_ERR_MISSING_PROPERTY_VALUE,
        ReadOnlyProperty = RLM_ERR_READ_ONLY_PROPERTY,
        CallbackFailed = RLM_ERR_CALLBACK,
        NotCloneable = RLM_ERR_NOT_CLONABLE,
        PermissionDenied = RLM_ERR_FILE_PERMISSION_DENIED,
        FileOperationFailed = RLM_ERR_FILE_OPERATION_FAILED,
        FileNotFound = RLM_ERR_FILE_NOT_FOUND,
        FileAlreadyExists = RLM_ERR_FILE_ALREADY_EXISTS,
        SystemError = RLM_ERR_SYSTEM_ERROR,
        InvalidDatabase = RLM_ERR_INVALID_DATABASE,
        IncompatibleHistories = RLM_ERR_INCOMPATIBLE_HISTORIES,
        DeleteOnOpenRealm = RLM_ERR_DELETE_OPENED_REALM,
        NotSupported = REALM_ERR_NOT_SUPPORTED,
        MaxError
    };

    static ErrorCategory error_categories(Error code);
    static std::string_view error_string(Error code);
    static Error from_string(std::string_view str);
};

} // namespace realm
