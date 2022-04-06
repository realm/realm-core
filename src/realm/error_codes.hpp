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

namespace realm {

// ErrorExtraInfo subclasses:

struct ErrorCategory {
    enum Type {
        generic_error = 1,
        logic_error = 2,
        runtime_error = 4,
        invalid_argument = 8,
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

private:
    int m_value;
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
        OK = 0,
        UnknownError = 1,
        RuntimeError = 2,
        LogicError = 3,
        BrokenPromise = 4,
        InvalidName = 5,
        OutOfMemory = 6,
        NoSuchTable = 7,
        CrossTableLinkTarget = 8,
        UnsupportedFileFormatVersion = 9,
        MultipleSyncAgents = 10,
        AddressSpaceExhausted = 11,
        OutOfDiskSpace = 12,
        KeyNotFound = 13,
        OutOfBounds = 14,
        IllegalOperation = 15,
        KeyAlreadyUsed = 16,
        SerializationError = 17,
        InvalidPath = 18,
        DuplicatePrimaryKeyValue = 19,
        SyntaxError = 20,
        InvalidQueryArg = 21,
        InvalidQuery = 22,
        WrongTransactioState = 23,
        WrongThread = 24,
        InvalidatedObject = 25,
        InvalidProperty = 26,
        MissingPrimaryKey = 27,
        UnexpectedPrimaryKey = 28,
        WrongPrimaryKeyType = 29,
        ModifyPrimaryKey = 30,
        ReadOnly = 31,
        PropertyNotNullable = 32,
        MaximumFileSizeExceeded = 33,
        TableNameInUse = 34,
        InvalidTableRef = 35,
        BadChangeset = 36,
        InvalidDictionaryKey = 37,
        InvalidDictionaryValue = 38,
        StaleAccessor = 39,
        SerialisationError = 40,
        IncompatibleLockFile = 41,
        InvalidSortDescriptor = 42,
        DecryptionFailed = 43,
        IncompatibleSession = 44,
        BrokenInvariant = 45,
        SubscriptionFailed = 46,
        RangeError = 47,
        TypeMismatch = 48,
        MaxError
    };

    static ErrorCategory error_categories(Error code);
    static std::string_view error_string(Error code);
    static Error from_string(std::string_view str);
};

} // namespace realm
