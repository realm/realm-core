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

#include <realm/string_data.hpp>

namespace realm {

// ErrorExtraInfo subclasses:

enum class ErrorCategory {
    generic_error,
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
        InvalidArgument = 5,
        OutOfMemory = 6,
        NoSuchTable = 7,
        NoSuchObject = 8,
        CrossTableLinkTarget = 9,
        UnsupportedFileFormatVersion = 10,
        MultipleSyncAgents = 11,
        AddressSpaceExhausted = 12,
        OutOfDiskSpace = 13,
        KeyNotFound = 14,
        ColumnNotFound = 15,
        ColumnExistsAlready = 16,
        KeyAlreadyUsed = 17,
        SerializationError = 18,
        InvalidPathError = 19,
        DuplicatePrimaryKeyValue = 20,
        InvalidQueryString = 21,
        InvalidQuery = 22,
        NotInATransaction = 23,
        WrongThread = 24,
        InvalidatedObject = 25,
        InvalidProperty = 26,
        MissingPrimaryKey = 27,
        UnexpectedPrimaryKey = 28,
        WrongPrimaryKeyType = 29,
        ModifyPrimaryKey = 30,
        ReadOnlyProperty = 31,
        PropertyNotNullable = 32,
        MaxError
    };

    static StringData error_string(Error code);
    static Error from_string(StringData str);

    /**
     * Generic predicate to test if a given error code is in a category.
     *
     * This version is intended to simplify forwarding by Status and ExceptionForStatus. Non-generic
     * callers should just use the specific isCategoryName() methods instead.
     */
    template <ErrorCategory category>
    static bool is_a(Error code);

    template <ErrorCategory category, typename ErrorContainer>
    static bool is_a(const ErrorContainer& object);

    static bool is_generic_error(Error code);
    template <typename ErrorContainer>
    static bool is_generic_error(const ErrorContainer& object);
};

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code);

template <ErrorCategory category, typename ErrorContainer>
inline bool ErrorCodes::is_a(const ErrorContainer& object)
{
    return is_a<category>(object.code());
}

// Category function declarations for "generic_error"
template <>
bool ErrorCodes::is_a<ErrorCategory::generic_error>(Error code);

inline bool ErrorCodes::is_generic_error(Error code)
{
    return is_a<ErrorCategory::generic_error>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::is_generic_error(const ErrorContainer& object)
{
    return is_a<ErrorCategory::generic_error>(object.code());
}

/**
 * This namespace contains implementation details for our error handling code and should not be used
 * directly in general code.
 */
namespace error_details {

template <int32_t code>
constexpr bool is_named_code = false;
template <>
constexpr inline bool is_named_code<ErrorCodes::OK> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::UnknownError> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::RuntimeError> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::LogicError> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::BrokenPromise> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::InvalidArgument> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::OutOfMemory> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::NoSuchTable> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::NoSuchObject> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::CrossTableLinkTarget> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::UnsupportedFileFormatVersion> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::MultipleSyncAgents> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::AddressSpaceExhausted> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::OutOfDiskSpace> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::KeyNotFound> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::ColumnNotFound> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::ColumnExistsAlready> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::KeyAlreadyUsed> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::SerializationError> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::InvalidPathError> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::DuplicatePrimaryKeyValue> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::InvalidQueryString> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::InvalidQuery> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::NotInATransaction> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::WrongThread> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::InvalidatedObject> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::InvalidProperty> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::MissingPrimaryKey> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::UnexpectedPrimaryKey> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::WrongPrimaryKeyType> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::ModifyPrimaryKey> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::ReadOnlyProperty> = true;
template <>
constexpr inline bool is_named_code<ErrorCodes::PropertyNotNullable> = true;

//
// ErrorCategoriesFor
//

template <ErrorCategory... categories>
struct CategoryList;

template <ErrorCodes::Error code>
struct ErrorCategoriesForImpl {
    using type = CategoryList<>;
};

template <>
struct ErrorCategoriesForImpl<ErrorCodes::UnknownError> {
    using type = CategoryList<ErrorCategory::generic_error>;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::RuntimeError> {
    using type = CategoryList<ErrorCategory::generic_error>;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::LogicError> {
    using type = CategoryList<ErrorCategory::generic_error>;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::InvalidArgument> {
    using type = CategoryList<ErrorCategory::generic_error>;
};

template <ErrorCodes::Error code>
using ErrorCategoriesFor = typename ErrorCategoriesForImpl<code>::type;

} // namespace error_details

} // namespace realm
