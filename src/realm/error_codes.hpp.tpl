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
//#for $ec in $codes:
//#if $ec.extra
//#if $ec.extra_ns
namespace $ec.extra_ns {
    //#end if
    class $ec.extra_class;
    //#if $ec.extra_ns
}  // namespace $ec.extra_ns
//#end if
//#end if
//#end for

enum class ErrorCategory {
    //#for $cat in $categories
    ${cat.name},
    //#end for
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
        //#for $ec in $codes
        $ec.name = $ec.code,
        //#end for
        MaxError
    };

    static std::string_view error_string(Error code);
    static Error from_string(std::string_view str);

    /**
     * Generic predicate to test if a given error code is in a category.
     *
     * This version is intended to simplify forwarding by Status and Exception. Non-generic
     * callers should just use the specific isCategoryName() methods instead.
     */
    template <ErrorCategory category>
    static bool is_a(Error code);

    template <ErrorCategory category, typename ErrorContainer>
    static bool is_a(const ErrorContainer& object);

    //#for $cat in $categories
    static bool is_${cat.name}(Error code);
    template <typename ErrorContainer>
    static bool is_${cat.name}(const ErrorContainer& object);

    //#end for
};

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code);

template <ErrorCategory category, typename ErrorContainer>
inline bool ErrorCodes::is_a(const ErrorContainer& object)
{
    return is_a<category>(object.code());
}

//#for $cat in $categories
// Category function declarations for "${cat.name}"
template <>
bool ErrorCodes::is_a<ErrorCategory::$cat.name>(Error code);

inline bool ErrorCodes::is_${cat.name}(Error code)
{
    return is_a<ErrorCategory::$cat.name>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::is_${cat.name}(const ErrorContainer& object)
{
    return is_a<ErrorCategory::$cat.name>(object.code());
}

//#end for
/**
 * This namespace contains implementation details for our error handling code and should not be used
 * directly in general code.
 */
namespace error_details {

template <int32_t code>
constexpr bool is_named_code = false;
//#for $ec in $codes
template <>
constexpr inline bool is_named_code<ErrorCodes::$ec.name> = true;
//#end for

//
// ErrorCategoriesFor
//

template <ErrorCategory... categories>
struct CategoryList;

template <ErrorCodes::Error code>
struct ErrorCategoriesForImpl {
    using type = CategoryList<>;
};

//#for $ec in $codes:
//#if $ec.categories
template <>
struct ErrorCategoriesForImpl<ErrorCodes::$ec.name> {
    using type = CategoryList<
        //#for $i, $cat in enumerate($ec.categories)
        //#set $comma = '' if i == len($ec.categories) - 1 else ', '
        ErrorCategory::$cat$comma
        //#end for
        >;
};
//#end if
//#end for

template <ErrorCodes::Error code>
using ErrorCategoriesFor = typename ErrorCategoriesForImpl<code>::type;

} // namespace error_details

} // namespace realm
