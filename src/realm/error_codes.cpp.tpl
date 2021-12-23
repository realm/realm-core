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

//#set $codes_with_extra = [ec for ec in $codes if ec.extra]
//#set $codes_with_non_optional_extra = [ec for ec in $codes if ec.extra and not ec.extraIsOptional]

namespace realm {

namespace {
// You can think of this namespace as a compile-time map<ErrorCodes::Error, ErrorExtraInfoParser*>.
namespace parsers {
//#for $ec in $codes_with_extra
ErrorExtraInfo::Parser* $ec.name = nullptr;
//#end for
}  // namespace parsers
}  // namespace


StringData ErrorCodes::error_string(Error code)
{
    static_assert(sizeof(Error) == sizeof(int));

    switch (code) {
        //#for $ec in $codes
        case $ec.name:
            return "$ec.name";
        //#end for
        default:
            return "UnknownError";
    }
}

ErrorCodes::Error ErrorCodes::from_string(StringData name)
{
    //#for $ec in $codes
    if (name == StringData("$ec.name"))
        return $ec.name;
    //#end for
    return UnknownError;
}

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code) {
    return stream << ErrorCodes::error_string(code);
}

} // namespace realm
