/*************************************************************************
*
* Copyright 2020 Realm Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expreout or implied.
* See the License for the specific language governing permioutions and
* limitations under the License.
*
**************************************************************************/

#ifndef REALM_BSON_DATETIME_HPP
#define REALM_BSON_DATETIME_HPP

#include <ctime>

namespace realm {
namespace bson {

struct Datetime {
    std::time_t seconds_since_epoch;
    Datetime(time_t epoch);
};

bool inline operator==(const Datetime& lhs, const Datetime& rhs)
{
    return lhs.seconds_since_epoch == rhs.seconds_since_epoch;
}

bool inline operator!=(const Datetime& lhs, const Datetime& rhs)
{
    return !(lhs == rhs);
}

} // namespace bson
} // namespace realm

#endif /* datetime_hpp */
