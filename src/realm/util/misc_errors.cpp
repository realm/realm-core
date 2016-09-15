/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#include <realm/util/features.h>
#include <realm/util/assert.hpp>
#include <realm/util/misc_errors.hpp>

using namespace realm::util;


namespace {

class misc_category : public std::error_category {
    const char* name() const noexcept override;
    std::string message(int) const override;
};

misc_category g_misc_category;

const char* misc_category::name() const noexcept
{
    return "tigthdb.misc";
}

std::string misc_category::message(int value) const
{
    switch (error::misc_errors(value)) {
        case error::unknown:
            return "Unknown error";
    }
    REALM_ASSERT(false);
    return std::string();
}

} // anonymous namespace


namespace realm {
namespace util {
namespace error {

std::error_code make_error_code(misc_errors err)
{
    return std::error_code(err, g_misc_category);
}

} // namespace error
} // namespace util
} // namespace realm
