/*************************************************************************
 *
 * Copyright 2024 Realm Inc.
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

#include "test.hpp"

#include "realm/status.hpp"

#include <Foundation/Foundation.h>

namespace realm {
namespace {
TEST(Status_NSException)
{
    try {
        @throw [NSException exceptionWithName:@"Exception Name" reason:@"Expected reason" userInfo:nil];
    }
    catch (...) {
        auto status = exception_to_status();
        CHECK_EQUAL(status.code(), ErrorCodes::UnknownError);
        CHECK_EQUAL(status.reason(), "Expected reason");
    }
}
} // namespace
} // namespace realm
