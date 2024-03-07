// Copyright 2010 Google
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef BASE_LOGGING_H
#define BASE_LOGGING_H

#include <realm/util/assert.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/to_string.hpp>

// Always-on checking
#define CHECK(x)	REALM_ASSERT(x)
#define CHECK_EX(x, ...)  REALM_ASSERT_EX(x, __VA_ARGS__)
#define CHECK_LT(x, y)	REALM_ASSERT_3((x), <, (y))
#define CHECK_GT(x, y)	REALM_ASSERT_3((x), >, (y))
#define CHECK_LE(x, y)	REALM_ASSERT_3((x), <=, (y))
#define CHECK_GE(x, y)	REALM_ASSERT_3((x), >=, (y))
#define CHECK_EQ(x, y)	REALM_ASSERT_3((x), ==, (y))
#define CHECK_NE(x, y)	REALM_ASSERT_3((x), !=, (y))

// Checking which is only fatal in debug mode
#define DCHECK(condition) REALM_ASSERT_DEBUG(condition)
#define DCHECK_EX(condition, ...) REALM_ASSERT_DEBUG_EX(condition, __VA_ARGS__)
#define DCHECK_EQ(val1, val2) REALM_ASSERT_DEBUG_EX(val1 == val2, val1, val2)
#define DCHECK_NE(val1, val2) REALM_ASSERT_DEBUG_EX(val1 != val2, val1, val2)
#define DCHECK_LE(val1, val2) REALM_ASSERT_DEBUG_EX(val1 <= val2, val1, val2)
#define DCHECK_LT(val1, val2) REALM_ASSERT_DEBUG_EX(val1 < val2, val1, val2)
#define DCHECK_GE(val1, val2) REALM_ASSERT_DEBUG_EX(val1 >= val2, val1, val2)
#define DCHECK_GT(val1, val2) REALM_ASSERT_DEBUG_EX(val1 > val2, val1, val2)

static inline realm::util::Logger* s2_logger()
{
    static realm::util::CategoryLogger my_logger(realm::util::LogCategory::query, realm::util::Logger::get_default_logger());
    return &my_logger;
}

#endif  // BASE_LOGGING_H
