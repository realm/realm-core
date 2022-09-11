////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_TEST_CATCH_MACROS_HPP
#define REALM_TEST_CATCH_MACROS_HPP

#include <catch2/catch_all.hpp>
#include <catch2/matchers/catch_matchers_all.hpp>

#define REQUIRE_DIR_EXISTS(macro_path)                                                                               \
    do {                                                                                                             \
        CHECK(util::File::is_dir(macro_path) == true);                                                               \
    } while (0)

#define REQUIRE_DIR_PATH_EXISTS(macro_path)                                                                          \
    do {                                                                                                             \
        REQUIRE(util::File::is_dir((macro_path).string()));                                                          \
    } while (0)

#define REQUIRE_DIR_DOES_NOT_EXIST(macro_path)                                                                       \
    do {                                                                                                             \
        CHECK(util::File::exists(macro_path) == false);                                                              \
    } while (0)

#define REQUIRE_DIR_PATH_DOES_NOT_EXIST(macro_path)                                                                  \
    do {                                                                                                             \
        REQUIRE_FALSE(util::File::exists((macro_path).string()));                                                    \
    } while (0)

#define REQUIRE_REALM_EXISTS(macro_path)                                                                             \
    do {                                                                                                             \
        REQUIRE(util::File::exists(macro_path));                                                                     \
        REQUIRE(util::File::exists((macro_path) + ".lock"));                                                         \
        REQUIRE_DIR_EXISTS((macro_path) + ".management");                                                            \
    } while (0)

#define REQUIRE_REALM_DOES_NOT_EXIST(macro_path)                                                                     \
    do {                                                                                                             \
        REQUIRE(!util::File::exists(macro_path));                                                                    \
        REQUIRE(!util::File::exists((macro_path) + ".lock"));                                                        \
        REQUIRE_DIR_DOES_NOT_EXIST((macro_path) + ".management");                                                    \
    } while (0)

#define REQUIRE_THROWS_CONTAINING(expr, msg) REQUIRE_THROWS_WITH(expr, Catch::Matchers::ContainsSubstring(msg))

#endif // REALM_TEST_CATCH_MACROS_HPP
