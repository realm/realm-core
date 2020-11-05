////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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

#include <catch2/catch.hpp>

#include "util/test_file.hpp"

#include <realm/object-store/dictionary.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm.hpp>
#include <realm/query_expression.hpp>

using namespace realm;
using namespace realm::util;


TEST_CASE("dictionary")
{
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Dictionary | PropertyType::String}}},
    };
    auto r = Realm::get_shared_realm(config);

    auto table = r->read_group().get_table("class_object");
    r->begin_transaction();
    Obj obj = table->create_object();
    ColKey col = table->get_column_key("value");

    object_store::Dictionary dict(r, obj, col);
    auto results = dict.as_results();
    CppContext ctx(r);

    SECTION("get_realm()")
    {
        REQUIRE(dict.get_realm() == r);
        REQUIRE(results.get_realm() == r);
    }

    std::vector<std::string> keys = {"a", "b", "c"};
    std::vector<std::string> values = {"apple", "banana", "clementine"};

    for (size_t i = 0; i < values.size(); ++i) {
        dict.insert(keys[i], values[i]);
    }


    SECTION("clear()")
    {
        REQUIRE(dict.size() == 3);
        results.clear();
        REQUIRE(dict.size() == 0);
        REQUIRE(results.size() == 0);
    }

    SECTION("get()")
    {
        for (size_t i = 0; i < values.size(); ++i) {
            REQUIRE(dict.get<String>(keys[i]) == values[i]);
            auto val = dict.get(ctx, util::Any(keys[i]));
            REQUIRE(any_cast<std::string>(val) == values[i]);
        }
    }

    SECTION("insert()")
    {
        for (size_t i = 0; i < values.size(); ++i) {
            auto rev = values.size() - i - 1;
            dict.insert(keys[i], values[rev]);
            REQUIRE(dict.get<StringData>(keys[i]) == values[rev]);
        }
        for (size_t i = 0; i < values.size(); ++i) {
            dict.insert(ctx, util::Any(keys[i]), util::Any(values[i]));
            REQUIRE(dict.get<StringData>(keys[i]) == values[i]);
        }
    }
}
