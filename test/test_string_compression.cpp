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

#include "testsettings.hpp"

#include <cstring>
#include <string>
#include <sstream>

#include <realm.hpp>
#include <realm/string_data.hpp>
#include <realm/unicode.hpp>
#include <realm/string_interner.hpp>

#include "test.hpp"

using namespace realm;


TEST(StringInterner_Basic_Creation)
{
    Array parent(Allocator::get_default());
    parent.create(NodeHeader::type_HasRefs, false, 1, 0);
    StringInterner interner(Allocator::get_default(), parent, ColKey(0), true);
    StringData my_string = "aaaaaaaaaaaaaaa";

    auto id = interner.intern(my_string);

    const auto stored_id = interner.lookup(my_string);
    CHECK(stored_id);
    CHECK(*stored_id == id);

    CHECK(interner.compare(my_string, *stored_id) == 0); // should be equal
    const auto origin_string = interner.get(id);
    CHECK_EQUAL(my_string, origin_string);

    CHECK(interner.compare(*stored_id, id) == 0); // compare agaist self.
    parent.destroy_deep();
}

TEST(StringInterner_VerifyInterningNull)
{
    Array parent(Allocator::get_default());
    parent.create(NodeHeader::type_HasRefs, false, 1, 0);
    StringInterner interner(Allocator::get_default(), parent, ColKey(0), true);
    const auto id = interner.intern({});
    CHECK_EQUAL(id, 0);
    const auto stored_id = interner.lookup({});
    CHECK_EQUAL(stored_id, 0);
    CHECK(interner.compare({}, 0) == 0);
}

TEST(StringInterner_InternMultipleStrings)
{
    Array parent(Allocator::get_default());
    parent.create(NodeHeader::type_HasRefs, false, 1, 0);
    StringInterner interner(Allocator::get_default(), parent, ColKey(0), true);

    std::vector<std::string> strings;
    for (size_t i = 0; i < 100; i++)
        strings.push_back("aaaaaaaaaaaaa" + std::to_string(i));

    std::vector<StringID> ids;
    for (const auto& s : strings)
        ids.push_back(interner.intern(s));

    size_t i = 0;
    for (const auto& id : ids) {
        const auto& str = interner.get(id);
        CHECK(str == strings[i++]);
        auto stored_id = interner.lookup(str);
        CHECK_EQUAL(*stored_id, id);
        CHECK_EQUAL(interner.compare(str, id), 0);
    }
    parent.destroy_deep();
}

TEST(StringInterner_TestLookup)
{
    Array parent(Allocator::get_default());
    parent.create(NodeHeader::type_HasRefs, false, 1, 0);
    StringInterner interner(Allocator::get_default(), parent, ColKey(0), true);

    std::vector<std::string> strings;
    for (size_t i = 0; i < 500; ++i) {
        std::string my_string = "aaaaaaaaaaaaaaa" + std::to_string(i);
        strings.push_back(my_string);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(strings.begin(), strings.end(), g);

    for (const auto& s : strings) {
        interner.intern(s);
        auto id = interner.lookup(StringData(s));
        CHECK(id);
        CHECK(interner.compare(StringData(s), *id) == 0);
    }
    parent.destroy_deep();
}
