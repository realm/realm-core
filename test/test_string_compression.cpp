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
    Group group;
    TableRef table = group.add_table("test");
    auto string_col_key = table->add_column(type_String, "string");
    auto obj = table->create_object();
    std::string my_string = "aaaaaaaaaaaaaaa";
    obj.set(string_col_key, StringData(my_string));

    auto* string_interner = table->get_string_interner(string_col_key);
    auto id = string_interner->intern(obj.get_any(string_col_key).get_string());

    const auto stored_id = string_interner->lookup(StringData(my_string));
    CHECK(stored_id);
    CHECK(*stored_id == id);

    CHECK(string_interner->compare(StringData(my_string), *stored_id) == 0); // should be equal
    const auto origin_string = string_interner->get(id);
    CHECK(obj.get_any(string_col_key).get_string() == origin_string);

    CHECK(string_interner->compare(*stored_id, id) == 0); // compare agaist self.
}

TEST(StringInterner_Creation_Multiple_String_One_ColKey)
{
    Group group;
    TableRef table = group.add_table("test");
    const auto colkey = table->add_column(type_String, "string");
    auto obj = table->create_object();

    // every leaf contains by default 16  entries, after this the strings are "rehashed", meaning
    // that the leaf capacity is extended (next power of 2)
    StringID prev_string_id = 0;
    for (size_t i = 0; i < 20; ++i) {
        std::string my_string = "aaaaaaaaaaaaaaa" + std::to_string(i);
        obj.set(colkey, StringData(my_string));

        auto string_interner = table->get_string_interner(colkey);

        const auto& db_string = obj.get_any(colkey).get_string();
        auto id = string_interner->intern(db_string);

        CHECK(prev_string_id == id - 1);
        const auto stored_id = string_interner->lookup(StringData(db_string));
        CHECK(stored_id);
        CHECK(*stored_id == id);

        CHECK(string_interner->compare(StringData(my_string), *stored_id) == 0); // should be equal
        const auto origin_string = string_interner->get(id);
        CHECK(obj.get_any(colkey).get_string() == origin_string);

        CHECK(string_interner->compare(*stored_id, id) == 0); // compare agaist self.
        prev_string_id = id;
    }
}

TEST(StringInterner_Verify_Lookup)
{
    Group group;
    TableRef table = group.add_table("test");
    const auto colkey1 = table->add_column(type_String, "string1");
    const auto colkey2 = table->add_column(type_String, "string2");
    auto obj = table->create_object();

    auto string_interner1 = table->get_string_interner(colkey1);
    auto string_interner2 = table->get_string_interner(colkey2);

    std::vector<std::string> strings;
    for (size_t i = 0; i < 500; ++i) {
        std::string my_string = "aaaaaaaaaaaaaaa" + std::to_string(i);
        strings.push_back(my_string);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(strings.begin(), strings.end(), g);

    for (const auto& s : strings) {
        obj.set(colkey1, StringData(s));
        string_interner1->intern(obj.get_any(colkey1).get_string());
        auto interner1_id = string_interner1->lookup(StringData(s));
        CHECK(string_interner1->compare(StringData(s), *interner1_id) == 0);
    }

    std::shuffle(strings.begin(), strings.end(), g);

    for (const auto& s : strings) {
        obj.set(colkey2, StringData(s));
        string_interner2->intern(obj.get_any(colkey2).get_string());
        auto interner2_id = string_interner2->lookup(StringData(s));
        CHECK(string_interner2->compare(StringData(s), *interner2_id) == 0);
    }
}


TEST(StringInterner_Creation_Multiple_String_ColKey)
{
    Group group;
    TableRef table = group.add_table("test");

    std::vector<std::string> string_col_names;
    std::vector<ColKey> col_keys;

    for (size_t i = 0; i < 10; ++i)
        string_col_names.push_back("string_" + std::to_string(i));

    for (const auto& col_name : string_col_names)
        col_keys.push_back(table->add_column(type_String, col_name));

    auto obj = table->create_object();

    std::vector<std::string> strings;
    std::string my_string = "aaaaaaaaaaaaaaa";
    for (size_t i = 0; i < col_keys.size(); ++i) {
        strings.push_back(my_string + std::to_string(i));
        obj.set(col_keys[i], StringData(strings[i]));
    }

    std::vector<StringInterner*> interners;
    for (size_t i = 0; i < col_keys.size(); ++i) {
        interners.push_back(table->get_string_interner(col_keys[i]));
        // interners.back()->update_from_parent(false);
    }

    for (size_t i = 0; i < interners.size(); ++i) {
        const auto& db_string = obj.get_any(col_keys[i]).get_string();
        auto id = interners[i]->intern(db_string);
        const auto stored_id = interners[i]->lookup(StringData(strings[i]));
        CHECK(stored_id);
        CHECK(*stored_id == id);

        CHECK(interners[i]->compare(StringData(strings[i]), *stored_id) == 0); // should be equal
        const auto origin_string = interners[i]->get(id);
        CHECK(obj.get_any(col_keys[i]).get_string() == origin_string);

        CHECK(interners[i]->compare(*stored_id, id) == 0); // compare agaist self.
    }
}
