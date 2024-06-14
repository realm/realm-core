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


ONLY(StringInterner_Basic_Creation)
{
    Group group;
    TableRef table = group.add_table("test");
    auto string_col_key = table->add_column(type_String, "string");
    auto obj = table->create_object();
    std::string my_string = "aaaaaaaaaaaaaaa";
    obj.set(string_col_key, StringData(my_string));

    Array dummy_parent{obj.get_alloc()};
    dummy_parent.create(realm::NodeHeader::type_HasRefs);
    dummy_parent.add(0);
    StringInterner string_interner(obj.get_alloc(), dummy_parent, string_col_key, true);
    auto id = string_interner.intern(obj.get_any(string_col_key).get_string());

    const auto stored_id = string_interner.lookup(StringData(my_string));
    CHECK(stored_id);
    CHECK(*stored_id == id);

    // this fails ... the id retured is 1 but the size of the compressed strings is 1.
    // either the stored id should be 0 or we should start indexing from 1.
    CHECK(string_interner.compare(StringData(my_string), *stored_id) == 0); // should be equal
    const auto origin_string = string_interner.get(id);
    CHECK(obj.get_any(string_col_key).get_string() == origin_string);

    CHECK(string_interner.compare(*stored_id, id) == 0); // compare agaist self.
}
