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

#include "testsettings.hpp"

#include <realm.hpp>
#include <realm/array_typed_link.hpp>
#include <realm/array_mixed.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

TEST(TypedLinks_Single)
{
    Group g;
    auto dog = g.add_table("dog");
    auto cat = g.add_table("cat");
    auto person = g.add_table("person");
    auto col_pet = person->add_column(type_TypedLink, "pet");

    auto pluto = dog->create_object();
    auto garfield = cat->create_object();
    auto paul = person->create_object(ObjKey{}, {{col_pet, ObjLink{dog->get_key(), pluto.get_key()}}});

    CHECK_EQUAL(pluto.get_backlink_count(), 1);
    CHECK_EQUAL(garfield.get_backlink_count(), 0);
    auto bl = pluto.get_backlink(*person, col_pet, 0);
    CHECK_EQUAL(bl, paul.get_key());

    auto john = person->create_object();
    ObjLink link{cat->get_key(), garfield.get_key()};
    john.set(col_pet, link);
    CHECK_EQUAL(pluto.get_backlink_count(), 1);
    CHECK_EQUAL(garfield.get_backlink_count(), 1);
    bl = garfield.get_backlink(*person, col_pet, 0);
    CHECK_EQUAL(bl, john.get_key());

    paul.remove();
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
    garfield.remove();
    CHECK_NOT(john.get<ObjLink>(col_pet));
}

TEST(TypedLinks_List)
{
    Group g;
    auto dog = g.add_table("dog");
    auto cat = g.add_table("cat");
    auto person = g.add_table("person");
    auto col_pet = person->add_column_list(type_TypedLink, "pets");

    auto pluto = dog->create_object();
    auto garfield = cat->create_object();
    auto paul = person->create_object();

    auto list = paul.get_list<ObjLink>(col_pet);
    list.add({dog->get_key(), pluto.get_key()});

    if (CHECK_EQUAL(pluto.get_backlink_count(), 1)) {
        auto bl = pluto.get_backlink(*person, col_pet, 0);
        CHECK_EQUAL(bl, paul.get_key());
    }
    CHECK_EQUAL(garfield.get_backlink_count(), 0);

    list.set(0, {cat->get_key(), garfield.get_key()});
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
    if (CHECK_EQUAL(garfield.get_backlink_count(), 1)) {
        auto bl = garfield.get_backlink(*person, col_pet, 0);
        CHECK_EQUAL(bl, paul.get_key());
    }

    list.remove(0);
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
    CHECK_EQUAL(garfield.get_backlink_count(), 0);

    list.add({dog->get_key(), pluto.get_key()});
    list.add({cat->get_key(), garfield.get_key()});
    CHECK_EQUAL(pluto.get_backlink_count(), 1);
    CHECK_EQUAL(garfield.get_backlink_count(), 1);
    garfield.remove();
    CHECK_EQUAL(list.size(), 1);
    paul.remove();
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
}

TEST(TypedLinks_Mixed)
{
    Group g;
    auto dog = g.add_table("dog");
    auto cat = g.add_table("cat");
    auto person = g.add_table("person");
    auto col_pet = person->add_column(type_Mixed, "pet");

    auto pluto = dog->create_object();
    auto garfield = cat->create_object();
    auto paul = person->create_object();
    paul.set(col_pet, Mixed(ObjLink{dog->get_key(), pluto.get_key()}));

    CHECK_EQUAL(pluto.get_backlink_count(), 1);
    CHECK_EQUAL(garfield.get_backlink_count(), 0);
    auto bl = pluto.get_backlink(*person, col_pet, 0);
    CHECK_EQUAL(bl, paul.get_key());

    auto john = person->create_object();
    ObjLink link{cat->get_key(), garfield.get_key()};
    john.set(col_pet, Mixed(link));
    CHECK_EQUAL(pluto.get_backlink_count(), 1);
    CHECK_EQUAL(garfield.get_backlink_count(), 1);
    bl = garfield.get_backlink(*person, col_pet, 0);
    CHECK_EQUAL(bl, john.get_key());

    paul.remove();
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
    garfield.remove();
    CHECK(john.get<Mixed>(col_pet).is_null());
}

TEST(TypedLinks_MixedList)
{
    Group g;
    auto dog = g.add_table("dog");
    auto cat = g.add_table("cat");
    auto person = g.add_table("person");
    auto col_pet = person->add_column_list(type_Mixed, "pets");

    auto pluto = dog->create_object();
    auto garfield = cat->create_object();
    auto paul = person->create_object();

    auto list = paul.get_list<Mixed>(col_pet);
    list.add(ObjLink{dog->get_key(), pluto.get_key()});

    if (CHECK_EQUAL(pluto.get_backlink_count(), 1)) {
        auto bl = pluto.get_backlink(*person, col_pet, 0);
        CHECK_EQUAL(bl, paul.get_key());
    }
    CHECK_EQUAL(garfield.get_backlink_count(), 0);

    list.set(0, ObjLink{cat->get_key(), garfield.get_key()});
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
    if (CHECK_EQUAL(garfield.get_backlink_count(), 1)) {
        auto bl = garfield.get_backlink(*person, col_pet, 0);
        CHECK_EQUAL(bl, paul.get_key());
    }

    list.remove(0);
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
    CHECK_EQUAL(garfield.get_backlink_count(), 0);

    list.add(ObjLink{dog->get_key(), pluto.get_key()});
    list.add(ObjLink{cat->get_key(), garfield.get_key()});
    CHECK_EQUAL(pluto.get_backlink_count(), 1);
    CHECK_EQUAL(garfield.get_backlink_count(), 1);
    garfield.remove();
    CHECK_EQUAL(list.size(), 1);
    paul.remove();
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
}

TEST(TypedLinks_Clear)
{
    Group g;
    auto dog = g.add_table("dog");
    auto cat = g.add_table("cat");
    auto person = g.add_table("person");
    auto col_typed = person->add_column(type_TypedLink, "typed");
    auto col_list_typed = person->add_column_list(type_TypedLink, "typed_list");
    auto col_mixed = person->add_column(type_Mixed, "mixed");
    auto col_list_mixed = person->add_column_list(type_Mixed, "mixed_list");

    auto pluto = dog->create_object();
    auto garfield = cat->create_object();
    auto paul = person->create_object();

    paul.set(col_typed, ObjLink{dog->get_key(), pluto.get_key()});
    paul.get_list<ObjLink>(col_list_typed).add({dog->get_key(), pluto.get_key()});
    paul.set(col_mixed, Mixed(ObjLink{dog->get_key(), pluto.get_key()}));
    paul.get_list<Mixed>(col_list_mixed).add(ObjLink{dog->get_key(), pluto.get_key()});

    person->clear();
    g.verify();
}

TEST(TypedLinks_CollectionClear)
{
    Group g;
    auto dog = g.add_table("dog");
    auto person = g.add_table("person");
    auto col_list_mixed = person->add_column_list(type_Mixed, "mixed_list");
    auto col_set_mixed = person->add_column_set(type_Mixed, "mixed_set");

    auto pluto = dog->create_object();
    auto paul = person->create_object();

    auto list = paul.get_list<Mixed>(col_list_mixed);
    auto set = paul.get_set<Mixed>(col_set_mixed);
    list.add(pluto);
    set.insert(pluto);
    CHECK_EQUAL(pluto.get_backlink_count(), 2);
    list.clear();
    set.clear();
    CHECK_EQUAL(pluto.get_backlink_count(), 0);

    pluto.remove();

    g.verify();
}
