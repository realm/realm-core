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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"
#ifdef TEST_LINKS

#include <realm.hpp>
#include <realm/util/file.hpp>
#include <realm/array_key.hpp>
#include <realm/history.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

TEST(Links_UnresolvedBasic)
{
    ObjKey k;

    CHECK_NOT(k);
    CHECK_NOT(k.get_unresolved());

    Group g;

    auto cars = g.add_table_with_primary_key("Car", type_String, "model");
    auto col_price = cars->add_column(type_Decimal, "price");
    auto persons = g.add_table_with_primary_key("Person", type_String, "e-mail");
    auto col_owns = persons->add_column_link(type_Link, "car", *cars);
    auto dealers = g.add_table_with_primary_key("Dealer", type_Int, "cvr");
    auto col_has = dealers->add_column_link(type_LinkList, "stock", *cars);

    auto finn = persons->create_object_with_primary_key("finn.schiermer-andersen@mongodb.com");
    auto mathias = persons->create_object_with_primary_key("mathias@10gen.com");
    auto joergen = dealers->create_object_with_primary_key(18454033);
    auto stock = joergen.get_linklist(col_has);

    auto skoda = cars->create_object_with_primary_key("Skoda Fabia").set(col_price, Decimal128("149999.5"));

    auto new_tesla = cars->get_objkey_from_primary_key("Tesla 10");
    CHECK(new_tesla.is_unresolved());
    finn.set(col_owns, new_tesla);
    mathias.set(col_owns, new_tesla);

    auto another_tesla = cars->get_objkey_from_primary_key("Tesla 10");
    stock.add(another_tesla);
    stock.add(skoda.get_key());

    CHECK_NOT(finn.get<ObjKey>(col_owns));
    CHECK_EQUAL(stock.size(), 1);
    CHECK_EQUAL(stock.get(0), skoda.get_key());
    CHECK_EQUAL(cars->size(), 1);
    auto q = cars->column<Decimal128>(col_price) < Decimal128("300000");
    CHECK_EQUAL(q.count(), 1);

    // cars->dump_objects();
    cars->create_object_with_primary_key("Tesla 10").set(col_price, Decimal128("499999.5"));
    CHECK_EQUAL(stock.size(), 2);
    CHECK_EQUAL(cars->size(), 2);
    CHECK(finn.get<ObjKey>(col_owns));

    // cars->dump_objects();
    cars->invalidate_object(new_tesla);
    CHECK_EQUAL(stock.size(), 1);
    CHECK_EQUAL(stock.get(0), skoda.get_key());
    CHECK_EQUAL(cars->size(), 1);
}

#endif
