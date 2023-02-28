/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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
#ifdef TEST_GEO

#include "test.hpp"

#include <realm/geospatial.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/query_expression.hpp>

#include <ostream>
#include <sstream>

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

TEST(Query_GeoWithin)
{
    Group g;
    auto table = g.add_table("Restuarnts");
    auto geo_table = g.add_table("MyPosition", Table::Type::Embedded);
    ColKey type_col = geo_table->add_column(type_String, "type");
    ColKey coords_col = geo_table->add_column_list(type_Double, "coordinates");
    auto col_int = table->add_column(type_UUID, "_id");
    auto col_any = table->add_column(type_Mixed, "mixed");
    auto col_link = table->add_column(*geo_table, "link");
    auto col_links = table->add_column_list(*geo_table, "links");

    auto add_data = [&](Geospatial geo) {
        Obj top_obj = table->create_object();
        Obj geo_obj = top_obj.create_and_set_linked_object(col_link);
        geo.assign_to(geo_obj);
    };
    std::vector<Geospatial> point_data = {GeoPoint{0, 0}, GeoPoint{0.5, 0.5}, GeoPoint{1, 1}, GeoPoint{2, 2}};
    for (auto& geo : point_data) {
        add_data(geo);
    }

    GeoBox box{GeoPoint{0.2, 0.2}, GeoPoint{0.7, 0.7}};

    CHECK_EQUAL(table->column<Link>(col_link).geo_within(box).count(), 1);
}

#endif
