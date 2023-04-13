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

    auto location_table = g.add_table("Location", Table::Type::Embedded);
    location_table->add_column(type_String, "type");
    location_table->add_column_list(type_Double, "coordinates");

    auto table = g.add_table("Restaurant");
    table->add_column(type_UUID, "_id");
    auto location_column_key = table->add_column(*location_table, "location");

    auto add_data = [&](const Geospatial& point) {
        Obj restaurant = table->create_object();
        Obj location = restaurant.create_and_set_linked_object(location_column_key);
        point.assign_to(location);
    };

    std::vector<Geospatial> data = {GeoPoint{-2, -1},   GeoPoint{-1, -2}, GeoPoint{0, 0},
                                    GeoPoint{0.5, 0.5}, GeoPoint{1, 1},   GeoPoint{2, 2}};
    for (auto& p : data) {
        add_data(p);
    }

    auto&& location = table->column<Link>(location_column_key);

    CHECK_EQUAL(location.geo_within(GeoBox{GeoPoint{0.2, 0.2}, GeoPoint{0.7, 0.7}}).count(), 1);
    CHECK_EQUAL(location.geo_within(GeoBox{GeoPoint{-2, -1.5}, GeoPoint{0.7, 0.5}}).count(), 3);

    GeoPolygon p{GeoPoint{-0.5, -0.5}, GeoPoint{1.0, 2.5}, GeoPoint{2.5, -0.5}};
    CHECK_EQUAL(location.geo_within(p).count(), 3);
    p = {GeoPoint{-3.0, -1.0}, GeoPoint{-2.0, -2.0}, GeoPoint{-1.0, -1.0}, GeoPoint{1.5, -1.0}, GeoPoint{-1.0, 1.5}};
    CHECK_EQUAL(location.geo_within(p).count(), 2);

    CHECK_EQUAL(location.geo_within(GeoCenterSphere::from_kms(150.0, GeoPoint{1.0, 0.5})).count(), 3);
    CHECK_EQUAL(location.geo_within(GeoCenterSphere::from_kms(90.0, GeoPoint{-1.5, -1.5})).count(), 2);
}

#endif
