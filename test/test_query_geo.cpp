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

TEST(Geospatial_Assignment)
{
    Group g;
    auto location_table = g.add_table("Location", Table::Type::Embedded);
    location_table->add_column(type_String, "type");
    location_table->add_column_list(type_Double, "coordinates");
    auto table = g.add_table("Restaurant");
    auto id_column_key = table->add_column(type_UUID, "_id");
    auto location_column_key = table->add_column(*location_table, "location");

    Geospatial geo{GeoPoint{1.1, 2.2, 3.3}};
    Obj obj = table->create_object().set(location_column_key, geo);
    Geospatial fetched = obj.get<Geospatial>(location_column_key);
    CHECK_EQUAL(fetched, geo);

    std::string_view err_expected = "Property '_id' must be a link to set a Geospatial value";
    CHECK_THROW_CONTAINING_MESSAGE(obj.set(id_column_key, geo), err_expected);
    CHECK_THROW_CONTAINING_MESSAGE(obj.set(id_column_key, std::make_optional(geo)), err_expected);

    obj.set(location_column_key, std::optional<Geospatial>{});
    CHECK(obj.is_null(location_column_key));
    CHECK(!obj.get<Geospatial>(location_column_key).is_valid());
    CHECK(!obj.get<std::optional<Geospatial>>(location_column_key));

    obj.set(location_column_key, geo);
    obj.set(location_column_key, Geospatial{});
    CHECK(obj.is_null(location_column_key));
    CHECK(!obj.get<Geospatial>(location_column_key).is_valid());
    CHECK(!obj.get<std::optional<Geospatial>>(location_column_key));

    Geospatial geo_without_altitude{GeoPoint{5.5, 6.6}};
    obj.set(location_column_key, geo);
    obj.set(location_column_key, geo_without_altitude);
    CHECK_EQUAL(obj.get<Geospatial>(location_column_key), geo_without_altitude);

    Geospatial geo_box(GeoBox{GeoPoint{1.1, 2.2}, GeoPoint{3.3, 4.4}});
    std::string_view err_msg = "The only Geospatial type currently supported for storage is 'point'";
    CHECK_THROW_CONTAINING_MESSAGE(obj.set(location_column_key, geo_box), err_msg);
    Geospatial geo_sphere(GeoCenterSphere{10, GeoPoint{1.1, 2.2}});
    CHECK_THROW_CONTAINING_MESSAGE(obj.set(location_column_key, geo_sphere), err_msg);
}

TEST(Query_GeoWithin)
{
    Group g;

    auto location_table = g.add_table("Location", Table::Type::Embedded);
    location_table->add_column(type_String, "type");
    location_table->add_column_list(type_Double, "coordinates");

    auto table = g.add_table("Restaurant");
    table->add_column(type_UUID, "_id");
    auto location_column_key = table->add_column(*location_table, "location");

    std::vector<Geospatial> data = {GeoPoint{-2, -1},   GeoPoint{-1, -2}, GeoPoint{0, 0},
                                    GeoPoint{0.5, 0.5}, GeoPoint{1, 1},   GeoPoint{2, 2}};
    std::vector<Obj> objs;
    for (auto& p : data) {
        objs.push_back(table->create_object().set(location_column_key, p));
    }

    for (size_t i = 0; i < data.size(); ++i) {
        Geospatial geo = objs[i].get<Geospatial>(location_column_key);
        CHECK_EQUAL(geo, data[i]);
    }

    auto&& location = table->column<Link>(location_column_key);

    CHECK_EQUAL(location.geo_within(GeoBox{GeoPoint{0.2, 0.2}, GeoPoint{0.7, 0.7}}).count(), 1);
    CHECK_EQUAL(location.geo_within(GeoBox{GeoPoint{-2, -1.5}, GeoPoint{0.7, 0.5}}).count(), 3);

    GeoPolygon p{GeoPoint{-0.5, -0.5}, GeoPoint{1.0, 2.5}, GeoPoint{2.5, -0.5}};
    CHECK_EQUAL(location.geo_within(p).count(), 3);
    p = {GeoPoint{-3.0, -1.0}, GeoPoint{-2.0, -2.0}, GeoPoint{-1.0, -1.0}, GeoPoint{1.5, -1.0}, GeoPoint{-1.0, 1.5}};
    CHECK_EQUAL(location.geo_within(p).count(), 2);

    CHECK_EQUAL(location.geo_within(GeoCenterSphere{150.0, GeoPoint{1.0, 0.5}}).count(), 3);
    CHECK_EQUAL(location.geo_within(GeoCenterSphere{90.0, GeoPoint{-1.5, -1.5}}).count(), 2);
}

#endif
