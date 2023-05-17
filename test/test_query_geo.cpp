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

#include "s2/util/math/mathutil.h"
// conflicting defines of CHECK from s2
#undef CHECK

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

// From https://github.com/10gen/mongo/pull/11605
// Test which verifies that the rounding functions used by s2 follow 'round to even' rounding
// behavior.
TEST(S2VerifyS2RoundingBehavior)
{
    const double roundDownToEven = 2.5;
    CHECK_EQUAL(2, MathUtil::FastIntRound(roundDownToEven));
    CHECK_EQUAL(2LL, MathUtil::FastInt64Round(roundDownToEven));

    const double roundUpToEven = 3.5;
    CHECK_EQUAL(4, MathUtil::FastIntRound(roundUpToEven));
    CHECK_EQUAL(4LL, MathUtil::FastInt64Round(roundUpToEven));

    const double roundDownToEvenNegative = -3.5;
    CHECK_EQUAL(-4, MathUtil::FastIntRound(roundDownToEvenNegative));
    CHECK_EQUAL(-4LL, MathUtil::FastInt64Round(roundDownToEvenNegative));

    const double roundUpToEvenNegative = -2.5;
    CHECK_EQUAL(-2, MathUtil::FastIntRound(roundUpToEvenNegative));
    CHECK_EQUAL(-2LL, MathUtil::FastInt64Round(roundUpToEvenNegative));

    const double point = 944920918.5;
    CHECK_EQUAL(944920918, MathUtil::FastIntRound(point));
    CHECK_EQUAL(944920918LL, MathUtil::FastInt64Round(point));
}


static TableRef setup_with_points(Group& g, const std::vector<Geospatial>& points)
{
    auto location_table = g.add_table("Location", Table::Type::Embedded);
    location_table->add_column(type_String, "type");
    location_table->add_column_list(type_Double, "coordinates");
    auto table = g.add_table_with_primary_key("Restaurant", type_Int, "_id");
    auto location_column_key = table->add_column(*location_table, "location");
    for (size_t i = 0; i < points.size(); ++i) {
        table->create_object_with_primary_key(int64_t(i)).set(location_column_key, points[i]);
    }
    return table;
}

TEST(Geospatial_Assignment)
{
    Group g;
    Geospatial geo{GeoPoint{1.1, 2.2, 3.3}};
    auto table = setup_with_points(g, {geo});
    ColKey location_column_key = table->get_column_key("location");
    Obj obj = table->get_object_with_primary_key(0);

    Geospatial fetched = obj.get<Geospatial>(location_column_key);
    CHECK_EQUAL(fetched, geo);

    std::string_view err_expected = "Property '_id' must be a link to set a Geospatial value";
    ColKey id_column_key = table->get_column_key("_id");
    CHECK_THROW_CONTAINING_MESSAGE(obj.set(id_column_key, geo), err_expected);
    CHECK_THROW_CONTAINING_MESSAGE(obj.set(id_column_key, std::make_optional(geo)), err_expected);

    obj.set(location_column_key, std::optional<Geospatial>{});
    CHECK(obj.is_null(location_column_key));
    CHECK(obj.get<Geospatial>(location_column_key).get_type() == Geospatial::Type::Invalid);
    CHECK(!obj.get<std::optional<Geospatial>>(location_column_key));

    obj.set(location_column_key, geo);
    obj.set(location_column_key, Geospatial{});
    CHECK(obj.is_null(location_column_key));
    CHECK(obj.get<Geospatial>(location_column_key).get_type() == Geospatial::Type::Invalid);
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

TEST(Query_GeoWithinBasics)
{
    Group g;
    std::vector<Geospatial> data = {GeoPoint{-2, -1},   GeoPoint{-1, -2}, GeoPoint{0, 0},
                                    GeoPoint{0.5, 0.5}, GeoPoint{1, 1},   GeoPoint{2, 2}};
    TableRef table = setup_with_points(g, data);
    ColKey location_column_key = table->get_column_key("location");
    for (size_t i = 0; i < data.size(); ++i) {
        Obj obj = table->get_object_with_primary_key(int64_t(i));
        CHECK(obj);
        Geospatial geo = obj.get<Geospatial>(location_column_key);
        CHECK_EQUAL(geo, data[i]);
    }

    auto&& location = table->column<Link>(location_column_key);

    CHECK_EQUAL(location.geo_within(GeoBox{GeoPoint{0.2, 0.2}, GeoPoint{0.7, 0.7}}).count(), 1);
    CHECK_EQUAL(location.geo_within(GeoBox{GeoPoint{-2, -1.5}, GeoPoint{0.7, 0.5}}).count(), 3);

    GeoPolygon p{{GeoPoint{-0.5, -0.5}, GeoPoint{1.0, 2.5}, GeoPoint{2.5, -0.5}, GeoPoint{-0.5, -0.5}}};
    CHECK_EQUAL(location.geo_within(p).count(), 3);
    p = {{{-3.0, -1.0}, {-2.0, -2.0}, {-1.0, -1.0}, {1.5, -1.0}, {-1.0, 1.5}, {-3.0, -1.0}}};
    CHECK_EQUAL(location.geo_within(p).count(), 2);

    CHECK_EQUAL(location.geo_within(GeoCenterSphere::from_kms(150.0, GeoPoint{1.0, 0.5})).count(), 3);
    CHECK_EQUAL(location.geo_within(GeoCenterSphere::from_kms(90.0, GeoPoint{-1.5, -1.5})).count(), 2);

    CHECK_THROW_CONTAINING_MESSAGE(location.geo_within(Geospatial{GeoPoint{0.0, 0.0}}),
                                   "Invalid region in GEOWITHIN query for parameter 'GeoPoint([0, 0])': 'A point "
                                   "cannot be used on the right hand side of GEOWITHIN query");
    CHECK_THROW_CONTAINING_MESSAGE(location.geo_within(Geospatial{}),
                                   "Invalid region in GEOWITHIN query for parameter 'NULL': 'NULL cannot be used on "
                                   "the right hand side of a GEOWITHIN query");
}

TEST(Geospatial_MeridianQuery)
{
    // Check that geoWithin works across the meridian. We insert points
    // on the meridian, and immediately on either side, and confirm that a poly
    // covering all of them returns them all.
    Group g;
    std::vector<Geospatial> points = {GeoPoint{-179.0, 1.0}, GeoPoint{180.0, 1.0}, GeoPoint{179.0, 1.0}};
    TableRef table = setup_with_points(g, points);
    ColKey location_column_key = table->get_column_key("location");
    Geospatial meridianCrossingPoly{
        GeoPolygon{{{-178.0, 10.0}, {178.0, 10.0}, {178.0, -10.0}, {-178.0, -10.0}, {-178.0, 10.0}}}};
    size_t num_results = table->column<Link>(location_column_key).geo_within(meridianCrossingPoly).count();
    CHECK_EQUAL(num_results, 3);
}

TEST(Geospatial_EquatorQuery)
{
    // Test a poly that runs horizontally along the equator.
    Group g;
    std::vector<Geospatial> points = {GeoPoint{0.0, 0.0}, GeoPoint{-179.0, 1.0}, GeoPoint{180.0, 1.0},
                                      GeoPoint{179.0, 1.0}};
    TableRef table = setup_with_points(g, points);
    ColKey location_column_key = table->get_column_key("location");
    Geospatial horizontalPoly{GeoPolygon{{{30.0, 1.0}, {-30.0, 1.0}, {-30.0, -1.0}, {30.0, -1.0}, {30.0, 1.0}}}};
    size_t num_results = table->column<Link>(location_column_key).geo_within(horizontalPoly).count();
    CHECK_EQUAL(num_results, 1);
}

TEST(Geospatial_CenterSphere)
{
    Group g;
    std::vector<Geospatial> points = {GeoPoint{-118.2400013, 34.073893}, GeoPoint{-118.2400012, 34.073894},
                                      GeoPoint{0.0, 0.0}};
    TableRef table = setup_with_points(g, points);
    ColKey location_column_key = table->get_column_key("location");
    ColKey id_col = table->get_primary_key_column();
    Geospatial geo_sphere{GeoCenterSphere{0.44915760491198753, GeoPoint{-118.240013, 34.073893}}};

    Query query = table->column<Link>(location_column_key).geo_within(geo_sphere);
    CHECK_EQUAL(query.count(), 2);
    CHECK_EQUAL((query && table->column<Int>(id_col) == 0).count(), 1);
    CHECK_EQUAL((query && table->column<Int>(id_col) == 1).count(), 1);
    CHECK_EQUAL((query && table->column<Int>(id_col) == 3).count(), 0);
}

TEST(Geospatial_GeoWithinShapes)
{
    Group g;
    std::vector<Geospatial> points = {GeoPoint{0, 0.001}};
    TableRef table = setup_with_points(g, points);
    ColKey location_column_key = table->get_column_key("location");

    std::vector<Geospatial> shapes = {
        Geospatial{GeoCenterSphere{1, GeoPoint{0, 0}}},
        Geospatial{GeoBox{GeoPoint{-5, -5}, GeoPoint{5, 5}}},
        Geospatial{GeoPolygon{{{-5, -5}, {5, -5}, {5, 5}, {-5, 5}, {-5, -5}}}},
    };
    for (auto& shape : shapes) {
        Query query = table->column<Link>(location_column_key).geo_within(shape);

        CHECK_EQUAL(query.count(), 1);
        if (query.count() != 1) {
            util::format(std::cerr, "Failing query: '%1'\n", query.get_description());
        }
    }
}

TEST(Geospatial_PolygonValidation)
{
    Group g;
    std::vector<Geospatial> points = {GeoPoint{40.7128, -74.0060}, GeoPoint{55.6761, 12.5683},
                                      GeoPoint{55.6280, 12.0826}};
    TableRef table = setup_with_points(g, points);
    ColKey location_column_key = table->get_column_key("location");
    Geospatial geo_poly{GeoPolygon{{GeoPoint{40.7128, -74.006}, GeoPoint{55.6761, 12.5683}, GeoPoint{55.628, 12.0826},
                                    GeoPoint{40.7128, -74.006}}}};
    CHECK(geo_poly.is_valid().is_ok());
    Query query = table->column<Link>(location_column_key).geo_within(geo_poly);
    CHECK_EQUAL(query.count(), 1);

    // same as above because the normalized polygon inverts when covering more than a hemisphere
    Geospatial geo_poly_reversed{GeoPolygon{{GeoPoint{40.7128, -74.006}, GeoPoint{55.628, 12.0826},
                                             GeoPoint{55.6761, 12.5683}, GeoPoint{40.7128, -74.006}}}};
    CHECK(geo_poly_reversed.is_valid().is_ok());
    query = table->column<Link>(location_column_key).geo_within(geo_poly_reversed);
    CHECK_EQUAL(query.count(), 1);

    Geospatial poly_mismatch_loop{GeoPolygon{{GeoPoint{40.7128, -74.006}, GeoPoint{55.6761, 12.5683},
                                              GeoPoint{55.628, 12.0826}, GeoPoint{40.7128, -74.000}}}};
    Status status = poly_mismatch_loop.is_valid();
    CHECK(!status.is_ok());
    CHECK_EQUAL(status.reason(), "Ring is not closed, first vertex 'GeoPoint([40.7128, -74.006])' does not equal "
                                 "last vertex 'GeoPoint([40.7128, -74])'");

    Geospatial poly_three_point{
        GeoPolygon{{GeoPoint{40.7128, -74.006}, GeoPoint{55.6761, 12.5683}, GeoPoint{40.7128, -74.006}}}};
    status = poly_three_point.is_valid();
    CHECK(!status.is_ok());
    CHECK_EQUAL(status.reason(), "Ring 0 must have at least 3 different vertices, 2 unique vertices were provided");

    Geospatial loop_outside{
        GeoPolygon{{{GeoPoint{40.7128, -74.006}, GeoPoint{55.6761, 12.5683}, GeoPoint{55.628, 12.0826},
                     GeoPoint{40.7128, -74.006}},
                    {GeoPoint{39, -74.006}, GeoPoint{56, 12.5683}, GeoPoint{56, 12.0826}, GeoPoint{39, -74.006}}}}};
    status = loop_outside.is_valid();
    CHECK(!status.is_ok());
    CHECK_EQUAL(
        status.reason(),
        "Secondary ring 1 not contained by first exterior ring - secondary rings must be holes in the first ring");

    Geospatial touching_vertices{GeoPolygon{{{GeoPoint{40.7128, -74.006}, GeoPoint{55.6761, 12.5683},
                                              GeoPoint{55.628, 12.0826}, GeoPoint{40.7128, -74.006}},
                                             {GeoPoint{40.7128, -74.006}, GeoPoint{55.6761, 12.5683},
                                              GeoPoint{55.628, 12.0826}, GeoPoint{40.7128, -74.006}}}}};
    status = touching_vertices.is_valid();
    CHECK(!status.is_ok());
    CHECK_EQUAL(status.reason(), "Polygon isn't valid: 'Duplicate edge: ring 1, edge 0 and ring 0, edge 0'");

    Geospatial touching_interior_holes{
        GeoPolygon{{{GeoPoint{55.652263, 12.046461}, GeoPoint{55.621198, 12.051422}, GeoPoint{55.615860, 12.132292},
                     GeoPoint{55.658441, 12.115444}, GeoPoint{55.652263, 12.046461}},
                    {GeoPoint{55.629568, 12.098421}, GeoPoint{55.628449, 12.098661}, GeoPoint{55.628670, 12.100613},
                     GeoPoint{55.629670, 12.100283}, GeoPoint{55.629568, 12.098421}},
                    {// shares the same north edge as the previous hole
                     GeoPoint{55.629568, 12.098421}, GeoPoint{55.626245, 12.099442}, GeoPoint{55.626432, 12.100973},
                     GeoPoint{55.629670, 12.100283}, GeoPoint{55.629568, 12.098421}}}}};
    status = touching_interior_holes.is_valid();
    CHECK(!status.is_ok());
    CHECK_EQUAL(status.reason(), "Polygon isn't valid: 'Duplicate edge: ring 2, edge 3 and ring 1, edge 3'");

    Geospatial empty_poly{GeoPolygon{std::vector<std::vector<GeoPoint>>{}}};
    status = empty_poly.is_valid();
    CHECK(!status.is_ok());
    CHECK_EQUAL(status.reason(), "Polygon has no rings.");

    Geospatial poly_duplicates{
        GeoPolygon{{GeoPoint{0, 0}, GeoPoint{0, 1}, GeoPoint{0, 1}, GeoPoint{0, 1}, GeoPoint{1, 1}, GeoPoint{0, 0}}}};
    status = poly_duplicates.is_valid();
    CHECK(status.is_ok()); // adjacent duplicates are removed

    Geospatial poly_intersect{
        GeoPolygon{{GeoPoint{0, 0}, GeoPoint{0, 1}, GeoPoint{2, 1}, GeoPoint{2, 2}, GeoPoint{0, 0}}}};
    status = poly_intersect.is_valid();
    CHECK(!status.is_ok());
    CHECK_EQUAL(status.reason(),
                "Ring 0 is not valid: 'Edges 1 and 3 cross. Edge locations in degrees: [1.0000000, "
                "0.0000000]-[1.0000000, 2.0000000] and [2.0000000, 2.0000000]-[0.0000000, 0.0000000]'");

    // this appears to be a line, but because the points are mapped to a sphere, it is not
    Geospatial poly_line{GeoPolygon{{GeoPoint{0, 0}, GeoPoint{1, 1}, GeoPoint{2, 2}, GeoPoint{0, 0}}}};
    status = poly_line.is_valid();
    CHECK(status.is_ok());
}

#endif
