/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#include <realm/geospatial.hpp>

#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-local-typedef"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
#pragma GCC diagnostic ignored "-Wundefined-var-template"
#endif

#include "s2/s2cap.h"
#include "s2/s2latlng.h"
#include "s2/s2polygon.h"

#ifndef _WIN32
#pragma GCC diagnostic pop
#endif

#include <realm/list.hpp>
#include <realm/obj.hpp>
#include <realm/table.hpp>

namespace {

static bool type_is_valid(std::string str_type)
{
    std::transform(str_type.begin(), str_type.end(), str_type.begin(), realm::toLowerAscii);
    return str_type == "point";
}

} // anonymous namespace

namespace realm {

const double Geospatial::c_radius_km = 6371.01;

Geospatial Geospatial::from_obj(const Obj& obj, ColKey type_col, ColKey coords_col)
{
    if (!type_col) {
        type_col = obj.get_table()->get_column_key(StringData(c_geo_point_type_col_name));
    }
    else {
        REALM_ASSERT_EX(type_col.get_type() == ColumnType(ColumnType::Type::String), type_col.get_type());
        REALM_ASSERT(!type_col.is_collection());
    }
    if (!coords_col) {
        coords_col = obj.get_table()->get_column_key(StringData(c_geo_point_coords_col_name));
    }
    else {
        REALM_ASSERT_EX(type_col.get_type() == ColumnType(ColumnType::Type::Double), type_col.get_type());
        REALM_ASSERT(type_col.is_list());
    }

    String geo_type = obj.get<String>(type_col);
    if (!type_is_valid(geo_type)) {
        throw IllegalOperation("The only Geospatial type currently supported is 'point'");
    }

    Lst<double> coords = obj.get_list<double>(coords_col);
    GeoPoint geo;
    if (coords.size() >= 1) {
        geo.longitude = coords[0];
    }
    if (coords.size() >= 2) {
        geo.latitude = coords[1];
    }
    if (coords.size() >= 3) {
        geo.altitude = coords[2];
    }
    return geo;
}

Geospatial Geospatial::from_link(const Obj& link)
{
    GeoPoint point;
    if (!link) {
        return Geospatial{point};
    }
    ColKey type_col = link.get_table()->get_column_key(StringData(c_geo_point_type_col_name));
    ColKey coords_col = link.get_table()->get_column_key(StringData(c_geo_point_coords_col_name));
    if (!type_col || !coords_col) {
        return Geospatial{point};
    }
    if (!type_is_valid(link.get<String>(type_col))) {
        return Geospatial(link.get<String>(type_col));
    }
    Lst<double> geo_data = link.get_list<double>(coords_col);
    const size_t num_entries = geo_data.size();
    if (num_entries >= 2) {
        point.longitude = geo_data.get(0);
        point.latitude = geo_data.get(1);
    }
    if (num_entries >= 3) {
        point.altitude = geo_data.get(2);
    }
    return Geospatial{point};
}

void Geospatial::assign_to(Obj& link) const
{
    REALM_ASSERT(link);
    ColKey type_col = link.get_table()->get_column_key(Geospatial::c_geo_point_type_col_name);
    if (!type_col) {
        throw InvalidArgument(ErrorCodes::TypeMismatch,
                              util::format("Property %1 doesn't exist", c_geo_point_type_col_name));
    }
    ColKey coords_col = link.get_table()->get_column_key(Geospatial::c_geo_point_coords_col_name);
    if (!coords_col) {
        throw InvalidArgument(ErrorCodes::TypeMismatch,
                              util::format("Property %1 doesn't exist", c_geo_point_coords_col_name));
    }
    if (!type_is_valid(get_type())) {
        throw IllegalOperation("The only Geospatial type currently supported is 'point'");
    }
    if (m_points.size() > 1) {
        throw IllegalOperation("Only one Geospatial point is currently supported");
    }
    if (m_points.size() == 0) {
        throw InvalidArgument("Geospatial value must have one point");
    }
    link.set(type_col, get_type());
    Lst<double> coords = link.get_list<double>(coords_col);
    if (coords.size() >= 1) {
        coords.set(0, m_points[0].longitude);
    }
    else {
        coords.add(m_points[0].longitude);
    }
    if (coords.size() >= 2) {
        coords.set(1, m_points[0].latitude);
    }
    else {
        coords.add(m_points[0].latitude);
    }
    if (m_points[0].altitude) {
        if (coords.size() >= 3) {
            coords.set(2, *m_points[0].altitude);
        }
        else {
            coords.add(*m_points[0].altitude);
        }
    }
}

S2Region& Geospatial::get_region() const
{
    if (m_region)
        return *m_region.get();

    switch (m_type) {
        // FIXME 'box' assumes legacy flat plane, should be removed?
        case Type::Box: {
            REALM_ASSERT(m_points.size() == 2);
            auto &&lo = m_points[0], &&hi = m_points[1];
            m_region = std::make_unique<S2LatLngRect>(S2LatLng::FromDegrees(lo.latitude, lo.longitude),
                                                      S2LatLng::FromDegrees(hi.latitude, hi.longitude));
        } break;
        case Type::Polygon: {
            REALM_ASSERT(m_points.size() >= 3);
            // should be really S2Polygon, but it's really needed only for MultyPolygon
            std::vector<S2Point> points;
            points.reserve(m_points.size());
            for (auto&& p : m_points)
                // FIXME rewrite without copying
                points.emplace_back(S2LatLng::FromDegrees(p.latitude, p.longitude).ToPoint());
            m_region = std::make_unique<S2Loop>(points);
        } break;
        case Type::CenterSphere: {
            REALM_ASSERT(m_points.size() == 1);
            REALM_ASSERT(m_radius_radians && m_radius_radians > 0.0);
            auto&& p = m_points.front();
            auto center = S2LatLng::FromDegrees(p.latitude, p.longitude).ToPoint();
            auto radius = S1Angle::Radians(m_radius_radians.value());
            m_region.reset(S2Cap::FromAxisAngle(center, radius).Clone()); // FIXME without extra copy
        } break;
        default:
            REALM_UNREACHABLE();
    }

    return *m_region.get();
}

bool Geospatial::is_within(const Geospatial& geometry) const noexcept
{
    REALM_ASSERT(m_points.size() == 1); // point

    auto point = S2LatLng::FromDegrees(m_points[0].latitude, m_points[0].longitude).ToPoint();

    auto& region = geometry.get_region();
    switch (geometry.m_type) {
        case Type::Box:
            return static_cast<S2LatLngRect&>(region).Contains(point);
        case Type::Polygon:
            return static_cast<S2Loop&>(region).Contains(point);
        case Type::CenterSphere:
            return static_cast<S2Cap&>(region).Contains(point);
        default:
            break;
    }

    REALM_UNREACHABLE(); // FIXME: other types and error handling
}

std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo)
{
    ostr << util::serializer::print_value(geo);
    return ostr;
}

} // namespace realm
