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

#include <realm/list.hpp>
#include <realm/obj.hpp>
#include <realm/table.hpp>

namespace {

static bool type_is_valid(std::string str_type)
{
    std::transform(str_type.begin(), str_type.end(), str_type.begin(), realm::toLowerAscii);
    return str_type == "point";
}

static bool type_is(std::string str_type, const char* lower)
{
    std::transform(str_type.begin(), str_type.end(), str_type.begin(), realm::toLowerAscii);
    return str_type == lower;
}


} // anonymous namespace

namespace realm {

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
    REALM_ASSERT_EX(type_is_valid(geo_type), geo_type); // FIXME: better error handling
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
    ColKey coords_col = link.get_table()->get_column_key(Geospatial::c_geo_point_coords_col_name);
    if (!type_col || !coords_col) {
        throw LogicError(LogicError::illegal_type);
    }
    if (!type_is_valid(get_type())) {
        throw std::runtime_error("The only Geospatial type currently supported is 'point'");
    }
    if (m_points.size() > 1) {
        throw std::runtime_error("Only one Geospatial point is currently supported");
    }
    if (m_points.size() == 0) {
        throw std::runtime_error("Geospatial value must have one point");
    }
    link.set(type_col, m_type);
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

bool Geospatial::is_within(const Geospatial& bounds) const noexcept
{
    REALM_ASSERT(m_points.size() == 1); // point

    if (type_is(bounds.get_type(), "box") && bounds.m_points.size() == 2) {
        return m_points[0].latitude >= bounds.m_points[0].latitude &&
               m_points[0].latitude <= bounds.m_points[1].latitude &&
               m_points[0].longitude >= bounds.m_points[0].longitude &&
               m_points[0].longitude <= bounds.m_points[1].longitude;
    }
    else {
        REALM_UNREACHABLE(); // FIXME: other types and error handling
    }
}

std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo)
{
    ostr << util::serializer::print_value(geo);
    return ostr;
}

} // namespace realm
