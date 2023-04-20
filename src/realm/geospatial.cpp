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

#ifdef _WIN32
#pragma warning(push)
#pragma warning(disable : 4068)
#pragma warning(disable : 4244)
#pragma warning(disable : 4267)
#else
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wunused-local-typedefs"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
#pragma GCC diagnostic ignored "-Wundefined-var-template"
#endif

#include <s2/s2cap.h>
#include <s2/s2latlng.h>
#include <s2/s2polygon.h>

#ifdef _WIN32
#pragma warning(pop)
#else
#pragma GCC diagnostic pop
#endif

#include <realm/list.hpp>
#include <realm/obj.hpp>
#include <realm/table.hpp>
#include <realm/table_ref.hpp>
#include <realm/util/overload.hpp>

namespace {

static bool type_is_valid(std::string str_type)
{
    std::transform(str_type.begin(), str_type.end(), str_type.begin(), realm::toLowerAscii);
    return str_type == "point";
}

} // anonymous namespace

namespace realm {

// src/mongo/db/geo/geoconstants.h
const double GeoCenterSphere::c_radius_meters = 6378100.0;

std::string Geospatial::get_type_string() const noexcept
{
    return is_valid() ? std::string(c_types[static_cast<size_t>(get_type())]) : "Invalid";
}

Geospatial::Type Geospatial::get_type() const noexcept
{
    return mpark::visit(util::overload{[&](const GeoPoint&) {
                                           return Type::Point;
                                       },
                                       [&](const GeoBox&) {
                                           return Type::Box;
                                       },
                                       [&](const GeoPolygon&) {
                                           return Type::Polygon;
                                       },
                                       [&](const GeoCenterSphere&) {
                                           return Type::CenterSphere;
                                       },
                                       [&](const mpark::monostate&) {
                                           return Type::Invalid;
                                       }},
                        m_value);
}

bool Geospatial::is_geospatial(const TableRef table, ColKey link_col)
{
    if (!table || !link_col) {
        return false;
    }
    if (!table->is_link_type(link_col.get_type())) {
        return false;
    }
    TableRef target = table->get_link_target(link_col);
    if (!target || !target->is_embedded()) {
        return false;
    }
    ColKey type_col = target->get_column_key(StringData(c_geo_point_type_col_name));
    if (!type_col || type_col.is_collection() || type_col.get_type() != col_type_String) {
        return false;
    }
    ColKey coords_col = target->get_column_key(StringData(c_geo_point_coords_col_name));
    if (!coords_col || !coords_col.is_list() || coords_col.get_type() != col_type_Double) {
        return false;
    }
    return true;
}

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
    if (coords.size() < 2) {
        return Geospatial(); // invalid
    }
    if (coords.size() > 2) {
        return GeoPoint{coords[0], coords[1], coords[2]};
    }
    return GeoPoint{coords[0], coords[1]};
}

Geospatial Geospatial::from_link(const Obj& link)
{
    if (!link) {
        return Geospatial{};
    }
    ColKey type_col = link.get_table()->get_column_key(StringData(c_geo_point_type_col_name));
    ColKey coords_col = link.get_table()->get_column_key(StringData(c_geo_point_coords_col_name));
    if (!type_col || !coords_col) {
        return Geospatial{};
    }
    if (!type_is_valid(link.get<String>(type_col))) {
        return Geospatial();
    }
    Lst<double> geo_data = link.get_list<double>(coords_col);
    const size_t num_entries = geo_data.size();
    if (num_entries < 2) {
        return Geospatial(); // invalid
    }
    if (num_entries > 2) {
        return GeoPoint{geo_data[0], geo_data[1], geo_data[2]};
    }
    return GeoPoint{geo_data[0], geo_data[1]};
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
    Geospatial::Type type = get_type();
    if (type == Type::Invalid) {
        link.remove();
        return;
    }
    if (type != Type::Point) {
        throw IllegalOperation("The only Geospatial type currently supported for storage is 'point'");
    }
    GeoPoint point = get<GeoPoint>();
    link.set(type_col, get_type_string());
    Lst<double> coords = link.get_list<double>(coords_col);
    if (coords.size() >= 1) {
        coords.set(0, point.longitude);
    }
    else {
        coords.add(point.longitude);
    }
    if (coords.size() >= 2) {
        coords.set(1, point.latitude);
    }
    else {
        coords.add(point.latitude);
    }
    std::optional<double> altitude = point.get_altitude();
    if (altitude) {
        if (coords.size() >= 3) {
            coords.set(2, *altitude);
        }
        else {
            coords.add(*altitude);
        }
    }
    else if (coords.size() >= 3) {
        coords.remove(2, coords.size());
    }
}

S2Region& Geospatial::get_region() const
{
    if (m_region)
        return *m_region.get();

    mpark::visit(util::overload{
                     [&](const GeoPoint&) {
                         REALM_UNREACHABLE();
                     },
                     [&](const GeoBox& box) {
                         m_region =
                             std::make_unique<S2LatLngRect>(S2LatLng::FromDegrees(box.lo.latitude, box.lo.longitude),
                                                            S2LatLng::FromDegrees(box.hi.latitude, box.hi.longitude));
                     },
                     [&](const GeoPolygon& polygon) {
                         REALM_ASSERT(polygon.points.size() >= 1);
                         // FIXME: should be really S2Polygon
                         std::vector<S2Point> points;
                         points.reserve(polygon.points[0].size());
                         for (auto&& p : polygon.points[0])
                             // FIXME rewrite without copying
                             points.emplace_back(S2LatLng::FromDegrees(p.latitude, p.longitude).ToPoint());
                         m_region = std::make_unique<S2Loop>(points);
                     },
                     [&](const GeoCenterSphere& sphere) {
                         auto center =
                             S2LatLng::FromDegrees(sphere.center.latitude, sphere.center.longitude).ToPoint();
                         auto radius = S1Angle::Radians(sphere.radius_radians);
                         m_region.reset(S2Cap::FromAxisAngle(center, radius).Clone()); // FIXME without extra copy
                     },
                     [&](const mpark::monostate&) {
                         REALM_UNREACHABLE();
                     }},
                 m_value);

    return *m_region.get();
}

bool Geospatial::is_within(const Geospatial& geometry) const noexcept
{
    REALM_ASSERT(get_type() == Geospatial::Type::Point);

    GeoPoint geo_point = mpark::get<GeoPoint>(m_value);
    auto point = S2LatLng::FromDegrees(geo_point.latitude, geo_point.longitude).ToPoint();

    auto& region = geometry.get_region();
    return mpark::visit(util::overload{[&](const GeoPoint&) {
                                           return false;
                                       },
                                       [&](const GeoBox&) {
                                           return static_cast<S2LatLngRect&>(region).Contains(point);
                                       },
                                       [&](const GeoPolygon&) {
                                           return static_cast<S2Loop&>(region).Contains(point);
                                       },
                                       [&](const GeoCenterSphere&) {
                                           return static_cast<S2Cap&>(region).Contains(point);
                                       },
                                       [&](const mpark::monostate&) {
                                           return false;
                                       }},
                        geometry.m_value);
}

std::string Geospatial::to_string() const
{
    auto point_str = [&](const GeoPoint& point) -> std::string {
        if (point.get_altitude()) {
            return util::format("[%1, %2, %3]", point.longitude, point.latitude, *point.get_altitude());
        }
        return util::format("[%1, %2]", point.longitude, point.latitude);
    };

    return mpark::visit(
        util::overload{[&](const GeoPoint& point) {
                           return util::format("GeoPoint(%1)", point_str(point));
                       },
                       [&](const GeoBox& box) {
                           return util::format("GeoBox(%1, %2)", point_str(box.lo), point_str(box.hi));
                       },
                       [&](const GeoPolygon& poly) {
                           std::string points = "";
                           for (size_t i = 0; i < poly.points.size(); ++i) {
                               if (i != 0) {
                                   points += ", ";
                               }
                               points += "{";
                               for (size_t j = 0; j < poly.points[i].size(); ++j) {
                                   points += util::format("%1%2", j == 0 ? "" : ", ", point_str(poly.points[i][j]));
                               }
                               points += "}";
                           }
                           return util::format("GeoPolygon(%1)", points);
                       },
                       [&](const GeoCenterSphere& sphere) {
                           return util::format("GeoSphere(%1, %2)", point_str(sphere.center), sphere.radius_radians);
                       },
                       [&](const mpark::monostate&) {
                           return std::string("NULL");
                       }},
        m_value);
    return "NULL";
}

std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo)
{
    ostr << geo.to_string();
    return ostr;
}

} // namespace realm
