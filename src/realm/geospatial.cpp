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

std::string Geospatial::get_type_string() const noexcept
{
    switch (get_type()) {
        case Type::Point:
            return "Point";
        case Type::Box:
            return "Box";
        case Type::Polygon:
            return "Polygon";
        case Type::CenterSphere:
            return "CenterSphere";
        case Type::Invalid:
            return "Invalid";
    }
    REALM_UNREACHABLE();
}

Geospatial::Type Geospatial::get_type() const noexcept
{
    return static_cast<Type>(m_value.index());
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
    auto&& point = get<GeoPoint>();
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

static std::string point_str(const GeoPoint& point)
{
    if (point.has_altitude()) {
        return util::format("[%1, %2, %3]", point.longitude, point.latitude, point.altitude);
    }
    return util::format("[%1, %2]", point.longitude, point.latitude);
}

std::string Geospatial::to_string() const
{
    return mpark::visit(
        util::overload{[](const GeoPoint& point) {
                           return util::format("GeoPoint(%1)", point_str(point));
                       },
                       [](const GeoBox& box) {
                           return util::format("GeoBox(%1, %2)", point_str(box.lo), point_str(box.hi));
                       },
                       [](const GeoPolygon& poly) {
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
                       [](const GeoCenterSphere& sphere) {
                           return util::format("GeoSphere(%1, %2)", point_str(sphere.center), sphere.radius_radians);
                       },
                       [](const mpark::monostate&) {
                           return std::string("NULL");
                       }},
        m_value);
}

std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo)
{
    ostr << geo.to_string();
    return ostr;
}

GeoRegion::GeoRegion(const Geospatial& geo)
{
    struct Visitor {
        std::unique_ptr<S2Region> operator()(const GeoBox& box) const
        {
            return std::make_unique<S2LatLngRect>(S2LatLng::FromDegrees(box.lo.latitude, box.lo.longitude),
                                                  S2LatLng::FromDegrees(box.hi.latitude, box.hi.longitude));
        }

        std::unique_ptr<S2Region> operator()(const GeoPolygon& polygon) const
        {
            REALM_ASSERT(polygon.points.size() >= 1);
            std::vector<S2Loop*> loops;
            loops.reserve(polygon.points.size());
            std::vector<S2Point> points;
            for (auto& geo_points : polygon.points) {
                points.clear();
                points.reserve(geo_points.size());
                for (auto&& p : geo_points) {
                    // FIXME rewrite without copying
                    points.push_back(S2LatLng::FromDegrees(p.latitude, p.longitude).ToPoint());
                }
                loops.push_back(new S2Loop(points));
            }
            // S2Polygon takes ownership of all the S2Loop pointers
            return std::make_unique<S2Polygon>(&loops);
        }

        std::unique_ptr<S2Region> operator()(const GeoCenterSphere& sphere) const
        {
            auto center = S2LatLng::FromDegrees(sphere.center.latitude, sphere.center.longitude).ToPoint();
            auto radius = S1Angle::Radians(sphere.radius_radians);
            return std::make_unique<S2Cap>(S2Cap::FromAxisAngle(center, radius));
        }

        std::unique_ptr<S2Region> operator()(const mpark::monostate&) const
        {
            REALM_UNREACHABLE();
        }

        std::unique_ptr<S2Region> operator()(const GeoPoint&) const
        {
            REALM_UNREACHABLE();
        }
    };

    m_region = mpark::visit(Visitor(), geo.m_value);
}

GeoRegion::~GeoRegion() = default;

bool GeoRegion::contains(const GeoPoint& geo_point) const noexcept
{
    auto point = S2LatLng::FromDegrees(geo_point.latitude, geo_point.longitude).ToPoint();
    return m_region->VirtualContainsPoint(point);
}

} // namespace realm
