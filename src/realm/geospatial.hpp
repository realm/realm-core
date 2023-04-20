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

#ifndef REALM_GEOSPATIAL_HPP
#define REALM_GEOSPATIAL_HPP

#include <realm/keys.hpp>
#include <realm/string_data.hpp>

#include <climits>
#include <cmath>
#include <optional>
#include <string_view>
#include <vector>

class S2Region;

namespace realm {

class Obj;
class TableRef;

struct GeoPoint {
    GeoPoint() = delete;
    GeoPoint(double lon, double lat)
        : longitude(lon)
        , latitude(lat)
    {
    }
    GeoPoint(double lon, double lat, double alt)
        : longitude(lon)
        , latitude(lat)
        , altitude(alt)
    {
    }

    double longitude = get_nan();
    double latitude = get_nan();
    double altitude = get_nan();

    bool operator==(const GeoPoint& other) const
    {
        return (longitude == other.longitude || (std::isnan(longitude) && std::isnan(other.longitude))) &&
               (latitude == other.latitude || (std::isnan(latitude) && std::isnan(other.latitude))) &&
               ((!has_altitude() && !other.has_altitude()) || altitude == other.altitude);
    }

    bool is_valid() const
    {
        return !std::isnan(longitude) && !std::isnan(latitude);
    }

    bool has_altitude() const
    {
        return !std::isnan(altitude);
    }

    std::optional<double> get_altitude() const noexcept
    {
        return std::isnan(altitude) ? std::optional<double>{} : altitude;
    }

    void set_altitude(std::optional<double> val) noexcept
    {
        altitude = val.value_or(get_nan());
    }

    constexpr static double get_nan()
    {
        return std::numeric_limits<double>::quiet_NaN();
    }
};

// Construct a rectangle from minimum and maximum latitudes and longitudes.
// If lo.lng() > hi.lng(), the rectangle spans the 180 degree longitude
// line. Both points must be normalized, with lo.lat() <= hi.lat().
// The rectangle contains all the points p such that 'lo' <= p <= 'hi',
// where '<=' is defined in the obvious way.
struct GeoBox {
    GeoPoint lo;
    GeoPoint hi;
};

// A simple spherical polygon. It consists of a single
// chain of vertices where the first vertex is implicitly connected to the
// last. Chain of vertices is defined to have a CCW orientation, i.e. the interior
// of the polygon is on the left side of the edges.
struct GeoPolygon {
    GeoPolygon(std::initializer_list<GeoPoint>&& l)
        : points(std::move(l))
    {
    }
    GeoPolygon(std::vector<GeoPoint>&& p)
        : points(std::move(p))
    {
    }
    GeoPolygon(std::vector<GeoPoint> p)
        : points(std::move(p))
    {
    }
    std::vector<GeoPoint> points;
};

struct GeoCenterSphere {
    double radius_radians = 0.0;
    GeoPoint center;

    // Equatorial radius of earth.
    static const double c_radius_meters;

    static GeoCenterSphere from_kms(double km, GeoPoint&& p)
    {
        return GeoCenterSphere{km * 1000 / c_radius_meters, p};
    }
};

class Geospatial {
public:
    // keep this type small so it doesn't bloat the size of a Mixed
    enum class Type : uint8_t { Point, Box, Polygon, CenterSphere, Invalid };

    Geospatial()
        : m_type(Type::Invalid)
    {
    }
    Geospatial(GeoPoint point)
        : m_type(Type::Point)
        , m_points({point})
    {
    }
    Geospatial(GeoBox box)
        : m_type(Type::Box)
        , m_points({box.lo, box.hi})
    {
    }
    Geospatial(GeoPolygon polygon)
        : m_type(Type::Polygon)
        , m_points(std::move(polygon.points))
    {
    }
    Geospatial(GeoCenterSphere centerSphere)
        : m_type(Type::CenterSphere)
        , m_points({centerSphere.center})
        , m_radius_radians(centerSphere.radius_radians)
    {
    }

    Geospatial(const Geospatial&) = default;
    Geospatial& operator=(const Geospatial&) = default;

    Geospatial(Geospatial&& other) = default;
    Geospatial& operator=(Geospatial&&) = default;

    static Geospatial from_obj(const Obj& obj, ColKey type_col = {}, ColKey coords_col = {});
    static Geospatial from_link(const Obj& obj);
    static bool is_geospatial(const TableRef table, ColKey link_col);
    void assign_to(Obj& link) const;

    std::string get_type_string() const noexcept
    {
        return is_valid() ? std::string(c_types[static_cast<size_t>(m_type)]) : "Invalid";
    }
    Type get_type() const noexcept
    {
        return m_type;
    }

    template <class T>
    T get() const noexcept;

    bool is_valid() const noexcept
    {
        return m_type != Type::Invalid;
    }

    bool is_within(const Geospatial& bounds) const noexcept;

    const std::vector<GeoPoint>& get_points() const
    {
        return m_points;
    }

    void add_point_to_polygon(const GeoPoint& p)
    {
        REALM_ASSERT_EX(m_type == Type::Polygon, get_type_string());
        m_points.push_back(p);
    }

    bool operator==(const Geospatial& other) const
    {
        return m_type == other.m_type && m_points == other.m_points &&
               ((!is_radius_valid() && !other.is_radius_valid()) || (m_radius_radians == other.m_radius_radians));
    }
    bool operator!=(const Geospatial& other) const
    {
        return !(*this == other);
    }
    bool operator>(const Geospatial&) const = delete;
    bool operator<(const Geospatial& other) const = delete;
    bool operator>=(const Geospatial& other) const = delete;
    bool operator<=(const Geospatial& other) const = delete;

    constexpr static std::string_view c_geo_point_type_col_name = "type";
    constexpr static std::string_view c_geo_point_coords_col_name = "coordinates";
    constexpr static std::string_view c_types[] = {"Point", "Box", "Polygon", "CenterSphere"};

private:
    Type m_type;
    std::vector<GeoPoint> m_points;

    double m_radius_radians = get_nan();

    constexpr static double get_nan()
    {
        return std::numeric_limits<double>::quiet_NaN();
    }

    bool is_radius_valid() const
    {
        return !std::isnan(m_radius_radians);
    }

    mutable std::shared_ptr<S2Region> m_region;
    S2Region& get_region() const;
};

template <>
inline GeoCenterSphere Geospatial::get<GeoCenterSphere>() const noexcept
{
    REALM_ASSERT_EX(m_type == Type::CenterSphere, get_type_string());
    REALM_ASSERT(is_radius_valid());
    REALM_ASSERT(m_points.size() >= 1);
    return GeoCenterSphere{m_radius_radians, m_points[0]};
}

template <>
inline GeoPolygon Geospatial::get<GeoPolygon>() const noexcept
{
    REALM_ASSERT_EX(m_type == Type::Polygon, get_type_string());
    REALM_ASSERT(m_points.size() >= 1);
    return GeoPolygon(m_points);
}

std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo);

} // namespace realm

#endif /* REALM_GEOSPATIAL_HPP */
