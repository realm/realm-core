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

struct GeoPoint {
    double longitude = 0;
    double latitude = 0;
    std::optional<double> altitude;
    bool operator==(const GeoPoint& other) const
    {
        return longitude == other.longitude && latitude == other.latitude && altitude == other.altitude;
    }
};

struct GeoBox {
    GeoPoint p1;
    GeoPoint p2;
};

struct GeoPolygon {
    GeoPolygon(std::initializer_list<GeoPoint>&& l)
        : points(l)
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
    enum class Type { Point, Box, Polygon, CenterSphere, Invalid };

    Geospatial(GeoPoint point)
        : m_type(Type::Point)
        , m_points({point})
    {
    }
    Geospatial(GeoBox box)
        : m_type(Type::Box)
        , m_points({box.p1, box.p2})
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

    Geospatial(StringData invalid_type) // FIXME remove or use for error outside of the type?
        : m_type(Type::Invalid)
        , m_invalid_type(invalid_type)
    {
    }

    Geospatial(const Geospatial&) = default;
    Geospatial& operator=(const Geospatial&) = default;

    Geospatial(Geospatial&& other) = default;
    Geospatial& operator=(Geospatial&&) = default;

    static Geospatial from_obj(const Obj& obj, ColKey type_col = {}, ColKey coords_col = {});
    static Geospatial from_link(const Obj& obj);
    void assign_to(Obj& link) const;

    std::string get_type() const noexcept
    {
        return is_valid() ? std::string(c_types[static_cast<size_t>(m_type)]) : *m_invalid_type;
    }

    bool is_valid() const noexcept
    {
        return m_type != Type::Invalid;
    }

    bool is_within(const Geospatial& bounds) const noexcept;

    const std::vector<GeoPoint>& get_points() const
    {
        return m_points;
    }

    bool operator==(const Geospatial& other) const
    {
        return m_type == other.m_type && m_points == other.m_points && get_radius() == other.get_radius();
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
    std::optional<std::string> m_invalid_type;

    std::vector<GeoPoint> m_points;

    double m_radius_radians = get_nan();

    constexpr static double get_nan()
    {
        return std::numeric_limits<double>::quiet_NaN();
    }

    std::optional<double> get_radius() const
    {
        return std::isnan(m_radius_radians) ? std::optional<double>{} : m_radius_radians;
    }

    mutable std::shared_ptr<S2Region> m_region;
    S2Region& get_region() const;
};

std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo);

} // namespace realm

#endif /* REALM_GEOSPATIAL_HPP */
