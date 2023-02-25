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
    double radius_km = 0.0;
    GeoPoint center;
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
        , m_radius_radians(centerSphere.radius_km / c_radius_km)
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

    std::string get_type_string() const noexcept
    {
        return is_valid() ? std::string(c_types[static_cast<size_t>(m_type)]) : *m_invalid_type;
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

    bool operator==(const Geospatial& other) const
    {
        return m_type == other.m_type && m_points == other.m_points;
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

    static const double c_radius_km;

private:
    Type m_type;
    std::optional<std::string> m_invalid_type;

    std::vector<GeoPoint> m_points;
    std::optional<double> m_radius_radians;

    mutable std::shared_ptr<S2Region> m_region;
    S2Region& get_region() const;
};

template <>
inline GeoCenterSphere Geospatial::get<GeoCenterSphere>() const noexcept
{
    REALM_ASSERT_EX(m_type == Type::CenterSphere, get_type_string());
    REALM_ASSERT(m_radius_radians);
    REALM_ASSERT(m_points.size() >= 1);
    return GeoCenterSphere{*m_radius_radians, m_points[0]};
}

class GeospatialStore {
public:
    GeospatialStore(GeoPoint* data, size_t size, Geospatial::Type type)
        : m_data(data)
        , m_size(size)
        , m_type(type)
    {
    }
    Geospatial get() const
    {
        REALM_ASSERT(m_data);
        switch (m_type) {
            case Geospatial::Type::Invalid:
                return Geospatial(""); // FIXME: error handling
            case Geospatial::Type::Point:
                REALM_ASSERT_EX(m_size == 1, m_size);
                return Geospatial(m_data[0]);
            case Geospatial::Type::Box:
                REALM_ASSERT_EX(m_size == 2, m_size);
                return Geospatial(GeoBox{m_data[0], m_data[1]});
            case Geospatial::Type::Polygon:
                REALM_UNREACHABLE(); // FIXME: implement dynamic fill
                break;
            case Geospatial::Type::CenterSphere:
                REALM_UNREACHABLE(); // FIXME: implement dynamic fill
                break;
        }
    }

private:
    const GeoPoint* m_data = nullptr;
    size_t m_size = 0;
    Geospatial::Type m_type = Geospatial::Type::Invalid;
};

std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo);

} // namespace realm

#endif /* REALM_GEOSPATIAL_HPP */
