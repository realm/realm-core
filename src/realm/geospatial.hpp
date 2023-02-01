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

#include <realm/string_data.hpp>

#include <optional>
#include <string_view>
#include <vector>

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

class Geospatial {
public:
    Geospatial(GeoPoint point)
        : m_type("Point")
        , m_points({point})
    {
    }

    Geospatial(StringData invalid_type)
        : m_type(invalid_type)
        , m_is_valid(false)
    {
    }

    static Geospatial from_link(const Obj& obj);
    void assign_to(Obj& link) const;

    std::string get_type() const noexcept
    {
        return m_type;
    }

    bool is_valid() const noexcept
    {
        return m_is_valid;
    }

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

private:
    std::string m_type;
    std::vector<GeoPoint> m_points;
    bool m_is_valid = true;
};

inline std::ostream& operator<<(std::ostream& ostr, const Geospatial& geo)
{
    std::string data_str;
    const auto& data = geo.get_points();
    for (size_t i = 0; i < data.size(); ++i) {
        const GeoPoint& point = data[i];
        if (point.altitude) {
            data_str += util::format("%1, %2, %3", point.longitude, point.latitude, *point.altitude);
        }
        else {
            data_str += util::format("%1, %2", point.longitude, point.latitude);
        }
    }
    ostr << util::format("Geospatial('%1', [%2])", geo.get_type(), data_str);
    return ostr;
}

} // namespace realm

#endif /* REALM_GEOSPATIAL_HPP */
