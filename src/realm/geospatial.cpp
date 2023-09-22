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
#include <realm/util/scope_exit.hpp>

namespace {

static bool type_is_valid(realm::StringData str_type)
{
    return str_type.size() == 5 && (str_type[0] == 'P' || str_type[0] == 'p') &&
           (str_type[1] == 'o' || str_type[1] == 'O') && (str_type[2] == 'i' || str_type[2] == 'I') &&
           (str_type[3] == 'n' || str_type[3] == 'N') && (str_type[4] == 't' || str_type[4] == 'T');
}

} // anonymous namespace

namespace realm {

GeoPolygon GeoBox::to_polygon() const
{
    // We rely on the inversion rule here to ignore ordering of points.
    // ie: A polygon that encompases more than a hemisphere is inverted.
    return GeoPolygon{{{lo, GeoPoint{lo.longitude, hi.latitude}, hi, GeoPoint{hi.longitude, lo.latitude}, lo}}};
}

std::optional<GeoBox> GeoBox::from_polygon(const GeoPolygon& polygon)
{
    if (polygon.points.size() != 1) {
        return {};
    }
    if (polygon.points[0].size() != 5) {
        return {};
    }
    if (polygon.points[0][0] != polygon.points[0][4]) {
        return {}; // closed loop
    }
    GeoPoint corner1{polygon.points[0][0].longitude, polygon.points[0][2].latitude};
    GeoPoint corner2{polygon.points[0][2].longitude, polygon.points[0][0].latitude};
    if ((polygon.points[0][1] == corner1 && polygon.points[0][3] == corner2) ||
        (polygon.points[0][1] == corner2 && polygon.points[0][3] == corner1)) {
        return GeoBox{polygon.points[0][0], polygon.points[0][2]};
    }
    return {};
}

std::string Geospatial::get_type_string() const noexcept
{
    switch (get_type()) {
        case Type::Point:
            return "Point";
        case Type::Box:
            return "Box";
        case Type::Polygon:
            return "Polygon";
        case Type::Circle:
            return "Circle";
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

std::optional<GeoPoint> Geospatial::point_from_obj(const Obj& obj, ColKey type_col, ColKey coords_col)
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
        REALM_ASSERT_EX(coords_col.get_type() == ColumnType(ColumnType::Type::Double), coords_col.get_type());
        REALM_ASSERT(coords_col.is_list());
    }

    if (!type_is_valid(obj.get<StringData>(type_col))) {
        return {}; // the only geospatial type currently supported is 'Point'
    }

    Lst<double> coords = obj.get_list<double>(coords_col);
    const size_t coord_size = coords.size();
    if (coord_size < 2) {
        return {}; // invalid
    }
    if (coord_size > 2) {
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
        throw IllegalOperation(util::format("Attempting to store a '%1' in  class '%2' but the only Geospatial type "
                                            "currently supported for storage is 'Point'",
                                            get_type_string(), link.get_table()->get_class_name()));
    }
    auto&& point = get<GeoPoint>();
    link.set(type_col, get_type_string());
    Lst<double> coords = link.get_list<double>(coords_col);
    size_t existing_size = coords.size();
    std::optional<double> altitude = point.get_altitude();
    if (existing_size >= 1) {
        coords.set(0, point.longitude);
    }
    else {
        coords.add(point.longitude);
    }
    if (existing_size >= 2) {
        coords.set(1, point.latitude);
    }
    else {
        coords.add(point.latitude);
    }
    if (altitude) {
        if (existing_size >= 3) {
            coords.set(2, *altitude);
        }
        else {
            coords.add(*altitude);
        }
    }
    else if (existing_size >= 3) {
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

static std::string polygon_str(const GeoPolygon& poly)
{
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
}

Status Geospatial::is_valid() const noexcept
{
    switch (get_type()) {
        case Type::Polygon:
        case Type::Box:
        case Type::Circle:
            return get_region().get_conversion_status();
        default:
            return Status::OK();
    }
}

bool Geospatial::contains(const GeoPoint& point) const noexcept
{
    return get_region().contains(point);
}

GeoRegion& Geospatial::get_region() const
{
    if (!m_region)
        m_region = std::make_unique<GeoRegion>(*this);
    return *m_region.get();
}

std::string Geospatial::to_string() const
{
    return mpark::visit(util::overload{[](const GeoPoint& point) {
                                           return util::format("GeoPoint(%1)", point_str(point));
                                       },
                                       [](const GeoBox& box) {
                                           return polygon_str(box.to_polygon());
                                       },
                                       [](const GeoPolygon& poly) {
                                           return polygon_str(poly);
                                       },
                                       [](const GeoCircle& circle) {
                                           return util::format("GeoCircle(%1, %2)", point_str(circle.center),
                                                               circle.radius_radians);
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

// The following validation follows the server:
// https://github.com/mongodb/mongo/blob/053ff9f355555cddddf3a476ffa9ddf899b1657d/src/mongo/db/geo/geoparser.cpp#L140


// Technically lat/long bounds, not really tied to earth radius.
static bool is_valid_lat_lng(double lng, double lat)
{
    return abs(lng) <= 180 && abs(lat) <= 90;
}

static Status coord_to_point(double lng, double lat, S2Point& out)
{
    if (!is_valid_lat_lng(lng, lat))
        return Status(ErrorCodes::InvalidQueryArg,
                      util::format("Longitude/latitude is out of bounds, lng: %1 lat: %2", lng, lat));
    // Note that it's (lat, lng) for S2 but (lng, lat) for MongoDB.
    S2LatLng ll = S2LatLng::FromDegrees(lat, lng).Normalized();
    // This shouldn't happen since we should only have valid lng/lats.
    REALM_ASSERT_EX(ll.is_valid(), util::format("coords invalid after normalization, lng = %1, lat = %2", lng, lat));
    out = ll.ToPoint();
    return Status::OK();
}

static void erase_duplicate_adjacent_points(std::vector<S2Point>& vertices)
{
    vertices.erase(std::unique(vertices.begin(), vertices.end()), vertices.end());
}

static Status is_ring_closed(const std::vector<S2Point>& ring, const std::vector<GeoPoint>& points)
{
    if (ring.empty()) {
        return Status(ErrorCodes::InvalidQueryArg, "Ring has no vertices");
    }

    if (points[0] != points[points.size() - 1]) {
        return Status(ErrorCodes::InvalidQueryArg,
                      util::format("Ring is not closed, first vertex '%1' does not equal last vertex '%2'", points[0],
                                   points[points.size() - 1]));
    }

    return Status::OK();
}

static Status parse_polygon_coordinates(const GeoPolygon& polygon, S2Polygon* out)
{
    REALM_ASSERT(out);
    std::vector<S2Loop*> rings;
    rings.reserve(polygon.points.size());
    Status status = Status::OK();
    std::string err;

    // if the polygon is successfully created s2 takes ownership
    // of the pointers and clears our vector
    auto guard = util::make_scope_exit([&rings]() noexcept {
        for (auto& ring : rings) {
            delete ring;
        }
    });
    // Iterate all rings of the polygon.
    for (size_t i = 0; i < polygon.points.size(); ++i) {
        // Check if the ring is closed.
        std::vector<S2Point> points;
        points.reserve(polygon.points[i].size());
        for (auto&& p : polygon.points[i]) {
            S2Point s2p;
            status = coord_to_point(p.longitude, p.latitude, s2p);
            if (!status.is_ok()) {
                return status;
            }
            points.push_back(s2p);
        }

        status = is_ring_closed(points, polygon.points[i]);
        if (!status.is_ok())
            return status;

        erase_duplicate_adjacent_points(points);
        // Drop the duplicated last point.
        points.resize(points.size() - 1);

        // At least 3 vertices.
        if (points.size() < 3) {
            return Status(
                ErrorCodes::InvalidQueryArg,
                util::format("Ring %1 must have at least 3 different vertices, %2 unique vertices were provided", i,
                             points.size()));
        }

        rings.push_back(new S2Loop(points));
        S2Loop* ring = rings.back();

        // Check whether this ring is valid if vaildation hasn't been already done on 2dSphere index
        // insertion which is controlled by the 'skipValidation' flag.
        // 1. At least 3 vertices.
        // 2. All vertices must be unit length. Guaranteed by parsePoints().
        // 3. Rings are not allowed to have any duplicate vertices.
        // 4. Non-adjacent edges are not allowed to intersect.
        if (!ring->IsValid(&err)) {
            return Status(ErrorCodes::InvalidQueryArg, util::format("Ring %1 is not valid: '%2'", i, err));
        }
        // If the ring is more than one hemisphere, invert it.
        ring->Normalize();

        // Check the first ring must be the exterior ring and any others must be
        // interior rings or holes.
        if (rings.size() > 1 && !rings[0]->Contains(ring)) {
            return Status(ErrorCodes::InvalidQueryArg,
                          util::format("Secondary ring %1 not contained by first exterior ring - "
                                       "secondary rings must be holes in the first ring",
                                       i));
        }
    }

    if (rings.empty()) {
        return Status(ErrorCodes::InvalidQueryArg, "Polygon has no rings.");
    }

    // Check if the given rings form a valid polygon.
    // 1. If a ring contains an edge AB, then no other ring may contain AB or BA.
    // 2. No ring covers more than half of the sphere.
    // 3. No two ring cross.
    if (!S2Polygon::IsValid(rings, &err))
        return Status(ErrorCodes::InvalidQueryArg, util::format("Polygon isn't valid: '%1'", err));

    // Given all rings are valid / normalized and S2Polygon::IsValid() above returns true.
    // The polygon must be valid. See S2Polygon member function IsValid().

    {
        // Transfer ownership of the rings and clears ring vector.
        out->Init(&rings);
    }

    // Check if every ring of this polygon shares at most one vertex with
    // its parent ring.
    if (!out->IsNormalized(&err))
        // "err" looks like "Ring 1 shares more than one vertex with its parent ring 0"
        return Status(ErrorCodes::InvalidQueryArg, util::format("Polygon is not normalized: '%1'", err));

    // S2Polygon contains more than one ring, which is allowed by S2, but not by GeoJSON.
    //
    // Rings are indexed according to a preorder traversal of the nesting hierarchy.
    // GetLastDescendant() returns the index of the last ring that is contained within
    // a given ring. We guarantee that the first ring is the exterior ring.
    if (out->GetLastDescendant(0) < out->num_loops() - 1) {
        return Status(ErrorCodes::InvalidQueryArg, "Only one exterior polygon ring is allowed");
    }

    // In GeoJSON, only one nesting is allowed.
    // The depth of a ring is set by polygon according to the nesting hierarchy of polygon,
    // so the exterior ring's depth is 0, a hole in it is 1, etc.
    for (int i = 0; i < out->num_loops(); i++) {
        if (out->loop(i)->depth() > 1) {
            return Status(ErrorCodes::InvalidQueryArg,
                          util::format("Polygon interior rings cannot be nested: %1", i));
        }
    }
    return Status::OK();
}

GeoRegion::GeoRegion(const Geospatial& geo)
    : m_status(Status::OK())
{
    struct Visitor {
        Visitor(Status& out)
            : m_status_out(out)
        {
            m_status_out = Status::OK();
        }
        Status& m_status_out;

        std::unique_ptr<S2Region> operator()(const GeoBox& box) const
        {
            GeoPolygon polygon = box.to_polygon();
            auto poly = std::make_unique<S2Polygon>();
            m_status_out = parse_polygon_coordinates(polygon, poly.get());
            return poly;
        }

        std::unique_ptr<S2Region> operator()(const GeoPolygon& polygon) const
        {
            auto poly = std::make_unique<S2Polygon>();
            m_status_out = parse_polygon_coordinates(polygon, poly.get());
            return poly;
        }

        std::unique_ptr<S2Region> operator()(const GeoCircle& circle) const
        {
            S2Point center;
            m_status_out = coord_to_point(circle.center.longitude, circle.center.latitude, center);
            if (!m_status_out.is_ok())
                return nullptr;
            if (circle.radius_radians < 0 || std::isnan(circle.radius_radians)) {
                m_status_out =
                    Status(ErrorCodes::InvalidQueryArg, "The radius of a circle must be a non-negative number");
                return nullptr;
            }
            auto radius = S1Angle::Radians(circle.radius_radians);
            return std::make_unique<S2Cap>(S2Cap::FromAxisAngle(center, radius));
        }

        std::unique_ptr<S2Region> operator()(const mpark::monostate&) const
        {
            m_status_out = Status(ErrorCodes::InvalidQueryArg,
                                  "NULL cannot be used on the right hand side of a GEOWITHIN query");
            return nullptr;
        }

        std::unique_ptr<S2Region> operator()(const GeoPoint&) const
        {
            m_status_out = Status(ErrorCodes::InvalidQueryArg,
                                  "A point cannot be used on the right hand side of GEOWITHIN query");
            return nullptr;
        }
    };

    m_region = mpark::visit(Visitor(m_status), geo.m_value);
}

GeoRegion::~GeoRegion() = default;

bool GeoRegion::contains(const std::optional<GeoPoint>& geo_point) const noexcept
{
    if (!m_status.is_ok() || !geo_point) {
        return false;
    }
    auto point = S2LatLng::FromDegrees(geo_point->latitude, geo_point->longitude);
    if (!point.is_valid()) {
        return false;
    }
    return m_region->VirtualContainsPoint(point.ToPoint());
}

Status GeoRegion::get_conversion_status() const noexcept
{
    return m_status;
}

} // namespace realm
