////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <catch2/catch_all.hpp>

#include "util/event_loop.hpp"
#include "util/index_helpers.hpp"
#include "util/test_file.hpp"

#include <realm/object-store/feature_checks.hpp>
#include <realm/object-store/collection_notifications.hpp>
#include <realm/object-store/object_accessor.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/util/scheduler.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm/geospatial.hpp>
#include <realm/group.hpp>
#include <realm/util/any.hpp>

#if REALM_ENABLE_AUTH_TESTS
#include "sync/flx_sync_harness.hpp"
#endif // REALM_ENABLE_AUTH_TESTS

#include <cstdint>

using namespace realm;
using util::any_cast;

namespace {
using AnyDict = std::map<std::string, std::any>;
using AnyVec = std::vector<std::any>;
template <class T>
std::vector<T> get_vector(std::initializer_list<T> list)
{
    return std::vector<T>(list);
}
} // namespace

struct TestContext : CppContext {
    std::map<std::string, AnyDict> defaults;

    using CppContext::CppContext;
    TestContext(TestContext& parent, realm::Obj& obj, realm::Property const& prop)
        : CppContext(parent, obj, prop)
        , defaults(parent.defaults)
    {
    }

    util::Optional<std::any> default_value_for_property(ObjectSchema const& object, Property const& prop)
    {
        auto obj_it = defaults.find(object.name);
        if (obj_it == defaults.end())
            return util::none;
        auto prop_it = obj_it->second.find(prop.name);
        if (prop_it == obj_it->second.end())
            return util::none;
        return prop_it->second;
    }

    void will_change(Object const&, Property const&) {}
    void did_change() {}
    std::string print(std::any)
    {
        return "not implemented";
    }
    bool allow_missing(std::any)
    {
        return false;
    }

    template <class T>
    T get(Object& obj, const std::string& name)
    {
        return util::any_cast<T>(obj.get_property_value<std::any>(*this, name));
    }
};

TEST_CASE("geospatial") {
    using namespace std::string_literals;

    Schema schema{
        {"restaurant",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"location", PropertyType::Object | PropertyType::Nullable, "geoPointType"},
             {"array", PropertyType::Object | PropertyType::Array, "geoPointType"},
         }},
        {"geoPointType",
         ObjectSchema::ObjectType::Embedded,
         {
             {"type", PropertyType::String},
             {"coordinates", PropertyType::Double | PropertyType::Array},
         }},
    };
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema_mode = SchemaMode::Automatic;
    config.schema = schema;

    auto realm = Realm::get_shared_realm(config);
    CppContext ctx(realm);

    auto create = [&](std::any&& value, CreatePolicy policy = CreatePolicy::UpdateAll) {
        realm->begin_transaction();
        auto obj = Object::create(ctx, realm, *realm->schema().find("restaurant"), value, policy);
        realm->commit_transaction();
        return obj;
    };

    SECTION("Basic object creation") {
        auto obj = create(AnyDict{
            {"_id", INT64_C(1)},
            {"location", AnyDict{{"type", "Point"s}, {"coordinates", AnyVec{1.1, 2.2, 3.3}}}},
        });

        {
            TableRef table = obj.obj().get_table();
            REQUIRE(!Geospatial::is_geospatial(table, table->get_column_key("_id")));
            REQUIRE(Geospatial::is_geospatial(table, table->get_column_key("location")));
        }

        REQUIRE(obj.obj().get<int64_t>("_id") == 1);
        auto linked_obj = util::any_cast<Object>(obj.get_property_value<std::any>(ctx, "location")).obj();
        REQUIRE(linked_obj.is_valid());
        REQUIRE(linked_obj.get<String>("type") == "Point");
        auto list = util::any_cast<List>(util::any_cast<Object>(obj.get_property_value<std::any>(ctx, "location"))
                                             .get_property_value<std::any>(ctx, "coordinates"));
        REQUIRE(list.size() == 3);
        REQUIRE(list.get<double>(0) == 1.1);
        REQUIRE(list.get<double>(1) == 2.2);
        REQUIRE(list.get<double>(2) == 3.3);

        {
            Geospatial geo = obj.obj().get<Geospatial>("location");
            REQUIRE(geo.get_type_string() == "Point");
            REQUIRE(geo.get_type() == Geospatial::Type::Point);
            auto&& point = geo.get<GeoPoint>();
            REQUIRE(point.longitude == 1.1);
            REQUIRE(point.latitude == 2.2);
            REQUIRE(point.get_altitude());
            REQUIRE(*point.get_altitude() == 3.3);
        }

        {
            Geospatial geo{GeoPoint{4.4, 5.5, 6.6}};
            realm->begin_transaction();
            obj.set_property_value(ctx, "location", std::any{geo});
            realm->commit_transaction();
            Geospatial fetched = Geospatial::from_link(
                util::any_cast<Object>(obj.get_property_value<std::any>(ctx, "location")).obj());
            REQUIRE(geo == fetched);
        }
    }
}
