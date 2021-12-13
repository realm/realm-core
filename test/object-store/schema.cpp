////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include <catch2/catch.hpp>
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>

#include <realm/group.hpp>
#include <realm/table.hpp>

using namespace realm;

struct SchemaChangePrinter {
    std::ostream& out;

    template <typename Value>
    void print(Value value) const
    {
        out << value;
    }

    template <typename Value, typename... Rest>
    void print(Value value, Rest... rest) const
    {
        out << value << ", ";
        print(rest...);
    }

#define REALM_SC_PRINT(type, ...)                                                                                    \
    void operator()(schema_change::type v) const                                                                     \
    {                                                                                                                \
        out << #type << "{";                                                                                         \
        print(__VA_ARGS__);                                                                                          \
        out << "}";                                                                                                  \
    }

    REALM_SC_PRINT(AddIndex, v.object, v.property)
    REALM_SC_PRINT(AddProperty, v.object, v.property)
    REALM_SC_PRINT(AddTable, v.object)
    REALM_SC_PRINT(RemoveTable, v.object)
    REALM_SC_PRINT(ChangeTableType, v.object)
    REALM_SC_PRINT(AddInitialProperties, v.object)
    REALM_SC_PRINT(ChangePrimaryKey, v.object, v.property)
    REALM_SC_PRINT(ChangePropertyType, v.object, v.old_property, v.new_property)
    REALM_SC_PRINT(MakePropertyNullable, v.object, v.property)
    REALM_SC_PRINT(MakePropertyRequired, v.object, v.property)
    REALM_SC_PRINT(RemoveIndex, v.object, v.property)
    REALM_SC_PRINT(RemoveProperty, v.object, v.property)

#undef REALM_SC_PRINT
};

namespace Catch {
template <>
struct StringMaker<SchemaChange> {
    static std::string convert(SchemaChange const& sc)
    {
        std::stringstream ss;
        sc.visit(SchemaChangePrinter{ss});
        return ss.str();
    }
};
} // namespace Catch

TEST_CASE("ObjectSchema") {
    SECTION("Aliases are still present in schema returned from the Realm") {
        TestFile config;
        config.schema_version = 1;
        config.schema = Schema{
            {"object",
             {{"value", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}, "alias"}}},
        };

        auto realm = Realm::get_shared_realm(config);
        REQUIRE(realm->schema().find("object")->property_for_name("value")->public_name == "alias");
    }

    SECTION("looking up properties by alias matches name if alias is not set") {
        auto schema = Schema{
            {"object",
             {{"value", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}, "alias"},
              {"other_value", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}}}},
        };

        REQUIRE(schema.find("object")->property_for_public_name("value") == nullptr);
        REQUIRE(schema.find("object")->property_for_public_name("alias")->name == "value");
        REQUIRE(schema.find("object")->property_for_public_name("other_value")->name == "other_value");
    }

    SECTION("from a Group") {
        Group g;

        TableRef table = g.add_table_with_primary_key("class_table", type_Int, "pk");
        TableRef target = g.add_table("class_target");
        TableRef embedded = g.add_embedded_table("class_embedded");

        table->add_column(type_Int, "int");
        table->add_column(type_Bool, "bool");
        table->add_column(type_Float, "float");
        table->add_column(type_Double, "double");
        table->add_column(type_String, "string");
        table->add_column(type_Binary, "data");
        table->add_column(type_Timestamp, "date");
        table->add_column(type_ObjectId, "object id");
        table->add_column(type_Decimal, "decimal");
        table->add_column(type_UUID, "uuid");

        table->add_column(*target, "object");
        table->add_column_list(*target, "array");
        table->add_column_set(*target, "set");
        table->add_column_dictionary(*target, "dictionary");

        table->add_column(type_Int, "int?", true);
        table->add_column(type_Bool, "bool?", true);
        table->add_column(type_Float, "float?", true);
        table->add_column(type_Double, "double?", true);
        table->add_column(type_String, "string?", true);
        table->add_column(type_Binary, "data?", true);
        table->add_column(type_Timestamp, "date?", true);
        table->add_column(type_ObjectId, "object id?", true);
        table->add_column(type_Decimal, "decimal?", true);
        table->add_column(type_UUID, "uuid?", true);

        auto add_list = [](TableRef table, DataType type, StringData name, bool nullable) {
            table->add_column_list(type, name, nullable);
        };

        add_list(table, type_Int, "int array", false);
        add_list(table, type_Bool, "bool array", false);
        add_list(table, type_Float, "float array", false);
        add_list(table, type_Double, "double array", false);
        add_list(table, type_String, "string array", false);
        add_list(table, type_Binary, "data array", false);
        add_list(table, type_Timestamp, "date array", false);
        add_list(table, type_ObjectId, "object id array", false);
        add_list(table, type_Decimal, "decimal array", false);
        add_list(table, type_UUID, "uuid array", false);
        add_list(table, type_Int, "int? array", true);
        add_list(table, type_Bool, "bool? array", true);
        add_list(table, type_Float, "float? array", true);
        add_list(table, type_Double, "double? array", true);
        add_list(table, type_String, "string? array", true);
        add_list(table, type_Binary, "data? array", true);
        add_list(table, type_Timestamp, "date? array", true);
        add_list(table, type_ObjectId, "object id? array", true);
        add_list(table, type_Decimal, "decimal? array", true);
        add_list(table, type_UUID, "uuid? array", true);

        auto add_set = [](TableRef table, DataType type, StringData name, bool nullable) {
            table->add_column_set(type, name, nullable);
        };

        add_set(table, type_Int, "int set", false);
        add_set(table, type_Bool, "bool set", false);
        add_set(table, type_Float, "float set", false);
        add_set(table, type_Double, "double set", false);
        add_set(table, type_String, "string set", false);
        add_set(table, type_Binary, "data set", false);
        add_set(table, type_Timestamp, "date set", false);
        add_set(table, type_ObjectId, "object id set", false);
        add_set(table, type_Decimal, "decimal set", false);
        add_set(table, type_UUID, "uuid set", false);
        add_set(table, type_Int, "int? set", true);
        add_set(table, type_Bool, "bool? set", true);
        add_set(table, type_Float, "float? set", true);
        add_set(table, type_Double, "double? set", true);
        add_set(table, type_String, "string? set", true);
        add_set(table, type_Binary, "data? set", true);
        add_set(table, type_Timestamp, "date? set", true);
        add_set(table, type_ObjectId, "object id? set", true);
        add_set(table, type_Decimal, "decimal? set", true);
        add_set(table, type_UUID, "uuid? set", true);

        auto add_dictionary = [](TableRef table, DataType type, StringData name, bool nullable = false) {
            table->add_column_dictionary(type, name, nullable);
        };

        add_dictionary(table, type_Int, "int dictionary?", true);
        add_dictionary(table, type_Bool, "bool dictionary");
        add_dictionary(table, type_Float, "float dictionary");
        add_dictionary(table, type_Double, "double dictionary");
        add_dictionary(table, type_String, "string dictionary");
        add_dictionary(table, type_Binary, "data dictionary");
        add_dictionary(table, type_Timestamp, "date dictionary");
        add_dictionary(table, type_ObjectId, "object id dictionary");
        add_dictionary(table, type_Decimal, "decimal dictionary");
        add_dictionary(table, type_UUID, "uuid dictionary");

        std::vector<ColKey> indexed_cols;
        indexed_cols.push_back(table->add_column(type_Int, "indexed int"));
        indexed_cols.push_back(table->add_column(type_Bool, "indexed bool"));
        indexed_cols.push_back(table->add_column(type_String, "indexed string"));
        indexed_cols.push_back(table->add_column(type_Timestamp, "indexed date"));
        indexed_cols.push_back(table->add_column(type_ObjectId, "indexed object id"));
        indexed_cols.push_back(table->add_column(type_UUID, "indexed uuid"));

        indexed_cols.push_back(table->add_column(type_Int, "indexed int?", true));
        indexed_cols.push_back(table->add_column(type_Bool, "indexed bool?", true));
        indexed_cols.push_back(table->add_column(type_String, "indexed string?", true));
        indexed_cols.push_back(table->add_column(type_Timestamp, "indexed date?", true));
        indexed_cols.push_back(table->add_column(type_ObjectId, "indexed object id?", true));
        indexed_cols.push_back(table->add_column(type_UUID, "indexed uuid?", true));

        for (auto col : indexed_cols)
            table->add_search_index(col);

        ObjectSchema os(g, "table", table->get_key());
        REQUIRE(os.table_key == table->get_key());
        ObjectSchema os1(g, "embedded", {});
        REQUIRE(os1.table_key == embedded->get_key());
        REQUIRE(os1.is_embedded);

#define REQUIRE_PROPERTY(name, type, ...)                                                                            \
    do {                                                                                                             \
        Property* prop;                                                                                              \
        REQUIRE((prop = os.property_for_name(name)));                                                                \
        REQUIRE((*prop == Property{name, PropertyType::type, __VA_ARGS__}));                                         \
        REQUIRE(prop->column_key == *expected_col++);                                                                \
    } while (0)

        auto all_column_keys = table->get_column_keys();
        auto expected_col = all_column_keys.begin();

        REQUIRE(os.property_for_name("nonexistent property") == nullptr);

        REQUIRE_PROPERTY("pk", Int, Property::IsPrimary{true});

        REQUIRE_PROPERTY("int", Int);
        REQUIRE_PROPERTY("bool", Bool);
        REQUIRE_PROPERTY("float", Float);
        REQUIRE_PROPERTY("double", Double);
        REQUIRE_PROPERTY("string", String);
        REQUIRE_PROPERTY("data", Data);
        REQUIRE_PROPERTY("date", Date);
        REQUIRE_PROPERTY("object id", ObjectId);
        REQUIRE_PROPERTY("decimal", Decimal);
        REQUIRE_PROPERTY("uuid", UUID);

        REQUIRE_PROPERTY("object", Object | PropertyType::Nullable, "target");
        REQUIRE_PROPERTY("array", Array | PropertyType::Object, "target");
        REQUIRE_PROPERTY("set", Set | PropertyType::Object, "target");
        REQUIRE_PROPERTY("dictionary", Dictionary | PropertyType::Object | PropertyType::Nullable, "target");

        REQUIRE_PROPERTY("int?", Int | PropertyType::Nullable);
        REQUIRE_PROPERTY("bool?", Bool | PropertyType::Nullable);
        REQUIRE_PROPERTY("float?", Float | PropertyType::Nullable);
        REQUIRE_PROPERTY("double?", Double | PropertyType::Nullable);
        REQUIRE_PROPERTY("string?", String | PropertyType::Nullable);
        REQUIRE_PROPERTY("data?", Data | PropertyType::Nullable);
        REQUIRE_PROPERTY("date?", Date | PropertyType::Nullable);
        REQUIRE_PROPERTY("object id?", ObjectId | PropertyType::Nullable);
        REQUIRE_PROPERTY("decimal?", Decimal | PropertyType::Nullable);
        REQUIRE_PROPERTY("uuid?", UUID | PropertyType::Nullable);

        REQUIRE_PROPERTY("int array", Int | PropertyType::Array);
        REQUIRE_PROPERTY("bool array", Bool | PropertyType::Array);
        REQUIRE_PROPERTY("float array", Float | PropertyType::Array);
        REQUIRE_PROPERTY("double array", Double | PropertyType::Array);
        REQUIRE_PROPERTY("string array", String | PropertyType::Array);
        REQUIRE_PROPERTY("data array", Data | PropertyType::Array);
        REQUIRE_PROPERTY("date array", Date | PropertyType::Array);
        REQUIRE_PROPERTY("object id array", ObjectId | PropertyType::Array);
        REQUIRE_PROPERTY("decimal array", Decimal | PropertyType::Array);
        REQUIRE_PROPERTY("uuid array", UUID | PropertyType::Array);
        REQUIRE_PROPERTY("int? array", Int | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("bool? array", Bool | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("float? array", Float | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("double? array", Double | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("string? array", String | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("data? array", Data | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("date? array", Date | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("object id? array", ObjectId | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("decimal? array", Decimal | PropertyType::Array | PropertyType::Nullable);
        REQUIRE_PROPERTY("uuid? array", UUID | PropertyType::Array | PropertyType::Nullable);

        REQUIRE_PROPERTY("int set", Int | PropertyType::Set);
        REQUIRE_PROPERTY("bool set", Bool | PropertyType::Set);
        REQUIRE_PROPERTY("float set", Float | PropertyType::Set);
        REQUIRE_PROPERTY("double set", Double | PropertyType::Set);
        REQUIRE_PROPERTY("string set", String | PropertyType::Set);
        REQUIRE_PROPERTY("data set", Data | PropertyType::Set);
        REQUIRE_PROPERTY("date set", Date | PropertyType::Set);
        REQUIRE_PROPERTY("object id set", ObjectId | PropertyType::Set);
        REQUIRE_PROPERTY("decimal set", Decimal | PropertyType::Set);
        REQUIRE_PROPERTY("uuid set", UUID | PropertyType::Set);
        REQUIRE_PROPERTY("int? set", Int | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("bool? set", Bool | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("float? set", Float | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("double? set", Double | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("string? set", String | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("data? set", Data | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("date? set", Date | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("object id? set", ObjectId | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("decimal? set", Decimal | PropertyType::Set | PropertyType::Nullable);
        REQUIRE_PROPERTY("uuid? set", UUID | PropertyType::Set | PropertyType::Nullable);

        REQUIRE_PROPERTY("int dictionary?", Int | PropertyType::Dictionary | PropertyType::Nullable);
        REQUIRE_PROPERTY("bool dictionary", Bool | PropertyType::Dictionary);
        REQUIRE_PROPERTY("float dictionary", Float | PropertyType::Dictionary);
        REQUIRE_PROPERTY("double dictionary", Double | PropertyType::Dictionary);
        REQUIRE_PROPERTY("string dictionary", String | PropertyType::Dictionary);
        REQUIRE_PROPERTY("data dictionary", Data | PropertyType::Dictionary);
        REQUIRE_PROPERTY("date dictionary", Date | PropertyType::Dictionary);
        REQUIRE_PROPERTY("object id dictionary", ObjectId | PropertyType::Dictionary);
        REQUIRE_PROPERTY("decimal dictionary", Decimal | PropertyType::Dictionary);
        REQUIRE_PROPERTY("uuid dictionary", UUID | PropertyType::Dictionary);

        REQUIRE_PROPERTY("indexed int", Int, Property::IsPrimary{false}, Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed bool", Bool, Property::IsPrimary{false}, Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed string", String, Property::IsPrimary{false}, Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed date", Date, Property::IsPrimary{false}, Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed object id", ObjectId, Property::IsPrimary{false}, Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed uuid", UUID, Property::IsPrimary{false}, Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed int?", Int | PropertyType::Nullable, Property::IsPrimary{false},
                         Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed bool?", Bool | PropertyType::Nullable, Property::IsPrimary{false},
                         Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed string?", String | PropertyType::Nullable, Property::IsPrimary{false},
                         Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed date?", Date | PropertyType::Nullable, Property::IsPrimary{false},
                         Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed object id?", ObjectId | PropertyType::Nullable, Property::IsPrimary{false},
                         Property::IsIndexed{true});
        REQUIRE_PROPERTY("indexed uuid?", UUID | PropertyType::Nullable, Property::IsPrimary{false},
                         Property::IsIndexed{true});
    }
}

TEST_CASE("Schema") {
    SECTION("validate()") {
        SECTION("rejects link properties with no target object") {
            Schema schema = {
                {"object", {{"link", PropertyType::Object | PropertyType::Nullable}}},
            };
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.link' of type 'object' has unknown object type ''");
        }

        SECTION("rejects array properties with no target object") {
            Schema schema = {
                {"object", {{"array", PropertyType::Array | PropertyType::Object}}},
            };
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.array' of type 'array' has unknown object type ''");
        }

        SECTION("rejects link properties with a target not in the schema") {
            Schema schema = {{"object", {{"link", PropertyType::Object | PropertyType::Nullable, "invalid target"}}}};
            REQUIRE_THROWS_CONTAINING(
                schema.validate(),
                "Property 'object.link' of type 'object' has unknown object type 'invalid target'");
        }

        SECTION("rejects array properties with a target not in the schema") {
            Schema schema = {{"object", {{"array", PropertyType::Array | PropertyType::Object, "invalid target"}}}};
            REQUIRE_THROWS_CONTAINING(
                schema.validate(),
                "Property 'object.array' of type 'array' has unknown object type 'invalid target'");
        }

        SECTION("allows embedded objects in lists and dictionaries") {
            Schema schema = {
                {"target", ObjectSchema::IsEmbedded{true}, {{"value", PropertyType::Int}}},
                {"object",
                 {
                     {"list", PropertyType::Object | PropertyType::Array, "target"},
                     {"dictionary", PropertyType::Object | PropertyType::Dictionary | PropertyType::Nullable,
                      "target"},
                 }},
            };
            REQUIRE_NOTHROW(schema.validate());
        }

        SECTION("rejects embedded objects in sets") {
            Schema schema = {
                {"target", ObjectSchema::IsEmbedded{true}, {{"value", PropertyType::Int}}},
                {"object", {{"set", PropertyType::Object | PropertyType::Set, "target"}}},
            };
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Set property 'object.set' cannot contain embedded object type 'target'. Set "
                                      "semantics are not applicable to embedded objects.");
        }

        SECTION("rejects explicitly included embedded object orphans") {
            Schema schema = {{"target", {{"value", PropertyType::Int}}},
                             {"origin",
                              ObjectSchema::IsEmbedded{true},
                              {{"link", PropertyType::Object | PropertyType::Nullable, "target"}}}};
            REQUIRE_NOTHROW(schema.validate());
            REQUIRE_THROWS_CONTAINING(
                schema.validate(SchemaValidationMode::RejectEmbeddedOrphans),
                "Embedded object 'origin' is unreachable by any link path from top level objects.");
        }

        SECTION("allows embedded object chains starting from a top level object") {
            Schema schema = {
                {"top", {{"linkA", PropertyType::Object | PropertyType::Nullable, "A"}}},
                {"A", ObjectSchema::IsEmbedded{true}, {{"link", PropertyType::Object | PropertyType::Nullable, "B"}}},
                {"B", ObjectSchema::IsEmbedded{true}, {{"link", PropertyType::Object | PropertyType::Nullable, "C"}}},
                {"C", ObjectSchema::IsEmbedded{true}, {{"value", PropertyType::Int}}}};
            REQUIRE_NOTHROW(schema.validate());
            REQUIRE_NOTHROW(schema.validate(SchemaValidationMode::RejectEmbeddedOrphans));
        }

        SECTION("allows link properties from embedded to top-level") {
            Schema schema = {{"target",
                              {{"value", PropertyType::Int},
                               {"link_to_embedded_object", PropertyType::Object | PropertyType::Nullable, "origin"}}},
                             {"origin",
                              ObjectSchema::IsEmbedded{true},
                              {{"link", PropertyType::Object | PropertyType::Nullable, "target"}}}};
            REQUIRE_NOTHROW(schema.validate());
        }

        SECTION("allows array properties from embedded to top-level") {
            Schema schema = {{"target",
                              {{"value", PropertyType::Int},
                               {"link_to_embedded_object", PropertyType::Object | PropertyType::Array, "origin"}}},
                             {"origin",
                              ObjectSchema::IsEmbedded{true},
                              {{"array", PropertyType::Array | PropertyType::Object, "target"}}}};
            REQUIRE_NOTHROW(schema.validate());
        }

        SECTION("allows linking objects from embedded to top-level") {
            Schema schema = {{"target",
                              ObjectSchema::IsEmbedded{true},
                              {{"value", PropertyType::Int}},
                              {{"incoming", PropertyType::Array | PropertyType::LinkingObjects, "origin", "array"}}},
                             {"origin", {{"array", PropertyType::Array | PropertyType::Object, "target"}}}};
            REQUIRE_NOTHROW(schema.validate());
        }

        SECTION("does not reject an embedded object loop via top level object") {
            Schema schema = {
                {"TopLevelObject",
                 {{"link_to_embedded_object", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject"}}},
                {"EmbeddedObject",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_top_level_object", PropertyType::Object | PropertyType::Nullable, "TopLevelObject"}}}};
            REQUIRE_NOTHROW(schema.validate());
            REQUIRE_NOTHROW(schema.validate(SchemaValidationMode::RejectEmbeddedOrphans));
        }

        SECTION("does not reject a top level loop via embedded object link") {
            Schema schema = {
                {"TopLevelObject",
                 {{"link_to_self", PropertyType::Object | PropertyType::Nullable, "TopLevelObject"},
                  {"link_to_embedded_object", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject"}}},
                {"EmbeddedObject",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_top_level_object", PropertyType::Object | PropertyType::Nullable, "TopLevelObject"}}}};
            REQUIRE_NOTHROW(schema.validate());
        }

        SECTION("rejects embedded objects loop to itself") {
            Schema schema = {
                {"TopLevelObject",
                 {{"link_to_embedded_object", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject"}}},
                {"EmbeddedObject",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_self", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject"}}}};
            REQUIRE_THROWS_CONTAINING(
                schema.validate(),
                "Cycles containing embedded objects are not currently supported: 'EmbeddedObject.link_to_self'");
        }

        SECTION("rejects embedded objects loop to itself from a list") {
            Schema schema = {
                {"TopLevelObject",
                 {{"link_to_embedded_object", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject"}}},
                {"EmbeddedObject",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_self", PropertyType::Object | PropertyType::Array, "EmbeddedObject"}}}};
            REQUIRE_THROWS_CONTAINING(
                schema.validate(),
                "Cycles containing embedded objects are not currently supported: 'EmbeddedObject.link_to_self'");
        }

        SECTION("rejects embedded objects loop via different embedded object") {
            Schema schema = {
                {"TopLevelObject",
                 {{"link_to_embedded_object", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"}}},
                {"EmbeddedObjectA",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_b", PropertyType::Object | PropertyType::Array, "EmbeddedObjectB"}}},
                {"EmbeddedObjectB",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_a", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(), "Cycles containing embedded objects are not currently "
                                                         "supported: 'EmbeddedObjectA.link_to_b.link_to_a'");
        }

        SECTION("rejects with descriptions of all embedded object loops") {
            Schema schema = {
                {"TopLevelObject",
                 {{"link_to_embedded_object", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"}}},
                {"EmbeddedObjectA",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_c", PropertyType::Object | PropertyType::Array, "EmbeddedObjectC"},
                  {"link_to_b", PropertyType::Object | PropertyType::Array, "EmbeddedObjectB"}}},
                {"EmbeddedObjectB",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_a", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"}}},
                {"EmbeddedObjectC",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_a", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"}}}};
            std::string message;
            try {
                schema.validate();
            }
            catch (const std::exception& e) {
                message = e.what();
            }
            bool found_loop_on_a = message.find("EmbeddedObjectA.link_to_c.link_to_a") != std::string::npos ||
                                   message.find("EmbeddedObjectA.link_to_b.link_to_a") != std::string::npos;
            bool found_loop_on_b = message.find("EmbeddedObjectB.link_to_a.link_to_b") != std::string::npos ||
                                   message.find("EmbeddedObjectB.link_to_a.link_to_c.link_to_a") != std::string::npos;
            bool found_loop_on_c = message.find("EmbeddedObjectC.link_to_a.link_to_c") != std::string::npos ||
                                   message.find("EmbeddedObjectC.link_to_a.link_to_b.link_to_a") != std::string::npos;
            REQUIRE(found_loop_on_a);
            REQUIRE(found_loop_on_b);
            REQUIRE(found_loop_on_c);
        }

        SECTION("allows top level loops") {
            Schema schema = {
                {"TopLevelObjectA",
                 {{"link_to_top_b", PropertyType::Object | PropertyType::Nullable, "TopLevelObjectB"},
                  {"link_to_embedded_b", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectB"}}},
                {"TopLevelObjectB",
                 {{"link_to_top_a", PropertyType::Object | PropertyType::Nullable, "TopLevelObjectA"}}},
                {"EmbeddedObjectA",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_b", PropertyType::Object | PropertyType::Array, "TopLevelObjectB"}}},
                {"EmbeddedObjectB",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_a", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"}}}};
            REQUIRE_NOTHROW(schema.validate());
            REQUIRE_NOTHROW(schema.validate(SchemaValidationMode::RejectEmbeddedOrphans));
        }

        SECTION("distinct paths to an embedded object is not a loop") {
            Schema schema = {
                {"TopLevelObjectA",
                 {{"link1_to_embedded", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"},
                  {"link2_to_embedded", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"}}},
                {"EmbeddedObjectA", ObjectSchema::IsEmbedded{true}, {{"prop", PropertyType::Int}}},
            };
            REQUIRE_NOTHROW(schema.validate());
            REQUIRE_NOTHROW(schema.validate(SchemaValidationMode::RejectEmbeddedOrphans));
        }

        SECTION("linked distinct paths to an embedded object is not a loop") {
            Schema schema = {
                {"TopLevelObjectA",
                 {{"link_to_embedded_A", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectA"},
                  {"link_to_embedded_B", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectB"}}},
                {"EmbeddedObjectA",
                 ObjectSchema::IsEmbedded{true},
                 {{"link_to_c", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectC"},
                  {"link_to_b", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectB"}}},
                {"EmbeddedObjectB",
                 ObjectSchema::IsEmbedded{true},
                 {
                     {"link_to_c", PropertyType::Object | PropertyType::Nullable, "EmbeddedObjectC"},
                 }},
                {"EmbeddedObjectC", ObjectSchema::IsEmbedded{true}, {{"prop", PropertyType::Int}}}

            };
            REQUIRE_NOTHROW(schema.validate());
            REQUIRE_NOTHROW(schema.validate(SchemaValidationMode::RejectEmbeddedOrphans));
        }

        SECTION("rejects linking objects without a source object") {
            Schema schema = {{"object",
                              {
                                  {"value", PropertyType::Int},
                              },
                              {{"incoming", PropertyType::Array | PropertyType::LinkingObjects, "", ""}}}};
            REQUIRE_THROWS_CONTAINING(
                schema.validate(), "Property 'object.incoming' of type 'linking objects' has unknown object type ''");
        }

        SECTION("rejects linking objects without a source property") {
            Schema schema = {{"object",
                              {
                                  {"value", PropertyType::Int},
                              },
                              {{"incoming", PropertyType::Array | PropertyType::LinkingObjects, "object", ""}}}};
            REQUIRE_THROWS_CONTAINING(
                schema.validate(),
                "Property 'object.incoming' of type 'linking objects' must have an origin property name.");
        }

        SECTION("rejects linking objects with invalid source object") {
            Schema schema = {
                {"object",
                 {
                     {"value", PropertyType::Int},
                 },
                 {{"incoming", PropertyType::Array | PropertyType::LinkingObjects, "not an object type", ""}}}};
            REQUIRE_THROWS_CONTAINING(
                schema.validate(),
                "Property 'object.incoming' of type 'linking objects' has unknown object type 'not an object type'");
        }

        SECTION("rejects linking objects with invalid source property") {
            Schema schema = {{"object",
                              {
                                  {"value", PropertyType::Int},
                              },
                              {{"incoming", PropertyType::Array | PropertyType::LinkingObjects, "object", "value"}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(), "Property 'object.value' declared as origin of linking "
                                                         "objects property 'object.incoming' is not a link");

            schema = {{"object",
                       {
                           {"value", PropertyType::Int},
                           {"link", PropertyType::Object | PropertyType::Nullable, "object 2"},
                       },
                       {{"incoming", PropertyType::Array | PropertyType::LinkingObjects, "object", "link"}}},
                      {"object 2",
                       {
                           {"value", PropertyType::Int},
                       }}};
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.link' declared as origin of linking objects property "
                                      "'object.incoming' links to type 'object 2'");
        }

        SECTION("rejects non-array linking objects") {
            Schema schema = {{"object",
                              {
                                  {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                              },
                              {{"incoming", PropertyType::LinkingObjects, "object", "link"}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Linking Objects property 'object.incoming' must be an array.");
        }

        SECTION("rejects target object types for non-link properties") {
            Schema schema = {{"object",
                              {
                                  {"int", PropertyType::Int},
                                  {"bool", PropertyType::Bool},
                                  {"float", PropertyType::Float},
                                  {"double", PropertyType::Double},
                                  {"string", PropertyType::String},
                                  {"date", PropertyType::Date},
                                  {"data", PropertyType::Data},
                                  {"object id", PropertyType::ObjectId},
                                  {"decimal", PropertyType::Decimal},
                              }}};
            for (auto& prop : schema.begin()->persisted_properties) {
                REQUIRE_NOTHROW(schema.validate());
                prop.object_type = "object";
                REQUIRE_THROWS_CONTAINING(schema.validate(), "cannot have an object type.");
                prop.object_type = "";
            }
        }

        SECTION("rejects source property name for non-linking objects properties") {
            Schema schema = {{"object",
                              {
                                  {"int", PropertyType::Int},
                                  {"bool", PropertyType::Bool},
                                  {"float", PropertyType::Float},
                                  {"double", PropertyType::Double},
                                  {"string", PropertyType::String},
                                  {"data", PropertyType::Data},
                                  {"date", PropertyType::Date},
                                  {"object id", PropertyType::ObjectId},
                                  {"decimal", PropertyType::Decimal},
                                  {"uuid", PropertyType::UUID},
                                  {"object", PropertyType::Object | PropertyType::Nullable, "object"},
                                  {"array", PropertyType::Object | PropertyType::Array, "object"},
                              }}};
            for (auto& prop : schema.begin()->persisted_properties) {
                REQUIRE_NOTHROW(schema.validate());
                prop.link_origin_property_name = "source";
                auto expected = util::format("Property 'object.%1' of type '%1' cannot have an origin property name.",
                                             prop.name, prop.name);
                REQUIRE_THROWS_CONTAINING(schema.validate(), expected);
                prop.link_origin_property_name = "";
            }
        }

        SECTION("rejects non-nullable link properties") {
            Schema schema = {{"object", {{"link", PropertyType::Object, "target"}}},
                             {"target", {{"value", PropertyType::Int}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(), "Property 'object.link' of type 'object' must be nullable.");
        }

        SECTION("rejects non-nullable dictionary properties") {
            Schema schema = {{"object", {{"dictionary", PropertyType::Dictionary | PropertyType::Object, "target"}}},
                             {"target", {{"value", PropertyType::Int}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.dictionary' of type 'object' must be nullable.");
        }

        SECTION("rejects nullable array properties") {
            Schema schema = {
                {"object",
                 {{"array", PropertyType::Array | PropertyType::Object | PropertyType::Nullable, "target"}}},
                {"target", {{"value", PropertyType::Int}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.array' of type 'array' cannot be nullable.");
        }

        SECTION("rejects nullable set properties") {
            Schema schema = {
                {"object", {{"set", PropertyType::Set | PropertyType::Object | PropertyType::Nullable, "target"}}},
                {"target", {{"value", PropertyType::Int}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(), "Property 'object.set' of type 'set' cannot be nullable.");
        }

        SECTION("rejects nullable linking objects") {
            Schema schema = {
                {"object",
                 {
                     {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                 },
                 {{"incoming", PropertyType::LinkingObjects | PropertyType::Array | PropertyType::Nullable, "object",
                   "link"}}}};
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.incoming' of type 'linking objects' cannot be nullable.");
        }

        SECTION("rejects duplicate primary keys") {
            Schema schema = {{"object",
                              {
                                  {"pk1", PropertyType::Int, Property::IsPrimary{true}},
                                  {"pk2", PropertyType::Int, Property::IsPrimary{true}},
                              }}};
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Properties 'pk2' and 'pk1' are both marked as the primary key of 'object'.");
        }

        SECTION("rejects primary key on embedded table") {
            Schema schema = {{"object",
                              ObjectSchema::IsEmbedded{true},
                              {
                                  {"pk1", PropertyType::Int, Property::IsPrimary{true}},
                                  {"int", PropertyType::Int},
                              }}};
            REQUIRE_THROWS_CONTAINING(schema.validate(), "Embedded object type 'object' cannot have a primary key.");
        }

        SECTION("rejects invalid primary key types") {
            Schema schema = {{"object",
                              {
                                  {"pk", PropertyType::Float, Property::IsPrimary{true}},
                              }}};

            schema.begin()->primary_key_property()->type = PropertyType::Mixed;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'mixed' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::Bool;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'bool' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::Float;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'float' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::Double;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'double' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::Object;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'object' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::LinkingObjects;
            REQUIRE_THROWS_CONTAINING(
                schema.validate(), "Property 'object.pk' of type 'linking objects' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::Data;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'data' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::Date;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'date' cannot be made the primary key.");

            schema.begin()->primary_key_property()->type = PropertyType::Decimal;
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Property 'object.pk' of type 'decimal' cannot be made the primary key.");
        }

        SECTION("allows valid primary key types") {
            Schema schema = {{"object",
                              {
                                  {"pk", PropertyType::Int, Property::IsPrimary{true}},
                              }}};

            REQUIRE_NOTHROW(schema.validate());

            schema.begin()->primary_key_property()->type = PropertyType::Int | PropertyType::Nullable;
            REQUIRE_NOTHROW(schema.validate());
            schema.begin()->primary_key_property()->type = PropertyType::String;
            REQUIRE_NOTHROW(schema.validate());
            schema.begin()->primary_key_property()->type = PropertyType::String | PropertyType::Nullable;
            REQUIRE_NOTHROW(schema.validate());
            schema.begin()->primary_key_property()->type = PropertyType::ObjectId;
            REQUIRE_NOTHROW(schema.validate());
            schema.begin()->primary_key_property()->type = PropertyType::ObjectId | PropertyType::Nullable;
            REQUIRE_NOTHROW(schema.validate());
            schema.begin()->primary_key_property()->type = PropertyType::UUID;
            REQUIRE_NOTHROW(schema.validate());
            schema.begin()->primary_key_property()->type = PropertyType::UUID | PropertyType::Nullable;
            REQUIRE_NOTHROW(schema.validate());
        }

        SECTION("rejects nonexistent primary key") {
            Schema schema = {{"object",
                              {
                                  {"value", PropertyType::Int},
                              }}};
            schema.begin()->primary_key = "nonexistent";
            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "Specified primary key 'object.nonexistent' does not exist.");
        }

        SECTION("rejects indexes for types that cannot be indexed") {
            Schema schema = {{"object",
                              {
                                  {"float", PropertyType::Float},
                                  {"double", PropertyType::Double},
                                  {"data", PropertyType::Data},
                                  {"object", PropertyType::Object | PropertyType::Nullable, "object"},
                                  {"array", PropertyType::Array | PropertyType::Object, "object"},
                                  {"set", PropertyType::Set | PropertyType::Int},
                                  {"decimal", PropertyType::Decimal},
                              }}};
            for (auto& prop : schema.begin()->persisted_properties) {
                REQUIRE_NOTHROW(schema.validate());
                prop.is_indexed = true;
                auto expected = util::format("Property 'object.%1' of type '%1' cannot be indexed.", prop.name);
                REQUIRE_THROWS_CONTAINING(schema.validate(), expected);
                prop.is_indexed = false;
            }
        }

        SECTION("allows indexing types that can be indexed") {
            Schema schema = {
                {"object",
                 {
                     {"int", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{true}},
                     {"bool", PropertyType::Bool, Property::IsPrimary{false}, Property::IsIndexed{true}},
                     {"string", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{true}},
                     {"date", PropertyType::Date, Property::IsPrimary{false}, Property::IsIndexed{true}},
                     {"object id", PropertyType::ObjectId, Property::IsPrimary{false}, Property::IsIndexed{true}},
                     {"uuid", PropertyType::UUID, Property::IsPrimary{false}, Property::IsIndexed{true}},
                 }}};
            REQUIRE_NOTHROW(schema.validate());
        }

        SECTION("rejects duplicate types with the same name") {
            Schema schema = {{"object1",
                              {
                                  {"int", PropertyType::Int},
                              }},
                             {"object2",
                              {
                                  {"int", PropertyType::Int},
                              }},
                             {"object3",
                              {
                                  {"int", PropertyType::Int},
                              }},
                             {"object2",
                              {
                                  {"int", PropertyType::Int},
                              }},
                             {"object1",
                              {
                                  {"int", PropertyType::Int},
                              }}};

            REQUIRE_THROWS_CONTAINING(schema.validate(), "- Type 'object1' appears more than once in the schema.\n"
                                                         "- Type 'object2' appears more than once in the schema.");
        }

        SECTION("rejects properties with the same name") {
            Schema schema = {{"object",
                              {
                                  {"child", PropertyType::Object | PropertyType::Nullable, "object"},
                                  {"parent", PropertyType::Int},
                                  {"field1", PropertyType::Int},
                                  {"field2", PropertyType::String},
                                  {"field1", PropertyType::String},
                                  {"field2", PropertyType::String},
                                  {"field1", PropertyType::Int},
                              },
                              {{"parent", PropertyType::Array | PropertyType::LinkingObjects, "object", "child"}}}};

            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "- Property 'field1' appears more than once in the schema for type 'object'.\n"
                                      "- Property 'field2' appears more than once in the schema for type 'object'.\n"
                                      "- Property 'parent' appears more than once in the schema for type 'object'.");
        }

        SECTION("rejects schema if all properties have the same name") {
            Schema schema = {{"object",
                              {
                                  {"field", PropertyType::Int},
                                  {"otherField", PropertyType::Int},
                                  {"field", PropertyType::Int},
                                  {"otherField", PropertyType::Int},
                                  {"field", PropertyType::Int},
                                  {"otherField", PropertyType::Int},
                                  {"field", PropertyType::Int},
                                  {"otherField", PropertyType::Int},
                                  {"field", PropertyType::Int},
                                  {"otherField", PropertyType::Int},
                              }}};

            REQUIRE_THROWS_CONTAINING(
                schema.validate(), "- Property 'field' appears more than once in the schema for type 'object'.\n"
                                   "- Property 'otherField' appears more than once in the schema for type 'object'.");
        }

        SECTION("rejects properties with the same alias") {
            Schema schema = {
                {"object",
                 {
                     {"child", PropertyType::Object | PropertyType::Nullable, "object"},

                     // Alias == Name on computed property
                     {"parentA", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false},
                      "_parent"},

                     // Name == Alias on other property
                     {"fieldA", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}, "_field1"},
                     {"fieldB", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{false},
                      "_field2"},
                     {"fieldC", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{false},
                      "_field1"},
                     {"fieldD", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{false},
                      "_field2"},
                     {"fieldE", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}, "_field1"},

                     // Name == Alias
                     {"fieldF", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}, "fieldF"},
                 },
                 {// Computed property alias == name on persisted property
                  {"parentB", PropertyType::Array | PropertyType::LinkingObjects, "object", "child", "_parent"}}}};

            REQUIRE_THROWS_CONTAINING(schema.validate(),
                                      "- Alias '_field1' appears more than once in the schema for type 'object'.\n"
                                      "- Alias '_field2' appears more than once in the schema for type 'object'.\n"
                                      "- Alias '_parent' appears more than once in the schema for type 'object'.");
        }

        SECTION("rejects properties whose name conflicts with an alias for another property") {
            Schema schema = {
                {"object",
                 {
                     {"child", PropertyType::Object | PropertyType::Nullable, "object"},
                     {"field1", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}, "field2"},
                     {"field2", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{false}, "parent"},
                 },
                 {{"parent", PropertyType::Array | PropertyType::LinkingObjects, "object", "child", "field1"}}}};

            REQUIRE_THROWS_CONTAINING(
                schema.validate(),
                "- Property 'object.parent' has an alias 'field1' that conflicts with a property of the same name.\n"
                "- Property 'object.field1' has an alias 'field2' that conflicts with a property of the same name.\n"
                "- Property 'object.field2' has an alias 'parent' that conflicts with a property of the same name.");
        }
    }

    SECTION("compare()") {
        using namespace schema_change;
        using vec = std::vector<SchemaChange>;
        SECTION("add table") {
            Schema schema1 = {{"object 1",
                               {
                                   {"int", PropertyType::Int},
                               }}};
            Schema schema2 = {{"object 1",
                               {
                                   {"int", PropertyType::Int},
                               }},
                              {"object 2",
                               {
                                   {"int", PropertyType::Int},
                               }}};
            auto obj = &*schema2.find("object 2");
            auto expected = vec{AddTable{obj}, AddInitialProperties{obj}};
            REQUIRE(schema1.compare(schema2) == expected);
        }

        SECTION("add property") {
            Schema schema1 = {{"object",
                               {
                                   {"int 1", PropertyType::Int},
                               }}};
            Schema schema2 = {{"object",
                               {
                                   {"int 1", PropertyType::Int},
                                   {"int 2", PropertyType::Int},
                               }}};
            REQUIRE(schema1.compare(schema2) ==
                    vec{(AddProperty{&*schema1.find("object"), &schema2.find("object")->persisted_properties[1]})});
        }

        SECTION("remove property") {
            Schema schema1 = {{"object",
                               {
                                   {"int 1", PropertyType::Int},
                                   {"int 2", PropertyType::Int},
                               }}};
            Schema schema2 = {{"object",
                               {
                                   {"int 1", PropertyType::Int},
                               }}};
            REQUIRE(
                schema1.compare(schema2) ==
                vec{(RemoveProperty{&*schema1.find("object"), &schema1.find("object")->persisted_properties[1]})});
        }

        SECTION("change property type") {
            Schema schema1 = {{"object",
                               {
                                   {"value", PropertyType::Int},
                               }}};
            Schema schema2 = {{"object",
                               {
                                   {"value", PropertyType::Double},
                               }}};
            REQUIRE(
                schema1.compare(schema2) ==
                vec{(ChangePropertyType{&*schema1.find("object"), &schema1.find("object")->persisted_properties[0],
                                        &schema2.find("object")->persisted_properties[0]})});
        };

        SECTION("change link target") {
            Schema schema1 = {
                {"object",
                 {
                     {"value", PropertyType::Object, "target 1"},
                 }},
                {"target 1",
                 {
                     {"value", PropertyType::Int},
                 }},
                {"target 2",
                 {
                     {"value", PropertyType::Int},
                 }},
            };
            Schema schema2 = {
                {"object",
                 {
                     {"value", PropertyType::Object, "target 2"},
                 }},
                {"target 1",
                 {
                     {"value", PropertyType::Int},
                 }},
                {"target 2",
                 {
                     {"value", PropertyType::Int},
                 }},
            };
            REQUIRE(
                schema1.compare(schema2) ==
                vec{(ChangePropertyType{&*schema1.find("object"), &schema1.find("object")->persisted_properties[0],
                                        &schema2.find("object")->persisted_properties[0]})});
        }

        SECTION("add index") {
            Schema schema1 = {{"object",
                               {
                                   {"int", PropertyType::Int},
                               }}};
            Schema schema2 = {{"object",
                               {
                                   {"int", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{true}},
                               }}};
            auto object_schema = &*schema1.find("object");
            REQUIRE(schema1.compare(schema2) ==
                    vec{(AddIndex{object_schema, &object_schema->persisted_properties[0]})});
        }

        SECTION("remove index") {
            Schema schema1 = {{"object",
                               {
                                   {"int", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{true}},
                               }}};
            Schema schema2 = {{"object",
                               {
                                   {"int", PropertyType::Int},
                               }}};
            auto object_schema = &*schema1.find("object");
            REQUIRE(schema1.compare(schema2) ==
                    vec{(RemoveIndex{object_schema, &object_schema->persisted_properties[0]})});
        }

        SECTION("add index and make nullable") {
            Schema schema1 = {{"object",
                               {
                                   {"int", PropertyType::Int},
                               }}};
            Schema schema2 = {{"object",
                               {
                                   {"int", PropertyType::Int | PropertyType::Nullable, Property::IsPrimary{false},
                                    Property::IsIndexed{true}},
                               }}};
            auto object_schema = &*schema1.find("object");
            REQUIRE(schema1.compare(schema2) ==
                    (vec{MakePropertyNullable{object_schema, &object_schema->persisted_properties[0]},
                         AddIndex{object_schema, &object_schema->persisted_properties[0]}}));
        }

        SECTION("add index and change type") {
            Schema schema1 = {{"object",
                               {
                                   {"value", PropertyType::Int},
                               }}};
            Schema schema2 = {
                {"object",
                 {
                     {"value", PropertyType::Double, Property::IsPrimary{false}, Property::IsIndexed{true}},
                 }}};
            REQUIRE(
                schema1.compare(schema2) ==
                vec{(ChangePropertyType{&*schema1.find("object"), &schema1.find("object")->persisted_properties[0],
                                        &schema2.find("object")->persisted_properties[0]})});
        }

        SECTION("make nullable and change type") {
            Schema schema1 = {{"object",
                               {
                                   {"value", PropertyType::Int},
                               }}};
            Schema schema2 = {{"object",
                               {
                                   {"value", PropertyType::Double | PropertyType::Nullable},
                               }}};
            REQUIRE(
                schema1.compare(schema2) ==
                vec{(ChangePropertyType{&*schema1.find("object"), &schema1.find("object")->persisted_properties[0],
                                        &schema2.find("object")->persisted_properties[0]})});
        }
    }

    SECTION("find in attached schema") {
        Group g;
        TableRef table = g.add_table_with_primary_key("class_table", type_Int, "pk");
        TableRef embedded = g.add_embedded_table("class_embedded");
        ObjectSchema os(g, "table", {});
        REQUIRE(os.table_key == table->get_key());
        ObjectSchema os1(g, "embedded", {});
        REQUIRE(os1.table_key == embedded->get_key());
        REQUIRE(os1.is_embedded);

        Schema schema = {os, os1};
        REQUIRE_NOTHROW(schema.validate());

        SECTION("find by name") {
            auto it = schema.find("table");
            REQUIRE(it != schema.end());
            REQUIRE(it->name == "table");
            REQUIRE(it->table_key == table->get_key());
        }
        SECTION("find by name embedded") {
            auto it = schema.find("embedded");
            REQUIRE(it != schema.end());
            REQUIRE(it->name == "embedded");
            REQUIRE(it->table_key == embedded->get_key());
        }
        SECTION("find non existant name") {
            auto it = schema.find("not_found");
            REQUIRE(it == schema.end());
        }
        SECTION("find empty string") {
            auto it = schema.find("");
            REQUIRE(it == schema.end());
        }
        SECTION("find by key") {
            auto it = schema.find(table->get_key());
            REQUIRE(it != schema.end());
            REQUIRE(it->name == "table");
            REQUIRE(it->table_key == table->get_key());
        }
        SECTION("find embedded by key") {
            auto it = schema.find(embedded->get_key());
            REQUIRE(it != schema.end());
            REQUIRE(it->name == "embedded");
            REQUIRE(it->table_key == embedded->get_key());
        }
        SECTION("find null key") {
            auto null_key = TableKey();
            REQUIRE(!null_key);
            auto it = schema.find(null_key);
            REQUIRE(it == schema.end());
        }
        SECTION("find missing key") {
            auto missing_key = TableKey(42);
            REQUIRE(missing_key);
            auto it = schema.find(missing_key);
            REQUIRE(it == schema.end());
        }
    }
    SECTION("find in unattached schema") {
        Schema schema = {{"object", {{"value", PropertyType::Int}}}};
        REQUIRE_NOTHROW(schema.validate());

        SECTION("find by name works") {
            auto it = schema.find("object");
            REQUIRE(it != schema.end());
            REQUIRE(it->name == "object");
            REQUIRE(!it->table_key);
        }
        SECTION("find missing name") {
            auto it = schema.find("not_a_valid_name");
            REQUIRE(it == schema.end());
        }
        SECTION("find empty name") {
            auto it = schema.find("");
            REQUIRE(it == schema.end());
        }
        SECTION("find by key") {
            std::vector<TableKey> test_keys = {TableKey{0}, TableKey{1}, TableKey{42}, TableKey{}};
            for (auto& key : test_keys) {
                auto it = schema.find(key);
                REQUIRE(it == schema.end());
            }
        }
    }
}
