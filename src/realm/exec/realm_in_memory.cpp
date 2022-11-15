/*
 * realm_in_memory.cpp
 *
 *  Created on: Nov 14, 2022
 *      Author: jed
 */

#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/property.hpp>

using namespace realm;


int main()
{
    RealmConfig config;
    config.schema = {
        {"person",
         {
             {"id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"age", PropertyType::Int},
             {"pet", PropertyType::Object | PropertyType::Nullable, "dog"},
         }},
        {"dog",
         {
             {"id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"name", PropertyType::String},
         }},
    };
    config.schema_version = 1;

    auto r = Realm::get_shared_realm(config);
    auto object_schema = r->schema().find("person");
    auto table = r->read_group().get_table(object_schema->table_key);
    r->begin_transaction();
    table->create_object_with_primary_key(ObjectId::gen());
    r->commit_transaction();
}
