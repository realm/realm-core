
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

#ifndef REALM_OS_OBJECT_ACCESSOR_HPP
#define REALM_OS_OBJECT_ACCESSOR_HPP

#include "object.hpp"

#include "list.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "util/format.hpp"

#include <realm/link_view.hpp>
#include <realm/util/assert.hpp>
#include <realm/table_view.hpp>

#include <string>

namespace realm {
//
// template method implementations
//
template <typename ValueType, typename ContextType>
void Object::set_property_value(ContextType& ctx, std::string prop_name, ValueType value, bool try_update)
{
    verify_attached();
    m_realm->verify_in_write();
    auto& property = property_for_name(prop_name);
    if (property.is_primary)
        throw std::logic_error("Cannot modify primary key after creation");

    set_property_value_impl(ctx, property, value, try_update);
}

template <typename ValueType, typename ContextType>
ValueType Object::get_property_value(ContextType& ctx, std::string prop_name)
{
    return get_property_value_impl<ValueType>(ctx, property_for_name(prop_name));
}

template <typename ValueType, typename ContextType>
void Object::set_property_value_impl(ContextType& ctx, const Property &property, ValueType value, bool try_update, bool is_default)
{
    auto& table = *m_row.get_table();
    size_t column = property.table_column;
    size_t row = m_row.get_index();
    if (property.is_nullable && ctx.is_null(value)) {
        if (property.type == PropertyType::Object) {
            if (!is_default)
                table.nullify_link(column, row);
        }
        else {
            table.set_null(column, row, is_default);
        }
        return;
    }

    switch (property.type) {
        case PropertyType::Bool:
            table.set_bool(column, row, ctx.to_bool(value), is_default);
            break;
        case PropertyType::Int:
            table.set_int(column, row, ctx.to_long(value), is_default);
            break;
        case PropertyType::Float:
            table.set_float(column, row, ctx.to_float(value), is_default);
            break;
        case PropertyType::Double:
            table.set_double(column, row, ctx.to_double(value), is_default);
            break;
        case PropertyType::String: {
            auto str = ctx.to_string(value);
            table.set_string(column, row, str, is_default);
            break;
        }
        case PropertyType::Data: {
            auto data = ctx.to_binary(value);
            table.set_binary(column, row, BinaryData(data), is_default);
            break;
        }
        case PropertyType::Any:
            table.set_mixed(column, row, ctx.to_mixed(value), is_default);
            break;
        case PropertyType::Date:
            table.set_timestamp(column, row, ctx.to_timestamp(value), is_default);
            break;
        case PropertyType::Object: {
            table.set_link(column, row, ctx.to_object_index(m_realm, value, property.object_type, try_update), is_default);
            break;
        }
        case PropertyType::Array: {
            LinkViewRef link_view = m_row.get_linklist(column);
            link_view->clear();
            if (!ctx.is_null(value)) {
                size_t count = ctx.list_size(value);
                for (size_t i = 0; i < count; i++) {
                    ValueType element = ctx.list_value_at_index(value, i);
                    link_view->add(ctx.to_object_index(m_realm, element, property.object_type, try_update));
                }
            }
            break;
        }
        case PropertyType::LinkingObjects:
            throw ReadOnlyPropertyException(m_object_schema->name, property.name);
    }
}

template <typename ValueType, typename ContextType>
ValueType Object::get_property_value_impl(ContextType& ctx, const Property &property)
{
    verify_attached();

    size_t column = property.table_column;
    if (property.is_nullable && m_row.is_null(column)) {
        return ctx.null_value();
    }

    switch (property.type) {
        case PropertyType::Bool:
            return ctx.from_bool(m_row.get_bool(column));
        case PropertyType::Int:
            return ctx.from_long(m_row.get_int(column));
        case PropertyType::Float:
            return ctx.from_float(m_row.get_float(column));
        case PropertyType::Double:
            return ctx.from_double(m_row.get_double(column));
        case PropertyType::String:
            return ctx.from_string(m_row.get_string(column));
        case PropertyType::Data:
            return ctx.from_binary(m_row.get_binary(column));
        case PropertyType::Any:
            throw "Any not supported";
        case PropertyType::Date:
            return ctx.from_timestamp(m_row.get_timestamp(column));
        case PropertyType::Object: {
            auto linkObjectSchema = m_realm->schema().find(property.object_type);
            TableRef table = ObjectStore::table_for_object_type(m_realm->read_group(), linkObjectSchema->name);
            return ctx.from_object(Object(m_realm, *linkObjectSchema, table->get(m_row.get_link(column))));
        }
        case PropertyType::Array:
            return ctx.from_list(List(m_realm, m_row.get_linklist(column)));
        case PropertyType::LinkingObjects: {
            auto target_object_schema = m_realm->schema().find(property.object_type);
            auto link_property = target_object_schema->property_for_name(property.link_origin_property_name);
            TableRef table = ObjectStore::table_for_object_type(m_realm->read_group(), target_object_schema->name);
            auto tv = m_row.get_table()->get_backlink_view(m_row.get_index(), table.get(), link_property->table_column);
            return ctx.from_results(Results(m_realm, std::move(tv)));
        }
    }
    REALM_UNREACHABLE();
}

template<typename ValueType, typename ContextType>
Object Object::create(ContextType& ctx, SharedRealm realm, const ObjectSchema &object_schema, ValueType value, bool try_update)
{
    realm->verify_in_write();

    // get or create our accessor
    bool created = false;

    // try to get existing row if updating
    size_t row_index = realm::not_found;
    realm::TableRef table = ObjectStore::table_for_object_type(realm->read_group(), object_schema.name);

    if (auto primary_prop = object_schema.primary_key_property()) {
        // search for existing object based on primary key type
        ValueType primary_value = ctx.dict_value_for_key(value, object_schema.primary_key);
        row_index = get_for_primary_key_impl(ctx, *table, *primary_prop, primary_value);

        if (row_index == realm::not_found) {
            row_index = table->add_empty_row();
            created = true;
            if (primary_prop->type == PropertyType::Int)
                table->set_int_unique(primary_prop->table_column, row_index, ctx.to_long(primary_value));
            else if (primary_prop->type == PropertyType::String) {
                auto value = ctx.to_string(primary_value);
                table->set_string_unique(primary_prop->table_column, row_index, value);
            }
            else
                REALM_UNREACHABLE();
        }
        else if (!try_update) {
            throw std::logic_error(util::format("Attempting to create an object of type '%1' with an existing primary key value.", object_schema.name));
        }
    }
    else {
        row_index = table->add_empty_row();
        created = true;
    }

    // populate
    Object object(realm, object_schema, table->get(row_index));
    for (const Property& prop : object_schema.persisted_properties) {
        if (prop.is_primary)
            continue;

        if (ctx.dict_has_value_for_key(value, prop.name)) {
            object.set_property_value_impl(ctx, prop, ctx.dict_value_for_key(value, prop.name), try_update);
        }
        else if (created) {
            if (ctx.has_default_value_for_property(realm.get(), object_schema, prop.name)) {
                object.set_property_value_impl(ctx, prop, ctx.default_value_for_property(realm.get(), object_schema, prop.name), try_update, true);
            }
            else if (prop.is_nullable || prop.type == PropertyType::Array) {
                object.set_property_value_impl(ctx, prop, ctx.null_value(), try_update);
            }
            else {
                throw MissingPropertyValueException(object_schema.name, prop.name);
            }
        }
    }
    return object;
}

template<typename ValueType, typename ContextType>
Object Object::get_for_primary_key(ContextType& ctx, SharedRealm realm,
                                   const ObjectSchema &object_schema, ValueType primary_value)
{
    auto primary_prop = object_schema.primary_key_property();
    if (!primary_prop) {
        throw MissingPrimaryKeyException(object_schema.name);
    }

    auto table = ObjectStore::table_for_object_type(realm->read_group(), object_schema.name);
    auto row_index = get_for_primary_key_impl(ctx, *table, *primary_prop, primary_value);

    return Object(realm, object_schema, row_index == realm::not_found ? Row() : table->get(row_index));
}

template<typename ValueType, typename ContextType>
size_t Object::get_for_primary_key_impl(ContextType& ctx, Table const& table,
                                        const Property &primary_prop, ValueType primary_value) {
    if (primary_prop.type == PropertyType::String) {
        auto primary_string = ctx.to_string(primary_value);
        return table.find_first_string(primary_prop.table_column, primary_string);
    }
    else {
        return table.find_first_int(primary_prop.table_column, ctx.to_long(primary_value));
    }
}

//
// List implementation
//
template<typename ValueType, typename ContextType>
void List::add(ContextType& ctx, ValueType value)
{
    add(ctx.to_object_index(m_realm, value, get_object_schema().name, false));
}

template<typename ValueType, typename ContextType>
void List::insert(ContextType& ctx, ValueType value, size_t list_ndx)
{
    insert(list_ndx, ctx.to_object_index(m_realm, value, get_object_schema().name, false));
}

template<typename ValueType, typename ContextType>
void List::set(ContextType& ctx, ValueType value, size_t list_ndx)
{
    set(list_ndx, ctx.to_object_index(m_realm, value, get_object_schema().name, false));
}
} // namespace realm

#endif /* defined(REALM_OS_OBJECT_ACCESSOR_HPP) */
