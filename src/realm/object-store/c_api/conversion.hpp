#ifndef REALM_OBJECT_STORE_C_API_CONVERSION_HPP
#define REALM_OBJECT_STORE_C_API_CONVERSION_HPP

#include <realm/realm.h>

#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/shared_realm.hpp>

#include <realm/string_data.hpp>
#include <realm/binary_data.hpp>
#include <realm/timestamp.hpp>
#include <realm/decimal128.hpp>
#include <realm/object_id.hpp>
#include <realm/mixed.hpp>

#include <string>

namespace realm {

static inline realm_string_t to_capi(StringData data)
{
    return realm_string_t{data.data(), data.size()};
}

static inline realm_string_t to_capi(const std::string& str)
{
    return to_capi(StringData{str});
}

static inline std::string capi_to_std(realm_string_t str)
{
    return std::string{str.data, 0, str.size};
}

static inline StringData from_capi(realm_string_t str)
{
    return StringData{str.data, str.size};
}

static inline realm_binary_t to_capi(BinaryData bin)
{
    return realm_binary_t{reinterpret_cast<const unsigned char*>(bin.data()), bin.size()};
}

static inline BinaryData from_capi(realm_binary_t bin)
{
    return BinaryData{reinterpret_cast<const char*>(bin.data), bin.size};
}

static inline realm_timestamp_t to_capi(Timestamp ts)
{
    return realm_timestamp_t{ts.get_seconds(), ts.get_nanoseconds()};
}

static inline Timestamp from_capi(realm_timestamp_t ts)
{
    return Timestamp{ts.seconds, ts.nanoseconds};
}

static inline realm_decimal128_t to_capi(const Decimal128& dec)
{
    auto raw = dec.raw();
    return realm_decimal128_t{{raw->w[0], raw->w[1]}};
}

static inline Decimal128 from_capi(realm_decimal128_t dec)
{
    return Decimal128{Decimal128::Bid128{{dec.w[0], dec.w[1]}}};
}

static inline realm_object_id_t to_capi(ObjectId)
{
    REALM_TERMINATE("Not implemented yet.");
}

static inline ObjectId from_capi(realm_object_id_t)
{
    REALM_TERMINATE("Not implemented yet.");
}

static inline realm_col_key_t to_capi(ColKey key)
{
    return realm_col_key_t{key.value};
}

static inline ColKey from_capi(realm_col_key_t key)
{
    return ColKey{key.col_key};
}

static inline realm_table_key_t to_capi(TableKey key)
{
    return realm_table_key_t{key.value};
}

static inline TableKey from_capi(realm_table_key_t key)
{
    return TableKey{key.table_key};
}

static inline realm_obj_key_t to_capi(ObjKey key)
{
    return realm_obj_key_t{key.value};
}

static inline ObjKey from_capi(realm_obj_key_t key)
{
    return ObjKey{key.obj_key};
}

static inline Mixed from_capi(realm_value_t val)
{
    switch (val.type) {
        case RLM_TYPE_NULL:
            return Mixed{};
        case RLM_TYPE_INT:
            return Mixed{val.integer};
        case RLM_TYPE_BOOL:
            return Mixed{val.boolean};
        case RLM_TYPE_STRING:
            return Mixed{from_capi(val.string)};
        case RLM_TYPE_BINARY:
            return Mixed{from_capi(val.binary)};
        case RLM_TYPE_TIMESTAMP:
            return Mixed{from_capi(val.timestamp)};
        case RLM_TYPE_FLOAT:
            return Mixed{val.fnum};
        case RLM_TYPE_DOUBLE:
            return Mixed{val.dnum};
        case RLM_TYPE_DECIMAL128:
            return Mixed{from_capi(val.decimal128)};
        case RLM_TYPE_OBJECT_ID:
            return Mixed{from_capi(val.object_id)};
        case RLM_TYPE_LINK:
            return Mixed{ObjLink{from_capi(val.link.target_table), from_capi(val.link.target)}};
    }
    REALM_TERMINATE("Invalid realm_value_t");
}

static inline realm_value_t to_capi(Mixed value)
{
    realm_value_t val;
    if (value.is_null()) {
        val.type = RLM_TYPE_NULL;
    }
    else {
        switch (value.get_type()) {
            case type_Int: {
                val.type = RLM_TYPE_INT;
                val.integer = value.get<int64_t>();
                break;
            }
            case type_Bool: {
                val.type = RLM_TYPE_BOOL;
                val.boolean = value.get<bool>();
                break;
            }
            case type_String: {
                val.type = RLM_TYPE_STRING;
                val.string = to_capi(value.get<StringData>());
                break;
            }
            case type_Binary: {
                val.type = RLM_TYPE_BINARY;
                val.binary = to_capi(value.get<BinaryData>());
                break;
            }
            case type_Timestamp: {
                val.type = RLM_TYPE_TIMESTAMP;
                val.timestamp = to_capi(value.get<Timestamp>());
                break;
            }
            case type_Float: {
                val.type = RLM_TYPE_FLOAT;
                val.fnum = value.get<float>();
                break;
            }
            case type_Double: {
                val.type = RLM_TYPE_DOUBLE;
                val.dnum = value.get<double>();
                break;
            }
            case type_Decimal: {
                val.type = RLM_TYPE_DECIMAL128;
                val.decimal128 = to_capi(value.get<Decimal128>());
                break;
            }
            case type_Link: {
                REALM_TERMINATE("Not implemented yet");
            }
            case type_ObjectId: {
                val.type = RLM_TYPE_OBJECT_ID;
                val.object_id = to_capi(value.get<ObjectId>());
                break;
            }
            case type_TypedLink: {
                REALM_TERMINATE("Not implemented yet");
            }

            case type_LinkList:
                [[fallthrough]];
            case type_Mixed:
                [[fallthrough]];
            case type_OldTable:
                [[fallthrough]];
            case type_OldDateTime:
                [[fallthrough]];
            default:
                REALM_TERMINATE("Invalid Mixed value type");
        }
    }

    return val;
}

static inline SchemaMode from_capi(realm_schema_mode_e mode)
{
    switch (mode) {
        case RLM_SCHEMA_MODE_AUTOMATIC: {
            return SchemaMode::Automatic;
        }
        case RLM_SCHEMA_MODE_IMMUTABLE: {
            return SchemaMode::Immutable;
        }
        case RLM_SCHEMA_MODE_READ_ONLY_ALTERNATIVE: {
            return SchemaMode::ReadOnlyAlternative;
        }
        case RLM_SCHEMA_MODE_RESET_FILE: {
            return SchemaMode::ResetFile;
        }
        case RLM_SCHEMA_MODE_ADDITIVE: {
            return SchemaMode::Additive;
        }
        case RLM_SCHEMA_MODE_MANUAL: {
            return SchemaMode::Manual;
        }
    }
    REALM_TERMINATE("Invalid schema mode.");
}

static inline realm_property_type_e to_capi(PropertyType type) noexcept
{
    type &= ~PropertyType::Flags;

    switch (type) {
        case PropertyType::Int:
            return RLM_PROPERTY_TYPE_INT;
        case PropertyType::Bool:
            return RLM_PROPERTY_TYPE_BOOL;
        case PropertyType::String:
            return RLM_PROPERTY_TYPE_STRING;
        case PropertyType::Data:
            return RLM_PROPERTY_TYPE_BINARY;
        case PropertyType::Any:
            return RLM_PROPERTY_TYPE_ANY;
        case PropertyType::Date:
            return RLM_PROPERTY_TYPE_TIMESTAMP;
        case PropertyType::Float:
            return RLM_PROPERTY_TYPE_FLOAT;
        case PropertyType::Double:
            return RLM_PROPERTY_TYPE_DOUBLE;
        case PropertyType::Decimal:
            return RLM_PROPERTY_TYPE_DECIMAL128;
        case PropertyType::Object:
            return RLM_PROPERTY_TYPE_OBJECT;
        case PropertyType::LinkingObjects:
            return RLM_PROPERTY_TYPE_LINKING_OBJECTS;
        case PropertyType::ObjectId:
            return RLM_PROPERTY_TYPE_OBJECT_ID;
        case PropertyType::Nullable:
            [[fallthrough]];
        case PropertyType::Flags:
            [[fallthrough]];
        case PropertyType::Array:
            REALM_UNREACHABLE();
    }
    REALM_TERMINATE("Unsupported property type");
}

static inline PropertyType from_capi(realm_property_type_e type) noexcept
{
    switch (type) {
        case RLM_PROPERTY_TYPE_INT:
            return PropertyType::Int;
        case RLM_PROPERTY_TYPE_BOOL:
            return PropertyType::Bool;
        case RLM_PROPERTY_TYPE_STRING:
            return PropertyType::String;
        case RLM_PROPERTY_TYPE_BINARY:
            return PropertyType::Data;
        case RLM_PROPERTY_TYPE_ANY:
            return PropertyType::Any;
        case RLM_PROPERTY_TYPE_TIMESTAMP:
            return PropertyType::Date;
        case RLM_PROPERTY_TYPE_FLOAT:
            return PropertyType::Float;
        case RLM_PROPERTY_TYPE_DOUBLE:
            return PropertyType::Double;
        case RLM_PROPERTY_TYPE_DECIMAL128:
            return PropertyType::Decimal;
        case RLM_PROPERTY_TYPE_OBJECT:
            return PropertyType::Object;
        case RLM_PROPERTY_TYPE_LINKING_OBJECTS:
            return PropertyType::LinkingObjects;
        case RLM_PROPERTY_TYPE_OBJECT_ID:
            return PropertyType::ObjectId;
    }
    REALM_TERMINATE("Unsupported property type");
}


static inline Property from_capi(const realm_property_info_t& p) noexcept
{
    Property prop;
    prop.name = capi_to_std(p.name);
    prop.public_name = capi_to_std(p.public_name);
    prop.type = from_capi(p.type);
    prop.object_type = capi_to_std(p.link_target);
    prop.link_origin_property_name = capi_to_std(p.link_origin_property_name);
    prop.is_primary = Property::IsPrimary{bool(p.flags & RLM_PROPERTY_PRIMARY_KEY)};
    prop.is_indexed = Property::IsIndexed{bool(p.flags & RLM_PROPERTY_INDEXED)};

    if (bool(p.flags & RLM_PROPERTY_NULLABLE)) {
        prop.type |= PropertyType::Nullable;
    }
    switch (p.collection_type) {
        case RLM_COLLECTION_TYPE_NONE:
            break;
        case RLM_COLLECTION_TYPE_LIST: {
            prop.type |= PropertyType::Array;
            break;
        }
        case RLM_COLLECTION_TYPE_SET:
            [[fallthrough]];
        case RLM_COLLECTION_TYPE_DICTIONARY:
            REALM_TERMINATE("Not implemented yet.");
    }
    return prop;
}

static inline realm_property_info_t to_capi(const Property& prop) noexcept
{
    realm_property_info_t p;
    p.name = to_capi(prop.name);
    p.public_name = to_capi(prop.public_name);
    p.type = to_capi(prop.type & ~PropertyType::Flags);
    p.link_target = to_capi(prop.object_type);
    p.link_origin_property_name = to_capi(prop.link_origin_property_name);

    p.flags = RLM_PROPERTY_NORMAL;
    if (prop.is_indexed)
        p.flags |= RLM_PROPERTY_INDEXED;
    if (prop.is_primary)
        p.flags |= RLM_PROPERTY_PRIMARY_KEY;
    if (bool(prop.type & PropertyType::Nullable))
        p.flags |= RLM_PROPERTY_NULLABLE;

    p.collection_type = RLM_COLLECTION_TYPE_NONE;
    if (bool(prop.type & PropertyType::Array))
        p.collection_type = RLM_COLLECTION_TYPE_LIST;

    p.key = to_capi(prop.column_key);

    return p;
}

static inline realm_class_info_t to_capi(const ObjectSchema& o)
{
    realm_class_info_t info;
    info.name = to_capi(o.name);
    info.primary_key = to_capi(o.primary_key);
    info.num_properties = o.persisted_properties.size();
    info.num_computed_properties = o.computed_properties.size();
    info.key = to_capi(o.table_key);
    if (o.is_embedded) {
        info.flags = RLM_CLASS_EMBEDDED;
    }
    else {
        info.flags = RLM_CLASS_NORMAL;
    }
    return info;
}

} // namespace realm


#endif // REALM_OBJECT_STORE_C_API_CONVERSION_HPP