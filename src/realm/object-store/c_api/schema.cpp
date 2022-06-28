#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>
#include "realm.hpp"

namespace realm::c_api {

RLM_API realm_schema_t* realm_schema_new(const realm_class_info_t* classes, size_t num_classes,
                                         const realm_property_info** class_properties)
{
    return wrap_err([&]() {
        std::vector<ObjectSchema> object_schemas;
        object_schemas.reserve(num_classes);

        for (size_t i = 0; i < num_classes; ++i) {
            const auto& class_info = classes[i];
            auto props_ptr = class_properties[i];
            auto computed_props_ptr = props_ptr + class_info.num_properties;

            ObjectSchema object_schema;
            object_schema.name = class_info.name;
            object_schema.primary_key = class_info.primary_key;
            object_schema.table_type = static_cast<ObjectSchema::ObjectType>(class_info.flags & RLM_CLASS_MASK);
            object_schema.persisted_properties.reserve(class_info.num_properties);
            object_schema.computed_properties.reserve(class_info.num_computed_properties);

            for (size_t j = 0; j < class_info.num_properties; ++j) {
                Property prop = from_capi(props_ptr[j]);
                object_schema.persisted_properties.push_back(std::move(prop));
            }

            for (size_t j = 0; j < class_info.num_computed_properties; ++j) {
                Property prop = from_capi(computed_props_ptr[j]);
                object_schema.computed_properties.push_back(std::move(prop));
            }

            object_schemas.push_back(std::move(object_schema));
        }

        auto schema = new realm_schema(std::make_unique<Schema>(std::move(object_schemas)));
        return schema;
    });
}

RLM_API realm_schema_t* realm_get_schema(const realm_t* realm)
{
    return wrap_err([&]() {
        auto& rlm = *realm;
        return new realm_schema_t{&rlm->schema()};
    });
}

RLM_API uint64_t realm_get_schema_version(const realm_t* realm)
{
    auto& rlm = *realm;
    return rlm->schema_version();
}

RLM_API bool realm_schema_validate(const realm_schema_t* schema, uint64_t validation_mode)
{
    return wrap_err([&]() {
        schema->ptr->validate(validation_mode);
        return true;
    });
}

RLM_API bool realm_update_schema(realm_t* realm, const realm_schema_t* schema)
{
    return wrap_err([&]() {
        realm->get()->update_schema(*schema->ptr);
        return true;
    });
}

RLM_API bool realm_schema_rename_property(realm_t* realm, realm_schema_t* schema, const char* object_type,
                                          const char* old_name, const char* new_name)
{
    return wrap_err([&]() {
        realm->get()->rename_property(*schema->ptr, object_type, old_name, new_name);
        return true;
    });
}

RLM_API size_t realm_get_num_classes(const realm_t* realm)
{
    size_t max = std::numeric_limits<size_t>::max();
    size_t n = 0;
    auto success = realm_get_class_keys(realm, nullptr, max, &n);
    REALM_ASSERT(success);
    return n;
}

RLM_API bool realm_get_class_keys(const realm_t* realm, realm_class_key_t* out_keys, size_t max, size_t* out_n)
{
    return wrap_err([&]() {
        const auto& shared_realm = **realm;
        const auto& schema = shared_realm.schema();
        set_out_param(out_n, schema.size());

        if (out_keys && max >= schema.size()) {
            size_t i = 0;
            for (auto& os : schema) {
                out_keys[i++] = os.table_key.value;
            }
        }
        return true;
    });
}

RLM_API bool realm_find_class(const realm_t* realm, const char* name, bool* out_found,
                              realm_class_info_t* out_class_info)
{
    return wrap_err([&]() {
        const auto& schema = (*realm)->schema();
        auto it = schema.find(name);
        if (it != schema.end()) {
            if (out_found)
                *out_found = true;
            if (out_class_info)
                *out_class_info = to_capi(*it);
        }
        else {
            if (out_found)
                *out_found = false;
        }
        return true;
    });
}

RLM_API bool realm_get_class(const realm_t* realm, realm_class_key_t key, realm_class_info_t* out_class_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(*realm, TableKey(key));
        *out_class_info = to_capi(os);
        return true;
    });
}

RLM_API bool realm_get_class_properties(const realm_t* realm, realm_class_key_t key,
                                        realm_property_info_t* out_properties, size_t max, size_t* out_n)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(*realm, TableKey(key));
        const size_t prop_size = os.persisted_properties.size() + os.computed_properties.size();
        set_out_param(out_n, prop_size);

        if (out_properties && max >= prop_size) {
            size_t i = 0;
            for (auto& prop : os.persisted_properties) {
                out_properties[i++] = to_capi(prop);
            }
            for (auto& prop : os.computed_properties) {
                out_properties[i++] = to_capi(prop);
            }
        }
        return true;
    });
}

RLM_API bool realm_get_property_keys(const realm_t* realm, realm_class_key_t key, realm_property_key_t* out_keys,
                                     size_t max, size_t* out_n)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(*realm, TableKey(key));
        const size_t prop_size = os.persisted_properties.size() + os.computed_properties.size();
        set_out_param(out_n, prop_size);
        if (out_keys && max >= prop_size) {
            size_t i = 0;
            for (auto& prop : os.persisted_properties) {
                out_keys[i++] = prop.column_key.value;
            }
            for (auto& prop : os.computed_properties) {
                out_keys[i++] = prop.column_key.value;
            }
        }
        return true;
    });
}

RLM_API bool realm_get_property(const realm_t* realm, realm_class_key_t class_key, realm_property_key_t key,
                                realm_property_info_t* out_property_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(*realm, TableKey(class_key));
        auto col_key = ColKey(key);

        // FIXME: We can do better than linear search.

        for (auto& prop : os.persisted_properties) {
            if (prop.column_key == col_key) {
                *out_property_info = to_capi(prop);
                return true;
            }
        }

        for (auto& prop : os.computed_properties) {
            if (prop.column_key == col_key) {
                *out_property_info = to_capi(prop);
                return true;
            }
        }

        throw InvalidPropertyKeyException{"Invalid property key for this class"};
    });
}

RLM_API bool realm_find_property(const realm_t* realm, realm_class_key_t class_key, const char* name, bool* out_found,
                                 realm_property_info_t* out_property_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(*realm, TableKey(class_key));
        auto prop = os.property_for_name(name);

        if (prop) {
            if (out_found)
                *out_found = true;
            if (out_property_info)
                *out_property_info = to_capi(*prop);
        }
        else {
            if (out_found)
                *out_found = false;
        }

        return true;
    });
}

RLM_API bool realm_find_property_by_public_name(const realm_t* realm, realm_class_key_t class_key,
                                                const char* public_name, bool* out_found,
                                                realm_property_info_t* out_property_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(*realm, TableKey(class_key));
        auto prop = os.property_for_public_name(public_name);

        if (prop) {
            if (out_found)
                *out_found = true;
            if (out_property_info)
                *out_property_info = to_capi(*prop);
        }
        else {
            if (out_found)
                *out_found = false;
        }

        return true;
    });
}

RLM_API realm_callback_token_t* realm_add_schema_changed_callback(realm_t* realm,
                                                                  realm_on_schema_change_func_t callback,
                                                                  realm_userdata_t userdata,
                                                                  realm_free_userdata_func_t free_userdata)
{
    util::UniqueFunction<void(const Schema&)> func =
        [callback, userdata = UserdataPtr{userdata, free_userdata}](const Schema& schema) {
            auto c_schema = new realm_schema_t(&schema);
            callback(userdata.get(), c_schema);
            realm_release(c_schema);
        };
    return new realm_callback_token_schema(
        realm, CBindingContext::get(*realm).schema_changed_callbacks().add(std::move(func)));
}

} // namespace realm::c_api
