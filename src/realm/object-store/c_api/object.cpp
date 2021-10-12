#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

#include <realm/util/overload.hpp>

namespace realm::c_api {

RLM_API bool realm_get_num_objects(const realm_t* realm, realm_class_key_t key, size_t* out_count)
{
    return wrap_err([&]() {
        auto& rlm = **realm;
        auto table = rlm.read_group().get_table(TableKey(key));
        if (out_count)
            *out_count = table->size();
        return true;
    });
}

RLM_API realm_object_t* realm_get_object(const realm_t* realm, realm_class_key_t tbl_key, realm_object_key_t obj_key)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto table_key = TableKey(tbl_key);
        auto table = shared_realm->read_group().get_table(table_key);
        auto obj = table->get_object(ObjKey(obj_key));
        auto object = Object{shared_realm, std::move(obj)};
        return new realm_object_t{std::move(object)};
    });
}

RLM_API realm_object_t* realm_object_find_with_primary_key(const realm_t* realm, realm_class_key_t class_key,
                                                           realm_value_t pk, bool* out_found)
{
    return wrap_err([&]() -> realm_object_t* {
        auto& shared_realm = *realm;
        auto table_key = TableKey(class_key);
        auto table = shared_realm->read_group().get_table(table_key);
        auto pk_val = from_capi(pk);

        auto pk_col = table->get_primary_key_column();
        if (pk_val.is_null() && !pk_col.is_nullable()) {
            if (out_found)
                *out_found = false;
            return nullptr;
        }
        if (!pk_val.is_null() && ColumnType(pk_val.get_type()) != pk_col.get_type() &&
            pk_col.get_type() != col_type_Mixed) {
            if (out_found)
                *out_found = false;
            return nullptr;
        }

        auto obj_key = table->find_primary_key(pk_val);
        if (obj_key) {
            if (out_found)
                *out_found = true;
            auto obj = table->get_object(obj_key);
            return new realm_object_t{Object{shared_realm, std::move(obj)}};
        }
        else {
            if (out_found)
                *out_found = false;
            return static_cast<realm_object_t*>(nullptr);
        }
    });
}

RLM_API realm_results_t* realm_object_find_all(const realm_t* realm, realm_class_key_t key)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto table = shared_realm->read_group().get_table(TableKey(key));
        return new realm_results{Results{shared_realm, table}};
    });
}

RLM_API realm_object_t* realm_object_create(realm_t* realm, realm_class_key_t table_key)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto tblkey = TableKey(table_key);
        auto table = shared_realm->read_group().get_table(tblkey);

        if (table->get_primary_key_column()) {
            auto& object_schema = schema_for_table(*realm, tblkey);
            throw MissingPrimaryKeyException{object_schema.name};
        }

        auto obj = table->create_object();
        auto object = Object{shared_realm, std::move(obj)};
        return new realm_object_t{std::move(object)};
    });
}

RLM_API realm_object_t* realm_object_create_with_primary_key(realm_t* realm, realm_class_key_t table_key,
                                                             realm_value_t pk)
{
    bool did_create;
    realm_object_t* object = realm_object_get_or_create_with_primary_key(realm, table_key, pk, &did_create);
    if (object && !did_create) {
        delete object;
        object = wrap_err([&]() {
            throw DuplicatePrimaryKeyException("Object with this primary key already exists");
            return nullptr;
        });
    }
    return object;
}

RLM_API realm_object_t* realm_object_get_or_create_with_primary_key(realm_t* realm, realm_class_key_t table_key,
                                                                    realm_value_t pk, bool* did_create)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto tblkey = TableKey(table_key);
        auto table = shared_realm->read_group().get_table(tblkey);
        auto pkval = from_capi(pk);
        if (did_create)
            *did_create = false;

        ColKey pkcol = table->get_primary_key_column();
        if (!pkcol) {
            throw UnexpectedPrimaryKeyException("Class does not have a primary key");
        }

        if (pkval.is_null() && !pkcol.is_nullable()) {
            auto& schema = schema_for_table(*realm, tblkey);
            throw NotNullableException{schema.name, schema.primary_key};
        }

        if (!pkval.is_null() && pkval.get_type() != DataType(pkcol.get_type())) {
            auto& schema = schema_for_table(*realm, tblkey);
            throw WrongPrimaryKeyTypeException{schema.name};
        }

        auto obj = table->create_object_with_primary_key(pkval, did_create);
        auto object = Object{shared_realm, std::move(obj)};
        return new realm_object_t{std::move(object)};
    });
}

RLM_API bool realm_object_delete(realm_object_t* obj)
{
    return wrap_err([&]() {
        obj->verify_attached();
        obj->obj().remove();
        return true;
    });
}

RLM_API realm_object_t* _realm_object_from_native_copy(const void* pobj, size_t n)
{
    REALM_ASSERT_RELEASE(n == sizeof(Object));

    return wrap_err([&]() {
        auto pobject = static_cast<const Object*>(pobj);
        return new realm_object_t{*pobject};
    });
}

RLM_API realm_object_t* _realm_object_from_native_move(void* pobj, size_t n)
{
    REALM_ASSERT_RELEASE(n == sizeof(Object));

    return wrap_err([&]() {
        auto pobject = static_cast<Object*>(pobj);
        return new realm_object_t{std::move(*pobject)};
    });
}

RLM_API const void* _realm_object_get_native_ptr(realm_object_t* obj)
{
    return static_cast<const Object*>(obj);
}

RLM_API bool realm_object_resolve_in(const realm_object_t* from_object, const realm_t* target_realm,
                                     realm_object_t** resolved)
{
    return wrap_err([&]() {
        try {
            const auto& realm = *target_realm;
            auto new_obj = from_object->freeze(realm);
            // clients of the C-API adhere to a different error handling strategy than Core.
            // Core represents lack of resolution as a new object which is invalid.
            // But clients of the C-API instead wants NO object to be produced.
            if (new_obj.is_valid()) {
                *resolved = new realm_object_t{std::move(new_obj)};
            }
            else {
                *resolved = nullptr;
            }
            return true;
        }
        catch (NoSuchTable&) {
            *resolved = nullptr;
            return true;
        }
        catch (KeyNotFound&) {
            *resolved = nullptr;
            return true;
        }
    });
}

RLM_API bool realm_object_is_valid(const realm_object_t* obj)
{
    return obj->is_valid();
}

RLM_API realm_object_key_t realm_object_get_key(const realm_object_t* obj)
{
    return obj->obj().get_key().value;
}

RLM_API realm_class_key_t realm_object_get_table(const realm_object_t* obj)
{
    return obj->obj().get_table()->get_key().value;
}

RLM_API realm_link_t realm_object_as_link(const realm_object_t* object)
{
    auto obj = object->obj();
    auto table = obj.get_table();
    auto table_key = table->get_key();
    auto obj_key = obj.get_key();
    return realm_link_t{table_key.value, obj_key.value};
}

RLM_API realm_object_t* realm_object_from_thread_safe_reference(const realm_t* realm,
                                                                realm_thread_safe_reference_t* tsr)
{
    return wrap_err([&]() {
        auto otsr = dynamic_cast<realm_object::thread_safe_reference*>(tsr);
        if (!otsr) {
            throw std::logic_error{"Thread safe reference type mismatch"};
        }

        auto obj = otsr->resolve<Object>(*realm);
        return new realm_object_t{std::move(obj)};
    });
}

RLM_API bool realm_get_value(const realm_object_t* obj, realm_property_key_t col, realm_value_t* out_value)
{
    return realm_get_values(obj, 1, &col, out_value);
}

RLM_API bool realm_get_values(const realm_object_t* obj, size_t num_values, const realm_property_key_t* properties,
                              realm_value_t* out_values)
{
    return wrap_err([&]() {
        obj->verify_attached();

        auto o = obj->obj();

        for (size_t i = 0; i < num_values; ++i) {
            auto col_key = ColKey(properties[i]);

            if (col_key.is_collection()) {
                auto table = o.get_table();
                auto& schema = schema_for_table(obj->get_realm(), table->get_key());
                throw PropertyTypeMismatch{schema.name, table->get_column_name(col_key)};
            }

            auto val = o.get_any(col_key);
            if (out_values) {
                auto converted = objkey_to_typed_link(val, col_key, *o.get_table());
                out_values[i] = to_capi(converted);
            }
        }

        return true;
    });
}

RLM_API bool realm_set_value(realm_object_t* obj, realm_property_key_t col, realm_value_t new_value, bool is_default)
{
    return realm_set_values(obj, 1, &col, &new_value, is_default);
}

RLM_API bool realm_set_values(realm_object_t* obj, size_t num_values, const realm_property_key_t* properties,
                              const realm_value_t* values, bool is_default)
{
    return wrap_err([&]() {
        obj->verify_attached();
        auto o = obj->obj();
        auto table = o.get_table();

        // Perform validation up front to avoid partial updates. This is
        // unlikely to incur performance overhead because the object itself is
        // not accessed here, just the bits of the column key and the input type.

        for (size_t i = 0; i < num_values; ++i) {
            auto col_key = ColKey(properties[i]);
            table->report_invalid_key(col_key);

            if (col_key.is_collection()) {
                auto& schema = schema_for_table(obj->get_realm(), table->get_key());
                throw PropertyTypeMismatch{schema.name, table->get_column_name(col_key)};
            }

            auto val = from_capi(values[i]);
            check_value_assignable(obj->get_realm(), *table, col_key, val);
        }

        // Actually write the properties.

        for (size_t i = 0; i < num_values; ++i) {
            auto col_key = ColKey(properties[i]);
            auto val = from_capi(values[i]);
            val = typed_link_to_objkey(val, col_key);
            o.set_any(col_key, val, is_default);
        }

        return true;
    });
}

RLM_API realm_list_t* realm_get_list(realm_object_t* object, realm_property_key_t key)
{
    return wrap_err([&]() {
        object->verify_attached();

        auto obj = object->obj();
        auto table = obj.get_table();

        auto col_key = ColKey(key);
        table->report_invalid_key(col_key);

        if (!col_key.is_list()) {
            auto table = obj.get_table();
            auto& schema = schema_for_table(object->get_realm(), table->get_key());
            throw PropertyTypeMismatch{schema.name, table->get_column_name(col_key)};
        }

        return new realm_list_t{List{object->get_realm(), std::move(obj), col_key}};
    });
}

RLM_API bool realm_list_size(const realm_list_t* list, size_t* out_size)
{
    return wrap_err([&]() {
        size_t size = list->size();
        if (out_size)
            *out_size = size;
        return true;
    });
}

RLM_API bool realm_list_get_property(const realm_list_t* list, realm_property_info_t* out_property_info)
{
    static_cast<void>(list);
    static_cast<void>(out_property_info);
    REALM_TERMINATE("Not implemented yet.");
}

RLM_API bool realm_list_get(const realm_list_t* list, size_t index, realm_value_t* out_value)
{
    return wrap_err([&]() {
        list->verify_attached();
        realm_value_t result{};

        auto getter = util::overload{
            [&](Obj*) {
                Obj o = list->get<Obj>(index);
                result.type = RLM_TYPE_LINK;
                result.link.target_table = o.get_table()->get_key().value;
                result.link.target = o.get_key().value;
            },
            [&](util::Optional<Obj>*) {
                REALM_TERMINATE("Nullable link lists not supported");
            },
            [&](auto p) {
                using T = std::remove_cv_t<std::remove_pointer_t<decltype(p)>>;
                Mixed mixed{list->get<T>(index)};
                result = to_capi(mixed);
            },
        };

        switch_on_type(list->get_type(), getter);

        if (out_value)
            *out_value = result;
        return true;
    });
}

RLM_API bool realm_list_insert(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        check_value_assignable(*list, val);

        auto col_key = list->get_parent_column_key();
        val = typed_link_to_objkey(val, col_key);

        list->insert_any(index, val);
        return true;
    });
}

RLM_API bool realm_list_set(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        check_value_assignable(*list, val);

        auto col_key = list->get_parent_column_key();
        val = typed_link_to_objkey(val, col_key);

        list->set_any(index, val);
        return true;
    });
}

RLM_API bool realm_list_erase(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->remove(index);
        return true;
    });
}

RLM_API bool realm_list_clear(realm_list_t* list)
{
    return wrap_err([&]() {
        // Note: Confusing naming.
        list->remove_all();
        return true;
    });
}

RLM_API bool realm_list_remove_all(realm_list_t* list)
{
    return wrap_err([&]() {
        // Note: Confusing naming.
        list->delete_all();
        return true;
    });
}

RLM_API realm_list_t* realm_list_from_thread_safe_reference(const realm_t* realm, realm_thread_safe_reference_t* tsr)
{
    return wrap_err([&]() {
        auto ltsr = dynamic_cast<realm_list::thread_safe_reference*>(tsr);
        if (!ltsr) {
            throw std::logic_error{"Thread safe reference type mismatch"};
        }

        auto list = ltsr->resolve<List>(*realm);
        return new realm_list_t{std::move(list)};
    });
}

RLM_API bool realm_list_resolve_in(const realm_list_t* from_list, const realm_t* target_realm,
                                   realm_list_t** resolved)
{
    return wrap_err([&]() {
        try {
            const auto& realm = *target_realm;
            auto frozen_list = from_list->freeze(realm);
            if (frozen_list.is_valid()) {
                *resolved = new realm_list_t{std::move(frozen_list)};
            }
            else {
                *resolved = nullptr;
            }
            return true;
        }
        catch (NoSuchTable&) {
            *resolved = nullptr;
            return true;
        }
        catch (KeyNotFound&) {
            *resolved = nullptr;
            return true;
        }
    });
}

RLM_API bool realm_list_is_valid(const realm_list_t* list)
{
    if (!list)
        return false;
    return list->is_valid();
}

} // namespace realm::c_api
