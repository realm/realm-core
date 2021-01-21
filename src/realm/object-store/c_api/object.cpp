#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

#include <realm/util/overload.hpp>

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

RLM_API realm_results_t* realm_object_find_all(realm_t* realm, realm_class_key_t key)
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
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto tblkey = TableKey(table_key);
        auto table = shared_realm->read_group().get_table(tblkey);
        // FIXME: Provide did_create?
        auto pkval = from_capi(pk);

        ColKey pkcol = table->get_primary_key_column();
        if (!pkcol) {
            // FIXME: Proper exception type.
            throw std::logic_error("Class does not have a primary key");
        }

        if (pkval.is_null() && !pkcol.is_nullable()) {
            auto& schema = schema_for_table(*realm, tblkey);
            throw NotNullableException{schema.name, schema.primary_key};
        }

        if (!pkval.is_null() && pkval.get_type() != DataType(pkcol.get_type())) {
            auto& schema = schema_for_table(*realm, tblkey);
            throw WrongPrimaryKeyTypeException{schema.name};
        }

        auto obj = table->create_object_with_primary_key(pkval);
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

RLM_API void* _realm_object_get_native_ptr(realm_object_t* obj)
{
    return static_cast<Object*>(obj);
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

        for (size_t i = 0; i < num_values; ++i) {
            auto col_key = ColKey(properties[i]);

            if (col_key.is_collection()) {
                // FIXME: Proper exception type.
                throw std::logic_error("Accessing collection property as value.");
            }

            auto o = obj->obj();
            auto val = o.get_any(col_key);
            out_values[i] = to_capi(val);
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

        // Perform validation up front to avoid partial updates. This is
        // unlikely to incur performance overhead because the object itself is
        // not accessed here, just the bits of the column key and the input type.

        for (size_t i = 0; i < num_values; ++i) {
            auto col_key = ColKey(properties[i]);

            if (col_key.is_collection()) {
                // FIXME: Proper exception type.
                throw std::logic_error("Accessing collection property as value.");
            }

            auto val = from_capi(values[i]);

            if (val.is_null() && !col_key.is_nullable()) {
                auto table = o.get_table();
                auto& schema = schema_for_table(obj->get_realm(), table->get_key());
                throw NotNullableException{schema.name, table->get_column_name(col_key)};
            }

            if (!val.is_null() && col_key.get_type() != ColumnType(val.get_type()) &&
                col_key.get_type() != col_type_Mixed) {
                auto table = o.get_table();
                auto& schema = schema_for_table(obj->get_realm(), table->get_key());
                throw PropertyTypeMismatch{schema.name, table->get_column_name(col_key)};
            }
        }

        // Actually write the properties.

        for (size_t i = 0; i < num_values; ++i) {
            auto col_key = ColKey(properties[i]);
            auto val = from_capi(values[i]);
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
            // FIXME: Proper exception type.
            throw std::logic_error{"Not a list property"};
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

template <class F>
auto value_or_object(const std::shared_ptr<Realm>& realm, PropertyType val_type, Mixed val, F&& f)
{
    // FIXME: Object Store has poor support for heterogeneous lists, and in
    // particular it relies on Core to check that the input types to
    // `List::insert()` etc. match the list property type. Once that is fixed /
    // made safer, this logic should move into Object Store.

    if (val.is_null()) {
        if (!is_nullable(val_type)) {
            // FIXME: Defer this exception to Object Store, which can produce
            // nicer message.
            throw std::invalid_argument("NULL in non-nullable field/list.");
        }

        // Produce a util::none matching the property type.
        return switch_on_type(val_type, [&](auto ptr) {
            using T = std::remove_cv_t<std::remove_pointer_t<decltype(ptr)>>;
            T nothing{};
            return f(nothing);
        });
    }

    PropertyType base_type = (val_type & ~PropertyType::Flags);

    // Note: The following checks PropertyType::Mixed on the assumption that it
    // will become un-deprecated when Mixed is exposed in Object Store.

    switch (val.get_type()) {
        case type_Int: {
            if (base_type != PropertyType::Int && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<int64_t>());
        }
        case type_Bool: {
            if (base_type != PropertyType::Bool && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<bool>());
        }
        case type_String: {
            if (base_type != PropertyType::String && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<StringData>());
        }
        case type_Binary: {
            if (base_type != PropertyType::Data && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<BinaryData>());
        }
        case type_Timestamp: {
            if (base_type != PropertyType::Date && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<Timestamp>());
        }
        case type_Float: {
            if (base_type != PropertyType::Float && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<float>());
        }
        case type_Double: {
            if (base_type != PropertyType::Double && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<double>());
        }
        case type_Decimal: {
            if (base_type != PropertyType::Decimal && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<Decimal128>());
        }
        case type_ObjectId: {
            if (base_type != PropertyType::ObjectId && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<ObjectId>());
        }
        case type_TypedLink: {
            if (base_type != PropertyType::Object && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            // Object Store performs link validation already. Just create an Obj
            // for the link, and pass it on.
            auto link = val.get<ObjLink>();
            auto target_table = realm->read_group().get_table(link.get_table_key());
            auto obj = target_table->get_object(link.get_obj_key());
            return f(std::move(obj));
        }
        case type_UUID: {
            if (base_type != PropertyType::UUID && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<UUID>());
        }

        case type_Link:
            // Note: from_capi(realm_value_t) never produces an untyped link.
        case type_Mixed:
        case type_LinkList:
            REALM_TERMINATE("Invalid value type.");
    }
}

RLM_API bool realm_list_insert(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        value_or_object(list->get_realm(), list->get_type(), val, [&](auto val) {
            list->insert(index, val);
        });
        return true;
    });
}

RLM_API bool realm_list_set(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        value_or_object(list->get_realm(), list->get_type(), val, [&](auto val) {
            list->set(index, val);
        });
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
        list->remove_all();
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
