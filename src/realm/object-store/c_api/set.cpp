#include <realm/object-store/c_api/util.hpp>

namespace realm::c_api {

RLM_API bool realm_set_size(const realm_set_t* set, size_t* out_size)
{
    return wrap_err([&]() {
        size_t size = set->size();
        if (out_size)
            *out_size = size;
        return true;
    });
}

RLM_API bool realm_set_get_property(const realm_set_t* set, realm_property_info_t* out_property_info)
{
    static_cast<void>(set);
    static_cast<void>(out_property_info);
    REALM_TERMINATE("Not implemented yet");
}

RLM_API bool realm_set_get(const realm_set_t* set, size_t index, realm_value_t* out_value)
{
    return wrap_err([&]() {
        set->verify_attached();

        auto val = set->get_any(index);
        if (out_value) {
            auto converted = objkey_to_typed_link(val, *set);
            *out_value = to_capi(converted);
        }

        return true;
    });
}

RLM_API bool realm_set_find(const realm_set_t* set, realm_value_t value, size_t* out_index, bool* out_found)
{
    return wrap_err([&]() {
        set->verify_attached();

        auto val = from_capi(value);

        // FIXME: Check this without try-catch.
        try {
            check_value_assignable(*set, val);
        }
        catch (const NotNullableException&) {
            if (out_index)
                *out_index = realm::not_found;
            if (out_found)
                *out_found = false;
            return true;
        }
        catch (const PropertyTypeMismatch&) {
            if (out_index)
                *out_index = realm::not_found;
            if (out_found)
                *out_found = false;
            return true;
        }

        auto converted = typed_link_to_objkey(val, set->get_parent_column_key());
        auto index = set->find_any(converted);
        if (out_index)
            *out_index = index;
        if (out_found)
            *out_found = index < set->size();
        return true;
    });
}

RLM_API bool realm_set_insert(realm_set_t* set, realm_value_t value, size_t* out_index, bool* out_inserted)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        check_value_assignable(*set, val);
        auto col_key = set->get_parent_column_key();
        val = typed_link_to_objkey(val, col_key);

        auto [index, inserted] = set->insert_any(val);
        if (out_index)
            *out_index = index;
        if (out_inserted)
            *out_inserted = inserted;
        return true;
    });
}

RLM_API bool realm_set_erase(realm_set_t* set, realm_value_t value, bool* out_erased)
{
    return wrap_err([&]() {
        auto val = from_capi(value);

        // FIXME: Check this without try-catch.
        try {
            check_value_assignable(*set, val);
        }
        catch (const NotNullableException&) {
            if (out_erased)
                *out_erased = false;
            return true;
        }
        catch (const PropertyTypeMismatch&) {
            if (out_erased)
                *out_erased = false;
            return true;
        }
        auto converted = typed_link_to_objkey(val, set->get_parent_column_key());
        auto [index, erased] = set->remove_any(converted);
        if (out_erased)
            *out_erased = erased;

        return true;
    });
}

RLM_API bool realm_set_clear(realm_set_t* set)
{
    return wrap_err([&]() {
        // Note: Confusing naming.
        set->remove_all();
        return true;
    });
}

RLM_API bool realm_set_remove_all(realm_set_t* set)
{
    return wrap_err([&]() {
        // Note: Confusing naming.
        set->delete_all();
        return true;
    });
}

} // namespace realm::c_api