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
            *out_value = to_capi(val);
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

        auto index = set->find_any(val);
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
        auto [index, erased] = set->remove_any(val);
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

RLM_API realm_set_t* realm_set_from_thread_safe_reference(const realm_t* realm, realm_thread_safe_reference_t* tsr)
{
    return wrap_err([&]() {
        auto stsr = dynamic_cast<realm_set::thread_safe_reference*>(tsr);
        if (!stsr) {
            throw std::logic_error{"Thread safe reference type mismatch"};
        }

        auto set = stsr->resolve<object_store::Set>(*realm);
        return new realm_set_t{std::move(set)};
    });
}

RLM_API bool realm_set_resolve_in(const realm_set_t* from_set, const realm_t* target_realm, realm_set_t** resolved)
{
    return wrap_err([&]() {
        try {
            const auto& realm = *target_realm;
            auto frozen_set = from_set->freeze(realm);
            if (frozen_set.is_valid()) {
                *resolved = new realm_set_t{std::move(frozen_set)};
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

RLM_API bool realm_set_is_valid(const realm_set_t* set)
{
    if (!set)
        return false;
    return set->is_valid();
}


} // namespace realm::c_api
