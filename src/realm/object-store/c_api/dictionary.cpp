#include "realm/object-store/c_api/types.hpp"
#include "realm/object-store/dictionary.hpp"
#include <realm/object-store/c_api/util.hpp>

namespace realm::c_api {

RLM_API bool realm_dictionary_size(const realm_dictionary_t* dict, size_t* out_size)
{
    return wrap_err([&]() {
        size_t size = dict->size();
        if (out_size)
            *out_size = size;
        return true;
    });
}

RLM_API bool realm_dictionary_get_property(const realm_dictionary_t* dict, realm_property_info_t* out_property_info)
{
    static_cast<void>(dict);
    static_cast<void>(out_property_info);
    REALM_TERMINATE("Not implemented yet.");
}

RLM_API bool realm_dictionary_find(const realm_dictionary_t* dict, realm_value_t key, realm_value_t* out_value,
                                   bool* out_found)
{
    if (key.type != RLM_TYPE_STRING) {
        if (out_found)
            *out_found = false;
        return true;
    }

    return wrap_err([&]() {
        dict->verify_attached();
        StringData k{key.string.data, key.string.size};
        auto val = dict->try_get_any(k);
        if (!val) {
            if (out_found)
                *out_found = false;
        }
        else {
            if (out_value)
                *out_value = to_capi(*val);
            if (out_found)
                *out_found = true;
        }
        return true;
    });
}

RLM_API bool realm_dictionary_get(const realm_dictionary_t* dict, size_t index, realm_value_t* out_key,
                                  realm_value_t* out_value)
{
    return wrap_err([&]() {
        dict->verify_attached();
        auto [key, value] = dict->get_pair(index);
        if (out_key) {
            out_key->type = RLM_TYPE_STRING;
            out_key->string = to_capi(key);
        }
        if (out_value)
            *out_value = to_capi(value);
        return true;
    });
}

RLM_API bool realm_dictionary_insert(realm_dictionary_t* dict, realm_value_t key, realm_value_t value,
                                     size_t* out_index, bool* out_inserted)
{
    return wrap_err([&]() {
        if (key.type != RLM_TYPE_STRING) {
            throw std::invalid_argument{"Only string keys are supported in dictionaries"};
        }

        StringData k{key.string.data, key.string.size};
        auto val = from_capi(value);
        check_value_assignable(*dict, val);
        auto [index, inserted] = dict->insert_any(k, val);

        if (out_index)
            *out_index = index;
        if (out_inserted)
            *out_inserted = inserted;

        return true;
    });
}

RLM_API bool realm_dictionary_erase(realm_dictionary_t* dict, realm_value_t key, bool* out_erased)
{
    return wrap_err([&]() {
        bool erased = false;
        if (key.type == RLM_TYPE_STRING) {
            StringData k{key.string.data, key.string.size};
            erased = dict->try_erase(k);
        }

        if (out_erased)
            *out_erased = erased;
        return true;
    });
}

RLM_API bool realm_dictionary_clear(realm_dictionary_t* dict)
{
    return wrap_err([&]() {
        // Note: confusing naming.
        dict->remove_all();
        return true;
    });
}

RLM_API realm_dictionary_t* realm_dictionary_from_thread_safe_reference(const realm_t* realm,
                                                                        realm_thread_safe_reference_t* tsr)
{
    return wrap_err([&]() {
        auto stsr = dynamic_cast<realm_dictionary::thread_safe_reference*>(tsr);
        if (!stsr) {
            throw std::logic_error{"Thread safe reference type mismatch"};
        }

        auto dict = stsr->resolve<object_store::Dictionary>(*realm);
        return new realm_dictionary_t{std::move(dict)};
    });
}

RLM_API bool realm_dictionary_resolve_in(const realm_dictionary_t* from_dictionary, const realm_t* target_realm,
                                         realm_dictionary_t** resolved)
{
    return wrap_err([&]() {
        try {
            const auto& realm = *target_realm;
            auto frozen_dictionary = from_dictionary->freeze(realm);
            if (frozen_dictionary.is_valid()) {
                *resolved = new realm_dictionary_t{std::move(frozen_dictionary)};
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

RLM_API bool realm_dictionary_is_valid(const realm_dictionary_t* dictionary)
{
    if (!dictionary)
        return false;
    return dictionary->is_valid();
}

} // namespace realm::c_api