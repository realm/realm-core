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
            auto converted = objkey_to_typed_link(*val, *dict);
            if (out_value)
                *out_value = to_capi(converted);
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
        auto val = objkey_to_typed_link(value, *dict);
        if (out_value)
            *out_value = to_capi(val);
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
        auto col_key = dict->get_parent_column_key();
        val = typed_link_to_objkey(val, col_key);
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
        static_cast<void>(dict);
        static_cast<void>(key);
        static_cast<void>(out_erased);
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

} // namespace realm::c_api