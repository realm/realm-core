////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

namespace realm::c_api {

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
        auto mixed = list->get_any(index);

        if (out_value) {
            *out_value = to_capi(mixed);
        }
        return true;
    });
}

RLM_API bool realm_list_find(const realm_list_t* list, const realm_value_t* value, size_t* out_index, bool* out_found)
{
    if (out_index)
        *out_index = realm::not_found;
    if (out_found)
        *out_found = false;

    return wrap_err([&] {
        list->verify_attached();
        auto val = from_capi(*value);
        check_value_assignable(*list, val);
        auto index = list->find_any(val);
        if (out_index)
            *out_index = index;
        if (out_found)
            *out_found = index < list->size();
        return true;
    });
}


RLM_API bool realm_list_insert(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        check_value_assignable(*list, val);

        list->insert_any(index, val);
        return true;
    });
}

RLM_API realm_list_t* realm_list_insert_list(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->insert_collection(index, CollectionType::List);
        return new realm_list_t{list->get_list(index)};
    });
}

RLM_API realm_dictionary_t* realm_list_insert_dictionary(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->insert_collection(index, CollectionType::Dictionary);
        return new realm_dictionary_t{list->get_dictionary(index)};
    });
}

RLM_API realm_list_t* realm_list_set_list(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->set_collection(index, CollectionType::List);
        return new realm_list_t{list->get_list(index)};
    });
}

RLM_API realm_dictionary_t* realm_list_set_dictionary(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->set_collection(index, CollectionType::Dictionary);
        return new realm_dictionary_t{list->get_dictionary(index)};
    });
}


RLM_API realm_list_t* realm_list_get_list(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        return new realm_list_t{list->get_list(index)};
    });
}

RLM_API realm_dictionary_t* realm_list_get_dictionary(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        return new realm_dictionary_t{list->get_dictionary(index)};
    });
}

RLM_API bool realm_list_move(realm_list_t* list, size_t from_index, size_t to_index)
{
    return wrap_err([&]() {
        list->move(from_index, to_index);
        return true;
    });
}


RLM_API bool realm_list_set(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        check_value_assignable(*list, val);

        list->set_any(index, val);
        return true;
    });
}

RLM_API realm_object_t* realm_list_insert_embedded(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        return new realm_object_t({list->get_realm(), list->insert_embedded(index)});
    });
}

RLM_API realm_object_t* realm_list_set_embedded(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->verify_attached();
        return new realm_object_t({list->get_realm(), list->set_embedded(index)});
    });
}

RLM_API realm_object_t* realm_list_get_linked_object(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->verify_attached();
        auto o = list->get_object(index);
        return o ? new realm_object_t({list->get_realm(), o}) : nullptr;
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
            throw LogicError{ErrorCodes::IllegalOperation, "Thread safe reference type mismatch"};
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
