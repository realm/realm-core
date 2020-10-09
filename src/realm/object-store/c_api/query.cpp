#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

RLM_API realm_query_t* realm_query_new(const realm_t* realm, realm_table_key_t key)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto table = shared_realm->read_group().get_table(from_capi(key));
        return new realm_query_t{table->where(), shared_realm};
    });
}

RLM_API realm_query_t* realm_query_new_with_results(realm_results_t* results)
{
    return wrap_err([&]() {
        return new realm_query_t{results->get_query(), results->get_realm()};
    });
}

RLM_API realm_descriptor_ordering_t* realm_new_descriptor_ordering()
{
    return wrap_err([&]() {
        return new realm_descriptor_ordering_t{};
    });
}

RLM_API realm_parsed_query_t* realm_query_parse(realm_string_t str)
{
    return wrap_err([&]() {
        auto input = from_capi(str);
        return new realm_parsed_query_t{parser::parse(input)};
    });
}

RLM_API bool realm_apply_parsed_predicate(realm_query_t* query, const realm_parsed_query_t* parsed,
                                          const realm_parsed_query_arguments_t*, const realm_key_path_mapping_t*)
{
    return wrap_err([&]() {
        // FIXME: arguments, key-path mapping
        auto args = query_builder::NoArguments{};
        auto key_path_mapping = parser::KeyPathMapping{};
        query_builder::apply_predicate(*query->ptr, parsed->predicate, args, key_path_mapping);
        return true;
    });
}

RLM_API bool realm_apply_parsed_descriptor_ordering(realm_descriptor_ordering_t* ordering, const realm_t*,
                                                    realm_table_key_t, const realm_parsed_query_t* parsed,
                                                    const realm_key_path_mapping_t* key_path_mapping)
{
    return wrap_err([&]() {
        static_cast<void>(ordering);
        static_cast<void>(parsed);
        static_cast<void>(key_path_mapping);
        REALM_TERMINATE("Not implemented yet.");
        return true;
    });
}

RLM_API bool realm_query_count(const realm_query_t* query, size_t* out_count)
{
    return wrap_err([&]() {
        *out_count = query->ptr->count();
        return true;
    });
}

RLM_API bool realm_query_find_first(realm_query_t* query, realm_obj_key_t* out_key, bool* out_found)
{
    return wrap_err([&]() {
        auto key = query->ptr->find();
        if (out_found)
            *out_found = bool(key);
        if (key && out_key)
            *out_key = to_capi(key);
        return true;
    });
}

RLM_API realm_results_t* realm_query_find_all(realm_query_t* query)
{
    return wrap_err([&]() {
        auto shared_realm = query->weak_realm.lock();
        REALM_ASSERT_RELEASE(shared_realm);
        return new realm_results{Results{shared_realm, *query->ptr}};
    });
}

RLM_API size_t realm_results_count(realm_results_t* results)
{
    return results->size();
}

RLM_API realm_value_t realm_results_get(realm_results_t* results, size_t index)
{
    return wrap_err([&]() {
        // FIXME: Support non-object results.
        auto obj = results->get<Obj>(index);
        auto table_key = obj.get_table()->get_key();
        auto obj_key = obj.get_key();
        realm_value_t val;
        val.type = RLM_TYPE_LINK;
        val.link.target_table = to_capi(table_key);
        val.link.target = to_capi(obj_key);
        return val;
    });
}

RLM_API realm_object_t* realm_results_get_object(realm_results_t* results, size_t index)
{
    return wrap_err([&]() {
        auto shared_realm = results->get_realm();
        auto obj = results->get<Obj>(index);
        return new realm_object_t{Object{shared_realm, std::move(obj)}};
    });
}

RLM_API bool realm_results_delete_all(realm_results_t* results)
{
    return wrap_err([&]() {
        // Note: This method is very confusingly named. It actually does erase
        // all the objects.
        results->clear();
        return true;
    });
}