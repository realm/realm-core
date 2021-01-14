#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

#include <realm/object-store/keypath_helpers.hpp>
#include <realm/parser/query_parser.hpp>
#include <realm/parser/keypath_mapping.hpp>

namespace {
struct QueryArgumentsAdapter : query_parser::Arguments {
    size_t m_num_args = 0;
    const realm_value_t* m_args = nullptr;

    QueryArgumentsAdapter(size_t num_args, const realm_value_t* args) noexcept
        : Arguments(num_args)
        , m_args(args)
    {
    }

    bool bool_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_BOOL) {
            return m_args[i].boolean;
        }
        throw LogicError{LogicError::type_mismatch};
    }

    long long long_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_INT) {
            return m_args[i].integer;
        }
        throw LogicError{LogicError::type_mismatch};
    }

    float float_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_FLOAT) {
            return m_args[i].fnum;
        }
        throw LogicError{LogicError::type_mismatch};
    }

    double double_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_DOUBLE) {
            return m_args[i].dnum;
        }
        throw LogicError{LogicError::type_mismatch};
    }

    StringData string_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_STRING) {
            return from_capi(m_args[i].string);
        }
        throw LogicError{LogicError::type_mismatch};
    }

    BinaryData binary_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_BINARY) {
            return from_capi(m_args[i].binary);
        }
        throw LogicError{LogicError::type_mismatch};
    }

    Timestamp timestamp_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_TIMESTAMP) {
            return from_capi(m_args[i].timestamp);
        }
        throw LogicError{LogicError::type_mismatch};
    }

    ObjKey object_index_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_LINK) {
            // FIXME: Somehow check the target table type?
            return from_capi(m_args[i].link).get_obj_key();
        }
        throw LogicError{LogicError::type_mismatch};
    }

    ObjectId objectid_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_OBJECT_ID) {
            return from_capi(m_args[i].object_id);
        }
        throw LogicError{LogicError::type_mismatch};
    }

    Decimal128 decimal128_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_DECIMAL128) {
            return from_capi(m_args[i].decimal128);
        }
        throw LogicError{LogicError::type_mismatch};
    }

    UUID uuid_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_UUID) {
            return from_capi(m_args[i].uuid);
        }
        throw LogicError{LogicError::type_mismatch};
    }

    bool is_argument_null(size_t i) final
    {
        verify_ndx(i);
        return m_args[i].type == RLM_TYPE_NULL;
    }
    DataType type_for_argument(size_t i) override
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_INT) {
            return type_Int;
        }
        if (m_args[i].type == RLM_TYPE_STRING) {
            return type_String;
        }
        if (m_args[i].type == RLM_TYPE_BOOL) {
            return type_Bool;
        }
        if (m_args[i].type == RLM_TYPE_FLOAT) {
            return type_Float;
        }
        if (m_args[i].type == RLM_TYPE_DOUBLE) {
            return type_Double;
        }
        if (m_args[i].type == RLM_TYPE_BINARY) {
            return type_Binary;
        }
        if (m_args[i].type == RLM_TYPE_TIMESTAMP) {
            return type_Timestamp;
        }
        if (m_args[i].type == RLM_TYPE_LINK) {
            return type_Link;
        }
        if (m_args[i].type == RLM_TYPE_OBJECT_ID) {
            return type_ObjectId;
        }
        if (m_args[i].type == RLM_TYPE_DECIMAL128) {
            return type_Decimal;
        }
        if (m_args[i].type == RLM_TYPE_UUID) {
            return type_UUID;
        }
        throw LogicError{LogicError::type_mismatch};
    }
};
} // namespace

static Query parse_and_apply_query(const std::shared_ptr<Realm>& realm, ConstTableRef table,
                                   DescriptorOrdering& ordering, const char* query_string, size_t num_args,
                                   const realm_value_t* args)
{
    query_parser::KeyPathMapping mapping;
    realm::populate_keypath_mapping(mapping, *realm);
    QueryArgumentsAdapter arguments{num_args, args};
    Query query = table->query(query_string, arguments, mapping);
    if (auto opt_ordering = query.get_ordering())
        ordering = *opt_ordering;
    return query;
}

RLM_API realm_query_t* realm_query_parse(const realm_t* realm, realm_class_key_t target_table_key,
                                         const char* query_string, size_t num_args, const realm_value_t* args)
{
    return wrap_err([&]() {
        auto table = (*realm)->read_group().get_table(TableKey(target_table_key));
        DescriptorOrdering ordering;
        Query query = parse_and_apply_query(*realm, table, ordering, query_string, num_args, args);
        return new realm_query_t{std::move(query), std::move(ordering), *realm};
    });
}

RLM_API realm_query_t* realm_query_parse_for_list(const realm_list_t* list, const char* query_string, size_t num_args,
                                                  const realm_value_t* args)
{
    return wrap_err([&]() {
        auto realm = list->get_realm();
        auto table = list->get_table();
        DescriptorOrdering ordering;
        Query query = parse_and_apply_query(realm, table, ordering, query_string, num_args, args);
        return new realm_query_t{std::move(query), std::move(ordering), realm};
    });
}

RLM_API realm_query_t* realm_query_parse_for_results(const realm_results_t* results, const char* query_string,
                                                     size_t num_args, const realm_value_t* args)
{
    return wrap_err([&]() {
        auto realm = results->get_realm();
        auto table = results->get_table();
        DescriptorOrdering ordering;
        Query query = parse_and_apply_query(realm, table, ordering, query_string, num_args, args);
        return new realm_query_t{std::move(query), std::move(ordering), realm};
    });
}

RLM_API bool realm_query_count(const realm_query_t* query, size_t* out_count)
{
    return wrap_err([&]() {
        *out_count = query->query.count();
        return true;
    });
}

RLM_API bool realm_query_find_first(realm_query_t* query, realm_value_t* out_value, bool* out_found)
{
    return wrap_err([&]() {
        auto key = query->query.find();
        if (out_found)
            *out_found = bool(key);
        if (key && out_value) {
            ObjLink link{query->query.get_table()->get_key(), key};
            out_value->type = RLM_TYPE_LINK;
            out_value->link = to_capi(link);
        }
        return true;
    });
}

RLM_API realm_results_t* realm_query_find_all(realm_query_t* query)
{
    return wrap_err([&]() {
        auto shared_realm = query->weak_realm.lock();
        REALM_ASSERT_RELEASE(shared_realm);
        return new realm_results{Results{shared_realm, query->query, query->ordering}};
    });
}

RLM_API bool realm_results_count(realm_results_t* results, size_t* out_count)
{
    return wrap_err([&]() {
        auto count = results->size();
        if (out_count) {
            *out_count = count;
        }
        return true;
    });
}

RLM_API bool realm_results_get(realm_results_t* results, size_t index, realm_value_t* out_value)
{
    return wrap_err([&]() {
        // FIXME: Support non-object results.
        auto obj = results->get<Obj>(index);
        auto table_key = obj.get_table()->get_key();
        auto obj_key = obj.get_key();
        if (out_value) {
            out_value->type = RLM_TYPE_LINK;
            out_value->link.target_table = table_key.value;
            out_value->link.target = obj_key.value;
        }
        return true;
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

RLM_API bool realm_results_min(realm_results_t* results, realm_property_key_t col, realm_value_t* out_value,
                               bool* out_found)
{
    return wrap_err([&]() {
        if (auto x = results->min(ColKey(col))) {
            if (out_found) {
                *out_found = true;
            }
            if (out_value) {
                *out_value = to_capi(*x);
            }
        }
        else {
            if (out_found) {
                *out_found = false;
            }
            if (out_value) {
                out_value->type = RLM_TYPE_NULL;
            }
        }
        return true;
    });
}

RLM_API bool realm_results_max(realm_results_t* results, realm_property_key_t col, realm_value_t* out_value,
                               bool* out_found)
{
    return wrap_err([&]() {
        if (auto x = results->max(ColKey(col))) {
            if (out_found) {
                *out_found = true;
            }
            if (out_value) {
                *out_value = to_capi(*x);
            }
        }
        else {
            if (out_found) {
                *out_found = false;
            }
            if (out_value) {
                out_value->type = RLM_TYPE_NULL;
            }
        }
        return true;
    });
}

RLM_API bool realm_results_sum(realm_results_t* results, realm_property_key_t col, realm_value_t* out_value,
                               bool* out_found)
{
    return wrap_err([&]() {
        if (auto x = results->sum(ColKey(col))) {
            if (out_found) {
                *out_found = true;
            }
            if (out_value)
                *out_value = to_capi(*x);
        }
        else {
            if (out_found) {
                *out_found = false;
            }
            if (out_value) {
                out_value->type = RLM_TYPE_INT;
                out_value->integer = 0;
            }
        }
        return true;
    });
}

RLM_API bool realm_results_average(realm_results_t* results, realm_property_key_t col, realm_value_t* out_value,
                                   bool* out_found)
{
    return wrap_err([&]() {
        if (auto x = results->average(ColKey(col))) {
            if (out_found) {
                *out_found = true;
            }
            if (out_value) {
                *out_value = to_capi(*x);
            }
        }
        else {
            if (out_found) {
                *out_found = false;
            }
            if (out_value) {
                out_value->type = RLM_TYPE_NULL;
            }
        }
        return true;
    });
}

RLM_API realm_results_t* realm_results_from_thread_safe_reference(const realm_t* realm,
                                                                  realm_thread_safe_reference_t* tsr)
{
    return wrap_err([&]() {
        auto rtsr = dynamic_cast<realm_results::thread_safe_reference*>(tsr);
        if (!rtsr) {
            throw std::logic_error{"Thread safe reference type mismatch"};
        }

        auto results = rtsr->resolve<Results>(*realm);
        return new realm_results_t{std::move(results)};
    });
}
