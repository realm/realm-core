#include "realm/sort_descriptor.hpp"
#include "realm/util/scope_exit.hpp"
#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

#include <realm/object-store/keypath_helpers.hpp>
#include <realm/parser/query_parser.hpp>
#include <realm/parser/keypath_mapping.hpp>

namespace realm::c_api {

namespace {
struct QueryArgumentsAdapter : query_parser::Arguments {
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
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    long long long_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_INT) {
            return m_args[i].integer;
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    float float_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_FLOAT) {
            return m_args[i].fnum;
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    double double_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_DOUBLE) {
            return m_args[i].dnum;
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    StringData string_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_STRING) {
            return from_capi(m_args[i].string);
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    BinaryData binary_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_BINARY) {
            return from_capi(m_args[i].binary);
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    Timestamp timestamp_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_TIMESTAMP) {
            return from_capi(m_args[i].timestamp);
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    ObjKey object_index_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_LINK) {
            // FIXME: Somehow check the target table type?
            return from_capi(m_args[i].link).get_obj_key();
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    ObjectId objectid_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_OBJECT_ID) {
            return from_capi(m_args[i].object_id);
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    Decimal128 decimal128_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_DECIMAL128) {
            return from_capi(m_args[i].decimal128);
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    UUID uuid_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_UUID) {
            return from_capi(m_args[i].uuid);
        }
        // Note: Unreachable.
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    ObjLink objlink_for_argument(size_t i) final
    {
        verify_ndx(i);
        if (m_args[i].type == RLM_TYPE_LINK) {
            return from_capi(m_args[i].link);
        }
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }

    bool is_argument_null(size_t i) final
    {
        verify_ndx(i);
        return m_args[i].type == RLM_TYPE_NULL;
    }

    DataType type_for_argument(size_t i) override
    {
        verify_ndx(i);
        switch (m_args[i].type) {
            case RLM_TYPE_NULL:                                                  // LCOV_EXCL_LINE
                REALM_TERMINATE("Query parser did not call is_argument_null()"); // LCOV_EXCL_LINE
            case RLM_TYPE_INT:
                return type_Int;
            case RLM_TYPE_STRING:
                return type_String;
            case RLM_TYPE_BOOL:
                return type_Bool;
            case RLM_TYPE_FLOAT:
                return type_Float;
            case RLM_TYPE_DOUBLE:
                return type_Double;
            case RLM_TYPE_BINARY:
                return type_Binary;
            case RLM_TYPE_TIMESTAMP:
                return type_Timestamp;
            case RLM_TYPE_LINK:
                return type_Link;
            case RLM_TYPE_OBJECT_ID:
                return type_ObjectId;
            case RLM_TYPE_DECIMAL128:
                return type_Decimal;
            case RLM_TYPE_UUID:
                return type_UUID;
        }
        throw LogicError{LogicError::type_mismatch}; // LCOV_EXCL_LINE
    }
};
} // namespace

static Query parse_and_apply_query(const std::shared_ptr<Realm>& realm, ConstTableRef table, const char* query_string,
                                   size_t num_args, const realm_value_t* args)
{
    query_parser::KeyPathMapping mapping;
    realm::populate_keypath_mapping(mapping, *realm);
    QueryArgumentsAdapter arguments{num_args, args};
    Query query = table->query(query_string, arguments, mapping);
    return query;
}

RLM_API realm_query_t* realm_query_parse(const realm_t* realm, realm_class_key_t target_table_key,
                                         const char* query_string, size_t num_args, const realm_value_t* args)
{
    return wrap_err([&]() {
        auto table = (*realm)->read_group().get_table(TableKey(target_table_key));
        Query query = parse_and_apply_query(*realm, table, query_string, num_args, args);
        auto ordering = query.get_ordering();
        return new realm_query_t{std::move(query), std::move(ordering), *realm};
    });
}

RLM_API const char* realm_query_get_description(realm_query_t* query)
{
    return wrap_err([&]() {
        return query->get_description();
    });
}

RLM_API realm_query_t* realm_query_append_query(const realm_query_t* existing_query, const char* query_string,
                                                size_t num_args, const realm_value_t* args)
{
    return wrap_err([&]() {
        auto realm = existing_query->weak_realm.lock();
        auto table = existing_query->query.get_table();
        Query query = parse_and_apply_query(realm, table, query_string, num_args, args);

        Query combined = Query(existing_query->query).and_query(query);
        auto ordering_copy = util::make_bind<DescriptorOrdering>();
        *ordering_copy = existing_query->get_ordering();
        if (auto ordering = query.get_ordering())
            ordering_copy->append(*ordering);
        return new realm_query_t{std::move(combined), std::move(ordering_copy), realm};
    });
}

RLM_API realm_query_t* realm_query_parse_for_list(const realm_list_t* list, const char* query_string, size_t num_args,
                                                  const realm_value_t* args)
{
    return wrap_err([&]() {
        auto realm = list->get_realm();
        auto table = list->get_table();
        Query query = parse_and_apply_query(realm, table, query_string, num_args, args);
        auto ordering = query.get_ordering();
        return new realm_query_t{std::move(query), std::move(ordering), realm};
    });
}

RLM_API realm_query_t* realm_query_parse_for_results(const realm_results_t* results, const char* query_string,
                                                     size_t num_args, const realm_value_t* args)
{
    return wrap_err([&]() {
        auto realm = results->get_realm();
        auto table = results->get_table();
        Query query = parse_and_apply_query(realm, table, query_string, num_args, args);
        auto ordering = query.get_ordering();
        return new realm_query_t{std::move(query), std::move(ordering), realm};
    });
}

RLM_API bool realm_query_count(const realm_query_t* query, size_t* out_count)
{
    return wrap_err([&]() {
        *out_count = Query(query->query).count(query->get_ordering());
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
        return new realm_results{Results{shared_realm, query->query, query->get_ordering()}};
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

RLM_API realm_results_t* realm_results_filter(realm_results_t* results, realm_query_t* query)
{
    return wrap_err([&]() {
        return new realm_results{results->filter(std::move(query->query))};
    });
}

namespace {
realm_results_t* realm_results_ordering(realm_results_t* results, const char* op, const char* ordering)
{
    return wrap_err([&]() -> realm_results_t* {
        std::string str = "TRUEPREDICATE " + std::string(op) + "(" + std::string(ordering) + ")";
        auto q = results->get_table()->query(str);
        auto ordering{q.get_ordering()};
        return new realm_results{results->apply_ordering(std::move(*ordering))};
        return nullptr;
    });
}
} // namespace

RLM_API realm_results_t* realm_results_sort(realm_results_t* results, const char* sort_string)
{
    return realm_results_ordering(results, "SORT", sort_string);
}

RLM_API realm_results_t* realm_results_distinct(realm_results_t* results, const char* distinct_string)
{
    return realm_results_ordering(results, "DISTINCT", distinct_string);
}

RLM_API realm_results_t* realm_results_limit(realm_results_t* results, size_t max_count)
{
    return wrap_err([&]() {
        return new realm_results{results->limit(max_count)};
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

RLM_API realm_results_t* realm_results_snapshot(const realm_results_t* results)
{
    return wrap_err([&]() {
        return new realm_results{results->snapshot()};
    });
}

RLM_API bool realm_results_min(realm_results_t* results, realm_property_key_t col, realm_value_t* out_value,
                               bool* out_found)
{
    return wrap_err([&]() {
        // FIXME: This should be part of Results.
        results->get_table()->check_column(ColKey(col));

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
        // FIXME: This should be part of Results.
        results->get_table()->check_column(ColKey(col));

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
        // FIXME: This should be part of Results.
        results->get_table()->check_column(ColKey(col));

        if (out_found) {
            *out_found = results->size() != 0;
        }

        if (auto x = results->sum(ColKey(col))) {
            if (out_value)
                *out_value = to_capi(*x);
        }
        else {
            // Note: This can only be hit when the `m_table` and `m_collection`
            // pointers in `Results` are NULL.
            //
            // FIXME: It is unclear when that happens.

            // LCOV_EXCL_START
            if (out_value) {
                out_value->type = RLM_TYPE_NULL;
            }
            // LCOV_EXCL_STOP
        }
        return true;
    });
}

RLM_API bool realm_results_average(realm_results_t* results, realm_property_key_t col, realm_value_t* out_value,
                                   bool* out_found)
{
    return wrap_err([&]() {
        // FIXME: This should be part of Results.
        results->get_table()->check_column(ColKey(col));

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

RLM_API realm_results_t* realm_results_resolve_in(realm_results_t* from_results, const realm_t* target_realm)
{
    return wrap_err([&]() {
        const auto& realm = *target_realm;
        auto resolved_results = from_results->freeze(realm);
        return new realm_results_t{std::move(resolved_results)};
    });
}

} // namespace realm::c_api
