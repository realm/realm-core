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


#if defined(RLM_ENABLE_QUERY_BUILDER_API)

static inline bool validate_query(const realm_query_t* query)
{
    // FIXME: Query::validate() performs a full query tree validation, but most
    // of the errors we want to check are confined to the builder API (QueryInterface).
    auto error = query->ptr->validate();
    if (error != "") {
        throw InvalidQueryException{std::move(error)};
    }
    return true;
}

RLM_API bool realm_query_push_op(realm_query_t* query, realm_query_op_e op)
{
    return wrap_err([&]() {
        switch (op) {
            case RLM_QUERY_AND:
                // Query nodes within a group are implicitly AND'ed.
                return true;
            case RLM_QUERY_OR: {
                query->ptr->Or();
                return validate_query(query);
            }
            case RLM_QUERY_NOT: {
                query->ptr->Not();
                return validate_query(query);
            }
        }
        throw std::logic_error{"Invalid query op"};
    });
}

RLM_API bool realm_query_begin_group(realm_query_t* query)
{
    return wrap_err([&]() {
        query->ptr->group();
        return true;
    });
}

RLM_API bool realm_query_end_group(realm_query_t* query)
{
    return wrap_err([&]() {
        query->ptr->end_group();
        return validate_query(query);
    });
}

template <class F>
auto visit_realm_value(const realm_value_t& val, F&& func)
{
    switch (val.type) {
        case RLM_TYPE_NULL: {
            return func(realm::null{});
        }
        case RLM_TYPE_INT: {
            return func(val.integer);
        }
        case RLM_TYPE_BOOL: {
            return func(val.boolean);
        }
        case RLM_TYPE_STRING: {
            return func(from_capi(val.string));
        }
        case RLM_TYPE_BINARY: {
            return func(from_capi(val.binary));
        }
        case RLM_TYPE_TIMESTAMP: {
            return func(from_capi(val.timestamp));
        }
        case RLM_TYPE_FLOAT: {
            return func(val.fnum);
        }
        case RLM_TYPE_DOUBLE: {
            return func(val.dnum);
        }
        case RLM_TYPE_DECIMAL128: {
            return func(from_capi(val.decimal128));
        }
        case RLM_TYPE_OBJECT_ID: {
            return func(from_capi(val.object_id));
        }
        case RLM_TYPE_LINK: {
            return func(from_capi(val.link));
        }
    }
    REALM_TERMINATE("Invalid realm_value_t");
}

static const char* cond_as_str(realm_query_cond_e cond)
{
    switch (cond) {
        case RLM_QUERY_EQUAL:
            return "RLM_QUERY_EQUAL";
        case RLM_QUERY_NOT_EQUAL:
            return "RLM_QUERY_NOT_EQUAL";
        case RLM_QUERY_GREATER:
            return "RLM_QUERY_GREATER";
        case RLM_QUERY_GREATER_EQUAL:
            return "RLM_QUERY_GREATER_EQUAL";
        case RLM_QUERY_LESS:
            return "RLM_QUERY_LESS";
        case RLM_QUERY_LESS_EQUAL:
            return "RLM_QUERY_LESS_EQUAL";
        case RLM_QUERY_BETWEEN:
            return "RLM_QUERY_BETWEEN";
        case RLM_QUERY_CONTAINS:
            return "RLM_QUERY_CONTAINS";
        case RLM_QUERY_LIKE:
            return "RLM_QUERY_LIKE";
        case RLM_QUERY_BEGINS_WITH:
            return "RLM_QUERY_BEGINS_WITH";
        case RLM_QUERY_ENDS_WITH:
            return "RLM_QUERY_ENDS_WITH";
        case RLM_QUERY_LINKS_TO:
            return "RLM_QUERY_LINKS_TO";
    }
    REALM_TERMINATE("Invalid realm_query_cond_e");
}

RLM_API bool realm_query_push_cond(realm_query_t* query, realm_col_key_t lhs, realm_query_cond_e cond,
                                   const realm_value_t* values, size_t num_values, int flags)
{
    return wrap_err([&]() {
        ColKey lhs_key = from_capi(lhs);
        const bool case_sensitive = bool(flags & RLM_QUERY_CASE_SENSITIVE);

        auto& q = *query->ptr;

        // The following wall of code just maps from type-erased `realm_value_t`
        // to the type-safe `Query` interface, throwing user-friendly-ish
        // exceptions along the way.

        auto forbid_null = [](realm_query_cond_e cond) {
            return [=](realm::null) {
                throw std::logic_error{util::format("%1 does not support NULL", cond_as_str(cond))};
            };
        };

        auto forbid_bool = [](realm_query_cond_e cond) {
            return [=](bool) {
                throw std::logic_error{util::format("%1 does not support booleans", cond_as_str(cond))};
            };
        };

        auto forbid_string = [](realm_query_cond_e cond) {
            return [=](StringData) {
                throw std::logic_error{util::format("%1 does not support strings", cond_as_str(cond))};
            };
        };

        auto forbid_binary = [](realm_query_cond_e cond) {
            return [=](BinaryData) {
                throw std::logic_error{util::format("%1 does not support binary data", cond_as_str(cond))};
            };
        };

        auto forbid_timestamp = [](realm_query_cond_e cond) {
            return [=](Timestamp) {
                throw std::logic_error{util::format("%1 does not support timestamps", cond_as_str(cond))};
            };
        };

        auto forbid_link = [](realm_query_cond_e cond) {
            return [=](ObjLink) {
                throw std::logic_error{util::format("%1 does not support object links", cond_as_str(cond))};
            };
        };

        auto forbid_numeric = [](realm_query_cond_e cond) {
            return util::overloaded{
                [=](int64_t) {
                    throw std::logic_error{util::format("%1 does not support integers", cond_as_str(cond))};
                },
                [=](float) {
                    throw std::logic_error{util::format("%1 does not support float", cond_as_str(cond))};
                },
                [=](double) {
                    throw std::logic_error{util::format("%1 does not support doubles", cond_as_str(cond))};
                },
                [=](Decimal128) {
                    throw std::logic_error{util::format("%1 does not support decimal", cond_as_str(cond))};
                },
            };
        };

        auto forbid_object_id = [](realm_query_cond_e cond) {
            return [=](ObjectId) {
                throw std::logic_error{util::format("%1 does not support object IDs", cond_as_str(cond))};
            };
        };

        auto only_strings_or_binary = [&](realm_query_cond_e cond) {
            return util::overloaded{
                forbid_null(cond),      forbid_bool(cond),      forbid_numeric(cond),
                forbid_timestamp(cond), forbid_object_id(cond), forbid_link(cond),
            };
        };

        auto only_numeric = [&](realm_query_cond_e cond) {
            return util::overloaded{
                forbid_null(cond),      forbid_bool(cond),      forbid_binary(cond), forbid_string(cond),
                forbid_timestamp(cond), forbid_object_id(cond), forbid_link(cond),
            };
        };

        auto only_links = [&](realm_query_cond_e cond) {
            return util::overloaded{
                forbid_null(cond),      forbid_bool(cond),      forbid_binary(cond),  forbid_string(cond),
                forbid_timestamp(cond), forbid_object_id(cond), forbid_numeric(cond),
            };
        };

        auto expect_args = [&](realm_query_cond_e cond, size_t expected, size_t actual) {
            if (expected != actual) {
                throw std::logic_error{
                    util::format("%1 expects %2 value arguments (got %3)", cond_as_str(cond), expected, actual)};
            }
        };

        switch (cond) {
            case RLM_QUERY_EQUAL: {
                expect_args(RLM_QUERY_EQUAL, 1, num_values);
                auto visitor = util::overloaded{
                    forbid_link(RLM_QUERY_EQUAL), // FIXME?
                    [&](StringData val) {
                        q.equal(lhs_key, val, case_sensitive);
                    },
                    [&](BinaryData val) {
                        q.equal(lhs_key, val, case_sensitive);
                    },
                    [&](auto val) {
                        q.equal(lhs_key, val);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_NOT_EQUAL: {
                expect_args(RLM_QUERY_NOT_EQUAL, 1, num_values);
                auto visitor = util::overloaded{
                    forbid_link(RLM_QUERY_EQUAL), // FIXME?
                    [&](StringData val) {
                        q.not_equal(lhs_key, val, case_sensitive);
                    },
                    [&](BinaryData val) {
                        q.not_equal(lhs_key, val, case_sensitive);
                    },
                    [&](auto val) {
                        q.not_equal(lhs_key, val);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_GREATER: {
                expect_args(RLM_QUERY_GREATER, 1, num_values);
                auto visitor = util::overloaded{
                    forbid_null(RLM_QUERY_GREATER),
                    forbid_string(RLM_QUERY_GREATER),
                    forbid_binary(RLM_QUERY_GREATER),
                    forbid_link(RLM_QUERY_GREATER),
                    [&](auto val) {
                        q.greater(lhs_key, val);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_GREATER_EQUAL: {
                expect_args(RLM_QUERY_GREATER_EQUAL, 1, num_values);
                auto visitor = util::overloaded{
                    forbid_null(RLM_QUERY_GREATER_EQUAL),
                    forbid_string(RLM_QUERY_GREATER_EQUAL),
                    forbid_binary(RLM_QUERY_GREATER_EQUAL),
                    forbid_link(RLM_QUERY_GREATER_EQUAL),
                    [&](auto val) {
                        q.greater_equal(lhs_key, val);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_LESS: {
                expect_args(RLM_QUERY_LESS, 1, num_values);
                auto visitor = util::overloaded{
                    forbid_null(RLM_QUERY_LESS),
                    forbid_string(RLM_QUERY_LESS),
                    forbid_binary(RLM_QUERY_LESS),
                    forbid_link(RLM_QUERY_LESS),
                    [&](auto val) {
                        q.less(lhs_key, val);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_LESS_EQUAL: {
                expect_args(RLM_QUERY_LESS_EQUAL, 1, num_values);
                auto visitor = util::overloaded{
                    forbid_null(RLM_QUERY_LESS_EQUAL),
                    forbid_string(RLM_QUERY_LESS_EQUAL),
                    forbid_binary(RLM_QUERY_LESS_EQUAL),
                    forbid_link(RLM_QUERY_LESS_EQUAL),
                    [&](auto val) {
                        q.less_equal(lhs_key, val);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_BETWEEN: {
                expect_args(RLM_QUERY_BETWEEN, 2, num_values);
                auto visitor = util::overloaded{
                    only_numeric(RLM_QUERY_BETWEEN),
                    [&](auto val) {
                        auto visitor2 = util::overloaded{
                            [&](decltype(val) val2) {
                                q.between(lhs_key, val, val2);
                            },
                            [&](auto) {
                                throw LogicError{LogicError::type_mismatch};
                            },
                        };
                        visit_realm_value(values[1], visitor2);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_CONTAINS: {
                expect_args(RLM_QUERY_CONTAINS, 1, num_values);
                auto visitor = util::overloaded{
                    only_strings_or_binary(RLM_QUERY_CONTAINS),
                    [&](auto val) {
                        q.contains(lhs_key, val, case_sensitive);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_LIKE: {
                expect_args(RLM_QUERY_LIKE, 1, num_values);
                auto visitor = util::overloaded{
                    only_strings_or_binary(RLM_QUERY_LIKE),
                    [&](auto val) {
                        q.like(lhs_key, val, case_sensitive);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_BEGINS_WITH: {
                expect_args(RLM_QUERY_BEGINS_WITH, 1, num_values);
                auto visitor = util::overloaded{
                    only_strings_or_binary(RLM_QUERY_BEGINS_WITH),
                    [&](auto val) {
                        q.begins_with(lhs_key, val, case_sensitive);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_ENDS_WITH: {
                expect_args(RLM_QUERY_ENDS_WITH, 1, num_values);
                auto visitor = util::overloaded{
                    only_strings_or_binary(RLM_QUERY_ENDS_WITH),
                    [&](auto val) {
                        q.ends_with(lhs_key, val, case_sensitive);
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
            case RLM_QUERY_LINKS_TO: {
                expect_args(RLM_QUERY_LINKS_TO, 1, num_values);
                auto visitor = util::overloaded{
                    only_links(RLM_QUERY_LINKS_TO),
                    [&](ObjLink val) {
                        // FIXME: Support TypedLink queries.
                        auto table = q.get_table();
                        if (auto target = table->get_link_target(lhs_key)) {
                            if (target->get_key() != val.get_table_key()) {
                                // FIXME: Better exception type.
                                throw std::logic_error{"Wrong link target table"};
                            }
                        }
                        else {
                            // Not a link column; Query handles this.
                        }
                        q.links_to(lhs_key, val.get_obj_key());
                    },
                };
                visit_realm_value(values[0], visitor);
                break;
            }
        }

        return validate_query(query);
    });
}

RLM_API bool realm_query_push_query(realm_query_t* query, realm_query_t* rhs)
{
    return wrap_err([&]() {
        // FIXME: This copies - do we want that?
        query->ptr->and_query(*rhs->ptr);
        return true;
    });
}

#endif // RLM_ENABLE_QUERY_BUILDER_API