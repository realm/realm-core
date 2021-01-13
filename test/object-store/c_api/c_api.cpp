#include "catch2/catch.hpp"

#include <realm.h>

#include <realm/util/file.hpp>

#include <cstring>

extern "C" int realm_c_api_tests(const char* file);

template <class T>
T checked(T x)
{
    if (!x) {
        realm_rethrow_last_error();
    }
    return x;
}

realm_value_t rlm_str_val(const char* str)
{
    realm_value_t val;
    val.type = RLM_TYPE_STRING;
    val.string = realm_string_t{str, std::strlen(str)};
    return val;
}

realm_value_t rlm_int_val(int64_t n)
{
    realm_value_t val;
    val.type = RLM_TYPE_INT;
    val.integer = n;
    return val;
}

realm_value_t rlm_null()
{
    realm_value_t null;
    null.type = RLM_TYPE_NULL;
    return null;
}

std::string rlm_stdstr(realm_value_t val)
{
    CHECK(val.type == RLM_TYPE_STRING);
    return std::string(val.string.data, 0, val.string.size);
}

struct RealmReleaseDeleter {
    void operator()(void* ptr)
    {
        realm_release(ptr);
    }
};

template <class T>
using CPtr = std::unique_ptr<T, RealmReleaseDeleter>;

template <class T>
CPtr<T> make_cptr(T* ptr)
{
    return CPtr<T>{ptr};
}

template <class T>
CPtr<T> clone_cptr(const CPtr<T>& ptr)
{
    void* clone = realm_clone(ptr.get());
    return CPtr<T>{static_cast<T*>(clone)};
}

template <class T>
CPtr<T> clone_cptr(const T* ptr)
{
    void* clone = realm_clone(ptr);
    return CPtr<T>{static_cast<T*>(clone)};
}

static void check_err(realm_errno_e e)
{
    realm_error_t err;
    CHECK(realm_get_last_error(&err));
    CHECK(err.error == e);
}

TEST_CASE("C API (C)") {
    const char* file_name = "c_api_test_c.realm";

    // FIXME: Use a better test file guard.
    if (realm::util::File::exists(file_name)) {
        CHECK(realm::util::File::try_remove(file_name));
    }

    CHECK(realm_c_api_tests(file_name) == 0);
}

TEST_CASE("C API") {
    const char* file_name = "c_api_test.realm";

    // FIXME: Use a better test file guard.
    if (realm::util::File::exists(file_name)) {
        CHECK(realm::util::File::try_remove(file_name));
    }

    realm_t* realm;
    {
        const realm_class_info_t classes[2] = {
            {
                "foo",
                "", // primary key
                3,  // properties
                0,  // computed_properties
                RLM_INVALID_CLASS_KEY,
                RLM_CLASS_NORMAL,
            },
            {
                "bar",
                "int", // primary key
                2,     // properties
                0,     // computed properties,
                RLM_INVALID_CLASS_KEY,
                RLM_CLASS_NORMAL,
            },
        };

        const realm_property_info_t foo_properties[3] = {
            {
                "int",
                "",
                RLM_PROPERTY_TYPE_INT,
                RLM_COLLECTION_TYPE_NONE,
                "",
                "",
                RLM_INVALID_PROPERTY_KEY,
                RLM_PROPERTY_NORMAL,
            },
            {
                "str",
                "",
                RLM_PROPERTY_TYPE_STRING,
                RLM_COLLECTION_TYPE_NONE,
                "",
                "",
                RLM_INVALID_PROPERTY_KEY,
                RLM_PROPERTY_NORMAL,
            },
            {
                "bars",
                "",
                RLM_PROPERTY_TYPE_OBJECT,
                RLM_COLLECTION_TYPE_LIST,
                "bar",
                "",
                RLM_INVALID_PROPERTY_KEY,
                RLM_PROPERTY_NORMAL,
            },
        };

        const realm_property_info_t bar_properties[2] = {
            {
                "int",
                "",
                RLM_PROPERTY_TYPE_INT,
                RLM_COLLECTION_TYPE_NONE,
                "",
                "",
                RLM_INVALID_PROPERTY_KEY,
                RLM_PROPERTY_INDEXED | RLM_PROPERTY_PRIMARY_KEY,
            },
            {
                "strings",
                "",
                RLM_PROPERTY_TYPE_STRING,
                RLM_COLLECTION_TYPE_LIST,
                "",
                "",
                RLM_INVALID_PROPERTY_KEY,
                RLM_PROPERTY_NORMAL | RLM_PROPERTY_NULLABLE,
            },
        };

        const realm_property_info_t* class_properties[2] = {foo_properties, bar_properties};

        auto schema = realm_schema_new(classes, 2, class_properties);
        CHECK(checked(schema));
        CHECK(checked(realm_schema_validate(schema)));

        auto config = realm_config_new();
        CHECK(checked(realm_config_set_path(config, "c_api_test.realm")));
        CHECK(checked(realm_config_set_schema_mode(config, RLM_SCHEMA_MODE_AUTOMATIC)));

        realm = realm_open(config);
        CHECK(realm_update_schema(realm, schema));
        CHECK(checked(realm));

        CHECK(!realm_equals(realm, nullptr));

        auto realm2 = realm_open(config);
        CHECK(checked(realm2));
        CHECK(!realm_equals(realm, realm2));
        realm_release(realm2);

        realm_release(schema);
        realm_release(config);
    }

    CHECK(realm_get_num_classes(realm) == 2);

    SECTION("schema is set after opening") {
        const realm_class_info_t baz = {
            "baz",
            "", // primary key
            1,  // properties
            0,  // computed_properties
            RLM_INVALID_CLASS_KEY,
            RLM_CLASS_NORMAL,
        };

        auto int_property = realm_property_info_t{
            "int", "", RLM_PROPERTY_TYPE_INT,    RLM_COLLECTION_TYPE_NONE,
            "",    "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NORMAL,
        };
        realm_property_info_t* baz_properties = &int_property;

        // get class count
        size_t num_classes = realm_get_num_classes(realm);
        realm_class_key_t* out_keys = (realm_class_key_t*)malloc(sizeof(realm_class_key_t) * num_classes);
        // get class keys
        realm_get_class_keys(realm, out_keys, num_classes, nullptr);
        realm_class_info_t* classes = (realm_class_info_t*)malloc(sizeof(realm_class_info_t) * (num_classes + 1));
        const realm_property_info_t** properties =
            (const realm_property_info_t**)malloc(sizeof(realm_property_info_t*) * (num_classes + 1));
        // iterating through each class, "recreate" the old schema
        for (size_t i = 0; i < num_classes; i++) {
            realm_get_class(realm, out_keys[i], &classes[i]);
            size_t out_n;
            realm_get_class_properties(realm, out_keys[i], nullptr, 0, &out_n);
            realm_property_info_t* out_props = (realm_property_info_t*)malloc(sizeof(realm_property_info_t) * out_n);
            realm_get_class_properties(realm, out_keys[i], out_props, out_n, nullptr);
            properties[i] = out_props;
        }
        // add the new class and its properties to the arrays
        classes[num_classes] = baz;

        properties[num_classes] = baz_properties;

        // create a new schema and update the realm
        auto new_schema = realm_schema_new(classes, num_classes + 1, properties);
        CHECK(realm_update_schema(realm, new_schema));
        auto new_num_classes = realm_get_num_classes(realm);
        CHECK(new_num_classes == (num_classes + 1));

        bool found;
        realm_class_info_t baz_info;
        CHECK(checked(realm_find_class(realm, "baz", &found, &baz_info)));
        CHECK(found);
        realm_property_info_t baz_int_property;
        CHECK(checked(realm_find_property(realm, baz_info.key, "int", &found, &baz_int_property)));
        CHECK(found);

        free(out_keys);
        free(classes);
        for (size_t i = 0; i < num_classes; i++) {
            free((realm_property_info_t*)properties[i]);
        }
        free(properties);
        realm_release(new_schema);
    }

    SECTION("schema validates") {
        auto schema = realm_get_schema(realm);
        CHECK(checked(schema));
        CHECK(checked(realm_schema_validate(schema)));

        auto schema2 = realm_get_schema(realm);
        CHECK(checked(schema2));
        CHECK(realm_equals(schema, schema2));
        realm_release(schema2);
        realm_release(schema);
    }

    auto write = [&](auto&& f) {
        checked(realm_begin_write(realm));
        f();
        checked(realm_commit(realm));
        checked(realm_refresh(realm));
    };

    bool found = false;

    realm_class_info_t foo_info, bar_info;
    CHECK(checked(realm_find_class(realm, "foo", &found, &foo_info)));
    CHECK(found);
    CHECK(checked(realm_find_class(realm, "bar", &found, &bar_info)));
    CHECK(found);

    realm_property_info_t foo_int_property, foo_str_property, foo_bars_property;
    realm_property_info_t bar_int_property, bar_strings_property;

    CHECK(checked(realm_find_property(realm, foo_info.key, "int", &found, &foo_int_property)));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, foo_info.key, "str", &found, &foo_str_property)));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, foo_info.key, "bars", &found, &foo_bars_property)));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, bar_info.key, "int", &found, &bar_int_property)));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, bar_info.key, "strings", &found, &bar_strings_property)));
    CHECK(found);

    SECTION("missing primary key") {
        write([&]() {
            auto p = realm_object_create(realm, bar_info.key);
            CHECK(!p);
            check_err(RLM_ERR_MISSING_PRIMARY_KEY);
        });
    }

    SECTION("wrong primary key type") {
        write([&]() {
            auto p = realm_object_create_with_primary_key(realm, bar_info.key, rlm_str_val("Hello"));
            CHECK(!p);
            check_err(RLM_ERR_WRONG_PRIMARY_KEY_TYPE);
        });

        write([&]() {
            auto p = realm_object_create_with_primary_key(realm, bar_info.key, rlm_null());
            CHECK(!p);
            check_err(RLM_ERR_PROPERTY_NOT_NULLABLE);
        });
    }

    SECTION("objects") {
        CPtr<realm_object_t> obj1;
        CPtr<realm_object_t> obj2;
        write([&]() {
            obj1 = checked(make_cptr(realm_object_create(realm, foo_info.key)));
            CHECK(obj1);
            CHECK(checked(realm_set_value(obj1.get(), foo_int_property.key, rlm_int_val(123), false)));
            CHECK(checked(realm_set_value(obj1.get(), foo_str_property.key, rlm_str_val("Hello, World!"), false)));
            obj2 = checked(make_cptr(realm_object_create_with_primary_key(realm, bar_info.key, rlm_int_val(1))));
            CHECK(obj2);
        });

        CHECK(!realm_equals(obj1.get(), obj2.get()));
        CHECK(realm_equals(obj1.get(), obj1.get()));

        size_t num_foos, num_bars;
        CHECK(checked(realm_get_num_objects(realm, foo_info.key, &num_foos)));
        CHECK(checked(realm_get_num_objects(realm, bar_info.key, &num_bars)));

        SECTION("find with primary key") {
            bool found = false;

            auto p =
                checked(make_cptr(realm_object_find_with_primary_key(realm, bar_info.key, rlm_int_val(1), &found)));
            CHECK(found);
            auto p_key = realm_object_get_key(p.get());
            auto obj2_key = realm_object_get_key(obj2.get());
            CHECK(p_key == obj2_key);
            CHECK(realm_equals(p.get(), obj2.get()));

            // Check that finding by type-mismatched values just find nothing.
            CHECK(!realm_object_find_with_primary_key(realm, bar_info.key, rlm_null(), &found));
            CHECK(!found);
            CHECK(!realm_object_find_with_primary_key(realm, bar_info.key, rlm_str_val("a"), &found));
            CHECK(!found);
        }

        SECTION("query basics") {
            auto arg = rlm_str_val("Hello, World!");
            auto q = make_cptr(checked(realm_query_parse(realm, foo_info.key, "str == $0", 1, &arg)));
            size_t count;
            CHECK(checked(realm_query_count(q.get(), &count)));
            CHECK(count == 1);

            // find first:
            realm_value_t found_value = rlm_null();
            bool found;
            CHECK(checked(realm_query_find_first(q.get(), &found_value, &found)));
            CHECK(found);
            CHECK(found_value.type == RLM_TYPE_LINK);
            CHECK(found_value.link.target_table == foo_info.key);
            CHECK(found_value.link.target == realm_object_get_key(obj1.get()));

            auto r = make_cptr(checked(realm_query_find_all(q.get())));

            // results count:
            CHECK(checked(realm_results_count(r.get(), &count)));
            CHECK(count == 1);

            realm_value_t value;

            // min:
            CHECK(checked(realm_results_min(r.get(), foo_int_property.key, &value, &found)));
            CHECK(found);
            CHECK(value.type == RLM_TYPE_INT);
            CHECK(value.integer == 123);

            // max:
            CHECK(checked(realm_results_max(r.get(), foo_int_property.key, &value, &found)));
            CHECK(found);
            CHECK(value.type == RLM_TYPE_INT);
            CHECK(value.integer == 123);

            // sum:
            CHECK(checked(realm_results_sum(r.get(), foo_int_property.key, &value, &found)));
            CHECK(found);
            CHECK(value.type == RLM_TYPE_INT);
            CHECK(value.integer == 123);

            // average:
            CHECK(checked(realm_results_average(r.get(), foo_int_property.key, &value, &found)));
            CHECK(found);
            CHECK(value.type == RLM_TYPE_DOUBLE);
            CHECK(value.dnum == 123.0);
        }

        SECTION("set wrong field type") {
            write([&]() {
                CHECK(!realm_set_value(obj1.get(), foo_int_property.key, rlm_null(), false));
                check_err(RLM_ERR_PROPERTY_NOT_NULLABLE);

                CHECK(!realm_set_value(obj1.get(), foo_int_property.key, rlm_str_val("a"), false));
                check_err(RLM_ERR_PROPERTY_TYPE_MISMATCH);
            });
        }

        SECTION("delete causes invalidation errors") {
            write([&]() {
                // Get a list instance for later
                auto list = checked(make_cptr(realm_get_list(obj1.get(), foo_bars_property.key)));

                CHECK(checked(realm_object_delete(obj1.get())));
                CHECK(!realm_object_is_valid(obj1.get()));

                realm_clear_last_error();
                CHECK(!realm_object_delete(obj1.get()));
                check_err(RLM_ERR_INVALIDATED_OBJECT);

                realm_clear_last_error();
                CHECK(!realm_set_value(obj1.get(), foo_int_property.key, rlm_int_val(123), false));
                check_err(RLM_ERR_INVALIDATED_OBJECT);

                realm_clear_last_error();
                auto list2 = realm_get_list(obj1.get(), foo_bars_property.key);
                CHECK(!list2);
                check_err(RLM_ERR_INVALIDATED_OBJECT);

                size_t size;
                CHECK(!realm_list_size(list.get(), &size));
                check_err(RLM_ERR_INVALIDATED_OBJECT);
            });
        }

        SECTION("lists") {
            SECTION("nullable strings") {
                auto strings = checked(make_cptr(realm_get_list(obj2.get(), bar_strings_property.key)));
                CHECK(strings);

                realm_value_t a = rlm_str_val("a");
                realm_value_t b = rlm_str_val("b");
                realm_value_t c = rlm_null();

                SECTION("insert, then get") {
                    write([&]() {
                        CHECK(checked(realm_list_insert(strings.get(), 0, a)));
                        CHECK(checked(realm_list_insert(strings.get(), 1, b)));
                        CHECK(checked(realm_list_insert(strings.get(), 2, c)));

                        realm_value_t a2, b2, c2;
                        CHECK(checked(realm_list_get(strings.get(), 0, &a2)));
                        CHECK(checked(realm_list_get(strings.get(), 1, &b2)));
                        CHECK(checked(realm_list_get(strings.get(), 2, &c2)));

                        CHECK(rlm_stdstr(a2) == "a");
                        CHECK(rlm_stdstr(b2) == "b");
                        CHECK(c2.type == RLM_TYPE_NULL);
                    });
                }

                SECTION("equality") {
                    auto strings2 = checked(make_cptr(realm_get_list(obj2.get(), bar_strings_property.key)));
                    CHECK(strings2);
                    CHECK(realm_equals(strings.get(), strings2.get()));

                    write([&]() {
                        auto obj3 = checked(
                            make_cptr(realm_object_create_with_primary_key(realm, bar_info.key, rlm_int_val(2))));
                        CHECK(obj3);
                        auto strings3 = checked(make_cptr(realm_get_list(obj3.get(), bar_strings_property.key)));
                        CHECK(!realm_equals(strings.get(), strings3.get()));
                    });
                }
            }

            SECTION("links") {
                CPtr<realm_list_t> bars;

                write([&]() {
                    bars = checked(make_cptr(realm_get_list(obj1.get(), foo_bars_property.key)));
                    auto bar_link = realm_object_as_link(obj2.get());
                    realm_value_t bar_link_val;
                    bar_link_val.type = RLM_TYPE_LINK;
                    bar_link_val.link = bar_link;
                    CHECK(checked(realm_list_insert(bars.get(), 0, bar_link_val)));
                    CHECK(checked(realm_list_insert(bars.get(), 1, bar_link_val)));
                    size_t size;
                    CHECK(checked(realm_list_size(bars.get(), &size)));
                    CHECK(size == 2);
                });

                SECTION("get") {
                    realm_value_t val;
                    CHECK(checked(realm_list_get(bars.get(), 0, &val)));
                    CHECK(val.type == RLM_TYPE_LINK);
                    CHECK(val.link.target_table == bar_info.key);
                    CHECK(val.link.target == realm_object_get_key(obj2.get()));

                    CHECK(checked(realm_list_get(bars.get(), 1, &val)));
                    CHECK(val.type == RLM_TYPE_LINK);
                    CHECK(val.link.target_table == bar_info.key);
                    CHECK(val.link.target == realm_object_get_key(obj2.get()));

                    auto result = realm_list_get(bars.get(), 2, &val);
                    CHECK(!result);
                    check_err(RLM_ERR_INDEX_OUT_OF_BOUNDS);
                }

                SECTION("set wrong type") {
                    write([&]() {
                        auto foo2 = make_cptr(realm_object_create(realm, foo_info.key));
                        CHECK(foo2);
                        realm_value_t foo2_link_val;
                        foo2_link_val.type = RLM_TYPE_LINK;
                        foo2_link_val.link = realm_object_as_link(foo2.get());

                        CHECK(!realm_list_set(bars.get(), 0, foo2_link_val));
                        check_err(RLM_ERR_INVALID_ARGUMENT);
                    });
                }
            }

            SECTION("notifications") {
                struct State {
                    CPtr<realm_collection_changes_t> changes;
                    CPtr<realm_async_error_t> error;
                };

                State state;

                auto on_change = [](void* userdata, const realm_collection_changes_t* changes) {
                    auto* state = static_cast<State*>(userdata);
                    state->changes = clone_cptr(changes);
                };

                auto on_error = [](void* userdata, const realm_async_error_t* err) {
                    auto* state = static_cast<State*>(userdata);
                    state->error = clone_cptr(err);
                };

                CPtr<realm_list_t> strings = checked(make_cptr(realm_get_list(obj2.get(), bar_strings_property.key)));

                auto str1 = rlm_str_val("a");
                auto str2 = rlm_str_val("b");
                auto null = rlm_null();

                auto require_change = [&]() {
                    auto token = make_cptr(checked(realm_list_add_notification_callback(
                        strings.get(), &state, nullptr, on_change, on_error, nullptr)));
                    checked(realm_refresh(realm));
                    return token;
                };

                SECTION("insertion sends a change callback") {
                    auto token = require_change();
                    write([&]() {
                        checked(realm_list_insert(strings.get(), 0, str1));
                        checked(realm_list_insert(strings.get(), 1, str2));
                        checked(realm_list_insert(strings.get(), 2, null));
                    });
                    CHECK(!state.error);
                    CHECK(state.changes);

                    size_t num_deletion_ranges, num_insertion_ranges, num_modification_ranges, num_moves;
                    realm_collection_changes_get_num_ranges(state.changes.get(), &num_deletion_ranges,
                                                            &num_insertion_ranges, &num_modification_ranges,
                                                            &num_moves);
                    CHECK(num_deletion_ranges == 0);
                    CHECK(num_insertion_ranges == 1);
                    CHECK(num_modification_ranges == 0);
                    CHECK(num_moves == 0);

                    realm_index_range_t insertion_range;
                    realm_collection_changes_get_ranges(state.changes.get(), nullptr, 0, &insertion_range, 1, nullptr,
                                                        0, nullptr, 0, nullptr, 0);
                    CHECK(insertion_range.from == 0);
                    CHECK(insertion_range.to == 3);
                }

                SECTION("insertion, deletion, modification, modification after") {
                    write([&]() {
                        checked(realm_list_insert(strings.get(), 0, str1));
                        checked(realm_list_insert(strings.get(), 1, str2));
                        checked(realm_list_insert(strings.get(), 2, str1));
                    });

                    auto token = require_change();

                    write([&]() {
                        checked(realm_list_erase(strings.get(), 1));
                        checked(realm_list_insert(strings.get(), 0, null));
                        checked(realm_list_insert(strings.get(), 1, null));

                        // This element was previously at 0, and ends up at 2.
                        checked(realm_list_set(strings.get(), 2, str1));
                    });
                    CHECK(!state.error);
                    CHECK(state.changes);

                    size_t num_deletion_ranges, num_insertion_ranges, num_modification_ranges, num_moves;
                    realm_collection_changes_get_num_ranges(state.changes.get(), &num_deletion_ranges,
                                                            &num_insertion_ranges, &num_modification_ranges,
                                                            &num_moves);
                    CHECK(num_deletion_ranges == 1);
                    CHECK(num_insertion_ranges == 1);
                    CHECK(num_modification_ranges == 1);
                    CHECK(num_moves == 0);

                    realm_index_range_t deletions, insertions, modifications, modifications_after;
                    realm_collection_changes_get_ranges(state.changes.get(), &deletions, 1, &insertions, 1,
                                                        &modifications, 1, &modifications_after, 1, nullptr, 0);
                    CHECK(deletions.from == 1);
                    CHECK(deletions.to == 2);

                    CHECK(insertions.from == 0);
                    CHECK(insertions.to == 2);

                    CHECK(modifications.from == 0);
                    CHECK(modifications.to == 1);

                    CHECK(modifications_after.from == 2);
                    CHECK(modifications_after.to == 3);
                }
            }
        }

        SECTION("notifications") {
            struct State {
                CPtr<realm_object_changes_t> changes;
                CPtr<realm_async_error_t> error;
            };

            State state;

            auto on_change = [](void* userdata, const realm_object_changes_t* changes) {
                auto state = static_cast<State*>(userdata);
                state->changes = clone_cptr(changes);
            };

            auto on_error = [](void* userdata, const realm_async_error_t* err) {
                auto state = static_cast<State*>(userdata);
                state->error = clone_cptr(err);
            };

            auto require_change = [&]() {
                auto token = make_cptr(realm_object_add_notification_callback(obj1.get(), &state, nullptr, on_change,
                                                                              on_error, nullptr));
                checked(realm_refresh(realm));
                return token;
            };

            SECTION("deleting the object sends a change notification") {
                auto token = require_change();
                write([&]() {
                    checked(realm_object_delete(obj1.get()));
                });
                CHECK(!state.error);
                CHECK(state.changes);
                bool deleted = realm_object_changes_is_deleted(state.changes.get());
                CHECK(deleted);
            }

            SECTION("modifying the object sends a change notification for the object, and for the changed column") {
                auto token = require_change();
                write([&]() {
                    checked(realm_set_value(obj1.get(), foo_int_property.key, rlm_int_val(999), false));
                    checked(realm_set_value(obj1.get(), foo_str_property.key, rlm_str_val("aaa"), false));
                });
                CHECK(!state.error);
                CHECK(state.changes);
                bool deleted = realm_object_changes_is_deleted(state.changes.get());
                CHECK(!deleted);
                size_t num_modified = realm_object_changes_get_num_modified_properties(state.changes.get());
                CHECK(num_modified == 2);
                realm_property_key_t modified_keys[2];
                size_t n = realm_object_changes_get_modified_properties(state.changes.get(), modified_keys, 2);
                CHECK(n == 2);
                CHECK(modified_keys[0] == foo_int_property.key);
                CHECK(modified_keys[1] == foo_str_property.key);
            }
        }
    }

    realm_release(realm);
}
