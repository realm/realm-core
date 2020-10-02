#include "catch2/catch.hpp"

#include <realm/realm.h>

#include <realm/util/file.hpp>

#include <cstring>

#define RLM_STR(x)                                                                                                   \
    realm_string_t                                                                                                   \
    {                                                                                                                \
        x, std::strlen(x)                                                                                            \
    }

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
    realm_value_t null = {0};
    null.type = RLM_TYPE_NULL;
    return null;
}

std::string rlm_stdstr(realm_value_t val)
{
    CHECK(val.type == RLM_TYPE_STRING);
    return std::string(val.string.data, 0, val.string.size);
}

template <class T>
using CPtr = std::unique_ptr<T, void (*)(const void*)>;

template <class T>
CPtr<T> make_cptr(T* ptr)
{
    return std::unique_ptr<T, void (*)(const void*)>{ptr, realm_release};
}

TEST_CASE("C API")
{
    const char* file_name = "c_api_test.realm";

    // FIXME: Use a better test file guard.
    if (realm::util::File::exists(file_name)) {
        CHECK(realm::util::File::try_remove(file_name));
    }

    realm_t* realm;
    {
        const realm_class_info_t classes[2] = {
            {
                RLM_STR("foo"),
                RLM_STR(""), // primary key
                3,           // properties
                0,           // computed_properties
                realm_table_key_t{},
                RLM_CLASS_NORMAL,
            },
            {
                RLM_STR("bar"),
                RLM_STR(""), // primary key
                2,           // properties
                0,           // computed properties,
                realm_table_key{},
                RLM_CLASS_NORMAL,
            },
        };

        const realm_property_info_t foo_properties[3] = {
            {
                RLM_STR("int"),
                RLM_STR(""),
                RLM_PROPERTY_TYPE_INT,
                RLM_COLLECTION_TYPE_NONE,
                RLM_STR(""),
                RLM_STR(""),
                realm_col_key_t{},
                RLM_PROPERTY_NORMAL,
            },
            {
                RLM_STR("str"),
                RLM_STR(""),
                RLM_PROPERTY_TYPE_STRING,
                RLM_COLLECTION_TYPE_NONE,
                RLM_STR(""),
                RLM_STR(""),
                realm_col_key_t{},
                RLM_PROPERTY_NORMAL,
            },
            {
                RLM_STR("bars"),
                RLM_STR(""),
                RLM_PROPERTY_TYPE_OBJECT,
                RLM_COLLECTION_TYPE_LIST,
                RLM_STR("bar"),
                RLM_STR(""),
                realm_col_key_t{},
                RLM_PROPERTY_NORMAL,
            },
        };

        const realm_property_info_t bar_properties[2] = {
            {
                RLM_STR("int"),
                RLM_STR(""),
                RLM_PROPERTY_TYPE_INT,
                RLM_COLLECTION_TYPE_NONE,
                RLM_STR(""),
                RLM_STR(""),
                realm_col_key_t{},
                RLM_PROPERTY_INDEXED,
            },
            {
                RLM_STR("strings"),
                RLM_STR(""),
                RLM_PROPERTY_TYPE_STRING,
                RLM_COLLECTION_TYPE_LIST,
                RLM_STR(""),
                RLM_STR(""),
                realm_col_key_t{},
                RLM_PROPERTY_NORMAL | RLM_PROPERTY_NULLABLE,
            },
        };

        const realm_property_info_t* class_properties[2] = {foo_properties, bar_properties};

        auto schema = realm_schema_new(classes, 2, class_properties);
        CHECK(checked(schema));
        CHECK(checked(realm_schema_validate(schema)));

        auto config = realm_config_new();
        CHECK(checked(realm_config_set_path(config, RLM_STR("c_api_test.realm"))));
        CHECK(checked(realm_config_set_schema(config, schema)));
        CHECK(checked(realm_config_set_schema_mode(config, RLM_SCHEMA_MODE_AUTOMATIC)));
        CHECK(checked(realm_config_set_schema_version(config, 1)));

        realm = realm_open(config);
        CHECK(checked(realm));
        realm_release(schema);
        realm_release(config);
    }

    SECTION("schema validates")
    {
        auto schema = realm_get_schema(realm);
        CHECK(checked(schema));
        CHECK(checked(realm_schema_validate(schema)));
        realm_release(schema);
    }

    CHECK(realm_get_num_classes(realm) == 2);
    bool found = false;

    realm_class_info_t foo_info, bar_info;
    CHECK(checked(realm_find_class(realm, RLM_STR("foo"), &found, &foo_info)));
    CHECK(found);
    CHECK(checked(realm_find_class(realm, RLM_STR("bar"), &found, &bar_info)));
    CHECK(found);

    realm_property_info_t foo_properties[3];
    realm_property_info_t bar_properties[2];

    CHECK(checked(realm_find_property(realm, foo_info.key, RLM_STR("int"), &found, &foo_properties[0])));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, foo_info.key, RLM_STR("str"), &found, &foo_properties[1])));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, foo_info.key, RLM_STR("bars"), &found, &foo_properties[2])));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, bar_info.key, RLM_STR("int"), &found, &bar_properties[0])));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, bar_info.key, RLM_STR("strings"), &found, &bar_properties[1])));
    CHECK(found);

    CHECK(checked(realm_begin_write(realm)));

    auto obj1 = realm_create_object(realm, foo_info.key);
    CHECK(checked(obj1));
    CHECK(checked(realm_set_value(obj1, foo_properties[0].key, rlm_int_val(123), false)));
    CHECK(checked(realm_set_value(obj1, foo_properties[1].key, rlm_str_val("Hello, World!"), false)));
    auto obj2 = realm_create_object(realm, bar_info.key);
    CHECK(checked(obj2));

    SECTION("lists")
    {
        auto bars = checked(realm_get_list(obj1, foo_properties[2].key));
        auto bar_link = realm_object_as_link(obj2);
        realm_value_t bar_link_val;
        bar_link_val.type = RLM_TYPE_LINK;
        bar_link_val.link = bar_link;
        CHECK(checked(realm_list_insert(bars, 0, bar_link_val)));
        CHECK(checked(realm_list_insert(bars, 1, bar_link_val)));
        CHECK(realm_list_size(bars) == 2);

        SECTION("nullable strings")
        {
            auto strings = make_cptr(realm_get_list(obj2, bar_properties[1].key));
            CHECK(strings);

            realm_value_t a = rlm_str_val("a");
            realm_value_t b = rlm_str_val("b");
            realm_value_t c = rlm_null();

            SECTION("insert, then get")
            {
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
            }
        }

        SECTION("links")
        {
            SECTION("get")
            {
                realm_value_t val;
                CHECK(checked(realm_list_get(bars, 0, &val)));
                CHECK(val.type == RLM_TYPE_LINK);
                CHECK(val.link.target_table.table_key == bar_info.key.table_key);
                CHECK(val.link.target.obj_key == realm_object_get_key(obj2).obj_key);

                CHECK(checked(realm_list_get(bars, 1, &val)));
                CHECK(val.type == RLM_TYPE_LINK);
                CHECK(val.link.target_table.table_key == bar_info.key.table_key);
                CHECK(val.link.target.obj_key == realm_object_get_key(obj2).obj_key);
            }

            SECTION("get out of bounds")
            {
                realm_value_t val;
                CHECK(!realm_list_get(bars, 3, &val));
                realm_error_t err;
                CHECK(realm_get_last_error(&err));
                CHECK(err.error == RLM_ERR_INDEX_OUT_OF_BOUNDS);
            }

            SECTION("set wrong type")
            {
                auto foo2 = make_cptr(realm_create_object(realm, foo_info.key));
                CHECK(foo2);
                realm_value_t foo2_link_val;
                foo2_link_val.type = RLM_TYPE_LINK;
                foo2_link_val.link = realm_object_as_link(foo2.get());

                CHECK(!realm_list_set(bars, 0, foo2_link_val));
                realm_error_t err;
                CHECK(realm_get_last_error(&err));
                CHECK(err.error == RLM_ERR_INVALID_ARGUMENT);
            }
        }

        realm_release(bars);
    }

    realm_release(obj1);
    realm_release(obj2);
    CHECK(checked(realm_commit(realm)));

    size_t num_foos, num_bars;
    CHECK(checked(realm_get_num_objects(realm, foo_info.key, &num_foos)));
    CHECK(checked(realm_get_num_objects(realm, bar_info.key, &num_bars)));

    realm_release(realm);
}