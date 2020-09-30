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
                1,           // properties
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

        const realm_property_info_t bar_properties[1] = {
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
    auto schema = realm_get_schema(realm);
    CHECK(checked(schema));
    CHECK(checked(realm_schema_validate(schema)));
    realm_release(schema);

    CHECK(realm_get_num_classes(realm) == 2);
    bool found = false;

    realm_class_info_t foo_info, bar_info;
    CHECK(checked(realm_find_class(realm, RLM_STR("foo"), &found, &foo_info)));
    CHECK(found);
    CHECK(checked(realm_find_class(realm, RLM_STR("bar"), &found, &bar_info)));
    CHECK(found);

    realm_property_info_t foo_properties[3];
    realm_property_info_t bar_properties[1];

    CHECK(checked(realm_find_property(realm, foo_info.key, RLM_STR("int"), &found, &foo_properties[0])));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, foo_info.key, RLM_STR("str"), &found, &foo_properties[1])));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, foo_info.key, RLM_STR("bars"), &found, &foo_properties[2])));
    CHECK(found);
    CHECK(checked(realm_find_property(realm, bar_info.key, RLM_STR("int"), &found, &bar_properties[0])));
    CHECK(found);

    CHECK(checked(realm_begin_write(realm)));

    auto obj1 = realm_create_object(realm, foo_info.key);
    CHECK(checked(obj1));
    CHECK(checked(realm_set_value(obj1, foo_properties[0].key, rlm_int_val(123), false)));
    CHECK(checked(realm_set_value(obj1, foo_properties[1].key, rlm_str_val("Hello, World!"), false)));
    auto obj2 = realm_create_object(realm, bar_info.key);
    CHECK(checked(obj2));

    auto bars = checked(realm_get_list(obj1, foo_properties[2].key));
    auto bar_link = realm_object_as_link(obj2);
    realm_value_t bar_link_val;
    bar_link_val.type = RLM_TYPE_LINK;
    bar_link_val.link = bar_link;
    CHECK(checked(realm_list_insert(bars, 0, bar_link_val)));
    CHECK(checked(realm_list_insert(bars, 1, bar_link_val)));

    realm_release(bars);
    realm_release(obj1);
    realm_release(obj2);
    CHECK(checked(realm_commit(realm)));

    size_t num_foos, num_bars;
    CHECK(checked(realm_get_num_objects(realm, foo_info.key, &num_foos)));
    CHECK(checked(realm_get_num_objects(realm, bar_info.key, &num_bars)));

    realm_release(realm);
}