#if defined(NDEBUG)
#undef NDEBUG
#endif

#include <realm.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK_ERROR()                                                                                                \
    do {                                                                                                             \
        realm_error_t err;                                                                                           \
        if (realm_get_last_error(&err)) {                                                                            \
            fprintf(stderr, "ERROR: %s\n", err.message);                                                             \
            return 1;                                                                                                \
        }                                                                                                            \
    } while (0)

static void check_property_info_equal(const realm_property_info_t* lhs, const realm_property_info_t* rhs)
{
    assert(strcmp(lhs->name, rhs->name) == 0);
    assert(strcmp(lhs->public_name, rhs->public_name) == 0);
    assert(lhs->type == rhs->type);
    assert(lhs->collection_type == rhs->collection_type);
    assert(strcmp(lhs->link_target, rhs->link_target) == 0);
    assert(strcmp(lhs->link_origin_property_name, rhs->link_origin_property_name) == 0);
    assert(lhs->key == rhs->key);
    assert(lhs->flags == rhs->flags);
}

int realm_c_api_tests(const char* file)
{
    const realm_class_info_t def_classes[] = {
        {
            .name = "Foo",
            .primary_key = "",
            .num_properties = 3,
            .num_computed_properties = 0,
            .key = RLM_INVALID_CLASS_KEY,
            .flags = RLM_CLASS_NORMAL,
        },
        {
            .name = "Bar",
            .primary_key = "int",
            .num_properties = 2,
            .num_computed_properties = 0,
            .key = RLM_INVALID_CLASS_KEY,
            .flags = RLM_CLASS_NORMAL,
        },
    };

    const realm_property_info_t def_foo_properties[] = {
        {
            .name = "int",
            .public_name = "",
            .type = RLM_PROPERTY_TYPE_INT,
            .collection_type = RLM_COLLECTION_TYPE_NONE,
            .link_target = "",
            .link_origin_property_name = "",
            .key = RLM_INVALID_PROPERTY_KEY,
            .flags = RLM_PROPERTY_NORMAL,
        },
        {
            .name = "str",
            .public_name = "",
            .type = RLM_PROPERTY_TYPE_STRING,
            .collection_type = RLM_COLLECTION_TYPE_NONE,
            .link_target = "",
            .link_origin_property_name = "",
            .key = RLM_INVALID_PROPERTY_KEY,
            .flags = RLM_PROPERTY_NORMAL,
        },
        {
            .name = "bars",
            .public_name = "",
            .type = RLM_PROPERTY_TYPE_OBJECT,
            .collection_type = RLM_COLLECTION_TYPE_LIST,
            .link_target = "Bar",
            .link_origin_property_name = "",
            .key = RLM_INVALID_PROPERTY_KEY,
            .flags = RLM_PROPERTY_NORMAL,
        },
    };

    const realm_property_info_t def_bar_properties[] = {
        {
            .name = "int",
            .public_name = "",
            .type = RLM_PROPERTY_TYPE_INT,
            .collection_type = RLM_COLLECTION_TYPE_NONE,
            .link_target = "",
            .link_origin_property_name = "",
            .key = RLM_INVALID_PROPERTY_KEY,
            .flags = RLM_PROPERTY_INDEXED | RLM_PROPERTY_PRIMARY_KEY,
        },
        {
            .name = "strings",
            .public_name = "",
            .type = RLM_PROPERTY_TYPE_STRING,
            .collection_type = RLM_COLLECTION_TYPE_LIST,
            .link_target = "",
            .link_origin_property_name = "",
            .key = RLM_INVALID_PROPERTY_KEY,
            .flags = RLM_PROPERTY_NORMAL | RLM_PROPERTY_NULLABLE,
        },
    };

    const realm_property_info_t* def_class_properties[] = {def_foo_properties, def_bar_properties};

    realm_schema_t* schema = realm_schema_new(def_classes, 2, def_class_properties);
    CHECK_ERROR();

    realm_config_t* config = realm_config_new();
    realm_config_set_schema(config, schema);
    realm_config_set_schema_mode(config, RLM_SCHEMA_MODE_AUTOMATIC);
    realm_config_set_schema_version(config, 1);
    realm_config_set_path(config, file);

    realm_t* realm = realm_open(config);
    CHECK_ERROR();
    realm_release(config);
    realm_release(schema);

    assert(!realm_is_frozen(realm));
    assert(!realm_is_closed(realm));
    assert(!realm_is_writable(realm));

    {
        realm_begin_write(realm);
        CHECK_ERROR();
        assert(realm_is_writable(realm));
        realm_rollback(realm);
        CHECK_ERROR();
    }

    size_t num_classes = realm_get_num_classes(realm);
    assert(num_classes == 2);

    realm_class_key_t class_keys[2];
    size_t n;
    realm_get_class_keys(realm, class_keys, 2, &n);
    CHECK_ERROR();
    assert(n == 2);

    bool found = false;
    realm_class_info_t foo_info;
    realm_class_info_t bar_info;

    realm_find_class(realm, "Foo", &found, &foo_info);
    CHECK_ERROR();
    assert(found);
    assert(foo_info.num_properties == 3);
    assert(foo_info.key == class_keys[0] || foo_info.key == class_keys[1]);

    realm_find_class(realm, "Bar", &found, &bar_info);
    CHECK_ERROR();
    assert(found);
    assert(bar_info.num_properties == 2);
    assert(bar_info.key == class_keys[0] || bar_info.key == class_keys[1]);

    realm_class_info_t dummy_info;
    realm_find_class(realm, "DoesNotExist", &found, &dummy_info);
    CHECK_ERROR();
    assert(!found);

    realm_property_info_t* foo_properties = malloc(sizeof(realm_property_info_t) * foo_info.num_properties);
    realm_property_info_t* bar_properties = malloc(sizeof(realm_property_info_t) * bar_info.num_properties);

    realm_get_class_properties(realm, foo_info.key, foo_properties, foo_info.num_properties, NULL);
    CHECK_ERROR();
    realm_get_class_properties(realm, bar_info.key, bar_properties, bar_info.num_properties, NULL);
    CHECK_ERROR();

    // Find properties by name.
    realm_property_info_t foo_int, foo_str, foo_bars, bar_int, bar_strings;
    realm_find_property(realm, foo_info.key, "int", &found, &foo_int);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, foo_info.key, "str", &found, &foo_str);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, foo_info.key, "bars", &found, &foo_bars);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, bar_info.key, "int", &found, &bar_int);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, bar_info.key, "strings", &found, &bar_strings);
    CHECK_ERROR();
    assert(found);

    check_property_info_equal(&foo_int, &foo_properties[0]);
    check_property_info_equal(&foo_str, &foo_properties[1]);
    check_property_info_equal(&foo_bars, &foo_properties[2]);
    check_property_info_equal(&bar_int, &bar_properties[0]);
    check_property_info_equal(&bar_strings, &bar_properties[1]);


    // Find properties by key.
    {
        realm_property_info_t foo_int, foo_str, foo_bars, bar_int, bar_strings;

        realm_get_property(realm, foo_info.key, foo_properties[0].key, &foo_int);
        CHECK_ERROR();
        assert(found);
        realm_get_property(realm, foo_info.key, foo_properties[1].key, &foo_str);
        CHECK_ERROR();
        assert(found);
        realm_get_property(realm, foo_info.key, foo_properties[2].key, &foo_bars);
        CHECK_ERROR();
        assert(found);
        realm_get_property(realm, bar_info.key, bar_properties[0].key, &bar_int);
        CHECK_ERROR();
        assert(found);
        realm_get_property(realm, bar_info.key, bar_properties[1].key, &bar_strings);
        CHECK_ERROR();
        assert(found);

        check_property_info_equal(&foo_int, &foo_properties[0]);
        check_property_info_equal(&foo_str, &foo_properties[1]);
        check_property_info_equal(&foo_bars, &foo_properties[2]);
        check_property_info_equal(&bar_int, &bar_properties[0]);
        check_property_info_equal(&bar_strings, &bar_properties[1]);
    }

    size_t num_foos, num_bars;
    realm_get_num_objects(realm, foo_info.key, &num_foos);
    CHECK_ERROR();
    assert(num_foos == 0);
    realm_get_num_objects(realm, bar_info.key, &num_bars);
    CHECK_ERROR();
    assert(num_bars == 0);

    assert(realm_refresh(realm));
    CHECK_ERROR();

    realm_object_create(realm, foo_info.key);
    realm_error_t err;
    assert(realm_get_last_error(&err));
    assert(err.error == RLM_ERR_NOT_IN_A_TRANSACTION);
    realm_clear_last_error();

    realm_object_t* foo_1;
    {
        realm_begin_write(realm);
        CHECK_ERROR();

        foo_1 = realm_object_create(realm, foo_info.key);
        CHECK_ERROR();
        assert(realm_object_is_valid(foo_1));

        realm_object_key_t foo_1_key = realm_object_get_key(foo_1);

        realm_class_key_t foo_1_table = realm_object_get_table(foo_1);
        assert(foo_1_table == foo_info.key);

        realm_link_t foo_1_link = realm_object_as_link(foo_1);
        assert(foo_1_link.target == foo_1_key);
        assert(foo_1_link.target_table == foo_1_table);

        realm_commit(realm);
        CHECK_ERROR();
    }

    assert(realm_object_is_valid(foo_1));

    realm_release(foo_1);

    realm_close(realm);
    CHECK_ERROR();
    assert(realm_is_closed(realm));

    realm_release(realm);
    CHECK_ERROR();

    free(foo_properties);
    free(bar_properties);

    return 0;
}