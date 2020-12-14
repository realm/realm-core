#if defined(NDEBUG)
#undef NDEBUG
#endif

#include <realm/realm.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static realm_string_t rlm_str(const char* str)
{
    realm_string_t s;
    s.data = str;
    s.size = strlen(str);
    return s;
}

static realm_table_key_t dummy_table_key()
{
    realm_table_key_t key;
    key.table_key = (uint32_t)-1;
    return key;
}

static realm_col_key_t dummy_col_key()
{
    realm_col_key_t key;
    key.col_key = -1;
    return key;
}

#define CHECK_ERROR()                                                                                                \
    do {                                                                                                             \
        realm_error_t err;                                                                                           \
        if (realm_get_last_error(&err)) {                                                                            \
            fprintf(stderr, "ERROR: %s\n", err.message.data);                                                        \
            return 1;                                                                                                \
        }                                                                                                            \
    } while (0)

static void check_property_info_equal(const realm_property_info_t* lhs, const realm_property_info_t* rhs)
{
    assert(strncmp(lhs->name.data, rhs->name.data, lhs->name.size) == 0);
    assert(strncmp(lhs->public_name.data, rhs->public_name.data, lhs->public_name.size) == 0);
    assert(lhs->type == rhs->type);
    assert(lhs->collection_type == rhs->collection_type);
    assert(strncmp(lhs->link_target.data, rhs->link_target.data, lhs->link_target.size) == 0);
    assert(strncmp(lhs->link_origin_property_name.data, rhs->link_origin_property_name.data,
                   lhs->link_origin_property_name.size) == 0);
    assert(lhs->key.col_key == rhs->key.col_key);
    assert(lhs->flags == rhs->flags);
}

int realm_c_api_tests(const char* file)
{
    const realm_class_info_t def_classes[] = {
        {
            .name = rlm_str("Foo"),
            .primary_key = rlm_str(""),
            .num_properties = 3,
            .num_computed_properties = 0,
            .key = dummy_table_key(),
            .flags = RLM_CLASS_NORMAL,
        },
        {
            .name = rlm_str("Bar"),
            .primary_key = rlm_str("int"),
            .num_properties = 2,
            .num_computed_properties = 0,
            .key = dummy_table_key(),
            .flags = RLM_CLASS_NORMAL,
        },
    };

    const realm_property_info_t def_foo_properties[] = {
        {
            .name = rlm_str("int"),
            .public_name = rlm_str(""),
            .type = RLM_PROPERTY_TYPE_INT,
            .collection_type = RLM_COLLECTION_TYPE_NONE,
            .link_target = rlm_str(""),
            .link_origin_property_name = rlm_str(""),
            .key = dummy_col_key(),
            .flags = RLM_PROPERTY_NORMAL,
        },
        {
            .name = rlm_str("str"),
            .public_name = rlm_str(""),
            .type = RLM_PROPERTY_TYPE_STRING,
            .collection_type = RLM_COLLECTION_TYPE_NONE,
            .link_target = rlm_str(""),
            .link_origin_property_name = rlm_str(""),
            .key = dummy_col_key(),
            .flags = RLM_PROPERTY_NORMAL,
        },
        {
            .name = rlm_str("bars"),
            .public_name = rlm_str(""),
            .type = RLM_PROPERTY_TYPE_OBJECT,
            .collection_type = RLM_COLLECTION_TYPE_LIST,
            .link_target = rlm_str("Bar"),
            .link_origin_property_name = rlm_str(""),
            .key = dummy_col_key(),
            .flags = RLM_PROPERTY_NORMAL,
        },
    };

    const realm_property_info_t def_bar_properties[] = {
        {
            .name = rlm_str("int"),
            .public_name = rlm_str(""),
            .type = RLM_PROPERTY_TYPE_INT,
            .collection_type = RLM_COLLECTION_TYPE_NONE,
            .link_target = rlm_str(""),
            .link_origin_property_name = rlm_str(""),
            .key = dummy_col_key(),
            .flags = RLM_PROPERTY_INDEXED | RLM_PROPERTY_PRIMARY_KEY,
        },
        {
            .name = rlm_str("strings"),
            .public_name = rlm_str(""),
            .type = RLM_PROPERTY_TYPE_STRING,
            .collection_type = RLM_COLLECTION_TYPE_LIST,
            .link_target = rlm_str(""),
            .link_origin_property_name = rlm_str(""),
            .key = dummy_col_key(),
            .flags = RLM_PROPERTY_NORMAL | RLM_PROPERTY_NULLABLE,
        },
    };

    const realm_property_info_t* def_class_properties[] = {def_foo_properties, def_bar_properties};

    realm_schema_t* schema = realm_schema_new(def_classes, 2, def_class_properties);
    CHECK_ERROR();

    realm_config_t* config = realm_config_new();
    realm_string_t path = rlm_str(file);
    realm_config_set_schema(config, schema);
    realm_config_set_schema_mode(config, RLM_SCHEMA_MODE_AUTOMATIC);
    realm_config_set_schema_version(config, 1);
    realm_config_set_path(config, path);

    realm_t* realm = realm_open(config);
    CHECK_ERROR();

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

    realm_table_key_t class_keys[2];
    size_t n;
    realm_get_class_keys(realm, class_keys, 2, &n);
    CHECK_ERROR();
    assert(n == 2);

    bool found = false;
    realm_class_info_t foo_info;
    realm_class_info_t bar_info;

    realm_find_class(realm, rlm_str("Foo"), &found, &foo_info);
    CHECK_ERROR();
    assert(found);
    assert(foo_info.num_properties == 3);
    assert(foo_info.key.table_key == class_keys[0].table_key || foo_info.key.table_key == class_keys[1].table_key);

    realm_find_class(realm, rlm_str("Bar"), &found, &bar_info);
    CHECK_ERROR();
    assert(found);
    assert(bar_info.num_properties == 2);
    assert(bar_info.key.table_key == class_keys[0].table_key || bar_info.key.table_key == class_keys[1].table_key);

    realm_class_info_t dummy_info;
    realm_find_class(realm, rlm_str("DoesNotExist"), &found, &dummy_info);
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
    realm_find_property(realm, foo_info.key, rlm_str("int"), &found, &foo_int);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, foo_info.key, rlm_str("str"), &found, &foo_str);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, foo_info.key, rlm_str("bars"), &found, &foo_bars);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, bar_info.key, rlm_str("int"), &found, &bar_int);
    CHECK_ERROR();
    assert(found);
    realm_find_property(realm, bar_info.key, rlm_str("strings"), &found, &bar_strings);
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
    assert(err.error == RLM_ERR_OTHER_EXCEPTION); // FIXME: RLM_ERR_NOT_IN_A_WRITE_TRANSACTION
    realm_clear_last_error();

    realm_object_t* foo_1;
    {
        realm_begin_write(realm);
        CHECK_ERROR();

        foo_1 = realm_object_create(realm, foo_info.key);
        CHECK_ERROR();
        assert(realm_object_is_valid(foo_1));

        realm_obj_key_t foo_1_key = realm_object_get_key(foo_1);

        realm_table_key_t foo_1_table = realm_object_get_table(foo_1);
        assert(foo_1_table.table_key == foo_info.key.table_key);

        realm_link_t foo_1_link = realm_object_as_link(foo_1);
        assert(foo_1_link.target.obj_key == foo_1_key.obj_key);
        assert(foo_1_link.target_table.table_key == foo_1_table.table_key);

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