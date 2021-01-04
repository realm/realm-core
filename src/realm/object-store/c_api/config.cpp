#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

RLM_API realm_config_t* realm_config_new()
{
    return new realm_config_t{};
}

RLM_API const char* realm_config_get_path(realm_config_t* config)
{
    return wrap_err([&]() {
        return config->path.data();
    });
}

RLM_API bool realm_config_set_path(realm_config_t* config, const char* path)
{
    return wrap_err([&]() {
        config->path = path;
        return true;
    });
}

RLM_API bool realm_config_set_schema(realm_config_t* config, const realm_schema_t* schema)
{
    return wrap_err([&]() {
        config->schema = *schema->ptr;
        return true;
    });
}

RLM_API uint64_t realm_config_get_schema_version(realm_config_t* config)
{
    return wrap_err([&]() {
        return config->schema_version;
    });
}

RLM_API bool realm_config_set_schema_version(realm_config_t* config, uint64_t version)
{
    return wrap_err([&]() {
        config->schema_version = version;
        return true;
    });
}

RLM_API bool realm_config_set_schema_mode(realm_config_t* config, realm_schema_mode_e mode)
{
    return wrap_err([&]() {
        config->schema_mode = from_capi(mode);
        return true;
    });
}

RLM_API bool realm_config_set_migration_function(realm_config_t* config, realm_migration_func_t func, void* userdata)
{
    return wrap_err([=]() {
        auto migration_func = [=](SharedRealm old_realm, SharedRealm new_realm, Schema& schema) {
            auto r1 = new realm_t{std::move(old_realm)};
            auto r2 = new realm_t{std::move(new_realm)};
            auto s = new realm_schema_t{&schema};
            bool success = wrap_err([=]() {
                func(userdata, r1, r2, s);
                return true;
            });
            realm_release(r1);
            realm_release(r2);
            realm_release(s);
            if (!success)
                realm_rethrow_last_error();
        };
        config->migration_function = std::move(migration_func);
        return true;
    });
}

RLM_API bool realm_config_set_data_initialization_function(realm_config_t* config,
                                                           realm_data_initialization_func_t func, void* userdata)
{
    return wrap_err([=]() {
        auto init_func = [=](SharedRealm realm) {
            auto r = new realm_t{std::move(realm)};
            bool success = wrap_err([=]() {
                func(userdata, r);
                return true;
            });
            realm_release(r);
            if (!success)
                realm_rethrow_last_error();
        };
        config->initialization_function = std::move(init_func);
        return true;
    });
}

RLM_API bool realm_config_set_should_compact_on_launch_function(realm_config_t* config,
                                                                realm_should_compact_on_launch_func_t func,
                                                                void* userdata)
{
    return wrap_err([=]() {
        auto should_func = [=](uint64_t total_bytes, uint64_t used_bytes) -> bool {
            return func(userdata, total_bytes, used_bytes);
        };
        config->should_compact_on_launch_function = std::move(should_func);
        return true;
    });
}

RLM_API bool realm_config_set_automatic_change_notifications(realm_config_t* config, bool b)
{
    return wrap_err([=]() {
        config->automatic_change_notifications = b;
        return true;
    });
}

RLM_API bool realm_config_set_scheduler(realm_config_t* config, const realm_scheduler_t* scheduler)
{
    return wrap_err([&]() {
        config->scheduler = *scheduler;
        return true;
    });
}

RLM_API bool realm_config_set_max_number_of_active_versions(realm_config_t* config, size_t n)
{
    return wrap_err([=]() {
        config->max_number_of_active_versions = n;
        return true;
    });
}
