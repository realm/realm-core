#include <realm/realm.h>
#include <realm/object-store/c_api/conversion.hpp>

#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_accessor.hpp>

#include <realm/parser/parser.hpp>
#include <realm/parser/query_builder.hpp>

#include <realm/util/overloaded.hpp>

using namespace realm;

#if REALM_PLATFORM_APPLE && !defined(RLM_NO_THREAD_LOCAL)
#define RLM_NO_THREAD_LOCAL
#endif

#if defined(RLM_NO_THREAD_LOCAL)
#include <pthread.h>
#endif

namespace {

struct NotClonableException : std::exception {
    const char* what() const noexcept
    {
        return "Not clonable";
    }
};

struct ImmutableException : std::exception {
    const char* what() const noexcept
    {
        return "Immutable object";
    }
};

struct WrapC {
    virtual ~WrapC() {}

    virtual WrapC* clone() const
    {
        throw NotClonableException();
    }

    virtual bool is_frozen() const
    {
        return false;
    }
};

#if !defined(RLM_NO_THREAD_LOCAL)

thread_local std::exception_ptr g_last_exception;

static void set_last_exception(std::exception_ptr eptr)
{
    g_last_exception = eptr;
}

static std::exception_ptr* get_last_exception()
{
    return &g_last_exception;
}

#else // RLM_NO_THREAD_LOCAL

static pthread_key_t g_last_exception_key;
static pthread_once_t g_last_exception_key_init_once = PTHREAD_ONCE_INIT;

static void destroy_last_exception(void* ptr)
{
    auto p = static_cast<std::exception_ptr*>(ptr);
    delete p;
}

static void init_last_exception_key()
{
    pthread_key_create(&g_last_exception_key, destroy_last_exception);
}

static void set_last_exception(std::exception_ptr eptr)
{
    pthread_once(&g_last_exception_key_init_once, init_last_exception_key);
    void* ptr = pthread_getspecific(g_last_exception_key);
    std::exception_ptr* p;
    if (!ptr) {
        p = new std::exception_ptr;
        pthread_setspecific(g_last_exception_key, p);
    }
    else {
        p = static_cast<std::exception_ptr*>(ptr);
    }
    *p = eptr;
}

static std::exception_ptr* get_last_exception()
{
    pthread_once(&g_last_exception_key_init_once, init_last_exception_key);
    void* ptr = pthread_getspecific(g_last_exception_key);
    return static_cast<std::exception_ptr*>(ptr);
}

#endif // RLM_NO_THREAD_LOCAL

template <class F>
auto wrap_err(F&& f) -> decltype(std::declval<F>()())
{
    try {
        return f();
    }
    catch (...) {
        set_last_exception(std::current_exception());
        return decltype(std::declval<F>()()){};
    }
}

template <class T>
inline T* cast_ptr(void* ptr)
{
    auto rptr = static_cast<WrapC*>(ptr);
    REALM_ASSERT(dynamic_cast<T*>(rptr) != nullptr);
    return static_cast<T*>(rptr);
}

template <class T>
inline const T* cast_ptr(const void* ptr)
{
    auto rptr = static_cast<const WrapC*>(ptr);
    REALM_ASSERT(dynamic_cast<const T*>(rptr) != nullptr);
    return static_cast<const T*>(rptr);
}

template <class T>
inline T& cast_ref(void* ptr)
{
    return *cast_ptr<T>(ptr);
}

template <class T>
inline const T& cast_ref(const void* ptr)
{
    return *cast_ptr<T>(ptr);
}

} // namespace

struct realm_config : WrapC, Realm::Config {
    using Realm::Config::Config;
};

struct realm_schema : WrapC {
    std::unique_ptr<Schema> owned;
    const Schema* ptr = nullptr;

    realm_schema(std::unique_ptr<Schema> o, const Schema* ptr = nullptr)
        : owned(std::move(o))
        , ptr(ptr ? ptr : owned.get())
    {
    }

    explicit realm_schema(const Schema* ptr)
        : ptr(ptr)
    {
    }

    realm_schema_t* clone() const override
    {
        auto o = std::make_unique<Schema>(*ptr);
        return new realm_schema_t{std::move(o)};
    }
};

struct shared_realm : WrapC, SharedRealm {
    shared_realm(SharedRealm rlm)
        : SharedRealm{std::move(rlm)}
    {
    }

    shared_realm* clone() const override
    {
        return new shared_realm{*this};
    }
};

struct realm_object : WrapC, Object {
    explicit realm_object(Object obj)
        : Object(std::move(obj))
    {
    }

    realm_object* clone() const override
    {
        return new realm_object{*this};
    }

    bool is_frozen() const override
    {
        return Object::is_frozen();
    }
};

struct realm_list : WrapC, List {
    explicit realm_list(List list)
        : List(std::move(list))
    {
    }

    realm_list* clone() const override
    {
        return new realm_list{*this};
    }

    bool is_frozen() const override
    {
        return List::is_frozen();
    }
};

struct realm_parsed_query : WrapC, parser::ParserResult {
    explicit realm_parsed_query(parser::ParserResult result)
        : parser::ParserResult(std::move(result))
    {
    }

    realm_parsed_query* clone() const override
    {
        return new realm_parsed_query{*this};
    }
};

struct realm_query : WrapC {
    std::unique_ptr<Query> ptr;
    std::weak_ptr<Realm> weak_realm;

    explicit realm_query(Query query, std::weak_ptr<Realm> realm)
        : ptr(std::make_unique<Query>(std::move(query)))
        , weak_realm(realm)
    {
    }

    realm_query* clone() const override
    {
        return new realm_query{*ptr, weak_realm};
    }
};

struct realm_results : WrapC, Results {
    explicit realm_results(Results results)
        : Results(std::move(results))
    {
    }

    realm_results* clone() const override
    {
        return new realm_results{static_cast<const Results&>(*this)};
    }

    bool is_frozen() const override
    {
        return Results::is_frozen();
    }
};

struct realm_descriptor_ordering : WrapC, DescriptorOrdering {
    realm_descriptor_ordering() = default;

    explicit realm_descriptor_ordering(DescriptorOrdering o)
        : DescriptorOrdering(std::move(o))
    {
    }

    realm_descriptor_ordering* clone() const override
    {
        return new realm_descriptor_ordering{static_cast<const DescriptorOrdering&>(*this)};
    }
};

RLM_API const char* realm_get_library_version()
{
    return REALM_VERSION_STRING;
}

RLM_API void realm_get_library_version_numbers(int* out_major, int* out_minor, int* out_patch, const char** out_extra)
{
    *out_major = REALM_VERSION_MAJOR;
    *out_minor = REALM_VERSION_MINOR;
    *out_patch = REALM_VERSION_PATCH;
    *out_extra = REALM_VERSION_EXTRA;
}

RLM_API bool realm_get_last_error(realm_error_t* err)
{
    std::exception_ptr* ptr = get_last_exception();
    if (ptr && *ptr) {
        err->kind.code = 0;

        auto populate_error = [&](const std::exception& ex, realm_errno_e error_number) {
            err->error = error_number;
            err->message.data = ex.what();
            err->message.size = std::strlen(err->message.data);
            set_last_exception(std::current_exception());
        };

        try {
            std::rethrow_exception(*ptr);
        }
        catch (const NotClonableException& ex) {
            populate_error(ex, RLM_ERR_NOT_CLONABLE);
        }
        catch (const List::OutOfBoundsIndexException& ex) {
            populate_error(ex, RLM_ERR_INDEX_OUT_OF_BOUNDS);
        }
        catch (const std::invalid_argument& ex) {
            populate_error(ex, RLM_ERR_INVALID_ARGUMENT);
        }
        catch (const std::bad_alloc& ex) {
            populate_error(ex, RLM_ERR_OUT_OF_MEMORY);
        }
        catch (const std::exception& ex) {
            populate_error(ex, RLM_ERR_OTHER_EXCEPTION);
        }
        // FIXME: Handle more exception types.
        catch (...) {
            err->error = RLM_ERR_UNKNOWN;
            err->message.data = "Unknown error";
            err->message.size = std::strlen(err->message.data);
            set_last_exception(std::current_exception());
        }
        return true;
    }
    return false;
}

RLM_EXPORT void realm_rethrow_last_error()
{
    std::exception_ptr* ptr = get_last_exception();
    if (ptr && *ptr) {
        std::rethrow_exception(*ptr);
    }
}

RLM_API bool realm_clear_last_error()
{
    std::exception_ptr* ptr = get_last_exception();
    if (ptr && *ptr) {
        *ptr = std::exception_ptr{};
        return true;
    }
    return false;
}

RLM_API void realm_release(const void* ptr)
{
    if (!ptr)
        return;
    delete cast_ptr<WrapC>(ptr);
}

RLM_API void* realm_clone(const void* ptr)
{
    return cast_ptr<WrapC>(ptr)->clone();
}

RLM_API bool realm_is_frozen(const void* ptr)
{
    return cast_ptr<WrapC>(ptr)->is_frozen();
}

RLM_API realm_config_t* realm_config_new()
{
    return new realm_config_t{};
}

RLM_API bool realm_config_set_path(realm_config_t* config, realm_string_t path)
{
    return wrap_err([&]() {
        config->path = capi_to_std(path);
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

RLM_API bool realm_config_set_max_number_of_active_versions(realm_config_t* config, size_t n)
{
    return wrap_err([=]() {
        config->max_number_of_active_versions = n;
        return true;
    });
}

RLM_API realm_t* realm_open(const realm_config_t* config)
{
    return wrap_err([&]() {
        return new shared_realm{Realm::get_shared_realm(*config)};
    });
}

RLM_API realm_t* _realm_from_native_ptr(const void* pshared_ptr, size_t n)
{
    REALM_ASSERT_RELEASE(n == sizeof(std::shared_ptr<Realm>));
    auto ptr = static_cast<const std::shared_ptr<Realm>*>(pshared_ptr);
    return new shared_realm{*ptr};
}

RLM_API bool realm_close(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->close();
        return true;
    });
}

RLM_API bool realm_begin_write(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->begin_transaction();
        return true;
    });
}

RLM_API bool realm_commit(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->commit_transaction();
        return true;
    });
}

RLM_API bool realm_rollback(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->cancel_transaction();
        return true;
    });
}

RLM_API bool realm_refresh(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->refresh();
        return true;
    });
}

RLM_API realm_t* realm_freeze(realm_t* realm)
{
    return wrap_err([&]() {
        auto& p = **realm;
        return new realm_t{p.freeze()};
    });
}

RLM_API bool realm_compact(realm_t* realm, bool* did_compact)
{
    return wrap_err([&]() {
        auto& p = **realm;
        *did_compact = p.compact();
        return true;
    });
}

RLM_API realm_schema_t* realm_schema_new(const realm_class_info_t* classes, size_t num_classes,
                                         const realm_property_info** class_properties)
{
    return wrap_err([&]() {
        std::vector<ObjectSchema> object_schemas;
        object_schemas.reserve(num_classes);

        for (size_t i = 0; i < num_classes; ++i) {
            const auto& class_info = classes[i];
            auto props_ptr = class_properties[i];
            auto computed_props_ptr = props_ptr + class_info.num_properties;

            ObjectSchema object_schema;
            object_schema.name = capi_to_std(class_info.name);
            object_schema.primary_key = capi_to_std(class_info.primary_key);
            object_schema.is_embedded = ObjectSchema::IsEmbedded{bool(class_info.flags & RLM_CLASS_EMBEDDED)};
            object_schema.persisted_properties.reserve(class_info.num_properties);
            object_schema.computed_properties.reserve(class_info.num_computed_properties);

            for (size_t j = 0; j < class_info.num_properties; ++j) {
                Property prop = from_capi(props_ptr[j]);
                object_schema.persisted_properties.push_back(std::move(prop));
            }

            for (size_t j = 0; j < class_info.num_computed_properties; ++j) {
                Property prop = from_capi(computed_props_ptr[j]);
                object_schema.computed_properties.push_back(std::move(prop));
            }

            object_schemas.push_back(std::move(object_schema));
        }

        auto schema = new realm_schema(std::make_unique<Schema>(std::move(object_schemas)));
        return schema;
    });
}

RLM_API const realm_schema_t* realm_get_schema(const realm_t* realm)
{
    return wrap_err([&]() {
        auto& rlm = *realm;
        return new realm_schema_t{&rlm->schema()};
    });
}

RLM_API bool realm_schema_validate(const realm_schema_t* schema)
{
    return wrap_err([&]() {
        schema->ptr->validate();
        return true;
    });
}

RLM_API size_t realm_get_num_classes(const realm_t* realm)
{
    size_t max = std::numeric_limits<size_t>::max();
    size_t n = 0;
    auto success = realm_get_class_keys(realm, nullptr, max, &n);
    REALM_ASSERT(success);
    return n;
}

RLM_API bool realm_get_class_keys(const realm_t* realm, realm_table_key_t* out_keys, size_t max, size_t* out_n)
{
    return wrap_err([&]() {
        const auto& shared_realm = **realm;
        const auto& schema = shared_realm.schema();
        if (out_keys) {
            size_t i = 0;
            for (auto& os : schema) {
                if (i >= max)
                    break;
                out_keys[i++] = to_capi(os.table_key);
            }
            if (out_n)
                *out_n = i;
        }
        else {
            if (out_n) {
                *out_n = schema.size();
            }
        }
        return true;
    });
}

RLM_API bool realm_find_class(const realm_t* realm, realm_string_t name, bool* out_found,
                              realm_class_info_t* out_class_info)
{
    return wrap_err([&]() {
        const auto& schema = (*realm)->schema();
        auto it = schema.find(from_capi(name));
        if (it != schema.end()) {
            if (out_found)
                *out_found = true;
            if (out_class_info)
                *out_class_info = to_capi(*it);
        }
        else {
            if (out_found)
                *out_found = false;
        }
        return true;
    });
}

static inline const ObjectSchema& schema_for_table(const realm_t* realm, realm_table_key_t key)
{
    auto& shared_realm = **realm;
    auto table_key = from_capi(key);

    // Validate the table key.
    shared_realm.read_group().get_table(table_key);
    const auto& schema = shared_realm.schema();

    // FIXME: Faster lookup than linear search.
    for (auto& os : schema) {
        if (os.table_key == table_key) {
            return os;
        }
    }

    // FIXME: Proper exception type.
    throw std::logic_error{"Class not in schema"};
}

RLM_API bool realm_get_class(const realm_t* realm, realm_table_key_t key, realm_class_info_t* out_class_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(realm, key);
        *out_class_info = to_capi(os);
        return true;
    });
}

RLM_API bool realm_get_class_properties(const realm_t* realm, realm_table_key_t key,
                                        realm_property_info_t* out_properties, size_t max, size_t* out_n)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(realm, key);

        if (out_properties) {
            size_t i = 0;

            for (auto& prop : os.persisted_properties) {
                if (i >= max)
                    break;
                out_properties[i++] = to_capi(prop);
            }

            for (auto& prop : os.computed_properties) {
                if (i >= max)
                    break;
                out_properties[i++] = to_capi(prop);
            }

            if (out_n) {
                *out_n = i;
            }
        }
        else {
            if (out_n) {
                *out_n = os.persisted_properties.size() + os.computed_properties.size();
            }
        }
        return true;
    });
}

RLM_API bool realm_get_property_keys(const realm_t* realm, realm_table_key_t key, realm_col_key_t* out_keys,
                                     size_t max, size_t* out_n)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(realm, key);

        if (out_keys) {
            size_t i = 0;

            for (auto& prop : os.persisted_properties) {
                if (i >= max)
                    break;
                out_keys[i++] = to_capi(prop.column_key);
            }

            for (auto& prop : os.computed_properties) {
                if (i >= max)
                    break;
                out_keys[i++] = to_capi(prop.column_key);
            }

            if (out_n) {
                *out_n = i;
            }
        }
        else {
            if (out_n) {
                *out_n = os.persisted_properties.size() + os.persisted_properties.size();
            }
        }
        return true;
    });
}

RLM_API bool realm_get_property(const realm_t* realm, realm_table_key_t class_key, realm_col_key_t key,
                                realm_property_info_t* out_property_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(realm, class_key);
        auto col_key = from_capi(key);

        // FIXME: We can do better than linear search.

        for (auto& prop : os.persisted_properties) {
            if (prop.column_key == col_key) {
                *out_property_info = to_capi(prop);
                return true;
            }
        }

        for (auto& prop : os.computed_properties) {
            if (prop.column_key == col_key) {
                *out_property_info = to_capi(prop);
                return true;
            }
        }

        // FIXME: Proper exception type.
        throw std::logic_error{"Invalid column key for this class"};
    });
}

RLM_API bool realm_find_property(const realm_t* realm, realm_table_key_t class_key, realm_string_t name,
                                 bool* out_found, realm_property_info_t* out_property_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(realm, class_key);
        auto prop = os.property_for_name(from_capi(name));

        if (prop) {
            if (out_found)
                *out_found = true;
            if (out_property_info)
                *out_property_info = to_capi(*prop);
        }
        else {
            if (out_found)
                *out_found = false;
        }

        return true;
    });
}

RLM_API bool realm_find_property_by_public_name(const realm_t* realm, realm_table_key_t class_key,
                                                realm_string_t public_name, bool* out_found,
                                                realm_property_info_t* out_property_info)
{
    return wrap_err([&]() {
        auto& os = schema_for_table(realm, class_key);
        auto prop = os.property_for_public_name(from_capi(public_name));

        if (prop) {
            if (out_found)
                *out_found = true;
            if (out_property_info)
                *out_property_info = to_capi(*prop);
        }
        else {
            if (out_found)
                *out_found = false;
        }

        return true;
    });
}

RLM_API bool realm_get_num_objects(const realm_t* realm, realm_table_key_t key, size_t* out_count)
{
    return wrap_err([&]() {
        auto& rlm = **realm;
        auto table = rlm.read_group().get_table(from_capi(key));
        if (out_count)
            *out_count = table->size();
        return true;
    });
}

RLM_API realm_object_t* realm_get_object(const realm_t* realm, realm_table_key_t tbl_key, realm_obj_key_t obj_key)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto table_key = from_capi(tbl_key);
        auto table = shared_realm->read_group().get_table(table_key);
        auto obj = table->get_object(from_capi(obj_key));
        auto object = Object{shared_realm, std::move(obj)};
        return new realm_object_t{std::move(object)};
    });
}

RLM_API realm_object_t* realm_find_object_with_primary_key(const realm_t* realm, realm_table_key_t class_key,
                                                           realm_value_t pk, bool* out_found)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto table_key = from_capi(class_key);
        auto table = shared_realm->read_group().get_table(table_key);
        auto obj_key = table->find_primary_key(from_capi(pk));
        if (obj_key) {
            if (out_found)
                *out_found = true;
            auto obj = table->get_object(obj_key);
            return new realm_object_t{Object{shared_realm, std::move(obj)}};
        }
        else {
            if (out_found)
                *out_found = false;
            return static_cast<realm_object_t*>(nullptr);
        }
    });
}

RLM_API realm_object_t* realm_create_object(realm_t* realm, realm_table_key_t table_key)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto tblkey = from_capi(table_key);
        auto table = shared_realm->read_group().get_table(tblkey);
        auto obj = table->create_object();
        auto object = Object{shared_realm, std::move(obj)};
        return new realm_object_t{std::move(object)};
    });
}

RLM_API realm_object_t* realm_create_object_with_primary_key(realm_t* realm, realm_table_key_t table_key,
                                                             realm_value_t pk)
{
    return wrap_err([&]() {
        auto& shared_realm = *realm;
        auto tblkey = from_capi(table_key);
        auto table = shared_realm->read_group().get_table(tblkey);
        // FIXME: Provide did_create?
        auto pkval = from_capi(pk);
        auto obj = table->create_object_with_primary_key(pkval);
        auto object = Object{shared_realm, std::move(obj)};
        return new realm_object_t{std::move(object)};
    });
}

RLM_API realm_object_t* _realm_object_from_native_copy(const void* pobj, size_t n)
{
    REALM_ASSERT_RELEASE(n == sizeof(Object));

    return wrap_err([&]() {
        auto pobject = static_cast<const Object*>(pobj);
        return new realm_object_t{*pobject};
    });
}

RLM_API realm_object_t* _realm_object_from_native_move(void* pobj, size_t n)
{
    REALM_ASSERT_RELEASE(n == sizeof(Object));

    return wrap_err([&]() {
        auto pobject = static_cast<Object*>(pobj);
        return new realm_object_t{std::move(*pobject)};
    });
}

RLM_API void* _realm_object_get_native_ptr(realm_object_t* obj)
{
    return static_cast<Object*>(obj);
}

RLM_API bool realm_object_is_valid(const realm_object_t* obj)
{
    return obj->is_valid();
}

RLM_API realm_obj_key_t realm_object_get_key(const realm_object_t* obj)
{
    return to_capi(obj->obj().get_key());
}

RLM_API realm_link_t realm_object_as_link(const realm_object_t* object)
{
    auto obj = object->obj();
    auto table = obj.get_table();
    auto table_key = table->get_key();
    auto obj_key = obj.get_key();
    return realm_link_t{to_capi(table_key), to_capi(obj_key)};
}

RLM_API bool realm_get_value(const realm_object_t* obj, realm_col_key_t col, realm_value_t* out_value)
{
    return wrap_err([&]() {
        auto col_key = from_capi(col);
        if (col_key.is_collection()) {
            // FIXME: Proper exception type.
            throw std::logic_error("Accessing collection property as value.");
        }
        auto o = obj->obj();
        auto val = o.get_any(col_key);
        *out_value = to_capi(val);
        return true;
    });
}

RLM_API bool realm_set_value(realm_object_t* obj, realm_col_key_t col, realm_value_t new_value, bool is_default)
{
    return wrap_err([&]() {
        auto col_key = from_capi(col);
        if (col_key.is_collection()) {
            // FIXME: Proper exception type.
            throw std::logic_error("Accessing collection property as value.");
        }
        auto o = obj->obj();
        o.set_any(col_key, from_capi(new_value), is_default);
        return true;
    });
}

RLM_API realm_list_t* realm_get_list(realm_object_t* object, realm_col_key_t key)
{
    return wrap_err([&]() {
        auto obj = object->obj();
        auto table = obj.get_table();
        auto col_key = from_capi(key);
        table->report_invalid_key(col_key);

        if (!col_key.is_list()) {
            // FIXME: Proper exception type.
            throw std::logic_error{"Not a list property"};
        }

        return new realm_list_t{List{object->get_realm(), std::move(obj), col_key}};
    });
}

RLM_API size_t realm_list_size(const realm_list_t* list)
{
    return list->size();
}

RLM_API bool realm_list_get_property(const realm_list_t* list, realm_property_info_t* out_property_info)
{
    static_cast<void>(list);
    static_cast<void>(out_property_info);
    REALM_TERMINATE("Not implemented yet.");
}

/// Convert a Mixed that potentially contains an ObjKey from a link list to a
/// Mixed containing an ObjLink.
static inline Mixed link_to_typed_link(Mixed value, const List& list)
{
    if (!value.is_null() && value.get_type() == type_Link) {
        auto col_key = list.get_parent_column_key();
        REALM_ASSERT(col_key.get_type() == col_type_LinkList ||
                     (col_key.get_type() == col_type_Link && col_key.is_list()));
        REALM_ASSERT(list.get_type() == (PropertyType::Object | PropertyType::Array));

        // Get the target table key.
        auto& shared_realm = *list.get_realm();
        auto source_table = shared_realm.read_group().get_table(list.get_parent_table_key());
        auto target_table = source_table->get_link_target(col_key);
        value = ObjLink{target_table->get_key(), value.get<ObjKey>()};
    }
    return value;
}

/// Convert a Mixed that potentially contains an ObjLink to a Mixed containing an ObjKey.
static inline Mixed typed_link_to_link(Mixed value)
{
    if (!value.is_null() && value.get_type() == type_TypedLink) {
        auto link = value.get<ObjLink>();
        value = link.get_obj_key();
    }
    return value;
}

RLM_API bool realm_list_get(const realm_list_t* list, size_t index, realm_value_t* out_value)
{
    return wrap_err([&]() {
        auto val = list->get_any(index);
        val = link_to_typed_link(val, *list);
        if (out_value)
            *out_value = to_capi(val);
        return true;
    });
}

template <class F>
auto value_or_object(const std::shared_ptr<Realm>& realm, PropertyType val_type, Mixed val, F&& f)
{
    // FIXME: Object Store has poor support for heterogeneous lists, and in
    // particular it relies on Core to check that the input types to
    // `List::insert()` etc. match the list property type. Once that is fixed /
    // made safer, this logic should move into Object Store.

    if (val.is_null()) {
        if (!is_nullable(val_type)) {
            // FIXME: Defer this exception to Object Store, which can produce
            // nicer message.
            throw std::invalid_argument("NULL in non-nullable field/list.");
        }

        // Produce a util::none matching the property type.
        return switch_on_type(val_type, [&](auto ptr) {
            using T = std::remove_cv_t<std::remove_pointer_t<decltype(ptr)>>;
            T nothing{};
            return f(nothing);
        });
    }

    PropertyType base_type = (val_type & ~PropertyType::Flags);

    // Note: The following checks PropertyType::Any on the assumption that it
    // will become un-deprecated when Mixed is exposed in Object Store.

    switch (val.get_type()) {
        case type_Int: {
            if (base_type != PropertyType::Int && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<int64_t>());
        }
        case type_Bool: {
            if (base_type != PropertyType::Bool && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<bool>());
        }
        case type_String: {
            if (base_type != PropertyType::String && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<StringData>());
        }
        case type_Binary: {
            if (base_type != PropertyType::Data && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<BinaryData>());
        }
        case type_Timestamp: {
            if (base_type != PropertyType::Date && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<Timestamp>());
        }
        case type_Float: {
            if (base_type != PropertyType::Float && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<float>());
        }
        case type_Double: {
            if (base_type != PropertyType::Double && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<double>());
        }
        case type_Decimal: {
            if (base_type != PropertyType::Decimal && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<Decimal128>());
        }
        case type_ObjectId: {
            if (base_type != PropertyType::ObjectId && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<ObjectId>());
        }
        case type_TypedLink: {
            if (base_type != PropertyType::Object && base_type != PropertyType::Any)
                throw std::invalid_argument{"Type mismatch"};
            // Object Store performs link validation already. Just create an Obj
            // for the link, and pass it on.
            auto link = val.get<ObjLink>();
            auto target_table = realm->read_group().get_table(link.get_table_key());
            auto obj = target_table->get_object(link.get_obj_key());
            return f(std::move(obj));
        }

        case type_Link:
            // Note: from_capi(realm_value_t) never produces an untyped link.
            [[fallthrough]];
        case type_OldTable:
            [[fallthrough]];
        case type_Mixed:
            [[fallthrough]];
        case type_OldDateTime:
            [[fallthrough]];
        case type_LinkList:
            REALM_TERMINATE("Invalid value type.");
    }
}

RLM_API bool realm_list_insert(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        value_or_object(list->get_realm(), list->get_type(), val, [&](auto val) {
            list->insert(index, val);
        });
        return true;
    });
}

RLM_API bool realm_list_set(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        value_or_object(list->get_realm(), list->get_type(), val, [&](auto val) {
            list->set(index, val);
        });
        return true;
    });
}

RLM_API bool realm_list_erase(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->remove(index);
        return true;
    });
}

RLM_API bool realm_list_clear(realm_list_t* list)
{
    return wrap_err([&]() {
        list->remove_all();
        return true;
    });
}

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