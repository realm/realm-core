#ifndef REALM_OBJECT_STORE_C_API_UTIL_HPP
#define REALM_OBJECT_STORE_C_API_UTIL_HPP

#include <realm/object-store/c_api/types.hpp>

void set_last_exception(std::exception_ptr eptr);

template <class F>
static inline auto wrap_err(F&& f) -> decltype(std::declval<F>()())
{
    try {
        return f();
    }
    catch (...) {
        set_last_exception(std::current_exception());
        return {};
    };
}

template <class T>
static inline T* cast_ptr(void* ptr)
{
    auto rptr = static_cast<WrapC*>(ptr);
    REALM_ASSERT(dynamic_cast<T*>(rptr) != nullptr);
    return static_cast<T*>(rptr);
}

template <class T>
static inline const T* cast_ptr(const void* ptr)
{
    auto rptr = static_cast<const WrapC*>(ptr);
    REALM_ASSERT(dynamic_cast<const T*>(rptr) != nullptr);
    return static_cast<const T*>(rptr);
}

template <class T>
static inline T& cast_ref(void* ptr)
{
    return *cast_ptr<T>(ptr);
}

template <class T>
static inline const T& cast_ref(const void* ptr)
{
    return *cast_ptr<T>(ptr);
}

static inline const ObjectSchema& schema_for_table(const std::shared_ptr<Realm>& realm, realm_table_key_t key)
{
    auto table_key = from_capi(key);

    // Validate the table key.
    realm->read_group().get_table(table_key);
    const auto& schema = realm->schema();

    // FIXME: Faster lookup than linear search.
    for (auto& os : schema) {
        if (os.table_key == table_key) {
            return os;
        }
    }

    // FIXME: Proper exception type.
    throw std::logic_error{"Class not in schema"};
}

static inline const ObjectSchema& schema_for_table(const realm_t* realm, realm_table_key_t key)
{
    auto& shared_realm = *realm;
    return schema_for_table(shared_realm, key);
}

#endif // REALM_OBJECT_STORE_C_API_UTIL_HPP