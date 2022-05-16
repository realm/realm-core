#ifndef REALM_OBJECT_STORE_C_API_UTIL_HPP
#define REALM_OBJECT_STORE_C_API_UTIL_HPP

#include <realm/object-store/c_api/error.hpp>
#include <realm/object-store/c_api/types.hpp>
#include <realm/util/functional.hpp>

namespace realm::c_api {

template <class F>
inline auto wrap_err(F&& f) noexcept -> decltype(f())
{
    try {
        return f();
    }
    catch (...) {
        set_last_exception(std::current_exception());
        return {};
    };
}

template <class F>
inline auto wrap_err(F&& f, const decltype(f())& e) noexcept
{
    try {
        return f();
    }
    catch (...) {
        set_last_exception(std::current_exception());
        return e;
    }
}

inline const ObjectSchema& schema_for_table(const std::shared_ptr<Realm>& realm, TableKey table_key)
{
    // Validate the table key.
    realm->read_group().get_table(table_key);
    const auto& schema = realm->schema();

    auto it = schema.find(table_key);
    if (it != schema.end()) {
        return *it;
    }

    throw NoSuchTable{};
}

inline void report_type_mismatch(const SharedRealm& realm, const Table& table, ColKey col_key)
{
    auto& schema = schema_for_table(realm, table.get_key());
    throw PropertyTypeMismatch{schema.name, table.get_column_name(col_key)};
}

/// Check that the value within a mixed is appropriate for a particular column.
///
/// Checks: Base type, nullability, link target match.
inline void check_value_assignable(const SharedRealm& realm, const Table& table, ColKey col_key, Mixed val)
{
    if (val.is_null()) {
        if (col_key.is_nullable()) {
            return;
        }
        auto& schema = schema_for_table(realm, table.get_key());
        throw NotNullableException{schema.name, table.get_column_name(col_key)};
    }

    if (val.get_type() == type_TypedLink &&
        (col_key.get_type() == col_type_Link || col_key.get_type() == col_type_LinkList)) {
        auto obj_link = val.get<ObjLink>();
        if (table.get_link_target(col_key)->get_key() != obj_link.get_table_key()) {
            report_type_mismatch(realm, table, col_key);
        }
    }
    else {
        if (ColumnType(val.get_type()) != col_key.get_type()) {
            report_type_mismatch(realm, table, col_key);
        }
    }
}

/// Check that a mixed value can be inserted in a list.
inline void check_value_assignable(const realm::object_store::Collection& list, Mixed val)
{
    auto realm = list.get_realm();
    auto table_key = list.get_parent_table_key();
    auto table = realm->read_group().get_table(table_key);
    auto col_key = list.get_parent_column_key();
    return check_value_assignable(realm, *table, col_key, val);
}

/// If the value is Mixed(ObjKey), convert it to Mixed(ObjLink).
inline Mixed objkey_to_typed_link(Mixed val, ColKey col_key, const Table& table)
{
    if (val.is_type(type_Link)) {
        auto target_table = table.get_link_target(col_key);
        return ObjLink{target_table->get_key(), val.get<ObjKey>()};
    }
    return val;
}

inline char* duplicate_string(const std::string& string)
{
    char* ret = reinterpret_cast<char*>(malloc(string.size() + 1));
    string.copy(ret, string.size());
    ret[string.size()] = '\0';
    return ret;
}

template <typename T>
inline void set_out_param(T* out_n, T n)
{
    if (out_n) {
        *out_n = n;
    }
}

struct FreeUserdata {
    realm_free_userdata_func_t m_func;
    FreeUserdata(realm_free_userdata_func_t func = nullptr)
        : m_func(func)
    {
    }
    void operator()(void* ptr)
    {
        if (m_func) {
            (m_func)(ptr);
        }
    }
};

using UserdataPtr = std::unique_ptr<void, FreeUserdata>;
using SharedUserdata = std::shared_ptr<void>;

/**
 * Convenience class for managing callbacks.
 *
 * WARNING: This class doesn't provide any thread-safety guarantees
 * about which threads modifies or invokes callbacks.
 *
 * @tparam Args The argument types of the callback.
 */
template <typename... Args>
class CallbackRegistry {
public:
    uint64_t add(util::UniqueFunction<void(Args...)>&& callback)
    {
        uint64_t token = m_next_token++;
        m_callbacks.emplace_hint(m_callbacks.end(), token, std::move(callback));
        return token;
    }

    void remove(uint64_t token)
    {
        m_callbacks.erase(token);
    }

    void invoke(Args... args)
    {
        for (auto& callback : m_callbacks) {
            callback.second(args...);
        }
    }

private:
    std::map<uint64_t, util::UniqueFunction<void(Args...)>> m_callbacks;
    uint64_t m_next_token = 0;
};

/**
 * Convenience struct for safely filling external arrays with new-allocated pointers.
 *
 * Calling new T() might throw, which requires that extra care needs to be put in
 * freeing any elements allocated into the buffer up to that point.
 */
template <typename T>
struct OutBuffer {
public:
    OutBuffer(T** buffer)
        : m_buffer(buffer)
    {
    }

    template <typename... Args>
    void emplace(Args&&... args)
    {
        m_buffer[m_size++] = new T(std::forward<Args>(args)...);
    }

    size_t size()
    {
        return m_size;
    }

    /**
     * Release ownership of the elements in the buffer so that they won't be
     * freed when this goes out of scope.
     *
     * @param out_n Total number of items added to the buffer. Can be null.
     */
    void release(size_t* out_n)
    {
        m_released = true;
        if (out_n) {
            *out_n = m_size;
        }
    }

    ~OutBuffer()
    {
        if (m_released) {
            return;
        }

        while (m_size--) {
            delete m_buffer[m_size];
            m_buffer[m_size] = nullptr;
        }
    }

private:
    T** m_buffer;
    size_t m_size = 0;
    bool m_released = false;
};
} // namespace realm::c_api

#endif // REALM_OBJECT_STORE_C_API_UTIL_HPP
