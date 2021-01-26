#ifndef REALM_OBJECT_STORE_C_API_UTIL_HPP
#define REALM_OBJECT_STORE_C_API_UTIL_HPP

#include <realm/object-store/c_api/types.hpp>

namespace realm::c_api {

void set_last_exception(std::exception_ptr eptr);

template <class F>
inline auto wrap_err(F&& f) -> decltype(std::declval<F>()())
{
    try {
        return f();
    }
    catch (...) {
        set_last_exception(std::current_exception());
        return {};
    };
}

inline const ObjectSchema& schema_for_table(const std::shared_ptr<Realm>& realm, TableKey table_key)
{
    // Validate the table key.
    realm->read_group().get_table(table_key);
    const auto& schema = realm->schema();

    // FIXME: Faster lookup than linear search.
    for (auto& os : schema) {
        if (os.table_key == table_key) {
            return os;
        }
    }

    throw NoSuchTable{};
}

inline void report_type_mismatch(const SharedRealm& realm, const Table& table, ColKey col_key)
{
    auto& schema = schema_for_table(realm, table.get_key());
    throw PropertyTypeMismatch{schema.name, table.get_column_name(col_key)};
}

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

inline void check_value_assignable(const List& list, Mixed val)
{
    auto realm = list.get_realm();
    auto table_key = list.get_parent_table_key();
    auto table = realm->read_group().get_table(table_key);
    auto col_key = list.get_parent_column_key();
    return check_value_assignable(realm, *table, col_key, val);
}

inline Mixed typed_link_to_objkey(Mixed val)
{
    if (!val.is_null() && val.get_type() == type_TypedLink) {
        auto link = val.get<ObjLink>();
        return link.get_obj_key();
    }
    return val;
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
} // namespace realm::c_api

#endif // REALM_OBJECT_STORE_C_API_UTIL_HPP