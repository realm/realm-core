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

static inline const ObjectSchema& schema_for_table(const std::shared_ptr<Realm>& realm, TableKey table_key)
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

    // FIXME: Proper exception type.
    throw std::logic_error{"Class not in schema"};
}

#endif // REALM_OBJECT_STORE_C_API_UTIL_HPP