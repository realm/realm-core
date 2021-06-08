#include <algorithm>
#include <vector>
#include <set>
#include <sstream>
#include <iostream>

#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/sync/object.hpp>
#include <realm/list.hpp>
#include <realm/dictionary.hpp>
#include <realm/set.hpp>

#include "compare_groups.hpp"


using namespace realm;

namespace {

class MuteLogger : public util::RootLogger {
public:
    void do_log(Level, const std::string&) override final {}
};


class TableCompareLogger : public util::Logger {
public:
    TableCompareLogger(StringData table_name, util::Logger& base_logger) noexcept
        : util::Logger{base_logger.level_threshold}
        , m_table_name{table_name}
        , m_base_logger{base_logger}
    {
    }
    void do_log(Level level, const std::string& message) override final
    {
        ensure_prefix();                                          // Throws
        Logger::do_log(m_base_logger, level, m_prefix + message); // Throws
    }

private:
    const StringData m_table_name;
    util::Logger& m_base_logger;
    std::string m_prefix;
    void ensure_prefix()
    {
        if (REALM_LIKELY(!m_prefix.empty()))
            return;
        std::ostringstream out;
        out << "Table[" << m_table_name << "]: "; // Throws
        m_prefix = out.str();                     // Throws
    }
};


class ObjectCompareLogger : public util::Logger {
public:
    ObjectCompareLogger(sync::PrimaryKey oid, util::Logger& base_logger) noexcept
        : util::Logger{base_logger.level_threshold}
        , m_oid{oid}
        , m_base_logger{base_logger}
    {
    }
    void do_log(Level level, const std::string& message) override final
    {
        ensure_prefix();                                          // Throws
        Logger::do_log(m_base_logger, level, m_prefix + message); // Throws
    }

private:
    const sync::PrimaryKey m_oid;
    util::Logger& m_base_logger;
    std::string m_prefix;
    void ensure_prefix()
    {
        if (REALM_LIKELY(!m_prefix.empty()))
            return;
        std::ostringstream out;
        out << sync::format_pk(m_oid) << ": "; // Throws
        m_prefix = out.str();                  // Throws
    }
};


template <class T, class Cmp = std::equal_to<>>
bool compare_arrays(T& a, T& b, Cmp equals = Cmp{})
{
    auto a_it = a.begin();
    auto b_it = b.begin();
    if (a.size() != b.size()) {
#if REALM_DEBUG
        std::cerr << "LEFT size: " << a.size() << std::endl;
        std::cerr << "RIGHT size: " << b.size() << std::endl;
#endif // REALM_DEBUG
        return false;
    }

    // Compare entries
    for (; a_it != a.end(); ++a_it, ++b_it) {
        if (!equals(*a_it, *b_it))
            goto different;
    }

    return true;
different:
#if REALM_DEBUG
    std::cerr << "LEFT: " << *a_it << std::endl;
    std::cerr << "RIGHT: " << *b_it << std::endl;
#endif // REALM_DEBUG
    return false;
}

template <class T>
bool compare_set_values(const Set<T>& a, const Set<T>& b)
{
    return compare_arrays(a, b, SetElementEquals<T>{});
}

bool compare_dictionaries(const Dictionary& a, const Dictionary& b)
{
    auto a_it = a.begin();
    auto b_it = b.begin();
    if (a.size() != b.size()) {
#if REALM_DEBUG
        std::cerr << "LEFT size: " << a.size() << std::endl;
        std::cerr << "RIGHT size: " << b.size() << std::endl;
#endif
        return false;
    }

    // Compare entries
    for (; a_it != a.end(); ++a_it, ++b_it) {
        if (*a_it != *b_it)
            goto different;
    }

    return true;

different:
#if REALM_DEBUG
    std::cerr << "LEFT: " << (*a_it).first << " => " << (*a_it).second << std::endl;
    std::cerr << "RIGHT: " << (*b_it).first << " => " << (*b_it).second << std::endl;
#endif
    return false;
}

struct Column {
    StringData name;
    ColKey key_1, key_2;

    DataType get_type() const noexcept
    {
        return DataType(key_1.get_type());
    }

    bool is_list() const noexcept
    {
        return key_1.is_list();
    }

    bool is_dictionary() const noexcept
    {
        return key_1.is_dictionary();
    }

    bool is_set() const noexcept
    {
        return key_1.is_set();
    }

    bool is_nullable() const noexcept
    {
        return key_1.is_nullable();
    }
};

} // unnamed namespace

namespace realm::test_util {

bool compare_objects(const Obj& obj_1, const Obj& obj_2, const std::vector<Column>& columns, util::Logger& logger);
bool compare_objects(sync::PrimaryKey& oid, const Table& table_1, const Table& table_2,
                     const std::vector<Column>& columns, util::Logger& logger);

bool compare_schemas(const Table& table_1, const Table& table_2, util::Logger& logger,
                     std::vector<Column>* out_columns = nullptr)
{
    bool equal = true;

    // Compare column names
    {
        auto col_keys = table_1.get_column_keys();
        for (auto key : col_keys) {
            StringData name = table_1.get_column_name(key);
            if (!table_2.get_column_key(name)) {
                logger.error("Column '%1' not found in right-hand side table", name);
                equal = false;
            }
        }
    }
    {
        auto col_keys = table_2.get_column_keys();
        for (auto key : col_keys) {
            StringData name = table_2.get_column_name(key);
            if (!table_1.get_column_key(name)) {
                logger.error("Column '%1' not found in left-hand side table", name);
                equal = false;
            }
        }
    }

    // Compare column signatures
    {
        auto keys_1 = table_1.get_column_keys();
        for (auto key_1 : keys_1) {
            StringData name = table_1.get_column_name(key_1);
            ColKey key_2 = table_2.get_column_key(name);
            if (!key_2)
                continue;
            DataType type_1 = table_1.get_column_type(key_1);
            DataType type_2 = table_2.get_column_type(key_2);
            if (type_1 != type_2) {
                logger.error("Type mismatch on column '%1'", name);
                equal = false;
                continue;
            }
            bool nullable_1 = table_1.is_nullable(key_1);
            bool nullable_2 = table_2.is_nullable(key_2);
            if (nullable_1 != nullable_2) {
                logger.error("Nullability mismatch on column '%1'", name);
                equal = false;
                continue;
            }
            bool is_list_1 = table_1.is_list(key_1);
            bool is_list_2 = table_2.is_list(key_2);
            if (is_list_1 != is_list_2) {
                logger.error("List type mismatch on column '%1'", name);
                equal = false;
                continue;
            }
            bool is_dictionary_1 = key_1.is_dictionary();
            bool is_dictionary_2 = key_2.is_dictionary();
            if (is_dictionary_1 != is_dictionary_2) {
                logger.error("Dictionary type mismatch on column '%1'", name);
                equal = false;
                continue;
            }
            bool is_set_1 = key_1.is_set();
            bool is_set_2 = key_2.is_set();
            if (is_set_1 != is_set_2) {
                logger.error("Set type mismatch on column '%1'", name);
                equal = false;
                continue;
            }
            if (type_1 == type_Link || type_1 == type_LinkList) {
                ConstTableRef target_1 = table_1.get_link_target(key_1);
                ConstTableRef target_2 = table_2.get_link_target(key_2);
                if (target_1->get_name() != target_2->get_name()) {
                    logger.error("Link target mismatch on column '%1'", name);
                    equal = false;
                    continue;
                }
            }
            if (out_columns)
                out_columns->push_back(Column{name, key_1, key_2});
        }
    }

    return equal;
}

bool compare_lists(const Column& col, const Obj& obj_1, const Obj& obj_2, util::Logger& logger)
{
    switch (col.get_type()) {
        case type_Int: {
            if (col.is_nullable()) {
                auto a = obj_1.get_list<util::Optional<int64_t>>(col.key_1);
                auto b = obj_2.get_list<util::Optional<int64_t>>(col.key_2);
                if (!compare_arrays(a, b)) {
                    logger.error("List mismatch in column '%1'", col.name);
                    return false;
                }
            }
            else {
                auto a = obj_1.get_list<int64_t>(col.key_1);
                auto b = obj_2.get_list<int64_t>(col.key_2);
                if (!compare_arrays(a, b)) {
                    logger.error("List mismatch in column '%1'", col.name);
                    return false;
                }
            }
            break;
        }
        case type_Bool: {
            auto a = obj_1.get_list<bool>(col.key_1);
            auto b = obj_2.get_list<bool>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_String: {
            auto a = obj_1.get_list<String>(col.key_1);
            auto b = obj_2.get_list<String>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Binary: {
            auto a = obj_1.get_list<Binary>(col.key_1);
            auto b = obj_2.get_list<Binary>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Float: {
            auto a = obj_1.get_list<float>(col.key_1);
            auto b = obj_2.get_list<float>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Double: {
            auto a = obj_1.get_list<double>(col.key_1);
            auto b = obj_2.get_list<double>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Timestamp: {
            auto a = obj_1.get_list<Timestamp>(col.key_1);
            auto b = obj_2.get_list<Timestamp>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_ObjectId: {
            auto a = obj_1.get_list<ObjectId>(col.key_1);
            auto b = obj_2.get_list<ObjectId>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_UUID: {
            auto a = obj_1.get_list<UUID>(col.key_1);
            auto b = obj_2.get_list<UUID>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Decimal: {
            auto a = obj_1.get_list<Decimal128>(col.key_1);
            auto b = obj_2.get_list<Decimal128>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Mixed: {
            auto a = obj_1.get_list<Mixed>(col.key_1);
            auto b = obj_2.get_list<Mixed>(col.key_2);
            if (!compare_arrays(a, b)) {
                logger.error("List mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_TypedLink:
            // FIXME: Implement
            break;
        case type_LinkList: {
            auto a = obj_1.get_list<ObjKey>(col.key_1);
            auto b = obj_2.get_list<ObjKey>(col.key_2);
            if (a.size() != b.size()) {
                logger.error("Link list size mismatch in column '%1'", col.name);
                return false;
                break;
            }
            auto table_1 = obj_1.get_table();
            auto table_2 = obj_2.get_table();
            ConstTableRef target_table_1 = table_1->get_link_target(col.key_1);
            ConstTableRef target_table_2 = table_2->get_link_target(col.key_2);

            bool is_embedded = target_table_1->is_embedded();
            std::vector<Column> embedded_columns;
            if (is_embedded) {
                // FIXME: This does the schema comparison for
                // embedded tables for every object with embedded
                // objects, just because we want to get the Column
                // info. Instead compare just the objects
                // themselves.
                bool schemas_equal = compare_schemas(*target_table_1, *target_table_2, logger, &embedded_columns);
                REALM_ASSERT(schemas_equal);
            }

            std::size_t n = a.size();
            for (std::size_t i = 0; i < n; ++i) {
                ObjKey link_1 = a.get(i);
                ObjKey link_2 = b.get(i);

                if (link_1.is_unresolved() || link_2.is_unresolved()) {
                    // if one link is unresolved, the other should also be unresolved
                    if (!link_1.is_unresolved() || !link_2.is_unresolved()) {
                        logger.error("Value mismatch in column '%1' at index %2 of the link "
                                     "list (%3 vs %4)",
                                     col.name, i, link_1, link_2);
                        return false;
                    }
                }
                else {
                    if (is_embedded) {
                        const Obj embedded_1 = target_table_1->get_object(link_1);
                        const Obj embedded_2 = target_table_2->get_object(link_2);
                        // Skip ID comparison for embedded objects, because
                        // they are only identified by their position in the
                        // database.
                        if (!compare_objects(embedded_1, embedded_2, embedded_columns, logger)) {
                            logger.error("Embedded object contents mismatch in column '%1'", col.name);
                            return false;
                        }
                    }
                    else {
                        sync::PrimaryKey target_oid_1 = sync::primary_key_for_row(*target_table_1, link_1);
                        sync::PrimaryKey target_oid_2 = sync::primary_key_for_row(*target_table_2, link_2);
                        if (target_oid_1 != target_oid_2) {
                            logger.error("Value mismatch in column '%1' at index %2 of the link "
                                         "list (%3 vs %4)",
                                         col.name, i, link_1, link_2);
                            return false;
                        }
                    }
                }
            }
            break;
        }
        case type_Link:
            REALM_TERMINATE("Unsupported column type.");
    }

    return true;
}

bool compare_sets(const Column& col, const Obj& obj_1, const Obj& obj_2, util::Logger& logger)
{
    switch (col.get_type()) {
        case type_Int: {
            if (col.is_nullable()) {
                auto a = obj_1.get_set<util::Optional<int64_t>>(col.key_1);
                auto b = obj_2.get_set<util::Optional<int64_t>>(col.key_2);
                if (!compare_set_values(a, b)) {
                    logger.error("Set mismatch in column '%1'", col.name);
                    return false;
                }
            }
            else {
                auto a = obj_1.get_set<int64_t>(col.key_1);
                auto b = obj_2.get_set<int64_t>(col.key_2);
                if (!compare_set_values(a, b)) {
                    logger.error("Set mismatch in column '%1'", col.name);
                    return false;
                }
            }
            break;
        }
        case type_Bool: {
            auto a = obj_1.get_set<bool>(col.key_1);
            auto b = obj_2.get_set<bool>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_String: {
            auto a = obj_1.get_set<String>(col.key_1);
            auto b = obj_2.get_set<String>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Binary: {
            auto a = obj_1.get_set<Binary>(col.key_1);
            auto b = obj_2.get_set<Binary>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Float: {
            auto a = obj_1.get_set<float>(col.key_1);
            auto b = obj_2.get_set<float>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Double: {
            auto a = obj_1.get_set<double>(col.key_1);
            auto b = obj_2.get_set<double>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Timestamp: {
            auto a = obj_1.get_set<Timestamp>(col.key_1);
            auto b = obj_2.get_set<Timestamp>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_ObjectId: {
            auto a = obj_1.get_set<ObjectId>(col.key_1);
            auto b = obj_2.get_set<ObjectId>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_UUID: {
            auto a = obj_1.get_set<UUID>(col.key_1);
            auto b = obj_2.get_set<UUID>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Decimal: {
            auto a = obj_1.get_set<Decimal128>(col.key_1);
            auto b = obj_2.get_set<Decimal128>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_Mixed: {
            auto a = obj_1.get_set<Mixed>(col.key_1);
            auto b = obj_2.get_set<Mixed>(col.key_2);
            if (!compare_set_values(a, b)) {
                logger.error("Set mismatch in column '%1'", col.name);
                return false;
            }
            break;
        }
        case type_TypedLink:
            // FIXME: Implement
            break;
        case type_Link:
        case type_LinkList:
            REALM_TERMINATE("Unsupported column type.");
    }

    return true;
}

bool compare_objects(const Obj& obj_1, const Obj& obj_2, const std::vector<Column>& columns, util::Logger& logger)
{
    bool equal = true;
    auto ptable_1 = obj_1.get_table();
    auto ptable_2 = obj_2.get_table();
    auto& table_1 = *ptable_1;
    auto& table_2 = *ptable_2;

    for (const Column& col : columns) {
        if (col.is_nullable()) {
            bool a = obj_1.is_null(col.key_1);
            bool b = obj_2.is_null(col.key_2);
            if (a && b)
                continue;
            if (a || b) {
                logger.error("Null/nonnull disagreement in column '%1' (%2 vs %3)", col.name, a, b);
                equal = false;
                continue;
            }
        }

        if (col.is_dictionary()) {
            auto a = obj_1.get_dictionary(col.key_1);
            auto b = obj_2.get_dictionary(col.key_2);
            if (!compare_dictionaries(a, b)) {
                logger.error("Dictionary mismatch in column '%1'", col.name);
                equal = false;
            }
            continue;
        }

        if (col.is_set()) {
            if (!compare_sets(col, obj_1, obj_2, logger)) {
                logger.error("Set mismatch in column '%1'", col.name);
                equal = false;
            }
            continue;
        }

        if (col.is_list()) {
            if (!compare_lists(col, obj_1, obj_2, logger)) {
                equal = false;
            }
            continue;
        }

        auto obj_a = obj_1;
        auto obj_b = obj_2;
        const bool nullable = table_1.is_nullable(col.key_1);
        REALM_ASSERT(table_2.is_nullable(col.key_2) == nullable);
        switch (col.get_type()) {
            case type_Int: {
                if (nullable) {
                    auto a = obj_1.get<util::Optional<int64_t>>(col.key_1);
                    auto b = obj_2.get<util::Optional<int64_t>>(col.key_2);
                    if (a != b) {
                        logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                        equal = false;
                    }
                }
                else {
                    auto a = obj_1.get<int64_t>(col.key_1);
                    auto b = obj_2.get<int64_t>(col.key_2);
                    if (a != b) {
                        logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                        equal = false;
                    }
                }
                continue;
            }
            case type_Bool: {
                if (nullable) {
                    auto a = obj_1.get<util::Optional<bool>>(col.key_1);
                    auto b = obj_2.get<util::Optional<bool>>(col.key_2);
                    if (a != b) {
                        logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                        equal = false;
                    }
                }
                else {
                    auto a = obj_1.get<bool>(col.key_1);
                    auto b = obj_2.get<bool>(col.key_2);
                    if (a != b) {
                        logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                        equal = false;
                    }
                }

                continue;
            }
            case type_Float: {
                auto a = obj_1.get<float>(col.key_1);
                auto b = obj_2.get<float>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                    equal = false;
                }
                continue;
            }
            case type_Double: {
                auto a = obj_1.get<double>(col.key_1);
                auto b = obj_2.get<double>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                    equal = false;
                }
                continue;
            }
            case type_String: {
                auto a = obj_1.get<StringData>(col.key_1);
                auto b = obj_2.get<StringData>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1'", col.name);
                    equal = false;
                }
                continue;
            }
            case type_Binary: {
                // FIXME: This looks like an incorrect way of comparing BLOBs (Table::get_binary_iterator()).
                auto a = obj_1.get<BinaryData>(col.key_1);
                auto b = obj_2.get<BinaryData>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1'", col.name);
                    equal = false;
                }
                continue;
            }
            case type_Timestamp: {
                auto a = obj_1.get<Timestamp>(col.key_1);
                auto b = obj_2.get<Timestamp>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                    equal = false;
                }
                continue;
            }
            case type_ObjectId: {
                auto a = obj_1.get<ObjectId>(col.key_1);
                auto b = obj_2.get<ObjectId>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                    equal = false;
                }
                continue;
            }
            case type_Decimal: {
                auto a = obj_1.get<Decimal128>(col.key_1);
                auto b = obj_2.get<Decimal128>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                    equal = false;
                }
                continue;
            }
            case type_Mixed: {
                auto a = obj_1.get<Mixed>(col.key_1);
                auto b = obj_2.get<Mixed>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                    equal = false;
                }
                continue;
            }
            case type_UUID: {
                auto a = obj_1.get<UUID>(col.key_1);
                auto b = obj_2.get<UUID>(col.key_2);
                if (a != b) {
                    logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, a, b);
                    equal = false;
                }
                continue;
            }
            case type_TypedLink:
                // FIXME: Implement
                continue;
            case type_Link: {
                auto link_1 = obj_1.get<ObjKey>(col.key_1);
                auto link_2 = obj_2.get<ObjKey>(col.key_2);
                ConstTableRef target_table_1 = table_1.get_link_target(col.key_1);
                ConstTableRef target_table_2 = table_2.get_link_target(col.key_2);

                if (!link_1 || !link_2) {
                    // If one link is null the other should also be null
                    if (link_1 != link_2) {
                        equal = false;
                        logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name, link_1, link_2);
                    }
                }
                else {
                    bool is_embedded = target_table_1->is_embedded();
                    std::vector<Column> embedded_columns;
                    if (is_embedded) {
                        // FIXME: This does the schema comparison for
                        // embedded tables for every object with embedded
                        // objects, just because we want to get the Column
                        // info. Instead compare just the objects
                        // themselves.
                        bool schemas_equal =
                            compare_schemas(*target_table_1, *target_table_2, logger, &embedded_columns);
                        REALM_ASSERT(schemas_equal);
                    }

                    if (is_embedded) {
                        const Obj embedded_1 = target_table_1->get_object(link_1);
                        const Obj embedded_2 = target_table_2->get_object(link_2);
                        // Skip ID comparison for embedded objects, because
                        // they are only identified by their position in the
                        // database.
                        if (!compare_objects(embedded_1, embedded_2, embedded_columns, logger)) {
                            logger.error("Embedded object contents mismatch in column '%1'", col.name);
                            equal = false;
                        }
                    }
                    else {
                        sync::PrimaryKey target_oid_1 = sync::primary_key_for_row(*target_table_1, link_1);
                        sync::PrimaryKey target_oid_2 = sync::primary_key_for_row(*target_table_2, link_2);
                        if (target_oid_1 != target_oid_2) {
                            logger.error("Value mismatch in column '%1' (%2 vs %3)", col.name,
                                         sync::format_pk(target_oid_1), sync::format_pk(target_oid_2));
                            equal = false;
                        }
                    }
                }

                continue;
            }
            case type_LinkList:
                break;
        }
        REALM_TERMINATE("Unsupported column type.");
    }
    return equal;
}

bool compare_objects(sync::PrimaryKey& oid, const Table& table_1, const Table& table_2,
                     const std::vector<Column>& columns, util::Logger& logger)
{
    ObjKey row_1 = sync::row_for_primary_key(table_1, oid);
    ObjKey row_2 = sync::row_for_primary_key(table_2, oid);

    // Note: This is ensured by the inventory handling in compare_tables().
    REALM_ASSERT(row_1);
    REALM_ASSERT(row_2);
    const Obj obj_1 = table_1.get_object(row_1);
    const Obj obj_2 = table_2.get_object(row_2);
    return compare_objects(obj_1, obj_2, columns, logger);
}

bool compare_tables(const Table& table_1, const Table& table_2)
{
    MuteLogger logger;
    return compare_tables(table_1, table_2, logger);
}

bool compare_tables(const Table& table_1, const Table& table_2, util::Logger& logger)
{
    bool equal = true;

    std::vector<Column> columns;
    equal = compare_schemas(table_1, table_2, logger, &columns);

    if (table_1.is_embedded() != table_2.is_embedded()) {
        logger.error("Table embeddedness mismatch");
        equal = false;
    }

    if (table_1.is_embedded() || table_2.is_embedded()) {
        if (table_1.size() != table_2.size()) {
            logger.error("Embedded table size mismatch (%1 vs %2): %3", table_1.size(), table_2.size(),
                         table_1.get_name());
            equal = false;
        }
        // Do not attempt to compare by row on embedded tables.
        return equal;
    }

    // Compare row sets
    using Objects = std::set<sync::PrimaryKey>;
    auto make_inventory = [](const Table& table, Objects& objects) {
        for (const Obj& obj : table) {
            auto oid = sync::primary_key_for_row(obj);
            objects.insert(oid);
        }
    };
    Objects objects_1, objects_2;
    make_inventory(table_1, objects_1);
    make_inventory(table_2, objects_2);
    auto report_missing = [&](const char* hand_2, Objects& objects_1, Objects& objects_2) {
        std::vector<sync::PrimaryKey> missing;
        for (auto oid : objects_1) {
            if (objects_2.find(oid) == objects_2.end())
                missing.push_back(oid);
        }
        if (missing.empty())
            return;
        std::size_t n = missing.size();
        if (n == 1) {
            logger.error("One object missing in %1 side table: %2", hand_2, sync::format_pk(missing[0]));
            equal = false;
            return;
        }
        std::ostringstream out;
        out << sync::format_pk(missing[0]);
        std::size_t m = std::min<std::size_t>(4, n);
        for (std::size_t i = 1; i < m; ++i)
            out << ", " << sync::format_pk(missing[i]);
        if (m < n)
            out << ", ...";
        logger.error("%1 objects missing in %2 side table: %3", n, hand_2, out.str());
        equal = false;
    };
    report_missing("right-hand", objects_1, objects_2);
    report_missing("left-hand", objects_2, objects_1);

    // Compare individual rows
    for (auto oid : objects_1) {
        if (objects_2.find(oid) != objects_2.end()) {
            ObjectCompareLogger sublogger{oid, logger};
            if (!compare_objects(oid, table_1, table_2, columns, sublogger)) {
                equal = false;
            }
        }
    }

    return equal;
}


bool compare_groups(const Transaction& group_1, const Transaction& group_2)
{
    MuteLogger logger;
    return compare_groups(group_1, group_2, logger);
}


bool compare_groups(const Transaction& group_1, const Transaction& group_2,
                    std::function<bool(StringData)> filter_func, util::Logger& logger)
{
    auto filter = [&](const Group& group, std::vector<StringData>& tables) {
        auto table_keys = group.get_table_keys();
        for (auto i : table_keys) {
            ConstTableRef table = group.get_table(i);
            StringData name = table->get_name();
            if (name != "pk" && name != "metadata" && filter_func(name))
                tables.push_back(name);
        }
    };

    std::vector<StringData> tables_1, tables_2;
    filter(group_1, tables_1);
    filter(group_2, tables_2);

    bool equal = true;
    for (StringData table_name : tables_1) {
        if (!group_2.has_table(table_name)) {
            logger.error("Table '%1' not found in right-hand side group", table_name);
            equal = false;
        }
    }
    for (StringData table_name : tables_2) {
        if (!group_1.has_table(table_name)) {
            logger.error("Table '%1' not found in left-hand side group", table_name);
            equal = false;
        }
    }

    for (StringData table_name : tables_1) {
        ConstTableRef table_1 = group_1.get_table(table_name);
        ConstTableRef table_2 = group_2.get_table(table_name);
        if (table_2) {
            TableCompareLogger sublogger{table_name, logger};
            if (!compare_tables(*table_1, *table_2, sublogger))
                equal = false;
        }
    }

    return equal;
}

} // namespace realm::test_util
