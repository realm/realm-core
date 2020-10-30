#include <realm/sync/object_id.hpp>
#include <realm/util/backtrace.hpp>
#include <realm/util/overload.hpp>

#include <sstream>
#include <iomanip>
#include <ostream>
#include <cctype>   // std::isxdigit
#include <stdlib.h> // strtoull

using namespace realm;
using namespace realm::sync;

std::ostream& realm::sync::operator<<(std::ostream& os, format_pk fmt)
{
    const auto& key = fmt.pk;
    auto formatter = util::overload{
        [&](mpark::monostate) {
            os << "NULL";
        },
        [&](int64_t x) {
            os << "Int(" << x << ")";
        },
        [&](StringData x) {
            os << "\"" << x << "\"";
        },
        [&](GlobalKey x) {
            os << "GlobalKey{" << x << "}";
        },
        [&](ObjectId x) {
            os << "ObjectId{" << x << "}";
        },
        [&](UUID x) {
            os << "UUID{" << x << "}";
        },
    };
    mpark::visit(formatter, key);
    return os;
}

void ObjectIDSet::insert(StringData table, const PrimaryKey& object_id)
{
    m_objects[table].insert(object_id);
}

void ObjectIDSet::erase(StringData table, const PrimaryKey& object_id)
{
    auto search = m_objects.find(table);
    if (search != m_objects.end()) {
        auto& single_table_ids = search->second;
        single_table_ids.erase(object_id);
        if (single_table_ids.empty())
            m_objects.erase(table);
    }
}

bool ObjectIDSet::contains(StringData table, const PrimaryKey& object_id) const noexcept
{
    auto search = m_objects.find(table);
    if (search != m_objects.end()) {
        const auto& single_table_ids = search->second;
        return single_table_ids.find(object_id) != single_table_ids.end();
    }
    return false;
}

void FieldSet::insert(StringData table, StringData column, const PrimaryKey& object_id)
{
    m_fields[table][column].insert(object_id);
}

void FieldSet::erase(StringData table, StringData column, const PrimaryKey& object_id)
{
    auto search_1 = m_fields.find(table);
    if (search_1 != m_fields.end()) {
        auto& single_table_fields = search_1->second;
        auto search_2 = single_table_fields.find(column);
        if (search_2 != single_table_fields.end()) {
            auto& single_field_ids = search_2->second;
            single_field_ids.erase(object_id);
            if (single_field_ids.empty()) {
                single_table_fields.erase(column);
                if (single_table_fields.empty()) {
                    m_fields.erase(table);
                }
            }
        }
    }
}

bool FieldSet::contains(StringData table, const PrimaryKey& object_id) const noexcept
{
    auto search_1 = m_fields.find(table);
    if (search_1 == m_fields.end())
        return false;

    const auto& single_table_fields = search_1->second;
    for (const auto& kv : single_table_fields) {
        const auto& single_field_ids = kv.second;
        if (single_field_ids.find(object_id) != single_field_ids.end())
            return true;
    }
    return false;
}

bool FieldSet::contains(StringData table, StringData column, const PrimaryKey& object_id) const noexcept
{
    auto search_1 = m_fields.find(table);
    if (search_1 == m_fields.end())
        return false;

    const auto& single_table_fields = search_1->second;
    auto search_2 = single_table_fields.find(column);
    if (search_2 == single_table_fields.end())
        return false;

    const auto& single_field_ids = search_2->second;
    return single_field_ids.find(object_id) != single_field_ids.end();
}
