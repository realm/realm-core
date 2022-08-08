///////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/transaction.hpp>
#include <realm/dictionary.hpp>
#include <realm/table_view.hpp>
#include <realm/set.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_recovery.hpp>
#include <realm/sync/subscriptions.hpp>

#include <realm/util/compression.hpp>
#include <realm/util/flat_map.hpp>

#include <algorithm>
#include <chrono>
#include <vector>

using namespace realm;
using namespace _impl;
using namespace sync;

namespace realm {

std::ostream& operator<<(std::ostream& os, const ClientResyncMode& mode)
{
    switch (mode) {
        case ClientResyncMode::Manual:
            os << "Manual";
            break;
        case ClientResyncMode::DiscardLocal:
            os << "DiscardLocal";
            break;
        case ClientResyncMode::Recover:
            os << "Recover";
            break;
        case ClientResyncMode::RecoverOrDiscard:
            os << "RecoveOrDiscard";
            break;
    }
    return os;
}

} // namespace realm


namespace realm::_impl::client_reset::converters {

// Takes two lists, src and dst, and makes dst equal src. src is unchanged.
void InterRealmValueConverter::copy_list(const Obj& src_obj, Obj& dst_obj, bool* update_out)
{
    // The two arrays are compared by finding the longest common prefix and
    // suffix.  The middle section differs between them and is made equal by
    // updating the middle section of dst.
    //
    // Example:
    // src = abcdefghi
    // dst = abcxyhi
    // The common prefix is abc. The common suffix is hi. xy is replaced by defg.
    LstBasePtr src = src_obj.get_listbase_ptr(m_src_col);
    LstBasePtr dst = dst_obj.get_listbase_ptr(m_dst_col);

    bool updated = false;
    size_t len_src = src->size();
    size_t len_dst = dst->size();
    size_t len_min = std::min(len_src, len_dst);

    size_t ndx = 0;
    size_t suffix_len = 0;

    while (ndx < len_min && cmp_src_to_dst(src->get_any(ndx), dst->get_any(ndx), nullptr, update_out) == 0) {
        ndx++;
    }

    size_t suffix_len_max = len_min - ndx;

    while (suffix_len < suffix_len_max &&
           cmp_src_to_dst(src->get_any(len_src - 1 - suffix_len), dst->get_any(len_dst - 1 - suffix_len), nullptr,
                          update_out) == 0) {
        suffix_len++;
    }

    len_min -= (ndx + suffix_len);

    for (size_t i = 0; i < len_min; i++) {
        InterRealmValueConverter::ConversionResult converted_src;
        if (cmp_src_to_dst(src->get_any(ndx), dst->get_any(ndx), &converted_src, update_out)) {
            if (converted_src.requires_new_embedded_object) {
                auto lnklist = dynamic_cast<LnkLst*>(dst.get());
                REALM_ASSERT(lnklist); // this is the only type of list that supports embedded objects
                Obj embedded = lnklist->create_and_set_linked_object(ndx);
                track_new_embedded(converted_src.src_embedded_to_check, embedded);
            }
            else {
                dst->set_any(ndx, converted_src.converted_value);
            }
            updated = true;
        }
        ndx++;
    }

    // New elements must be inserted in dst.
    while (len_dst < len_src) {
        InterRealmValueConverter::ConversionResult converted_src;
        cmp_src_to_dst(src->get_any(ndx), Mixed{}, &converted_src, update_out);
        if (converted_src.requires_new_embedded_object) {
            auto lnklist = dynamic_cast<LnkLst*>(dst.get());
            REALM_ASSERT(lnklist); // this is the only type of list that supports embedded objects
            Obj embedded = lnklist->create_and_insert_linked_object(ndx);
            track_new_embedded(converted_src.src_embedded_to_check, embedded);
        }
        else {
            dst->insert_any(ndx, converted_src.converted_value);
        }
        len_dst++;
        ndx++;
        updated = true;
    }
    // Excess elements must be removed from ll_dst.
    if (len_dst > len_src) {
        dst->remove(len_src - suffix_len, len_dst - suffix_len);
        updated = true;
    }

    REALM_ASSERT(dst->size() == len_src);
    if (updated && update_out) {
        *update_out = updated;
    }
}

void InterRealmValueConverter::copy_set(const Obj& src_obj, Obj& dst_obj, bool* update_out)
{
    SetBasePtr src = src_obj.get_setbase_ptr(m_src_col);
    SetBasePtr dst = dst_obj.get_setbase_ptr(m_dst_col);

    std::vector<size_t> sorted_src, sorted_dst, to_insert, to_delete;
    constexpr bool ascending = true;
    // the implementation could be storing elements in sorted order, but
    // we don't assume that here.
    src->sort(sorted_src, ascending);
    dst->sort(sorted_dst, ascending);

    size_t dst_ndx = 0;
    size_t src_ndx = 0;
    while (src_ndx < sorted_src.size()) {
        if (dst_ndx == sorted_dst.size()) {
            // if we have reached the end of the dst items, all remaining
            // src items should be added
            while (src_ndx < sorted_src.size()) {
                to_insert.push_back(sorted_src[src_ndx++]);
            }
            break;
        }
        size_t ndx_in_src = sorted_src[src_ndx];
        Mixed src_val = src->get_any(ndx_in_src);
        while (dst_ndx < sorted_dst.size()) {
            size_t ndx_in_dst = sorted_dst[dst_ndx];

            int cmp = cmp_src_to_dst(src_val, dst->get_any(ndx_in_dst), nullptr, update_out);
            if (cmp == 0) {
                // equal: advance both src and dst
                ++dst_ndx;
                ++src_ndx;
                break;
            }
            else if (cmp < 0) {
                // src < dst: insert src, advance src only
                to_insert.push_back(ndx_in_src);
                ++src_ndx;
                break;
            }
            else {
                // src > dst: delete dst, advance only dst
                to_delete.push_back(ndx_in_dst);
                ++dst_ndx;
                continue;
            }
        }
    }
    while (dst_ndx < sorted_dst.size()) {
        to_delete.push_back(sorted_dst[dst_ndx++]);
    }

    std::sort(to_delete.begin(), to_delete.end());
    for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
        dst->erase_any(dst->get_any(*it));
    }
    for (auto ndx : to_insert) {
        InterRealmValueConverter::ConversionResult converted_src;
        cmp_src_to_dst(src->get_any(ndx), Mixed{}, &converted_src, update_out);
        // we do not support a set of embedded objects
        REALM_ASSERT(!converted_src.requires_new_embedded_object);
        dst->insert_any(converted_src.converted_value);
    }

    if (update_out && (to_delete.size() || to_insert.size())) {
        *update_out = true;
    }
}

void InterRealmValueConverter::copy_dictionary(const Obj& src_obj, Obj& dst_obj, bool* update_out)
{
    Dictionary src = src_obj.get_dictionary(m_src_col);
    Dictionary dst = dst_obj.get_dictionary(m_dst_col);

    std::vector<size_t> sorted_src, sorted_dst, to_insert, to_delete;
    constexpr bool ascending = true;
    src.sort_keys(sorted_src, ascending);
    dst.sort_keys(sorted_dst, ascending);

    size_t dst_ndx = 0;
    size_t src_ndx = 0;
    while (src_ndx < sorted_src.size()) {
        if (dst_ndx == sorted_dst.size()) {
            // if we have reached the end of the dst items, all remaining
            // src items should be added
            while (src_ndx < sorted_src.size()) {
                to_insert.push_back(sorted_src[src_ndx++]);
            }
            break;
        }

        auto src_val = src.get_pair(sorted_src[src_ndx]);
        while (dst_ndx < sorted_dst.size()) {
            auto dst_val = dst.get_pair(sorted_dst[dst_ndx]);
            int cmp = src_val.first.compare(dst_val.first);
            if (cmp == 0) {
                // Check if the values differ
                if (cmp_src_to_dst(src_val.second, dst_val.second, nullptr, update_out)) {
                    // values are different - modify destination, advance both
                    to_insert.push_back(sorted_src[src_ndx]);
                }
                // keys and values equal: advance both src and dst
                ++dst_ndx;
                ++src_ndx;
                break;
            }
            else if (cmp < 0) {
                // src < dst: insert src, advance src only
                to_insert.push_back(sorted_src[src_ndx++]);
                break;
            }
            else {
                // src > dst: delete dst, advance only dst
                to_delete.push_back(sorted_dst[dst_ndx++]);
                continue;
            }
        }
    }
    // at this point, we've gone through all src items but still have dst items
    // oustanding; these should all be deleted because they are not in src
    while (dst_ndx < sorted_dst.size()) {
        to_delete.push_back(sorted_dst[dst_ndx++]);
    }

    std::sort(to_delete.begin(), to_delete.end());
    for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
        dst.erase(dst.begin() + *it);
    }
    for (auto ndx : to_insert) {
        auto pair = src.get_pair(ndx);
        InterRealmValueConverter::ConversionResult converted_val;
        cmp_src_to_dst(pair.second, Mixed{}, &converted_val, update_out);
        if (converted_val.requires_new_embedded_object) {
            Obj new_embedded = dst.create_and_insert_linked_object(pair.first);
            track_new_embedded(converted_val.src_embedded_to_check, new_embedded);
        }
        else {
            dst.insert(pair.first, converted_val.converted_value);
        }
    }
    if (update_out && (to_delete.size() || to_insert.size())) {
        *update_out = true;
    }
}

void InterRealmValueConverter::copy_value(const Obj& src_obj, Obj& dst_obj, bool* update_out)
{
    if (m_src_col.is_list()) {
        copy_list(src_obj, dst_obj, update_out);
    }
    else if (m_src_col.is_dictionary()) {
        copy_dictionary(src_obj, dst_obj, update_out);
    }
    else if (m_src_col.is_set()) {
        copy_set(src_obj, dst_obj, update_out);
    }
    else {
        REALM_ASSERT(!m_src_col.is_collection());
        InterRealmValueConverter::ConversionResult converted_src;
        if (cmp_src_to_dst(src_obj.get_any(m_src_col), dst_obj.get_any(m_dst_col), &converted_src, update_out)) {
            if (converted_src.requires_new_embedded_object) {
                Obj new_embedded = dst_obj.create_and_set_linked_object(m_dst_col);
                track_new_embedded(converted_src.src_embedded_to_check, new_embedded);
            }
            else {
                dst_obj.set_any(m_dst_col, converted_src.converted_value);
            }
        }
    }
}


// If an embedded object is encountered, add it to a list of embedded objects to process.
// This relies on the property that embedded objects only have one incoming link
// otherwise there could be an infinite loop while discovering embedded objects.
void EmbeddedObjectConverter::track(Obj e_src, Obj e_dst)
{
    embedded_pending.push_back({e_src, e_dst});
}

void EmbeddedObjectConverter::process_pending()
{
    // Conceptually this is a map, but doing a linear search through a vector is known
    // to be faster for small number of elements. Since the number of tables expected
    // to be processed here is assumed to be small < 20, use linear search instead of
    // hashing. N is the depth to which embedded objects are connected and the upper
    // bound is the total number of tables which is finite, and is usually small.
    util::FlatMap<TableKey, InterRealmObjectConverter> converters;

    while (!embedded_pending.empty()) {
        EmbeddedToCheck pending = embedded_pending.back();
        embedded_pending.pop_back();
        TableRef src_table = pending.embedded_in_src.get_table();
        TableRef dst_table = pending.embedded_in_dst.get_table();
        TableKey dst_table_key = dst_table->get_key();
        auto it_with_did_insert =
            converters.insert({dst_table_key, InterRealmObjectConverter{src_table, dst_table, shared_from_this()}});
        InterRealmObjectConverter& converter = it_with_did_insert.first->second;
        converter.copy(pending.embedded_in_src, pending.embedded_in_dst, nullptr);
    }
}

InterRealmValueConverter::InterRealmValueConverter(ConstTableRef src_table, ColKey src_col, ConstTableRef dst_table,
                                                   ColKey dst_col, std::shared_ptr<EmbeddedObjectConverter> ec)
    : m_src_table(src_table)
    , m_dst_table(dst_table)
    , m_src_col(src_col)
    , m_dst_col(dst_col)
    , m_embedded_converter(ec)
    , m_is_embedded_link(false)
    , m_primitive_types_only(!(src_col.get_type() == col_type_TypedLink || src_col.get_type() == col_type_Link ||
                               src_col.get_type() == col_type_LinkList || src_col.get_type() == col_type_Mixed))
{
    if (!m_primitive_types_only) {
        REALM_ASSERT(src_table);
        m_opposite_of_src = src_table->get_opposite_table(src_col);
        m_opposite_of_dst = dst_table->get_opposite_table(dst_col);
        REALM_ASSERT(bool(m_opposite_of_src) == bool(m_opposite_of_dst));
        if (m_opposite_of_src) {
            m_is_embedded_link = m_opposite_of_src->is_embedded();
        }
    }
}

void InterRealmValueConverter::track_new_embedded(Obj src, Obj dst)
{
    m_embedded_converter->track(src, dst);
}

// convert `src` to the destination Realm and compare that value with `dst`
// If `converted_src_out` is provided, it will be set to the converted src value
int InterRealmValueConverter::cmp_src_to_dst(Mixed src, Mixed dst, ConversionResult* converted_src_out,
                                             bool* did_update_out)
{
    int cmp = 0;
    Mixed converted_src;
    if (m_primitive_types_only || !src.is_type(type_Link, type_TypedLink)) {
        converted_src = src;
        cmp = src.compare(dst);
    }
    else if (m_opposite_of_src) {
        ObjKey src_link_key = src.get<ObjKey>();
        if (m_is_embedded_link) {
            Obj src_embedded = m_opposite_of_src->get_object(src_link_key);
            REALM_ASSERT_DEBUG(src_embedded.is_valid());
            if (dst.is_type(type_Link, type_TypedLink)) {
                cmp = 0; // no need to set this link, there is already an embedded object here
                Obj dst_embedded = m_opposite_of_dst->get_object(dst.get<ObjKey>());
                REALM_ASSERT_DEBUG(dst_embedded.is_valid());
                converted_src = dst_embedded.get_key();
                track_new_embedded(src_embedded, dst_embedded);
            }
            else {
                cmp = src.compare(dst);
                if (converted_src_out) {
                    converted_src_out->requires_new_embedded_object = true;
                    converted_src_out->src_embedded_to_check = src_embedded;
                }
            }
        }
        else {
            Mixed src_link_pk = m_opposite_of_src->get_primary_key(src_link_key);
            Obj dst_link = m_opposite_of_dst->create_object_with_primary_key(src_link_pk, did_update_out);
            converted_src = dst_link.get_key();
            if (dst.is_type(type_TypedLink)) {
                cmp = converted_src.compare(dst.get<ObjKey>());
            }
            else {
                cmp = converted_src.compare(dst);
            }
        }
    }
    else {
        ObjLink src_link = src.get<ObjLink>();
        if (src_link.is_unresolved()) {
            converted_src = Mixed{}; // no need to transfer over unresolved links
            cmp = converted_src.compare(dst);
        }
        else {
            TableRef src_link_table = m_src_table->get_parent_group()->get_table(src_link.get_table_key());
            REALM_ASSERT_EX(src_link_table, src_link.get_table_key());
            TableRef dst_link_table = m_dst_table->get_parent_group()->get_table(src_link_table->get_name());
            REALM_ASSERT_EX(dst_link_table, src_link_table->get_name());
            // embedded tables should always be covered by the m_opposite_of_src case above.
            REALM_ASSERT_EX(!src_link_table->is_embedded(), src_link_table->get_name());
            // regular table, convert by pk
            Mixed src_pk = src_link_table->get_primary_key(src_link.get_obj_key());
            Obj dst_link = dst_link_table->create_object_with_primary_key(src_pk, did_update_out);
            converted_src = ObjLink{dst_link_table->get_key(), dst_link.get_key()};
            cmp = converted_src.compare(dst);
        }
    }
    if (converted_src_out) {
        converted_src_out->converted_value = converted_src;
    }
    if (did_update_out && cmp) {
        *did_update_out = true;
    }
    return cmp;
}

InterRealmObjectConverter::InterRealmObjectConverter(ConstTableRef table_src, TableRef table_dst,
                                                     std::shared_ptr<EmbeddedObjectConverter> embedded_tracker)
    : m_embedded_tracker(embedded_tracker)
{
    populate_columns_from_table(table_src, table_dst);
}

void InterRealmObjectConverter::copy(const Obj& src, Obj& dst, bool* update_out)
{
    for (auto& column : m_columns_cache) {
        column.copy_value(src, dst, update_out);
    }
}

void InterRealmObjectConverter::populate_columns_from_table(ConstTableRef table_src, ConstTableRef table_dst)
{
    m_columns_cache.clear();
    m_columns_cache.reserve(table_src->get_column_count());
    ColKey pk_col = table_src->get_primary_key_column();
    for (ColKey col_key_src : table_src->get_column_keys()) {
        if (col_key_src == pk_col)
            continue;
        StringData col_name = table_src->get_column_name(col_key_src);
        ColKey col_key_dst = table_dst->get_column_key(col_name);
        REALM_ASSERT(col_key_dst);
        m_columns_cache.emplace_back(
            InterRealmValueConverter(table_src, col_key_src, table_dst, col_key_dst, m_embedded_tracker));
    }
}

} // namespace realm::_impl::client_reset::converters


namespace realm::_impl::client_reset {

static inline bool should_skip_table(const Transaction& group, TableKey key)
{
    return !group.table_is_public(key);
}

void transfer_group(const Transaction& group_src, Transaction& group_dst, util::Logger& logger)
{
    logger.debug("transfer_group, src size = %1, dst size = %2", group_src.size(), group_dst.size());

    // Find all tables in dst that should be removed.
    std::set<std::string> tables_to_remove;
    for (auto table_key : group_dst.get_table_keys()) {
        if (should_skip_table(group_dst, table_key))
            continue;
        StringData table_name = group_dst.get_table_name(table_key);
        logger.debug("key = %1, table_name = %2", table_key.value, table_name);
        ConstTableRef table_src = group_src.get_table(table_name);
        if (!table_src) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        // Check whether the table type is the same.
        TableRef table_dst = group_dst.get_table(table_key);
        auto pk_col_src = table_src->get_primary_key_column();
        auto pk_col_dst = table_dst->get_primary_key_column();
        bool has_pk_src = bool(pk_col_src);
        bool has_pk_dst = bool(pk_col_dst);
        if (has_pk_src != has_pk_dst) {
            throw ClientResetFailed(util::format("Client reset requires a primary key column in %1 table '%2'",
                                                 (has_pk_src ? "dest" : "source"), table_name));
        }
        if (!has_pk_src)
            continue;

        // Now the tables both have primary keys. Check type.
        if (pk_col_src.get_type() != pk_col_dst.get_type()) {
            throw ClientResetFailed(
                util::format("Client reset found incompatible primary key types (%1 vs %2) on '%3'",
                             pk_col_src.get_type(), pk_col_dst.get_type(), table_name));
        }
        // Check collection type, nullability etc. but having an index doesn't matter;
        ColumnAttrMask pk_col_src_attr = pk_col_src.get_attrs();
        ColumnAttrMask pk_col_dst_attr = pk_col_dst.get_attrs();
        pk_col_src_attr.reset(ColumnAttr::col_attr_Indexed);
        pk_col_dst_attr.reset(ColumnAttr::col_attr_Indexed);
        if (pk_col_src_attr != pk_col_dst_attr) {
            throw ClientResetFailed(
                util::format("Client reset found incompatible primary key attributes (%1 vs %2) on '%3'",
                             pk_col_src.value, pk_col_dst.value, table_name));
        }
        // Check name.
        StringData pk_col_name_src = table_src->get_column_name(pk_col_src);
        StringData pk_col_name_dst = table_dst->get_column_name(pk_col_dst);
        if (pk_col_name_src != pk_col_name_dst) {
            throw ClientResetFailed(
                util::format("Client reset requires equal pk column names but '%1' != '%2' on '%3'", pk_col_name_src,
                             pk_col_name_dst, table_name));
        }
        // The table survives.
        logger.debug("Table '%1' will remain", table_name);
    }

    // If there have been any tables marked for removal stop.
    // We consider two possible options for recovery:
    // 1: Remove the tables. But this will generate destructive schema
    //    schema changes that the local Realm cannot advance through.
    //    Since this action will fail down the line anyway, give up now.
    // 2: Keep the tables locally and ignore them. But the local app schema
    //    still has these classes and trying to modify anything in them will
    //    create sync instructions on tables that sync doesn't know about.
    if (!tables_to_remove.empty()) {
        std::string names_list;
        for (const std::string& table_name : tables_to_remove) {
            names_list += Group::table_name_to_class_name(table_name);
            names_list += ", ";
        }
        if (names_list.size() > 2) {
            // remove the final ", "
            names_list = names_list.substr(0, names_list.size() - 2);
        }
        throw ClientResetFailed(
            util::format("Client reset cannot recover when classes have been removed: {%1}", names_list));
    }

    // Create new tables in dst if needed.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        auto pk_col_src = table_src->get_primary_key_column();
        TableRef table_dst = group_dst.get_table(table_name);
        if (!table_dst) {
            // Create the table.
            if (table_src->is_embedded()) {
                REALM_ASSERT(!pk_col_src);
                group_dst.add_table(table_name, Table::Type::Embedded);
            }
            else {
                REALM_ASSERT(pk_col_src); // a sync table will have a pk
                auto pk_col_src = table_src->get_primary_key_column();
                DataType pk_type = DataType(pk_col_src.get_type());
                StringData pk_col_name = table_src->get_column_name(pk_col_src);
                group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, pk_col_src.is_nullable(),
                                                     table_src->get_table_type());
            }
        }
    }

    // Now the class tables are identical.
    size_t num_tables;
    {
        size_t num_tables_src = 0;
        for (auto table_key : group_src.get_table_keys()) {
            if (!should_skip_table(group_src, table_key))
                ++num_tables_src;
        }
        size_t num_tables_dst = 0;
        for (auto table_key : group_dst.get_table_keys()) {
            if (!should_skip_table(group_dst, table_key))
                ++num_tables_dst;
        }
        REALM_ASSERT(num_tables_src == num_tables_dst);
        num_tables = num_tables_src;
    }
    logger.debug("The number of tables is %1", num_tables);

    // Remove columns in dst if they are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        std::vector<std::string> columns_to_remove;
        for (ColKey col_key : table_dst->get_column_keys()) {
            StringData col_name = table_dst->get_column_name(col_key);
            ColKey col_key_src = table_src->get_column_key(col_name);
            if (!col_key_src) {
                columns_to_remove.push_back(col_name);
                continue;
            }
        }
        if (!columns_to_remove.empty()) {
            std::string columns_list;
            for (const std::string& col_name : columns_to_remove) {
                columns_list += col_name;
                columns_list += ", ";
            }
            throw ClientResetFailed(
                util::format("Client reset cannot recover when columns have been removed from '%1': {%2}", table_name,
                             columns_list));
        }
    }

    // Add columns in dst if present in src and absent in dst.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        for (ColKey col_key : table_src->get_column_keys()) {
            StringData col_name = table_src->get_column_name(col_key);
            ColKey col_key_dst = table_dst->get_column_key(col_name);
            if (!col_key_dst) {
                DataType col_type = table_src->get_column_type(col_key);
                bool nullable = col_key.is_nullable();
                bool has_search_index = table_src->has_search_index(col_key);
                logger.trace("Create column, table = %1, column name = %2, "
                             " type = %3, nullable = %4, has_search_index = %5",
                             table_name, col_name, col_key.get_type(), nullable, has_search_index);
                ColKey col_key_dst;
                if (Table::is_link_type(col_key.get_type())) {
                    ConstTableRef target_src = table_src->get_link_target(col_key);
                    TableRef target_dst = group_dst.get_table(target_src->get_name());
                    if (col_key.is_list()) {
                        col_key_dst = table_dst->add_column_list(*target_dst, col_name);
                    }
                    else if (col_key.is_set()) {
                        col_key_dst = table_dst->add_column_set(*target_dst, col_name);
                    }
                    else if (col_key.is_dictionary()) {
                        DataType key_type = table_src->get_dictionary_key_type(col_key);
                        col_key_dst = table_dst->add_column_dictionary(*target_dst, col_name, key_type);
                    }
                    else {
                        REALM_ASSERT(!col_key.is_collection());
                        col_key_dst = table_dst->add_column(*target_dst, col_name);
                    }
                }
                else if (col_key.is_list()) {
                    col_key_dst = table_dst->add_column_list(col_type, col_name, nullable);
                }
                else if (col_key.is_set()) {
                    col_key_dst = table_dst->add_column_set(col_type, col_name, nullable);
                }
                else if (col_key.is_dictionary()) {
                    DataType key_type = table_src->get_dictionary_key_type(col_key);
                    col_key_dst = table_dst->add_column_dictionary(col_type, col_name, nullable, key_type);
                }
                else {
                    REALM_ASSERT(!col_key.is_collection());
                    col_key_dst = table_dst->add_column(col_type, col_name, nullable);
                }

                if (has_search_index)
                    table_dst->add_search_index(col_key_dst);
            }
            else {
                // column preexists in dest, make sure the types match
                if (col_key.get_type() != col_key_dst.get_type()) {
                    throw ClientResetFailed(util::format(
                        "Incompatable column type change detected during client reset for '%1.%2' (%3 vs %4)",
                        table_name, col_name, col_key.get_type(), col_key_dst.get_type()));
                }
                ColumnAttrMask src_col_attrs = col_key.get_attrs();
                ColumnAttrMask dst_col_attrs = col_key_dst.get_attrs();
                src_col_attrs.reset(ColumnAttr::col_attr_Indexed);
                dst_col_attrs.reset(ColumnAttr::col_attr_Indexed);
                // make sure the attributes such as collection type, nullability etc. match
                // but index equality doesn't matter here.
                if (src_col_attrs != dst_col_attrs) {
                    throw ClientResetFailed(util::format(
                        "Incompatable column attribute change detected during client reset for '%1.%2' (%3 vs %4)",
                        table_name, col_name, col_key.value, col_key_dst.value));
                }
            }
        }
    }

    // Now the schemas are identical.

    // Remove objects in dst that are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        auto table_src = group_src.get_table(table_key);
        // There are no primary keys in embedded tables but this is ok, because
        // embedded objects are tied to the lifetime of top level objects.
        if (table_src->is_embedded())
            continue;
        StringData table_name = table_src->get_name();
        logger.debug("Removing objects in '%1'", table_name);
        auto table_dst = group_dst.get_table(table_name);

        auto pk_col = table_dst->get_primary_key_column();
        REALM_ASSERT_DEBUG(pk_col); // sync realms always have a pk
        std::vector<std::pair<Mixed, ObjKey>> objects_to_remove;
        for (auto obj : *table_dst) {
            auto pk = obj.get_any(pk_col);
            if (!table_src->find_primary_key(pk)) {
                objects_to_remove.emplace_back(pk, obj.get_key());
            }
        }
        for (auto& pair : objects_to_remove) {
            logger.debug("  removing '%1'", pair.first);
            table_dst->remove_object(pair.second);
        }
    }

    std::shared_ptr<converters::EmbeddedObjectConverter> embedded_tracker =
        std::make_shared<converters::EmbeddedObjectConverter>();

    // Now src and dst have identical schemas and no extraneous objects from dst.
    // There may be missing object from src and the values of existing objects may
    // still differ. Diff all the values and create missing objects on the fly.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        // Embedded objects don't have a primary key, so they are handled
        // as a special case when they are encountered as a link value.
        if (table_src->is_embedded())
            continue;
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_src->get_column_count() == table_dst->get_column_count());
        auto pk_col = table_src->get_primary_key_column();
        REALM_ASSERT(pk_col);
        logger.debug("Updating values for table '%1', number of rows = %2, "
                     "number of columns = %3, primary_key_col = %4, "
                     "primary_key_type = %5",
                     table_name, table_src->size(), table_src->get_column_count(), pk_col.get_index().val,
                     pk_col.get_type());

        converters::InterRealmObjectConverter converter(table_src, table_dst, embedded_tracker);

        for (const Obj& src : *table_src) {
            auto src_pk = src.get_primary_key();
            bool updated = false;
            // get or create the object
            auto dst = table_dst->create_object_with_primary_key(src_pk, &updated);
            REALM_ASSERT(dst);

            converter.copy(src, dst, &updated);
            if (updated) {
                logger.debug("  updating %1", src_pk);
            }
        }
        embedded_tracker->process_pending();
    }
}

// A table without a "class_" prefix will not generate sync instructions.
constexpr static std::string_view s_meta_reset_table_name("client_reset_metadata");
constexpr static std::string_view s_pk_col_name("id");
constexpr static std::string_view s_version_column_name("version");
constexpr static std::string_view s_timestamp_col_name("event_time");
constexpr static std::string_view s_reset_type_col_name("type_of_reset");
constexpr int64_t metadata_version = 1;

void remove_pending_client_resets(TransactionRef wt)
{
    REALM_ASSERT(wt);
    if (auto table = wt->get_table(s_meta_reset_table_name)) {
        if (table->size()) {
            table->clear();
        }
    }
}

util::Optional<PendingReset> has_pending_reset(TransactionRef rt)
{
    REALM_ASSERT(rt);
    ConstTableRef table = rt->get_table(s_meta_reset_table_name);
    if (!table || table->size() == 0) {
        return util::none;
    }
    ColKey timestamp_col = table->get_column_key(s_timestamp_col_name);
    ColKey type_col = table->get_column_key(s_reset_type_col_name);
    ColKey version_col = table->get_column_key(s_version_column_name);
    REALM_ASSERT(timestamp_col);
    REALM_ASSERT(type_col);
    REALM_ASSERT(version_col);
    if (table->size() > 1) {
        // this may happen if a future version of this code changes the format and expectations around reset metadata.
        throw ClientResetFailed(
            util::format("Previous client resets detected (%1) but only one is expected.", table->size()));
    }
    Obj first = *table->begin();
    REALM_ASSERT(first);
    PendingReset pending;
    int64_t version = first.get<int64_t>(version_col);
    pending.time = first.get<Timestamp>(timestamp_col);
    if (version > metadata_version) {
        throw ClientResetFailed(util::format("Unsupported client reset metadata version: %1 vs %2, from %3", version,
                                             metadata_version, pending.time));
    }
    int64_t type = first.get<int64_t>(type_col);
    if (type == 0) {
        pending.type = ClientResyncMode::DiscardLocal;
    }
    else if (type == 1) {
        pending.type = ClientResyncMode::Recover;
    }
    else {
        throw ClientResetFailed(
            util::format("Unsupported client reset metadata type: %1 from %2", type, pending.time));
    }
    return pending;
}

void track_reset(TransactionRef wt, ClientResyncMode mode)
{
    REALM_ASSERT(wt);
    REALM_ASSERT(mode != ClientResyncMode::Manual);
    TableRef table = wt->get_table(s_meta_reset_table_name);
    ColKey version_col, timestamp_col, type_col;
    if (!table) {
        table = wt->add_table_with_primary_key(s_meta_reset_table_name, type_ObjectId, s_pk_col_name);
        REALM_ASSERT(table);
        version_col = table->add_column(type_Int, s_version_column_name);
        timestamp_col = table->add_column(type_Timestamp, s_timestamp_col_name);
        type_col = table->add_column(type_Int, s_reset_type_col_name);
    }
    else {
        version_col = table->get_column_key(s_version_column_name);
        timestamp_col = table->get_column_key(s_timestamp_col_name);
        type_col = table->get_column_key(s_reset_type_col_name);
    }
    REALM_ASSERT(version_col);
    REALM_ASSERT(timestamp_col);
    REALM_ASSERT(type_col);
    int64_t mode_val = 0; // Discard
    if (mode == ClientResyncMode::Recover || mode == ClientResyncMode::RecoverOrDiscard) {
        mode_val = 1; // Recover
    }
    table->create_object_with_primary_key(ObjectId::gen(),
                                          {{version_col, metadata_version},
                                           {timestamp_col, Timestamp(std::chrono::system_clock::now())},
                                           {type_col, mode_val}});
}

static ClientResyncMode reset_precheck_guard(TransactionRef wt, ClientResyncMode mode, bool recovery_is_allowed,
                                             util::Logger& logger)
{
    REALM_ASSERT(wt);
    if (auto previous_reset = has_pending_reset(wt)) {
        logger.info("A previous reset was detected of type: '%1' at: %2", previous_reset->type, previous_reset->time);
        switch (previous_reset->type) {
            case ClientResyncMode::Manual:
                REALM_UNREACHABLE();
                break;
            case ClientResyncMode::DiscardLocal:
                throw ClientResetFailed(util::format("A previous '%1' mode reset from %2 did not succeed, "
                                                     "giving up on '%3' mode to prevent a cycle",
                                                     previous_reset->type, previous_reset->time, mode));
            case ClientResyncMode::Recover:
                if (mode == ClientResyncMode::Recover) {
                    throw ClientResetFailed(util::format("A previous '%1' mode reset from %2 did not succeed, "
                                                         "giving up on '%3' mode to prevent a cycle",
                                                         previous_reset->type, previous_reset->time, mode));
                }
                else if (mode == ClientResyncMode::RecoverOrDiscard) {
                    mode = ClientResyncMode::DiscardLocal;
                    logger.info("A previous '%1' mode reset from %2 downgrades this mode ('%3') to DiscardLocal",
                                previous_reset->type, previous_reset->time, mode);
                }
                else if (mode == ClientResyncMode::DiscardLocal) {
                    // previous mode Recover and this mode is Discard, this is not a cycle yet
                }
                else {
                    REALM_UNREACHABLE();
                }
                break;
            case ClientResyncMode::RecoverOrDiscard:
                throw ClientResetFailed(util::format("Unexpected previous '%1' mode reset from %2 did not "
                                                     "succeed, giving up on '%3' mode to prevent a cycle",
                                                     previous_reset->type, previous_reset->time, mode));
        }
    }
    if (!recovery_is_allowed) {
        if (mode == ClientResyncMode::Recover) {
            throw ClientResetFailed(
                "Client reset mode is set to 'Recover' but the server does not allow recovery for this client");
        }
        else if (mode == ClientResyncMode::RecoverOrDiscard) {
            logger.info("Client reset in 'RecoverOrDiscard' is choosing 'DiscardLocal' because the server does not "
                        "permit recovery for this client");
            mode = ClientResyncMode::DiscardLocal;
        }
    }
    track_reset(wt, mode);
    return mode;
}

LocalVersionIDs perform_client_reset_diff(DBRef db_local, DBRef db_remote, sync::SaltedFileIdent client_file_ident,
                                          util::Logger& logger, ClientResyncMode mode, bool recovery_is_allowed,
                                          bool* did_recover_out, sync::SubscriptionStore* sub_store,
                                          util::UniqueFunction<void(int64_t)> on_flx_version_complete)
{
    REALM_ASSERT(db_local);
    REALM_ASSERT(db_remote);
    logger.info("Client reset, path_local = %1, "
                "client_file_ident.ident = %2, "
                "client_file_ident.salt = %3,"
                "remote = %4, mode = %5, recovery_is_allowed = %6",
                db_local->get_path(), client_file_ident.ident, client_file_ident.salt, db_remote->get_path(), mode,
                recovery_is_allowed);

    auto remake_active_subscription = [&]() {
        if (!sub_store) {
            return;
        }
        auto mut_subs = sub_store->get_active().make_mutable_copy();
        int64_t before_version = mut_subs.version();
        mut_subs.update_state(sync::SubscriptionSet::State::Complete);
        auto sub = std::move(mut_subs).commit();
        if (on_flx_version_complete) {
            on_flx_version_complete(sub.version());
        }
        logger.info("Recreated the active subscription set in the complete state (%1 -> %2)", before_version,
                    sub.version());
    };

    auto frozen_pre_local_state = db_local->start_frozen();
    auto wt_local = db_local->start_write();
    auto history_local = dynamic_cast<ClientHistory*>(wt_local->get_replication()->_get_history_write());
    REALM_ASSERT(history_local);
    VersionID old_version_local = wt_local->get_version_of_current_transaction();
    sync::version_type current_version_local = old_version_local.version;
    wt_local->get_history()->ensure_updated(current_version_local);
    SaltedFileIdent orig_file_ident;
    {
        sync::version_type old_version_unused;
        SyncProgress old_progress_unused;
        history_local->get_status(old_version_unused, orig_file_ident, old_progress_unused);
    }
    std::vector<ClientHistory::LocalChange> local_changes;

    mode = reset_precheck_guard(wt_local, mode, recovery_is_allowed, logger);
    bool recover_local_changes = (mode == ClientResyncMode::Recover || mode == ClientResyncMode::RecoverOrDiscard);

    if (recover_local_changes) {
        local_changes = history_local->get_local_changes(current_version_local);
        logger.info("Local changesets to recover: %1", local_changes.size());
    }

    sync::SaltedVersion fresh_server_version = {0, 0};
    auto wt_remote = db_remote->start_write();
    auto history_remote = dynamic_cast<ClientHistory*>(wt_remote->get_replication()->_get_history_write());
    REALM_ASSERT(history_remote);

    SyncProgress remote_progress;
    {
        sync::version_type remote_version_unused;
        SaltedFileIdent remote_ident_unused;
        history_remote->get_status(remote_version_unused, remote_ident_unused, remote_progress);
    }
    fresh_server_version = remote_progress.latest_server_version;
    BinaryData recovered_changeset;

    // FLX with recovery has to be done in multiple commits, which is significantly different than other modes
    if (recover_local_changes && sub_store) {
        // In FLX recovery, save a copy of the pending subscriptions for later. This
        // needs to be done before they are wiped out by remake_active_subscription()
        std::vector<SubscriptionSet> pending_subscriptions = sub_store->get_pending_subscriptions();
        // transform the local Realm such that all public tables become identical to the remote Realm
        transfer_group(*wt_remote, *wt_local, logger);
        // now that the state of the fresh and local Realms are identical,
        // reset the local sync history.
        // Note that we do not set the new file ident yet! This is done in the last commit.
        history_local->set_client_reset_adjustments(current_version_local, orig_file_ident, fresh_server_version,
                                                    recovered_changeset);
        // The local Realm is committed. There are no changes to the remote Realm.
        wt_remote->rollback_and_continue_as_read();
        wt_local->commit_and_continue_as_read();
        // Make a copy of the active subscription set and mark it as
        // complete. This will cause all other subscription sets to become superceded.
        remake_active_subscription();
        // Apply local changes interleaved with pending subscriptions in separate commits
        // as needed. This has the consequence that there may be extra notifications along
        // the way to the final state, but since separate commits are necessary, this is
        // unavoidable.
        wt_local = db_local->start_write();
        RecoverLocalChangesetsHandler handler{*wt_local, *frozen_pre_local_state, logger};
        handler.process_changesets(local_changes, std::move(pending_subscriptions)); // throws on error
        // The new file ident is set as part of the final commit. This is to ensure that if
        // there are any exceptions during recovery, or the process is killed for some
        // reason, the client reset cycle detection will catch this and we will not attempt
        // to recover again. If we had set the ident in the first commit, a Realm which was
        // partially recovered, but interrupted may continue sync the next time it is
        // opened with only partially recovered state while having lost the history of any
        // offline modifications.
        history_local->set_client_file_ident_in_wt(current_version_local, client_file_ident);
        wt_local->commit_and_continue_as_read();
    }
    else {
        if (recover_local_changes) {
            // In PBS recovery, the strategy is to apply all local changes to the remote realm first,
            // and then transfer the modified state all at once to the local Realm. This creates a
            // nice side effect for notifications because only the minimal state change is made.
            RecoverLocalChangesetsHandler handler{*wt_remote, *frozen_pre_local_state, logger};
            handler.process_changesets(local_changes, {}); // throws on error
            ClientReplication* client_repl = dynamic_cast<ClientReplication*>(wt_remote->get_replication());
            REALM_ASSERT_RELEASE(client_repl);
            ChangesetEncoder& encoder = client_repl->get_instruction_encoder();
            const sync::ChangesetEncoder::Buffer& buffer = encoder.buffer();
            recovered_changeset = {buffer.data(), buffer.size()};
        }

        // transform the local Realm such that all public tables become identical to the remote Realm
        transfer_group(*wt_remote, *wt_local, logger);

        // now that the state of the fresh and local Realms are identical,
        // reset the local sync history and steal the fresh Realm's ident
        history_local->set_client_reset_adjustments(current_version_local, client_file_ident, fresh_server_version,
                                                    recovered_changeset);

        // Finally, the local Realm is committed. The changes to the remote Realm are discarded.
        wt_remote->rollback_and_continue_as_read();
        wt_local->commit_and_continue_as_read();

        // If in FLX mode, make a copy of the active subscription set and mark it as
        // complete. This will cause all other subscription sets to become superceded.
        // In DiscardLocal mode, only the active subscription set is preserved, so we
        // are done.
        remake_active_subscription();
    }

    if (did_recover_out) {
        *did_recover_out = recover_local_changes;
    }
    VersionID new_version_local = wt_local->get_version_of_current_transaction();
    logger.info("perform_client_reset_diff is done, old_version.version = %1, "
                "old_version.index = %2, new_version.version = %3, "
                "new_version.index = %4",
                old_version_local.version, old_version_local.index, new_version_local.version,
                new_version_local.index);

    return LocalVersionIDs{old_version_local, new_version_local};
}

} // namespace realm::_impl::client_reset
