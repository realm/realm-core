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

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/set.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>

#include <vector>

using namespace realm;
using namespace _impl;
using namespace sync;

namespace {

struct EmbeddedObjectConverter {
    // If an embedded object is encountered, add it to a list of embedded objects to process.
    // This relies on the property that embedded objects only have one incoming link
    // otherwise there could be an infinite loop while discovering embedded objects.
    void track(Obj e_src, Obj e_dst)
    {
        embedded_pending.push_back({e_src, e_dst});
    }
    void process_pending();

private:
    struct EmbeddedToCheck {
        Obj embedded_in_src;
        Obj embedded_in_dst;
    };

    std::vector<EmbeddedToCheck> embedded_pending;
};

struct InterRealmValueConverter {
    InterRealmValueConverter(ConstTableRef src_table, ColKey src_col, ConstTableRef dst_table, ColKey dst_col,
                             EmbeddedObjectConverter& ec)
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

    void track_new_embedded(Obj src, Obj dst)
    {
        m_embedded_converter.track(src, dst);
    }

    struct ConversionResult {
        Mixed converted_value;
        bool requires_new_embedded_object = false;
        Obj src_embedded_to_check;
    };

    // convert `src` to the destination Realm and compare that value with `dst`
    // If `converted_src_out` is provided, it will be set to the converted src value
    int cmp_src_to_dst(Mixed src, Mixed dst, ConversionResult* converted_src_out = nullptr,
                       bool* did_update_out = nullptr)
    {
        int cmp = 0;
        Mixed converted_src;
        if (m_primitive_types_only || !src.is_type(type_Link, type_TypedLink)) {
            converted_src = src;
            cmp = src.compare(dst);
        }
        else {
            if (m_opposite_of_src) {
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

    inline ColKey source_col() const
    {
        return m_src_col;
    }

    inline ColKey dest_col() const
    {
        return m_dst_col;
    }

private:
    TableRef m_dst_link_table;
    ConstTableRef m_src_table;
    ConstTableRef m_dst_table;
    ColKey m_src_col;
    ColKey m_dst_col;
    TableRef m_opposite_of_src;
    TableRef m_opposite_of_dst;
    EmbeddedObjectConverter& m_embedded_converter;
    bool m_is_embedded_link;
    const bool m_primitive_types_only;
};

// Takes two lists, src and dst, and makes dst equal src. src is unchanged.
void copy_list(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    // The two arrays are compared by finding the longest common prefix and
    // suffix.  The middle section differs between them and is made equal by
    // updating the middle section of dst.
    //
    // Example:
    // src = abcdefghi
    // dst = abcxyhi
    // The common prefix is abc. The common suffix is hi. xy is replaced by defg.
    LstBasePtr src = src_obj.get_listbase_ptr(convert.source_col());
    LstBasePtr dst = dst_obj.get_listbase_ptr(convert.dest_col());

    bool updated = false;
    size_t len_src = src->size();
    size_t len_dst = dst->size();
    size_t len_min = std::min(len_src, len_dst);

    size_t ndx = 0;
    size_t suffix_len = 0;

    while (ndx < len_min && convert.cmp_src_to_dst(src->get_any(ndx), dst->get_any(ndx), nullptr, update_out) == 0) {
        ndx++;
    }

    size_t suffix_len_max = len_min - ndx;

    while (suffix_len < suffix_len_max &&
           convert.cmp_src_to_dst(src->get_any(len_src - 1 - suffix_len), dst->get_any(len_dst - 1 - suffix_len),
                                  nullptr, update_out) == 0) {
        suffix_len++;
    }

    len_min -= (ndx + suffix_len);

    for (size_t i = 0; i < len_min; i++) {
        InterRealmValueConverter::ConversionResult converted_src;
        if (convert.cmp_src_to_dst(src->get_any(ndx), dst->get_any(ndx), &converted_src, update_out)) {
            if (converted_src.requires_new_embedded_object) {
                auto lnklist = dynamic_cast<LnkLst*>(dst.get());
                REALM_ASSERT(lnklist); // this is the only type of list that supports embedded objects
                Obj embedded = lnklist->create_and_set_linked_object(ndx);
                convert.track_new_embedded(converted_src.src_embedded_to_check, embedded);
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
        convert.cmp_src_to_dst(src->get_any(ndx), Mixed{}, &converted_src, update_out);
        if (converted_src.requires_new_embedded_object) {
            auto lnklist = dynamic_cast<LnkLst*>(dst.get());
            REALM_ASSERT(lnklist); // this is the only type of list that supports embedded objects
            Obj embedded = lnklist->create_and_insert_linked_object(ndx);
            convert.track_new_embedded(converted_src.src_embedded_to_check, embedded);
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

void copy_set(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    SetBasePtr src = src_obj.get_setbase_ptr(convert.source_col());
    SetBasePtr dst = dst_obj.get_setbase_ptr(convert.dest_col());

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

            int cmp = convert.cmp_src_to_dst(src_val, dst->get_any(ndx_in_dst), nullptr, update_out);
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
        convert.cmp_src_to_dst(src->get_any(ndx), Mixed{}, &converted_src, update_out);
        // we do not support a set of embedded objects
        REALM_ASSERT(!converted_src.requires_new_embedded_object);
        dst->insert_any(converted_src.converted_value);
    }

    if (update_out && (to_delete.size() || to_insert.size())) {
        *update_out = true;
    }
}

void copy_dictionary(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    Dictionary src = src_obj.get_dictionary(convert.source_col());
    Dictionary dst = dst_obj.get_dictionary(convert.dest_col());

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
                if (convert.cmp_src_to_dst(src_val.second, dst_val.second, nullptr, update_out)) {
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
        convert.cmp_src_to_dst(pair.second, Mixed{}, &converted_val, update_out);
        if (converted_val.requires_new_embedded_object) {
            Obj new_embedded = dst.create_and_insert_linked_object(pair.first);
            convert.track_new_embedded(converted_val.src_embedded_to_check, new_embedded);
        }
        else {
            dst.insert(pair.first, converted_val.converted_value);
        }
    }
    if (update_out && (to_delete.size() || to_insert.size())) {
        *update_out = true;
    }
}

void copy_value(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    if (convert.source_col().is_list()) {
        copy_list(src_obj, dst_obj, convert, update_out);
    }
    else if (convert.source_col().is_dictionary()) {
        copy_dictionary(src_obj, dst_obj, convert, update_out);
    }
    else if (convert.source_col().is_set()) {
        copy_set(src_obj, dst_obj, convert, update_out);
    }
    else {
        REALM_ASSERT(!convert.source_col().is_collection());
        InterRealmValueConverter::ConversionResult converted_src;
        if (convert.cmp_src_to_dst(src_obj.get_any(convert.source_col()), dst_obj.get_any(convert.dest_col()),
                                   &converted_src, update_out)) {
            if (converted_src.requires_new_embedded_object) {
                Obj new_embedded = dst_obj.create_and_set_linked_object(convert.dest_col());
                convert.track_new_embedded(converted_src.src_embedded_to_check, new_embedded);
            }
            else {
                dst_obj.set_any(convert.dest_col(), converted_src.converted_value);
            }
        }
    }
}

struct InterRealmObjectConverter {
    InterRealmObjectConverter(ConstTableRef table_src, TableRef table_dst, EmbeddedObjectConverter& embedded_tracker)
        : m_embedded_tracker(embedded_tracker)
    {
        populate_columns_from_table(table_src, table_dst);
    }

    void copy(const Obj& src, Obj& dst, bool* update_out)
    {
        for (auto& column : m_columns_cache) {
            copy_value(src, dst, column, update_out);
        }
    }

private:
    void populate_columns_from_table(ConstTableRef table_src, ConstTableRef table_dst)
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

    EmbeddedObjectConverter& m_embedded_tracker;
    std::vector<InterRealmValueConverter> m_columns_cache;
};

void EmbeddedObjectConverter::process_pending()
{
    // Conceptually this is a map, but doing a linear search through a vector is known
    // to be faster for small number of elements. Since the number of tables expected
    // to be processed here is assumed to be small < 20, use linear search instead of
    // hashing. N is the depth to which embedded objects are connected and the upper
    // bound is the total number of tables which is finite, and is usually small.

    std::vector<std::pair<TableKey, InterRealmObjectConverter>> converters;
    auto get_converter = [this, &converters](ConstTableRef src_table,
                                             TableRef dst_table) -> InterRealmObjectConverter& {
        TableKey dst_table_key = dst_table->get_key();
        auto it = std::find_if(converters.begin(), converters.end(), [&dst_table_key](auto& val) {
            return val.first == dst_table_key;
        });
        if (it == converters.end()) {
            return converters.emplace_back(dst_table_key, InterRealmObjectConverter{src_table, dst_table, *this})
                .second;
        }
        return it->second;
    };

    while (!embedded_pending.empty()) {
        EmbeddedToCheck pending = embedded_pending.back();
        embedded_pending.pop_back();
        InterRealmObjectConverter& converter =
            get_converter(pending.embedded_in_src.get_table(), pending.embedded_in_dst.get_table());
        converter.copy(pending.embedded_in_src, pending.embedded_in_dst, nullptr);
    }
}


} // namespace

void client_reset::transfer_group(const Transaction& group_src, Transaction& group_dst, util::Logger& logger)
{
    logger.debug("transfer_group, src size = %1, dst size = %2", group_src.size(), group_dst.size());

    // Find all tables in dst that should be removed.
    std::set<std::string> tables_to_remove;
    for (auto table_key : group_dst.get_table_keys()) {
        if (!group_dst.table_is_public(table_key))
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
        if (!group_src.table_is_public(table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        auto pk_col_src = table_src->get_primary_key_column();
        TableRef table_dst = group_dst.get_table(table_name);
        if (!table_dst) {
            // Create the table.
            if (table_src->is_embedded()) {
                REALM_ASSERT(!pk_col_src);
                group_dst.add_embedded_table(table_name);
            }
            else {
                REALM_ASSERT(pk_col_src); // a sync table will have a pk
                auto pk_col_src = table_src->get_primary_key_column();
                DataType pk_type = DataType(pk_col_src.get_type());
                StringData pk_col_name = table_src->get_column_name(pk_col_src);
                group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, pk_col_src.is_nullable());
            }
        }
    }

    // Now the class tables are identical.
    size_t num_tables;
    {
        size_t num_tables_src = 0;
        for (auto table_key : group_src.get_table_keys()) {
            if (group_src.table_is_public(table_key))
                ++num_tables_src;
        }
        size_t num_tables_dst = 0;
        for (auto table_key : group_dst.get_table_keys()) {
            if (group_dst.table_is_public(table_key))
                ++num_tables_dst;
        }
        REALM_ASSERT(num_tables_src == num_tables_dst);
        num_tables = num_tables_src;
    }
    logger.debug("The number of tables is %1", num_tables);

    // Remove columns in dst if they are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        if (!group_src.table_is_public(table_key))
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
        if (!group_src.table_is_public(table_key))
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
        if (!group_src.table_is_public(table_key))
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

    EmbeddedObjectConverter embedded_tracker;

    // Now src and dst have identical schemas and no extraneous objects from dst.
    // There may be missing object from src and the values of existing objects may
    // still differ. Diff all the values and create missing objects on the fly.
    for (auto table_key : group_src.get_table_keys()) {
        if (!group_src.table_is_public(table_key))
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

        InterRealmObjectConverter converter(table_src, table_dst, embedded_tracker);

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
        embedded_tracker.process_pending();
    }
}

client_reset::LocalVersionIDs client_reset::perform_client_reset_diff(DB& db_local, DBRef db_remote,
                                                                      sync::SaltedFileIdent client_file_ident,
                                                                      util::Logger& logger)
{
    logger.info("Client reset, path_local = %1, "
                "client_file_ident.ident = %2, "
                "client_file_ident.salt = %3,"
                "remote = %4,",
                db_local.get_path(), client_file_ident.ident, client_file_ident.salt,
                (db_remote ? db_remote->get_path() : "<none>"));

    auto wt_local = db_local.start_write();
    auto history_local = dynamic_cast<ClientHistory*>(wt_local->get_replication()->_get_history_write());
    VersionID old_version_local = wt_local->get_version_of_current_transaction();
    sync::version_type current_version_local = old_version_local.version;
    wt_local->get_history()->ensure_updated(current_version_local);
    BinaryData recovered_changeset;
    sync::SaltedVersion fresh_server_version = {0, 0};

    if (db_remote) { // 'discard local' mode
        auto wt_remote = db_remote->start_write();
        auto history_remote = dynamic_cast<ClientHistory*>(wt_remote->get_replication()->_get_history_write());
        sync::version_type current_version_remote = wt_remote->get_version();
        history_local->set_client_file_ident_in_wt(current_version_local, client_file_ident);
        history_remote->set_client_file_ident_in_wt(current_version_remote, client_file_ident);

        sync::version_type remote_version;
        SaltedFileIdent remote_ident;
        SyncProgress remote_progress;
        history_remote->get_status(remote_version, remote_ident, remote_progress);
        fresh_server_version = remote_progress.latest_server_version;

        transfer_group(*wt_remote, *wt_local, logger);
    }
    history_local->set_client_reset_adjustments(current_version_local, client_file_ident, fresh_server_version,
                                                recovered_changeset);

    // Finally, the local Realm is committed.
    wt_local->commit_and_continue_as_read();
    VersionID new_version_local = wt_local->get_version_of_current_transaction();
    logger.debug("perform_client_reset_diff is done, old_version.version = %1, "
                 "old_version.index = %2, new_version.version = %3, "
                 "new_version.index = %4",
                 old_version_local.version, old_version_local.index, new_version_local.version,
                 new_version_local.index);

    return LocalVersionIDs{old_version_local, new_version_local};
}
