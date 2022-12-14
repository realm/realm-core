/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/object_converter.hpp>

#include <realm/dictionary.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>

#include <realm/util/flat_map.hpp>

namespace realm::converters {

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

    std::vector<size_t> to_insert, to_delete;

    size_t dst_ndx = 0;
    size_t src_ndx = 0;
    while (src_ndx < src.size()) {
        if (dst_ndx == dst.size()) {
            // if we have reached the end of the dst items, all remaining
            // src items should be added
            while (src_ndx < src.size()) {
                to_insert.push_back(src_ndx++);
            }
            break;
        }

        auto src_val = src.get_pair(src_ndx);
        while (dst_ndx < dst.size()) {
            auto dst_val = dst.get_pair(dst_ndx);
            int cmp = src_val.first.compare(dst_val.first);
            if (cmp == 0) {
                // Check if the values differ
                if (cmp_src_to_dst(src_val.second, dst_val.second, nullptr, update_out)) {
                    // values are different - modify destination, advance both
                    to_insert.push_back(src_ndx);
                }
                // keys and values equal: advance both src and dst
                ++dst_ndx;
                ++src_ndx;
                break;
            }
            else if (cmp < 0) {
                // src < dst: insert src, advance src only
                to_insert.push_back(src_ndx++);
                break;
            }
            else {
                // src > dst: delete dst, advance only dst
                to_delete.push_back(dst_ndx++);
                continue;
            }
        }
    }
    // at this point, we've gone through all src items but still have dst items
    // oustanding; these should all be deleted because they are not in src
    while (dst_ndx < dst.size()) {
        to_delete.push_back(dst_ndx++);
    }

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
void EmbeddedObjectConverter::track(const Obj& e_src, const Obj& e_dst)
{
    embedded_pending.push_back({e_src, e_dst});
}

void EmbeddedObjectConverter::process_pending()
{
    util::FlatMap<TableKey, InterRealmObjectConverter> converters;

    while (!embedded_pending.empty()) {
        EmbeddedToCheck pending = embedded_pending.back();
        embedded_pending.pop_back();

        TableRef dst_table = pending.embedded_in_dst.get_table();
        TableKey dst_table_key = dst_table->get_key();
        auto it = converters.find(dst_table_key);
        if (it == converters.end()) {
            TableRef src_table = pending.embedded_in_src.get_table();
            it = converters.insert({dst_table_key, InterRealmObjectConverter{src_table, dst_table, this}}).first;
        }
        InterRealmObjectConverter& converter = it->second;
        converter.copy(pending.embedded_in_src, pending.embedded_in_dst, nullptr);
    }
}

InterRealmValueConverter::InterRealmValueConverter(ConstTableRef src_table, ColKey src_col, ConstTableRef dst_table,
                                                   ColKey dst_col, EmbeddedObjectConverter* ec)
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

void InterRealmValueConverter::track_new_embedded(const Obj& src, const Obj& dst)
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
            Obj dst_link;
            if (m_opposite_of_dst == m_opposite_of_src) {
                // if this is the same Realm, we can use the ObjKey
                dst_link = m_opposite_of_dst->get_object(src_link_key);
            }
            else {
                // in different Realms we create a new object
                if (m_opposite_of_src->get_primary_key_column()) {
                    Mixed src_link_pk = m_opposite_of_src->get_primary_key(src_link_key);
                    dst_link = m_opposite_of_dst->create_object_with_primary_key(src_link_pk, did_update_out);
                }
                else {
                    dst_link = m_opposite_of_dst->create_object();
                }
            }
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
            if (src_link_table->get_primary_key_column()) {
                Mixed src_pk = src_link_table->get_primary_key(src_link.get_obj_key());
                Obj dst_link = dst_link_table->create_object_with_primary_key(src_pk, did_update_out);
                converted_src = ObjLink{dst_link_table->get_key(), dst_link.get_key()};
            }
            else if (src_link_table == dst_link_table) {
                // no pk, but this is the same Realm, so convert by ObjKey
                Obj dst_link = dst_link_table->get_object(src_link.get_obj_key());
                converted_src = ObjLink{dst_link_table->get_key(), dst_link.get_key()};
            }
            else {
                // no pk, and different Realm, create an object
                Obj dst_link = dst_link_table->create_object();
                converted_src = ObjLink{dst_link_table->get_key(), dst_link.get_key()};
            }
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
                                                     EmbeddedObjectConverter* embedded_tracker)
    : m_embedded_tracker(embedded_tracker)
{
    m_columns_cache.reserve(table_src->get_column_count());
    ColKey pk_col = table_src->get_primary_key_column();
    for (ColKey col_key_src : table_src->get_column_keys()) {
        if (col_key_src == pk_col)
            continue;
        StringData col_name = table_src->get_column_name(col_key_src);
        ColKey col_key_dst = table_dst->get_column_key(col_name);
        REALM_ASSERT(col_key_dst);
        m_columns_cache.emplace_back(table_src, col_key_src, table_dst, col_key_dst, m_embedded_tracker);
    }
}

void InterRealmObjectConverter::copy(const Obj& src, Obj& dst, bool* update_out)
{
    for (auto& column : m_columns_cache) {
        column.copy_value(src, dst, update_out);
    }
}

} // namespace realm::converters
