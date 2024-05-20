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

#ifndef REALM_OBJECT_CONVERTER_HPP
#define REALM_OBJECT_CONVERTER_HPP

#include <realm/mixed.hpp>
#include <realm/obj.hpp>

namespace realm::converters {

struct EmbeddedObjectConverter {
    void track(const Obj& e_src, const Obj& e_dst);
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
                             EmbeddedObjectConverter* ec);
    void track_new_embedded(const Obj& src, const Obj& dst) const;
    struct ConversionResult {
        Mixed converted_value;
        bool requires_new_embedded_object = false;
        Obj src_embedded_to_check;
    };

    // convert `src` to the destination Realm and compare that value with `dst`
    // If `converted_src_out` is provided, it will be set to the converted src value
    int cmp_src_to_dst(Mixed src, Mixed dst, ConversionResult* converted_src_out = nullptr,
                       bool* did_update_out = nullptr) const;
    void copy_value(const Obj& src_obj, Obj& dst_obj, bool* update_out);
    void copy_list(const LstBase& src_list, LstBase& dst_list) const;

private:
    void copy_list(const LstBase& src_obj, LstBase& dst_obj, bool* update_out) const;
    void copy_set(const SetBase& src_obj, SetBase& dst_obj, bool* update_out) const;
    void copy_dictionary(const Dictionary& src_obj, Dictionary& dst_obj, bool* update_out) const;
    // collection in mixed.
    void handle_list_in_mixed(const Lst<Mixed>& src_list, Lst<Mixed>& dst_list) const;
    void handle_dictionary_in_mixed(Dictionary& src_dict, Dictionary& dst_dict) const;
    void copy_list_in_mixed(const Lst<Mixed>& src_list, Lst<Mixed>& dst_list, size_t ndx_src, size_t ndx_dst,
                            CollectionType type) const;
    void copy_dictionary_in_mixed(const Dictionary& src_list, Dictionary& dst_list, StringData key,
                                  CollectionType type) const;
    bool check_if_list_elements_match(const Lst<Mixed>& src_list, Lst<Mixed>& dst_list, size_t ndx_src,
                                      size_t ndx_dst) const;
    bool check_if_dictionary_elements_match(const Dictionary& src_list, const Dictionary& dst_list,
                                            StringData key) const;
    bool is_collection(Mixed) const;
    CollectionType to_collection_type(Mixed) const;

    TableRef m_dst_link_table;
    ConstTableRef m_src_table;
    ConstTableRef m_dst_table;
    ColKey m_src_col;
    ColKey m_dst_col;
    TableRef m_opposite_of_src;
    TableRef m_opposite_of_dst;
    EmbeddedObjectConverter* m_embedded_converter;
    bool m_is_embedded_link;
    const bool m_primitive_types_only;
};

struct InterRealmObjectConverter {
    InterRealmObjectConverter(ConstTableRef table_src, TableRef table_dst, EmbeddedObjectConverter* embedded_tracker);
    void copy(const Obj& src, Obj& dst, bool* update_out);

private:
    EmbeddedObjectConverter* m_embedded_tracker;
    std::vector<InterRealmValueConverter> m_columns_cache;
};

} // namespace realm::converters

#endif // REALM_OBJECT_CONVERTER_HPP
