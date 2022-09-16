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

#include <memory>

namespace realm::converters {

struct EmbeddedObjectConverter : std::enable_shared_from_this<EmbeddedObjectConverter> {
    void track(Obj e_src, Obj e_dst);
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
                             std::shared_ptr<EmbeddedObjectConverter> ec);
    void track_new_embedded(Obj src, Obj dst);
    struct ConversionResult {
        Mixed converted_value;
        bool requires_new_embedded_object = false;
        Obj src_embedded_to_check;
    };

    // convert `src` to the destination Realm and compare that value with `dst`
    // If `converted_src_out` is provided, it will be set to the converted src value
    int cmp_src_to_dst(Mixed src, Mixed dst, ConversionResult* converted_src_out = nullptr,
                       bool* did_update_out = nullptr);
    void copy_value(const Obj& src_obj, Obj& dst_obj, bool* update_out);

private:
    void copy_list(const Obj& src_obj, Obj& dst_obj, bool* update_out);
    void copy_set(const Obj& src_obj, Obj& dst_obj, bool* update_out);
    void copy_dictionary(const Obj& src_obj, Obj& dst_obj, bool* update_out);

    TableRef m_dst_link_table;
    ConstTableRef m_src_table;
    ConstTableRef m_dst_table;
    ColKey m_src_col;
    ColKey m_dst_col;
    TableRef m_opposite_of_src;
    TableRef m_opposite_of_dst;
    std::shared_ptr<EmbeddedObjectConverter> m_embedded_converter;
    bool m_is_embedded_link;
    const bool m_primitive_types_only;
};

struct InterRealmObjectConverter {
    InterRealmObjectConverter(ConstTableRef table_src, TableRef table_dst,
                              std::shared_ptr<EmbeddedObjectConverter> embedded_tracker);
    void copy(const Obj& src, Obj& dst, bool* update_out);

private:
    void populate_columns_from_table(ConstTableRef table_src, ConstTableRef table_dst);
    std::shared_ptr<EmbeddedObjectConverter> m_embedded_tracker;
    std::vector<InterRealmValueConverter> m_columns_cache;
};

} // namespace realm::converters

#endif // REALM_OBJECT_CONVERTER_HPP
