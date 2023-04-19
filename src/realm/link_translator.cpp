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

#include "realm/link_translator.hpp"

#include <realm/dictionary.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>

namespace realm {

LinkTranslator::LinkTranslator(Obj origin, ColKey origin_col_key)
    : m_origin_obj(origin)
    , m_origin_col_key(origin_col_key)
{
}

void LinkTranslator::run()
{
    ColumnAttrMask attr = m_origin_col_key.get_attrs();
    if (attr.test(col_attr_List)) {
        if (m_origin_col_key.get_type() == col_type_LinkList) {
            LnkLst link_list = m_origin_obj.get_linklist(m_origin_col_key);
            on_list_of_links(link_list);
        }
        else if (m_origin_col_key.get_type() == col_type_Mixed) {
            Lst<Mixed> list = m_origin_obj.get_list<Mixed>(m_origin_col_key);
            on_list_of_mixed(list);
        }
        else if (m_origin_col_key.get_type() == col_type_TypedLink) {
            Lst<ObjLink> list = m_origin_obj.get_list<ObjLink>(m_origin_col_key);
            on_list_of_typedlink(list);
        }
        else {
            throw std::runtime_error(
                util::format("LinkTranslator unhandled list type: %1", m_origin_col_key.get_type()));
        }
    }
    else if (attr.test(col_attr_Set)) {
        if (m_origin_col_key.get_type() == col_type_Link) {
            LnkSet set = m_origin_obj.get_linkset(m_origin_col_key);
            on_set_of_links(set);
        }
        else if (m_origin_col_key.get_type() == col_type_Mixed) {
            Set<Mixed> set = m_origin_obj.get_set<Mixed>(m_origin_col_key);
            on_set_of_mixed(set);
        }
        else if (m_origin_col_key.get_type() == col_type_TypedLink) {
            Set<ObjLink> set = m_origin_obj.get_set<ObjLink>(m_origin_col_key);
            on_set_of_typedlink(set);
        }
        else {
            throw std::runtime_error(
                util::format("LinkTranslator unhandled set type: %1", m_origin_col_key.get_type()));
        }
    }
    else if (attr.test(col_attr_Dictionary)) {
        auto dict = m_origin_obj.get_dictionary(m_origin_col_key);
        on_dictionary(dict);
    }
    else {
        REALM_ASSERT_EX(!m_origin_col_key.is_collection(), m_origin_col_key);
        if (m_origin_col_key.get_type() == col_type_Link) {
            on_link_property(m_origin_col_key);
        }
        else if (m_origin_col_key.get_type() == col_type_Mixed) {
            on_mixed_property(m_origin_col_key);
        }
        else if (m_origin_col_key.get_type() == col_type_TypedLink) {
            on_typedlink_property(m_origin_col_key);
        }
        else {
            throw std::runtime_error(
                util::format("LinkTranslator unhandled property type: %1", m_origin_col_key.get_type()));
        }
    }
}

} // namespace realm
