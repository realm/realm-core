/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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


#include "realm/list.hpp"
#include "realm/cluster_tree.hpp"
#include "realm/array_basic.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/column_type_traits.hpp"

using namespace realm;


/********************************* ListBase **********************************/
template <class T>
ConstListIf<T>::ConstListIf(size_t col_ndx, Allocator& alloc)
    : ConstListBase(col_ndx)
    , m_leaf(new LeafType(alloc))
{
    m_leaf->set_parent(this, 0); // ndx not used, implicit in m_owner
}

template <class T>
ConstList<T>::ConstList(const ConstObj& obj, size_t col_ndx)
    : ConstListIf<T>(col_ndx, obj.get_alloc())
    , m_obj(obj)
{
    this->set_obj(&m_obj);
    this->init_from_parent();
}

ConstListBase::~ConstListBase()
{
}

ref_type ConstListBase::get_child_ref(size_t) const noexcept
{
    return to_ref(m_const_obj->get<int64_t>(m_col_ndx));
}

std::pair<ref_type, size_t> ConstListBase::get_to_dot_parent(size_t) const
{
    // TODO
    return {};
}

template <class T>
List<T>::List(const Obj& obj, size_t col_ndx)
    : ConstListIf<T>(col_ndx, obj.m_tree_top->get_alloc())
    , m_obj(obj)
{
    this->set_obj(&m_obj);
    this->init_from_parent();
    if (!ConstListIf<T>::m_valid) {
        create();
        ref_type ref = m_leaf->get_ref();
        m_obj.set_int(col_ndx, from_ref(ref));
    }
}

template <typename U>
ConstList<U> ConstObj::get_list(size_t col_ndx) const
{
    return ConstList<U>(*this, col_ndx);
}

template <typename U>
List<U> Obj::get_list(size_t col_ndx)
{
    return List<U>(*this, col_ndx);
}

namespace realm {
template ConstList<int64_t> ConstObj::get_list<int64_t>(size_t col_ndx) const;
template ConstList<bool> ConstObj::get_list<bool>(size_t col_ndx) const;
template ConstList<float> ConstObj::get_list<float>(size_t col_ndx) const;
template ConstList<double> ConstObj::get_list<double>(size_t col_ndx) const;
template ConstList<StringData> ConstObj::get_list<StringData>(size_t col_ndx) const;
template ConstList<BinaryData> ConstObj::get_list<BinaryData>(size_t col_ndx) const;
template ConstList<Timestamp> ConstObj::get_list<Timestamp>(size_t col_ndx) const;

template List<int64_t> Obj::get_list<int64_t>(size_t col_ndx);
template List<bool> Obj::get_list<bool>(size_t col_ndx);
template List<float> Obj::get_list<float>(size_t col_ndx);
template List<double> Obj::get_list<double>(size_t col_ndx);
template List<StringData> Obj::get_list<StringData>(size_t col_ndx);
template List<BinaryData> Obj::get_list<BinaryData>(size_t col_ndx);
template List<Timestamp> Obj::get_list<Timestamp>(size_t col_ndx);
}
