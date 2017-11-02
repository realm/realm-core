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

namespace realm {
template ConstList<int64_t>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<bool>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<float>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<double>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<StringData>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<BinaryData>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<Timestamp>::ConstList(const ConstObj& obj, size_t col_ndx);

template List<int64_t>::List(const Obj& obj, size_t col_ndx);
template List<bool>::List(const Obj& obj, size_t col_ndx);
template List<float>::List(const Obj& obj, size_t col_ndx);
template List<double>::List(const Obj& obj, size_t col_ndx);
template List<StringData>::List(const Obj& obj, size_t col_ndx);
template List<BinaryData>::List(const Obj& obj, size_t col_ndx);
template List<Timestamp>::List(const Obj& obj, size_t col_ndx);
}
