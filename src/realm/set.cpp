/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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


#include "realm/set.hpp"
#include "realm/replication.hpp"

namespace realm {

template <class T>
Set<T>::Set(const Obj& obj, ColKey col_key)
    : CollectionBase(obj, col_key)
    , Collection<T>(obj.get_alloc())
{
    if (CollectionBase::m_obj) {
        Collection<T>::init_from_parent();
    }
}


template Set<int64_t>::Set(const Obj& obj, ColKey col_key);
template Set<StringData>::Set(const Obj& obj, ColKey col_key);

/***************************** Set<T>::insert_repl *****************************/

template <>
void Set<Int>::insert_repl(Replication*, int64_t)
{
    // repl->set_insert_int(*this, value);
}

template <>
void Set<String>::insert_repl(Replication*, StringData)
{
    // repl->set_insert_string(*this, value);
}

} // namespace realm
