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

#ifndef REALM_SET_HPP
#define REALM_SET_HPP

#include <realm/list.hpp>

namespace realm {

template <class T>
class Set : public Collection<T> {
public:
    using Collection<T>::m_tree;
    using Collection<T>::get;

    Set() = default;
    Set(const Obj& owner, ColKey col_key);

    void insert(T value)
    {
        REALM_ASSERT_DEBUG(!update_if_needed());

        ensure_created();
        this->ensure_writeable();
        auto b = this->begin();
        auto e = this->end();
        auto it = std::lower_bound(b, e, value);
        if (it != e && *it == value) {
            return;
        }
        if (Replication* repl = this->m_obj.get_replication()) {
            insert_repl(repl, value);
        }
        m_tree->insert(it.m_ndx, value);
        CollectionBase::m_obj.bump_content_version();
    }

    size_t find(T value)
    {
        return m_tree->find_first(value);
    }

    void erase(size_t ndx)
    {
        REALM_ASSERT_DEBUG(!update_if_needed());
        this->ensure_writeable();
        if (Replication* repl = this->m_obj.get_replication()) {
            CollectionBase::erase_repl(repl, ndx);
        }
        m_tree->erase(ndx);
        CollectionBase::adj_remove(ndx);
        CollectionBase::m_obj.bump_content_version();
    }

private:
    void create()
    {
        m_tree->create();
        Collection<T>::m_valid = true;
    }

    bool update_if_needed()
    {
        if (CollectionBase::m_obj.update_if_needed()) {
            return this->init_from_parent();
        }
        return false;
    }
    void ensure_created()
    {
        if (!Collection<T>::m_valid && CollectionBase::m_obj.is_valid()) {
            create();
        }
    }
    void insert_repl(Replication* repl, T value);
};

template <typename U>
Set<U> Obj::get_set(ColKey col_key) const
{
    return Set<U>(*this, col_key);
}

} // namespace realm

#endif // REALM_SET_HPP
