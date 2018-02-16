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

#ifndef REALM_OBJ_LIST_HPP
#define REALM_OBJ_LIST_HPP

#include <realm/array_key.hpp>
#include <realm/table_ref.hpp>
#include <realm/handover_defs.hpp>
#include <realm/obj.hpp>

namespace realm {

class DescriptorOrdering;
class Table;
class ConstTableView;

class ObjList {
public:
    ObjList(KeyColumn& key_values);
    ObjList(KeyColumn& key_values, const Table* parent);

    virtual ~ObjList()
    {
#ifdef REALM_COOKIE_CHECK
        m_debug_cookie = 0x7765697633333333; // 0x77656976 = 'view'; 0x33333333 = '3333' = destructed
#endif
    }

    const Table& get_parent() const noexcept
    {
        return *m_table;
    }

    virtual size_t size() const
    {
        return m_key_values.size();
    }

    // Get key for object this view is "looking" at.
    ObjKey get_key(size_t ndx) const
    {
        return ObjKey(m_key_values.get(ndx));
    }

    ConstObj get(size_t row_ndx) const;
    ConstObj front() const noexcept
    {
        return get(0);
    }
    ConstObj back() const noexcept
    {
        size_t last_row_ndx = size() - 1;
        return get(last_row_ndx);
    }
    ConstObj operator[](size_t row_ndx) const noexcept
    {
        return get(row_ndx);
    }

    template <class F>
    void for_each(F func) const;

    template <class T>
    ConstTableView find_all(ColKey column_key, T value);

    template <class T>
    ObjKey find_first(ColKey column_key, T value);

    // These two methods are overridden by TableView and LinkView.
    virtual TableVersions sync_if_needed() const = 0;
    virtual bool is_in_sync() const = 0;
    void check_cookie() const
    {
#ifdef REALM_COOKIE_CHECK
        REALM_ASSERT_RELEASE(m_debug_cookie == cookie_expected);
#endif
    }

protected:
    friend class Query;
    static const uint64_t cookie_expected = 0x7765697677777777ull; // 0x77656976 = 'view'; 0x77777777 = '7777' = alive

    // Null if, and only if, the view is detached.
    mutable ConstTableRef m_table;
    KeyColumn& m_key_values;
    uint64_t m_debug_cookie;

    void do_sort(const DescriptorOrdering&);
    void detach() const noexcept // may have to remove const
    {
        m_table = TableRef();
    }
};

template <class F>
void ObjList::for_each(F func) const
{
    auto sz = size();
    for (size_t i = 0; i < sz; i++) {
        try {
            ConstObj o = get(i);
            if (func(o))
                return;
        }
        catch (const InvalidKey&) {
        }
    }
}

template <class T>
ObjKey ObjList::find_first(ColKey column_key, T value)
{
    ObjKey k;
    for_each([column_key, value, &k](ConstObj& o) {
        T v = o.get<T>(column_key);
        if (v == value) {
            k = o.get_key();
            return true;
        }
        return false;
    });
    return k;
}
}

#endif /* SRC_REALM_OBJ_LIST_HPP_ */
