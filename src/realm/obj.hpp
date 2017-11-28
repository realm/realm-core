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

#ifndef REALM_OBJ_HPP
#define REALM_OBJ_HPP

#include <realm/array.hpp>
#include <realm/cluster.hpp>
#include <realm/table_ref.hpp>
#include <realm/keys.hpp>

namespace realm {

template <class>
class ConstListIf;

template <class>
class ConstList;
template <class T>
using ConstListPtr = std::unique_ptr<ConstList<T>>;

template <class>
class List;
template <class T>
using ListPtr = std::unique_ptr<List<T>>;

class LinkList;
class ConstLinkList;
using LinkListPtr = std::unique_ptr<LinkList>;
using ConstLinkListPtr = std::unique_ptr<ConstLinkList>;

// 'Object' would have been a better name, but it clashes with a class in ObjectStore
class ConstObj {
public:
    ConstObj(const ClusterTree* tree_top, ref_type ref, Key key, size_t row_ndx);
    ConstObj& operator=(const ConstObj&) = delete;

    Allocator& get_alloc() const;

    Key get_key() const
    {
        return m_key;
    }
    const Table* get_table() const;

    template <typename U>
    U get(size_t col_ndx) const;

    template <typename U>
    U get(StringData col_name) const
    {
        return get<U>(get_column_index(col_name));
    }

    template <typename U>
    ConstList<U> get_list(size_t col_ndx) const;
    template <typename U>
    ConstListPtr<U> get_list_ptr(size_t col_ndx) const;

    ConstLinkList get_linklist(size_t col_ndx);
    ConstLinkListPtr get_linklist_ptr(size_t col_ndx);

    size_t get_link_count(size_t col_ndx) const;

    bool is_null(size_t col_ndx) const;
    size_t get_backlink_count(const Table& origin, size_t origin_col_ndx) const;
    Key get_backlink(const Table& origin, size_t origin_col_ndx, size_t backlink_ndx) const;
    size_t get_backlink_count(size_t backlink_col_ndx) const;
    Key get_backlink(size_t backlink_col_ndx, size_t backlink_ndx) const;

    // To be used by the query system when a single object should
    // be tested. Will allow a function to be called in the context
    // of the owning cluster.
    template <class T>
    bool evaluate(T func) const
    {
        Cluster cluster(get_alloc(), *m_tree_top);
        cluster.init_from_mem(m_mem);
        return func(&cluster, m_row_ndx);
    }

protected:
    friend class ConstListBase;
    friend class ConstLinkListIf;
    friend class LinkList;

    const ClusterTree* m_tree_top;
    Key m_key;
    mutable MemRef m_mem;
    mutable size_t m_row_ndx;
    mutable uint64_t m_version;
    bool update_if_needed() const;
    void update(ConstObj other) const
    {
        m_mem = other.m_mem;
        m_row_ndx = other.m_row_ndx;
        m_version = other.m_version;
    }
    template <class T>
    bool do_is_null(size_t col_ndx) const;

    size_t get_column_index(StringData col_name) const;
    TableKey get_table_key() const;
    TableRef get_target_table(size_t col_ndx) const;
};

class Obj : public ConstObj {
public:
    Obj(ClusterTree* tree_top, ref_type ref, Key key, size_t row_ndx);

    template <typename U>
    Obj& set(size_t col_ndx, U value, bool is_default = false);
    Obj& set_null(size_t col_ndx, bool is_default = false);

    Obj& add_int(size_t col_ndx, int64_t value);

    template <typename U>
    Obj& set_list_values(size_t col_ndx, const std::vector<U>& values);

    template <typename U>
    std::vector<U> get_list_values(size_t col_ndx);

    template <class Head, class... Tail>
    Obj& set_all(Head v, Tail... tail);

    template <typename U>
    List<U> get_list(size_t col_ndx);
    template <typename U>
    ListPtr<U> get_list_ptr(size_t col_ndx);

    LinkList get_linklist(size_t col_ndx);
    LinkListPtr get_linklist_ptr(size_t col_ndx);

private:
    friend class Cluster;
    friend class ConstListBase;
    friend class ArrayBacklink;
    template <class>
    friend class List;

    mutable bool m_writeable;

    template <class Val>
    Obj& _set(size_t col_ndx, Val v);
    template <class Head, class... Tail>
    Obj& _set(size_t col_ndx, Head v, Tail... tail);
    bool update_if_needed() const;
    bool is_writeable() const
    {
        return m_writeable;
    }
    void ensure_writeable();
    void bump_version();
    template <class T>
    void do_set_null(size_t col_ndx);

    void set_int(size_t col_ndx, int64_t value);
    void add_backlink(size_t backlink_col, Key origin_key);
    void remove_one_backlink(size_t backlink_col, Key origin_key);
    void nullify_link(size_t origin_col, Key target_key);
};


template <>
inline Optional<float> ConstObj::get<Optional<float>>(size_t col_ndx) const
{
    float f = get<float>(col_ndx);
    return null::is_null_float(f) ? util::none : util::make_optional(f);
}

template <>
inline Optional<double> ConstObj::get<Optional<double>>(size_t col_ndx) const
{
    double f = get<double>(col_ndx);
    return null::is_null_float(f) ? util::none : util::make_optional(f);
}

template <>
Obj& Obj::set(size_t, int64_t value, bool is_default);

template <>
Obj& Obj::set(size_t, Key value, bool is_default);


template <>
inline Obj& Obj::set(size_t col_ndx, int value, bool is_default)
{
    return set(col_ndx, int_fast64_t(value), is_default);
}

template <>
inline Obj& Obj::set(size_t col_ndx, const char* str, bool is_default)
{
    return set(col_ndx, StringData(str), is_default);
}

template <typename U>
Obj& Obj::set_list_values(size_t col_ndx, const std::vector<U>& values)
{
    size_t sz = values.size();
    auto list = get_list<U>(col_ndx);
    list.resize(sz);
    for (size_t i = 0; i < sz; i++)
        list.set(i, values[i]);

    return *this;
}

template <typename U>
std::vector<U> Obj::get_list_values(size_t col_ndx)
{
    std::vector<U> values;
    auto list = get_list<U>(col_ndx);
    for (auto v : list)
        values.push_back(v);

    return values;
}

template <class Val>
inline Obj& Obj::_set(size_t col_ndx, Val v)
{
    return set(col_ndx, v);
}

template <class Head, class... Tail>
inline Obj& Obj::_set(size_t col_ndx, Head v, Tail... tail)
{
    set(col_ndx, v);
    return _set(col_ndx + 1, tail...);
}

template <class Head, class... Tail>
inline Obj& Obj::set_all(Head v, Tail... tail)
{
    return _set(0, v, tail...);
}
}

#endif // REALM_OBJ_HPP
