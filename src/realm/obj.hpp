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

namespace realm {

template <class>
class ConstListIf;
template <class>
class ConstList;
template <class>
class List;

template <class T>
using ConstListRef = std::unique_ptr<ConstList<T>>;
template <class T>
using ListRef = std::unique_ptr<List<T>>;

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

    template <typename U>
    U get(size_t col_ndx) const;

    template <typename U>
    U get(StringData col_name) const
    {
        return get<U>(get_column_index(col_name));
    }

    template <typename U>
    ConstList<U> get_list(size_t col_ndx) const;

    bool is_null(size_t col_ndx) const;

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

private:
    size_t get_column_index(StringData col_name) const;
};

class Obj : public ConstObj {
public:
    Obj(ClusterTree* tree_top, ref_type ref, Key key, size_t row_ndx);

    template <typename U>
    Obj& set(size_t col_ndx, U value, bool is_default = false);
    Obj& set_null(size_t col_ndx, bool is_default = false);

    template <typename U>
    Obj& set_list_values(size_t col_ndx, const std::vector<U>& values);

    template <typename U>
    std::vector<U> get_list_values(size_t col_ndx);

    template <class Head, class... Tail>
    Obj& set_all(Head v, Tail... tail);

    template <typename U>
    List<U> get_list(size_t col_ndx);

private:
    friend class ConstListBase;
    template <class>
    friend class List;

    mutable bool m_writeable;

    template <class Val>
    Obj& _set(size_t col_ndx, Val v);
    template <class Head, class... Tail>
    Obj& _set(size_t col_ndx, Head v, Tail... tail);
    bool update_if_needed() const;
    template <class T>
    void do_set_null(size_t col_ndx);

    void set_int(size_t col_ndx, int64_t value);
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
