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

#define REALM_CLUSTER_IF

namespace realm {

class LstBase;

template <class>
class ConstLstIf;

template <class>
class ConstLst;

template <class>
class Lst;
template <class T>
using LstPtr = std::unique_ptr<Lst<T>>;
template <class T>
using ConstLstPtr = std::unique_ptr<const Lst<T>>;
using LstBasePtr = std::unique_ptr<LstBase>;

class LnkLst;
class ConstLnkLst;
using LnkLstPtr = std::unique_ptr<LnkLst>;
using ConstLnkLstPtr = std::unique_ptr<const LnkLst>;

// 'Object' would have been a better name, but it clashes with a class in ObjectStore
class ConstObj {
public:
    ConstObj()
        : m_table(nullptr)
        , m_row_ndx(size_t(-1))
        , m_storage_version(-1)
        , m_instance_version(-1)
        , m_valid(false)
    {
    }
    ConstObj(const ClusterTree* tree_top, ref_type ref, ObjKey key, size_t row_ndx);

    Allocator& get_alloc() const;

    bool operator==(const ConstObj& other) const;

    ObjKey get_key() const
    {
        return m_key;
    }

    const Table* get_table() const
    {
        return m_table;
    }

    // Check if this object is default constructed
    explicit operator bool()
    {
        return m_table != nullptr;
    }

    // Check if the object is still alive
    bool is_valid() const;
    // Delete object from table. Object is invalid afterwards.
    void remove();

    template <typename U>
    U get(ColKey col_key) const;

    template <typename U>
    U get(StringData col_name) const
    {
        return get<U>(get_column_key(col_name));
    }
    ConstObj get_linked_object(ColKey link_col_key) const;
    int cmp(const ConstObj& other, ColKey col_key) const;

    template <typename U>
    ConstLst<U> get_list(ColKey col_key) const;
    template <typename U>
    ConstLstPtr<U> get_list_ptr(ColKey col_key) const;

    ConstLnkLst get_linklist(ColKey col_key);
    ConstLnkLstPtr get_linklist_ptr(ColKey col_key);

    size_t get_link_count(ColKey col_key) const;

    bool is_null(ColKey col_key) const;
    bool has_backlinks(bool only_strong_links) const;
    size_t get_backlink_count(bool only_strong_links = false) const;
    size_t get_backlink_count(const Table& origin, ColKey origin_col_key) const;
    ObjKey get_backlink(const Table& origin, ColKey origin_col_key, size_t backlink_ndx) const;

    // To be used by the query system when a single object should
    // be tested. Will allow a function to be called in the context
    // of the owning cluster.
    template <class T>
    bool evaluate(T func) const
    {
        Cluster cluster(0, get_alloc(), *get_tree_top());
        cluster.init_from_mem(m_mem);
        return func(&cluster, m_row_ndx);
    }

protected:
    friend class ConstLstBase;
    friend class ConstLnkLst;
    friend class LnkLst;
    friend class LinkMap;
    friend class ConstTableView;
    friend class Transaction;

    const Table* m_table;
    ObjKey m_key;
    mutable MemRef m_mem;
    mutable size_t m_row_ndx;
    mutable uint64_t m_storage_version;
    mutable uint64_t m_instance_version;
    mutable bool m_valid;
    bool is_in_sync() const;
    void do_update() const;
    bool update_if_needed() const;
    void update(ConstObj& other) const
    {
        m_mem = other.m_mem;
        m_row_ndx = other.m_row_ndx;
        m_storage_version = other.m_storage_version;
        m_instance_version = other.m_instance_version;
    }
    template <class T>
    bool do_is_null(size_t col_ndx) const;

    const ClusterTree* get_tree_top() const;
    ColKey get_column_key(StringData col_name) const;
    TableKey get_table_key() const;
    TableRef get_target_table(ColKey col_key) const;
    const Spec& get_spec() const;

    template <typename U>
    U _get(size_t col_ndx) const;

    template <class T>
    int cmp(const ConstObj& other, size_t col_ndx) const;
    int cmp(const ConstObj& other, size_t col_ndx) const;
    size_t get_backlink_count(size_t backlink_col_ndx) const;
    ObjKey get_backlink(size_t backlink_col_ndx, size_t backlink_ndx) const;
};


class Obj : public ConstObj {
public:
    Obj()
        : m_writeable(false)
    {
    }
    Obj(ClusterTree* tree_top, ref_type ref, ObjKey key, size_t row_ndx);

    template <typename U>
    Obj& set(ColKey col_key, U value, bool is_default = false);

    template <typename U>
    Obj& set(StringData col_name, U value, bool is_default = false)
    {
        return set(get_column_key(col_name), value, is_default);
    }

    Obj& set_null(ColKey col_key, bool is_default = false);

    Obj& add_int(ColKey col_key, int64_t value);

    template <typename U>
    Obj& set_list_values(ColKey col_key, const std::vector<U>& values);

    template <typename U>
    std::vector<U> get_list_values(ColKey col_key);

    template <class Head, class... Tail>
    Obj& set_all(Head v, Tail... tail);

    template <typename U>
    Lst<U> get_list(ColKey col_key);
    template <typename U>
    LstPtr<U> get_list_ptr(ColKey col_key);

    LnkLst get_linklist(ColKey col_key);
    LnkLstPtr get_linklist_ptr(ColKey col_key);

    LstBasePtr get_listbase_ptr(ColKey col_key, DataType type);

private:
    friend class Cluster;
    friend class ConstLstBase;
    friend class ArrayBacklink;
    friend class ConstObj;
    template <class>
    friend class Lst;
    friend class LnkLst;

    mutable bool m_writeable;

    Obj(const ConstObj& other)
        : ConstObj(other)
        , m_writeable(false)
    {
    }
    template <class Val>
    Obj& _set(size_t col_ndx, Val v);
    template <class Head, class... Tail>
    Obj& _set(size_t col_ndx, Head v, Tail... tail);
    ColKey ndx2colkey(size_t col_ndx);
    bool update_if_needed() const;
    bool is_writeable() const
    {
        return m_writeable;
    }
    void ensure_writeable();
    void bump_content_version();
    void bump_both_versions();
    template <class T>
    void do_set_null(size_t col_ndx);

    void set_int(ColKey col_key, int64_t value);
    void add_backlink(ColKey backlink_col, ObjKey origin_key);
    bool remove_one_backlink(ColKey backlink_col, ObjKey origin_key);
    void nullify_link(ColKey origin_col, ObjKey target_key);
    // Used when inserting a new link. You will not remove existing links in this process
    void set_backlink(ColKey col_key, ObjKey new_key);
    // Used when replacing a link, return true if CascadeState contains objects to remove
    bool replace_backlink(ColKey col_key, ObjKey old_key, ObjKey new_key, CascadeState& state);
    // Used when removing a backlink, return true if CascadeState contains objects to remove
    bool remove_backlink(ColKey col_key, ObjKey old_key, CascadeState& state);
};


template <>
inline Optional<float> ConstObj::get<Optional<float>>(ColKey col_key) const
{
    float f = get<float>(col_key);
    return null::is_null_float(f) ? util::none : util::make_optional(f);
}

template <>
inline Optional<double> ConstObj::get<Optional<double>>(ColKey col_key) const
{
    double f = get<double>(col_key);
    return null::is_null_float(f) ? util::none : util::make_optional(f);
}

template <>
Obj& Obj::set(ColKey, int64_t value, bool is_default);

template <>
Obj& Obj::set(ColKey, ObjKey value, bool is_default);


template <>
inline Obj& Obj::set(ColKey col_key, int value, bool is_default)
{
    return set(col_key, int_fast64_t(value), is_default);
}

template <>
inline Obj& Obj::set(ColKey col_key, const char* str, bool is_default)
{
    return set(col_key, StringData(str), is_default);
}

template <>
inline Obj& Obj::set(ColKey col_key, char* str, bool is_default)
{
    return set(col_key, StringData(str), is_default);
}

template <>
inline Obj& Obj::set(ColKey col_key, std::string str, bool is_default)
{
    return set(col_key, StringData(str), is_default);
}

template <>
inline Obj& Obj::set(ColKey col_key, realm::null, bool is_default)
{
    return set_null(col_key, is_default);
}

template <typename U>
Obj& Obj::set_list_values(ColKey col_key, const std::vector<U>& values)
{
    size_t sz = values.size();
    auto list = get_list<U>(col_key);
    list.resize(sz);
    for (size_t i = 0; i < sz; i++)
        list.set(i, values[i]);

    return *this;
}

template <typename U>
std::vector<U> Obj::get_list_values(ColKey col_key)
{
    std::vector<U> values;
    auto list = get_list<U>(col_key);
    for (auto v : list)
        values.push_back(v);

    return values;
}

template <class Val>
inline Obj& Obj::_set(size_t col_ndx, Val v)
{
    return set(ndx2colkey(col_ndx), v);
}

template <class Head, class... Tail>
inline Obj& Obj::_set(size_t col_ndx, Head v, Tail... tail)
{
    set(ndx2colkey(col_ndx), v);
    return _set(col_ndx + 1, tail...);
}

template <class Head, class... Tail>
inline Obj& Obj::set_all(Head v, Tail... tail)
{
    return _set(0, v, tail...);
}
}

#endif // REALM_OBJ_HPP
