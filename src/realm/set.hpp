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

#include <realm/collection.hpp>

#include <numeric> // std::iota

namespace realm {

class SetBase : public CollectionBase {
public:
    using CollectionBase::CollectionBase;

    virtual ~SetBase() {}
    SetBasePtr clone() const
    {
        return m_obj.get_setbase_ptr(m_col_key);
    }

    virtual void insert_null() = 0;
    virtual void erase_null() = 0;
    virtual void insert_any(Mixed value) = 0;
    virtual void erase_any(Mixed value) = 0;
    virtual void clear() = 0;

protected:
    void insert_repl(Replication* repl, size_t index, Mixed value) const;
    void erase_repl(Replication* repl, size_t index, Mixed value) const;
    void clear_repl(Replication* repl) const;
};

template <class T>
class Set : public Collection<T, SetBase> {
public:
    using Collection<T, SetBase>::m_tree;
    using Collection<T, SetBase>::size;
    using Collection<T, SetBase>::begin;
    using Collection<T, SetBase>::end;
    using Collection<T, SetBase>::get;

    Set() = default;
    Set(const Obj& owner, ColKey col_key);

    /// Insert a value into the set if it does not already exist, returning the index of the inserted value,
    /// or the index of the already-existing value.
    size_t insert(T value);

    /// Find the index of a value in the set, or `size_t(-1)` if it is not in the set.
    size_t find(T value) const;

    /// Erase an element from the set, returning the index at which it was removed, or `size_t(-1)` if it did
    /// not exist.
    size_t erase(T value);

    // Overriding members of CollectionBase:
    Mixed min(size_t* return_ndx = nullptr) const final;
    Mixed max(size_t* return_ndx = nullptr) const final;
    Mixed sum(size_t* return_cnt = nullptr) const final;
    Mixed avg(size_t* return_cnt = nullptr) const final;
    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;

    // Overriding members of SetBase:
    void insert_null() override;
    void erase_null() override;
    void insert_any(Mixed value) override;
    void erase_any(Mixed value) override;
    void clear() override;

private:
    using Collection<T, SetBase>::m_valid;
    using Collection<T, SetBase>::m_obj;

    void create()
    {
        m_tree->create();
        m_valid = true;
    }

    bool update_if_needed()
    {
        if (m_obj.update_if_needed()) {
            return this->init_from_parent();
        }
        return false;
    }
    void ensure_created()
    {
        if (!m_valid && m_obj.is_valid()) {
            create();
        }
    }
};

/// Compare set elements.
///
/// We cannot use `std::less<>` directly, because the ordering of set elements
/// impacts the file format. For primitive types this is trivial (and can indeed
/// be just `std::less<>`), but for example `Mixed` has specialized comparison
/// that defines equality of numeric types.
template <class T>
struct SetElementLessThan {
    bool operator()(const T& a, const T& b) const noexcept
    {
        // CAUTION: This routine is technically part of the file format, because
        // it determines the storage order of Set elements.
        return a < b;
    }
};

template <class T>
struct SetElementEquals {
    bool operator()(const T& a, const T& b) const noexcept
    {
        // CAUTION: This routine is technically part of the file format, because
        // it determines the storage order of Set elements.
        return a == b;
    }
};

template <>
struct SetElementLessThan<Mixed> {
    bool operator()(const Mixed& a, const Mixed& b) const noexcept
    {
        // CAUTION: This routine is technically part of the file format, because
        // it determines the storage order of Set elements.

        if (a.is_null() != b.is_null()) {
            // If a is NULL but not b, a < b.
            return a.is_null();
        }
        else if (a.is_null()) {
            // NULLs are equal.
            return false;
        }

        if (a.get_type() != b.get_type()) {
            return a.get_type() < b.get_type();
        }

        switch (a.get_type()) {
            case type_Int:
                return a.get<int64_t>() < b.get<int64_t>();
            case type_Bool:
                return a.get<bool>() < b.get<bool>();
            case type_String:
                return a.get<StringData>() < b.get<StringData>();
            case type_Binary:
                return a.get<BinaryData>() < b.get<BinaryData>();
            case type_Timestamp:
                return a.get<Timestamp>() < b.get<Timestamp>();
            case type_Float:
                return a.get<float>() < b.get<float>();
            case type_Double:
                return a.get<double>() < b.get<double>();
            case type_Decimal:
                return a.get<Decimal128>() < b.get<Decimal128>();
            case type_ObjectId:
                return a.get<ObjectId>() < b.get<ObjectId>();
            case type_TypedLink:
                return a.get<ObjLink>() < b.get<ObjLink>();
            case type_OldTable:
                [[fallthrough]];
            case type_Mixed:
                [[fallthrough]];
            case type_OldDateTime:
                [[fallthrough]];
            case type_Link:
                [[fallthrough]];
            case type_LinkList:
                REALM_TERMINATE("Invalid Mixed payload in Set.");
        }
        return false;
    }
};

template <>
struct SetElementEquals<Mixed> {
    bool operator()(const Mixed& a, const Mixed& b) const noexcept
    {
        // CAUTION: This routine is technically part of the file format, because
        // it determines the storage order of Set elements.

        if (a.is_null() != b.is_null()) {
            return false;
        }
        else if (a.is_null()) {
            return true;
        }

        if (a.get_type() != b.get_type()) {
            return false;
        }

        switch (a.get_type()) {
            case type_Int:
                return a.get<int64_t>() == b.get<int64_t>();
            case type_Bool:
                return a.get<bool>() == b.get<bool>();
            case type_String:
                return a.get<StringData>() == b.get<StringData>();
            case type_Binary:
                return a.get<BinaryData>() == b.get<BinaryData>();
            case type_Timestamp:
                return a.get<Timestamp>() == b.get<Timestamp>();
            case type_Float:
                return a.get<float>() == b.get<float>();
            case type_Double:
                return a.get<double>() == b.get<double>();
            case type_Decimal:
                return a.get<Decimal128>() == b.get<Decimal128>();
            case type_ObjectId:
                return a.get<ObjectId>() == b.get<ObjectId>();
            case type_TypedLink:
                return a.get<ObjLink>() == b.get<ObjLink>();
            case type_OldTable:
                [[fallthrough]];
            case type_Mixed:
                [[fallthrough]];
            case type_OldDateTime:
                [[fallthrough]];
            case type_Link:
                [[fallthrough]];
            case type_LinkList:
                REALM_TERMINATE("Invalid Mixed payload in Set.");
        }
        return false;
    }
};

template <class T>
inline Set<T>::Set(const Obj& obj, ColKey col_key)
    : Collection<T, SetBase>(obj, col_key)
{
    if (m_obj) {
        this->init_from_parent();
    }
}

template <typename U>
Set<U> Obj::get_set(ColKey col_key) const
{
    return Set<U>(*this, col_key);
}

template <class T>
size_t Set<T>::find(T value) const
{
    auto b = this->begin();
    auto e = this->end();
    auto it = std::lower_bound(b, e, value, SetElementLessThan<T>{});
    if (it != e && SetElementEquals<T>{}(*it, value)) {
        return it.index();
    }
    return npos;
}

template <class T>
size_t Set<T>::insert(T value)
{
    REALM_ASSERT_DEBUG(!update_if_needed());

    ensure_created();
    this->ensure_writeable();
    auto b = this->begin();
    auto e = this->end();
    auto it = std::lower_bound(b, e, value, SetElementLessThan<T>{});

    if (it != e && SetElementEquals<T>{}(*it, value)) {
        return it.index();
    }

    if (Replication* repl = m_obj.get_replication()) {
        // FIXME: We should emit an instruction regardless of element presence for the purposes of conflict
        // resolution in synchronized databases. The reason is that the new insertion may come at a later time
        // than an interleaving erase instruction, so emitting the instruction ensures that last "write" wins.
        this->insert_repl(repl, it.index(), value);
    }

    m_tree->insert(it.index(), value);
    CollectionBase::m_obj.bump_content_version();
    return it.index();
}

template <class T>
void Set<T>::insert_any(Mixed value)
{
    if constexpr (std::is_same_v<T, Mixed>) {
        insert(value);
    }
    else {
        if (value.is_null()) {
            insert_null();
        }
        else {
            insert(value.get<typename util::RemoveOptional<T>::type>());
        }
    }
}

template <class T>
size_t Set<T>::erase(T value)
{
    REALM_ASSERT_DEBUG(!update_if_needed());
    this->ensure_writeable();

    auto b = this->begin();
    auto e = this->end();
    auto it = std::lower_bound(b, e, value, SetElementLessThan<T>{});

    if (it == e || !SetElementEquals<T>{}(*it, value)) {
        return not_found;
    }

    if (Replication* repl = m_obj.get_replication()) {
        this->erase_repl(repl, it.index(), value);
    }
    m_tree->erase(it.index());
    CollectionBase::m_obj.bump_content_version();
    return it.index();
}

template <class T>
void Set<T>::erase_any(Mixed value)
{
    if constexpr (std::is_same_v<T, Mixed>) {
        erase(value);
    }
    else {
        if (value.is_null()) {
            erase_null();
        }
        else {
            erase(value.get<typename util::RemoveOptional<T>::type>());
        }
    }
}

template <class T>
inline void Set<T>::insert_null()
{
    insert(BPlusTree<T>::default_value(this->m_nullable));
}

template <class T>
inline void Set<T>::erase_null()
{
    erase(BPlusTree<T>::default_value(this->m_nullable));
}

template <class T>
inline void Set<T>::clear()
{
    ensure_created();
    update_if_needed();
    this->ensure_writeable();
    if (size() > 0) {
        if (Replication* repl = this->m_obj.get_replication()) {
            this->clear_repl(repl);
        }
        m_tree->clear();
        m_obj.bump_content_version();
    }
}

template <class T>
inline Mixed Set<T>::min(size_t* return_ndx) const
{
    if (size() != 0) {
        if (return_ndx) {
            *return_ndx = 0;
        }
        return *begin();
    }
    else {
        if (return_ndx) {
            *return_ndx = not_found;
        }
        return Mixed{};
    }
}

template <class T>
inline Mixed Set<T>::max(size_t* return_ndx) const
{
    auto sz = size();
    if (sz != 0) {
        if (return_ndx) {
            *return_ndx = sz - 1;
        }
        auto e = end();
        --e;
        return *e;
    }
    else {
        if (return_ndx) {
            *return_ndx = not_found;
        }
        return Mixed{};
    }
}

template <class T>
inline Mixed Set<T>::sum(size_t* return_cnt) const
{
    return SumHelper<T>::eval(*m_tree, return_cnt);
}

template <class T>
inline Mixed Set<T>::avg(size_t* return_cnt) const
{
    return AverageHelper<T>::eval(*m_tree, return_cnt);
}

template <class T>
inline void Set<T>::sort(std::vector<size_t>& indices, bool ascending) const
{
    auto sz = size();
    indices.resize(sz);
    if (ascending) {
        std::iota(indices.begin(), indices.end(), 0);
    }
    else {
        std::iota(indices.rbegin(), indices.rend(), 0);
    }
}

template <class T>
inline void Set<T>::distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order) const
{
    auto ascending = !sort_order || *sort_order;
    sort(indices, ascending);
}

} // namespace realm

#endif // REALM_SET_HPP
