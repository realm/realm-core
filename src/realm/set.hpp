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
    virtual SetBasePtr clone() const = 0;
    virtual std::pair<size_t, bool> insert_null() = 0;
    virtual std::pair<size_t, bool> erase_null() = 0;
    virtual std::pair<size_t, bool> insert_any(Mixed value) = 0;
    virtual std::pair<size_t, bool> erase_any(Mixed value) = 0;

protected:
    void insert_repl(Replication* repl, size_t index, Mixed value) const;
    void erase_repl(Replication* repl, size_t index, Mixed value) const;
    void clear_repl(Replication* repl) const;
};

template <class T>
class Set final : public CollectionBaseImpl<SetBase> {
public:
    using Base = CollectionBaseImpl<SetBase>;
    using value_type = T;
    using iterator = CollectionIterator<Set<T>>;

    Set() = default;
    Set(const Obj& owner, ColKey col_key);
    Set(const Set& other);
    Set(Set&& other) noexcept;
    Set& operator=(const Set& other);
    Set& operator=(Set&& other) noexcept;

    SetBasePtr clone() const final
    {
        return std::make_unique<Set<T>>(*this);
    }

    T get(size_t ndx) const
    {
        const auto current_size = size();
        if (ndx >= current_size) {
            throw std::out_of_range("Index out of range");
        }
        return m_tree->get(ndx);
    }

    iterator begin() const noexcept
    {
        return iterator{this, 0};
    }

    iterator end() const noexcept
    {
        return iterator{this, size()};
    }

    size_t find_first(const T& value) const
    {
        return find(value);
    }

    template <class Func>
    void find_all(T value, Func&& func) const
    {
        size_t found = find(value);
        if (found != not_found) {
            func(found);
        }
    }

    /// Insert a value into the set if it does not already exist, returning the index of the inserted value,
    /// or the index of the already-existing value.
    std::pair<size_t, bool> insert(T value);

    /// Find the index of a value in the set, or `size_t(-1)` if it is not in the set.
    size_t find(T value) const;

    /// Erase an element from the set, returning true if the set contained the element.
    std::pair<size_t, bool> erase(T value);

    // Overriding members of CollectionBase:
    size_t size() const final;
    bool is_null(size_t ndx) const final;
    Mixed get_any(size_t ndx) const final
    {
        return get(ndx);
    }
    void clear() final;
    Mixed min(size_t* return_ndx = nullptr) const final;
    Mixed max(size_t* return_ndx = nullptr) const final;
    Mixed sum(size_t* return_cnt = nullptr) const final;
    Mixed avg(size_t* return_cnt = nullptr) const final;
    std::unique_ptr<CollectionBase> clone_collection() const final
    {
        return std::make_unique<Set<T>>(*this);
    }
    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;

    // Overriding members of SetBase:
    std::pair<size_t, bool> insert_null() final;
    std::pair<size_t, bool> erase_null() final;
    std::pair<size_t, bool> insert_any(Mixed value) final;
    std::pair<size_t, bool> erase_any(Mixed value) final;

    const BPlusTree<T>& get_tree() const
    {
        return *m_tree;
    }

private:
    mutable std::unique_ptr<BPlusTree<value_type>> m_tree;
    using Base::m_col_key;
    using Base::m_obj;
    using Base::m_valid;

    void create()
    {
        m_tree->create();
        m_valid = true;
    }

    bool init_from_parent() const final
    {
        m_valid = m_tree->init_from_parent();
        update_content_version();
        return m_valid;
    }

    void ensure_created()
    {
        if (!m_valid && m_obj.is_valid()) {
            create();
        }
    }
    void do_insert(size_t ndx, T value);
    void do_erase(size_t ndx);
};

template <>
void Set<ObjKey>::do_insert(size_t, ObjKey);
template <>
void Set<ObjKey>::do_erase(size_t);

template <>
void Set<ObjLink>::do_insert(size_t, ObjLink);
template <>
void Set<ObjLink>::do_erase(size_t);

template <>
void Set<Mixed>::do_insert(size_t, Mixed);
template <>
void Set<Mixed>::do_erase(size_t);

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
            case type_UUID:
                return a.get<UUID>() < b.get<UUID>();
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
            case type_UUID:
                return a.get<UUID>() == b.get<UUID>();
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
    : Base(obj, col_key)
    , m_tree(new BPlusTree<value_type>(obj.get_alloc()))
{
    if (!col_key.is_set()) {
        throw LogicError(LogicError::collection_type_mismatch);
    }

    check_column_type<value_type>(m_col_key);

    m_tree->set_parent(this, 0); // ndx not used, implicit in m_owner
    if (m_obj) {
        // Fine because init_from_parent() is final.
        this->init_from_parent();
    }
}

template <class T>
inline Set<T>::Set(const Set& other)
    : Base(static_cast<const Base&>(other))
{
    if (other.m_tree) {
        Allocator& alloc = other.m_tree->get_alloc();
        m_tree = std::make_unique<BPlusTree<T>>(alloc);
        m_tree->set_parent(this, 0);
        if (m_valid)
            m_tree->init_from_ref(other.m_tree->get_ref());
    }
}

template <class T>
inline Set<T>::Set(Set&& other) noexcept
    : Base(static_cast<Base&&>(other))
    , m_tree(std::exchange(other.m_tree, nullptr))
{
    if (m_tree) {
        m_tree->set_parent(this, 0);
    }
}

template <class T>
inline Set<T>& Set<T>::operator=(const Set& other)
{
    Base::operator=(static_cast<const Base&>(other));

    if (this != &other) {
        m_tree.reset();
        if (other.m_tree) {
            Allocator& alloc = other.m_tree->get_alloc();
            m_tree = std::make_unique<BPlusTree<T>>(alloc);
            m_tree->set_parent(this, 0);
            if (m_valid) {
                m_tree->init_from_ref(other.m_tree->get_ref());
            }
        }
    }

    return *this;
}

template <class T>
inline Set<T>& Set<T>::operator=(Set&& other) noexcept
{
    Base::operator=(static_cast<Base&&>(other));

    if (this != &other) {
        m_tree = std::exchange(other.m_tree, nullptr);
        if (m_tree) {
            m_tree->set_parent(this, 0);
        }
    }

    return *this;
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
std::pair<size_t, bool> Set<T>::insert(T value)
{
    update_if_needed();

    ensure_created();
    this->ensure_writeable();
    auto b = this->begin();
    auto e = this->end();
    auto it = std::lower_bound(b, e, value, SetElementLessThan<T>{});

    if (it != e && SetElementEquals<T>{}(*it, value)) {
        return {it.index(), false};
    }

    if (Replication* repl = m_obj.get_replication()) {
        // FIXME: We should emit an instruction regardless of element presence for the purposes of conflict
        // resolution in synchronized databases. The reason is that the new insertion may come at a later time
        // than an interleaving erase instruction, so emitting the instruction ensures that last "write" wins.
        this->insert_repl(repl, it.index(), value);
    }

    do_insert(it.index(), value);
    bump_content_version();
    return {it.index(), true};
}

template <class T>
std::pair<size_t, bool> Set<T>::insert_any(Mixed value)
{
    if constexpr (std::is_same_v<T, Mixed>) {
        return insert(value);
    }
    else {
        if (value.is_null()) {
            return insert_null();
        }
        else {
            return insert(value.get<typename util::RemoveOptional<T>::type>());
        }
    }
}

template <class T>
std::pair<size_t, bool> Set<T>::erase(T value)
{
    update_if_needed();
    this->ensure_writeable();

    auto b = this->begin();
    auto e = this->end();
    auto it = std::lower_bound(b, e, value, SetElementLessThan<T>{});

    if (it == e || !SetElementEquals<T>{}(*it, value)) {
        return {npos, false};
    }

    if (Replication* repl = m_obj.get_replication()) {
        this->erase_repl(repl, it.index(), value);
    }
    do_erase(it.index());
    bump_content_version();
    return {it.index(), true};
}

template <class T>
std::pair<size_t, bool> Set<T>::erase_any(Mixed value)
{
    if constexpr (std::is_same_v<T, Mixed>) {
        return erase(value);
    }
    else {
        if (value.is_null()) {
            return erase_null();
        }
        else {
            return erase(value.get<typename util::RemoveOptional<T>::type>());
        }
    }
}

template <class T>
inline std::pair<size_t, bool> Set<T>::insert_null()
{
    return insert(BPlusTree<T>::default_value(this->m_nullable));
}

template <class T>
std::pair<size_t, bool> Set<T>::erase_null()
{
    return erase(BPlusTree<T>::default_value(this->m_nullable));
}

template <class T>
size_t Set<T>::size() const
{
    if (!is_attached())
        return 0;
    update_if_needed();
    if (!m_valid) {
        return 0;
    }
    return m_tree->size();
}

template <class T>
inline bool Set<T>::is_null(size_t ndx) const
{
    return m_nullable && value_is_null(get(ndx));
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
        bump_content_version();

        // For Set<ObjKey>, we are sure that there are no longer any unresolved
        // links.
        m_tree->set_context_flag(false);
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

template <class T>
void Set<T>::do_insert(size_t ndx, T value)
{
    m_tree->insert(ndx, value);
}

template <class T>
void Set<T>::do_erase(size_t ndx)
{
    m_tree->erase(ndx);
}

} // namespace realm

#endif // REALM_SET_HPP
