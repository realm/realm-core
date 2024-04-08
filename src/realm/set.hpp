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
#include <realm/bplustree.hpp>
#include <realm/array_key.hpp>

namespace realm {
class SetBase : public CollectionBase {
public:
    using CollectionBase::CollectionBase;
    SetBase(const SetBase& other);
    SetBase(SetBase&& other) noexcept;
    SetBase& operator=(const SetBase& other);
    SetBase& operator=(SetBase&& other) noexcept;

    virtual SetBasePtr clone() const = 0;
    virtual std::pair<size_t, bool> insert_null() = 0;
    virtual std::pair<size_t, bool> erase_null() = 0;
    virtual std::pair<size_t, bool> insert_any(Mixed value) = 0;
    virtual std::pair<size_t, bool> erase_any(Mixed value) = 0;

    bool is_subset_of(const CollectionBase&) const;
    bool is_strict_subset_of(const CollectionBase& rhs) const;
    bool is_superset_of(const CollectionBase& rhs) const;
    bool is_strict_superset_of(const CollectionBase& rhs) const;
    bool intersects(const CollectionBase& rhs) const;
    bool set_equals(const CollectionBase& rhs) const;
    void assign_union(const CollectionBase&);
    void assign_intersection(const CollectionBase&);
    void assign_difference(const CollectionBase&);
    void assign_symmetric_difference(const CollectionBase&);

protected:
    static constexpr CollectionType s_collection_type = CollectionType::Set;
    mutable std::unique_ptr<BPlusTreeBase> m_tree;

    void insert_repl(Replication* repl, size_t index, Mixed value) const;
    void erase_repl(Replication* repl, size_t index, Mixed value) const;
    void clear_repl(Replication* repl) const;
    static std::vector<Mixed> convert_to_mixed_set(const CollectionBase& rhs);

    void resort_range(size_t from, size_t to);

    REALM_COLD REALM_NORETURN void throw_invalid_null()
    {
        throw InvalidArgument(ErrorCodes::PropertyNotNullable,
                              util::format("Set: %1", CollectionBase::get_property_name()));
    }

private:
    template <class It1, class It2>
    bool is_subset_of(It1, It2) const;

    template <class It1, class It2>
    bool is_superset_of(It1, It2) const;

    template <class It1, class It2>
    bool intersects(It1, It2) const;

    template <class It1, class It2>
    void assign_union(It1, It2);

    template <class It1, class It2>
    void assign_intersection(It1, It2);

    template <class It1, class It2>
    void assign_difference(It1, It2);

    template <class It1, class It2>
    void assign_symmetric_difference(It1, It2);
};

template <class T>
class Set final : public CollectionBaseImpl<SetBase> {
public:
    using Base = CollectionBaseImpl<SetBase>;
    using value_type = T;
    using iterator = CollectionIterator<Set<T>>;

    Set() = default;
    Set(const Obj& owner, ColKey col_key)
        : Set<T>(col_key)
    {
        this->set_owner(owner, col_key);
    }

    Set(ColKey col_key, size_t = 0)
        : Base(col_key)
    {
        if (!col_key.is_set()) {
            throw InvalidArgument(ErrorCodes::TypeMismatch, "Property not a set");
        }

        check_column_type<value_type>(m_col_key);
    }
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
        CollectionBase::validate_index("get()", ndx, current_size);
        return tree().get(ndx);
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
    util::Optional<Mixed> min(size_t* return_ndx = nullptr) const final;
    util::Optional<Mixed> max(size_t* return_ndx = nullptr) const final;
    util::Optional<Mixed> sum(size_t* return_cnt = nullptr) const final;
    util::Optional<Mixed> avg(size_t* return_cnt = nullptr) const final;
    CollectionBasePtr clone_collection() const final
    {
        return std::make_unique<Set<T>>(*this);
    }
    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;

    // Overriding members of SetBase:
    size_t find_any(Mixed) const final;
    std::pair<size_t, bool> insert_null() final;
    std::pair<size_t, bool> erase_null() final;
    std::pair<size_t, bool> insert_any(Mixed value) final;
    std::pair<size_t, bool> erase_any(Mixed value) final;

    const BPlusTree<T>& get_tree() const
    {
        return tree();
    }

    UpdateStatus update_if_needed() const final;
    void ensure_created();

    void migrate();
    void migration_resort();

private:
    // Friend because it needs access to `m_tree` in the implementation of
    // `ObjCollectionBase::get_mutable_tree()`.
    friend class LnkSet;

    using Base::bump_content_version;
    using Base::get_alloc;
    using Base::m_col_key;
    using Base::m_nullable;

    BPlusTree<T>& tree() const
    {
        return static_cast<BPlusTree<T>&>(*m_tree);
    }

    UpdateStatus init_from_parent(bool allow_create) const;

    /// Update the accessor and return true if it is attached after the update.
    inline bool update() const
    {
        return update_if_needed() != UpdateStatus::Detached;
    }

    // `do_` methods here perform the action after preconditions have been
    // checked (bounds check, writability, etc.).
    void do_insert(size_t ndx, T value);
    void do_erase(size_t ndx);
    void do_clear();

    iterator find_impl(const T& value) const;
};

class LnkSet final : public ObjCollectionBase<SetBase> {
public:
    using Base = ObjCollectionBase<SetBase>;
    using value_type = ObjKey;
    using iterator = CollectionIterator<LnkSet>;

    LnkSet() = default;
    LnkSet(const Obj& owner, ColKey col_key)
        : m_set(owner, col_key)
    {
    }
    LnkSet(ColKey col_key)
        : m_set(col_key)
    {
    }


    LnkSet(const LnkSet&) = default;
    LnkSet(LnkSet&&) = default;
    LnkSet& operator=(const LnkSet&) = default;
    LnkSet& operator=(LnkSet&&) = default;
    bool operator==(const LnkSet& other) const;
    bool operator!=(const LnkSet& other) const;

    ObjKey get(size_t ndx) const;
    size_t find(ObjKey) const;
    size_t find_first(ObjKey) const;
    std::pair<size_t, bool> insert(ObjKey);
    std::pair<size_t, bool> erase(ObjKey);

    // Overriding members of CollectionBase:
    using CollectionBase::get_owner_key;
    CollectionBasePtr clone_collection() const override
    {
        return clone_linkset();
    }
    size_t size() const final;
    bool is_null(size_t ndx) const final;
    Mixed get_any(size_t ndx) const final;
    void clear() final;
    util::Optional<Mixed> min(size_t* return_ndx = nullptr) const final;
    util::Optional<Mixed> max(size_t* return_ndx = nullptr) const final;
    util::Optional<Mixed> sum(size_t* return_cnt = nullptr) const final;
    util::Optional<Mixed> avg(size_t* return_cnt = nullptr) const final;
    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;
    const Obj& get_obj() const noexcept final;
    bool is_attached() const noexcept final;
    bool has_changed() const noexcept final;
    ColKey get_col_key() const noexcept final;
    CollectionType get_collection_type() const noexcept override
    {
        return CollectionType::Set;
    }

    FullPath get_path() const noexcept final
    {
        return m_set.get_path();
    }

    Path get_short_path() const noexcept final
    {
        return m_set.get_short_path();
    }

    StablePath get_stable_path() const noexcept final
    {
        return m_set.get_stable_path();
    }

    // Overriding members of SetBase:
    SetBasePtr clone() const override
    {
        return clone_linkset();
    }
    // Overriding members of ObjList:
    LinkCollectionPtr clone_obj_list() const final
    {
        return clone_linkset();
    }
    size_t find_any(Mixed) const final;
    std::pair<size_t, bool> insert_null() final;
    std::pair<size_t, bool> erase_null() final;
    std::pair<size_t, bool> insert_any(Mixed value) final;
    std::pair<size_t, bool> erase_any(Mixed value) final;

    // Overriding members of ObjList:
    Obj get_object(size_t ndx) const final;
    ObjKey get_key(size_t ndx) const final;

    // LnkSet interface:

    std::unique_ptr<LnkSet> clone_linkset() const
    {
        // FIXME: The copy constructor requires this.
        update_if_needed();
        return std::make_unique<LnkSet>(*this);
    }

    template <class Func>
    void find_all(ObjKey value, Func&& func) const
    {
        if (value.is_unresolved()) {
            return;
        }

        m_set.find_all(value, [&](size_t ndx) {
            func(real2virtual(ndx));
        });
    }

    TableView get_sorted_view(SortDescriptor order) const;
    TableView get_sorted_view(ColKey column_key, bool ascending = true);
    void remove_target_row(size_t link_ndx);
    void remove_all_target_rows();

    iterator begin() const noexcept
    {
        return iterator{this, 0};
    }

    iterator end() const noexcept
    {
        return iterator{this, size()};
    }

    void set_owner(const Obj& obj, ColKey ck) override
    {
        m_set.set_owner(obj, ck);
    }

    void set_owner(std::shared_ptr<CollectionParent> parent, CollectionParent::Index index) override
    {
        m_set.set_owner(std::move(parent), index);
    }

    void to_json(std::ostream&, JSONOutputMode, util::FunctionRef<void(const Mixed&)>) const override;

private:
    Set<ObjKey> m_set;

    // Overriding members of ObjCollectionBase:
    UpdateStatus do_update_if_needed() const final
    {
        return m_set.update_if_needed();
    }

    BPlusTree<ObjKey>* get_mutable_tree() const final
    {
        return &m_set.tree();
    }
};

template <>
void Set<ObjKey>::do_insert(size_t, ObjKey);
template <>
void Set<ObjKey>::do_erase(size_t);
template <>
void Set<ObjKey>::do_clear();

template <>
void Set<ObjLink>::do_insert(size_t, ObjLink);
template <>
void Set<ObjLink>::do_erase(size_t);

template <>
void Set<Mixed>::do_insert(size_t, Mixed);
template <>
void Set<Mixed>::do_erase(size_t);
template <>
void Set<Mixed>::do_clear();
template <>
void Set<Mixed>::migrate();
template <>
void Set<Mixed>::migration_resort();
template <>
void Set<StringData>::migration_resort();
template <>
void Set<BinaryData>::migration_resort();

inline SetBase::SetBase(const SetBase& other)
    : CollectionBase(other)
{
    // Note: does not copy m_tree and instead that's initialized on demand
}

inline SetBase::SetBase(SetBase&& other) noexcept
    : CollectionBase(std::move(other))
    , m_tree(std::exchange(other.m_tree, nullptr))
{
}

inline SetBase& SetBase::operator=(const SetBase& other)
{
    if (this != &other) {
        // Just reset the pointer and rely on init_from_parent() being called
        // when the accessor is actually used.
        m_tree.reset();
    }

    return *this;
}

inline SetBase& SetBase::operator=(SetBase&& other) noexcept
{
    if (this != &other) {
        m_tree = std::exchange(other.m_tree, nullptr);
    }

    return *this;
}

template <class T>
inline Set<T>::Set(const Set& other)
    : Base(static_cast<const Base&>(other))
{
    // Reset the content version so we can rely on init_from_parent() being
    // called lazily when the accessor is used.
    Base::reset_content_version();
}

template <class T>
inline Set<T>::Set(Set&& other) noexcept
    : Base(static_cast<Base&&>(other))
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
        // Just reset the pointer and rely on init_from_parent() being called
        // when the accessor is actually used.
        Base::reset_content_version();
    }

    return *this;
}

template <class T>
inline Set<T>& Set<T>::operator=(Set&& other) noexcept
{
    Base::operator=(static_cast<Base&&>(other));

    if (this != &other) {
        if (m_tree) {
            m_tree->set_parent(this, 0);
            // Note: We do not need to call reset_content_version(), because we
            // took both `m_tree` and `m_content_version` from `other`.
        }
    }

    return *this;
}

template <typename T>
UpdateStatus Set<T>::update_if_needed() const
{
    switch (get_update_status()) {
        case UpdateStatus::Detached: {
            m_tree.reset();
            return UpdateStatus::Detached;
        }
        case UpdateStatus::NoChange:
            if (m_tree && tree().is_attached()) {
                return UpdateStatus::NoChange;
            }
            // The tree has not been initialized yet for this accessor, so
            // perform lazy initialization by treating it as an update.
            [[fallthrough]];
        case UpdateStatus::Updated:
            return init_from_parent(false);
    }
    REALM_UNREACHABLE();
}

template <typename T>
void Set<T>::ensure_created()
{
    if (Base::should_update() || !(m_tree && tree().is_attached())) {
        // When allow_create is true, init_from_parent will always succeed
        // In case of errors, an exception is thrown.
        constexpr bool allow_create = true;
        init_from_parent(allow_create); // Throws
    }
}

template <typename T>
UpdateStatus Set<T>::init_from_parent(bool allow_create) const
{
    Base::update_content_version();
    if (!m_tree) {
        m_tree.reset(new BPlusTree<T>(get_alloc()));
        const ArrayParent* parent = this;
        m_tree->set_parent(const_cast<ArrayParent*>(parent), 0);
    }
    return do_init_from_parent(m_tree.get(), Base::get_collection_ref(), allow_create);
}

template <typename U>
Set<U> Obj::get_set(ColKey col_key) const
{
    return Set<U>(*this, col_key);
}

template <typename U>
inline SetPtr<U> Obj::get_set_ptr(ColKey col_key) const
{
    return std::make_unique<Set<U>>(*this, col_key);
}

inline LnkSet Obj::get_linkset(ColKey col_key) const
{
    return LnkSet{*this, col_key};
}

inline LnkSet Obj::get_linkset(StringData col_name) const
{
    return get_linkset(get_column_key(col_name));
}

inline LnkSetPtr Obj::get_linkset_ptr(ColKey col_key) const
{
    return std::make_unique<LnkSet>(*this, col_key);
}

template <class T>
size_t Set<T>::find(T value) const
{
    auto it = find_impl(value);
    if (it != end() && *it == value) {
        return it.index();
    }
    return npos;
}

template <class T>
size_t Set<T>::find_any(Mixed value) const
{
    if constexpr (std::is_same_v<T, Mixed>) {
        return find(value);
    }
    else {
        if (value.is_null()) {
            if (!m_nullable) {
                return not_found;
            }
            return find(BPlusTree<T>::default_value(true));
        }
        else {
            return find(value.get<typename util::RemoveOptional<T>::type>());
        }
    }
}

template <class T>
REALM_NOINLINE auto Set<T>::find_impl(const T& value) const -> iterator
{
    auto b = this->begin();
    auto e = this->end(); // Note: This ends up calling `update_if_needed()`.
    return std::lower_bound(b, e, value);
}

template <class T>
std::pair<size_t, bool> Set<T>::insert(T value)
{
    ensure_created();

    if (!m_nullable && value_is_null(value))
        throw_invalid_null();

    auto it = find_impl(value);
    if (it != this->end() && *it == value) {
        return {it.index(), false};
    }

    if (Replication* repl = Base::get_replication()) {
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
    auto it = find_impl(value); // Note: This ends up calling `update_if_needed()`.

    if (it == end() || *it != value) {
        return {npos, false};
    }

    if (Replication* repl = Base::get_replication()) {
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
REALM_NOINLINE size_t Set<T>::size() const
{
    return update() ? m_tree->size() : 0;
}

template <class T>
inline bool Set<T>::is_null(size_t ndx) const
{
    return m_nullable && value_is_null(get(ndx));
}

template <class T>
inline void Set<T>::clear()
{
    if (size() > 0) {
        if (Replication* repl = Base::get_replication()) {
            this->clear_repl(repl);
        }
        do_clear();
        bump_content_version();
    }
}

template <class T>
inline util::Optional<Mixed> Set<T>::min(size_t* return_ndx) const
{
    if (update()) {
        return MinHelper<T>::eval(tree(), return_ndx);
    }
    return MinHelper<T>::not_found(return_ndx);
}

template <class T>
inline util::Optional<Mixed> Set<T>::max(size_t* return_ndx) const
{
    if (update()) {
        return MaxHelper<T>::eval(tree(), return_ndx);
    }
    return MaxHelper<T>::not_found(return_ndx);
}

template <class T>
inline util::Optional<Mixed> Set<T>::sum(size_t* return_cnt) const
{
    if (update()) {
        return SumHelper<T>::eval(tree(), return_cnt);
    }
    return SumHelper<T>::not_found(return_cnt);
}

template <class T>
inline util::Optional<Mixed> Set<T>::avg(size_t* return_cnt) const
{
    if (update()) {
        return AverageHelper<T>::eval(tree(), return_cnt);
    }
    return AverageHelper<T>::not_found(return_cnt);
}

REALM_NOINLINE void set_sorted_indices(size_t sz, std::vector<size_t>& indices, bool ascending);

template <class T>
inline void Set<T>::sort(std::vector<size_t>& indices, bool ascending) const
{
    auto sz = size();
    set_sorted_indices(sz, indices, ascending);
}

template <class T>
void Set<T>::distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order) const
{
    auto ascending = !sort_order || *sort_order;
    set_sorted_indices(size(), indices, ascending);
}

template <class T>
inline void Set<T>::do_insert(size_t ndx, T value)
{
    tree().insert(ndx, value);
}

template <class T>
inline void Set<T>::do_erase(size_t ndx)
{
    tree().erase(ndx);
}

template <class T>
inline void Set<T>::do_clear()
{
    tree().clear();
}

template <class T>
inline void Set<T>::migration_resort()
{
}

inline bool LnkSet::operator==(const LnkSet& other) const
{
    return m_set == other.m_set;
}

inline bool LnkSet::operator!=(const LnkSet& other) const
{
    return m_set != other.m_set;
}

inline ObjKey LnkSet::get(size_t ndx) const
{
    const auto current_size = size();
    if (ndx >= current_size) {
        throw OutOfBounds(util::format("Invalid index into set: %1", CollectionBase::get_property_name()), ndx,
                          current_size);
    }
    return m_set.tree().get(virtual2real(ndx));
}

inline size_t LnkSet::find(ObjKey value) const
{
    if (value.is_unresolved()) {
        return not_found;
    }

    update_if_needed();

    size_t ndx = m_set.find(value);
    if (ndx == not_found) {
        return not_found;
    }
    return real2virtual(ndx);
}

inline size_t LnkSet::find_first(ObjKey value) const
{
    return find(value);
}

inline size_t LnkSet::size() const
{
    update_if_needed();
    return m_set.size() - num_unresolved();
}

inline std::pair<size_t, bool> LnkSet::insert(ObjKey value)
{
    REALM_ASSERT(!value.is_unresolved());
    update_if_needed();

    auto [ndx, inserted] = m_set.insert(value);
    if (inserted) {
        update_unresolved(UpdateStatus::Updated);
    }
    return {real2virtual(ndx), inserted};
}

inline std::pair<size_t, bool> LnkSet::erase(ObjKey value)
{
    REALM_ASSERT(!value.is_unresolved());
    update_if_needed();

    auto [ndx, removed] = m_set.erase(value);
    if (removed) {
        update_unresolved(UpdateStatus::Updated);
        ndx = real2virtual(ndx);
    }
    return {ndx, removed};
}

inline bool LnkSet::is_null(size_t ndx) const
{
    update_if_needed();
    return m_set.is_null(virtual2real(ndx));
}

inline Mixed LnkSet::get_any(size_t ndx) const
{
    update_if_needed();
    auto obj_key = m_set.get(virtual2real(ndx));
    return ObjLink{get_target_table()->get_key(), obj_key};
}

inline std::pair<size_t, bool> LnkSet::insert_null()
{
    update_if_needed();
    auto [ndx, inserted] = m_set.insert_null();
    if (inserted) {
        update_unresolved(UpdateStatus::Updated);
    }
    return {real2virtual(ndx), inserted};
}

inline std::pair<size_t, bool> LnkSet::erase_null()
{
    update_if_needed();
    auto [ndx, erased] = m_set.erase_null();
    if (erased) {
        update_unresolved(UpdateStatus::Updated);
        ndx = real2virtual(ndx);
    }
    return {ndx, erased};
}

inline std::pair<size_t, bool> LnkSet::insert_any(Mixed value)
{
    update_if_needed();
    auto [ndx, inserted] = m_set.insert_any(value);
    if (inserted) {
        update_unresolved(UpdateStatus::Updated);
    }
    return {real2virtual(ndx), inserted};
}

inline std::pair<size_t, bool> LnkSet::erase_any(Mixed value)
{
    auto [ndx, erased] = m_set.erase_any(value);
    if (erased) {
        update_unresolved(UpdateStatus::Updated);
        ndx = real2virtual(ndx);
    }
    return {ndx, erased};
}

inline void LnkSet::clear()
{
    // Note: Explicit call to `ensure_writable()` not needed, because we
    // explicitly call `clear_unresolved()`.
    m_set.clear();
    clear_unresolved();
}

inline util::Optional<Mixed> LnkSet::min(size_t* return_ndx) const
{
    update_if_needed();
    size_t found = not_found;
    auto value = m_set.min(&found);
    if (found != not_found && return_ndx) {
        *return_ndx = real2virtual(found);
    }
    return value;
}

inline util::Optional<Mixed> LnkSet::max(size_t* return_ndx) const
{
    update_if_needed();
    size_t found = not_found;
    auto value = m_set.max(&found);
    if (found != not_found && return_ndx) {
        *return_ndx = real2virtual(found);
    }
    return value;
}

inline util::Optional<Mixed> LnkSet::sum(size_t* return_cnt) const
{
    static_cast<void>(return_cnt);
    REALM_TERMINATE("Not implemented");
}

inline util::Optional<Mixed> LnkSet::avg(size_t* return_cnt) const
{
    static_cast<void>(return_cnt);
    REALM_TERMINATE("Not implemented");
}

inline void LnkSet::sort(std::vector<size_t>& indices, bool ascending) const
{
    update_if_needed();

    // Map the input indices to real indices.
    std::transform(indices.begin(), indices.end(), indices.begin(), [this](size_t ndx) {
        return virtual2real(ndx);
    });

    m_set.sort(indices, ascending);

    if (has_unresolved()) {
        indices.erase(std::remove_if(indices.begin(), indices.end(),
                                     [&](size_t ndx) {
                                         return real_is_unresolved(ndx);
                                     }),
                      indices.end());
    }

    // Map the output indices to virtual indices.
    std::transform(indices.begin(), indices.end(), indices.begin(), [this](size_t ndx) {
        return real2virtual(ndx);
    });
}

inline void LnkSet::distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order) const
{
    update_if_needed();

    // Map the input indices to real indices.
    std::transform(indices.begin(), indices.end(), indices.begin(), [this](size_t ndx) {
        return virtual2real(ndx);
    });

    m_set.distinct(indices, sort_order);

    if (has_unresolved()) {
        indices.erase(std::remove_if(indices.begin(), indices.end(),
                                     [&](size_t ndx) {
                                         return real_is_unresolved(ndx);
                                     }),
                      indices.end());
    }

    // Map the output indices to virtual indices.
    std::transform(indices.begin(), indices.end(), indices.begin(), [this](size_t ndx) {
        return real2virtual(ndx);
    });
}

inline const Obj& LnkSet::get_obj() const noexcept
{
    return m_set.get_obj();
}

inline bool LnkSet::is_attached() const noexcept
{
    return m_set.is_attached();
}

inline bool LnkSet::has_changed() const noexcept
{
    return m_set.has_changed();
}

inline ColKey LnkSet::get_col_key() const noexcept
{
    return m_set.get_col_key();
}

inline size_t LnkSet::find_any(Mixed value) const
{
    if (value.is_null())
        return not_found;

    const auto type = value.get_type();
    if (type == type_Link) {
        return find(value.get<ObjKey>());
    }
    if (type == type_TypedLink) {
        auto link = value.get_link();
        if (link.get_table_key() == get_target_table()->get_key()) {
            return find(link.get_obj_key());
        }
    }
    return not_found;
}

inline Obj LnkSet::get_object(size_t ndx) const
{
    ObjKey key = get(ndx);
    return get_target_table()->get_object(key);
}

inline ObjKey LnkSet::get_key(size_t ndx) const
{
    return get(ndx);
}

} // namespace realm

#endif // REALM_SET_HPP
