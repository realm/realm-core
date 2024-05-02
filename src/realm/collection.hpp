#ifndef REALM_COLLECTION_HPP
#define REALM_COLLECTION_HPP

#include <realm/obj.hpp>
#include <realm/bplustree.hpp>
#include <realm/obj_list.hpp>
#include <realm/table.hpp>

#include <iosfwd>      // std::ostream
#include <type_traits> // std::void_t
#include <set>

namespace realm {

template <class L>
struct CollectionIterator;

// Used in Cluster when removing owning object
class DummyParent : public CollectionParent {
public:
    DummyParent(TableRef t, ref_type ref)
        : m_obj(t, MemRef(), ObjKey(), 0)
        , m_ref(ref)
    {
    }
    FullPath get_path() const noexcept final
    {
        return {};
    }
    Path get_short_path() const noexcept final
    {
        return {};
    }
    StablePath get_stable_path() const noexcept final
    {
        return {};
    }
    ColKey get_col_key() const noexcept final
    {
        return {};
    }
    void add_index(Path&, const Index&) const noexcept final {}
    size_t find_index(const Index&) const noexcept final
    {
        return realm::npos;
    }

    TableRef get_table() const noexcept final
    {
        return m_obj.get_table();
    }
    const Obj& get_object() const noexcept final
    {
        return m_obj;
    }
    uint32_t parent_version() const noexcept final
    {
        return 0;
    }
    void update_content_version() const noexcept final {}

protected:
    Obj m_obj;
    ref_type m_ref;
    UpdateStatus update_if_needed() const final
    {
        return UpdateStatus::Updated;
    }
    ref_type get_collection_ref(Index, CollectionType) const final
    {
        return m_ref;
    }
    void set_collection_ref(Index, ref_type, CollectionType) {}
};

class Collection {
public:
    virtual ~Collection();
    /// The size of the collection.
    virtual size_t size() const = 0;
    /// Get element at @a ndx as a `Mixed`.
    virtual Mixed get_any(size_t ndx) const = 0;
    /// True if `size()` returns 0.
    bool is_empty() const
    {
        return size() == 0;
    }
    virtual void to_json(std::ostream&, JSONOutputMode, util::FunctionRef<void(const Mixed&)>) const {}
    /// Get collection type (set, list, dictionary)
    virtual CollectionType get_collection_type() const noexcept = 0;

    virtual void insert_collection(const PathElement&, CollectionType)
    {
        throw IllegalOperation("insert_collection is not legal on this collection type");
    }
    virtual void set_collection(const PathElement&, CollectionType)
    {
        throw IllegalOperation("set_collection is not legal on this collection type");
    }
    // Returns the path to the collection. Uniquely identifies the collection within the Group.
    virtual FullPath get_path() const = 0;
    // Returns the path from the owning object. Starting with the column key. Identifies
    // the collection within the object
    virtual Path get_short_path() const = 0;
    // Return a path based on keys instead of indices
    virtual StablePath get_stable_path() const = 0;

    struct QueryCtrlBlock {
        Path path;
        std::vector<std::vector<Mixed>> matches;
        bool path_only_unary_keys = false; // Not from list
        Allocator* alloc = nullptr;
        Group* group = nullptr;
    };
    static void get_any(QueryCtrlBlock&, Mixed, size_t);
};

using CollectionPtr = std::shared_ptr<Collection>;

/// Base class for all collection accessors.
///
/// Collections are bound to particular properties of an object. In a
/// collection's public interface, the implementation must take care to keep the
/// object consistent with the persisted state, mindful of the fact that the
/// state may have changed as a consequence of modifications from other instances
/// referencing the same persisted state.
class CollectionBase : public Collection {
public:
    /// True if the element at @a ndx is NULL.
    virtual bool is_null(size_t ndx) const = 0;

    /// Clear the collection.
    virtual void clear() = 0;

    /// Get the min element, according to whatever comparison function is
    /// meaningful for the collection, or none if min is not supported for this type.
    virtual util::Optional<Mixed> min(size_t* return_ndx = nullptr) const = 0;

    /// Get the max element, according to whatever comparison function is
    /// meaningful for the collection, or none if max is not supported for this type.
    virtual util::Optional<Mixed> max(size_t* return_ndx = nullptr) const = 0;

    /// For collections of arithmetic types, return the sum of all elements.
    /// For non arithmetic types, returns none.
    virtual util::Optional<Mixed> sum(size_t* return_cnt = nullptr) const = 0;

    /// For collections of arithmetic types, return the average of all elements.
    /// For non arithmetic types, returns none.
    virtual util::Optional<Mixed> avg(size_t* return_cnt = nullptr) const = 0;

    /// Produce a clone of the collection accessor referring to the same
    /// underlying memory.
    virtual CollectionBasePtr clone_collection() const = 0;

    /// Modifies a vector of indices so that they refer to values sorted
    /// according to the specified sort order.
    virtual void sort(std::vector<size_t>& indices, bool ascending = true) const = 0;

    /// Modifies a vector of indices so that they refer to distinct values. If
    /// @a sort_order is supplied, the indices will refer to values in sort
    /// order, otherwise the indices will be in the same order as they appear in
    /// the collection.
    virtual void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const = 0;

    // Return index of the first occurrence of 'value'
    virtual size_t find_any(Mixed value) const = 0;

    /// Get the object that owns this collection.
    virtual const Obj& get_obj() const noexcept = 0;

    /// Get the column key for this collection.
    virtual ColKey get_col_key() const noexcept = 0;

    virtual PathElement get_path_element(size_t ndx) const
    {
        return PathElement(ndx);
    }

    // Clone this collection if it contains objects, and return nullptr otherwise
    virtual LinkCollectionPtr clone_as_obj_list() const
    {
        return nullptr;
    }

    /// Return true if the collection has changed since the last call to
    /// `has_changed()`. Note that this function is not idempotent and updates
    /// the internal state of the accessor if it has changed.
    virtual bool has_changed() const noexcept = 0;

    /// Returns true if the accessor is in the attached state. By default, this
    /// checks if the owning object is still valid.
    virtual bool is_attached() const noexcept
    {
        return get_obj().is_valid();
    }

    // Note: virtual..final prevents static override.

    /// Get the key of the object that owns this collection.
    virtual ObjKey get_owner_key() const noexcept final
    {
        return get_obj().get_key();
    }

    /// Get the table of the object that owns this collection.
    ConstTableRef get_table() const noexcept
    {
        return get_obj().get_table();
    }

    /// If this is a collection of links, get the target table.
    TableRef get_target_table() const
    {
        return get_obj().get_target_table(get_col_key());
    }

    virtual size_t translate_index(size_t ndx) const noexcept
    {
        return ndx;
    }

    virtual DictionaryPtr get_dictionary(const PathElement&) const
    {
        throw IllegalOperation("get_dictionary for this collection is not allowed");
    }
    virtual ListMixedPtr get_list(const PathElement&) const
    {
        throw IllegalOperation("get_list for this collection is not allowed");
    }

    virtual void set_owner(const Obj& obj, ColKey) = 0;
    virtual void set_owner(std::shared_ptr<CollectionParent> parent, CollectionParent::Index index) = 0;


    StringData get_property_name() const
    {
        return get_table()->get_column_name(get_col_key());
    }

    bool operator==(const CollectionBase& other) const noexcept
    {
        return get_table() == other.get_table() && get_owner_key() == other.get_owner_key() &&
               get_col_key() == other.get_col_key();
    }

    bool operator!=(const CollectionBase& other) const noexcept
    {
        return !(*this == other);
    }

    // These are shadowed by typed versions in subclasses
    using value_type = Mixed;
    CollectionIterator<CollectionBase> begin() const;
    CollectionIterator<CollectionBase> end() const;

protected:
    friend class Transaction;
    CollectionBase() noexcept = default;
    CollectionBase(const CollectionBase&) noexcept = default;
    CollectionBase(CollectionBase&&) noexcept = default;
    CollectionBase& operator=(const CollectionBase&) noexcept = default;
    CollectionBase& operator=(CollectionBase&&) noexcept = default;

    void validate_index(const char* msg, size_t index, size_t size) const
    {
        if (index >= size) {
            out_of_bounds(msg, index, size);
        }
    }
    void out_of_bounds(const char* msg, size_t index, size_t size) const;
    static UpdateStatus do_init_from_parent(BPlusTreeBase* tree, ref_type ref, bool allow_create);
};

inline std::string_view collection_type_name(CollectionType col_type, bool uppercase = false)
{
    switch (col_type) {
        case CollectionType::List:
            return uppercase ? "List" : "list";
        case CollectionType::Set:
            return uppercase ? "Set" : "set";
        case CollectionType::Dictionary:
            return uppercase ? "Dictionary" : "dictionary";
    }
    return "";
}

template <class T>
inline void check_column_type(ColKey col)
{
    if (col && col.get_type() != ColumnTypeTraits<T>::column_id) {
        throw InvalidColumnKey();
    }
}

template <>
inline void check_column_type<Int>(ColKey col)
{
    if (col && (col.get_type() != col_type_Int || col.get_attrs().test(col_attr_Nullable))) {
        throw InvalidColumnKey();
    }
}

template <>
inline void check_column_type<util::Optional<Int>>(ColKey col)
{
    if (col && (col.get_type() != col_type_Int || !col.get_attrs().test(col_attr_Nullable))) {
        throw InvalidColumnKey();
    }
}

template <>
inline void check_column_type<ObjKey>(ColKey col)
{
    if (col) {
        if (!((col.is_list() || col.is_set()) && col.get_type() == col_type_Link))
            throw InvalidArgument(ErrorCodes::TypeMismatch, "Property not a list or set");
    }
}

template <class T, class = void>
struct MinHelper {
    template <class U>
    static util::Optional<Mixed> eval(U&, size_t*) noexcept
    {
        return util::none;
    }
    static util::Optional<Mixed> not_found(size_t*) noexcept
    {
        return util::none;
    }
};

template <class T>
struct MinHelper<T, std::void_t<ColumnMinMaxType<T>>> {
    template <class U>
    static util::Optional<Mixed> eval(U& tree, size_t* return_ndx)
    {
        auto optional_min = bptree_minimum<T>(tree, return_ndx);
        if (optional_min) {
            return Mixed{*optional_min};
        }
        return Mixed{};
    }
    static util::Optional<Mixed> not_found(size_t* return_ndx) noexcept
    {
        if (return_ndx)
            *return_ndx = realm::not_found;
        return Mixed{};
    }
};

template <class T, class Enable = void>
struct MaxHelper {
    template <class U>
    static util::Optional<Mixed> eval(U&, size_t*) noexcept
    {
        return util::none;
    }
    static util::Optional<Mixed> not_found(size_t*) noexcept
    {
        return util::none;
    }
};

template <class T>
struct MaxHelper<T, std::void_t<ColumnMinMaxType<T>>> {
    template <class U>
    static util::Optional<Mixed> eval(U& tree, size_t* return_ndx)
    {
        auto optional_max = bptree_maximum<T>(tree, return_ndx);
        if (optional_max) {
            return Mixed{*optional_max};
        }
        return Mixed{};
    }
    static util::Optional<Mixed> not_found(size_t* return_ndx) noexcept
    {
        if (return_ndx)
            *return_ndx = realm::not_found;
        return Mixed{};
    }
};

template <class T, class Enable = void>
class SumHelper {
public:
    template <class U>
    static util::Optional<Mixed> eval(U&, size_t* return_cnt) noexcept
    {
        if (return_cnt)
            *return_cnt = 0;
        return util::none;
    }
    static util::Optional<Mixed> not_found(size_t*) noexcept
    {
        return util::none;
    }
};

template <class T>
class SumHelper<T, std::void_t<ColumnSumType<T>>> {
public:
    template <class U>
    static util::Optional<Mixed> eval(U& tree, size_t* return_cnt)
    {
        return Mixed{bptree_sum<T>(tree, return_cnt)};
    }
    static util::Optional<Mixed> not_found(size_t* return_cnt) noexcept
    {
        if (return_cnt)
            *return_cnt = 0;
        using ResultType = typename aggregate_operations::Sum<typename util::RemoveOptional<T>::type>::ResultType;
        return Mixed{ResultType{}};
    }
};

template <class T, class = void>
struct AverageHelper {
    template <class U>
    static util::Optional<Mixed> eval(U&, size_t* return_cnt) noexcept
    {
        if (return_cnt)
            *return_cnt = 0;
        return util::none;
    }
    static util::Optional<Mixed> not_found(size_t*) noexcept
    {
        return util::none;
    }
};

template <class T>
struct AverageHelper<T, std::void_t<ColumnSumType<T>>> {
    template <class U>
    static util::Optional<Mixed> eval(U& tree, size_t* return_cnt)
    {
        size_t count = 0;
        auto result = Mixed{bptree_average<T>(tree, &count)};
        if (return_cnt) {
            *return_cnt = count;
        }
        return count == 0 ? util::none : result;
    }
    static util::Optional<Mixed> not_found(size_t* return_cnt) noexcept
    {
        if (return_cnt)
            *return_cnt = 0;
        return Mixed{};
    }
};

/// Convenience base class for collections, which implements most of the
/// relevant interfaces for a collection that is bound to an object accessor and
/// representable as a BPlusTree<T>.
template <class Interface>
class CollectionBaseImpl : public Interface, protected ArrayParent {
public:
    static_assert(std::is_base_of_v<CollectionBase, Interface>);

    FullPath get_path() const override
    {
        auto path = m_parent->get_path();
        m_parent->add_index(path.path_from_top, m_index);
        return path;
    }

    Path get_short_path() const override
    {
        Path ret = m_parent->get_short_path();
        m_parent->add_index(ret, m_index);
        return ret;
    }

    StablePath get_stable_path() const override
    {
        auto ret = m_parent->get_stable_path();
        ret.push_back(m_index);
        return ret;
    }

    // Overriding members of CollectionBase:
    ColKey get_col_key() const noexcept override
    {
        return m_col_key;
    }

    const Obj& get_obj() const noexcept final
    {
        return m_obj_mem;
    }

    // The tricky thing here is that we should return true, even if the
    // collection has not yet been created.
    bool is_attached() const noexcept final
    {
        if (m_parent) {
            try {
                // Update the parent. Will throw if parent is not existing.
                switch (m_parent->update_if_needed()) {
                    case UpdateStatus::Updated:
                        // Make sure to update next time around
                        m_content_version = 0;
                        [[fallthrough]];
                    case UpdateStatus::NoChange:
                        // Check if it would be legal to try and get a ref from parent
                        // Will return true even if the current ref is 0.
                        return m_parent->check_collection_ref(m_index, Interface::s_collection_type);
                    case UpdateStatus::Detached:
                        break;
                }
            }
            catch (...) {
            }
        }
        return false;
    }

    /// Returns true if the accessor has changed since the last time
    /// `has_changed()` was called.
    ///
    /// Note: This method is not idempotent.
    ///
    /// Note: This involves a call to `update_if_needed()`.
    ///
    /// Note: This function returns false for an accessor that became
    /// detached since the last call
    bool has_changed() const noexcept final
    {
        try {
            // `has_changed()` sneakily modifies internal state.
            update_if_needed();
            if (m_last_content_version != m_content_version) {
                m_last_content_version = m_content_version;
                return true;
            }
        }
        catch (...) {
        }
        return false;
    }

    CollectionType get_collection_type() const noexcept override
    {
        return Interface::s_collection_type;
    }

    void set_owner(const Obj& obj, ColKey ck) override
    {
        m_obj_mem = obj;
        m_parent = &m_obj_mem;
        m_index = obj.build_index(ck);
        if (obj) {
            m_alloc = &obj.get_alloc();
        }
    }

    void set_owner(std::shared_ptr<CollectionParent> parent, CollectionParent::Index index) override
    {
        m_obj_mem = parent->get_object();
        m_col_parent = std::move(parent);
        m_parent = m_col_parent.get();
        m_index = index;
        if (m_obj_mem) {
            m_alloc = &m_obj_mem.get_alloc();
        }
        // Force update on next access
        m_content_version = 0;
    }

    CollectionParent* get_owner() const noexcept
    {
        return m_parent;
    }

    void to_json(std::ostream&, JSONOutputMode, util::FunctionRef<void(const Mixed&)>) const override;

    using Interface::get_owner_key;
    using Interface::get_table;
    using Interface::get_target_table;

protected:
    ObjCollectionParent m_obj_mem;
    std::shared_ptr<CollectionParent> m_col_parent;
    CollectionParent::Index m_index;
    ColKey m_col_key;
    mutable uint_fast64_t m_content_version = 0;
    // Content version used by `has_changed()`.
    mutable uint_fast64_t m_last_content_version = 0;
    mutable uint32_t m_parent_version = 0;
    bool m_nullable = false;

    CollectionBaseImpl() = default;
    CollectionBaseImpl(const CollectionBaseImpl& other)
        : Interface(static_cast<const Interface&>(other))
        , m_obj_mem(other.m_obj_mem)
        , m_col_parent(other.m_col_parent)
        , m_index(other.m_index)
        , m_col_key(other.m_col_key)
        , m_nullable(other.m_nullable)
        , m_parent(m_col_parent ? m_col_parent.get() : &m_obj_mem)
        , m_alloc(other.m_alloc)
    {
    }

    CollectionBaseImpl(const Obj& obj, ColKey col_key) noexcept
        : m_obj_mem(obj)
        , m_index(obj.build_index(col_key))
        , m_col_key(col_key)
        , m_nullable(col_key.is_nullable())
        , m_parent(&m_obj_mem)
    {
        if (obj) {
            m_alloc = &m_obj_mem.get_alloc();
        }
    }

    CollectionBaseImpl(ColKey col_key) noexcept
        : m_col_key(col_key)
        , m_nullable(col_key.is_nullable())
    {
    }

    CollectionBaseImpl(CollectionParent& parent, CollectionParent::Index index) noexcept
        : m_obj_mem(parent.get_object())
        , m_index(index)
        , m_col_key(parent.get_col_key())
        , m_parent(&parent)
        , m_alloc(&m_obj_mem.get_alloc())
    {
    }

    CollectionBaseImpl& operator=(const CollectionBaseImpl& other)
    {
        Interface::operator=(static_cast<const Interface&>(other));
        if (this != &other) {
            m_obj_mem = other.m_obj_mem;
            m_col_parent = other.m_col_parent;
            m_parent = m_col_parent ? m_col_parent.get() : &m_obj_mem;
            m_alloc = other.m_alloc;
            m_index = other.m_index;
            m_col_key = other.m_col_key;
            m_nullable = other.m_nullable;
        }

        return *this;
    }

    ref_type get_collection_ref() const
    {
        return m_parent->get_collection_ref(m_index, Interface::s_collection_type);
    }

    void set_collection_ref(ref_type ref)
    {
        m_parent->set_collection_ref(m_index, ref, Interface::s_collection_type);
    }

    UpdateStatus get_update_status() const
    {
        UpdateStatus status = m_parent ? m_parent->update_if_needed() : UpdateStatus::Detached;

        if (status != UpdateStatus::Detached) {
            auto content_version = m_alloc->get_content_version();
            auto parent_version = m_parent->parent_version();
            if (content_version != m_content_version || m_parent_version != parent_version) {
                m_content_version = content_version;
                m_parent_version = parent_version;
                status = UpdateStatus::Updated;
            }
        }

        return status;
    }

    /// Refresh the parent object (if needed) and compare version numbers.
    /// Return true if the collection should initialize from parent
    /// Throws if the owning object no longer exists.
    bool should_update() const
    {
        check_parent();
        auto status = get_update_status();
        if (status == UpdateStatus::Detached) {
            throw StaleAccessor("Parent no longer exists");
        }
        return status == UpdateStatus::Updated;
    }

    void bump_content_version() noexcept
    {
        REALM_ASSERT(m_alloc);
        m_content_version = m_alloc->bump_content_version();
        m_parent->update_content_version();
    }

    void update_content_version() const noexcept
    {
        REALM_ASSERT(m_alloc);
        m_content_version = m_alloc->get_content_version();
    }

    void bump_both_versions()
    {
        REALM_ASSERT(m_alloc);
        m_alloc->bump_content_version();
        m_alloc->bump_storage_version();
        m_parent->update_content_version();
    }

    Replication* get_replication() const
    {
        check_parent();
        return m_parent->get_table()->get_repl();
    }

    Table* get_table_unchecked() const
    {
        check_parent();
        auto t = m_parent->get_table();
        REALM_ASSERT(t);
        return t.unchecked_ptr();
    }

    Allocator& get_alloc() const
    {
        check_alloc();
        return *m_alloc;
    }

    void set_alloc(Allocator& alloc)
    {
        m_alloc = &alloc;
    }

    void set_backlink(ColKey col_key, ObjLink new_link) const
    {
        check_parent();
        m_parent->get_object().set_backlink(col_key, new_link);
    }
    // Used when replacing a link, return true if CascadeState contains objects to remove
    bool replace_backlink(ColKey col_key, ObjLink old_link, ObjLink new_link, CascadeState& state) const
    {
        check_parent();
        return m_parent->get_object().replace_backlink(col_key, old_link, new_link, state);
    }
    // Used when removing a backlink, return true if CascadeState contains objects to remove
    bool remove_backlink(ColKey col_key, ObjLink old_link, CascadeState& state) const
    {
        check_parent();
        return m_parent->get_object().remove_backlink(col_key, old_link, state);
    }

    /// Reset the accessor's tracking of the content version. Derived classes
    /// may choose to call this to force the accessor to become out of date,
    /// such that `update_if_needed()` returns `UpdateStatus::Updated` the next
    /// time it is called (or `UpdateStatus::Detached` if the data vanished in
    /// the meantime).
    void reset_content_version()
    {
        m_content_version = 0;
    }

    // Overriding ArrayParent interface:
    ref_type get_child_ref(size_t child_ndx) const noexcept final
    {
        static_cast<void>(child_ndx);
        return get_collection_ref();
    }

    void update_child_ref(size_t child_ndx, ref_type new_ref) final
    {
        static_cast<void>(child_ndx);
        set_collection_ref(new_ref);
    }

private:
    CollectionParent* m_parent = nullptr;
    Allocator* m_alloc = nullptr;

    void check_parent() const
    {
        if (!m_parent) {
            throw StaleAccessor("Collection has no owner");
        }
    }
    void check_alloc() const
    {
        if (!m_alloc) {
            throw StaleAccessor("Allocator not set");
        }
    }
    /// Refresh the associated `Obj` (if needed), and update the internal
    /// content version number. This is meant to be called from a derived class
    /// before accessing its data.
    ///
    /// If the `Obj` changed since the last call, or the content version was
    /// bumped, this returns `UpdateStatus::Updated`. In response, the caller
    /// must invoke `init_from_parent()` or similar on its internal state
    /// accessors to refresh its view of the data.
    ///
    /// If the owning object (or parent container) was deleted, an exception will
    /// be thrown and the caller should enter a degenerate state.
    ///
    /// If no change has happened to the data, this function returns
    /// `UpdateStatus::NoChange`, and the caller is allowed to not do anything.
    virtual UpdateStatus update_if_needed() const = 0;
};

namespace _impl {
/// Translate from condensed index to uncondensed index in collections that hide
/// tombstones.
size_t virtual2real(const std::vector<size_t>& vec, size_t ndx) noexcept;
size_t virtual2real(const BPlusTree<ObjKey>* tree, size_t ndx) noexcept;

/// Translate from uncondensed index to condensed into in collections that hide
/// tombstones.
size_t real2virtual(const std::vector<size_t>& vec, size_t ndx) noexcept;

/// Rebuild the list of unresolved keys for tombstone handling.
void update_unresolved(std::vector<size_t>& vec, const BPlusTree<ObjKey>* tree);

/// Clear the context flag on the tree if there are no more unresolved links.
void check_for_last_unresolved(BPlusTree<ObjKey>* tree);
} // namespace _impl

/// Base class for collections of objects, where unresolved links (tombstones)
/// can occur.
template <class Interface>
class ObjCollectionBase : public Interface, public ObjList {
public:
    static_assert(std::is_base_of_v<CollectionBase, Interface>);

    using Interface::get_col_key;
    using Interface::get_obj;
    using Interface::get_table;
    using Interface::is_attached;
    using Interface::size;

    // Overriding methods in ObjList:

    void get_dependencies(TableVersions& versions) const final
    {
        if (is_attached()) {
            auto table = this->get_table();
            versions.emplace_back(table->get_key(), table->get_content_version());
        }
    }

    void sync_if_needed() const final
    {
        update_if_needed();
    }

    bool is_in_sync() const noexcept final
    {
        return true;
    }

    bool has_unresolved() const noexcept
    {
        update_if_needed();
        return m_unresolved.size() != 0;
    }

    LinkCollectionPtr clone_as_obj_list() const final
    {
        return clone_obj_list();
    }

    TableRef get_target_table() const final
    {
        return Interface::get_target_table();
    }

protected:
    ObjCollectionBase() = default;
    ObjCollectionBase(const ObjCollectionBase&) = default;
    ObjCollectionBase(ObjCollectionBase&&) = default;
    ObjCollectionBase& operator=(const ObjCollectionBase&) = default;
    ObjCollectionBase& operator=(ObjCollectionBase&&) = default;

    /// Implementations should call `update_if_needed()` on their inner accessor
    /// (without `update_unresolved()`).
    virtual UpdateStatus do_update_if_needed() const = 0;

    /// Implementations should return a non-const reference to their internal
    /// `BPlusTree<T>`.
    virtual BPlusTree<ObjKey>* get_mutable_tree() const = 0;

    /// Implements `update_if_needed()` in a way that ensures the consistency of
    /// the unresolved list. Derived classes should call this instead of calling
    /// `update_if_needed()` on their inner accessor.
    UpdateStatus update_if_needed() const
    {
        auto status = do_update_if_needed();
        update_unresolved(status);
        return status;
    }

    /// Translate from condensed index to uncondensed.
    size_t virtual2real(size_t ndx) const noexcept
    {
        return _impl::virtual2real(m_unresolved, ndx);
    }

    /// Translate from uncondensed index to condensed.
    size_t real2virtual(size_t ndx) const noexcept
    {
        return _impl::real2virtual(m_unresolved, ndx);
    }

    bool real_is_unresolved(size_t ndx) const noexcept
    {
        return std::find(m_unresolved.begin(), m_unresolved.end(), ndx) != m_unresolved.end();
    }

    /// Rebuild the list of tombstones if there is a possibility that it has
    /// changed.
    ///
    /// If the accessor became detached, this clears the unresolved list.
    void update_unresolved(UpdateStatus status) const
    {
        switch (status) {
            case UpdateStatus::Detached: {
                clear_unresolved();
                break;
            }
            case UpdateStatus::Updated: {
                _impl::update_unresolved(m_unresolved, get_mutable_tree());
                break;
            }
            case UpdateStatus::NoChange:
                break;
        }
    }

    /// When a tombstone is removed from a list, call this to update internal
    /// flags that indicate the presence of tombstones.
    void check_for_last_unresolved()
    {
        _impl::check_for_last_unresolved(get_mutable_tree());
    }

    /// Clear the list of tombstones. It will be rebuilt the next time
    /// `update_if_needed()` is called.
    void clear_unresolved() const noexcept
    {
        m_unresolved.clear();
    }

    /// Return the number of tombstones.
    size_t num_unresolved() const noexcept
    {
        return m_unresolved.size();
    }

private:
    // Sorted set of indices containing unresolved links.
    mutable std::vector<size_t> m_unresolved;

    bool matches(const ObjList& other) const final
    {
        return get_owning_obj().get_key() == other.get_owning_obj().get_key() &&
               get_owning_col_key() == other.get_owning_col_key();
    }
    Obj get_owning_obj() const final
    {
        return get_obj();
    }
    ColKey get_owning_col_key() const final
    {
        return get_col_key();
    }
};

/// Random-access iterator over elements of a collection.
///
/// Values are cached into a member variable in order to support `operator->`
/// and `operator*` returning a pointer and a reference, respectively.
template <class L>
struct CollectionIterator {
    using iterator_category = std::random_access_iterator_tag;
    using value_type = typename L::value_type;
    using difference_type = ptrdiff_t;
    using pointer = const value_type*;
    using reference = const value_type&;

    CollectionIterator() noexcept = default;
    CollectionIterator(const L* l, size_t ndx) noexcept
        : m_list(l)
        , m_ndx(ndx)
    {
    }

    pointer operator->() const
    {
        if constexpr (std::is_same_v<L, CollectionBase>) {
            m_val = m_list->get_any(m_ndx);
        }
        else {
            m_val = m_list->get(m_ndx);
        }
        return &m_val;
    }

    reference operator*() const
    {
        return *operator->();
    }

    CollectionIterator& operator++() noexcept
    {
        ++m_ndx;
        return *this;
    }

    CollectionIterator operator++(int) noexcept
    {
        auto tmp = *this;
        operator++();
        return tmp;
    }

    CollectionIterator& operator--() noexcept
    {
        --m_ndx;
        return *this;
    }

    CollectionIterator operator--(int) noexcept
    {
        auto tmp = *this;
        operator--();
        return tmp;
    }

    CollectionIterator& operator+=(ptrdiff_t n) noexcept
    {
        m_ndx += n;
        return *this;
    }

    CollectionIterator& operator-=(ptrdiff_t n) noexcept
    {
        m_ndx -= n;
        return *this;
    }

    friend ptrdiff_t operator-(const CollectionIterator& lhs, const CollectionIterator& rhs) noexcept
    {
        return ptrdiff_t(lhs.m_ndx) - ptrdiff_t(rhs.m_ndx);
    }

    friend CollectionIterator operator+(CollectionIterator lhs, ptrdiff_t rhs) noexcept
    {
        lhs.m_ndx += rhs;
        return lhs;
    }

    friend CollectionIterator operator+(ptrdiff_t lhs, CollectionIterator rhs) noexcept
    {
        return rhs + lhs;
    }

    bool operator!=(const CollectionIterator& rhs) const noexcept
    {
        REALM_ASSERT_DEBUG(m_list == rhs.m_list);
        return m_ndx != rhs.m_ndx;
    }

    bool operator==(const CollectionIterator& rhs) const noexcept
    {
        REALM_ASSERT_DEBUG(m_list == rhs.m_list);
        return m_ndx == rhs.m_ndx;
    }

    size_t index() const noexcept
    {
        return m_ndx;
    }

private:
    mutable value_type m_val;
    const L* m_list = nullptr;
    size_t m_ndx = size_t(-1);
};


inline CollectionIterator<CollectionBase> CollectionBase::begin() const
{
    return CollectionIterator<CollectionBase>(this, 0);
}
inline CollectionIterator<CollectionBase> CollectionBase::end() const
{
    return CollectionIterator<CollectionBase>(this, size());
}
template <class T>
class IteratorAdapter {
public:
    IteratorAdapter(T* keys)
        : m_list(keys)
    {
    }
    CollectionIterator<T> begin() const
    {
        return CollectionIterator<T>(m_list, 0);
    }
    CollectionIterator<T> end() const
    {
        return CollectionIterator<T>(m_list, m_list->size());
    }

private:
    T* m_list;
};

namespace _impl {
size_t get_collection_size_from_ref(ref_type, Allocator& alloc);
}

} // namespace realm

#endif // REALM_COLLECTION_HPP
