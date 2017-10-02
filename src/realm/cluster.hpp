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

#ifndef REALM_CLUSTER_HPP
#define REALM_CLUSTER_HPP

#include <realm/array.hpp>
#include <realm/data_type.hpp>
#include <realm/column_type_traits.hpp>

namespace realm {

class Spec;
class Table;
class Cluster;
class ClusterNodeInner;
class ClusterTree;
template <class>
class ConstList;
template <class>
class List;

struct Key {
    constexpr Key()
        : value(-1)
    {
    }
    explicit Key(int64_t val)
        : value(val)
    {
    }
    Key& operator=(int64_t val)
    {
        value = val;
        return *this;
    }
    bool operator==(const Key& rhs) const
    {
        return value == rhs.value;
    }
    bool operator!=(const Key& rhs) const
    {
        return value != rhs.value;
    }
    bool operator<(const Key& rhs) const
    {
        return value < rhs.value;
    }
    int64_t value;
};

inline std::ostream& operator<<(std::ostream& ostr, Key key)
{
    ostr << key.value;
    return ostr;
}

constexpr Key null_key;

class ClusterNode : public Array {
public:
    // This structure is used to bring information back to the upper nodes when
    // inserting new objects or finding existing ones.
    struct State {
        int64_t split_key; // When a node is split, this variable holds the value of the
                           // first key in the new node. (Relative to the key offset)
        ref_type ref;      // Ref to the Cluster holding the new/found object
        size_t index;      // The index within the Cluster at which the object is stored.
    };

    struct IteratorState {
        struct NodeInfo {
            NodeInfo(std::unique_ptr<ClusterNode> n, size_t s, size_t i)
                : node(std::move(n))
                , size(s)
                , index(i)
            {
            }
            std::unique_ptr<ClusterNode> node;
            size_t size;
            size_t index;
        };
        IteratorState(Cluster& leaf)
            : m_current_leaf(leaf)
        {
        }
        IteratorState(const IteratorState&);
        void clear();

        Cluster& m_current_leaf;
        int64_t m_key_offset = 0;
        size_t m_leaf_start_ndx = 0;
        size_t m_leaf_end_ndx = 0;
        size_t m_root_index = 0;
    };

    ClusterNode(Allocator& allocator, const ClusterTree& tree_top)
        : Array(allocator)
        , m_tree_top(tree_top)
        , m_keys(allocator)
    {
        m_keys.set_parent(this, 0);
    }
    virtual ~ClusterNode()
    {
    }
    void init_from_parent()
    {
        ref_type ref = get_ref_from_parent();
        char* header = m_alloc.translate(ref);
        init(MemRef(header, ref, m_alloc));
    }

    unsigned node_size() const
    {
        return is_attached() ? unsigned(m_keys.size()) : 0;
    }

    int64_t get_key(size_t ndx) const
    {
        return m_keys.get(ndx);
    }
    void adjust_keys(int64_t offset)
    {
        m_keys.adjust(0, m_keys.size(), offset);
    }

    virtual bool update_from_parent(size_t old_baseline) noexcept = 0;
    virtual bool is_leaf() const = 0;
    /// Number of elements in this subtree
    virtual size_t get_tree_size() const = 0;
    /// Last key in this subtree
    virtual int64_t get_last_key() const = 0;

    /// Create an empty node
    virtual void create() = 0;
    /// Initialize node from 'mem'
    virtual void init(MemRef mem) = 0;
    /// Descend the tree from the root and copy-on-write the leaf
    /// This will update all parents accordingly
    virtual MemRef ensure_writeable(Key k) = 0;

    /// Insert a column at position 'ndx'
    virtual void insert_column(size_t ndx) = 0;
    /// Remove a column at position 'ndx'
    virtual void remove_column(size_t ndx) = 0;
    /// Create a new object identified by 'key' and update 'state' accordingly
    /// Return reference to new node created (if any)
    virtual ref_type insert(Key k, State& state) = 0;
    /// Locate object identified by 'key' and update 'state' accordingly
    virtual void get(Key key, State& state) const = 0;

    /// Erase element identified by 'key'
    virtual unsigned erase(Key key) = 0;

    /// Move elements from position 'ndx' to 'new_node'. The new node is supposed
    /// to be a sibling positioned right after this one. All key values must
    /// be subtracted 'key_adj'
    virtual void move(size_t ndx, ClusterNode* new_leaf, int64_t key_adj) = 0;

    virtual void dump_objects(int64_t key_offset, std::string lead) const = 0;

protected:
    const ClusterTree& m_tree_top;
    Array m_keys;
};

class Cluster : public ClusterNode {
public:
    using ClusterNode::ClusterNode;
    ~Cluster() override;

    void create() override;
    void init(MemRef mem) override;
    bool update_from_parent(size_t old_baseline) noexcept override;
    bool is_writeable() const
    {
        return !Array::is_read_only();
    }
    MemRef ensure_writeable(Key k) override;

    bool is_leaf() const override
    {
        return true;
    }
    size_t get_tree_size() const override
    {
        return node_size();
    }
    int64_t get_last_key() const override
    {
        return m_keys.size() ? get_key(m_keys.size() - 1) : 0;
    }

    void insert_column(size_t ndx) override;
    void remove_column(size_t ndx) override;
    ref_type insert(Key k, State& state) override;
    void get(Key k, State& state) const override;
    unsigned erase(Key k) override;

    template <class T>
    void init_leaf(size_t col_ndx, T* leaf) const noexcept;
    const Array* get_key_array() const
    {
        return &m_keys;
    }

    void dump_objects(int64_t key_offset, std::string lead) const override;

private:
    void insert_row(size_t ndx, Key k);
    void move(size_t ndx, ClusterNode* new_node, int64_t key_adj) override;
    template <class T>
    void do_create(size_t col_ndx);
    template <class T>
    void do_insert_column(size_t col_ndx, bool nullable);
    template <class T>
    void do_insert_row(size_t ndx, size_t col_ndx, int attr);
    template <class T>
    void do_move(size_t ndx, size_t col_ndx, Cluster* to);
    template <class T>
    void do_erase(size_t ndx, size_t col_ndx);
};

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
    U get(StringData col_name) const;

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
};

class Obj : public ConstObj {
public:
    Obj(ClusterTree* tree_top, ref_type ref, Key key, size_t row_ndx);

    template <typename U>
    Obj& set(size_t col_ndx, U value, bool is_default = false);
    Obj& set_null(size_t col_ndx, bool is_default = false);

    template <typename U>
    Obj& set_list_values(size_t col_ndx, std::vector<U>& values);

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

class ConstListBase : public ArrayParent {
public:
    virtual ~ConstListBase();
    /*
     * Operations that makes sense without knowing the specific type
     * can be made virtual.
     */
    virtual size_t size() const = 0;
    virtual bool is_null() const = 0;

protected:
    const ConstObj* m_const_obj = nullptr;
    const size_t m_col_ndx;

    ConstListBase(size_t col_ndx)
        : m_col_ndx(col_ndx)
    {
    }
    virtual void init_from_parent() const = 0;

    void set_obj(const ConstObj* obj)
    {
        m_const_obj = obj;
    }
    ref_type get_child_ref(size_t) const noexcept override;
    std::pair<ref_type, size_t> get_to_dot_parent(size_t) const override;

    void update_if_needed() const
    {
        if (m_const_obj->update_if_needed()) {
            init_from_parent();
        }
    }
};

/// This class defines the interface to ConstList, except for the constructor
/// The ConstList class has the ConstObj member m_obj, which should not be
/// inherited from List<T>.
template <class T>
class ConstListIf : public ConstListBase {
public:
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;

    /**
     * Only member functions not referring to an index in the list will check if
     * the object is up-to-date. The logic is that the user must always check the
     * size before referring to a particular index, and size() will check for update.
     */
    size_t size() const override
    {
        update_if_needed();
        return m_valid ? m_leaf->size() : 0;
    }
    bool is_null() const override
    {
        return !m_valid;
    }
    T get(size_t ndx) const
    {
        return m_leaf->get(ndx);
    }
    T operator[](size_t ndx) const
    {
        return get(ndx);
    }

protected:
    mutable std::unique_ptr<LeafType> m_leaf;
    mutable bool m_valid = false;

    ConstListIf(size_t col_ndx, Allocator& alloc);

    void init_from_parent() const override
    {
        ref_type ref = get_child_ref(0);
        if (ref && (!m_valid || ref != m_leaf->get_ref())) {
            m_leaf->init_from_ref(ref);
            m_valid = true;
        }
    }
};

template <class T>
class ConstList : public ConstListIf<T> {
public:
    ConstList(const ConstObj& owner, size_t col_ndx);

private:
    ConstObj m_obj;
    void update_child_ref(size_t, ref_type) override
    {
    }
};
/*
 * This class defines a virtual interface to a writable list
 */
class ListBase {
public:
    virtual ~ListBase()
    {
    }
    virtual void resize(size_t new_size) = 0;
    virtual void remove(size_t from, size_t to) = 0;
    virtual void move(size_t from, size_t to) = 0;
    virtual void swap(size_t ndx1, size_t ndx2) = 0;
    virtual void clear() = 0;
};

template <class T>
class List : public ConstListIf<T>, public ListBase {
public:
    using ConstListIf<T>::m_leaf;
    using ConstListIf<T>::get;

    List(const Obj& owner, size_t col_ndx);

    void update_child_ref(size_t, ref_type new_ref) override
    {
        m_obj.set(ConstListBase::m_col_ndx, from_ref(new_ref));
    }

    void create()
    {
        m_leaf->create();
        ConstListIf<T>::m_valid = true;
    }
    void resize(size_t new_size) override
    {
        update_if_needed();
        size_t current_size = m_leaf->size();
        while (new_size > current_size) {
            m_leaf->add(ConstList<T>::LeafType::default_value(false));
            current_size++;
        }
        if (current_size > new_size) {
            m_leaf->truncate_and_destroy_children(new_size);
        }
    }
    void add(T value)
    {
        update_if_needed();
        m_leaf->insert(m_leaf->size(), value);
    }
    T set(size_t ndx, T value)
    {
        T old = m_leaf->get(ndx);
        if (old != value) {
            m_leaf->set(ndx, value);
        }
        return old;
    }
    void insert(size_t ndx, T value)
    {
        m_leaf->insert(ndx, value);
    }
    T remove(size_t ndx)
    {
        T ret = m_leaf->get(ndx);
        m_leaf->erase(ndx);
        return ret;
    }
    void remove(size_t from, size_t to) override
    {
        while (from < to) {
            remove(--to);
        }
    }
    void move(size_t from, size_t to) override
    {
        if (from != to) {
            T tmp = get(from);
            int incr = (from < to) ? 1 : -1;
            while (from != to) {
                size_t neighbour = from + incr;
                set(from, get(neighbour));
                from = neighbour;
            }
            set(to, tmp);
        }
    }
    void swap(size_t ndx1, size_t ndx2) override
    {
        if (ndx1 != ndx2) {
            T tmp = get(ndx1);
            set(ndx1, get(ndx2));
            set(ndx2, tmp);
        }
    }
    void clear() override
    {
        update_if_needed();
        m_leaf->truncate_and_destroy_children(0);
    }

private:
    Obj m_obj;
    void update_if_needed()
    {
        if (m_obj.update_if_needed()) {
            m_leaf->init_from_parent();
        }
    }
};

class ClusterTree {
public:
    class ConstIterator;
    class Iterator;
    using TraverseFunction = std::function<bool(const Cluster*, int64_t)>;

    ClusterTree(Table* owner, Allocator& alloc);
    static MemRef create_empty_cluster(Allocator& alloc);

    ClusterTree(ClusterTree&&) = default;
    ClusterTree& operator=(ClusterTree&&) = default;

    // Disable copying, this is not allowed.
    ClusterTree& operator=(const ClusterTree&) = delete;
    ClusterTree(const ClusterTree&) = delete;

    void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept
    {
        m_root->set_parent(parent, ndx_in_parent);
    }
    bool is_attached() const
    {
        return m_root->is_attached();
    }
    Allocator& get_alloc() const
    {
        return m_root->get_alloc();
    }
    const Table* get_owner() const
    {
        return m_owner;
    }
    const Spec& get_spec() const;

    void init_from_parent();
    bool update_from_parent(size_t old_baseline) noexcept;

    size_t size() const noexcept
    {
        return m_size;
    }
    void clear();
    bool is_empty() const noexcept
    {
        return size() == 0;
    }
    int64_t get_last_key() const
    {
        return m_root->get_last_key();
    }
    MemRef ensure_writeable(Key k)
    {
        return m_root->ensure_writeable(k);
    }
    uint64_t bump_version();
    uint64_t get_version_counter() const;
    void insert_column(size_t ndx)
    {
        m_root->insert_column(ndx);
    }
    void remove_column(size_t ndx)
    {
        m_root->remove_column(ndx);
    }
    Obj insert(Key k);
    void erase(Key k);
    ConstObj get(Key k) const;
    Obj get(Key k);
    void get_leaf(size_t ndx, ClusterNode::IteratorState& state) const noexcept;
    bool traverse(TraverseFunction func) const;
    void dump_objects()
    {
        m_root->dump_objects(0, "");
    }
    void verify() const
    {
        // TODO: implement
    }

private:
    friend class ConstObj;
    friend class Obj;
    friend class Cluster;
    friend class ClusterNodeInner;
    Table* m_owner;
    std::unique_ptr<ClusterNode> m_root;
    size_t m_size = 0;

    void replace_root(std::unique_ptr<ClusterNode> leaf);

    std::unique_ptr<ClusterNode> create_root_from_mem(Allocator& alloc, MemRef mem);
    std::unique_ptr<ClusterNode> create_root_from_ref(Allocator& alloc, ref_type ref)
    {
        return create_root_from_mem(alloc, MemRef{alloc.translate(ref), ref, alloc});
    }
    std::unique_ptr<ClusterNode> get_node(ref_type ref) const;

    size_t get_column_index(StringData col_name) const;
};

class ClusterTree::ConstIterator {
public:
    typedef std::output_iterator_tag iterator_category;
    typedef const Obj value_type;
    typedef ptrdiff_t difference_type;
    typedef const Obj* pointer;
    typedef const Obj& reference;

    ConstIterator(const ClusterTree& t, size_t ndx);
    ConstIterator(Iterator&&);
    ConstIterator(const ConstIterator& other)
        : ConstIterator(other.m_tree, other.m_ndx)
    {
    }
    reference operator*() const
    {
        return *operator->();
    }
    pointer operator->() const;
    ConstIterator& operator++();
    bool operator!=(const ConstIterator& rhs) const
    {
        return m_ndx != rhs.m_ndx;
    }

protected:
    const ClusterTree& m_tree;
    mutable uint64_t m_version = uint64_t(-1);
    mutable Cluster m_leaf;
    mutable ClusterNode::IteratorState m_state;
    mutable size_t m_ndx;
    mutable std::aligned_storage<sizeof(Obj), alignof(Obj)>::type m_obj_cache_storage;

    void load_leaf() const;
};

class ClusterTree::Iterator : public ClusterTree::ConstIterator {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef Obj value_type;
    typedef Obj* pointer;
    typedef Obj& reference;

    using ConstIterator::ConstIterator;
    reference operator*() const
    {
        return *operator->();
    }
    pointer operator->() const
    {
        return const_cast<pointer>(ConstIterator::operator->());
    }
};

template <class T>
void Cluster::init_leaf(size_t col_ndx, T* leaf) const noexcept
{
    ref_type ref = to_ref(Array::get(col_ndx + 1));
    leaf->init_from_ref(ref);
}

template <typename U>
U ConstObj::get(StringData col_name) const
{
    return get<U>(m_tree_top->get_column_index(col_name));
}

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
Obj& Obj::set_list_values(size_t col_ndx, std::vector<U>& values)
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

#endif /* SRC_REALM_CLUSTER_HPP_ */
