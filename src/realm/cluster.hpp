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

namespace realm {

class Spec;
class Table;
class Cluster;
class ClusterNodeInner;
class ClusterTree;

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
    ref_type insert(Key k, State& state) override;
    void get(Key k, State& state) const override;
    unsigned erase(Key k) override;

    void dump_objects(int64_t key_offset, std::string lead) const override;

private:
    void insert_row(size_t ndx, Key k);
    void move(size_t ndx, ClusterNode* new_node, int64_t key_adj) override;
};

// 'Object' would have been a better name, but it clashes with a class in ObjectStore
class ConstObj {
public:
    ConstObj(const ClusterTree* tree_top, ref_type ref, Key key, size_t row_ndx);
    ConstObj& operator=(const ConstObj&) = delete;

    Key get_key() const
    {
        return m_key;
    }

    template <typename U>
    U get(size_t col_ndx) const;

    bool is_null(size_t col_ndx) const;

protected:
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
};

class Obj : public ConstObj {
public:
    Obj(ClusterTree* tree_top, ref_type ref, Key key, size_t row_ndx);

    template <typename U>
    Obj& set(size_t col_ndx, U value, bool is_default = false);
    Obj& set_null(size_t col_ndx, bool is_default = false);

    template <class Head, class... Tail>
    Obj& set_all(Head v, Tail... tail);

private:
    mutable bool m_writeable;

    template <class Val>
    Obj& _set(size_t col_ndx, Val v);
    template <class Head, class... Tail>
    Obj& _set(size_t col_ndx, Head v, Tail... tail);
    void update_if_needed() const;
};

class ClusterTree {
public:
    class ConstIterator;
    class Iterator;

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
    bool is_attached()
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
    Obj insert(Key k);
    void erase(Key k);
    ConstObj get(Key k) const;
    Obj get(Key k);
    void get_leaf(size_t ndx, ClusterNode::IteratorState& state) const noexcept;
    void dump_objects()
    {
        m_root->dump_objects(0, "");
    }

private:
    friend class ConstObj;
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

template <>
Obj& Obj::set<int64_t>(size_t, int64_t value, bool is_default);

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
