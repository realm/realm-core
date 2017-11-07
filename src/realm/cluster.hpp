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
        IteratorState(Cluster& leaf)
            : m_current_leaf(leaf)
        {
        }
        IteratorState(const IteratorState&);
        void clear();

        Cluster& m_current_leaf;
        int64_t m_key_offset = 0;
        size_t m_current_index = 0;
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
    size_t lower_bound_key(Key key)
    {
        return m_keys.lower_bound_int(key.value);
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

}

#endif /* SRC_REALM_CLUSTER_HPP_ */
