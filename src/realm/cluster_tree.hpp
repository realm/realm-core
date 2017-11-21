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

#ifndef REALM_CLUSTER_TREE_HPP
#define REALM_CLUSTER_TREE_HPP

#include <realm/obj.hpp>

namespace realm {

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
    bool is_valid(Key k) const;
    ConstObj get(Key k) const;
    Obj get(Key k);
    bool get_leaf(Key key, ClusterNode::IteratorState& state) const noexcept;
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
    ConstIterator(const ClusterTree& t, Key key);
    ConstIterator(Iterator&&);
    ConstIterator(const ConstIterator& other)
        : ConstIterator(other.m_tree, other.m_key)
    {
    }
    ConstIterator& operator=(ConstIterator&& other)
    {
        REALM_ASSERT(&m_tree == &other.m_tree);
        m_key = other.m_key;
        return *this;
    }
    reference operator*() const
    {
        return *operator->();
    }
    pointer operator->() const;
    ConstIterator& operator++();
    bool operator!=(const ConstIterator& rhs) const
    {
        return m_key != rhs.m_key;
    }

protected:
    const ClusterTree& m_tree;
    mutable uint64_t m_version = uint64_t(-1);
    mutable Cluster m_leaf;
    mutable ClusterNode::IteratorState m_state;
    mutable Key m_key;
    mutable std::aligned_storage<sizeof(Obj), alignof(Obj)>::type m_obj_cache_storage;

    Key load_leaf(Key key) const;
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
    Iterator& operator++()
    {
        return static_cast<Iterator&>(ConstIterator::operator++());
    }
};

template <class T>
void Cluster::init_leaf(size_t col_ndx, T* leaf) const noexcept
{
    ref_type ref = to_ref(Array::get(col_ndx + 1));
    leaf->init_from_ref(ref);
}
}

#endif /* REALM_CLUSTER_TREE_HPP */
