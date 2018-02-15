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
    using TraverseFunction = std::function<bool(const Cluster*)>;

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
    Allocator& get_alloc() const { return m_alloc; }
    const Table* get_owner() const
    {
        return m_owner;
    }
    const Spec& get_spec() const;

    void init_from_ref(ref_type ref);
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
    int64_t get_last_key_value() const
    {
        return m_root->get_last_key_value();
    }
    MemRef ensure_writeable(ObjKey k)
    {
        return m_root->ensure_writeable(k);
    }
    Array& get_fields_accessor(Array& fallback, MemRef mem) const
    {
        if (m_root->is_leaf()) {
            return *m_root;
        }
        fallback.init_from_mem(mem);
        return fallback;
    }

    uint64_t bump_content_version()
    {
        m_alloc.bump_content_version();
        return m_alloc.get_content_version();
    }
    void bump_storage_version() { m_alloc.bump_storage_version(); }
    uint64_t get_content_version() const { return m_alloc.get_content_version(); }
    uint64_t get_instance_version() const { return m_alloc.get_instance_version(); }
    uint64_t get_storage_version(uint64_t inst_ver) const { return m_alloc.get_storage_version(inst_ver); }
    void insert_column(size_t ndx)
    {
        m_root->insert_column(ndx);
    }
    void remove_column(size_t ndx)
    {
        m_root->remove_column(ndx);
    }
    Obj insert(ObjKey k);
    void erase(ObjKey k, CascadeState& state);
    bool is_valid(ObjKey k) const;
    ConstObj get(ObjKey k) const;
    Obj get(ObjKey k);
    ConstObj get(size_t ndx) const;
    Obj get(size_t ndx);
    bool get_leaf(ObjKey key, ClusterNode::IteratorState& state) const noexcept;
    bool traverse(TraverseFunction func) const;
    void enumerate_string_column(size_t ndx);
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
    Allocator& m_alloc;
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
    void remove_links();
};

class ClusterTree::ConstIterator {
public:
    typedef std::output_iterator_tag iterator_category;
    typedef const Obj value_type;
    typedef ptrdiff_t difference_type;
    typedef const Obj* pointer;
    typedef const Obj& reference;

    ConstIterator(const ClusterTree& t, size_t ndx);
    ConstIterator(const ClusterTree& t, ObjKey key);
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
    mutable uint64_t m_storage_version = uint64_t(-1);
    mutable Cluster m_leaf;
    mutable ClusterNode::IteratorState m_state;
    mutable uint64_t m_instance_version = uint64_t(-1);
    mutable ObjKey m_key;
    mutable std::aligned_storage<sizeof(Obj), alignof(Obj)>::type m_obj_cache_storage;

    ObjKey load_leaf(ObjKey key) const;
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
}

#endif /* REALM_CLUSTER_TREE_HPP */
