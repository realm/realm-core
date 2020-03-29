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
#include <realm/util/function_ref.hpp>

namespace realm {

class ClusterTree {
public:
    class Iterator;
    using TraverseFunction = util::FunctionRef<bool(const Cluster*)>;
    using UpdateFunction = util::FunctionRef<void(Cluster*)>;
    using ColIterateFunction = util::FunctionRef<bool(ColKey)>;

    ClusterTree(Allocator& alloc);
    virtual ~ClusterTree();
    static MemRef create_empty_cluster(Allocator& alloc);

    ClusterTree(ClusterTree&&) = default;

    // Disable copying, this is not allowed.
    ClusterTree& operator=(const ClusterTree&) = delete;
    ClusterTree(const ClusterTree&) = delete;

    bool is_attached() const
    {
        return m_root->is_attached();
    }
    Allocator& get_alloc() const
    {
        return m_alloc;
    }

    void init_from_ref(ref_type ref);
    void init_from_parent();
    bool update_from_parent(size_t old_baseline) noexcept;

    size_t size() const noexcept
    {
        return m_size;
    }
    void clear(CascadeState&);
    void destroy()
    {
        m_root->destroy_deep();
    }
    void nullify_links(ObjKey, CascadeState&);
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
    void bump_storage_version()
    {
        m_alloc.bump_storage_version();
    }
    uint64_t get_content_version() const
    {
        return m_alloc.get_content_version();
    }
    uint64_t get_instance_version() const
    {
        return m_alloc.get_instance_version();
    }
    uint64_t get_storage_version(uint64_t inst_ver) const
    {
        return m_alloc.get_storage_version(inst_ver);
    }
    void insert_column(ColKey col)
    {
        m_root->insert_column(col);
    }
    void remove_column(ColKey col)
    {
        m_root->remove_column(col);
    }

    // Insert entry for object, but do not create and return the object accessor
    void insert_fast(ObjKey k, const FieldValues& init_values, ClusterNode::State& state);
    // Create and return object
    ClusterNode::State insert(ObjKey k, const FieldValues&);
    // Delete object with given key
    void erase(ObjKey k, CascadeState& state);
    // Check if an object with given key exists
    bool is_valid(ObjKey k) const;
    // Lookup and return object
    ClusterNode::State get(ObjKey k) const;
    // Lookup by index
    ClusterNode::State get(size_t ndx, ObjKey& k) const;
    // Get logical index of object identified by k
    size_t get_ndx(ObjKey k) const;
    // Find the leaf containing the requested object
    bool get_leaf(ObjKey key, ClusterNode::IteratorState& state) const noexcept;
    // Visit all leaves and call the supplied function. Stop when function returns true.
    // Not allowed to modify the tree
    bool traverse(TraverseFunction func) const;
    // Visit all leaves and call the supplied function. The function can modify the leaf.
    void update(UpdateFunction func);
    virtual void for_each_and_every_column(ColIterateFunction) const = 0;
    virtual void set_spec(ArrayPayload& arr, ColKey::Idx col_ndx) const = 0;
    virtual bool is_string_enum_type(ColKey::Idx col_ndx) const = 0;
    virtual const Table* get_owner() const = 0;
    virtual size_t get_ndx_in_parent() const = 0;
    virtual ArrayParent* get_parent() const = 0;
    virtual size_t num_leaf_cols() const = 0;

    void dump_objects()
    {
        m_root->dump_objects(0, "");
    }
    void verify() const;

protected:
    Allocator& m_alloc;

private:
    friend class ConstObj;
    friend class Obj;
    friend class Cluster;
    friend class ClusterNodeInner;

    std::unique_ptr<ClusterNode> m_root;
    size_t m_size = 0;

    void replace_root(std::unique_ptr<ClusterNode> leaf);

    std::unique_ptr<ClusterNode> create_root_from_mem(Allocator& alloc, MemRef mem);
    std::unique_ptr<ClusterNode> create_root_from_ref(Allocator& alloc, ref_type ref)
    {
        return create_root_from_mem(alloc, MemRef{alloc.translate(ref), ref, alloc});
    }
    std::unique_ptr<ClusterNode> get_node(ref_type ref) const;

    void remove_all_links(CascadeState&);
};

class TableClusterTree : public ClusterTree {
public:
    class ConstIterator;
    class Iterator;

    TableClusterTree(Table* owner, Allocator& alloc, size_t top_position_for_cluster_tree);
    ~TableClusterTree() override;

    Obj insert(ObjKey k, const FieldValues& values)
    {
        auto state = ClusterTree::insert(k, values);
        return Obj(get_table_ref(), state.mem, k, state.index);
    }
    ConstObj get(ObjKey k) const
    {
        auto state = ClusterTree::get(k);
        return ConstObj(get_table_ref(), state.mem, k, state.index);
    }
    Obj get(ObjKey k)
    {
        auto state = ClusterTree::get(k);
        return Obj(get_table_ref(), state.mem, k, state.index);
    }
    ConstObj get(size_t ndx) const
    {
        ObjKey k;
        auto state = ClusterTree::get(ndx, k);
        return Obj(get_table_ref(), state.mem, k, state.index);
    }
    Obj get(size_t ndx)
    {
        ObjKey k;
        auto state = ClusterTree::get(ndx, k);
        return Obj(get_table_ref(), state.mem, k, state.index);
    }
    const Table* get_owner() const override
    {
        return m_owner;
    }

    void enumerate_string_column(ColKey col_key);

    TableRef get_table_ref() const;
    void for_each_and_every_column(ColIterateFunction func) const override;
    void set_spec(ArrayPayload& arr, ColKey::Idx col_ndx) const override;
    bool is_string_enum_type(ColKey::Idx col_ndx) const override;
    size_t get_ndx_in_parent() const override
    {
        return m_top_position_for_cluster_tree;
    }
    ArrayParent* get_parent() const override;
    size_t num_leaf_cols() const override;

private:
    Table* m_owner;
    size_t m_top_position_for_cluster_tree;
};


class ClusterTree::Iterator {
public:
    Iterator(const ClusterTree& t, size_t ndx);
    Iterator(const Iterator& other);

    Iterator& operator=(const Iterator& other)
    {
        REALM_ASSERT(&m_tree == &other.m_tree);
        m_position = other.m_position;
        m_key = other.m_key;
        m_leaf_invalid = true;

        return *this;
    }

    ObjKey go(size_t n);
    bool update() const;
    // Advance the iterator to the next object in the table. This also holds if the object
    // pointed to is deleted. That is - you will get the same result of advancing no matter
    // if the previous object is deleted or not.
    Iterator& operator++();

    Iterator& operator+=(ptrdiff_t adj);

    Iterator operator+(ptrdiff_t adj)
    {
        return Iterator(m_tree, get_position() + adj);
    }
    bool operator==(const Iterator& rhs) const
    {
        return m_key == rhs.m_key;
    }
    bool operator!=(const Iterator& rhs) const
    {
        return m_key != rhs.m_key;
    }

protected:
    const ClusterTree& m_tree;
    mutable uint64_t m_storage_version = uint64_t(-1);
    mutable Cluster m_leaf;
    mutable ClusterNode::IteratorState m_state;
    mutable uint64_t m_instance_version = uint64_t(-1);
    ObjKey m_key;
    mutable bool m_leaf_invalid;
    mutable size_t m_position;
    mutable size_t m_leaf_start_pos = size_t(-1);

    ObjKey load_leaf(ObjKey key) const;
    size_t get_position();
};

class TableClusterTree::ConstIterator : public ClusterTree::Iterator {
public:
    typedef std::output_iterator_tag iterator_category;
    typedef const Obj value_type;
    typedef ptrdiff_t difference_type;
    typedef const Obj* pointer;
    typedef const Obj& reference;

    ConstIterator(const TableClusterTree& t, size_t ndx)
        : ClusterTree::Iterator(t, ndx)
    {
        m_table = t.get_table_ref();
    }
    ConstIterator(TableRef table, const ClusterTree& t, size_t ndx)
        : ClusterTree::Iterator(t, ndx)
    {
        m_table = table;
    }

    // If the object pointed to by the iterator is deleted, you will get an exception if
    // you try to dereference the iterator before advancing it.

    // Random access relative to iterator position.
    reference operator[](size_t n);
    reference operator*() const
    {
        return *operator->();
    }
    pointer operator->() const;

protected:
    mutable Obj m_obj;
    TableRef m_table;
};

class TableClusterTree::Iterator : public TableClusterTree::ConstIterator {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef Obj value_type;
    typedef Obj* pointer;
    typedef Obj& reference;

    Iterator(const TableClusterTree& t, size_t ndx)
        : ConstIterator(t, ndx)
    {
    }
    Iterator(TableRef table, const ClusterTree& t, size_t ndx)
        : ConstIterator(table, t, ndx)
    {
    }

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
    Iterator& operator+=(ptrdiff_t adj)
    {
        return static_cast<Iterator&>(ConstIterator::operator+=(adj));
    }
    Iterator operator+(ptrdiff_t adj)
    {
        return Iterator(m_table, m_tree, get_position() + adj);
    }
};
}

#endif /* REALM_CLUSTER_TREE_HPP */
