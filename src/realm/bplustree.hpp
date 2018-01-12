/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#ifndef REALM_BPLUSTREE_HPP
#define REALM_BPLUSTREE_HPP

#include <realm/column_type_traits.hpp>
#include <iostream>

namespace realm {

class BPlusTreeBase;

/*****************************************************************************/
/* BPlusTreeNode                                                             */
/* Base class for all nodes in the BPlusTree. Provides an abstract interface */
/* that can be used by the BPlusTreeBase class to manipulate the tree.       */
/*****************************************************************************/
class BPlusTreeNode {
public:
    struct State {
        int64_t split_offset;
        size_t split_size;
    };

    // Insert an element at 'insert_pos'. May cause node to be split
    using InsertFunc = std::function<size_t(BPlusTreeNode*, size_t insert_pos)>;
    // Access element at 'ndx'. Insertion/deletion not allowed
    using AccessFunc = std::function<void(BPlusTreeNode*, size_t ndx)>;
    // Erase element at erase_pos. May cause nodes to be merged
    using EraseFunc = std::function<size_t(BPlusTreeNode*, size_t erase_pos)>;
    // Function to be called for all leaves in the tree until the function
    // returns 'true'. 'offset' gives index of the first element in the leaf.
    using TraverseFunc = std::function<bool(BPlusTreeNode*, size_t offset)>;

    BPlusTreeNode(BPlusTreeBase* tree)
        : m_tree(tree)
    {
    }

    virtual ~BPlusTreeNode();

    virtual bool is_leaf() const = 0;
    virtual bool is_compact() const = 0;
    virtual ref_type get_ref() const = 0;

    virtual void init_from_ref(ref_type ref) noexcept = 0;

    virtual void set_parent(ArrayParent* parent, size_t ndx_in_parent) = 0;
    virtual void update_parent() = 0;

    // Number of elements in this node
    virtual size_t get_node_size() const = 0;
    // Size of subtree
    virtual size_t get_tree_size() const = 0;

    virtual ref_type bptree_insert(size_t n, State& state, InsertFunc&) = 0;
    virtual void bptree_access(size_t n, AccessFunc&) = 0;
    virtual size_t bptree_erase(size_t n, EraseFunc&) = 0;
    virtual bool bptree_traverse(size_t n, TraverseFunc&) = 0;

    // Move elements over in new node, starting with element at position 'ndx'.
    // If this is an inner node, the index offsets should be adjusted with 'adj'
    virtual void move(BPlusTreeNode* new_node, size_t ndx, int64_t offset_adj) = 0;

protected:
    BPlusTreeBase* m_tree;
};

/*****************************************************************************/
/* BPlusTreeLeaf                                                             */
/* Base class for all leaf nodes.                                            */
/*****************************************************************************/
class BPlusTreeLeaf : public BPlusTreeNode {
public:
    using BPlusTreeNode::BPlusTreeNode;

    bool is_leaf() const override
    {
        return true;
    }
    bool is_compact() const override
    {
        return true;
    }

    ref_type bptree_insert(size_t n, State& state, InsertFunc&) override;
    void bptree_access(size_t n, AccessFunc&) override;
    size_t bptree_erase(size_t n, EraseFunc&) override;
    bool bptree_traverse(size_t n, TraverseFunc&) override;
};

/*****************************************************************************/
/* BPlusTreeInner                                                            */
/* All interior nodes is of this class                                       */
/*****************************************************************************/
class BPlusTreeInner : public BPlusTreeNode, private Array {
public:
    BPlusTreeInner(BPlusTreeBase* tree);
    ~BPlusTreeInner() override;

    void init_from_mem(MemRef mem);
    void create(size_t elems_per_child);

    void init_from_ref(ref_type ref) noexcept override
    {
        char* header = m_alloc.translate(ref);
        init_from_mem(MemRef(header, ref, m_alloc));
    }

    bool is_leaf() const override
    {
        return false;
    }
    bool is_compact() const override
    {
        return (Array::get(0) & 1) != 0;
    }
    ref_type get_ref() const override
    {
        return Array::get_ref();
    }

    void set_parent(ArrayParent* p, size_t n) override
    {
        Array::set_parent(p, n);
    }
    void update_parent() override
    {
        Array::update_parent();
    }

    size_t get_node_size() const override
    {
        return Array::size() - 2;
    }
    size_t get_tree_size() const override
    {
        return size_t(back()) >> 1;
    }

    ref_type bptree_insert(size_t n, State& state, InsertFunc&) override;
    void bptree_access(size_t n, AccessFunc&) override;
    size_t bptree_erase(size_t n, EraseFunc&) override;
    bool bptree_traverse(size_t n, TraverseFunc&) override;

protected:
    friend BPlusTreeBase;
    ArrayUnsigned m_offsets;
    void move(BPlusTreeNode* new_node, size_t ndx, int64_t offset_adj) override;
    void ensure_offsets();
    void set_tree_size(size_t sz)
    {
        Array::set(m_size - 1, (sz << 1) + 1);
    }
    void append_tree_size(size_t sz)
    {
        Array::add((sz << 1) + 1);
    }
    size_t get_elems_per_child()
    {
        // Only relevant when in compact form
        return !m_offsets.is_attached() ? size_t(Array::get(0)) >> 1 : 0;
    }
    void _add_child_ref(ref_type ref, int64_t offset = 0)
    {
        Array::add(from_ref(ref));
        REALM_ASSERT_DEBUG(offset >= 0);
        if (offset && m_offsets.is_attached()) {
            m_offsets.add(offset);
        }
    }
    void _insert_child_ref(size_t ndx, ref_type ref)
    {
        Array::insert(ndx + 1, from_ref(ref));
    }
    void _clear_child_ref(size_t ndx)
    {
        Array::set(ndx + 1, 0);
    }
    ref_type _get_child_ref(size_t ndx) const
    {
        return Array::get_as_ref(ndx + 1);
    }
    BPlusTreeLeaf* cache_leaf(MemRef mem, size_t ndx);
    void erase_and_destroy_child(size_t ndx);
    ref_type insert_child(size_t child_ndx, ref_type new_sibling_ref, State& state);
    size_t get_child_offset(size_t child_ndx)
    {
        return (child_ndx) > 0 ? size_t(m_offsets.get(child_ndx - 1)) : 0;
    }
};

/*****************************************************************************/
/* BPlusTreeBase                                                             */
/* Base class for the actual tree classes.                                   */
/*****************************************************************************/
class BPlusTreeBase {
public:
    BPlusTreeBase(Allocator& alloc)
        : m_alloc(alloc)
    {
    }
    virtual ~BPlusTreeBase();

    Allocator& get_alloc()
    {
        return m_alloc;
    }

    ref_type get_ref() const
    {
        return m_root->get_ref();
    }

    void init_from_ref(ref_type ref)
    {
        this->replace_root(this->create_root_from_ref(ref));
    }

    void set_parent(ArrayParent* parent, size_t ndx_in_parent)
    {
        m_parent = parent;
        m_ndx_in_parent = ndx_in_parent;
        if (m_root)
            m_root->set_parent(parent, ndx_in_parent);
    }

    size_t size() const
    {
        return m_root ? m_root->get_tree_size() : 0;
    }

    void create()
    {
        REALM_ASSERT(!m_root);
        m_root = create_leaf_node();
        m_root->set_parent(m_parent, m_ndx_in_parent);
    }

    void destroy()
    {
        ref_type ref = m_root->get_ref();
        Array::destroy_deep(ref, m_alloc);
        m_root = nullptr;
    }

    void bptree_insert(size_t n, BPlusTreeNode::InsertFunc& func);
    void bptree_erase(size_t n, BPlusTreeNode::EraseFunc& func);

    // Create an un-attached leaf node
    virtual std::unique_ptr<BPlusTreeLeaf> create_leaf_node() = 0;
    // Create a leaf node and initialize it with 'ref'
    virtual std::unique_ptr<BPlusTreeLeaf> init_leaf_node(ref_type ref) = 0;

    // Initialize the leaf cache with 'mem' and set the proper parent
    virtual BPlusTreeLeaf* cache_leaf(MemRef mem, ArrayParent* parent, size_t ndx_in_parent) = 0;

protected:
    std::unique_ptr<BPlusTreeNode> m_root;
    Allocator& m_alloc;
    ArrayParent* m_parent = nullptr;
    size_t m_ndx_in_parent = 0;

    void replace_root(std::unique_ptr<BPlusTreeNode> new_root);
    std::unique_ptr<BPlusTreeNode> create_root_from_ref(ref_type ref);
};

/*****************************************************************************/
/* BPlusTree                                                                 */
/* Actual implementation of the BPlusTree to hold elements of type T.        */
/*****************************************************************************/
template <class T>
class BPlusTree : public BPlusTreeBase {
public:
    using LeafArray = typename ColumnTypeTraits<T>::cluster_leaf_type;

    /**
     * Actual class for the leaves. Maps the abstract interface defined
     * in BPlusTreeNode onto the specific array class
     **/
    class LeafNode : public BPlusTreeLeaf, public LeafArray {
    public:
        LeafNode(BPlusTreeBase* tree)
            : BPlusTreeLeaf(tree)
            , LeafArray(tree->get_alloc())
        {
        }

        void init_from_ref(ref_type ref) noexcept override
        {
            LeafArray::init_from_ref(ref);
        }

        ref_type get_ref() const override
        {
            return LeafArray::get_ref();
        }

        void set_parent(realm::ArrayParent* p, size_t n) override
        {
            LeafArray::set_parent(p, n);
        }
        void update_parent() override
        {
            LeafArray::update_parent();
        }

        size_t get_node_size() const override
        {
            return LeafArray::size();
        }
        size_t get_tree_size() const override
        {
            return LeafArray::size();
        }

        void move(BPlusTreeNode* new_node, size_t ndx, int64_t) override
        {
            LeafNode* dst(static_cast<LeafNode*>(new_node));
            size_t end = get_node_size();

            for (size_t j = ndx; j < end; j++) {
                dst->add(LeafArray::get(j));
            }
            LeafArray::truncate_and_destroy_children(ndx);
        }
    };

    /******************** Constructor ********************/

    BPlusTree(Allocator& alloc)
        : BPlusTreeBase(alloc)
        , m_leaf_cache(this)
    {
    }

    /******** Implementation of abstract interface *******/

    std::unique_ptr<BPlusTreeLeaf> create_leaf_node() override
    {
        std::unique_ptr<BPlusTreeLeaf> leaf = std::make_unique<LeafNode>(this);
        static_cast<LeafNode*>(leaf.get())->create();
        return leaf;
    }
    std::unique_ptr<BPlusTreeLeaf> init_leaf_node(ref_type ref) override
    {
        std::unique_ptr<BPlusTreeLeaf> leaf = std::make_unique<LeafNode>(this);
        leaf->init_from_ref(ref);
        return leaf;
    }
    BPlusTreeLeaf* cache_leaf(MemRef mem, ArrayParent* parent, size_t ndx_in_parent) override
    {
        m_leaf_cache.init_from_mem(mem);
        m_leaf_cache.set_parent(parent, ndx_in_parent);
        return &m_leaf_cache;
    }

    /************ Tree manipulation functions ************/

    void add(T value)
    {
        insert(npos, value);
    }
    void insert(size_t n, T value)
    {
        BPlusTreeNode::InsertFunc func = [value](BPlusTreeNode* node, size_t ndx) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            leaf->LeafArray::insert(ndx, value);
            return leaf->size();
        };

        bptree_insert(n, func);
    }
    T get(size_t n)
    {
        T value;
        BPlusTreeNode::AccessFunc func = [&value](BPlusTreeNode* node, size_t ndx) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            value = leaf->get(ndx);
        };

        m_root->bptree_access(n, func);

        return value;
    }
    void set(size_t n, T value)
    {
        BPlusTreeNode::AccessFunc func = [value](BPlusTreeNode* node, size_t ndx) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            leaf->set(ndx, value);
        };

        m_root->bptree_access(n, func);
    }
    void erase(size_t n)
    {
        BPlusTreeNode::EraseFunc func = [](BPlusTreeNode* node, size_t ndx) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            leaf->LeafArray::erase(ndx);
            return leaf->size();
        };

        bptree_erase(n, func);
    }
    size_t find_first(T value) const noexcept
    {
        size_t result = realm::npos;

        BPlusTreeNode::TraverseFunc func = [&result, value](BPlusTreeNode* node, size_t offset) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            size_t sz = leaf->size();
            auto i = leaf->find_first(value, 0, sz);
            if (i < sz) {
                result = i + offset;
                return true;
            }
            return false;
        };

        m_root->bptree_traverse(0, func);

        return result;
    }
    void dump_values() const
    {
        BPlusTreeNode::TraverseFunc func = [](BPlusTreeNode* node, size_t offset) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            size_t sz = leaf->size();
            std::cout << "Offset: " << offset << std::endl;
            for (size_t i = 0; i < sz; i++) {
                std::cout << "  " << leaf->get(i) << std::endl;
            }
            return false;
        };

        m_root->bptree_traverse(0, func);
    }

private:
    LeafNode m_leaf_cache;
};
}

#endif /* REALM_BPLUSTREE_HPP */
