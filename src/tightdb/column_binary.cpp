#include <algorithm>
#include <iostream>
#include <iomanip>

#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/column_binary.hpp>

using namespace std;
using namespace tightdb;
using namespace tightdb::util;


namespace {

const size_t small_blob_max_size = 64;

void copy_leaf(const ArrayBinary& from, ArrayBigBlobs& to)
{
    size_t n = from.size();
    for (size_t i = 0; i != n; ++i) {
        BinaryData bin = from.get(i);
        to.add(bin); // Throws
    }
}

} // anonymous namespace


ColumnBinary::ColumnBinary(Allocator& alloc, ref_type ref)
{
    char* header = alloc.translate(ref);
    MemRef mem(header, ref);
    bool root_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
    if (root_is_leaf) {
        bool is_big = Array::get_context_flag_from_header(header);
        if (!is_big) {
            // Small blobs root leaf
            ArrayBinary* root = new ArrayBinary(alloc); // Throws
            root->init_from_mem(mem);
            m_array = root;
            return;
        }
        // Big blobs root leaf
        ArrayBigBlobs* root = new ArrayBigBlobs(alloc); // Throws
        root->init_from_mem(mem);
        m_array = root;
        return;
    }
    // Non-leaf root
    Array* root = new Array(alloc); // Throws
    root->init_from_mem(mem);
    m_array = root;
}


ColumnBinary::~ColumnBinary() TIGHTDB_NOEXCEPT
{
    delete m_array;
}


namespace {

struct SetLeafElem: Array::UpdateHandler {
    Allocator& m_alloc;
    const BinaryData m_value;
    const bool m_add_zero_term;
    SetLeafElem(Allocator& alloc, BinaryData value, bool add_zero_term) TIGHTDB_NOEXCEPT:
        m_alloc(alloc), m_value(value), m_add_zero_term(add_zero_term) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        bool is_big = Array::get_context_flag_from_header(mem.m_addr);
        if (is_big) {
            ArrayBigBlobs leaf(m_alloc);
            leaf.init_from_mem(mem);
            leaf.set_parent(parent, ndx_in_parent);
            leaf.set(elem_ndx_in_leaf, m_value, m_add_zero_term); // Throws
            return;
        }
        ArrayBinary leaf(m_alloc);
        leaf.init_from_mem(mem);
        leaf.set_parent(parent, ndx_in_parent);
        if (m_value.size() <= small_blob_max_size) {
            leaf.set(elem_ndx_in_leaf, m_value, m_add_zero_term); // Throws
            return;
        }
        // Upgrade leaf from small to big blobs
        ArrayBigBlobs new_leaf(m_alloc);
        new_leaf.create(); // Throws
        new_leaf.set_parent(parent, ndx_in_parent); // Throws
        new_leaf.update_parent(); // Throws
        copy_leaf(leaf, new_leaf); // Throws
        leaf.destroy();
        new_leaf.set(elem_ndx_in_leaf, m_value, m_add_zero_term); // Throws
    }
};

} // anonymous namespace

void ColumnBinary::set(size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx < size());

    bool root_is_leaf = !m_array->is_inner_bptree_node();
    if (root_is_leaf) {
        bool is_big = upgrade_root_leaf(value.size()); // Throws
        if (!is_big) {
            // Small blobs root leaf
            ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
            leaf->set(ndx, value, add_zero_term); // Throws
            return;
        }
        // Big blobs root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array);
        leaf->set(ndx, value, add_zero_term); // Throws
        return;
    }

    // Non-leaf root
    SetLeafElem set_leaf_elem(m_array->get_alloc(), value, add_zero_term);
    m_array->update_bptree_elem(ndx, set_leaf_elem); // Throws
}


bool ColumnBinary::compare_binary(const ColumnBinary& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i != n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


void ColumnBinary::do_insert(size_t row_ndx, BinaryData value, bool add_zero_term, size_t num_rows)
{
    TIGHTDB_ASSERT(row_ndx == tightdb::npos || row_ndx < size());
    ref_type new_sibling_ref;
    InsertState state;
    for (size_t i = 0; i != num_rows; ++i) {
        size_t row_ndx_2 = row_ndx == tightdb::npos ? tightdb::npos : row_ndx + i;
        if (root_is_leaf()) {
            TIGHTDB_ASSERT(row_ndx_2 == tightdb::npos || row_ndx_2 < TIGHTDB_MAX_BPNODE_SIZE);
            bool is_big = upgrade_root_leaf(value.size()); // Throws
            if (!is_big) {
                // Small blobs root leaf
                ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
                new_sibling_ref =
                    leaf->bptree_leaf_insert(row_ndx_2, value, add_zero_term, state); // Throws
            }
            else {
                // Big blobs root leaf
                ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array);
                new_sibling_ref =
                    leaf->bptree_leaf_insert(row_ndx_2, value, add_zero_term, state); // Throws
            }
        }
        else {
            // Non-leaf root
            state.m_value = value;
            state.m_add_zero_term = add_zero_term;
            if (row_ndx_2 == tightdb::npos) {
                new_sibling_ref = m_array->bptree_append(state);
            }
            else {
                new_sibling_ref = m_array->bptree_insert(row_ndx_2, state);
            }
        }
        if (TIGHTDB_UNLIKELY(new_sibling_ref)) {
            bool is_append = row_ndx_2 == tightdb::npos;
            introduce_new_root(new_sibling_ref, state, is_append);
        }
    }
}


ref_type ColumnBinary::leaf_insert(MemRef leaf_mem, ArrayParent& parent,
                                   size_t ndx_in_parent, Allocator& alloc,
                                   size_t insert_ndx,
                                   Array::TreeInsert<ColumnBinary>& state)
{
    InsertState& state_2 = static_cast<InsertState&>(state);
    bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
    if (is_big) {
        ArrayBigBlobs leaf(alloc);
        leaf.init_from_mem(leaf_mem);
        leaf.set_parent(&parent, ndx_in_parent);
        return leaf.bptree_leaf_insert(insert_ndx, state_2.m_value, state_2.m_add_zero_term,
                                       state); // Throws
    }
    ArrayBinary leaf(alloc);
    leaf.init_from_mem(leaf_mem);
    leaf.set_parent(&parent, ndx_in_parent);
    if (state_2.m_value.size() <= small_blob_max_size)
        return leaf.bptree_leaf_insert(insert_ndx, state_2.m_value, state_2.m_add_zero_term,
                                       state); // Throws
    // Upgrade leaf from small to big blobs
    ArrayBigBlobs new_leaf(alloc);
    new_leaf.create(); // Throws
    new_leaf.set_parent(&parent, ndx_in_parent);
    new_leaf.update_parent(); // Throws
    copy_leaf(leaf, new_leaf); // Throws
    leaf.destroy();
    return new_leaf.bptree_leaf_insert(insert_ndx, state_2.m_value, state_2.m_add_zero_term,
                                       state); // Throws
}


class ColumnBinary::EraseLeafElem: public ColumnBase::EraseHandlerBase {
public:
    EraseLeafElem(ColumnBinary& column) TIGHTDB_NOEXCEPT:
        EraseHandlerBase(column) {}
    bool erase_leaf_elem(MemRef leaf_mem, ArrayParent* parent,
                         size_t leaf_ndx_in_parent,
                         size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
        if (!is_big) {
            // Small blobs
            ArrayBinary leaf(get_alloc());
            leaf.init_from_mem(leaf_mem);
            leaf.set_parent(parent, leaf_ndx_in_parent);
            TIGHTDB_ASSERT(leaf.size() >= 1);
            size_t last_ndx = leaf.size() - 1;
            if (last_ndx == 0)
                return true;
            size_t ndx = elem_ndx_in_leaf;
            if (ndx == npos)
                ndx = last_ndx;
            leaf.erase(ndx); // Throws
            return false;
        }
        // Big blobs
        ArrayBigBlobs leaf(get_alloc());
        leaf.init_from_mem(leaf_mem);
        leaf.set_parent(parent, leaf_ndx_in_parent);
        TIGHTDB_ASSERT(leaf.size() >= 1);
        size_t last_ndx = leaf.size() - 1;
        if (last_ndx == 0)
            return true;
        size_t ndx = elem_ndx_in_leaf;
        if (ndx == npos)
            ndx = last_ndx;
        leaf.erase(ndx); // Throws
        return false;
    }
    void destroy_leaf(MemRef leaf_mem) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        Array::destroy_deep(leaf_mem, get_alloc());
    }
    void replace_root_by_leaf(MemRef leaf_mem) TIGHTDB_OVERRIDE
    {
        Array* leaf;
        bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
        if (!is_big) {
            // Small blobs
            ArrayBinary* leaf_2 = new ArrayBinary(get_alloc()); // Throws
            leaf_2->init_from_mem(leaf_mem);
            leaf = leaf_2;
        }
        else {
            // Big blobs
            ArrayBigBlobs* leaf_2 = new ArrayBigBlobs(get_alloc()); // Throws
            leaf_2->init_from_mem(leaf_mem);
            leaf = leaf_2;
        }
        replace_root(leaf); // Throws, but accessor ownership is passed to callee
    }
    void replace_root_by_empty_leaf() TIGHTDB_OVERRIDE
    {
        UniquePtr<ArrayBinary> leaf;
        leaf.reset(new ArrayBinary(get_alloc())); // Throws
        leaf->create(); // Throws
        replace_root(leaf.release()); // Throws, but accessor ownership is passed to callee
    }
};

void ColumnBinary::do_erase(size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(ndx < size());
    TIGHTDB_ASSERT(is_last == (ndx == size()-1));

    bool root_is_leaf = !m_array->is_inner_bptree_node();
    if (root_is_leaf) {
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Small blobs root leaf
            ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
            leaf->erase(ndx); // Throws
            return;
        }
        // Big blobs root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array);
        leaf->erase(ndx); // Throws
        return;
    }

    // Non-leaf root
    size_t ndx_2 = is_last ? npos : ndx;
    EraseLeafElem erase_leaf_elem(*this);
    Array::erase_bptree_elem(m_array, ndx_2, erase_leaf_elem); // Throws
}


void ColumnBinary::do_move_last_over(size_t row_ndx, size_t last_row_ndx)
{
    TIGHTDB_ASSERT(row_ndx <= last_row_ndx);
    TIGHTDB_ASSERT(last_row_ndx + 1 == size());

    // FIXME: ExceptionSafety: The current implementation of this
    // function is not exception-safe, and it is hard to see how to
    // repair it.

    // FIXME: Consider doing two nested calls to
    // update_bptree_elem(). If the two leaves are not the same, no
    // copying is needed. If they are the same, call
    // ArrayBinary::move_last_over() (does not yet
    // exist). ArrayBinary::move_last_over() could be implemented in a
    // way that avoids the intermediate copy. This approach is also
    // likely to be necesseray for exception safety.

    BinaryData value = get(last_row_ndx);

    // Copying binary data from a column to itself requires an
    // intermediate copy of the data (constr:bptree-copy-to-self).
    UniquePtr<char[]> buffer(new char[value.size()]); // Throws
    copy(value.data(), value.data()+value.size(), buffer.get());
    BinaryData copy_of_value(buffer.get(), value.size());

    set(row_ndx, copy_of_value); // Throws

    bool is_last = true;
    erase(last_row_ndx, is_last); // Throws
}


void ColumnBinary::do_clear()
{
    bool root_is_leaf = !m_array->is_inner_bptree_node();
    if (root_is_leaf) {
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Small blobs root leaf
            ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
            leaf->clear(); // Throws
            return;
        }
        // Big blobs root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array);
        leaf->clear(); // Throws
        return;
    }

    // Non-leaf root - revert to small blobs leaf
    Allocator& alloc = m_array->get_alloc();
    UniquePtr<ArrayBinary> array;
    array.reset(new ArrayBinary(alloc)); // Throws
    array->create(); // Throws
    array->set_parent(m_array->get_parent(), m_array->get_ndx_in_parent());
    array->update_parent(); // Throws

    // Remove original node
    m_array->destroy_deep();
    delete m_array;

    m_array = array.release();
}


bool ColumnBinary::upgrade_root_leaf(size_t value_size)
{
    TIGHTDB_ASSERT(root_is_leaf());

    bool is_big = m_array->get_context_flag();
    if (is_big)
        return true; // Big
    if (value_size <= small_blob_max_size)
        return false; // Small
    // Upgrade root leaf from small to big blobs
    ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
    Allocator& alloc = leaf->get_alloc();
    UniquePtr<ArrayBigBlobs> new_leaf;
    new_leaf.reset(new ArrayBigBlobs(alloc)); // Throws
    new_leaf->create(); // Throws
    new_leaf->set_parent(leaf->get_parent(), leaf->get_ndx_in_parent());
    new_leaf->update_parent(); // Throws
    copy_leaf(*leaf, *new_leaf); // Throws
    leaf->destroy();
    delete leaf;
    m_array = new_leaf.release();
    return true; // Big
}


class ColumnBinary::CreateHandler: public ColumnBase::CreateHandler {
public:
    CreateHandler(Allocator& alloc): m_alloc(alloc) {}
    ref_type create_leaf(size_t size) TIGHTDB_OVERRIDE
    {
        MemRef mem = ArrayBinary::create_array(size, m_alloc); // Throws
        return mem.m_ref;
    }
private:
    Allocator& m_alloc;
};

ref_type ColumnBinary::create(Allocator& alloc, size_t size)
{
    CreateHandler handler(alloc);
    return ColumnBase::create(alloc, size, handler);
}


class ColumnBinary::SliceHandler: public ColumnBase::SliceHandler {
public:
    SliceHandler(Allocator& alloc): m_alloc(alloc) {}
    MemRef slice_leaf(MemRef leaf_mem, size_t offset, size_t size,
                      Allocator& target_alloc) TIGHTDB_OVERRIDE
    {
        bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
        if (!is_big) {
            // Small blobs
            ArrayBinary leaf(m_alloc);
            leaf.init_from_mem(leaf_mem);
            return leaf.slice(offset, size, target_alloc); // Throws
        }
        // Big blobs
        ArrayBigBlobs leaf(m_alloc);
        leaf.init_from_mem(leaf_mem);
        return leaf.slice(offset, size, target_alloc); // Throws
    }
private:
    Allocator& m_alloc;
};

ref_type ColumnBinary::write(size_t slice_offset, size_t slice_size,
                             size_t table_size, _impl::OutputStream& out) const
{
    ref_type ref;
    if (root_is_leaf()) {
        Allocator& alloc = Allocator::get_default();
        MemRef mem;
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Small blobs
            ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
            mem = leaf->slice(slice_offset, slice_size, alloc); // Throws
        }
        else {
            // Big blobs
            ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array);
            mem = leaf->slice(slice_offset, slice_size, alloc); // Throws
        }
        Array slice(alloc);
        _impl::DeepArrayDestroyGuard dg(&slice);
        slice.init_from_mem(mem);
        size_t pos = slice.write(out); // Throws
        ref = pos;
    }
    else {
        SliceHandler handler(get_alloc());
        ref = ColumnBase::write(m_array, slice_offset, slice_size,
                                table_size, handler, out); // Throws
    }
    return ref;
}


void ColumnBinary::refresh_accessor_tree(size_t, const Spec&)
{
    // The type of the cached root array accessor may no longer match the
    // underlying root node. In that case we need to replace it. Note that when
    // the root node is an inner B+-tree node, then only the top array accessor
    // of that node is cached. The top array accessor of an inner B+-tree node
    // is of type Array.

    ref_type root_ref = m_array->get_ref_from_parent();
    MemRef root_mem(root_ref, m_array->get_alloc());
    bool new_root_is_leaf  = !Array::get_is_inner_bptree_node_from_header(root_mem.m_addr);
    bool new_root_is_small = !Array::get_context_flag_from_header(root_mem.m_addr);
    bool old_root_is_leaf  = !m_array->is_inner_bptree_node();
    bool old_root_is_small = !m_array->get_context_flag();

    bool root_type_changed = old_root_is_leaf != new_root_is_leaf ||
        (old_root_is_leaf && old_root_is_small != new_root_is_small);
    if (!root_type_changed) {
        // Keep, but refresh old root accessor
        if (old_root_is_leaf) {
            if (old_root_is_small) {
                // Root is 'small blobs' leaf
                ArrayBinary* root = static_cast<ArrayBinary*>(m_array);
                root->init_from_parent();
                return;
            }
            // Root is 'big blobs' leaf
            ArrayBigBlobs* root = static_cast<ArrayBigBlobs*>(m_array);
            root->init_from_parent();
            return;
        }
        // Root is inner node
        Array* root = m_array;
        root->init_from_parent();
        return;
    }

    // Create new root accessor
    Array* new_root;
    Allocator& alloc = m_array->get_alloc();
    if (new_root_is_leaf) {
        if (new_root_is_small) {
            // New root is 'small blobs' leaf
            ArrayBinary* root = new ArrayBinary(alloc); // Throws
            root->init_from_mem(root_mem);
            new_root = root;
        }
        else {
            // New root is 'big blobs' leaf
            ArrayBigBlobs* root = new ArrayBigBlobs(alloc); // Throws
            root->init_from_mem(root_mem);
            new_root = root;
        }
    }
    else {
        // New root is inner node
        Array* root = new Array(alloc); // Throws
        root->init_from_mem(root_mem);
        new_root = root;
    }
    new_root->set_parent(m_array->get_parent(), m_array->get_ndx_in_parent());

    // Destroy old root accessor
    if (old_root_is_leaf) {
        if (old_root_is_small) {
            // Old root is 'small blobs' leaf
            ArrayBinary* old_root = static_cast<ArrayBinary*>(m_array);
            delete old_root;
        }
        else {
            // Old root is 'big blobs' leaf
            ArrayBigBlobs* old_root = static_cast<ArrayBigBlobs*>(m_array);
            delete old_root;
        }
    }
    else {
        // Old root is inner node
        Array* old_root = m_array;
        delete old_root;
    }

    // Instate new root
    m_array = new_root;
}


#ifdef TIGHTDB_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    bool is_big = Array::get_context_flag_from_header(mem.m_addr);
    if (!is_big) {
        // Small blobs
        ArrayBinary leaf(alloc);
        leaf.init_from_mem(mem);
        leaf.Verify();
        return leaf.size();
    }
    // Big blobs
    ArrayBigBlobs leaf(alloc);
    leaf.init_from_mem(mem);
    leaf.Verify();
    return leaf.size();
}

} // anonymous namespace

void ColumnBinary::Verify() const
{
    if (root_is_leaf()) {
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Small blobs root leaf
            ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
            leaf->Verify();
            return;
        }
        // Big blobs root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array);
        leaf->Verify();
        return;
    }
    // Non-leaf root
    m_array->verify_bptree(&verify_leaf);
}


void ColumnBinary::to_dot(ostream& out, StringData title) const
{
    ref_type ref = m_array->get_ref();
    out << "subgraph cluster_binary_column" << ref << " {" << endl;
    out << " label = \"Binary column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;
    tree_to_dot(out);
    out << "}" << endl;
}

void ColumnBinary::leaf_to_dot(MemRef leaf_mem, ArrayParent* parent, size_t ndx_in_parent,
                               ostream& out) const
{
    bool is_strings = false; // FIXME: Not necessarily the case
    bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
    if (!is_big) {
        // Small blobs
        ArrayBinary leaf(m_array->get_alloc());
        leaf.init_from_mem(leaf_mem);
        leaf.set_parent(parent, ndx_in_parent);
        leaf.to_dot(out, is_strings);
        return;
    }
    // Big blobs
    ArrayBigBlobs leaf(m_array->get_alloc());
    leaf.init_from_mem(leaf_mem);
    leaf.set_parent(parent, ndx_in_parent);
    leaf.to_dot(out, is_strings);
}


namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, ostream& out, int level)
{
    size_t leaf_size;
    const char* leaf_type;
    bool is_big = Array::get_context_flag_from_header(mem.m_addr);
    if (!is_big) {
        // Small blobs
        ArrayBinary leaf(alloc);
        leaf.init_from_mem(mem);
        leaf_size = leaf.size();
        leaf_type = "Small blobs leaf";
    }
    else {
        // Big blobs
        ArrayBigBlobs leaf(alloc);
        leaf.init_from_mem(mem);
        leaf_size = leaf.size();
        leaf_type = "Big blobs leaf";
    }
    int indent = level * 2;
    out << setw(indent) << "" << leaf_type << " (size: "<<leaf_size<<")\n";
}

} // anonymous namespace

void ColumnBinary::do_dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // TIGHTDB_DEBUG
