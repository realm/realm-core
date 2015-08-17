#include <realm/bptree.hpp>
#include <realm/array_integer.hpp>

using namespace realm;

void BpTreeBase::replace_root(std::unique_ptr<Array> leaf)
{
    if (m_root) {
        // Maintain parent.
        ArrayParent* parent = m_root->get_parent();
        std::size_t index_in_parent = m_root->get_index_in_parent();
        leaf->set_parent(parent, index_in_parent);
        leaf->update_parent(); // Throws
    }
    m_root = std::move(leaf);
}

void BpTreeBase::introduce_new_root(ref_type new_sibling_ref, Array::TreeInsertBase& state,
                                    bool is_append)
{
    // At this point the original root and its new sibling is either
    // both leaves, or both inner nodes on the same form, compact or
    // general. Due to invar:bptree-node-form, the new root is allowed
    // to be on the compact form if is_append is true and both
    // siblings are either leaves or inner nodes on the compact form.

    Array* orig_root = &root();
    Allocator& alloc = get_alloc();
    std::unique_ptr<Array> new_root(new Array(alloc)); // Throws
    new_root->create(Array::type_InnerBptreeNode); // Throws
    new_root->set_parent(orig_root->get_parent(), orig_root->get_index_in_parent());
    new_root->update_parent(); // Throws
    bool compact_form =
        is_append && (!orig_root->is_inner_bptree_node() || orig_root->get(0) % 2 != 0);
    // Something is wrong if we were not appending and the original
    // root is still on the compact form.
    REALM_ASSERT(!compact_form || is_append);
    if (compact_form) {
        // FIXME: Dangerous cast here (unsigned -> signed)
        int_fast64_t v = state.m_split_offset; // elems_per_child
        new_root->add(1 + 2*v); // Throws
    }
    else {
        Array new_offsets(alloc);
        new_offsets.create(Array::type_Normal); // Throws
        // FIXME: Dangerous cast here (unsigned -> signed)
        new_offsets.add(state.m_split_offset); // Throws
        // FIXME: Dangerous cast here (unsigned -> signed)
        new_root->add(new_offsets.get_ref()); // Throws
    }
    // FIXME: Dangerous cast here (unsigned -> signed)
    new_root->add(orig_root->get_ref()); // Throws
    // FIXME: Dangerous cast here (unsigned -> signed)
    new_root->add(new_sibling_ref); // Throws
    // FIXME: Dangerous cast here (unsigned -> signed)
    int_fast64_t v = state.m_split_size; // total_elems_in_tree
    new_root->add(1 + 2*v); // Throws
    replace_root(std::move(new_root));
}

namespace {

class TreeWriter {
public:
    TreeWriter(_impl::OutputStream&) REALM_NOEXCEPT;
    ~TreeWriter() REALM_NOEXCEPT;

    void add_leaf_ref(ref_type child_ref, size_t elems_in_child, ref_type* is_last);

private:
    Allocator& m_alloc;
    _impl::OutputStream& m_out;
    class ParentLevel;
    std::unique_ptr<ParentLevel> m_last_parent_level;
};

class TreeWriter::ParentLevel {
public:
    ParentLevel(Allocator&, _impl::OutputStream&, size_t max_elems_per_child);
    ~ParentLevel() REALM_NOEXCEPT;

    void add_child_ref(ref_type child_ref, size_t elems_in_child,
                       bool leaf_or_compact, ref_type* is_last);

private:
    const size_t m_max_elems_per_child; // A power of `REALM_MAX_BPNODE_SIZE`
    size_t m_elems_in_parent; // Zero if reinitialization is needed
    bool m_is_on_general_form; // Defined only when m_elems_in_parent > 0
    Array m_main;
    ArrayInteger m_offsets;
    _impl::OutputStream& m_out;
    std::unique_ptr<ParentLevel> m_prev_parent_level;
};


inline TreeWriter::TreeWriter(_impl::OutputStream& out) REALM_NOEXCEPT:
    m_alloc(Allocator::get_default()),
    m_out(out)
{
}

inline TreeWriter::~TreeWriter() REALM_NOEXCEPT
{
}

void TreeWriter::add_leaf_ref(ref_type leaf_ref, size_t elems_in_leaf, ref_type* is_last)
{
    if (!m_last_parent_level) {
        if (is_last) {
            *is_last = leaf_ref;
            return;
        }
        m_last_parent_level.reset(new ParentLevel(m_alloc, m_out,
                                                  REALM_MAX_BPNODE_SIZE)); // Throws
    }
    bool leaf_or_compact = true;
    m_last_parent_level->add_child_ref(leaf_ref, elems_in_leaf,
                                       leaf_or_compact, is_last); // Throws
}


inline TreeWriter::ParentLevel::ParentLevel(Allocator& alloc, _impl::OutputStream& out,
                                            size_t max_elems_per_child):
    m_max_elems_per_child(max_elems_per_child),
    m_elems_in_parent(0),
    m_main(alloc),
    m_offsets(alloc),
    m_out(out)
{
    m_main.create(Array::type_InnerBptreeNode); // Throws
}

inline TreeWriter::ParentLevel::~ParentLevel() REALM_NOEXCEPT
{
    m_offsets.destroy(); // Shallow
    m_main.destroy(); // Shallow
}

void TreeWriter::ParentLevel::add_child_ref(ref_type child_ref, size_t elems_in_child,
                                            bool leaf_or_compact, ref_type* is_last)
{
    bool force_general_form = !leaf_or_compact ||
        (elems_in_child != m_max_elems_per_child &&
         m_main.size() != 1 + REALM_MAX_BPNODE_SIZE - 1 &&
         !is_last);

    // Add the incoming child to this inner node
    if (m_elems_in_parent > 0) { // This node contains children already
        if (!m_is_on_general_form && force_general_form) {
            if (!m_offsets.is_attached())
                m_offsets.create(Array::type_Normal); // Throws
            int_fast64_t v(m_max_elems_per_child); // FIXME: Dangerous cast (unsigned -> signed)
            size_t n = m_main.size();
            for (size_t i = 1; i != n; ++i)
                m_offsets.add(v); // Throws
            m_is_on_general_form = true;
        }
        {
            int_fast64_t v(child_ref); // FIXME: Dangerous cast (unsigned -> signed)
            m_main.add(v); // Throws
        }
        if (m_is_on_general_form) {
            int_fast64_t v(m_elems_in_parent); // FIXME: Dangerous cast (unsigned -> signed)
            m_offsets.add(v); // Throws
        }
        m_elems_in_parent += elems_in_child;
        if (!is_last && m_main.size() < 1 + REALM_MAX_BPNODE_SIZE)
          return;
    }
    else { // First child in this node
        m_main.add(0); // Placeholder for `elems_per_child` or `offsets_ref`
        int_fast64_t v(child_ref); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.add(v); // Throws
        m_elems_in_parent = elems_in_child;
        m_is_on_general_form = force_general_form; // `invar:bptree-node-form`
        if (m_is_on_general_form && !m_offsets.is_attached())
            m_offsets.create(Array::type_Normal); // Throws
        if (!is_last)
            return;
    }

    // No more children will be added to this node

    // Write this inner node to the output stream
    if (!m_is_on_general_form) {
        int_fast64_t v(m_max_elems_per_child); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.set(0, 1 + 2*v); // Throws
    }
    else {
        size_t pos = m_offsets.write(m_out); // Throws
        ref_type ref = pos;
        int_fast64_t v(ref); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.set(0, v); // Throws
    }
    {
        int_fast64_t v(m_elems_in_parent); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.add(1 + 2*v); // Throws
    }
    bool recurse = false; // Shallow
    size_t pos = m_main.write(m_out, recurse); // Throws
    ref_type parent_ref = pos;

    // Whether the resulting ref must be added to the previous parent
    // level, or reported as the final ref (through `is_last`) depends
    // on whether more children are going to be added, and on whether
    // a previous parent level already exists
    if (!is_last) {
        if (!m_prev_parent_level) {
            Allocator& alloc = m_main.get_alloc();
            size_t next_level_elems_per_child = m_max_elems_per_child;
            if (util::int_multiply_with_overflow_detect(next_level_elems_per_child,
                                                  REALM_MAX_BPNODE_SIZE))
                throw std::runtime_error("Overflow in number of elements per child");
            m_prev_parent_level.reset(new ParentLevel(alloc, m_out,
                                                      next_level_elems_per_child)); // Throws
        }
    }
    else if (!m_prev_parent_level) {
        *is_last = parent_ref;
        return;
    }
    m_prev_parent_level->add_child_ref(parent_ref, m_elems_in_parent,
                                       !m_is_on_general_form, is_last); // Throws

    // Clear the arrays in preperation for the next child
    if (!is_last) {
        if (m_offsets.is_attached())
            m_offsets.clear(); // Shallow
        m_main.clear(); // Shallow
        m_elems_in_parent = 0;
    }
}

} // anonymous namespace

struct BpTreeBase::WriteSliceHandler: public Array::VisitHandler {
public:
    WriteSliceHandler(size_t offset, size_t size, Allocator& alloc,
                      BpTreeBase::SliceHandler &slice_handler,
                      _impl::OutputStream& out) REALM_NOEXCEPT:
        m_begin(offset), m_end(offset + size),
        m_leaf_cache(alloc),
        m_slice_handler(slice_handler),
        m_out(out),
        m_tree_writer(out),
        m_top_ref(0)
    {
    }
    ~WriteSliceHandler() REALM_NOEXCEPT
    {
    }
    bool visit(const Array::NodeInfo& leaf_info) override
    {
        size_t size = leaf_info.m_size, pos;
        size_t leaf_begin = leaf_info.m_offset;
        size_t leaf_end   = leaf_begin + size;
        REALM_ASSERT_3(leaf_begin, <=, m_end);
        REALM_ASSERT_3(leaf_end, >=, m_begin);
        bool no_slicing = leaf_begin >= m_begin && leaf_end <= m_end;
        if (no_slicing) {
            // Warning: Initializing leaf as Array.
            m_leaf_cache.init_from_mem(leaf_info.m_mem);
            pos = m_leaf_cache.write(m_out); // Throws
        }
        else {
            // Slice the leaf
            Allocator& slice_alloc = Allocator::get_default();
            size_t begin = std::max(leaf_begin, m_begin);
            size_t end   = std::min(leaf_end,   m_end);
            size_t offset = begin - leaf_begin;
            size = end - begin;
            MemRef mem =
                m_slice_handler.slice_leaf(leaf_info.m_mem, offset, size, slice_alloc); // Throws
            Array slice(slice_alloc);
            _impl::DeepArrayDestroyGuard dg(&slice);
            // Warning: Initializing leaf as Array.
            slice.init_from_mem(mem);
            pos = slice.write(m_out); // Throws
        }
        ref_type ref = pos;
        ref_type* is_last = nullptr;
        if (leaf_end >= m_end)
            is_last = &m_top_ref;
        m_tree_writer.add_leaf_ref(ref, size, is_last); // Throws
        return !is_last;
    }
    ref_type get_top_ref() const REALM_NOEXCEPT
    {
        return m_top_ref;
    }
private:
    size_t m_begin, m_end;
    Array m_leaf_cache;
    BpTreeBase::SliceHandler& m_slice_handler;
    _impl::OutputStream& m_out;
    TreeWriter m_tree_writer;
    ref_type m_top_ref;
};

ref_type BpTreeBase::write_subtree(const Array& root, size_t slice_offset, size_t slice_size,
                                   size_t table_size, SliceHandler& handler, _impl::OutputStream& out)
{
    REALM_ASSERT(root.is_inner_bptree_node());

    size_t offset = slice_offset;
    if (slice_size == 0)
        offset = 0;
    // At this point we know that `offset` refers to an element that
    // exists in the tree (this is required by
    // Array::visit_bptree_leaves()). There are two cases to consider:
    // First, if `slice_size` is non-zero, then `offset` must already
    // refer to an existsing element. If `slice_size` is zero, then
    // `offset` has been set to zero at this point. Zero is the index
    // of an existing element, because the tree cannot be empty at
    // this point. This follows from the fact that the root is an
    // inner node, and that an inner node must contain at least one
    // element (invar:bptree-nonempty-inner +
    // invar:bptree-nonempty-leaf).
    WriteSliceHandler handler_2(offset, slice_size, root.get_alloc(), handler, out);
    const_cast<Array&>(root).visit_bptree_leaves(offset, table_size, handler_2); // Throws
    return handler_2.get_top_ref();
}
