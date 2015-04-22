#include <realm/bptree.hpp>

using namespace realm;

void BpTreeBase::replace_root(std::unique_ptr<Array> leaf)
{
    ArrayParent* parent = m_root->get_parent();
    std::size_t ndx_in_parent = m_root->get_ndx_in_parent();
    leaf->set_parent(parent, ndx_in_parent);
    leaf->update_parent(); // Throws
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
    new_root->set_parent(orig_root->get_parent(), orig_root->get_ndx_in_parent());
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
