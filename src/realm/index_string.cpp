#include <cstdio>

#include <realm/exceptions.hpp>
#include <realm/index_string.hpp>

using namespace std;
using namespace realm;
using namespace realm::util;


namespace {

void get_child(Array& parent, size_t child_ref_ndx, Array& child) REALM_NOEXCEPT
{
    ref_type child_ref = parent.get_as_ref(child_ref_ndx);
    child.init_from_ref(child_ref);
    child.set_parent(&parent, child_ref_ndx);
}

} // anonymous namespace


// FIXME: Indexing strings containing zero bytes is currently broken because
// they result in non-equal strings having identical keys. Inserting such
// strings can corrupt the index data structures as a result, so we need to not
// allow users to do so until the index is fixed (which requires a breaking
// change to how values are indexed). Once the bug is fixed, validate_value()
// should be removed.
void StringIndex::validate_value(int64_t) const REALM_NOEXCEPT
{
    // no-op: All ints are valid
}

void StringIndex::validate_value(StringData str) const
{
    if (std::find(str.data(), str.data() + str.size(), '\0') != str.data() + str.size())
        throw std::invalid_argument("Cannot add string with embedded NULs to indexed column");
}


ArrayInteger* StringIndex::create_node(Allocator& alloc, bool is_leaf)
{
    Array::Type type = is_leaf ? Array::type_HasRefs : Array::type_InnerBptreeNode;
    std::unique_ptr<ArrayInteger> top(new ArrayInteger(alloc)); // Throws
    top->create(type); // Throws

    // Mark that this is part of index
    // (as opposed to columns under leaves)
    top->set_context_flag(true);

    // Add subcolumns for leaves
    ArrayInteger values(alloc);
    values.create(Array::type_Normal); // Throws
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit
    top->add(values.get_ref()); // first entry in refs points to offsets

    return top.release();
}


void StringIndex::set_target(void* target_column, StringGetter get_func) REALM_NOEXCEPT
{
    REALM_ASSERT(target_column);
    m_target_column = target_column;
    m_get_func      = get_func;
}


StringIndex::key_type StringIndex::GetLastKey() const
{
    ArrayInteger offsets(m_array->get_alloc());
    get_child(*m_array, 0, offsets);
    return key_type(offsets.back());
}


void StringIndex::insert_with_offset(size_t row_ndx, StringData value, size_t offset)
{
    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    TreeInsert(row_ndx, key, offset, value); // Throws
}


void StringIndex::InsertRowList(size_t ref, size_t offset, StringData value)
{
    REALM_ASSERT(!m_array->is_inner_bptree_node()); // only works in leaves

    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    // Get subnode table
    Allocator& alloc = m_array->get_alloc();
    ArrayInteger values(alloc);
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size()+1);

    size_t ins_pos = values.lower_bound(key);
    if (ins_pos == values.size()) {
        // When key is outside current range, we can just add it
        values.add(key);
        array()->add(ref);
        return;
    }

#ifdef REALM_DEBUG
    // Since we only use this for moving existing values to new
    // subindexes, there should never be an existing match.
    key_type k = key_type(values.get(ins_pos));
    REALM_ASSERT(k != key);
#endif

    // If key is not present we add it at the correct location
    values.insert(ins_pos, key);
    m_array->insert(ins_pos+1, ref);
}


void StringIndex::TreeInsert(size_t row_ndx, key_type key, size_t offset, StringData value)
{
    NodeChange nc = DoInsert(row_ndx, key, offset, value);
    switch (nc.type) {
        case NodeChange::none:
            return;
        case NodeChange::insert_before: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.NodeAddKey(nc.ref1);
            new_node.NodeAddKey(get_ref());
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
        case NodeChange::insert_after: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.NodeAddKey(get_ref());
            new_node.NodeAddKey(nc.ref1);
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
        case NodeChange::split: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.NodeAddKey(nc.ref1);
            new_node.NodeAddKey(nc.ref2);
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
    }
    REALM_ASSERT(false);
}


StringIndex::NodeChange StringIndex::DoInsert(size_t row_ndx, key_type key, size_t offset, StringData value)
{
    Allocator& alloc = m_array->get_alloc();
    if (!root_is_leaf()) {
        // Get subnode table
        ArrayInteger offsets(alloc);
        get_child(*m_array, 0, offsets);
        REALM_ASSERT(m_array->size() == offsets.size()+1);

        // Find the subnode containing the item
        size_t node_ndx = offsets.lower_bound(key);
        if (node_ndx == offsets.size()) {
            // node can never be empty, so try to fit in last item
            node_ndx = offsets.size()-1;
        }

        // Get sublist
        size_t refs_ndx = node_ndx+1; // first entry in refs points to offsets
        ref_type ref = m_array->get_as_ref(refs_ndx);
        StringIndex target(ref, m_array.get(), refs_ndx, m_target_column, m_get_func,
                           m_deny_duplicate_values, alloc);

        // Insert item
        NodeChange nc = target.DoInsert(row_ndx, key, offset, value);
        if (nc.type ==  NodeChange::none) {
            // update keys
            key_type last_key = target.GetLastKey();
            offsets.set(node_ndx, last_key);
            return NodeChange::none; // no new nodes
        }

        if (nc.type == NodeChange::insert_after) {
            ++node_ndx;
            ++refs_ndx;
        }

        // If there is room, just update node directly
        if (offsets.size() < REALM_MAX_BPNODE_SIZE) {
            if (nc.type == NodeChange::split) {
                NodeInsertSplit(node_ndx, nc.ref2);
            }
            else {
                NodeInsert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
            }
            return NodeChange::none;
        }

        // Else create new node
        StringIndex new_node(inner_node_tag(), alloc);
        if (nc.type == NodeChange::split) {
            // update offset for left node
            key_type last_key = target.GetLastKey();
            offsets.set(node_ndx, last_key);

            new_node.NodeAddKey(nc.ref2);
            ++node_ndx;
            ++refs_ndx;
        }
        else {
            new_node.NodeAddKey(nc.ref1);
        }

        switch (node_ndx) {
            case 0:             // insert before
                return NodeChange(NodeChange::insert_before, new_node.get_ref());
            case REALM_MAX_BPNODE_SIZE: // insert after
                if (nc.type == NodeChange::split)
                    return NodeChange(NodeChange::split, get_ref(), new_node.get_ref());
                return NodeChange(NodeChange::insert_after, new_node.get_ref());
            default:            // split
                // Move items after split to new node
                size_t len = m_array->size();
                for (size_t i = refs_ndx; i < len; ++i) {
                    ref_type ref = m_array->get_as_ref(i);
                    new_node.NodeAddKey(ref);
                }
                offsets.truncate(node_ndx);
                m_array->truncate(refs_ndx);
                return NodeChange(NodeChange::split, get_ref(), new_node.get_ref());
        }
    }
    else {
        // Is there room in the list?
        ArrayInteger old_offsets(m_array->get_alloc());
        get_child(*m_array, 0, old_offsets);
        REALM_ASSERT(m_array->size() == old_offsets.size()+1);

        size_t count = old_offsets.size();
        bool noextend = count >= REALM_MAX_BPNODE_SIZE;

        // See if we can fit entry into current leaf
        // Works if there is room or it can join existing entries
        if (LeafInsert(row_ndx, key, offset, value, noextend))
            return NodeChange::none;

        // Create new list for item (a leaf)
        StringIndex new_list(m_target_column, m_get_func, m_array->get_alloc());

        new_list.LeafInsert(row_ndx, key, offset, value);

        size_t ndx = old_offsets.lower_bound(key);

        // insert before
        if (ndx == 0)
            return NodeChange(NodeChange::insert_before, new_list.get_ref());

        // insert after
        if (ndx == old_offsets.size())
            return NodeChange(NodeChange::insert_after, new_list.get_ref());

        // split
        ArrayInteger new_offsets(alloc);
        get_child(*new_list.m_array, 0, new_offsets);
        // Move items after split to new list
        for (size_t i = ndx; i < count; ++i) {
            int64_t v2 = old_offsets.get(i);
            int64_t v3 = array()->get(i+1);

            new_offsets.add(v2);
            new_list.array()->add(v3);
        }
        old_offsets.truncate(ndx);
        m_array->truncate(ndx+1);

        return NodeChange(NodeChange::split, get_ref(), new_list.get_ref());
    }

    REALM_ASSERT(false); // never reach here
    return NodeChange::none;
}


void StringIndex::NodeInsertSplit(size_t ndx, size_t new_ref)
{
    REALM_ASSERT(!root_is_leaf());
    REALM_ASSERT(new_ref);

    Allocator& alloc = m_array->get_alloc();
    ArrayInteger offsets(alloc);
    get_child(*m_array, 0, offsets);

    REALM_ASSERT(m_array->size() == offsets.size()+1);
    REALM_ASSERT(ndx < offsets.size());
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE);

    // Get sublists
    size_t refs_ndx = ndx+1; // first entry in refs points to offsets
    ref_type orig_ref = m_array->get_as_ref(refs_ndx);
    StringIndex orig_col(orig_ref, m_array.get(), refs_ndx, m_target_column, m_get_func,
                         m_deny_duplicate_values, alloc);
    StringIndex new_col(new_ref, 0, 0, m_target_column, m_get_func,
                        m_deny_duplicate_values, alloc);

    // Update original key
    key_type last_key = orig_col.GetLastKey();
    offsets.set(ndx, last_key);

    // Insert new ref
    key_type new_key = new_col.GetLastKey();
    offsets.insert(ndx+1, new_key);
    m_array->insert(ndx+2, new_ref);
}


void StringIndex::NodeInsert(size_t ndx, size_t ref)
{
    REALM_ASSERT(ref);
    REALM_ASSERT(!root_is_leaf());

    Allocator& alloc = m_array->get_alloc();
    Array offsets(alloc);
    get_child(*m_array, 0, offsets);
    REALM_ASSERT(m_array->size() == offsets.size()+1);

    REALM_ASSERT(ndx <= offsets.size());
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE);

    StringIndex col(ref, 0, 0, m_target_column, m_get_func,
                    m_deny_duplicate_values, alloc);
    key_type last_key = col.GetLastKey();

    offsets.insert(ndx, last_key);
    m_array->insert(ndx+1, ref);
}


bool StringIndex::LeafInsert(size_t row_ndx, key_type key, size_t offset, StringData value, bool noextend)
{
    REALM_ASSERT(root_is_leaf());

    // Get subnode table
    Allocator& alloc = m_array->get_alloc();
    ArrayInteger values(alloc);
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size()+1);

    size_t ins_pos = values.lower_bound(key);
    if (ins_pos == values.size()) {
        if (noextend)
            return false;

        // When key is outside current range, we can just add it
        values.add(key);
        int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
        array()->add(shifted);
        return true;
    }

    size_t ins_pos_refs = ins_pos + 1; // first entry in refs points to offsets
    key_type k = key_type(values.get(ins_pos));

    // If key is not present we add it at the correct location
    if (k != key) {
        if (noextend)
            return false;

        values.insert(ins_pos, key);
        int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
        array()->insert(ins_pos_refs, shifted);
        return true;
    }

    // This leaf already has a slot for for the key

    int_fast64_t slot_value = array()->get(ins_pos+1);
    size_t suboffset = offset + 4;

    // Single match (lowest bit set indicates literal row_ndx)
    if (slot_value % 2 != 0) {
        size_t row_ndx2 = to_size_t(slot_value / 2);
        // for integer index, get_func fills out 'buffer' and makes str point at it
        char buffer[8];
        StringData v2 = get(row_ndx2, buffer);
        if (v2 == value) {
            if (m_deny_duplicate_values)
                throw LogicError(LogicError::unique_constraint_violation);
            // convert to list (in sorted order)
            ArrayInteger row_list(alloc);
            row_list.create(Array::type_Normal); // Throws
            row_list.add(row_ndx < row_ndx2 ? row_ndx : row_ndx2);
            row_list.add(row_ndx < row_ndx2 ? row_ndx2 : row_ndx);
            array()->set(ins_pos_refs, row_list.get_ref());
        }
        else {
            // convert to subindex
            StringIndex subindex(m_target_column, m_get_func, array()->get_alloc());
            subindex.insert_with_offset(row_ndx2, v2, suboffset);
            subindex.insert_with_offset(row_ndx, value, suboffset);
            array()->set(ins_pos_refs, subindex.get_ref());
        }
        return true;
    }

    // If there already is a list of matches, we see if we fit there
    // or it has to be split into a subindex
    ref_type ref = to_ref(slot_value);
    if (!Array::get_context_flag_from_header(alloc.translate(ref))) {
        Column sub(alloc, ref); // Throws
        sub.set_parent(array(), ins_pos_refs);

        size_t r1 = to_size_t(sub.get(0));
        // for integer index, get_func fills out 'buffer' and makes str point at it
        char buffer[8];
        StringData v2 = get(r1, buffer);
        if (v2 == value) {
            if (m_deny_duplicate_values)
                throw LogicError(LogicError::unique_constraint_violation);
            // find insert position (the list has to be kept in sorted order)
            // In most cases we refs will be added to the end. So we test for that
            // first to see if we can avoid the binary search for insert position
            size_t last_ref = size_t(sub.back());
            if (row_ndx > last_ref) {
                sub.add(row_ndx);
            }
            else {
                size_t pos = sub.lower_bound_int(row_ndx);
                if (pos == sub.size()) {
                    sub.add(row_ndx);
                }
                else {
                    sub.insert(pos, row_ndx);
                }
            }
        }
        else {
            StringIndex subindex(m_target_column, m_get_func, array()->get_alloc());
            subindex.InsertRowList(sub.get_ref(), suboffset, v2);
            subindex.insert_with_offset(row_ndx, value, suboffset);
            array()->set(ins_pos_refs, subindex.get_ref());
        }
        return true;
    }

    // subindex
    StringIndex subindex(ref, array(), ins_pos_refs, m_target_column, m_get_func,
                         m_deny_duplicate_values, alloc);
    subindex.insert_with_offset(row_ndx, value, suboffset);

    return true;
}

void StringIndex::distinct(Column& result) const
{
    Allocator& alloc = m_array->get_alloc();
    const size_t count = m_array->size();

    // Get first matching row for every key
    if (array()->is_inner_bptree_node()) {
        for (size_t i = 1; i < count; ++i) {
            size_t ref = array()->get_as_ref(i);
            StringIndex ndx(ref, 0, 0, m_target_column, m_get_func,
                            m_deny_duplicate_values, alloc);
            ndx.distinct(result);
        }
    }
    else {
        for (size_t i = 1; i < count; ++i) {
            int64_t ref = array()->get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                size_t r = to_size_t((uint64_t(ref) >> 1));
                result.add(r);
            }
            else {
                // A real ref either points to a list or a subindex
                if (Array::get_context_flag_from_header(alloc.translate(to_ref(ref)))) {
                    StringIndex ndx(to_ref(ref), m_array.get(), i, m_target_column, m_get_func,
                                    m_deny_duplicate_values, alloc);
                    ndx.distinct(result);
                }
                else {
                    Column sub(alloc, to_ref(ref)); // Throws
                    size_t r = to_size_t(sub.get(0)); // get first match
                    result.add(r);
                }
            }
        }
    }
}

void StringIndex::adjust_row_indexes(size_t min_row_ndx, int diff)
{
    REALM_ASSERT(diff == 1 || diff == -1); // only used by insert and delete

    Allocator& alloc = array()->get_alloc();
    const size_t count = array()->size();

    if (array()->is_inner_bptree_node()) {
        for (size_t i = 1; i < count; ++i) {
            size_t ref = array()->get_as_ref(i);
            StringIndex ndx(ref, array(), i, m_target_column, m_get_func,
                            m_deny_duplicate_values, alloc);
            ndx.adjust_row_indexes(min_row_ndx, diff);
        }
    }
    else {
        for (size_t i = 1; i < count; ++i) {
            int64_t ref = array()->get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                size_t r = size_t(uint64_t(ref) >> 1);
                if (r >= min_row_ndx) {
                    size_t adjusted_ref = ((r + diff) << 1)+1;
                    array()->set(i, adjusted_ref);
                }
            }
            else {
                // A real ref either points to a list or a subindex
                if (Array::get_context_flag_from_header(alloc.translate(to_ref(ref)))) {
                    StringIndex ndx(to_ref(ref), array(), i, m_target_column, m_get_func,
                                    m_deny_duplicate_values, alloc);
                    ndx.adjust_row_indexes(min_row_ndx, diff);
                }
                else {
                    Column sub(alloc, to_ref(ref)); // Throws
                    sub.set_parent(array(), i);
                    sub.adjust_ge(min_row_ndx, diff);
                }
            }
        }
    }
}


void StringIndex::clear()
{
    Array values(m_array->get_alloc());
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size()+1);

    values.clear();
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit

    size_t size = 1;
    m_array->truncate_and_destroy_children(size); // Don't touch `values` array

    m_array->set_type(Array::type_HasRefs);
}


void StringIndex::DoDelete(size_t row_ndx, StringData value, size_t offset)
{
    Allocator& alloc = array()->get_alloc();
    ArrayInteger values(alloc);
    get_child(*array(), 0, values);
    REALM_ASSERT(array()->size() == values.size()+1);

    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    const size_t pos = values.lower_bound(key);
    const size_t pos_refs = pos + 1; // first entry in refs points to offsets
    REALM_ASSERT(pos != values.size());

    if (array()->is_inner_bptree_node()) {
        ref_type ref = array()->get_as_ref(pos_refs);
        StringIndex node(ref, array(), pos_refs, m_target_column, m_get_func,
                         m_deny_duplicate_values, alloc);
        node.DoDelete(row_ndx, value, offset);

        // Update the ref
        if (node.is_empty()) {
            values.erase(pos);
            array()->erase(pos_refs);
            node.destroy();
        }
        else {
            key_type max_val = node.GetLastKey();
            if (max_val != key_type(values.get(pos)))
                values.set(pos, max_val);
        }
    }
    else {
        int64_t ref = array()->get(pos_refs);
        if (ref & 1) {
            REALM_ASSERT((uint64_t(ref) >> 1) == uint64_t(row_ndx));
            values.erase(pos);
            array()->erase(pos_refs);
        }
        else {
            // A real ref either points to a list or a subindex
            if (Array::get_context_flag_from_header(alloc.translate(to_ref(ref)))) {
                StringIndex subindex(to_ref(ref), array(), pos_refs, m_target_column, m_get_func,
                                     m_deny_duplicate_values, alloc);
                subindex.DoDelete(row_ndx, value, offset+4);

                if (subindex.is_empty()) {
                    values.erase(pos);
                    array()->erase(pos_refs);
                    subindex.destroy();
                }
            }
            else {
                Column sub(alloc, to_ref(ref)); // Throws
                sub.set_parent(array(), pos_refs);
                size_t r = sub.find_first(row_ndx);
                REALM_ASSERT(r != not_found);
                bool is_last = r == sub.size() - 1;
                sub.erase(r, is_last);

                if (sub.size() == 0) {
                    values.erase(pos);
                    array()->erase(pos_refs);
                    sub.destroy();
                }
            }
        }
    }
}


void StringIndex::do_update_ref(StringData value, size_t row_ndx, size_t new_row_ndx, size_t offset)
{
    Allocator& alloc = array()->get_alloc();
    ArrayInteger values(alloc);
    get_child(*array(), 0, values);
    REALM_ASSERT(array()->size() == values.size()+1);

    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    size_t pos = values.lower_bound(key);
    size_t pos_refs = pos + 1; // first entry in refs points to offsets
    REALM_ASSERT(pos != values.size());

    if (array()->is_inner_bptree_node()) {
        ref_type ref = array()->get_as_ref(pos_refs);
        StringIndex node(ref, array(), pos_refs, m_target_column, m_get_func,
                         m_deny_duplicate_values, alloc);
        node.do_update_ref(value, row_ndx, new_row_ndx, offset);
    }
    else {
        int64_t ref = array()->get(pos_refs);
        if (ref & 1) {
            REALM_ASSERT((uint64_t(ref) >> 1) == uint64_t(row_ndx));
            size_t shifted = (new_row_ndx << 1) + 1; // shift to indicate literal
            array()->set(pos_refs, shifted);
        }
        else {
            // A real ref either points to a list or a subindex
            if (Array::get_context_flag_from_header(alloc.translate(to_ref(ref)))) {
                StringIndex subindex(to_ref(ref), array(), pos_refs, m_target_column, m_get_func,
                                     m_deny_duplicate_values, alloc);
                subindex.do_update_ref(value, row_ndx, new_row_ndx, offset+4);
            }
            else {
                Column sub(alloc, to_ref(ref)); // Throws
                sub.set_parent(array(), pos_refs);
                size_t r = sub.find_first(row_ndx);
                REALM_ASSERT(r != not_found);
                sub.set(r, new_row_ndx);
            }
        }
    }
}


namespace {

bool has_duplicate_values(const Array& node) REALM_NOEXCEPT
{
    Allocator& alloc = node.get_alloc();
    Array child(alloc);
    size_t n = node.size();
    REALM_ASSERT(n >= 1);
    if (node.is_inner_bptree_node()) {
        // Inner node
        for (size_t i = 1; i < n; ++i) {
            ref_type ref = node.get_as_ref(i);
            child.init_from_ref(ref);
            if (has_duplicate_values(child))
                return true;
        }
        return false;
    }

    // Leaf node
    for (size_t i = 1; i < n; ++i) {
        int_fast64_t value = node.get(i);
        bool is_single_row_index = value % 2 != 0;
        if (is_single_row_index)
            continue;

        ref_type ref = to_ref(value);
        child.init_from_ref(ref);

        bool is_subindex = child.get_context_flag();
        if (is_subindex) {
            if (has_duplicate_values(child))
                return true;
            continue;
        }

        // Child is root of B+-tree of row indexes
        size_t num_rows = child.is_inner_bptree_node() ? child.get_bptree_size() : child.size();
        if (num_rows > 1)
            return true;
    }

    return false;
}

} // anonymous namespace


bool StringIndex::has_duplicate_values() const REALM_NOEXCEPT
{
    return ::has_duplicate_values(*m_array);
}


bool StringIndex::is_empty() const
{
    return m_array->size() == 1; // first entry in refs points to offsets
}


void StringIndex::NodeAddKey(ref_type ref)
{
    REALM_ASSERT(ref);
    REALM_ASSERT(!root_is_leaf());

    Allocator& alloc = m_array->get_alloc();
    ArrayInteger offsets(alloc);
    get_child(*m_array, 0, offsets);
    REALM_ASSERT(m_array->size() == offsets.size()+1);
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE+1);

    Array new_top(alloc);
    ArrayInteger new_offsets(alloc);
    new_top.init_from_ref(ref);
    new_offsets.init_from_ref(new_top.get_as_ref(0));
    REALM_ASSERT(!new_offsets.is_empty());

    int64_t key = new_offsets.back();
    offsets.add(key);
    array()->add(ref);
}


#ifdef REALM_DEBUG

void StringIndex::Verify() const
{
    m_array->Verify();

    // FIXME: Extend verification along the lines of Column::Verify().
}


void StringIndex::verify_entries(const AdaptiveStringColumn& column) const
{
    Allocator& alloc = Allocator::get_default();
    ref_type results_ref = Column::create(alloc); // Throws
    Column results(alloc, results_ref); // Throws

    size_t count = column.size();
    for (size_t i = 0; i < count; ++i) {
        StringData value = column.get(i);

        find_all(results, value);

        size_t ndx = results.find_first(i);
        REALM_ASSERT(ndx != not_found);
        results.clear();
    }
    results.destroy(); // clean-up
}


void StringIndex::dump_node_structure(const Array& node, ostream& out, int level)
{
    int indent = level * 2;
    Allocator& alloc = node.get_alloc();
    Array subnode(alloc);

    size_t node_size = node.size();
    REALM_ASSERT(node_size >= 1);

    bool node_is_leaf = !node.is_inner_bptree_node();
    if (node_is_leaf) {
        out << setw(indent) << "" << "Leaf (B+ tree) (ref: "<<node.get_ref()<<")\n";
    }
    else {
        out << setw(indent) << "" << "Inner node (B+ tree) (ref: "<<node.get_ref()<<")\n";
    }

    subnode.init_from_ref(to_ref(node.front()));
    out << setw(indent) << "" << "  Keys (keys_ref: "
        ""<<subnode.get_ref()<<", ";
    if (subnode.is_empty()) {
        out << "no keys";
    }
    else {
        out << "keys: ";
        for (size_t i = 0; i != subnode.size(); ++i) {
            if (i != 0)
                out << ", ";
            out << subnode.get(i);
        }
    }
    out << ")\n";

    if (node_is_leaf) {
        for (size_t i = 1; i != node_size; ++i) {
            int_fast64_t value = node.get(i);
            bool is_single_row_index = value % 2 != 0;
            if (is_single_row_index) {
                out << setw(indent) << "" << "  Single row index (value: "<<(value/2)<<")\n";
                continue;
            }
            subnode.init_from_ref(to_ref(value));
            bool is_subindex = subnode.get_context_flag();
            if (is_subindex) {
                out << setw(indent) << "" << "  Subindex\n";
                dump_node_structure(subnode, out, level+2);
                continue;
            }
            out << setw(indent) << "" << "  List of row indexes\n";
            Column::dump_node_structure(subnode, out, level+2);
        }
        return;
    }


    size_t num_children = node_size - 1;
    size_t child_ref_begin = 1;
    size_t child_ref_end = 1 + num_children;
    for (size_t i = child_ref_begin; i != child_ref_end; ++i) {
        subnode.init_from_ref(node.get_as_ref(i));
        dump_node_structure(subnode, out, level+1);
    }
}


void StringIndex::do_dump_node_structure(std::ostream& out, int level) const
{
    dump_node_structure(*m_array, out, level);
}


void StringIndex::to_dot(ostream& out, StringData title) const
{
    out << "digraph G {" << endl;

    to_dot_2(out, title);

    out << "}" << endl;
}


void StringIndex::to_dot_2(ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_string_index" << ref << " {" << endl;
    out << " label = \"String index";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;

    array_to_dot(out, *m_array);

    out << "}" << endl;
}


void StringIndex::array_to_dot(ostream& out, const Array& array)
{
    if (!array.get_context_flag()) {
        Column col(array.get_alloc(), array.get_ref()); // Throws
        col.set_parent(array.get_parent(), array.get_ndx_in_parent());
        col.to_dot(out, "ref_list");
        return;
    }

    Allocator& alloc = array.get_alloc();
    ArrayInteger offsets(alloc);
    get_child(const_cast<Array&>(array), 0, offsets);
    REALM_ASSERT(array.size() == offsets.size()+1);
    ref_type ref  = array.get_ref();

    if (array.is_inner_bptree_node()) {
        out << "subgraph cluster_string_index_inner_node" << ref << " {" << endl;
        out << " label = \"Inner node\";" << endl;
    }
    else {
        out << "subgraph cluster_string_index_leaf" << ref << " {" << endl;
        out << " label = \"Leaf\";" << endl;
    }

    array.to_dot(out);
    keys_to_dot(out, offsets, "keys");

    out << "}" << endl;

    size_t count = array.size();
    for (size_t i = 1; i < count; ++i) {
        int64_t v = array.get(i);
        if (v & 1)
            continue; // ignore literals

        Array r(alloc);
        get_child(const_cast<Array&>(array), i, r);
        array_to_dot(out, r);
    }
}


void StringIndex::keys_to_dot(ostream& out, const Array& array, StringData title)
{
    ref_type ref = array.get_ref();

    if (0 < title.size()) {
        out << "subgraph cluster_" << ref << " {" << endl;
        out << " label = \"" << title << "\";" << endl;
        out << " color = white;" << endl;
    }

    out << "n" << hex << ref << dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << hex << ref << dec << "<BR/>";
    if (array.is_inner_bptree_node())
        out << "IsNode<BR/>";
    if (array.has_refs())
        out << "HasRefs<BR/>";
    out << "</FONT></TD>" << endl;

    // Values
    size_t count = array.size();
    for (size_t i = 0; i < count; ++i) {
        uint64_t v =  array.get(i); // Never right shift signed values

        char str[5] = "\0\0\0\0";
        str[3] = char(v & 0xFF);
        str[2] = char((v >> 8) & 0xFF);
        str[1] = char((v >> 16) & 0xFF);
        str[0] = char((v >> 24) & 0xFF);
        const char* s = str;

        out << "<TD>" << s << "</TD>" << endl;
    }

    out << "</TR></TABLE>>];" << endl;
    if (0 < title.size())
        out << "}" << endl;

    array.to_dot_parent_edge(out);

    out << endl;
}


#endif // REALM_DEBUG
