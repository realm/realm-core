#include <cstdio>

#include <tightdb/index_string.hpp>

using namespace std;
using namespace tightdb;


Array* StringIndex::create_node(Allocator& alloc, bool is_leaf)
{
    Array::Type type = is_leaf ? Array::type_HasRefs : Array::type_InnerColumnNode;
    UniquePtr<Array> top(new Array(type, 0, 0, alloc));

    // Mark that this is part of index
    // (as opposed to columns under leafs)
    top->set_is_index_node(true);

    // Add subcolumns for leafs
    Array values(Array::type_Normal, 0, 0, alloc);
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit
    Array refs(Array::type_HasRefs, 0, 1, alloc);
    top->add(values.get_ref());
    top->add(refs.get_ref());
    values.set_parent(top.get(), 0);
    refs.set_parent(top.get(), 1);

    return top.release();
}

void StringIndex::set_target(void* target_column, StringGetter get_func) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(target_column);
    m_target_column = target_column;
    m_get_func      = get_func;
}

StringIndex::key_type StringIndex::GetLastKey() const
{
    Array offsets =  m_array->GetSubArray(0);
    return key_type(offsets.back());
}

void StringIndex::set(size_t ndx, StringData old_value, StringData new_value)
{
    bool is_last = true; // To avoid updating refs
    erase(ndx, old_value, is_last);
    insert(ndx, new_value, is_last);
}

void StringIndex::insert(size_t row_ndx, StringData value, bool is_last)
{
    // If it is last item in column, we don't have to update refs
    if (!is_last) UpdateRefs(row_ndx, 1);

    InsertWithOffset(row_ndx, 0, value);
}

void StringIndex::InsertWithOffset(size_t row_ndx, size_t offset, StringData value)
{
    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    TreeInsert(row_ndx, key, offset, value);
}

void StringIndex::InsertRowList(size_t ref, size_t offset, StringData value)
{
    TIGHTDB_ASSERT(m_array->is_leaf()); // only works in leafs

    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    size_t ins_pos = values.lower_bound_int(key);
    if (ins_pos == values.size()) {
        // When key is outside current range, we can just add it
        values.add(key);
        refs.add(ref);
        return;
    }

#ifdef TIGHTDB_DEBUG
    // Since we only use this for moving existing values to new
    // sub-indexes, there should never be an existing match.
    key_type k = key_type(values.get(ins_pos));
    TIGHTDB_ASSERT(k != key);
#endif

    // If key is not present we add it at the correct location
    values.insert(ins_pos, key);
    refs.insert(ins_pos, ref);
}

void StringIndex::TreeInsert(size_t row_ndx, key_type key, size_t offset, StringData value)
{
    const NodeChange nc = DoInsert(row_ndx, key, offset, value);
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
    TIGHTDB_ASSERT(false);
}

StringIndex::NodeChange StringIndex::DoInsert(size_t row_ndx, key_type key, size_t offset, StringData value)
{
    if (!root_is_leaf()) {
        // Get subnode table
        Array offsets = NodeGetOffsets();
        Array refs = NodeGetRefs();

        // Find the subnode containing the item
        size_t node_ndx = offsets.lower_bound_int(key);
        if (node_ndx == offsets.size()) {
            // node can never be empty, so try to fit in last item
            node_ndx = offsets.size()-1;
        }

        // Get sublist
        ref_type ref = refs.get_as_ref(node_ndx);
        StringIndex target(ref, &refs, node_ndx, m_target_column, m_get_func, m_array->get_alloc());

        // Insert item
        const NodeChange nc = target.DoInsert(row_ndx, key, offset, value);
        if (nc.type ==  NodeChange::none) {
            // update keys
            key_type last_key = target.GetLastKey();
            offsets.set(node_ndx, last_key);
            return NodeChange::none; // no new nodes
        }

        if (nc.type == NodeChange::insert_after) ++node_ndx;

        // If there is room, just update node directly
        if (offsets.size() < TIGHTDB_MAX_LIST_SIZE) {
            if (nc.type == NodeChange::split) NodeInsertSplit(node_ndx, nc.ref2);
            else NodeInsert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
            return NodeChange::none;
        }

        // Else create new node
        StringIndex new_node(inner_node_tag(), m_array->get_alloc());
        if (nc.type == NodeChange::split) {
            // update offset for left node
            key_type last_key = target.GetLastKey();
            offsets.set(node_ndx, last_key);

            new_node.NodeAddKey(nc.ref2);
            ++node_ndx;
        }
        else {
            new_node.NodeAddKey(nc.ref1);
        }

        switch (node_ndx) {
            case 0:             // insert before
                return NodeChange(NodeChange::insert_before, new_node.get_ref());
            case TIGHTDB_MAX_LIST_SIZE: // insert after
                if (nc.type == NodeChange::split)
                    return NodeChange(NodeChange::split, get_ref(), new_node.get_ref());
                else return NodeChange(NodeChange::insert_after, new_node.get_ref());
            default:            // split
                // Move items after split to new node
                size_t len = refs.size();
                for (size_t i = node_ndx; i < len; ++i) {
                    ref_type ref = refs.get_as_ref(i);
                    new_node.NodeAddKey(ref);
                }
                offsets.resize(node_ndx);
                refs.resize(node_ndx);
                return NodeChange(NodeChange::split, get_ref(), new_node.get_ref());
        }
    }
    else {
        // Is there room in the list?
        Array old_offsets = m_array->GetSubArray(0);
        size_t count = old_offsets.size();
        bool noextend = count >= TIGHTDB_MAX_LIST_SIZE;

        // See if we can fit entry into current leaf
        // Works if there is room or it can join existing entries
        if (LeafInsert(row_ndx, key, offset, value, noextend))
            return NodeChange::none;

        // Create new list for item (a leaf)
        StringIndex new_list(m_target_column, m_get_func, m_array->get_alloc());

        new_list.LeafInsert(row_ndx, key, offset, value);

        size_t ndx = old_offsets.lower_bound_int(key);

        // insert before
        if (ndx == 0) {
            return NodeChange(NodeChange::insert_before, new_list.get_ref());
        }

        // insert after
        if (ndx == old_offsets.size()) {
            return NodeChange(NodeChange::insert_after, new_list.get_ref());
        }

        // split
        Array old_refs = m_array->GetSubArray(1);
        Array new_offsets = new_list.m_array->GetSubArray(0);
        Array new_refs = new_list.m_array->GetSubArray(1);
        // Move items after split to new list
        for (size_t i = ndx; i < count; ++i) {
            int64_t v2 = old_offsets.get(i);
            int64_t v3 = old_refs.get(i);

            new_offsets.add(v2);
            new_refs.add(v3);
        }
        old_offsets.resize(ndx);
        old_refs.resize(ndx);

        return NodeChange(NodeChange::split, get_ref(), new_list.get_ref());
    }

    TIGHTDB_ASSERT(false); // never reach here
    return NodeChange::none;
}

void StringIndex::NodeInsertSplit(size_t ndx, size_t new_ref)
{
    TIGHTDB_ASSERT(!root_is_leaf());
    TIGHTDB_ASSERT(new_ref);

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();

    TIGHTDB_ASSERT(ndx < offsets.size());
    TIGHTDB_ASSERT(offsets.size() < TIGHTDB_MAX_LIST_SIZE);

    // Get sublists
    ref_type orig_ref = refs.get_as_ref(ndx);
    StringIndex orig_col(orig_ref, &refs, ndx, m_target_column, m_get_func, m_array->get_alloc());
    StringIndex new_col(new_ref, 0, 0, m_target_column, m_get_func, m_array->get_alloc());

    // Update original key
    key_type last_key = orig_col.GetLastKey();
    offsets.set(ndx, last_key);

    // Insert new ref
    key_type new_key = new_col.GetLastKey();
    offsets.insert(ndx+1, new_key);
    refs.insert(ndx+1, new_ref);
}

void StringIndex::NodeInsert(size_t ndx, size_t ref)
{
    TIGHTDB_ASSERT(ref);
    TIGHTDB_ASSERT(!root_is_leaf());

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();

    TIGHTDB_ASSERT(ndx <= offsets.size());
    TIGHTDB_ASSERT(offsets.size() < TIGHTDB_MAX_LIST_SIZE);

    StringIndex col(ref, 0, 0, m_target_column, m_get_func, m_array->get_alloc());
    key_type last_key = col.GetLastKey();

    offsets.insert(ndx, last_key);
    refs.insert(ndx, ref);
}

bool StringIndex::LeafInsert(size_t row_ndx, key_type key, size_t offset, StringData value, bool noextend)
{
    TIGHTDB_ASSERT(root_is_leaf());

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    size_t ins_pos = values.lower_bound_int(key);
    if (ins_pos == values.size()) {
        if (noextend) return false;

        // When key is outside current range, we can just add it
        values.add(key);
        int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
        refs.add(shifted);
        return true;
    }

    key_type k = key_type(values.get(ins_pos));

    // If key is not present we add it at the correct location
    if (k != key) {
        if (noextend) return false;

        values.insert(ins_pos, key);
        int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
        refs.insert(ins_pos, shifted);
        return true;
    }

    int64_t ref = refs.get(ins_pos);
    size_t sub_offset = offset + 4;
    Allocator& alloc = m_array->get_alloc();

    // Single match (lowest bit set indicates literal row_ndx)
    if (ref & 1) {
        size_t row_ndx2 = size_t(uint64_t(ref) >> 1);
        StringData v2 = get(row_ndx2);
        if (v2 == value) {
            // convert to list (in sorted order)
            Array row_list(Array::type_Normal, 0, 0, alloc);
            row_list.add(row_ndx < row_ndx2 ? row_ndx : row_ndx2);
            row_list.add(row_ndx < row_ndx2 ? row_ndx2 : row_ndx);
            refs.set(ins_pos, row_list.get_ref());
        }
        else {
            // convert to sub-index
            StringIndex sub_index(m_target_column, m_get_func, m_array->get_alloc());
            sub_index.InsertWithOffset(row_ndx2, sub_offset, v2);
            sub_index.InsertWithOffset(row_ndx, sub_offset, value);
            refs.set(ins_pos, sub_index.get_ref());
        }
        return true;
    }

    // If there alrady is a list of matches, we see if we fit there
    // or it has to be split into a sub-index
    if (!Array::is_index_node(to_ref(ref), alloc)) {
        Column sub(to_ref(ref), &refs, ins_pos, alloc);

        size_t r1 = size_t(sub.get(0));
        StringData v2 = get(r1);
        if (v2 ==  value) {
            // find insert position (the list has to be kept in sorted order)
            // In most cases we refs will be added to the end. So we test for that
            // first to see if we can avoid the binary search for insert position
            size_t last_ref = size_t(sub.Back());
            if (row_ndx > last_ref)
                sub.add(row_ndx);
            else {
                size_t pos = sub.lower_bound_int(row_ndx);
                if (pos == sub.size())
                    sub.add(row_ndx);
                else
                    sub.insert(pos, row_ndx);
            }
        }
        else {
            StringIndex sub_index(m_target_column, m_get_func, m_array->get_alloc());
            sub_index.InsertRowList(sub.get_ref(), sub_offset, v2);
            sub_index.InsertWithOffset(row_ndx, sub_offset, value);
            refs.set(ins_pos, sub_index.get_ref());
        }
        return true;
    }

    // sub-index
    StringIndex sub_index(to_ref(ref), &refs, ins_pos, m_target_column, m_get_func, alloc);
    sub_index.InsertWithOffset(row_ndx, sub_offset, value);

    return true;
}

size_t StringIndex::find_first(StringData value) const
{
    // Use direct access method
    return m_array->IndexStringFindFirst(value, m_target_column, m_get_func);
}

void StringIndex::find_all(Array& result, StringData value) const
{
    // Use direct access method
    return m_array->IndexStringFindAll(result, value, m_target_column, m_get_func);
}


FindRes StringIndex::find_all(StringData value, size_t& ref) const
{
    // Use direct access method
    return m_array->IndexStringFindAllNoCopy(value, ref, m_target_column, m_get_func);
}

size_t StringIndex::count(StringData value) const

{
    // Use direct access method
    return m_array->IndexStringCount(value, m_target_column, m_get_func);
}

void StringIndex::distinct(Array& result) const
{
    Array refs = m_array->GetSubArray(1);
    const size_t count = refs.size();
    Allocator& alloc = m_array->get_alloc();

    // Get first matching row for every key
    if (!m_array->is_leaf()) {
        for (size_t i = 0; i < count; ++i) {
            size_t ref = refs.get_as_ref(i);
            const StringIndex ndx(ref, 0, 0, m_target_column, m_get_func, alloc);
            ndx.distinct(result);
        }
    }
    else {
        for (size_t i = 0; i < count; ++i) {
            int64_t ref = refs.get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
               size_t r = to_size_t((uint64_t(ref) >> 1));
               result.add(r);
            }
            else {
                // A real ref either points to a list or a sub-index
                if (Array::is_index_node(to_ref(ref), alloc)) {
                    const StringIndex ndx(to_ref(ref), &refs, i, m_target_column, m_get_func, alloc);
                    ndx.distinct(result);
                }
                else {
                    const Column sub(to_ref(ref), &refs, i, alloc);
                    const size_t r = to_size_t(sub.get(0)); // get first match
                    result.add(r);
                }
            }
        }
    }
}

void StringIndex::UpdateRefs(size_t pos, int diff)
{
    TIGHTDB_ASSERT(diff == 1 || diff == -1); // only used by insert and delete

    Array refs = m_array->GetSubArray(1);
    const size_t count = refs.size();
    Allocator& alloc = m_array->get_alloc();

    if (!m_array->is_leaf()) {
        for (size_t i = 0; i < count; ++i) {
            size_t ref = refs.get_as_ref(i);
            StringIndex ndx(ref, &refs, i, m_target_column, m_get_func, alloc);
            ndx.UpdateRefs(pos, diff);
        }
    }
    else {
        for (size_t i = 0; i < count; ++i) {
            int64_t ref = refs.get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                //const size_t r = (ref >> 1); Please NEVER right shift signed values - result varies btw Intel/AMD
                size_t r = size_t(uint64_t(ref) >> 1);
                if (r >= pos) {
                    size_t adjusted_ref = ((r + diff) << 1)+1;
                    refs.set(i, adjusted_ref);
                }
            }
            else {
                // A real ref either points to a list or a sub-index
                if (Array::is_index_node(to_ref(ref), alloc)) {
                    StringIndex ndx(to_ref(ref), &refs, i, m_target_column, m_get_func, alloc);
                    ndx.UpdateRefs(pos, diff);
                }
                else {
                    Column sub(to_ref(ref), &refs, i, alloc);
                    sub.IncrementIf(pos, diff);
                }
            }
        }
    }
}

void StringIndex::clear()
{
    Array values = m_array->GetSubArray(0);
    Array refs   = m_array->GetSubArray(1);
    values.clear();
    refs.clear();
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit
}

void StringIndex::erase(size_t row_ndx, StringData value, bool is_last)
{
    DoDelete(row_ndx, value, 0);

    // Collapse top nodes with single item
    while (!root_is_leaf()) {
        Array refs = m_array->GetSubArray(1);
        TIGHTDB_ASSERT(refs.size() != 0); // node cannot be empty
        if (refs.size() > 1)
            break;

        ref_type ref = refs.get_as_ref(0);
        refs.erase(0); // avoid deleting subtree
        m_array->destroy();
        m_array->init_from_ref(ref);
        m_array->update_parent();
    }

    // If it is last item in column, we don't have to update refs
    if (!is_last)
        UpdateRefs(row_ndx, -1);
}

void StringIndex::DoDelete(size_t row_ndx, StringData value, size_t offset)
{
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);
    Allocator& alloc = m_array->get_alloc();

    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    const size_t pos = values.lower_bound_int(key);
    TIGHTDB_ASSERT(pos != values.size());

    if (!m_array->is_leaf()) {
        ref_type ref = refs.get_as_ref(pos);
        StringIndex node(ref, &refs, pos, m_target_column, m_get_func, alloc);
        node.DoDelete(row_ndx, value, offset);

        // Update the ref
        if (node.is_empty()) {
            values.erase(pos);
            refs.erase(pos);
            node.destroy();
        }
        else {
            key_type max_val = node.GetLastKey();
            if (max_val != key_type(values.get(pos)))
                values.set(pos, max_val);
        }
    }
    else {
        int64_t ref = refs.get(pos);
        if (ref & 1) {
            TIGHTDB_ASSERT((uint64_t(ref) >> 1) == uint64_t(row_ndx));
            values.erase(pos);
            refs.erase(pos);
        }
        else {
            // A real ref either points to a list or a sub-index
            if (Array::is_index_node(to_ref(ref), alloc)) {
                StringIndex subNdx(to_ref(ref), &refs, pos, m_target_column, m_get_func, alloc);
                subNdx.DoDelete(row_ndx, value, offset+4);

                if (subNdx.is_empty()) {
                    values.erase(pos);
                    refs.erase(pos);
                    subNdx.destroy();
                }
            }
            else {
                Column sub(to_ref(ref), &refs, pos, alloc);
                size_t r = sub.find_first(row_ndx);
                TIGHTDB_ASSERT(r != not_found);
                sub.erase(r);

                if (sub.is_empty()) {
                    values.erase(pos);
                    refs.erase(pos);
                    sub.destroy();
                }
            }
        }
    }
}

void StringIndex::update_ref(StringData value, size_t old_row_ndx, size_t new_row_ndx)
{
    do_update_ref(value, old_row_ndx, new_row_ndx, 0);
}

void StringIndex::do_update_ref(StringData value, size_t row_ndx, size_t new_row_ndx, size_t offset)
{
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);
    Allocator& alloc = m_array->get_alloc();

    // Create 4 byte index key
    key_type key = create_key(value.substr(offset));

    size_t pos = values.lower_bound_int(key);
    TIGHTDB_ASSERT(pos != values.size());

    if (!m_array->is_leaf()) {
        ref_type ref = refs.get_as_ref(pos);
        StringIndex node(ref, &refs, pos, m_target_column, m_get_func, alloc);
        node.do_update_ref(value, row_ndx, new_row_ndx, offset);
    }
    else {
        int64_t ref = refs.get(pos);
        if (ref & 1) {
            TIGHTDB_ASSERT((uint64_t(ref) >> 1) == uint64_t(row_ndx));
            size_t shifted = (new_row_ndx << 1) + 1; // shift to indicate literal
            refs.set(pos, shifted);
        }
        else {
            // A real ref either points to a list or a sub-index
            if (Array::is_index_node(to_ref(ref), alloc)) {
                StringIndex subNdx(to_ref(ref), &refs, pos, m_target_column, m_get_func, alloc);
                subNdx.do_update_ref(value, row_ndx, new_row_ndx, offset+4);
            }
            else {
                Column sub(to_ref(ref), &refs, pos, alloc);
                size_t r = sub.find_first(row_ndx);
                TIGHTDB_ASSERT(r != not_found);
                sub.set(r, new_row_ndx);
            }
        }
    }
}

bool StringIndex::is_empty() const
{
    Array values = m_array->GetSubArray(0);
    return values.is_empty();
}


void StringIndex::NodeAddKey(ref_type ref)
{
    TIGHTDB_ASSERT(ref);
    TIGHTDB_ASSERT(!root_is_leaf());

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();
    TIGHTDB_ASSERT(offsets.size() < TIGHTDB_MAX_LIST_SIZE);

    Array new_top(ref, 0, 0, m_array->get_alloc());
    Array new_offsets(new_top.get_as_ref(0), 0, 0,m_array->get_alloc());
    TIGHTDB_ASSERT(!new_offsets.is_empty());

    int64_t key = new_offsets.back();
    offsets.add(key);
    refs.add(ref);
}


#ifdef TIGHTDB_DEBUG

void StringIndex::verify_entries(const AdaptiveStringColumn& column) const
{
    Array results;

    size_t count = column.size();
    for (size_t i = 0; i < count; ++i) {
        StringData value = column.get(i);

        find_all(results, value);

        size_t ndx = results.find_first(i);
        TIGHTDB_ASSERT(ndx != not_found);
        results.clear();
    }
    results.destroy(); // clean-up
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

    out << "subgraph cluster_stringindex" << ref << " {" << endl;
    out << " label = \"StringIndex";
    if (0 < title.size()) out << "\\n'" << title << "'";
    out << "\";" << endl;

    array_to_dot(out, *m_array);

    out << "}" << endl;
}

void StringIndex::array_to_dot(ostream& out, const Array& array) const
{
    if (!array.has_refs()) {
        array.to_dot(out);
        return;
    }

    Array offsets = array.GetSubArray(0);
    Array refs    = array.GetSubArray(1);
    ref_type ref  = array.get_ref();

    if (array.is_leaf()) {
        out << "subgraph cluster_stringindex_leaf" << ref << " {" << endl;
        out << " label = \"Leaf\";" << endl;
    }
    else {
        out << "subgraph cluster_stringindex_node" << ref << " {" << endl;
        out << " label = \"Node\";" << endl;
    }

    array.to_dot(out);
    keys_to_dot(out, offsets, "keys");

    out << "}" << endl;

    refs.to_dot(out, "refs");

    size_t count = refs.size();
    for (size_t i = 0; i < count; ++i) {
        int64_t v = refs.get(i);
        if (v & 1) continue; // ignore literals

        Array r = refs.GetSubArray(i);
        array_to_dot(out, r);
    }
}

void StringIndex::keys_to_dot(ostream& out, const Array& array, StringData title) const
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
    if (!array.is_leaf()) out << "IsNode<BR/>";
    if (array.has_refs()) out << "HasRefs<BR/>";
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
    if (0 < title.size()) out << "}" << endl;

    out << endl;
}


#endif // TIGHTDB_DEBUG
