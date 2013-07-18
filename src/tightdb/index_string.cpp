#include <cstdio>

#include <tightdb/index_string.hpp>

using namespace std;
using namespace tightdb;

namespace {

int32_t create_key(const char* begin, const char* end)
{
    // Create 4 byte index key
    // (encoded like this to allow literal comparisons
    // independently of endianness)
    int32_t key = 0;
    if (begin != end) key  = (int32_t(*begin++) << 24);
    if (begin != end) key |= (int32_t(*begin++) << 16);
    if (begin != end) key |= (int32_t(*begin++) << 8);
    if (begin != end) key |=  int32_t(*begin++);
    return key;
}

} // anonymous namespace


StringIndex::StringIndex(void* target_column, StringGetter get_func, Allocator& alloc):
    Column(Array::type_HasRefs, 0, 0, alloc), m_target_column(target_column), m_get_func(get_func)
{
    Create();
}

StringIndex::StringIndex(Array::Type type, Allocator& alloc):
    Column(type, 0, 0, alloc), m_target_column(0), m_get_func(0)
{
    TIGHTDB_ASSERT(type == Array::type_InnerColumnNode); // only used for node creation at this point

    // Mark that this is part of index
    // (as opposed to columns under leafs)
    m_array->SetIsIndexNode(true);

    // no need to call create as sub-arrays have been created by column constructor
}

StringIndex::StringIndex(ref_type ref, ArrayParent* parent, size_t pndx, void* target_column, StringGetter get_func, Allocator& alloc)
: Column(ref, parent, pndx, alloc), m_target_column(target_column), m_get_func(get_func)
{
    TIGHTDB_ASSERT(Array::is_index_node(ref, alloc));
}

void StringIndex::Create()
{
    // Mark that this is part of index
    // (as opposed to columns under leafs)
    m_array->SetIsIndexNode(true);

    // Add subcolumns for leafs
    Allocator& alloc = m_array->get_alloc();
    Array values(Array::type_Normal, NULL, 0, alloc);
    Array refs(Array::type_HasRefs, NULL, 1, alloc);
    m_array->add(values.get_ref());
    m_array->add(refs.get_ref());
    values.set_parent(m_array, 0);
    refs.set_parent(m_array, 1);
}

void StringIndex::SetTarget(void* target_column, StringGetter get_func)
{
    TIGHTDB_ASSERT(target_column);
    m_target_column = target_column;
    m_get_func      = get_func;
}

int32_t StringIndex::GetLastKey() const
{
    const Array offsets =  m_array->GetSubArray(0);
    return int32_t(offsets.back());
}

void StringIndex::set(size_t ndx, StringData oldValue, StringData newValue)
{
    erase(ndx, oldValue, true); // set isLast to avoid updating refs
    insert(ndx, newValue, true); // set isLast to avoid updating refs
}

void StringIndex::insert(size_t row_ndx, StringData value, bool isLast)
{
    // If it is last item in column, we don't have to update refs
    if (!isLast) UpdateRefs(row_ndx, 1);

    InsertWithOffset(row_ndx, 0, value);
}

void StringIndex::InsertWithOffset(size_t row_ndx, size_t offset, StringData value)
{
    // Create 4 byte index key
    const char* v = value.data() + offset;
    int32_t key = create_key(v, value.data() + value.size());

    TreeInsert(row_ndx, key, offset, value);
}

void StringIndex::InsertRowList(size_t ref, size_t offset, StringData value)
{
    TIGHTDB_ASSERT(m_array->is_leaf()); // only works in leafs

    // Create 4 byte index key
    const char* const v = value.data() + offset;
    const int32_t key = create_key(v, value.data() + value.size());

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = values.FindPos2(key);

    if (ins_pos == not_found) {
        // When key is outside current range, we can just add it
        values.add(key);
        refs.add(ref);
        return;
    }

#ifdef TIGHTDB_DEBUG
    // Since we only use this for moving existing values to new
    // sub-indexes, there should never be an existing match.
    int32_t k = int32_t(values.get(ins_pos));
    TIGHTDB_ASSERT(k != key);
#endif

    // If key is not present we add it at the correct location
    values.insert(ins_pos, key);
    refs.insert(ins_pos, ref);
}

void StringIndex::TreeInsert(size_t row_ndx, int32_t key, size_t offset, StringData value)
{
    const NodeChange nc = DoInsert(row_ndx, key, offset, value);
    switch (nc.type) {
        case NodeChange::none:
            return;
        case NodeChange::insert_before: {
            StringIndex newNode(Array::type_InnerColumnNode, m_array->get_alloc());
            newNode.NodeAddKey(nc.ref1);
            newNode.NodeAddKey(get_ref());
            update_ref(newNode.get_ref());
            return;
        }
        case NodeChange::insert_after: {
            StringIndex newNode(Array::type_InnerColumnNode, m_array->get_alloc());
            newNode.NodeAddKey(get_ref());
            newNode.NodeAddKey(nc.ref1);
            update_ref(newNode.get_ref());
            return;
        }
        case NodeChange::split: {
            StringIndex newNode(Array::type_InnerColumnNode, m_array->get_alloc());
            newNode.NodeAddKey(nc.ref1);
            newNode.NodeAddKey(nc.ref2);
            update_ref(newNode.get_ref());
            return;
        }
    }
    TIGHTDB_ASSERT(false);
}

Column::NodeChange StringIndex::DoInsert(size_t row_ndx, int32_t key, size_t offset, StringData value)
{
    if (!root_is_leaf()) {
        // Get subnode table
        Array offsets = NodeGetOffsets();
        Array refs = NodeGetRefs();

        // Find the subnode containing the item
        size_t node_ndx = offsets.FindPos2(key);
        if (node_ndx == size_t(-1)) {
            // node can never be empty, so try to fit in last item
            node_ndx = offsets.size()-1;
        }

        // Get sublist
        const size_t ref = refs.get_as_ref(node_ndx);
        StringIndex target(ref, &refs, node_ndx, m_target_column, m_get_func, m_array->get_alloc());

        // Insert item
        const NodeChange nc = target.DoInsert(row_ndx, key, offset, value);
        if (nc.type ==  NodeChange::none) {
            // update keys
            int64_t lastKey = target.GetLastKey();
            offsets.set(node_ndx, lastKey);
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
        StringIndex newNode(Array::type_InnerColumnNode, m_array->get_alloc());
        if (nc.type == NodeChange::split) {
            // update offset for left node
            int32_t lastKey = target.GetLastKey();
            offsets.set(node_ndx, lastKey);

            newNode.NodeAddKey(nc.ref2);
            ++node_ndx;
        }
        else newNode.NodeAddKey(nc.ref1);

        switch (node_ndx) {
            case 0:             // insert before
                return NodeChange(NodeChange::insert_before, newNode.get_ref());
            case TIGHTDB_MAX_LIST_SIZE: // insert after
                if (nc.type == NodeChange::split)
                    return NodeChange(NodeChange::split, get_ref(), newNode.get_ref());
                else return NodeChange(NodeChange::insert_after, newNode.get_ref());
            default:            // split
                // Move items after split to new node
                const size_t len = refs.size();
                for (size_t i = node_ndx; i < len; ++i) {
                    size_t ref = refs.get_as_ref(i);
                    newNode.NodeAddKey(ref);
                }
                offsets.resize(node_ndx);
                refs.resize(node_ndx);
                return NodeChange(NodeChange::split, get_ref(), newNode.get_ref());
        }
    }
    else {
        // Is there room in the list?
        Array old_offsets = m_array->GetSubArray(0);
        const size_t count = old_offsets.size();
        const bool noextend = count >= TIGHTDB_MAX_LIST_SIZE;

        // See if we can fit entry into current leaf
        // Works if there is room or it can join existing entries
        if (LeafInsert(row_ndx, key, offset, value, noextend))
            return NodeChange::none;

        // Create new list for item
        StringIndex newList(m_target_column, m_get_func, m_array->get_alloc());

        newList.LeafInsert(row_ndx, key, offset, value);

        const size_t ndx = old_offsets.FindPos2(key);

        switch (ndx) {
            case 0:             // insert before
                return NodeChange(NodeChange::insert_before, newList.get_ref());
            case size_t(-1): // insert after
                return NodeChange(NodeChange::insert_after, newList.get_ref());
            default: // split
            {
                Array old_refs = m_array->GetSubArray(1);
                Array new_offsets = newList.m_array->GetSubArray(0);
                Array new_refs = newList.m_array->GetSubArray(1);
                // Move items after split to new list
                for (size_t i = ndx; i < count; ++i) {
                    int64_t v2 = old_offsets.get(i);
                    int64_t v3 = old_refs.get(i);

                    new_offsets.add(v2);
                    new_refs.add(v3);
                }
                old_offsets.resize(ndx);
                old_refs.resize(ndx);

                return NodeChange(NodeChange::split, get_ref(), newList.get_ref());
            }
        }
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
    const size_t orig_ref = refs.get_as_ref(ndx);
    const StringIndex orig_col(orig_ref, &refs, ndx, m_target_column, m_get_func, m_array->get_alloc());
    const StringIndex new_col(new_ref, NULL, 0, m_target_column, m_get_func, m_array->get_alloc());

    // Update original key
    const int64_t lastKey = orig_col.GetLastKey();
    offsets.set(ndx, lastKey);

    // Insert new ref
    const size_t newKey = new_col.GetLastKey();
    offsets.insert(ndx+1, newKey);
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

    const StringIndex col(ref, NULL, 0, m_target_column, m_get_func, m_array->get_alloc());
    const int64_t lastKey = col.GetLastKey();

    offsets.insert(ndx, lastKey);
    refs.insert(ndx, ref);
}

bool StringIndex::LeafInsert(size_t row_ndx, int32_t key, size_t offset, StringData value, bool noextend)
{
    TIGHTDB_ASSERT(root_is_leaf());

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = values.FindPos2(key);

    if (ins_pos == not_found) {
        if (noextend) return false;

        // When key is outside current range, we can just add it
        values.add(key);
        size_t shifted = (row_ndx << 1) + 1; // shift to indicate literal
        refs.add(shifted);
        return true;
    }

    const int32_t k = int32_t(values.get(ins_pos));

    // If key is not present we add it at the correct location
    if (k != key) {
        if (noextend) return false;

        values.insert(ins_pos, key);
        size_t shifted = (row_ndx << 1) + 1; // shift to indicate literal
        refs.insert(ins_pos, shifted);
        return true;
    }

    const size_t ref = refs.get_as_ref(ins_pos);
    const size_t sub_offset = offset + 4;
    Allocator& alloc = m_array->get_alloc();

    // Single match (lowest bit set indicates literal row_ndx)
    if (ref & 1) {
        const size_t row_ndx2 = ref >> 1;
        StringData v2 = get(row_ndx2);
        if (v2 == value) {
            // convert to list (in sorted order)
            Array row_list(Array::type_Normal, NULL, 0, alloc);
            row_list.add(row_ndx < row_ndx2 ? row_ndx : row_ndx2);
            row_list.add(row_ndx < row_ndx2 ? row_ndx2 : row_ndx);
            refs.set(ins_pos, row_list.get_ref());
        }
        else {
            // convert to sub-index
            StringIndex sub_index(m_target_column, m_get_func, alloc);
            sub_index.InsertWithOffset(row_ndx2, sub_offset, v2);
            sub_index.InsertWithOffset(row_ndx, sub_offset, value);
            refs.set(ins_pos, sub_index.get_ref());
        }
        return true;
    }

    // If there alrady is a list of matches, we see if we fit there
    // or it has to be split into a sub-index
    if (!Array::is_index_node(ref, alloc)) {
        Column sub(ref, &refs, ins_pos, alloc);

        const size_t r1 = size_t(sub.get(0));
        StringData v2 = get(r1);
        if (v2 ==  value) {
            // find insert position (the list has to be kept in sorted order)
            // In most cases we refs will be added to the end. So we test for that
            // first to see if we can avoid the binary search for insert position
            size_t lastRef = size_t(sub.Back());
            if (row_ndx > lastRef)
                sub.add(row_ndx);
            else {
                size_t pos = sub.find_pos2(row_ndx);
                if (pos == not_found)
                    sub.add(row_ndx);
                else
                    sub.insert(pos, row_ndx);
            }
        }
        else {
            StringIndex sub_index(m_target_column, m_get_func, alloc);
            sub_index.InsertRowList(sub.get_ref(), sub_offset, v2);
            sub_index.InsertWithOffset(row_ndx, sub_offset, value);
            refs.set(ins_pos, sub_index.get_ref());
        }
        return true;
    }

    // sub-index
    StringIndex sub_index(ref, &refs, ins_pos, m_target_column, m_get_func, alloc);
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
            const StringIndex ndx(ref, NULL, 0, m_target_column, m_get_func, alloc);
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
            size_t ref = refs.get_as_ref(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                //const size_t r = (ref >> 1); Please NEVER right shift signed values - result varies btw Intel/AMD
                size_t r = ref >> 1;
                if (r >= pos) {
                    size_t adjusted_ref = ((r + diff) << 1)+1;
                    refs.set(i, adjusted_ref);
                }
            }
            else {
                // A real ref either points to a list or a sub-index
                if (Array::is_index_node(ref, alloc)) {
                    StringIndex ndx(ref, &refs, i, m_target_column, m_get_func, alloc);
                    ndx.UpdateRefs(pos, diff);
                }
                else {
                    Column sub(ref, &refs, i, alloc);
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
}

void StringIndex::erase(size_t row_ndx, StringData value, bool isLast)
{
    DoDelete(row_ndx, value, 0);

    // Collapse top nodes with single item
    while (!root_is_leaf()) {
        Array refs = m_array->GetSubArray(1);
        TIGHTDB_ASSERT(refs.size() != 0); // node cannot be empty
        if (refs.size() > 1) break;

        size_t ref = refs.get_as_ref(0);
        refs.erase(0); // avoid deleting subtree
        m_array->destroy();
        m_array->update_ref(ref);
    }

    // If it is last item in column, we don't have to update refs
    if (!isLast) UpdateRefs(row_ndx, -1);
}

void StringIndex::DoDelete(size_t row_ndx, StringData value, size_t offset)
{
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);
    Allocator& alloc = m_array->get_alloc();

    // Create 4 byte index key
    const char* const v = value.data() + offset;
    const int32_t key = create_key(v, value.data() + value.size());

    const size_t pos = values.FindPos2(key);
    TIGHTDB_ASSERT(pos != not_found);

    if (!m_array->is_leaf()) {
        size_t ref = refs.get_as_ref(pos);
        StringIndex node(ref, &refs, pos, m_target_column, m_get_func, alloc);
        node.DoDelete(row_ndx, value, offset);

        // Update the ref
        if (node.is_empty()) {
            values.erase(pos);
            refs.erase(pos);
            node.destroy();
        }
        else {
            int64_t maxval = node.GetLastKey();
            if (maxval != values.get(pos))
                values.set(pos, maxval);
        }
    }
    else {
        const int64_t ref = refs.get(pos);
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
    const char* const v = value.data() + offset;
    const int32_t key = create_key(v, value.data() + value.size());

    const size_t pos = values.FindPos2(key);
    TIGHTDB_ASSERT(pos != not_found);

    if (!m_array->is_leaf()) {
        size_t ref = refs.get_as_ref(pos);
        StringIndex node(ref, &refs, pos, m_target_column, m_get_func, alloc);
        node.do_update_ref(value, row_ndx, new_row_ndx, offset);
    }
    else {
        const int64_t ref = refs.get(pos);
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
    const Array values = m_array->GetSubArray(0);
    return values.is_empty();
}

#ifdef TIGHTDB_DEBUG

void StringIndex::verify_entries(const AdaptiveStringColumn& column) const
{
    Array results;

    const size_t count = column.size();
    for (size_t i = 0; i < count; ++i) {
        StringData value = column.get(i);

        find_all(results, value);

        size_t ndx = results.find_first(i);
        TIGHTDB_ASSERT(ndx != not_found);
        results.clear();
    }
    results.destroy(); // clean-up
}

void StringIndex::to_dot(ostream& out) const
{
    out << "digraph G {" << endl;

    ToDot(out);

    out << "}" << endl;
}


void StringIndex::ToDot(ostream& out, StringData title) const
{
    const size_t ref = get_ref();

    out << "subgraph cluster_stringindex" << ref << " {" << endl;
    out << " label = \"StringIndex";
    if (0 < title.size()) out << "\\n'" << title << "'";
    out << "\";" << endl;

    ArrayToDot(out, *m_array);

    out << "}" << endl;
}

void StringIndex::ArrayToDot(ostream& out, const Array& array) const
{
    if (array.has_refs()) {
        const Array offsets = array.GetSubArray(0);
        const Array refs    = array.GetSubArray(1);
        const size_t ref    = array.get_ref();

        if (!array.is_leaf()) {
            out << "subgraph cluster_stringindex_node" << ref << " {" << endl;
            out << " label = \"Node\";" << endl;
        }
        else {
            out << "subgraph cluster_stringindex_leaf" << ref << " {" << endl;
            out << " label = \"Leaf\";" << endl;
        }

        array.ToDot(out);
        KeysToDot(out, offsets, "keys");

        out << "}" << endl;

        refs.ToDot(out, "refs");

        const size_t count = refs.size();
        for (size_t i = 0; i < count; ++i) {
            const size_t ref = refs.get_as_ref(i);
            if (ref & 1) continue; // ignore literals

            const Array r = refs.GetSubArray(i);
            ArrayToDot(out, r);
        }
    }
    else {
        array.ToDot(out);
    }
}

void StringIndex::KeysToDot(ostream& out, const Array& array, StringData title) const
{
    const size_t ref = array.get_ref();

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
    const size_t count = array.size();
    for (size_t i = 0; i < count; ++i) {
        const uint64_t v =  array.get(i); // Never right shift signed values

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
