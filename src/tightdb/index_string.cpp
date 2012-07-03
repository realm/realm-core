#include "index_string.hpp"

using namespace tightdb;

StringIndex::StringIndex(const AdaptiveStringColumn& c) : Column(COLUMN_HASREFS, NULL, 0, c.GetAllocator()), m_column(c)
{
    // Add subcolumns for leafs
    const Array values(COLUMN_NORMAL);
    const Array refs(COLUMN_HASREFS);
    m_array->add(values.GetRef());
    m_array->add(refs.GetRef());
}

StringIndex::StringIndex(size_t ref, ArrayParent* parent, size_t pndx, const AdaptiveStringColumn& c) : Column(ref, parent, pndx, c.GetAllocator()), m_column(c)
{
}

int64_t StringIndex::GetLastKey() const
{
    const Array offsets =  m_array->GetSubArray(0);
    return offsets.back();
}


bool StringIndex::Insert(size_t row_ndx, const char* value, bool isLast)
{
    //TODO: Handle if not last

    return InsertWithOffset(row_ndx, 0, value);
}


bool StringIndex::InsertWithOffset(size_t row_ndx, size_t offset, const char* value)
{
    const char* v = value + offset;

    // Create 4 byte index key
    // (encoded like this to allow literal comparisons
    // independently of endianness)
    int32_t key = 0;
    if (*v) key  = ((int32_t)(*v++) << 24);
    if (*v) key |= ((int32_t)(*v++) << 16);
    if (*v) key |= ((int32_t)(*v++) << 8);
    if (*v) key |=  (int32_t)(*v++);

    return TreeInsert(row_ndx, key, offset, value);
}

bool StringIndex::InsertRowList(size_t ref, size_t offset, const char* value)
{
    assert(!m_array->IsNode()); // only works in leafs

    const char* v = value + offset;

    // Create 4 byte index key
    // (encoded like this to allow literal comparisons
    // independently of endianness)
    int32_t key = 0;
    if (*v) key  = ((int32_t)(*v++) << 24);
    if (*v) key |= ((int32_t)(*v++) << 16);
    if (*v) key |= ((int32_t)(*v++) << 8);
    if (*v) key |=  (int32_t)(*v++);

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = values.FindPos2(key);

    if (ins_pos == (size_t)-1) {
        // When key is outside current range, we can just add it
        values.add(key);
        refs.add(ref);
        return true;
    }

#ifdef _DEBUG
    // Since we only use this for moving existing values to new
    // sub-indexes, there should never be an existing match.
    const int32_t k = (int32_t)values.Get(ins_pos);
    assert(k != key);
#endif

    // If key is not present we add it at the correct location
    values.Insert(ins_pos, key);
    refs.Insert(ins_pos, ref);
    return true;
}

bool StringIndex::TreeInsert(size_t row_ndx, int32_t key, size_t offset, const char* value)
{
    const NodeChange nc = DoInsert(row_ndx, key, offset, value);

    switch (nc.type) {
        case NodeChange::CT_ERROR:
            return false; // allocation error
        case NodeChange::CT_NONE:
            break;
        case NodeChange::CT_INSERT_BEFORE:
        {
            Column newNode(COLUMN_NODE, m_array->GetAllocator());
            newNode.NodeAddKey(nc.ref1);
            newNode.NodeAddKey(GetRef());
            UpdateRef(newNode.GetRef());
            break;
        }
        case NodeChange::CT_INSERT_AFTER:
        {
            Column newNode(COLUMN_NODE, m_array->GetAllocator());
            newNode.NodeAddKey(GetRef());
            newNode.NodeAddKey(nc.ref1);
            UpdateRef(newNode.GetRef());
            break;
        }
        case NodeChange::CT_SPLIT:
        {
            Column newNode(COLUMN_NODE, m_array->GetAllocator());
            newNode.NodeAddKey(nc.ref1);
            newNode.NodeAddKey(nc.ref2);
            UpdateRef(newNode.GetRef());
            break;
        }
        default:
            assert(false);
            return false;
    }

    return true;
}

Column::NodeChange StringIndex::DoInsert(size_t row_ndx, int32_t key, size_t offset, const char* value)
{
    if (IsNode()) {
        // Get subnode table
        Array offsets = NodeGetOffsets();
        Array refs = NodeGetRefs();

        // Find the subnode containing the item
        size_t node_ndx = offsets.FindPos2(key);
        if (node_ndx == (size_t)-1) {
            // node can never be empty, so try to fit in last item
            node_ndx = offsets.Size()-1;
        }

        // Get sublist
        const size_t ref = refs.GetAsRef(node_ndx);
        StringIndex target(ref, &refs, node_ndx, m_column);

        // Insert item
        const NodeChange nc = target.DoInsert(row_ndx, key, offset, value);
        if (nc.type ==  NodeChange::CT_ERROR) return NodeChange(NodeChange::CT_ERROR); // allocation error
        else if (nc.type ==  NodeChange::CT_NONE) {
            // update keys
            const int64_t lastKey = target.GetLastKey();
            offsets.Set(node_ndx, lastKey);
            return NodeChange(NodeChange::CT_NONE); // no new nodes
        }

        if (nc.type == NodeChange::CT_INSERT_AFTER) ++node_ndx;

        // If there is room, just update node directly
        if (offsets.Size() < MAX_LIST_SIZE) {
            if (nc.type == NodeChange::CT_SPLIT) return NodeInsertSplit(node_ndx, nc.ref2);
            else return NodeInsert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
        }

        // Else create new node
        Column newNode(COLUMN_NODE, m_array->GetAllocator());
        if (nc.type == NodeChange::CT_SPLIT) {
            // update offset for left node
            const size_t newsize = target.Size();
            const size_t preoffset = node_ndx ? offsets.GetAsRef(node_ndx-1) : 0;
            offsets.Set(node_ndx, preoffset + newsize);

            newNode.NodeAddKey(nc.ref2);
            ++node_ndx;
        }
        else newNode.NodeAddKey(nc.ref1);

        switch (node_ndx) {
            case 0:             // insert before
                return NodeChange(NodeChange::CT_INSERT_BEFORE, newNode.GetRef());
            case MAX_LIST_SIZE: // insert after
                if (nc.type == NodeChange::CT_SPLIT)
                    return NodeChange(NodeChange::CT_SPLIT, GetRef(), newNode.GetRef());
                else return NodeChange(NodeChange::CT_INSERT_AFTER, newNode.GetRef());
            default:            // split
                // Move items after split to new node
                const size_t len = refs.Size();
                for (size_t i = node_ndx; i < len; ++i) {
                    const size_t ref = refs.GetAsRef(i);
                    newNode.NodeAddKey(ref);
                }
                offsets.Resize(node_ndx);
                refs.Resize(node_ndx);
                return NodeChange(NodeChange::CT_SPLIT, GetRef(), newNode.GetRef());
        }
    }
    else {
        // Is there room in the list?
        Array old_offsets = m_array->GetSubArray(0);
        const size_t count = old_offsets.Size();
        const bool noextend = count >= MAX_LIST_SIZE;

        // See if we can fit entry into current leaf
        // Works if there is room or it can join existing entries
        if (LeafInsert(row_ndx, key, offset, value, noextend))
            return true;

        // Create new list for item
        StringIndex newList(m_array->GetAllocator());

        if (!newList.LeafInsert(row_ndx, key, offset, value))
            return NodeChange(NodeChange::CT_ERROR);

        const size_t ndx = old_offsets.FindPos2(key);

        switch (ndx) {
            case 0:             // insert before
                return NodeChange(NodeChange::CT_INSERT_BEFORE, newList.GetRef());
            case -1: // insert after
                return NodeChange(NodeChange::CT_INSERT_AFTER, newList.GetRef());
            default: // split
            {
                Array old_refs = m_array->GetSubArray(1);
                Array new_offsets = newList.m_array->GetSubArray(0);
                Array new_refs = newList.m_array->GetSubArray(1);
                // Move items after split to new list
                for (size_t i = ndx; i < count; ++i) {
                    const int64_t v2 = old_offsets.Get(i);
                    const int64_t v3 = old_refs.Get(i);

                    new_offsets.add(v2);
                    new_refs.add(v3);
                }
                old_offsets.Resize(ndx);
                old_refs.Resize(ndx);

                return NodeChange(NodeChange::CT_SPLIT, GetRef(), newList.GetRef());
            }
        }
    }

    return NodeChange(NodeChange::CT_NONE); // test
}

bool StringIndex::NodeInsertSplit(size_t ndx, size_t new_ref)
{
    assert(IsNode());
    assert(new_ref);

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();

    assert(ndx < offsets.Size());
    assert(offsets.Size() < MAX_LIST_SIZE);

    // Get sublists
    const StringIndex orig_col = GetColumnFromRef<StringIndex>(refs, ndx);
    const StringIndex new_col(new_ref, NULL, 0, m_array->GetAllocator());

    // Update original key
    const int64_t lastKey = orig_col.GetLastKey();
    offsets.Set(ndx, lastKey);

    // Insert new ref
    const size_t newKey = new_col.GetLastKey();
    offsets.Insert(ndx+1, newKey);
    refs.Insert(ndx+1, new_ref);

    return true;
}

bool StringIndex::NodeInsert(size_t ndx, size_t ref)
{
    assert(ref);
    assert(IsNode());

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();

    assert(ndx <= offsets.Size());
    assert(offsets.Size() < MAX_LIST_SIZE);

    const StringIndex col(ref, (Array*)NULL, 0, m_array->GetAllocator());
    const int64_t lastKey = col.GetLastKey();

    if (!offsets.Insert(ndx, lastKey)) return false;
    return refs.Insert(ndx, ref);
}

bool StringIndex::LeafInsert(size_t row_ndx, int32_t key, size_t offset, const char* value, bool noextend)
{
    assert(!IsNode());

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = values.FindPos2(key);

    if (ins_pos == (size_t)-1) {
        if (noextend) return false;

        // When key is outside current range, we can just add it
        values.add(key);
        const size_t shifted = (row_ndx << 1) + 1; // shift to indicate literal
        refs.add(shifted);
        return true;
    }

    const int32_t k = (int32_t)values.Get(ins_pos);

    // If key is not present we add it at the correct location
    if (k != key) {
        if (noextend) return false;

        values.Insert(ins_pos, key);
        const size_t shifted = (row_ndx << 1) + 1; // shift to indicate literal
        refs.Insert(ins_pos, shifted);
        return true;
    }

    const size_t ref = refs.Get(ins_pos);
    const size_t sub_offset = offset + 4;

    // Single match (lowest bit set indicates literal row_ndx)
    if (ref & 1) {
        const size_t row_ndx2 = ref >> 1;
        const char* const v2 = m_column.Get(row_ndx2);
        if (strcmp(v2, value) == 0) {
            // convert to list
            Array row_list(COLUMN_NORMAL, NULL, 0, m_array->GetAllocator());
            row_list.add(row_ndx2);
            row_list.add(row_ndx);
            refs.Set(ins_pos, row_list.GetRef());
        }
        else {
            // convert to sub-index
            StringIndex sub_index(m_column);
            sub_index.InsertWithOffset(row_ndx2, sub_offset, v2);
            sub_index.InsertWithOffset(row_ndx, sub_offset, value);
            refs.Set(ins_pos, sub_index.GetRef());
        }
        return true;
    }

    Array sub(ref, &refs, ins_pos, m_array->GetAllocator());

    // If there alrady is a list of matches, we see if we fit there
    // or it has to be split into a sub-index
    if (!sub.HasRefs()) {
        const size_t r1 = (size_t)sub.Get(0);
        const char* const v2 = m_column.Get(r1);
        if (strcmp(v2, value) == 0) {
            sub.add(row_ndx);
        }
        else {
            StringIndex sub_index(m_column);
            sub_index.InsertRowList(sub.GetRef(), sub_offset, v2);
            sub_index.InsertWithOffset(row_ndx, sub_offset, value);
            refs.Set(ins_pos, sub_index.GetRef());
        }
        return true;
    }

    // sub-index
    StringIndex sub_index(ref, &refs, ins_pos, m_column);
    sub_index.InsertWithOffset(row_ndx, sub_offset, value);

    return true;
}

size_t StringIndex::find_first(const char* value) const
{
    return m_array->IndexStringFindFirst(value, m_column);
}

#ifdef _DEBUG

void StringIndex::to_dot(std::ostream& out)
{
    out << "digraph G {" << std::endl;

    ToDot(out);

    out << "}" << std::endl;
}


void StringIndex::ToDot(std::ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    out << "subgraph cluster_stringindex" << ref << " {" << std::endl;
    out << " label = \"StringIndex";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    ArrayToDot(out, *m_array);

    out << "}" << std::endl;
}

void StringIndex::ArrayToDot(std::ostream& out, const Array& array) const
{
    if (array.HasRefs()) {
        const Array offsets = array.GetSubArray(0);
        const Array refs    = array.GetSubArray(1);
        const size_t ref    = array.GetRef();

        if (array.IsNode()) {
            out << "subgraph cluster_stringindex_node" << ref << " {" << std::endl;
            out << " label = \"Node\";" << std::endl;
        }
        else {
            out << "subgraph cluster_stringindex_leaf" << ref << " {" << std::endl;
            out << " label = \"Leaf\";" << std::endl;
        }

        array.ToDot(out);
        KeysToDot(out, offsets, "keys");

        out << "}" << std::endl;

        refs.ToDot(out, "refs");

        const size_t count = refs.Size();
        for (size_t i = 0; i < count; ++i) {
            const size_t ref = refs.GetAsRef(i);
            if (ref & 1) continue; // ignore literals

            const Array r = refs.GetSubArray(i);
            ArrayToDot(out, r);
        }
    }
    else {
        array.ToDot(out);
    }
}

void StringIndex::KeysToDot(std::ostream& out, const Array& array, const char* title) const
{
    const size_t ref = array.GetRef();

    if (title) {
        out << "subgraph cluster_" << ref << " {" << std::endl;
        out << " label = \"" << title << "\";" << std::endl;
        out << " color = white;" << std::endl;
    }

    out << "n" << std::hex << ref << std::dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << std::endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << std::hex << ref << std::dec << "<BR/>";
    if (array.IsNode()) out << "IsNode<BR/>";
    if (array.HasRefs()) out << "HasRefs<BR/>";
    out << "</FONT></TD>" << std::endl;

    // Values
    const size_t count = array.Size();
    for (size_t i = 0; i < count; ++i) {
        const int64_t v =  array.Get(i);

        char str[5] = "\0\0\0\0";
        str[3] = (char)(v & 0xFF);
        str[2] = (char)((v >> 8) & 0xFF);
        str[1] = (char)((v >> 16) & 0xFF);
        str[0] = (char)((v >> 24) & 0xFF);
        const char* s = str;

        out << "<TD>" << s << "</TD>" << std::endl;
    }

    out << "</TR></TABLE>>];" << std::endl;
    if (title) out << "}" << std::endl;

    out << std::endl;
}


#endif //_DEBUG
