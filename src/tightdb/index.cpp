#include <tightdb/config.h>
#include <tightdb/index.hpp>

namespace {

using namespace tightdb;

Index GetIndexFromRef(Array& parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.HasRefs());
    TIGHTDB_ASSERT(ndx < parent.size());
    return Index(parent.GetAsRef(ndx), &parent, ndx);
}

const Index GetIndexFromRef(const Array& parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.HasRefs());
    TIGHTDB_ASSERT(ndx < parent.size());
    return Index(parent.GetAsRef(ndx));
}

}



namespace tightdb {

Index::Index(): Column(coldef_HasRefs)
{
    // Add subcolumns for leafs
    const Array values(coldef_Normal);
    const Array refs(coldef_Normal); // we do not own these refs (to column positions), so no COLUMN_HASREF
    m_array->add(intptr_t(values.GetRef()));
    m_array->add(intptr_t(refs.GetRef()));
}

Index::Index(ColumnDef type, Array* parent, size_t pndx): Column(type, parent, pndx) {}

Index::Index(size_t ref): Column(ref) {}

Index::Index(size_t ref, Array* parent, size_t pndx): Column(ref, parent, pndx) {}

bool Index::is_empty() const
{
    const Array offsets = m_array->GetSubArray(0);
    return offsets.is_empty();
}

void Index::BuildIndex(const Column& src)
{
    //TIGHTDB_ASSERT(is_empty());

    // Brute-force build-up
    // TODO: sort and merge
    for (size_t i = 0; i < src.Size(); ++i) {
        Insert(i, src.Get(i), true);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif // TIGHTDB_DEBUG
}

void Index::Set(size_t ndx, int64_t oldValue, int64_t newValue)
{
    Delete(ndx, oldValue, true); // set isLast to avoid updating refs
    Insert(ndx, newValue, true); // set isLast to avoid updating refs
}

void Index::Delete(size_t ndx, int64_t value, bool isLast)
{
    DoDelete(ndx, value);

    // Collapse top nodes with single item
    while (IsNode()) {
        Array refs = m_array->GetSubArray(1);
        TIGHTDB_ASSERT(refs.size() != 0); // node cannot be empty
        if (refs.size() > 1) break;

        const size_t ref = refs.GetAsRef(0);
        refs.Delete(0); // avoid deleting subtree
        m_array->Destroy();
        m_array->UpdateRef(ref);
    }

    // If it is last item in column, we don't have to update refs
    if (!isLast) UpdateRefs(ndx, -1);
}

bool Index::DoDelete(size_t ndx, int64_t value)
{
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    size_t pos = values.FindPos2(value);
    TIGHTDB_ASSERT(pos != size_t(-1));

    // There may be several nodes with the same values,
    // so we have to find the one with the matching ref
    if (m_array->IsNode()) {
        do {
            Index node = GetIndexFromRef(refs, pos);
            if (node.DoDelete(ndx, value)) {
                // Update the ref
                if (node.is_empty()) {
                    refs.Delete(pos);
                    node.Destroy();
                }
                else {
                    const int64_t maxval = node.MaxValue();
                    if (maxval != values.Get(pos)) values.Set(pos, maxval);
                }
                return true;
            }
            else ++pos;
        } while (pos < refs.size());
        TIGHTDB_ASSERT(false); // we should never reach here
    }
    else {
        do {
            if (refs.Get(pos) == (int)ndx) {
                values.Delete(pos);
                refs.Delete(pos);
                return true;
            }
            else ++pos;
        } while (pos < refs.size());
    }
    return false;
}

void Index::Insert(size_t ndx, int64_t value, bool isLast)
{
    // If it is last item in column, we don't have to update refs
    if (!isLast) UpdateRefs(ndx, 1);

    const NodeChange nc = DoInsert(ndx, value);
    switch (nc.type) {
        case NodeChange::none:
            return;
        case NodeChange::insert_before: {
            Index newNode(coldef_InnerNode);
            newNode.NodeAdd(nc.ref1);
            newNode.NodeAdd(GetRef());
            m_array->UpdateRef(newNode.GetRef());
            return;
        }
        case NodeChange::insert_after: {
            Index newNode(coldef_InnerNode);
            newNode.NodeAdd(GetRef());
            newNode.NodeAdd(nc.ref1);
            m_array->UpdateRef(newNode.GetRef());
            return;
        }
        case NodeChange::split: {
            Index newNode(coldef_InnerNode);
            newNode.NodeAdd(nc.ref1);
            newNode.NodeAdd(nc.ref2);
            m_array->UpdateRef(newNode.GetRef());
            return;
        }
    }
    TIGHTDB_ASSERT(false);
}

void Index::LeafInsert(size_t ref, int64_t value)
{
    TIGHTDB_ASSERT(!IsNode());

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = values.FindPos2(value);

    if (ins_pos == (size_t)-1) {
        values.add(value);
        refs.add(ref);
    }
    else {
        values.Insert(ins_pos, value);
        refs.Insert(ins_pos, ref);
    }
}

void Index::NodeAdd(size_t ref)
{
    TIGHTDB_ASSERT(ref);
    TIGHTDB_ASSERT(IsNode());

    const Index col(ref);
    TIGHTDB_ASSERT(!col.is_empty());
    const int64_t maxval = col.MaxValue();

    Array offsets = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = offsets.FindPos2(maxval);

    if (ins_pos == (size_t)-1) {
        offsets.add(maxval);
        refs.add(ref);
    }
    else {
        offsets.Insert(ins_pos, maxval);
        refs.Insert(ins_pos, ref);
    }
}

int64_t Index::MaxValue() const
{
    const Array values = m_array->GetSubArray(0);
    return values.is_empty() ? 0 : values.back();
}

Column::NodeChange Index::DoInsert(size_t ndx, int64_t value)
{
    if (IsNode()) {
        // Get subnode table
        Array offsets = m_array->GetSubArray(0);
        Array refs = m_array->GetSubArray(1);

        // Find the subnode containing the item
        size_t node_ndx = offsets.FindPos2(ndx);
        if (node_ndx == (size_t)-1) {
            // node can never be empty, so try to fit in last item
            node_ndx = offsets.size()-1;
        }

        // Calc index in subnode
        const size_t offset = node_ndx ? (size_t)offsets.Get(node_ndx-1) : 0;
        const size_t local_ndx = ndx - offset;

        // Get sublist
        Index target = GetIndexFromRef(refs, node_ndx);

        // Insert item
        const NodeChange nc = target.DoInsert(local_ndx, value);
        if (nc.type ==  NodeChange::none) {
            offsets.Increment(1, node_ndx);  // update offsets
            return NodeChange::none; // no new nodes
        }

        if (nc.type == NodeChange::insert_after) ++node_ndx;

        // If there is room, just update node directly
        if (offsets.size() < TIGHTDB_MAX_LIST_SIZE) {
            if (nc.type == NodeChange::split) NodeInsertSplit<Column>(node_ndx, nc.ref2);
            else NodeInsert<Column>(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
            return NodeChange::none;
        }

        // Else create new node
        Index newNode(coldef_InnerNode);
        newNode.NodeAdd(nc.ref1);

        switch (node_ndx) {
        case 0:             // insert before
            return NodeChange(NodeChange::insert_before, newNode.GetRef());
        case TIGHTDB_MAX_LIST_SIZE: // insert below
            return NodeChange(NodeChange::insert_after, newNode.GetRef());
        default:            // split
            // Move items below split to new node
            const size_t len = refs.size();
            for (size_t i = node_ndx; i < len; ++i) {
                newNode.NodeAdd((size_t)refs.Get(i));
            }
            offsets.Resize(node_ndx);
            refs.Resize(node_ndx);
            return NodeChange(NodeChange::split, GetRef(), newNode.GetRef());
        }
    }
    else {
        // Is there room in the list?
        if (Size() < TIGHTDB_MAX_LIST_SIZE) {
            LeafInsert(ndx, value);
            return NodeChange::none;
        }

        // Create new list for item
        Index newList;
        LeafInsert(ndx, value);

        switch (ndx) {
        case 0:             // insert before
            return NodeChange(NodeChange::insert_before, newList.GetRef());
        case TIGHTDB_MAX_LIST_SIZE: // insert below
            return NodeChange(NodeChange::insert_after, newList.GetRef());
        default:            // split
            // Move items below split to new list
            for (size_t i = ndx; i < m_array->size(); ++i) {
                newList.add(m_array->Get(i));
            }
            m_array->Resize(ndx);

            return NodeChange(NodeChange::split, GetRef(), newList.GetRef());
        }
    }
}

size_t Index::find_first(int64_t value) const
{
    size_t ref = GetRef();
    for (;;) {
        const Array node(ref);
        const Array values = node.GetSubArray(0);
        const Array refs = node.GetSubArray(1);

        const size_t pos = values.FindPos2(value);

        if (pos == (size_t)-1) return (size_t)-1;
        else if (!m_array->IsNode()) {
            if (values.Get(pos) == value) return (size_t)refs.Get(pos);
            else return (size_t)-1;
        }

        ref = (size_t)refs.Get(pos);
    }
}

bool Index::find_all(Column& result, int64_t value) const
{
    const Array values = m_array->GetSubArray(0);
    const Array refs = m_array->GetSubArray(1);

    size_t pos = values.FindPos2(value);
    TIGHTDB_ASSERT(pos != (size_t)-1);

    // There may be several nodes with the same values,
    if (m_array->IsNode()) {
        do {
            const Index node = GetIndexFromRef(refs, pos);
            if (!node.find_all(result, value)) return false;
            ++pos;
        } while (pos < refs.size());
    }
    else {
        do {
            if (values.Get(pos) == value) {
                result.add(refs.Get(pos));
                ++pos;
            }
            else return false; // no more matches
        } while (pos < refs.size());
    }

    return true; // may be more matches in next node
}

bool Index::FindAllRange(Column& result, int64_t start, int64_t end) const
{
    const Array values = m_array->GetSubArray(0);
    const Array refs = m_array->GetSubArray(1);

    size_t pos = values.FindPos2(start);
    TIGHTDB_ASSERT(pos != (size_t)-1);

    // There may be several nodes with the same values,
    if (m_array->IsNode()) {
        do {
            const Index node = GetIndexFromRef(refs, pos);
            if (!node.FindAllRange(result, start, end)) return false;
            ++pos;
        } while (pos < refs.size());
    }
    else {
        do {
            const int64_t v = values[pos];
            if (v >= start && v < end) {
                result.add(refs.Get(pos));
                ++pos;
            }
            else return false; // no more matches
        } while (pos < refs.size());
    }

    return true; // may be more matches in next node
}

void Index::UpdateRefs(size_t pos, int diff)
{
    TIGHTDB_ASSERT(diff == 1 || diff == -1); // only used by insert and delete

    Array refs = m_array->GetSubArray(1);

    if (m_array->IsNode()) {
        for (size_t i = 0; i < refs.size(); ++i) {
            const size_t ref = size_t(refs.Get(i));
            Index ndx(ref);
            ndx.UpdateRefs(pos, diff);
        }
    }
    else {
        refs.IncrementIf(pos, diff);
    }
}

#ifdef TIGHTDB_DEBUG

void Index::Verify() const
{
    TIGHTDB_ASSERT(m_array->size() == 2);
    TIGHTDB_ASSERT(m_array->HasRefs());

    const Array offsets = m_array->GetSubArray(0);
    const Array refs = m_array->GetSubArray(1);
    offsets.Verify();
    refs.Verify();
    TIGHTDB_ASSERT(offsets.size() == refs.size());

    if (m_array->IsNode()) {
        TIGHTDB_ASSERT(refs.HasRefs());

        // Make sure that all offsets matches biggest value in ref
        for (size_t i = 0; i < refs.size(); ++i) {
            const size_t ref = (size_t)refs.Get(i);
            TIGHTDB_ASSERT(ref);

            const Index col(ref);
            col.Verify();

            if (offsets.Get(i) != col.MaxValue()) {
                TIGHTDB_ASSERT(false);
            }
        }
    }
    else {
        TIGHTDB_ASSERT(!refs.HasRefs());
    }
}

#endif // TIGHTDB_DEBUG

}
