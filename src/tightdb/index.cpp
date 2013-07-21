#include <tightdb/config.h>
#include <tightdb/index.hpp>

namespace {

using namespace tightdb;

Index GetIndexFromRef(Array& parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.has_refs());
    TIGHTDB_ASSERT(ndx < parent.size());
    return Index(parent.get_as_ref(ndx), &parent, ndx);
}

const Index GetIndexFromRef(const Array& parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.has_refs());
    TIGHTDB_ASSERT(ndx < parent.size());
    return Index(parent.get_as_ref(ndx));
}

}



namespace tightdb {

Index::Index(): Column(Array::type_HasRefs)
{
    // Add subcolumns for leafs
    Array values(Array::type_Normal);
    Array refs(Array::type_Normal); // we do not own these refs (to column positions), so no COLUMN_HASREF
    m_array->add(intptr_t(values.get_ref()));
    m_array->add(intptr_t(refs.get_ref()));
}

Index::Index(Array::Type type, Array* parent, size_t pndx): Column(type, parent, pndx) {}

Index::Index(ref_type ref): Column(ref) {}

Index::Index(ref_type ref, Array* parent, size_t pndx): Column(ref, parent, pndx) {}

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
    for (size_t i = 0; i < src.size(); ++i) {
        insert(i, src.get(i), true);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif // TIGHTDB_DEBUG
}

void Index::set(size_t ndx, int64_t oldValue, int64_t newValue)
{
    erase(ndx, oldValue, true); // set isLast to avoid updating refs
    insert(ndx, newValue, true); // set isLast to avoid updating refs
}

void Index::erase(size_t ndx, int64_t value, bool isLast)
{
    DoDelete(ndx, value);

    // Collapse top nodes with single item
    while (!root_is_leaf()) {
        Array refs = m_array->GetSubArray(1);
        TIGHTDB_ASSERT(refs.size() != 0); // node cannot be empty
        if (refs.size() > 1) break;

        const size_t ref = refs.get_as_ref(0);
        refs.erase(0); // avoid deleting subtree
        m_array->destroy();
        m_array->update_ref(ref);
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
    if (!m_array->is_leaf()) {
        do {
            Index node = GetIndexFromRef(refs, pos);
            if (node.DoDelete(ndx, value)) {
                // Update the ref
                if (node.is_empty()) {
                    refs.erase(pos);
                    node.destroy();
                }
                else {
                    const int64_t maxval = node.MaxValue();
                    if (maxval != values.get(pos)) values.set(pos, maxval);
                }
                return true;
            }
            else ++pos;
        } while (pos < refs.size());
        TIGHTDB_ASSERT(false); // we should never reach here
    }
    else {
        do {
            if (refs.get(pos) == int(ndx)) {
                values.erase(pos);
                refs.erase(pos);
                return true;
            }
            else ++pos;
        } while (pos < refs.size());
    }
    return false;
}

void Index::insert(size_t ndx, int64_t value, bool isLast)
{
    // If it is last item in column, we don't have to update refs
    if (!isLast) UpdateRefs(ndx, 1);

    const NodeChange nc = DoInsert(ndx, value);
    switch (nc.type) {
        case NodeChange::none:
            return;
        case NodeChange::insert_before: {
            Index newNode(Array::type_InnerColumnNode);
            newNode.NodeAdd(nc.ref1);
            newNode.NodeAdd(get_ref());
            m_array->update_ref(newNode.get_ref());
            return;
        }
        case NodeChange::insert_after: {
            Index newNode(Array::type_InnerColumnNode);
            newNode.NodeAdd(get_ref());
            newNode.NodeAdd(nc.ref1);
            m_array->update_ref(newNode.get_ref());
            return;
        }
        case NodeChange::split: {
            Index newNode(Array::type_InnerColumnNode);
            newNode.NodeAdd(nc.ref1);
            newNode.NodeAdd(nc.ref2);
            m_array->update_ref(newNode.get_ref());
            return;
        }
    }
    TIGHTDB_ASSERT(false);
}

void Index::LeafInsert(size_t ref, int64_t value)
{
    TIGHTDB_ASSERT(root_is_leaf());

    // Get subnode table
    Array values = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = values.FindPos2(value);

    if (ins_pos == size_t(-1)) {
        values.add(value);
        refs.add(ref);
    }
    else {
        values.insert(ins_pos, value);
        refs.insert(ins_pos, ref);
    }
}

void Index::NodeAdd(size_t ref)
{
    TIGHTDB_ASSERT(ref);
    TIGHTDB_ASSERT(!root_is_leaf());

    const Index col(ref);
    TIGHTDB_ASSERT(!col.is_empty());
    const int64_t maxval = col.MaxValue();

    Array offsets = m_array->GetSubArray(0);
    Array refs = m_array->GetSubArray(1);

    const size_t ins_pos = offsets.FindPos2(maxval);

    if (ins_pos == size_t(-1)) {
        offsets.add(maxval);
        refs.add(ref);
    }
    else {
        offsets.insert(ins_pos, maxval);
        refs.insert(ins_pos, ref);
    }
}

int64_t Index::MaxValue() const
{
    const Array values = m_array->GetSubArray(0);
    return values.is_empty() ? 0 : values.back();
}

Column::NodeChange Index::DoInsert(size_t ndx, int64_t value)
{
    if (!root_is_leaf()) {
        // Get subnode table
        Array offsets = m_array->GetSubArray(0);
        Array refs = m_array->GetSubArray(1);

        // Find the subnode containing the item
        size_t node_ndx = offsets.FindPos2(ndx);
        if (node_ndx == size_t(-1)) {
            // node can never be empty, so try to fit in last item
            node_ndx = offsets.size()-1;
        }

        // Calc index in subnode
        const size_t offset = node_ndx ? to_size_t(offsets.get(node_ndx-1)) : 0;
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
        Index newNode(Array::type_InnerColumnNode);
        newNode.NodeAdd(nc.ref1);

        switch (node_ndx) {
        case 0:             // insert before
            return NodeChange(NodeChange::insert_before, newNode.get_ref());
        case TIGHTDB_MAX_LIST_SIZE: // insert below
            return NodeChange(NodeChange::insert_after, newNode.get_ref());
        default:            // split
            // Move items below split to new node
            const size_t len = refs.size();
            for (size_t i = node_ndx; i < len; ++i) {
                newNode.NodeAdd(to_size_t(refs.get(i)));
            }
            offsets.resize(node_ndx);
            refs.resize(node_ndx);
            return NodeChange(NodeChange::split, get_ref(), newNode.get_ref());
        }
    }
    else {
        // Is there room in the list?
        if (size() < TIGHTDB_MAX_LIST_SIZE) {
            LeafInsert(ndx, value);
            return NodeChange::none;
        }

        // Create new list for item
        Index newList;
        LeafInsert(ndx, value);

        switch (ndx) {
        case 0:             // insert before
            return NodeChange(NodeChange::insert_before, newList.get_ref());
        case TIGHTDB_MAX_LIST_SIZE: // insert below
            return NodeChange(NodeChange::insert_after, newList.get_ref());
        default:            // split
            // Move items below split to new list
            for (size_t i = ndx; i < m_array->size(); ++i) {
                newList.add(m_array->get(i));
            }
            m_array->resize(ndx);

            return NodeChange(NodeChange::split, get_ref(), newList.get_ref());
        }
    }
}

size_t Index::find_first(int64_t value) const
{
    size_t ref = get_ref();
    for (;;) {
        const Array node(ref);
        const Array values = node.GetSubArray(0);
        const Array refs = node.GetSubArray(1);

        const size_t pos = values.FindPos2(value);

        if (pos == size_t(-1)) return size_t(-1);
        else if (m_array->is_leaf()) {
            if (values.get(pos) == value) return to_size_t(refs.get(pos));
            else return size_t(-1);
        }

        ref = to_size_t(refs.get(pos));
    }
}

bool Index::find_all(Column& result, int64_t value) const
{
    const Array values = m_array->GetSubArray(0);
    const Array refs = m_array->GetSubArray(1);

    size_t pos = values.FindPos2(value);
    TIGHTDB_ASSERT(pos != size_t(-1));

    // There may be several nodes with the same values,
    if (!m_array->is_leaf()) {
        do {
            const Index node = GetIndexFromRef(refs, pos);
            if (!node.find_all(result, value)) return false;
            ++pos;
        } while (pos < refs.size());
    }
    else {
        do {
            if (values.get(pos) == value) {
                result.add(refs.get(pos));
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
    TIGHTDB_ASSERT(pos != size_t(-1));

    // There may be several nodes with the same values,
    if (!m_array->is_leaf()) {
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
                result.add(refs.get(pos));
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

    if (!m_array->is_leaf()) {
        for (size_t i = 0; i < refs.size(); ++i) {
            const size_t ref = size_t(refs.get(i));
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
    TIGHTDB_ASSERT(m_array->has_refs());

    const Array offsets = m_array->GetSubArray(0);
    const Array refs = m_array->GetSubArray(1);
    offsets.Verify();
    refs.Verify();
    TIGHTDB_ASSERT(offsets.size() == refs.size());

    if (!m_array->is_leaf()) {
        TIGHTDB_ASSERT(refs.has_refs());

        // Make sure that all offsets matches biggest value in ref
        for (size_t i = 0; i < refs.size(); ++i) {
            const size_t ref = to_size_t(refs.get(i));
            TIGHTDB_ASSERT(ref);

            const Index col(ref);
            col.Verify();

            if (offsets.get(i) != col.MaxValue()) {
                TIGHTDB_ASSERT(false);
            }
        }
    }
    else {
        TIGHTDB_ASSERT(!refs.has_refs());
    }
}

#endif // TIGHTDB_DEBUG

}
