/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_COLUMN_TPL_HPP
#define TIGHTDB_COLUMN_TPL_HPP

#include <cstdlib>

#include <tightdb/config.h>
#include <tightdb/array.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_fwd.hpp>

namespace tightdb {

template<class T, class cond> class BasicNode;
template<class T, class cond> class IntegerNode;
template<class T> class SequentialGetter;

template<class cond, class T> struct ColumnTypeTraits2;

template<class cond> struct ColumnTypeTraits2<cond, int64_t> {
    typedef Column column_type;
    typedef IntegerNode<int64_t,cond> node_type;
};
template<class cond> struct ColumnTypeTraits2<cond, bool> {
    typedef Column column_type;
    typedef IntegerNode<bool,cond> node_type;
};
template<class cond> struct ColumnTypeTraits2<cond, float> {
    typedef ColumnFloat column_type;
    typedef BasicNode<float,cond> node_type;
};
template<class cond> struct ColumnTypeTraits2<cond, double> {
    typedef ColumnDouble column_type;
    typedef BasicNode<double,cond> node_type;
};


template <class T, class R, Action action, class condition>
R ColumnBase::aggregate(T target, size_t start, size_t end, size_t* matchcount) const
{
    typedef typename ColumnTypeTraits2<condition,T>::column_type ColType;
    typedef typename ColumnTypeTraits2<condition,T>::node_type NodeType;

    if (end == size_t(-1))
        end = size();

    NodeType node(target, 0);

    node.QuickInit(const_cast<ColType*>(static_cast<const ColType*>(this)), target);
    QueryState<R> state;
    state.init(action, NULL, size_t(-1));

    ColType* column = const_cast<ColType*>(static_cast<const ColType*>(this));
    SequentialGetter<T> sg(column);
    node.template aggregate_local<action, R, T>(&state, start, end, size_t(-1), &sg, matchcount);

    return state.m_state;
}


template<class T> T GetColumnFromRef(Array& parent, size_t ndx) // Throws
{
    //TIGHTDB_ASSERT(parent.has_refs());
    //TIGHTDB_ASSERT(ndx < parent.size());
    return T(size_t(parent.get(ndx)), &parent, ndx, parent.get_alloc()); // Throws
}

template<class T, class C> void ColumnBase::TreeSet(size_t ndx, T value)
{
    //const T oldVal = m_index ? get(ndx) : 0; // cache oldval for index

    if (!root_is_leaf()) {
        // Get subnode table
        const Array offsets = NodeGetOffsets();
        Array refs = NodeGetRefs();

        // Find the subnode containing the item
        const size_t node_ndx = offsets.FindPos(ndx);

        // Calc index in subnode
        const size_t offset = node_ndx ? to_size_t(offsets.get(node_ndx-1)) : 0;
        const size_t local_ndx = ndx - offset;

        // Set item
        C target = GetColumnFromRef<C>(refs, node_ndx);
        target.set(local_ndx, value);
    }
    else {
        static_cast<C*>(this)->LeafSet(ndx, value);
    }

    // Update index
    //if (m_index) m_index->set(ndx, oldVal, value);
}

template<class T, class C> void ColumnBase::TreeDelete(size_t ndx)
{
    if (root_is_leaf()) {
        static_cast<C*>(this)->LeafDelete(ndx);
    }
    else {
        // Get subnode table
        Array offsets = NodeGetOffsets();
        Array refs = NodeGetRefs();

        // Find the subnode containing the item
        const size_t node_ndx = offsets.FindPos(ndx);
        TIGHTDB_ASSERT(node_ndx != size_t(-1));

        // Calc index in subnode
        const size_t offset = node_ndx ? to_size_t(offsets.get(node_ndx-1)) : 0;
        const size_t local_ndx = ndx - offset;

        // Get sublist
        C target = GetColumnFromRef<C>(refs, node_ndx);
        target.template TreeDelete<T,C>(local_ndx);

        // Remove ref in node
        if (target.is_empty()) {
            offsets.erase(node_ndx);
            refs.erase(node_ndx);
            target.destroy();
        }

        if (offsets.is_empty()) {
            // All items deleted, we can revert to being array
            static_cast<C*>(this)->clear();
        }
        else {
            // Update lower offsets
            if (node_ndx < offsets.size()) offsets.Increment(-1, node_ndx);
        }
    }
}


template<class T, class C, class F>
size_t ColumnBase::TreeFind(T value, size_t start, size_t end) const
{
    // Use index if possible
    /*if (m_index && start == 0 && end == -1) {
     return FindWithIndex(value);
     }*/
//  F function;
    if (root_is_leaf()) {
        const C* c = static_cast<const C*>(this);
        return c->template LeafFind<F>(value, start, end);
    }
    else {
        // Get subnode table
        const Array offsets = NodeGetOffsets();
        const Array refs = NodeGetRefs();
        const size_t count = refs.size();

        if (start == 0 && end == size_t(-1)) {
            for (size_t i = 0; i < count; ++i) {
                C col(size_t(refs.get(i)), 0, 0, m_array->get_alloc());
                size_t ndx = col.template TreeFind<T,C,F>(value, 0, size_t(-1));
                if (ndx != size_t(-1)) {
                    size_t offset = i ? to_size_t(offsets.get(i-1)) : 0;
                    return offset + ndx;
                }
            }
        }
        else {
            // partial search
            size_t i = offsets.FindPos(start);
            size_t offset = i ? to_size_t(offsets.get(i-1)) : 0;
            size_t s = start - offset;
            size_t e = (end == size_t(-1) || int(end) >= offsets.get(i)) ? size_t(-1) : end - offset;

            for (;;) {
                C col(size_t(refs.get(i)), 0, 0, m_array->get_alloc());

                size_t ndx = col.template TreeFind<T,C,F>(value, s, e);
                if (ndx != not_found) {
                    size_t offset = i ? to_size_t(offsets.get(i-1)) : 0;
                    return offset + ndx;
                }

                ++i;
                if (i >= count) break;

                s = 0;
                if (end != size_t(-1)) {
                    if (end >= to_size_t(offsets.get(i)))
                        e = size_t(-1);
                    else {
                        offset = to_size_t(offsets.get(i-1));
                        if (offset >= end)
                            break;
                        e = end - offset;
                    }
                }
            }
        }

        return size_t(-1); // not found
    }
}

template<class T, class C> void ColumnBase::TreeFindAll(Array &result, T value, size_t add_offset, size_t start, size_t end) const
{
    if (root_is_leaf()) {
        return static_cast<const C*>(this)->LeafFindAll(result, value, add_offset, start, end);
    }
    else {
        // Get subnode table
        const Array offsets = NodeGetOffsets();
        const Array refs = NodeGetRefs();
        const size_t count = refs.size();
        size_t i = offsets.FindPos(start);
        size_t offset = i ? to_size_t(offsets.get(i-1)) : 0;
        size_t s = start - offset;
        size_t e = (end == size_t(-1) || int(end) >= offsets.get(i)) ? size_t(-1) : end - offset;

        for (;;) {
            const size_t ref = refs.get_as_ref(i);
            const C col(ref, 0, 0, m_array->get_alloc());

            size_t add = i ? to_size_t(offsets.get(i-1)) : 0;
            add += add_offset;
            col.template TreeFindAll<T, C>(result, value, add, s, e);
            ++i;
            if (i >= count) break;

            s = 0;
            if (end != size_t(-1)) {
                if (end >= to_size_t(offsets.get(i))) e = size_t(-1);
                else {
                    offset = to_size_t(offsets.get(i-1));
                    if (offset >= end)
                        return;
                    e = end - offset;
                }
            }
        }
    }
}



template<class T, class C>
void ColumnBase::TreeVisitLeafs(size_t start, size_t end, size_t caller_offset,
                                bool (*call)(T *arr, size_t start, size_t end,
                                             size_t caller_offset, void *state),
                                void *state) const
{
    if (root_is_leaf()) {
        if (end == size_t(-1))
            end = m_array->size();
        if (m_array->size() > 0)
            call(m_array, start, end, caller_offset, state);
    }
    else {
        const Array offsets = NodeGetOffsets();
        const Array refs = NodeGetRefs();
        const size_t count = refs.size();
        size_t i = offsets.FindPos(start);
        size_t offset = i ? to_size_t(offsets.get(i-1)) : 0;
        size_t s = start - offset;
        size_t e = (end == size_t(-1) || int(end) >= offsets.get(i)) ? size_t(-1) : end - offset;

        for (;;) {
            const size_t ref = refs.get_as_ref(i);
            const C col(ref, 0, 0, m_array->get_alloc());

            size_t add = i ? to_size_t(offsets.get(i-1)) : 0;
            add += caller_offset;
            col.template TreeVisitLeafs<T, C>(s, e, add, call, state);
            ++i;
            if (i >= count) break;

            s = 0;
            if (end != size_t(-1)) {
                if (end >= to_size_t(offsets.get(i))) e = size_t(-1);
                else {
                    offset = to_size_t(offsets.get(i-1));
                    if (offset >= end)
                        return;
                    e = end - offset;
                }
            }
        }
    }
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_TPL_HPP
