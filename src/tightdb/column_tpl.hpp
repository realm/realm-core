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
R ColumnBase::aggregate(T target, std::size_t start, std::size_t end, std::size_t* matchcount,
                        std::size_t limit) const
{
    typedef typename ColumnTypeTraits2<condition,T>::column_type ColType;
    typedef typename ColumnTypeTraits2<condition,T>::node_type NodeType;

    if (end == std::size_t(-1))
        end = size();

    NodeType node(target, 0);

    node.QuickInit(const_cast<ColType*>(static_cast<const ColType*>(this)), target);
    QueryState<R> state;
    state.init(action, 0, limit);

    ColType* column = const_cast<ColType*>(static_cast<const ColType*>(this));
    SequentialGetter<T> sg(column);
    node.template aggregate_local<action, R, T>(&state, start, end, std::size_t(-1),
                                                &sg, matchcount);

    return state.m_state;
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_TPL_HPP
