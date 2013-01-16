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
#ifndef TIGHTDB_QUERY_TPL_HPP
#define TIGHTDB_QUERY_TPL_HPP

template <typename R, typename T>
R Query::sum(size_t column, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if (end == size_t(-1)) 
        end = m_table->size();

    typedef ColumnTypeTraits<T>::column_type ColType;
    const ColType& c = m_table->GetColumn<ColType, ColumnTypeTraits<T>::id>(column);

    if (first.size() == 0 || first[0] == 0) {
        // User created query with no criteria; sum() range
        if (resultcount)
            *resultcount = end-start;

        return c.sum(start, end);
    }

    Init(*m_table);
    size_t matchcount = 0; 
    state_state<R> st;
    st.init(TDB_SUM, NULL, limit);
    R r = first[0]->aggregate<TDB_SUM, T>(&st, start, end, column, &matchcount);
    if (resultcount)
        *resultcount = matchcount;
    return r;
}


template <typename R, typename T>
double Query::average(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    Init(*m_table);

    size_t resultcount2 = 0;
    const R sum1 = sum<R, T>(column_ndx, &resultcount2, start, end, limit);
    const double avg1 = (double)sum1 / (double)(resultcount2 > 0 ? resultcount2 : 1);

    if (resultcount != NULL)
        *resultcount = resultcount2;
    return avg1;
}


template <typename T>
T Query::maximum(size_t column, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if (end == size_t(-1)) 
        end = m_table->size();

    typedef typename ColumnTypeTraits<T>::column_type ColType;
    const ColType& c = m_table->GetColumn< ColType, ColumnTypeTraits<T>::id >(column);

    if (first.size() == 0 || first[0] == 0) {
        // User created query with no criteria; max() range
        if (resultcount)
            *resultcount = end-start;

        return c.maximum(start, end);
    }
        
    Init(*m_table);
    size_t matchcount = 0;
    state_state<T> st;
    st.init(TDB_MAX, NULL, limit);
    T r = first[0]->aggregate<TDB_MAX, T>(&st, start, end, column, &matchcount);
    if (resultcount)
        *resultcount = matchcount;
    return r;
}

template <typename T>
T Query::minimum(size_t column, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if (end == size_t(-1)) 
        end = m_table->size();

    typedef ColumnTypeTraits<T>::column_type ColType;
    const ColType& c = m_table->GetColumn<ColType, ColumnTypeTraits<T>::id>(column);

    if (first.size() == 0 || first[0] == 0) {
        // User created query with no criteria; min() range
        if (resultcount)
            *resultcount = end-start;

        return c.minimum(start, end);
    }

    Init(*m_table);
    size_t matchcount = 0;
    state_state<T> st;
    st.init(TDB_MIN, NULL, limit);
    T r = first[0]->aggregate<TDB_MIN, T>(&st, start, end, column, &matchcount);
    if (resultcount)
        *resultcount = matchcount;
    return r;
}


#endif // TIGHTDB_QUERY_TPL_HPP