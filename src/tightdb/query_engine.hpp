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
#ifndef TIGHTDB_QUERY_ENGINE_HPP
#define TIGHTDB_QUERY_ENGINE_HPP

#include <string>
#include <functional>
#include "meta.hpp"

#include <tightdb/table.hpp>
#include <tightdb/table_view.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/utf8.hpp>
#include <tightdb/query_conditions.hpp>

namespace tightdb {


class ParentNode {
public:
    ParentNode() : m_child(NULL), m_cond(-1), m_table(NULL), m_array(false) {}
    virtual ~ParentNode() {}
    virtual void Init(const Table& table) {m_table = &table; if (m_child) m_child->Init(table);}
    virtual size_t find_first(size_t start, size_t end) = 0;

    template <ACTION action> int64_t agg(Array* res, size_t start, size_t end, size_t limit, size_t agg_col, size_t* matchcount = 0) 
    {
        m_array.state_init(action, &m_state, res);
        Column *column_agg;            // Column on which aggregate function is executed (or not_found actions that don't use row value, such as COUNT and FIND_ALL)
        if (agg_col != not_found)
            column_agg = (Column*)&m_table->GetColumnBase(agg_col); 

        size_t r = start - 1;
        size_t count = 0;
        for (;;) {
            r = find_first(r + 1, end);
            if (r == end || count == limit)
                break;
            count++;
            
            if (agg_col != size_t(-1) && m_array.USES_VAL<action>())
                m_array.FIND_ACTION<action>(r, column_agg->Get(r), &m_state, &tightdb_dummy);
            else
                m_array.FIND_ACTION<action>(r, 0, &m_state, &tightdb_dummy);  
        }

        return m_state.state;

    }


    virtual int64_t aggregate(Array* res, size_t start, size_t end, size_t limit, ACTION action = TDB_FINDALL, size_t agg_col = not_found,  size_t* matchcount = 0) 
    {
        if (action == TDB_FINDALL)
            return agg<TDB_FINDALL>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_SUM)
            return agg<TDB_SUM>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_MAX)
            return agg<TDB_MAX>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_MIN)
            return agg<TDB_MIN>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_COUNT)
            return agg<TDB_COUNT>(res, start, end, limit, agg_col, matchcount);

        TIGHTDB_ASSERT(false);
        return uint64_t(-1);

    }

    virtual std::string Verify(void)
    {
        if (m_error_code != "")
            return m_error_code;
        if (m_child == 0)
            return "";
        else
            return m_child->Verify();
    };

protected:
    friend class Query;
    ParentNode*     m_child;
protected:  
    int             m_cond;
    size_t          m_column_id;    // initialized in classes that inherit
    const Table*    m_table;
    std::string     m_error_code;
    Array           m_array; 
    state_state     m_state;
};


class ARRAYNODE: public ParentNode {
public:
    ARRAYNODE(const Array& arr) : m_arr(arr), m_max(0), m_next(0), m_size(arr.Size()) {}

    void Init(const Table& table)
    {
        m_next = 0;
        if (m_size > 0)
            m_max = m_arr.Get(m_size - 1);
        if (m_child) 
            m_child->Init(table);
    }

    size_t find_first(size_t start, size_t end)
    {
        for (size_t s = start; s < end; ++s) {
            // Test first few values and end
            if (m_size == 0)
                return end;               

            if (m_next < m_size && m_arr.GetAsSizeT(m_next) >= start) goto match; else ++m_next;
            if (m_next < m_size && m_arr.GetAsSizeT(m_next) >= start) goto match; else ++m_next;

            if (start > m_max) return end;

            if (m_next < m_size && m_arr.GetAsSizeT(m_next) >= start) goto match; else ++m_next;
            if (m_next < m_size && m_arr.GetAsSizeT(m_next) >= start) goto match; else ++m_next;

            // Find bounds
            --m_next;
            size_t add;
            add = 1;
            while (m_next + add < m_size && TO_SIZET(m_arr.Get(m_next + add)) < start)
                add *= 2;

            // Do binary search inside bounds
            TIGHTDB_ASSERT(m_arr.GetAsSizeT(m_arr.Size() - 1) >= start);

            size_t high;
            high = m_next + add < m_size ? m_next + add : m_size;
            m_next = m_next + add / 2 - 1;

            while (high - m_next > 1) {
                const size_t probe = (m_next + high) / 2;
                const size_t v = m_arr.GetAsSizeT(probe);
                if (v < start) 
                    m_next = probe;
                else           
                    high = probe;
            }
            if (high == m_next + add)         
                m_next = end;
            else
                m_next = high;
match:
            // Test remaining query criterias
            s = m_arr.Get(m_next);
            ++m_next;
            if (m_child == 0)
                return s;
            else {
                const size_t a = m_child->find_first(s, end);
                if (s == a)
                    return s;
                else
                    s = a - 1;
            }
        }
        return end;
    }

protected:
    const Array& m_arr;
    size_t m_max;
    size_t m_next;
    size_t m_size;
};




/*
// This is a template NODE, used if you want to special-handle new data types for performance.

template <class T, class C, class F> class NODE: public ParentNode {
public:
    NODE(T v, size_t column) : m_value(v), m_column(column)  {}
    ~NODE() {delete m_child; }

    size_t find_first(size_t start, size_t end, const Table& table)
    {
        const C& column = (C&)(table.GetColumnBase(m_column));
        const F function = {};
        for (size_t s = start; s < end; ++s) {
            const T t = column.Get(s);
            if (function(t, m_value)) {
                if (m_child == 0)
                    return s;
                else {
                    const size_t a = m_child->find_first(s, end, table);
                    if (s == a)
                        return s;
                    else
                        s = a - 1;
                }
            }
        }
        return end;
    }

protected:
    T m_value;
    size_t m_column;
};
*/

// TODO: Not finished
class SUBTABLE: public ParentNode {
public:
    SUBTABLE(size_t column): m_column(column) {m_child2 = 0;}
    SUBTABLE() {};
    void Init(const Table& table)
    {
        m_table = &table;

        if (m_child) 
            m_child->Init(table);
        if (m_child2) 
            m_child2->Init(table);
    }

    size_t find_first(size_t start, size_t end)
    {
        TIGHTDB_ASSERT(m_table);
        TIGHTDB_ASSERT(m_child);

        for (size_t s = start; s < end; ++s) {
            const TableRef subtable = ((Table*)m_table)->get_subtable(m_column, s);

            m_child->Init(*subtable);
            const size_t subsize = subtable->size();
            const size_t sub = m_child->find_first(0, subsize);

            if (sub != subsize) {
                if (m_child2 == 0)
                    return s;
                else {
                    const size_t a = m_child2->find_first(s, end);
                    if (s == a)
                        return s;
                    else
                        s = a - 1;
                }
            }
        }
        return end;
    }
protected:
    friend class Query;
    ParentNode* m_child2;
    size_t m_column;
};


template <class T, class C, class F> class NODE: public ParentNode {
public:
    // NOTE: Be careful to call Array(false) constructors on m_array and m_array_agg in the initializer list, otherwise
    // their default constructors are called which are slow
    NODE(T v, size_t column) : m_value(v), m_leaf_start(0), m_leaf_end(0), m_local_end(0), m_array_agg(false), m_leaf_end_agg(0) {
        m_column_id = column;
        F f;
        m_cond = f.condition();
    }

    // Only purpose of this function is to let you quickly create a NODE object and call aggregate() on it to aggregate
    // on a single stand-alone column, with 1 or 0 search criterias, without involving any tables, etc. 
    // TODO, could be merged with Init somehow to simplify
    void QuickInit(Column *column, int64_t value) 
    {
        m_column = column; 
        m_leaf_end = 0;
        m_value = value;
    }

    // TODO, Make caller set m_column_id through this function instead of through constructor, to simplify code. 
    // This also allows to get rid of QuickInit()
    void Init(const Table& table)
    {
        m_column = (C*)&table.GetColumnBase(m_column_id);
        m_table = &table;
        m_leaf_end = 0;
        if (m_child)
            m_child->Init(table);
    }

    int64_t aggregate(Array* res, size_t start, size_t end, size_t limit, ACTION action = TDB_FINDALL, 
                      size_t agg_col = not_found, size_t* matchcount = 0) 
    {
        if (action == TDB_FINDALL)
            return aggregate<TDB_FINDALL>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_SUM)
            return aggregate<TDB_SUM>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_MAX)
            return aggregate<TDB_MAX>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_MIN)
            return aggregate<TDB_MIN>(res, start, end, limit, agg_col, matchcount);
        if (action == TDB_COUNT)
            return aggregate<TDB_COUNT>(res, start, end, limit, agg_col, matchcount);

        TIGHTDB_ASSERT(false);
        return uint64_t(-1);
    }

    // This function is called from Array::find() for each search result if action == TDB_CALLBACK_IDX
    // in the NODE::aggregate() call. Used if aggregate source column (column on which SUM or MAX or MIN or whatever is being performed) is different from search criteria column
    template <ACTION action>bool match_callback(int64_t v) {
        if (TO_SIZET(v) >= m_leaf_end_agg) {
            m_column_agg->GetBlock(TO_SIZET(v), m_array_agg, m_leaf_start_agg);
            const size_t leaf_size = m_array_agg.Size();
            m_leaf_end_agg = m_leaf_start_agg + leaf_size;
        }

        int64_t av = 0;        
        if (m_array.USES_VAL<action>()) // Compiler cannot see that Column::Get has no side effect and result is discarded
            av = m_array_agg.Get(TO_SIZET(v) - m_leaf_start_agg);
        bool b = m_array.FIND_ACTION<action>(TO_SIZET(v), av, &m_state, &tightdb_dummy);

        return b;
    }

    // res          destination array if action == TDB_FINDALL. Ignored if other action
    // m_column     Set in NODE constructor and is used as source for search criteria for this NODE
    // agg_col      column number in m_table which must act as source for aggreate action
    template <ACTION action> int64_t aggregate(Array* res, size_t start, size_t end, size_t limit, size_t agg_col, size_t* matchcount = 0) {
        F f;
        int c = f.condition();

        if (agg_col != not_found)
            m_column_agg = (C*)&m_table->GetColumnBase(agg_col);

        m_array.state_init(action, &m_state, res);

        // If query only has 1 criteria, and arrays have built-in intrinsics for it, then perform it directly on array
        if (m_child == 0 && limit > m_column->Size() && (SameType<F, EQUAL>::value || SameType<F, NOTEQUAL>::value || SameType<F, LESS>::value || SameType<F, GREATER>::value || SameType<F, NONE>::value)) {
            const Array* criteria_arr = NULL;
            for (size_t s = start; s < end; ) {
                // Cache internal leafs
                if (s >= m_leaf_end) {                    
                    criteria_arr = m_column->GetBlock(s, m_array, m_leaf_start, true);                                       
                    const size_t leaf_size = criteria_arr->Size();
                    m_leaf_end = m_leaf_start + leaf_size;
                    const size_t e = end - m_leaf_start;
                    m_local_end = leaf_size < e ? leaf_size : e;
                }

                if (agg_col == m_column_id || agg_col == size_t(-1))
                    criteria_arr->find(c, action, m_value, s - m_leaf_start, m_local_end, m_leaf_start, &m_state);
                else
                    criteria_arr->find<F, TDB_CALLBACK_IDX>(m_value, s - m_leaf_start, m_local_end, m_leaf_start, &m_state, std::bind1st(std::mem_fun(&NODE::match_callback<action>), this));
                    
                s = m_leaf_end;
            }

            if (action == TDB_FINDALL && res->Size() > limit) {
                res->Resize(limit); // todo, optimize by adding limit argument to find()
            }

            if (matchcount)
                *matchcount = int64_t(m_state.match_count);
            return m_state.state;

        }
        else {
            size_t r = start - 1;
            size_t n = 0;
            while (n < limit) {
                r = find_first(r + 1, end);
                if (r == end)
                    break;

                if (agg_col == m_column_id || agg_col == size_t(-1)) {
                    if (m_array.USES_VAL<action>()) // Compiler cannot see that Column::Get has no side effect and result is discarded
                        m_array.FIND_ACTION<action>(r, m_column->Get(r), &m_state, &tightdb_dummy);
                    else
                        m_array.FIND_ACTION<action>(r, 0, &m_state, &tightdb_dummy);
                }
                else
                    match_callback<action>(r);
                n++;
            } 
        }
        
        if (matchcount)
            *matchcount = int64_t(m_state.match_count);
        return m_state.state;
    }

    size_t find_first(size_t start, size_t end)
    {
        TIGHTDB_ASSERT(m_table);

        for (size_t s = start; s < end; ++s) {
            // Cache internal leafs
            if (s >= m_leaf_end) {
                m_column->GetBlock(s, m_array, m_leaf_start);
                const size_t leaf_size = m_array.Size();
                m_leaf_end = m_leaf_start + leaf_size;
                const size_t e = end - m_leaf_start;
                m_local_end = leaf_size < e ? leaf_size : e;
            }
            
            // Do search directly on cached leaf array
            s = m_array.find_first<F>(m_value, s - m_leaf_start, m_local_end);

            if (s == not_found) {
                s = m_leaf_end - 1;
                continue;
            }
            else
                s += m_leaf_start;

            if (m_child == 0)
                return s;
            else {
                const size_t a = m_child->find_first(s, end);
                if (s == a)
                    return s;
                else
                    s = a - 1;
            }
        }
        return end;
    }

protected:
    T m_value;

protected:

    C* m_column;                // Column on which search criteria is applied
    C *m_column_agg;            // Column on which aggregate function is executed (can be same as m_column)
    size_t m_leaf_start;
    size_t m_leaf_end;
    size_t m_local_end;

    Array m_array_agg;
    size_t m_leaf_start_agg;
    size_t m_leaf_end_agg;
    size_t m_local_end_agg;
};


template <class F> class STRINGNODE: public ParentNode {
public:
    template <ACTION action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {TIGHTDB_ASSERT(false); return 0;}

    STRINGNODE(const char* v, size_t column)
    {
        m_column_id = column;

        m_value = (char *)malloc(strlen(v)*6);
        memcpy(m_value, v, strlen(v) + 1);
        m_ucase = (char *)malloc(strlen(v)*6);
        m_lcase = (char *)malloc(strlen(v)*6);

        const bool b1 = utf8case(v, m_lcase, false);
        const bool b2 = utf8case(v, m_ucase, true);
        if (!b1 || !b2)
            m_error_code = "Malformed UTF-8: " + std::string(m_value);
    }
    ~STRINGNODE() {
        free((void*)m_value); free((void*)m_ucase); free((void*)m_lcase);
    }

    void Init(const Table& table)
    {
        m_table = &table;
        m_column = &table.GetColumnBase(m_column_id);
        m_column_type = table.GetRealColumnType(m_column_id);

        if (m_child) 
            m_child->Init(table);
    }

    size_t find_first(size_t start, size_t end)
    {
        F function;

        for (size_t s = start; s < end; ++s) {
            const char* t;

            // todo, can be optimized by placing outside loop
            if (m_column_type == COLUMN_TYPE_STRING)
                t = ((const AdaptiveStringColumn*)m_column)->Get(s);
            else {
                //TODO: First check if string is in key list
                t = ((const ColumnStringEnum*)m_column)->Get(s);
            }

            if (function(m_value, m_ucase, m_lcase, t)) {
                if (m_child == 0)
                    return s;
                else {
                    const size_t a = m_child->find_first(s, end);
                    if (s == a)
                        return s;
                    else
                        s = a - 1;
                }
            }
        }
        return end;
    }

protected:
    char* m_value;
    char* m_lcase;
    char* m_ucase;
    const ColumnBase* m_column;
    ColumnType m_column_type;
};



template <> class STRINGNODE<EQUAL>: public ParentNode {
public:
    template <ACTION action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {TIGHTDB_ASSERT(false); return 0;}

    STRINGNODE(const char* v, size_t column): m_key_ndx((size_t)-1) {
        m_column_id = column;
        m_value = (char *)malloc(strlen(v)*6);
        memcpy(m_value, v, strlen(v) + 1);
    }
    ~STRINGNODE() {
        free((void*)m_value);
    }

    void Init(const Table& table)
    {
        m_table = &table;
        m_column = &table.GetColumnBase(m_column_id);
        m_column_type = table.GetRealColumnType(m_column_id);

        if (m_column_type == COLUMN_TYPE_STRING_ENUM) {
            m_key_ndx = ((const ColumnStringEnum*)m_column)->GetKeyNdx(m_value);
        }

        if (m_column->HasIndex()) {
            ((AdaptiveStringColumn*)m_column)->find_all(m_index, m_value);
            m_has_index = true;
            m_last_indexed = 0;
        }
        else
            m_has_index = false;

        if (m_child) 
            m_child->Init(table);
    }

    size_t find_first(size_t start, size_t end)
    {
        TIGHTDB_ASSERT(m_table);

        for (size_t s = start; s < end; ++s) {

            if (m_has_index) {
                for(;;) {
                    if (m_last_indexed >= m_index.Size()) {
                        s = not_found;
                        break;
                    }
                    size_t cand = m_index.GetAsSizeT(m_last_indexed);
                    ++m_last_indexed;
                    if(cand >= s && cand < end) {
                        s = cand;
                        break;
                    }
                    else if(cand >= end) {
                        s = not_found;
                        break;
                    }
                }
            }
            else {

                // todo, can be optimized by placing outside loop
                if (m_column_type == COLUMN_TYPE_STRING)
                    s = ((const AdaptiveStringColumn*)m_column)->find_first(m_value, s, end);
                else {
                    if (m_key_ndx == size_t(-1)) s = end; // not in key set
                    else {
                        const ColumnStringEnum* const cse = (const ColumnStringEnum*)m_column;
                        s = cse->find_first(m_key_ndx, s, end);
                    }
                }
            }

            if (s == (size_t)-1)
                s = end;

            if (m_child == 0)
                return s;
            else {
                const size_t a = m_child->find_first(s, end);
                if (s == a)
                    return s;
                else
                    s = a - 1;
            }
        }
        return end;
    }

protected:
    char*  m_value;
    bool   m_has_index;
    size_t m_last_indexed;

private:
    const ColumnBase* m_column;
    ColumnType m_column_type;
    size_t m_key_ndx;
    Array m_index;
};


class OR_NODE: public ParentNode {
public:
    template <ACTION action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {TIGHTDB_ASSERT(false); return 0;}

    OR_NODE(ParentNode* p1) : m_table(NULL) {m_cond1 = p1; m_cond2 = NULL;};

    void Init(const Table& table)
    {
        m_cond1->Init(table);
        m_cond2->Init(table);

        if (m_child)
            m_child->Init(table);

        m_last1 = size_t(-1);
        m_last2 = size_t(-1);

        m_table = &table;
    }

// Keep old un-optimized or code until new has been sufficiently tested
#if 0
    size_t find_first(size_t start, size_t end)
    {
        for (size_t s = start; s < end; ++s) {
            // Todo, redundant searches can occur
            // We have to init here to reset nodes internal
            // leaf cache (since we may go backwards)
            m_cond1->Init(*m_table);
            m_cond2->Init(*m_table);
            const size_t f1 = m_cond1->find_first(s, end);
            const size_t f2 = m_cond2->find_first(s, end);
            s = f1 < f2 ? f1 : f2;

            if (m_child == 0)
                return s;
            else {
                const size_t a = m_child->find_first(s, end);
                if (s == a)
                    return s;
                else
                    s = a - 1;
            }
        }
        return end;
    }
#else
    size_t find_first(size_t start, size_t end)
    {
        for (size_t s = start; s < end; ++s) {
            size_t f1;
            size_t f2;

            if (m_last1 >= s && m_last1 != (size_t)-1)
                f1 = m_last1;
            else {
                f1 = m_cond1->find_first(s, end);
                m_last1 = f1;
            }

            if (m_last2 >= s && m_last2 != (size_t)-1)
                f2 = m_last2;
            else {
                f2 = m_cond2->find_first(s, end);
                m_last2 = f2;
            }
            s = f1 < f2 ? f1 : f2;

            if (m_child == 0)
                return s;
            else {
                const size_t a = m_child->find_first(s, end);
                if (s == a)
                    return s;
                else
                    s = a - 1;
            }
        }
        return end;
    }
#endif


    virtual std::string Verify(void)
    {
        if (m_error_code != "")
            return m_error_code;
        if (m_cond1 == 0)
            return "Missing left-hand side of OR";
        if (m_cond2 == 0)
            return "Missing right-hand side of OR";
        std::string s;
        if (m_child != 0)
            s = m_child->Verify();
        if (s != "")
            return s;
        s = m_cond1->Verify();
        if (s != "")
            return s;
        s = m_cond2->Verify();
        if (s != "")
            return s;
        return "";
    }

    ParentNode* m_cond1;
    ParentNode* m_cond2;
private:
    size_t m_last1;
    size_t m_last2;
    const Table* m_table;
};


} // namespace tightdb

#endif // TIGHTDB_QUERY_ENGINE_HPP
