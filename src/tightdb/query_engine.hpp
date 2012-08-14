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
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/utf8.hpp>
#include <tightdb/query_conditions.hpp>

namespace tightdb {


class ParentNode {
public:
    ParentNode() : m_table(NULL), cond(-1) {}
    virtual ~ParentNode() {

    }
    virtual void Init(const Table& table) {m_table = &table; if (m_child) m_child->Init(table);}
    virtual size_t find_first(size_t start, size_t end) = 0;

    virtual int64_t aggregate(Array* res, size_t start, size_t end, size_t limit, int action = TDB_ACCUMULATE, size_t agg_col = not_found) 
    {
        size_t r = start - 1;
        size_t count = 0;
        for(;;) {
            r = find_first(r + 1, end);
            if (r == end || count == limit)
                break;
            count++;

            // Only aggregate function available for non-integer nodes are accumulate and count
            if(action == TDB_ACCUMULATE)
                res->add(r); // todo, AddPositiveLocal
        }

        return count;
    }

    virtual std::string Verify(void)
    {
        if(error_code != "")
            return error_code;
        if(m_child == 0)
            return "";
        else
            return m_child->Verify();
    };

    ParentNode* m_child;
    int cond;
    size_t m_column_id;

protected:
    const Table* m_table;
    std::string error_code;
};



/*
template <class T, class C, class F> class NODE: public ParentNode {
public:
    NODE(T v, size_t column) : m_value(v), m_column(column)  {m_child = 0;}
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

// Not finished
class SUBTABLE: public ParentNode {
public:
    SUBTABLE(size_t column): m_column(column) {m_child = 0; m_child2 = 0;}
    SUBTABLE() {};
    void Init(const Table& table)
    {
        m_table = &table;

        if (m_child) m_child->Init(table);
        if (m_child2) m_child2->Init(table);
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

            if(sub != subsize) {
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

    ParentNode* m_child2;
    size_t m_column;
};


template <class T, class C, class F> class NODE: public ParentNode {
public:
    NODE(T v, size_t column) : m_array(GetDefaultAllocator()), m_leaf_start(0), m_leaf_end(0), m_local_end(0), m_value(v), m_leaf_end_agg(0) {
        m_column_id = column;
        m_child = 0;
        F f;
        cond = f.condition();

    }

    void Init(const Table& table)
    {
        m_table = &table;
        m_column = (C*)&table.GetColumnBase(m_column_id);
        m_leaf_end = 0;

        if (m_child)m_child->Init(table);
    }


    int64_t aggregate(Array* res, size_t start, size_t end, size_t limit, int action = TDB_ACCUMULATE, size_t agg_col = not_found) {
        if(action == TDB_ACCUMULATE)
            return aggregate<TDB_ACCUMULATE>(res, start, end, limit, agg_col);
        if(action == TDB_SUM)
            return aggregate<TDB_SUM>(res, start, end, limit, agg_col);
        if(action == TDB_MAX)
            return aggregate<TDB_MAX>(res, start, end, limit, agg_col);
        if(action == TDB_MIN)
            return aggregate<TDB_MIN>(res, start, end, limit, agg_col);
        if(action == TDB_COUNT)
            return aggregate<TDB_COUNT>(res, start, end, limit, agg_col);

        assert(false);
        return uint64_t(-1);
    }


    template <int action>bool match_callback(int64_t v) {
        if (TO_SIZET(v) >= m_leaf_end_agg) {
            m_column_agg->GetBlock(TO_SIZET(v), m_array_agg, m_leaf_start_agg);
            const size_t leaf_size = m_array_agg.Size();
            m_leaf_end_agg = m_leaf_start_agg + leaf_size;
        }

        int64_t av = m_array_agg.Get(TO_SIZET(v) - m_leaf_start_agg);
        bool b = m_array.FIND_ACTION<action>(TO_SIZET(v), av, &state, &tdb_dummy);

        return b;
    }

    template <int action> int64_t aggregate(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {
        F f;
        int c = f.condition();
        Array rr;

        C* m_column = (C*)&m_table->GetColumnBase(m_column_id);
        if(agg_col != not_found)
            m_column_agg = (C*)&m_table->GetColumnBase(agg_col);

        rr.state_init(action, &state);
        state.state = (int64_t)res;

        if(m_child == 0 && (SameType<F, EQUAL>::value || SameType<F, NOTEQUAL>::value || SameType<F, LESS>::value || SameType<F, GREATER>::value || SameType<F, NONE>::value)) {
            
            for (size_t s = start; s < end; ) {
                // Cache internal leafs
                if (s >= m_leaf_end) {
                    m_column->GetBlock(s, m_array, m_leaf_start);
                    const size_t leaf_size = m_array.Size();
                    m_leaf_end = m_leaf_start + leaf_size;
                    m_local_end = leaf_size;
                }

                if(agg_col == m_column_id || agg_col == -1)
                    m_array.find(c, action, m_value, s - m_leaf_start, m_local_end, m_leaf_start, &state);
                else
                    m_array.find<F, TDB_CALLBACK_IDX>(m_value, s - m_leaf_start, m_local_end, m_leaf_start, &state, std::bind1st(std::mem_fun(&NODE::match_callback<action>), this));
                    
                s = m_leaf_end;
            }

            if(action == TDB_ACCUMULATE && res->Size() > limit) {
                res->Resize(limit); // todo, optimize by adding limit argument to find_all()
            }

            return state.state;

        }
        else {
            size_t r = start - 1;
            for(;;) {
                r = find_first(r + 1, end);
                if (r == end || res->Size() == limit) // todo, optimize away Size() call
                    break;

                if(agg_col == m_column_id || agg_col == -1)
                    m_array.FIND_ACTION<action>(r, m_column->Get(r), &state, &tdb_dummy);
                else
                    match_callback<action>(r);
            } 
        }
        
        return state.state;
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
                s = m_leaf_end-1;
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

    T m_value;

protected:
    state_state state;

    C* m_column;
    C *m_column_agg;
    Array m_array;
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
    template <int action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {assert(false); return 0;};

    STRINGNODE(const char* v, size_t column)
    {
        m_column_id = column;
        m_child = 0;

        m_value = (char *)malloc(strlen(v)*6);
        memcpy(m_value, v, strlen(v) + 1);
        m_ucase = (char *)malloc(strlen(v)*6);
        m_lcase = (char *)malloc(strlen(v)*6);

        const bool b1 = utf8case(v, m_lcase, false);
        const bool b2 = utf8case(v, m_ucase, true);
        if (!b1 || !b2)
            error_code = "Malformed UTF-8: " + std::string(m_value);
    }
    ~STRINGNODE() {
        free((void*)m_value); free((void*)m_ucase); free((void*)m_lcase);
    }

    void Init(const Table& table)
    {
        m_table = &table;
        m_column = &table.GetColumnBase(m_column_id);
        m_column_type = table.GetRealColumnType(m_column_id);

        if (m_child) m_child->Init(table);
    }

    size_t find_first(size_t start, size_t end)
    {
        F function;// = {};

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
    template <int action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {assert(false); return 0;};

    STRINGNODE(const char* v, size_t column): m_key_ndx((size_t)-1) {
        m_column_id = column;
        m_child = 0;
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
            m_key_ndx =  ((const ColumnStringEnum*)m_column)->GetKeyNdx(m_value);
        }

        if (m_child) m_child->Init(table);
    }

    size_t find_first(size_t start, size_t end)
    {
        TIGHTDB_ASSERT(m_table);

        for (size_t s = start; s < end; ++s) {
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

private:
    const ColumnBase* m_column;
    ColumnType m_column_type;
    size_t m_key_ndx;
};


class OR_NODE: public ParentNode {
public:
    template <int action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {assert(false); return 0;};

    OR_NODE(ParentNode* p1) : m_table(NULL) {m_child = NULL; m_cond1 = p1; m_cond2 = NULL;};


    void Init(const Table& table)
    {
        m_cond1->Init(table);
        m_cond2->Init(table);

        if(m_child)
            m_child->Init(table);

        m_last1 = -1;
        m_last2 = -1;

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
        if(error_code != "")
            return error_code;
        if(m_cond1 == 0)
            return "Missing left-hand side of OR";
        if(m_cond2 == 0)
            return "Missing right-hand side of OR";
        std::string s;
        if(m_child != 0)
            s = m_child->Verify();
        if(s != "")
            return s;
        s = m_cond1->Verify();
        if(s != "")
            return s;
        s = m_cond2->Verify();
        if(s != "")
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
