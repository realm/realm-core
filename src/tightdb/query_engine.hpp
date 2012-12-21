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
#include <tightdb/column_binary.hpp>
#include <tightdb/utf8.hpp>
#include <tightdb/query_conditions.hpp>


namespace tightdb {

typedef bool (*CallbackDummy)(int64_t);

class SequentialGetter {
public:
    SequentialGetter(const Table& table, size_t column) 
    {
        if(column != not_found)
            m_column = (Column*)&table.GetColumnBase(column);
        m_leaf_end = 0;
    }

    inline int64_t GetNext(size_t index)
    {
        if (index >= m_leaf_end) {
            m_array = (Array*)(m_column->GetBlock(index, *m_array, m_leaf_start, true));
            const size_t leaf_size = m_array->Size();
            m_leaf_end = m_leaf_start + leaf_size;
        }

        int64_t av = m_array->Get(index - m_leaf_start);
        return av;
    }

    size_t m_leaf_start;
    size_t m_leaf_end;
    Column* m_column;
    Array* m_array;
};

// Distance between matches where performance no longer increases 
const size_t bestdist = 100;

class ParentNode {
public:
    ParentNode() : cond(-1), m_table(NULL) {}

    std::vector<ParentNode*> gather_children(std::vector<ParentNode*> v) {
        m_children.clear();
        ParentNode* p = this;
        size_t i = v.size();
        v.push_back(this);
        p = p->child_criteria();

        if (p != NULL)
            v = p->gather_children(v);

        m_children = v;
        m_children.erase(m_children.begin() + i);
        m_children.insert(m_children.begin(), this);

        m_conds = m_children.size();        
        return v;                              
    }

    size_t find_first(size_t start, size_t end) {
        
        size_t m = 0;
        size_t next_cond = 0;
        size_t first_cond = 0;

        while(start < end) {
            
            m = m_children[next_cond]->find_first_local(start, end);

            next_cond++;
            if (next_cond == m_conds)
                next_cond = 0;

            if (m == start) {
                if (next_cond == first_cond)
                    return m;
            }
            else {
                first_cond = next_cond;
                start = m;
            }
        }

        return end;
    }


    virtual ~ParentNode() {}
    virtual void Init(const Table& table) {
        m_table = &table; 
        if (m_child) 
            m_child->Init(table);
    }
    virtual size_t find_first_local(size_t start, size_t end) = 0;
    virtual ParentNode* child_criteria(void) {
        return m_child;
    }


    struct score_compare {
        bool operator ()(ParentNode const* a, ParentNode const* b) const { return (a->cost() < b->cost()); }
    };

    double cost(void) const
    {
        return 16.0 / m_dD + m_dT;
    }

    template<ACTION action> int64_t aggregate(state_state* st, size_t start, size_t end, size_t agg_col2 = not_found, size_t* matchcount = 0) 
    {
        if(end == size_t(-1)) end = m_table->size();

        SequentialGetter* agg_col = NULL;
        
        if(agg_col2 != not_found)
            agg_col = new SequentialGetter(*m_table, agg_col2);

        size_t td;

        while(start < end) {
            size_t best = std::distance(m_children.begin(), std::min_element(m_children.begin(), m_children.end(), score_compare()));

            // Find a large amount of local matches in best condition
            td = m_children[best]->m_dT == 0.0 ? end : (start + 1000 > end ? end : start + 1000);
            start = (size_t)m_children[best]->aggregate_local(st, start, td, 16, action, agg_col, matchcount);

            // Make remaining conditions compute their m_dD (statistics)
            for(size_t c = 0; c < m_children.size() && start < end; c++) {
                if(c == best)
                    continue;

                // Skip test if there is no way its cost can ever be better than best node's
                double cost = m_children[c]->cost(); // cost() = 16.0 / m_dD + m_dT;
                if(m_children[c]->m_dT < cost) {
                    size_t maxN = 2;

                    // Limit to +256 in order not to skip too large parts of index nodes
                    size_t maxD = m_children[c]->m_dT == 0.0 ? end - start : 256;
                    td = m_children[c]->m_dT == 0.0 ? end : (start + maxD > end ? end : start + maxD);
                    start = (size_t)m_children[c]->aggregate_local(st, start, td, maxN, action, agg_col, matchcount);
                }
            }

        }

        if(matchcount != 0)
            *matchcount = st->match_count;
        return st->state;

    }

    virtual int64_t aggregate_local(state_state* st, size_t start, size_t end, size_t local_limit, ACTION action = TDB_FINDALL, SequentialGetter* agg_col = NULL, size_t* matchcount = 0) 
    {
		// aggregate called on non-integer column type. Speed of this function is not as critical as speed of the integer version, because
        // find_first_local() is relatively slower here (because it's non-integers).
		(void)matchcount;
		(void)agg_col;
        size_t local_matches = 0;

        size_t r = start - 1;
        for (;;) {
            if(local_matches == local_limit) { 
                m_dD = (r - start) / local_matches; 
                return r + 1;
            }

            r = find_first_local(r + 1, end);
            if (r == end) { 
                m_dD = (r - start) / local_matches; 
                return end;
            }

            local_matches++;

            size_t m = r;
            for (size_t c = 1; c < m_conds; c++) {
                m = m_children[c]->find_first_local(r, r + 1);
                if (m != r) {
                    break;
                }
            }

            if (m == r) {
                int64_t av = 0;
                if(agg_col != NULL)
                    av = agg_col->GetNext(r); // todo, avoid getting if not needed (if !uses_val)
                
                if (action == TDB_RETURN_FIRST)
                    st->state_match<TDB_RETURN_FIRST, 0>(r, 0, av, CallbackDummy());
                else if (action == TDB_CALLBACK_IDX)
                    st->state_match<TDB_CALLBACK_IDX, 0>(r, 0, av, CallbackDummy());
                else if (action == TDB_COUNT)
                   st->state_match<TDB_COUNT, 0>(r, 0, av, CallbackDummy());
                else if (action == TDB_FINDALL)
                   st->state_match<TDB_FINDALL, 0>(r, 0, av, CallbackDummy());
                else if (action == TDB_MAX)
                   st->state_match<TDB_MAX, 0>(r, 0, av, CallbackDummy());
                else if (action == TDB_MIN)
                   st->state_match<TDB_MIN, 0>(r, 0, av, CallbackDummy());
                else if (action == TDB_SUM)
                   st->state_match<TDB_SUM, 0>(r, 0, av, CallbackDummy());
                else
                    TIGHTDB_ASSERT(false);
             }   
        }
    }


    virtual std::string Verify(void)
    {
        if (error_code != "")
            return error_code;
        if (m_child == 0)
            return "";
        else
            return m_child->Verify();
    }

    ParentNode* m_child;
    std::vector<ParentNode*>m_children;
    int cond;
    size_t m_column_id;
    size_t m_conds;
    double m_dD; // Average row distance between each local match at current position
    double m_dT; // time overhead testing index i + 1 after testing index i. > 1 for linear scans, 0 for index/tableview

    size_t m_probes;
    size_t m_matches;

protected:
    const Table* m_table;
    std::string error_code;

    SequentialGetter* m_column_agg;
};


class ARRAYNODE: public ParentNode {
public:
    ARRAYNODE(const Array& arr) : m_arr(arr), m_max(0), m_next(0), m_size(arr.Size()) {m_child = 0;}

    void Init(const Table& table)
    {
        m_table = &table;

        m_dT = 0.0;
        m_dD =  m_table->size() / (m_arr.Size() + 1);
        m_probes = 0;
        m_matches = 0;

        m_next = 0;
        if (m_size > 0)
            m_max = m_arr.Get(m_size - 1);
        if (m_child) m_child->Init(table);
    }

    size_t find_first_local(size_t start, size_t end)
    {
        size_t r = m_arr.FindGTE(start, m_next);
        if(r == not_found)
            return end;

        m_next = r;
        return m_arr.Get(r);
    }

protected:
    const Array& m_arr;
    size_t m_max;
    size_t m_next;
    size_t m_size;

};


// Not finished
class SUBTABLE: public ParentNode {
public:
    SUBTABLE(size_t column): m_column(column) {m_child = 0; m_child2 = 0;}
    SUBTABLE() {};
    void Init(const Table& table)
    {
        m_dT = 100.0;
        m_dD = 10.0;
        m_probes = 0;
        m_matches = 0;

        m_table = &table;
        
        if (m_child) {
            m_child->Init(table);
            std::vector<ParentNode*>v;
            m_child->gather_children(v);
        }

        if (m_child2) m_child2->Init(table);
    }
    
    size_t find_first_local(size_t start, size_t end)
    {
        TIGHTDB_ASSERT(m_table);
        TIGHTDB_ASSERT(m_child);

        for (size_t s = start; s < end; ++s) {
            const TableRef subtable = ((Table*)m_table)->get_subtable(m_column, s);

            m_child->Init(*subtable);
            const size_t subsize = subtable->size();
            const size_t sub = m_child->find_first(0, subsize);

            if (sub != subsize)
                return s;           
        }
        return end;
    }

    ParentNode* child_criteria(void) {
        return m_child2;
    }

    ParentNode* m_child2;
    size_t m_column;
};

// NODE is for conditions for types stored as integers in a tightdb::Array (int, date, bool)
template <class T, class C, class F> class NODE: public ParentNode {
public:
    // NOTE: Be careful to call Array(false) constructors on m_array and m_array_agg in the initializer list, otherwise
    // their default constructors are called which are slow
    NODE(T v, size_t column) : m_value(v), m_array(false),m_leaf_start(0), m_leaf_end(0), m_local_end(0) {
        m_column_id = column;
        m_child = 0;
        F f;
        cond = f.condition();
        m_conds = 0;
    }


    // Only purpose of this function is to let you quickly create a NODE object and call aggregate_local() on it to aggregate
    // on a single stand-alone column, with 1 or 0 search criterias, without involving any tables, etc. Todo, could
    // be merged with Init somehowt to simplify
    void QuickInit(Column *column, int64_t value) {
        m_dT = 1.0;
        m_dD = 100.0;
        m_probes = 0;
        m_matches = 0;

        m_column = column; 
        m_leaf_end = 0;
        m_value = value;
    }

    // Todo, Make caller set m_column_id through this function instead of through constructor, to simplify code. 
    // This also allows to get rid of QuickInit()
    void Init(const Table& table)
    {
        m_dT = 1.0 / 8.0;
        m_dD = 10.0;
        m_probes = 0;
        m_matches = 0;

        m_column = (C*)&table.GetColumnBase(m_column_id);
        m_table = &table;
        m_leaf_end = 0;
        if (m_child)m_child->Init(table);
    }

    int64_t aggregate_local(state_state* st, size_t start, size_t end, size_t local_limit, ACTION action = TDB_FINDALL, SequentialGetter* agg_col = NULL, size_t* matchcount = 0) {
        if (action == TDB_FINDALL)
            return aggregate_local<TDB_FINDALL>(st, start, end, local_limit, agg_col, matchcount);
        else if (action == TDB_SUM)
            return aggregate_local<TDB_SUM>(st, start, end, local_limit, agg_col, matchcount);
        else if (action == TDB_MAX)
            return aggregate_local<TDB_MAX>(st, start, end, local_limit, agg_col, matchcount);
        else if (action == TDB_MIN)
            return aggregate_local<TDB_MIN>(st, start, end, local_limit, agg_col, matchcount);
        else if (action == TDB_COUNT)
            return aggregate_local<TDB_COUNT>(st, start, end, local_limit, agg_col, matchcount);
        else
            TIGHTDB_ASSERT(false);

        assert(false);
        return uint64_t(-1);
    }

    // This function is called from Array::find() for each search result if action == TDB_CALLBACK_IDX
    // in the NODE::aggregate_local() call. Used if aggregate source column is different from search criteria column
    template <ACTION action>bool match_callback(int64_t v) {
        size_t i = to_size_t(v);
        m_last_local_match = i;
        m_local_matches++;

        // Test remaining sub conditions of this node. m_children[0] is the node that called match_callback(), so skip it
        for (size_t c = 1; c < m_conds; c++) {
            m_children[c]->m_probes++;
            size_t m = m_children[c]->find_first_local(i, i + 1);
            if (m != i)
                return (m_local_matches != m_local_limit);
        }

        int64_t av = NULL;        
        if (m_state->uses_val<action>()) // Compiler cannot see that Column::Get has no side effect and result is discarded         
            av = m_column_agg->GetNext(i);

        bool b = m_state->state_match<action, false>(i, 0, av, CallbackDummy());  

        if(m_local_matches == m_local_limit)
            return false;
        else
            return b;
    }

    // m_column     Set in NODE constructor and is used as source for search criteria for this NODE
    // agg_col      column number in m_table which must act as source for aggreate action
    template <ACTION action> int64_t aggregate_local(state_state* st, size_t start, size_t end, size_t local_limit, SequentialGetter* agg_col, size_t* matchcount = 0) {
        F f;
        int c = f.condition();
       
        m_local_matches = 0;
        m_local_limit = local_limit;
        m_last_local_match = start - 1;

        m_column_agg = agg_col;

        m_state = st;
        for (size_t s = start; s < end; ) {    
            // Cache internal leafs
            if (s >= m_leaf_end) {                    
                m_column->GetBlock(s, m_array, m_leaf_start);                                       
                m_leaf_end = m_leaf_start + m_array.Size();
                size_t w = m_array.GetBitWidth();
                m_dT = (w == 0 ? 1.0 / MAX_LIST_SIZE : w / 8.0); // todo, define what width must take "1" constant-time unit. Now it's 8 bit
            }

            size_t end2;
            if(end > m_leaf_end)
                end2 = m_leaf_end - m_leaf_start;
            else
                end2 = end - m_leaf_start;

            if (m_conds <= 1 && agg_col == NULL)
                m_array.find(c, action, m_value, s - m_leaf_start, end2, m_leaf_start, st);
            else
                m_array.find<F, TDB_CALLBACK_IDX>(m_value, s - m_leaf_start, end2, m_leaf_start, st, std::bind1st(std::mem_fun(&NODE::match_callback<action>), this));
                    
            if(m_local_matches == m_local_limit)
                break;

            s = end2 + m_leaf_start;
        }

        if (matchcount)
            *matchcount = int64_t(st->match_count);

        if(m_local_matches == m_local_limit) {
            m_dD = (m_last_local_match + 1 - start) / (m_local_matches + 1);
            return m_last_local_match + 1;
        }
        else {
            m_dD = (end - start) / (m_local_matches + 1);
            return end;
        }
    }

    size_t find_first_local(size_t start, size_t end)
    {
        F function;
        TIGHTDB_ASSERT(m_table);

        while(start < end) {

            // Cache internal leafs
            if (start >= m_leaf_end) {
                m_column->GetBlock(start, m_array, m_leaf_start);
                m_leaf_end = m_leaf_start + m_array.Size();
            }
            
            // Do search directly on cached leaf array
            if (start + 1 == end) {
                if (function(m_array.Get(start - m_leaf_start), m_value))
                    return start;
                else
                    return end;
            }

            size_t end2;
            if(end > m_leaf_end)
                end2 = m_leaf_end - m_leaf_start;
            else
                end2 = end - m_leaf_start;

            size_t s = m_array.find_first<F>(m_value, start - m_leaf_start, end2);

            if (s == not_found) {
                start = m_leaf_end;
                continue;
            }
            else
                return s + m_leaf_start;
        }  


        return end;
    }

    T m_value;

protected:

    state_state *m_state;
    size_t m_last_local_match;
    C* m_column;                // Column on which search criteria is applied
    const Array* criteria_arr;
    Array m_array;              
    size_t m_leaf_start;
    size_t m_leaf_end;
    size_t m_local_end;

    size_t m_local_matches;
    size_t m_local_limit;

};


template <class F> class STRINGNODE: public ParentNode {
public:
    template <ACTION action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {assert(false); return 0;}

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
        m_dT = 10.0;
        m_dD = 10.0;
        m_probes = 0;
        m_matches = 0;

        m_table = &table;
        m_column = &table.GetColumnBase(m_column_id);
        m_column_type = table.GetRealColumnType(m_column_id);

        if (m_child) m_child->Init(table);
    }

    size_t find_first_local(size_t start, size_t end)
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

            if (function(m_value, m_ucase, m_lcase, t))
                return s;
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




template <class F> class BINARYNODE: public ParentNode {
public:
    template <ACTION action> int64_t find_all(Array* /*res*/, size_t /*start*/, size_t /*end*/, size_t /*limit*/, size_t /*agg_col*/) {TIGHTDB_ASSERT(false); return 0;}

    BINARYNODE(const char* v, size_t len, size_t column)
    {
        m_column_id = column;
        m_child = 0;
        m_len = len;
        m_value = (char *)malloc(len);
        memcpy(m_value, v, len);
    }
    ~BINARYNODE() {
        free((void*)m_value);
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
            const char* t = ((const ColumnBinary*)m_column)->Get(s).pointer;
            size_t len2 = ((const ColumnBinary*)m_column)->Get(s).len;

            if (function(m_value, m_len, t, len2)) {
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

    size_t find_first_local(size_t start, size_t end) {
        assert(false);
        (void)start;
        (void)end;
        return 0;
    }

protected:
    char* m_value;
    size_t m_len;
    const ColumnBase* m_column;
    ColumnType m_column_type;
};


template <> class STRINGNODE<EQUAL>: public ParentNode {
public:
    template <ACTION action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {assert(false); return 0;}

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
        m_dD = 10.0;
        
        m_table = &table;
        m_column = &table.GetColumnBase(m_column_id);
        m_column_type = table.GetRealColumnType(m_column_id);

        if (m_column_type == COLUMN_TYPE_STRING_ENUM) {
            m_dT = 1.0;
            m_key_ndx =  ((const ColumnStringEnum*)m_column)->GetKeyNdx(m_value);
        }
        else {
            m_dT = 10.0;
        }

        if(m_column->HasIndex()) {
            if(m_column_type == COLUMN_TYPE_STRING_ENUM)
                ((ColumnStringEnum*)m_column)->find_all(m_index, m_value);
            else {
                ((AdaptiveStringColumn*)m_column)->find_all(m_index, m_value);
            }
            last_indexed = 0;
        }

        if (m_child) m_child->Init(table);
    }

    size_t find_first_local(size_t start, size_t end)
    {
        TIGHTDB_ASSERT(m_table);

        for (size_t s = start; s < end; ++s) {
            if(m_column->HasIndex()) {
                size_t f = m_index.FindGTE(s, last_indexed);
                if(f != not_found)
                    s = m_index.Get(f);
                else
                    s = not_found;
                last_indexed = f;
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
            return s;
        }
        return end;
    }

protected:
    char*  m_value;

private:
    const ColumnBase* m_column;
    ColumnType m_column_type;
    size_t m_key_ndx;
    Array m_index;
    size_t last_indexed;
};


class OR_NODE: public ParentNode {
public:
    template <ACTION action> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t agg_col) {assert(false); return 0;}

    OR_NODE(ParentNode* p1) : m_table(NULL) {m_child = NULL; m_cond[0] = p1; m_cond[1] = NULL;};


    void Init(const Table& table)
    {
        m_dT = 50.0;
        m_dD = 10.0;

        std::vector<ParentNode*>v;

        for(size_t c = 0; c < 2; ++c) {
            m_cond[c]->Init(table);
            m_cond[c]->gather_children(v);
            m_last[c] = 0;
            m_was_match[c] = false;
        }

        if (m_child)
            m_child->Init(table);

        m_table = &table;
    }

    size_t find_first_local(size_t start, size_t end)
    {
        for (size_t s = start; s < end; ++s) {
            size_t f[2];

            for(size_t c = 0; c < 2; ++c) {
                if(m_last[c] >= end)
                    f[c] = end;
                else if(m_was_match[c] && m_last[c] >= s)
                    f[c] = m_last[c];
                else {
                    size_t fmax = m_last[c] > s ? m_last[c] : s;
                    f[c] = m_cond[c]->find_first(fmax, end);
                    m_was_match[c] = (f[c] != end);
                    m_last[c] = f[c];
                }
            }

            s = f[0] < f[1] ? f[0] : f[1];
            s = s > end ? end : s;

            return s;
        }
        return end;
    }

    virtual std::string Verify(void)
    {
        if (error_code != "")
            return error_code;
        if (m_cond[0] == 0)
            return "Missing left-hand side of OR";
        if (m_cond[1] == 0)
            return "Missing right-hand side of OR";
        std::string s;
        if (m_child != 0)
            s = m_child->Verify();
        if (s != "")
            return s;
        s = m_cond[0]->Verify();
        if (s != "")
            return s;
        s = m_cond[1]->Verify();
        if (s != "")
            return s;
        return "";
    }

    ParentNode* m_cond[2];
private:
    size_t m_last[2];

    bool m_was_match[2];

    const Table* m_table;
};


} // namespace tightdb

#endif // TIGHTDB_QUERY_ENGINE_HPP
