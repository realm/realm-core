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

/*




TConditionFunction: Each node has a condition from query_conditions.c such as EQUAL, GREATER_EQUAL, etc

TConditionValue:    Type of values in condition column. That is, int64_t, float, int, bool, etc

TAction:            What to do with each search result, from the enums TDB_RETURN_FIRST, TDB_COUNT, TDB_SUM, etc

TResult:            Type of result of actions - float, double, int64_t, etc. Special notes: For TDB_COUNT it's 
                    int64_t, for TDB_FIND_ALL it's int64_t which points at destination array.

TSourceColumn:      Type of source column used in actions, or *ignored* if no source column is used (like for 
                    TDB_COUNT, TDB_RETURN_FIRST)
*/



#ifndef TIGHTDB_QUERY_ENGINE_HPP
#define TIGHTDB_QUERY_ENGINE_HPP

#include <string>
#include <functional>
#include "meta.hpp"

#include <tightdb/table.hpp>
#include <tightdb/table_view.hpp>
#include <tightdb/column_fwd.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_binary.hpp>
#include <tightdb/utf8.hpp>
#include <tightdb/query_conditions.hpp>
#include <tightdb/array_basic.hpp>


namespace tightdb {

// Number of matches to find in best condition loop before breaking out to probe other conditions. Too low value gives too many
// constant time overheads everywhere in the query engine. Too high value makes it adapt less rapidly to changes in match
// frequencies.
const size_t findlocals = 16;   

// Distance between matches from which performance begins to flatten out because various initial overheads become insignificant
const size_t bestdist = 2;    

// Minimum number of matches required in a certain condition before it can be used to compute statistics. Too high value can spent 
// too much time in a bad node (with high match frequency). Too low value gives inaccurate statistics.
const size_t probe_matches = 2;

// 
const size_t bitwidth_time_unit = 8; 


typedef bool (*CallbackDummy)(int64_t);

template<class T> struct ColumnTypeTraits;

template<> struct ColumnTypeTraits<int64_t> {
    typedef Column column_type;
    typedef Array array_type;
    typedef int64_t sum_type;
    static const ColumnType id = COLUMN_TYPE_INT;
};
template<> struct ColumnTypeTraits<bool> {
    typedef Column column_type;
    typedef Array array_type;
    typedef int64_t sum_type;
    static const ColumnType id = COLUMN_TYPE_BOOL;
};
template<> struct ColumnTypeTraits<float> {
    typedef ColumnFloat column_type;
    typedef ArrayFloat array_type;
    typedef double sum_type;
    static const ColumnType id = COLUMN_TYPE_FLOAT;
};
template<> struct ColumnTypeTraits<double> {
    typedef ColumnDouble column_type;
    typedef ArrayDouble array_type;
    typedef double sum_type;
    static const ColumnType id = COLUMN_TYPE_DOUBLE;
};



// Lets you access elements of an integer column in increasing order in a fast way where leafs are cached
class SequentialGetterParent {};

template<class T>class SequentialGetter : public SequentialGetterParent {
public:
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    typedef typename ColumnTypeTraits<T>::array_type ArrayType;

    // We must destroy m_array immediately after its instantiation to avoid leak of what it preallocates. We cannot 
    // wait until a SequentialGetter destructor because GetBlock() maps it to data that we don't have ownership of.
    SequentialGetter() 
    {        
        m_array.Destroy(); 
    }

    SequentialGetter(const Table& table, size_t column_ndx) 
    {
        m_array.Destroy();
        if (column_ndx != not_found)
            m_column = (ColType *)&table.GetColumnBase(column_ndx);
        m_leaf_end = 0;
    }

    SequentialGetter(ColType* column) 
    {
        m_array.Destroy();
        m_column = column;
        m_leaf_end = 0;
    }

    inline bool CacheNext(size_t index)
    {
        // Return wether or not leaf array has changed (could be useful to know for caller)
        if (index >= m_leaf_end) {
            // GetBlock() does following: If m_column contains only a leaf, then just return pointer to that leaf and
            // leave m_array untouched. Else call CreateFromHeader() on m_array (more time consuming) and return pointer to m_array.
            m_array_ptr = (ArrayType*) (((Column*)m_column)->GetBlock(index, m_array, m_leaf_start, true));
            const size_t leaf_size = m_array_ptr->Size();
            m_leaf_end = m_leaf_start + leaf_size;
            return true;
        }
        return false;
    }

    inline T GetNext(size_t index)
    {
        CacheNext(index);
        T av = m_array_ptr->Get(index - m_leaf_start);
        return av;
    }

    size_t m_leaf_start;
    size_t m_leaf_end;
    ColType* m_column;

    // See reason for having both a pointer and instance above
    ArrayType* m_array_ptr;
private:
    // Never access through m_array because it's uninitialized if column is just a leaf
    ArrayType m_array;
};

class ParentNode {
public:
    ParentNode() : m_is_integer_node(false), m_table(NULL) {}

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

    struct score_compare {
        bool operator ()(ParentNode const* a, ParentNode const* b) const { return (a->cost() < b->cost()); }
    };

    double cost(void) const
    {
        return 16.0 / m_dD + m_dT;
    }

    size_t find_first(size_t start, size_t end) {
        
        size_t m = 0;
        size_t next_cond = 0;
        size_t first_cond = 0;

        while (start < end) {
            
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

    // Only purpose is to make all NODE classes have this function (overloaded only in NODE)
    virtual size_t aggregate_call_specialized(ACTION /*TAction*/, ColumnType /*TResult*/, 
                                              QueryStateBase* /*st*/, 
                                              size_t /*start*/, size_t /*end*/, size_t /*local_limit*/, 
                                              SequentialGetterParent* /*source_column*/, size_t* /*matchcount*/)
    {
        TIGHTDB_ASSERT(false);
        return 0;
    }

    template<ACTION TAction, class TResult, class TSourceColumn>
    size_t aggregate_local_selector(ParentNode* node, QueryState<TResult>* st, size_t start, size_t end, size_t local_limit, 
                                    SequentialGetter<TSourceColumn>* source_column, size_t* matchcount)
    {
        size_t r;
        
        if (node->m_is_integer_node)
            // call method in NODE
            r = node->aggregate_call_specialized(TAction, ColumnTypeTraits<TResult>::id,(QueryStateBase*)st,
                                                 start, end, local_limit, source_column, matchcount);
        else
             // call method in ParentNode
            r = aggregate_local<TAction, TResult, TSourceColumn>(st, start, end, local_limit, source_column, matchcount);
        return r;
    }


    template<ACTION TAction, class TResult, class TSourceColumn>
    TResult aggregate(QueryState<TResult>* st, size_t start, size_t end, size_t agg_col, size_t* matchcount) 
    {
        if (end == size_t(-1)) 
            end = m_table->size();

        SequentialGetter<TSourceColumn>* source_column = NULL;
        
        if (agg_col != not_found)
            source_column = new SequentialGetter<TSourceColumn>(*m_table, agg_col);

        size_t td;

        while (start < end) {
            size_t best = std::distance(m_children.begin(), std::min_element(m_children.begin(), m_children.end(), score_compare()));

            // Find a large amount of local matches in best condition
            td = m_children[best]->m_dT == 0.0 ? end : (start + 1000 > end ? end : start + 1000);

            start = aggregate_local_selector<TAction, TResult, TSourceColumn>(m_children[best], st, start, td, findlocals, source_column, matchcount);

            // Make remaining conditions compute their m_dD (statistics)
            for (size_t c = 0; c < m_children.size() && start < end; c++) {
                if (c == best)
                    continue;

                // Skip test if there is no way its cost can ever be better than best node's
                double cost = m_children[c]->cost();
                if (m_children[c]->m_dT < cost) {

                    // Limit to bestdist in order not to skip too large parts of index nodes
                    size_t maxD = m_children[c]->m_dT == 0.0 ? end - start : bestdist;
                    td = m_children[c]->m_dT == 0.0 ? end : (start + maxD > end ? end : start + maxD);
                    start = aggregate_local_selector<TAction, TResult, TSourceColumn>(m_children[best], st, start, td, probe_matches, source_column, matchcount);
                }
            }
        }

        if (matchcount)
            *matchcount = st->m_match_count;
        delete source_column;

        return st->m_state;

    }

    template<ACTION TAction, class TResult, class TSourceColumn>
    size_t aggregate_local(QueryStateBase* st, size_t start, size_t end, size_t local_limit, 
                           SequentialGetterParent* source_column, size_t* matchcount) 
    {
		// aggregate called on non-integer column type. Speed of this function is not as critical as speed of the
        // integer version, because find_first_local() is relatively slower here (because it's non-integers).
		//
        // Todo: Two speedups are possible. Simple: Initially test if there are no sub criterias and run find_first_local()
        // in a tight loop if so (instead of testing if there are sub criterias after each match). Harder: Specialize 
        // data type array to make array call match() directly on each match, like for integers.
        
        (void)matchcount;
        size_t local_matches = 0;

        size_t r = start - 1;
        for (;;) {
            if (local_matches == local_limit) { 
                m_dD = double(r - start) / local_matches; 
                return r + 1;
            }

            // Find first match in this condition node
            r = find_first_local(r + 1, end);
            if (r == end) { 
                m_dD = double(r - start) / local_matches; 
                return end;
            }

            local_matches++;

            // Find first match in remaining condition nodes
            size_t m = r;
            for (size_t c = 1; c < m_conds; c++) {
                m = m_children[c]->find_first_local(r, r + 1);
                if (m != r) {
                    break;
                }
            }

            // If index of first match in this node equals index of first match in all remaining nodes, we have a final match
            if (m == r) {
                TSourceColumn av = (TSourceColumn)0;
                if (source_column != NULL)
                    av = static_cast<SequentialGetter<TSourceColumn>*>(source_column)->GetNext(r); // todo, avoid GetNext if value not needed (if !uses_val)
                ((QueryState<TResult>*)st)->template match<TAction, 0>(r, 0, TResult(av), CallbackDummy());
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

    size_t m_condition_column_idx; // Column of search criteria
    bool m_is_integer_node; // true for NODE, false for any other

    size_t m_conds;
    double m_dD; // Average row distance between each local match at current position
    double m_dT; // Time overhead of testing index i + 1 if we have just tested index i. > 1 for linear scans, 0 for index/tableview

    size_t m_probes;
    size_t m_matches;


protected:
    const Table* m_table;
    std::string error_code;

};


class ARRAYNODE: public ParentNode {
public:
    ARRAYNODE(const Array& arr) : m_arr(arr), m_max(0), m_next(0), m_size(arr.Size()) {m_child = 0;}

    void Init(const Table& table)
    {
        m_table = &table;

        m_dT = 0.0;
        m_dD =  m_table->size() / (m_arr.Size() + 1.0);
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
        if (r == not_found)
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

        if (m_child2) 
            m_child2->Init(table);
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
template <class TConditionValue, class TConditionFunction> class NODE: public ParentNode {
public:
    typedef typename ColumnTypeTraits<TConditionValue>::column_type ColType;

    // NOTE: Be careful to call Array(no_prealloc_tag) constructors on m_array in the initializer list, otherwise
    // their default constructors are called which are slow
    NODE(TConditionValue v, size_t column) : m_value(v), m_array(Array::no_prealloc_tag()) {
        m_is_integer_node = true;
        m_condition_column_idx = column;
        m_child = 0;
        m_conds = 0;
        m_dT = 1.0;
        m_dD = 100.0;
        m_probes = 0;
        m_matches = 0;
    }

    // Only purpose of this function is to let you quickly create a NODE object and call aggregate_local() on it to aggregate
    // on a single stand-alone column, with 1 or 0 search criterias, without involving any tables, etc. Todo, could
    // be merged with Init somehow to simplify
    void QuickInit(Column *column, int64_t value) {
        m_condition_column = column; 
        m_leaf_end = 0;
        m_value = value;
        m_conds = 0;
    }

    void Init(const Table& table)
    {
        m_condition_column = (ColType*)&table.GetColumnBase(m_condition_column_idx);
        m_table = &table;
        m_leaf_end = 0;
        if (m_child)
            m_child->Init(table);
    }

    // This function is called from Array::find() for each search result if TAction == TDB_CALLBACK_IDX
    // in the NODE::aggregate_local() call. Used if aggregate source column is different from search criteria column
    template <ACTION TAction, class TResult> bool match_callback(int64_t v) {
        size_t i = to_size_t(v);
        m_last_local_match = i;
        m_local_matches++;
        QueryState<TResult>* state = static_cast<QueryState<TResult>*>(m_state);
        SequentialGetter<TResult>* source_column = static_cast<SequentialGetter<TResult>*>(m_source_column);

        // Test remaining sub conditions of this node. m_children[0] is the node that called match_callback(), so skip it
        for (size_t c = 1; c < m_conds; c++) {
            m_children[c]->m_probes++;
            size_t m = m_children[c]->find_first_local(i, i + 1);
            if (m != i)
                return (m_local_matches != m_local_limit);
        }

        bool b;
        if (state->template uses_val<TAction>())    { // Compiler cannot see that Column::Get has no side effect and result is discarded         
            TResult av = source_column->GetNext(i);
            b = state->template match<TAction, false>(i, 0, av, CallbackDummy());  
        }
        else {
            b = state->template match<TAction, false>(i, 0, TResult(0), CallbackDummy());  
        }

        if (m_local_matches == m_local_limit)
            return false;
        else
            return b;
    }

    size_t aggregate_call_specialized(ACTION TAction, ColumnType col_id, QueryStateBase* st, 
                                      size_t start, size_t end, size_t local_limit, 
                                      SequentialGetterParent* source_column, size_t* matchcount) 
    {
        size_t ret;

        if (TAction == TDB_RETURN_FIRST)
            ret = aggregate_local<TDB_RETURN_FIRST, int64_t, int64_t>(st, start, end, local_limit, source_column, matchcount);        

        else if (TAction == TDB_SUM && col_id == COLUMN_TYPE_INT)
            ret = aggregate_local<TDB_SUM, int64_t, int64_t>(st, start, end, local_limit, source_column, matchcount);
        else if (TAction == TDB_SUM && col_id == COLUMN_TYPE_FLOAT)
            // todo, fixme, see if we must let sum return a double even when summing a float coltype 
            ret = aggregate_local<TDB_SUM, float, float>(st, start, end, local_limit, source_column, matchcount);
        else if (TAction == TDB_SUM && col_id == COLUMN_TYPE_DOUBLE)
            ret = aggregate_local<TDB_SUM, double, double>(st, start, end, local_limit, source_column, matchcount);

        else if (TAction == TDB_MAX && col_id == COLUMN_TYPE_INT)
            ret = aggregate_local<TDB_MAX, int64_t, int64_t>(st, start, end, local_limit, source_column, matchcount);
        else if (TAction == TDB_MAX && col_id == COLUMN_TYPE_FLOAT)
            ret = aggregate_local<TDB_MAX, float, float>(st, start, end, local_limit, source_column, matchcount);
        else if (TAction == TDB_MAX && col_id == COLUMN_TYPE_DOUBLE)
            ret = aggregate_local<TDB_MAX, double, double>(st, start, end, local_limit, source_column, matchcount);

        else if (TAction == TDB_MIN && col_id == COLUMN_TYPE_INT)
            ret = aggregate_local<TDB_MIN, int64_t, int64_t>(st, start, end, local_limit, source_column, matchcount);
        else if (TAction == TDB_MIN && col_id == COLUMN_TYPE_FLOAT)
            ret = aggregate_local<TDB_MIN, float, float>(st, start, end, local_limit, source_column, matchcount);
        else if (TAction == TDB_MIN && col_id == COLUMN_TYPE_DOUBLE)
            ret = aggregate_local<TDB_MIN, double, double>(st, start, end, local_limit, source_column, matchcount);

        else if (TAction == TDB_COUNT)
            ret = aggregate_local<TDB_COUNT, int64_t, int64_t>(st, start, end, local_limit, source_column, matchcount);

        else if (TAction == TDB_FINDALL)
            ret = aggregate_local<TDB_FINDALL, int64_t, int64_t>(st, start, end, local_limit, source_column, matchcount);

        else if (TAction == TDB_CALLBACK_IDX)
            ret = aggregate_local<TDB_CALLBACK_IDX, int64_t, int64_t>(st, start, end, local_limit, source_column, matchcount);

        else { 
            TIGHTDB_ASSERT(false);
            return 0;
        }
        return ret;
    }


    // source_column: column number in m_table which must act as source for aggreate TAction
    template <ACTION TAction, class TResult, class unused> 
    size_t aggregate_local(QueryStateBase* st, size_t start, size_t end, size_t local_limit, 
                           SequentialGetterParent* source_column, size_t* matchcount) 
    {
        TConditionFunction f;
        int c = f.condition();
        m_local_matches = 0;
        m_local_limit = local_limit;
        m_last_local_match = start - 1;
        m_state = st;

        for (size_t s = start; s < end; ) {    
            // Cache internal leafs
            if (s >= m_leaf_end) {                    
                m_condition_column->GetBlock(s, m_array, m_leaf_start);                                       
                m_leaf_end = m_leaf_start + m_array.Size();
                size_t w = m_array.GetBitWidth();
                m_dT = (w == 0 ? 1.0 / MAX_LIST_SIZE : w / float(bitwidth_time_unit));
            }

            size_t end2;
            if (end > m_leaf_end)
                end2 = m_leaf_end - m_leaf_start;
            else
                end2 = end - m_leaf_start;

            if (m_conds <= 1 && source_column != NULL && SameType<TResult, int64_t>::value && 
                static_cast<SequentialGetter<int64_t>*>(source_column)->m_column == m_condition_column)    {
                    m_array.find(c, TAction, m_value, s - m_leaf_start, end2, m_leaf_start, (QueryState<int64_t>*)st);
            }
            else {
                QueryState<int64_t> jumpstate; // todo optimize by moving outside for loop
                m_source_column = source_column; 
                m_array.find<TConditionFunction, TDB_CALLBACK_IDX>(m_value, s - m_leaf_start, end2, m_leaf_start, &jumpstate, 
                             std::bind1st(std::mem_fun(&NODE::match_callback<TAction, TResult>), this));
            }
                    
            if (m_local_matches == m_local_limit)
                break;

            s = end2 + m_leaf_start;
        }

        if (matchcount)
            *matchcount = int64_t(static_cast< QueryState<TResult>* >(st)->m_match_count);

        if (m_local_matches == m_local_limit) {
            m_dD = (m_last_local_match + 1 - start) / (m_local_matches + 1.0);
            return m_last_local_match + 1;
        }
        else {
            m_dD = (end - start) / (m_local_matches + 1.0);
            return end;
        }
    }

    size_t find_first_local(size_t start, size_t end)
    {
        TConditionFunction condition;
        TIGHTDB_ASSERT(m_table);

        while (start < end) {

            // Cache internal leafs
            if (start >= m_leaf_end) {
                m_condition_column->GetBlock(start, m_array, m_leaf_start);
                m_leaf_end = m_leaf_start + m_array.Size();
            }
            
            // Do search directly on cached leaf array
            if (start + 1 == end) {
                if (condition(m_array.Get(start - m_leaf_start), m_value))
                    return start;
                else
                    return end;
            }

            size_t end2;
            if (end > m_leaf_end)
                end2 = m_leaf_end - m_leaf_start;
            else
                end2 = end - m_leaf_start;

            size_t s = m_array.find_first<TConditionFunction>(m_value, start - m_leaf_start, end2);

            if (s == not_found) {
                start = m_leaf_end;
                continue;
            }
            else
                return s + m_leaf_start;
        }  

        return end;
    }

    TConditionValue m_value;

protected:

    size_t m_last_local_match;
    ColType* m_condition_column;                // Column on which search criteria is applied
//    const Array* m_criteria_arr;
    Array m_array;              
    size_t m_leaf_start;
    size_t m_leaf_end;
    size_t m_local_end;

    size_t m_local_matches;
    size_t m_local_limit;
 
    QueryStateBase* m_state;
    SequentialGetterParent* m_source_column; // Column of values used in aggregate (TDB_FINDALL, TDB_RETURN_FIRST, TDB_SUM, etc)
};


template <class TConditionFunction> class STRINGNODE: public ParentNode {
public:
    template <ACTION TAction>
    int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t source_column)
    {
        TIGHTDB_ASSERT(false);
        return 0;
    }

    STRINGNODE(const char* v, size_t column)
    {
        m_condition_column_idx = column;
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
        m_condition_column = &table.GetColumnBase(m_condition_column_idx);
        m_column_type = table.GetRealColumnType(m_condition_column_idx);

        if (m_child) m_child->Init(table);
    }

    size_t find_first_local(size_t start, size_t end)
    {
        TConditionFunction cond;

        for (size_t s = start; s < end; ++s) {
            const char* t;

            // todo, can be optimized by placing outside loop
            if (m_column_type == COLUMN_TYPE_STRING)
                t = ((const AdaptiveStringColumn*)m_condition_column)->Get(s);
            else {
                //TODO: First check if string is in key list
                t = ((const ColumnStringEnum*)m_condition_column)->Get(s);
            }

            if (cond(m_value, m_ucase, m_lcase, t))
                return s;
        }
        return end;
    }

protected:
    char* m_value;
    char* m_lcase;
    char* m_ucase;
    const ColumnBase* m_condition_column;
    ColumnType m_column_type;
};


// Can be used for simple types (currently float and double)
template <class TConditionValue, class TConditionFunction> class BASICNODE: public ParentNode {
public:
    typedef typename ColumnTypeTraits<TConditionValue>::column_type ColType;
    
    BASICNODE(TConditionValue v, size_t column_ndx) : m_value(v)
    {
        m_condition_column_idx = column_ndx;
        m_child = 0;
    }

    // Only purpose of this function is to let you quickly create a NODE object and call aggregate_local() on it to aggregate
    // on a single stand-alone column, with 1 or 0 search criterias, without involving any tables, etc. Todo, could
    // be merged with Init somehow to simplify
    void QuickInit(BasicColumn<TConditionValue> *column, TConditionValue value) {
        m_condition_column.m_column = (ColType*)column;
        m_condition_column.m_leaf_end = 0;
        m_value = value;
        m_conds = 0;
    }

    void Init(const Table& table)
    {
        m_table = &table;
        m_condition_column.m_column = (ColType*)(&table.GetColumnBase(m_condition_column_idx));
        m_condition_column.m_leaf_end = 0;

        if (m_child) 
            m_child->Init(table);
    }
    
    size_t find_first_local(size_t start, size_t end) {
        TConditionFunction cond;

        for (size_t s = start; s < end; ++s) {            
            TConditionValue v = m_condition_column.GetNext(s);
            if (cond(v, m_value))
                return s;
        }
        return end;
    }

protected:
    TConditionValue m_value;
    SequentialGetter<TConditionValue> m_condition_column;
};


template <class TConditionFunction> class BINARYNODE: public ParentNode {
public:
    template <ACTION TAction> int64_t find_all(Array* /*res*/, size_t /*start*/, size_t /*end*/, size_t /*limit*/, size_t /*source_column*/) {TIGHTDB_ASSERT(false); return 0;}

    BINARYNODE(const char* v, size_t len, size_t column)
    {
        m_condition_column_idx = column;
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
        m_condition_column = (const ColumnBinary*)&table.GetColumnBase(m_condition_column_idx);
        m_column_type = table.GetRealColumnType(m_condition_column_idx);

        if (m_child) 
            m_child->Init(table);
    }

    size_t find_first(size_t start, size_t end)
    {
        TConditionFunction condition;

        for (size_t s = start; s < end; ++s) {
            const char* t = m_condition_column->Get(s).pointer;
            size_t len2 = m_condition_column->Get(s).len;

            if (condition(m_value, m_len, t, len2)) {
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
        TConditionFunction condition;

        for (size_t s = start; s < end; ++s) {            
            const char* value = m_condition_column->Get(s).pointer;
            size_t len = m_condition_column->Get(s).len;

            if (condition(m_value, m_len, value, len))
                return s;
        }
        return end;
    }

protected:
    char* m_value;
    size_t m_len;
    const ColumnBinary* m_condition_column;
    ColumnType m_column_type;
};


template <> class STRINGNODE<EQUAL>: public ParentNode {
public:
    template <ACTION TAction> 
    int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t source_column) 
    {
        TIGHTDB_ASSERT(false); 
        return 0;
    }

    STRINGNODE(const char* v, size_t column): m_key_ndx((size_t)-1) {
        m_condition_column_idx = column;
        m_child = 0;
        m_value = (char *)malloc(strlen(v)*6);
        memcpy(m_value, v, strlen(v) + 1);
    }
    ~STRINGNODE() {
        free((void*)m_value);
        m_index.Destroy();
    }

    void Init(const Table& table)
    {
        m_dD = 10.0;
        
        m_table = &table;
        m_condition_column = &table.GetColumnBase(m_condition_column_idx);
        m_column_type = table.GetRealColumnType(m_condition_column_idx);

        if (m_column_type == COLUMN_TYPE_STRING_ENUM) {
            m_dT = 1.0;
            m_key_ndx = ((const ColumnStringEnum*)m_condition_column)->GetKeyNdx(m_value);
        }
        else {
            m_dT = 10.0;
        }

        if (m_condition_column->HasIndex()) {
            if (m_column_type == COLUMN_TYPE_STRING_ENUM)
                ((ColumnStringEnum*)m_condition_column)->find_all(m_index, m_value);
            else {
                ((AdaptiveStringColumn*)m_condition_column)->find_all(m_index, m_value);
            }
            last_indexed = 0;
        }

        if (m_child) 
            m_child->Init(table);
    }

    size_t find_first_local(size_t start, size_t end)
    {
        TIGHTDB_ASSERT(m_table);

        for (size_t s = start; s < end; ++s) {
            if (m_condition_column->HasIndex()) {
                size_t f = m_index.FindGTE(s, last_indexed);
                if (f != not_found)
                    s = m_index.Get(f);
                else
                    s = not_found;
                last_indexed = f;
            }
            else {
                // todo, can be optimized by placing outside loop
                if (m_column_type == COLUMN_TYPE_STRING)
                    s = ((const AdaptiveStringColumn*)m_condition_column)->find_first(m_value, s, end);
                else {
                    if (m_key_ndx == size_t(-1)) 
                        s = end; // not in key set
                    else {
                        const ColumnStringEnum* const cse = (const ColumnStringEnum*)m_condition_column;
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
    const ColumnBase* m_condition_column;
    ColumnType m_column_type;
    size_t m_key_ndx;
    Array m_index;
    size_t last_indexed;
};


class OR_NODE: public ParentNode {
public:
    template <ACTION TAction> int64_t find_all(Array* res, size_t start, size_t end, size_t limit, size_t source_column)
    {
        TIGHTDB_ASSERT(false);
        return 0;
    }

    OR_NODE(ParentNode* p1) : m_table(NULL) {m_child = NULL; m_cond[0] = p1; m_cond[1] = NULL;};


    void Init(const Table& table)
    {
        m_dT = 50.0;
        m_dD = 10.0;

        std::vector<ParentNode*>v;

        for (size_t c = 0; c < 2; ++c) {
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

            for (size_t c = 0; c < 2; ++c) {
                if (m_last[c] >= end)
                    f[c] = end;
                else if (m_was_match[c] && m_last[c] >= s)
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
