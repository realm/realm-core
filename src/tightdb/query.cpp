#include <cstdio>
#include <algorithm>

#include <tightdb/array.hpp>
#include <tightdb/column_basic.hpp>
#include <tightdb/column_fwd.hpp>
#include <tightdb/query.hpp>
#include <tightdb/query_engine.hpp>

using namespace std;
using namespace tightdb;

namespace {
const size_t thread_chunk_size = 1000;
}

Query::Query() 
{
    Create();
//    expression(static_cast<Expression*>(this));
}

Query::Query(Table& table) : m_table(table.get_table_ref())
{
    Create();
}

Query::Query(const Table& table) : m_table((const_cast<Table&>(table)).get_table_ref())
{
    Create();
}

void Query::Create()
{
    update.push_back(0);
    update_override.push_back(0);
    first.push_back(0);
#if TIGHTDB_MULTITHREAD_QUERY
    m_threadcount = 0;
#endif
    do_delete = true;
}

// FIXME: Try to remove this
Query::Query(const Query& copy)
{
    m_table = copy.m_table;
    all_nodes = copy.all_nodes;
    update = copy.update;
    update_override = copy.update_override;
    first = copy.first;
    error_code = copy.error_code;
#if TIGHTDB_MULTITHREAD_QUERY
    m_threadcount = copy.m_threadcount;
#endif
    //    copy.first[0] = 0;
    copy.do_delete = false;
    do_delete = true;
}

Query::~Query() TIGHTDB_NOEXCEPT
{
#if TIGHTDB_MULTITHREAD_QUERY
    for (size_t i = 0; i < m_threadcount; i++)
        pthread_detach(threads[i]);
#endif

    if (do_delete) {
        for (size_t t = 0; t < all_nodes.size(); t++) {
            ParentNode *p = all_nodes[t];
            std::vector<ParentNode *>::iterator it = std::find(all_nodes.begin(), all_nodes.begin() + t, p);
            if(it == all_nodes.begin() + t)
                delete p;
        }
    }
}
/*
// use and_query() instead!
Expression* Query::get_expression() {
    return (static_cast<ExpressionNode*>(first[first.size()-1]))->m_compare;
}
*/
Query& Query::expression(Expression* compare, bool auto_delete)
{
    ParentNode* const p = new ExpressionNode(compare, auto_delete);
    UpdatePointers(p, &p->m_child);
    return *this;
}

// Makes query search only in rows contained in tv
Query& Query::tableview(const TableView& tv)
{
    const Array& arr = tv.get_ref_column();
    return tableview(arr);
}

// Makes query search only in rows contained in tv
Query& Query::tableview(const Array &arr)
{
    ParentNode* const p = new ListviewNode(arr);
    UpdatePointers(p, &p->m_child);
    return *this;
}


// Binary
Query& Query::equal(size_t column_ndx, BinaryData b)
{
    ParentNode* const p = new BinaryNode<Equal>(b, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, BinaryData b)
{
    ParentNode* const p = new BinaryNode<NotEqual>(b, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::begins_with(size_t column_ndx, BinaryData b)
{
    ParentNode* p = new BinaryNode<BeginsWith>(b, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::ends_with(size_t column_ndx, BinaryData b)
{
    ParentNode* p = new BinaryNode<EndsWith>(b, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::contains(size_t column_ndx, BinaryData b)
{
    ParentNode* p = new BinaryNode<Contains>(b, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}

// Generic 'simple type' condition
template <typename T, class N>
Query& Query::add_condition(size_t column_ndx, T value)
{
    ParentNode* const parent = new N(value, column_ndx);
    UpdatePointers(parent, &parent->m_child);
    return *this;
}


template <class TColumnType> Query& Query::equal(size_t column_ndx1, size_t column_ndx2)
{
    ParentNode* const p = new TwoColumnsNode<TColumnType, Equal>(column_ndx1, column_ndx2);
    UpdatePointers(p, &p->m_child);
    return *this;
}

// Two column methods, any type
template <class TColumnType> Query& Query::less(size_t column_ndx1, size_t column_ndx2)
{
    ParentNode* const p = new TwoColumnsNode<TColumnType, Less>(column_ndx1, column_ndx2);
    UpdatePointers(p, &p->m_child);
    return *this;
}
template <class TColumnType> Query& Query::less_equal(size_t column_ndx1, size_t column_ndx2)
{
    ParentNode* const p = new TwoColumnsNode<TColumnType, LessEqual>(column_ndx1, column_ndx2);
    UpdatePointers(p, &p->m_child);
    return *this;
}
template <class TColumnType> Query& Query::greater(size_t column_ndx1, size_t column_ndx2)
{
    ParentNode* const p = new TwoColumnsNode<TColumnType, Greater>(column_ndx1, column_ndx2);
    UpdatePointers(p, &p->m_child);
    return *this;
}
template <class TColumnType> Query& Query::greater_equal(size_t column_ndx1, size_t column_ndx2)
{
    ParentNode* const p = new TwoColumnsNode<TColumnType, GreaterEqual>(column_ndx1, column_ndx2);
    UpdatePointers(p, &p->m_child);
    return *this;
}
template <class TColumnType> Query& Query::not_equal(size_t column_ndx1, size_t column_ndx2)
{
    ParentNode* const p = new TwoColumnsNode<TColumnType, NotEqual>(column_ndx1, column_ndx2);
    UpdatePointers(p, &p->m_child);
    return *this;
}

// column vs column, integer
Query& Query::equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return equal<int64_t>(column_ndx1, column_ndx2);
}

Query& Query::not_equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return not_equal<int64_t>(column_ndx1, column_ndx2);
}

Query& Query::less_int(size_t column_ndx1, size_t column_ndx2)
{
    return less<int64_t>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<int64_t>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<int64_t>(column_ndx1, column_ndx2);
}

Query& Query::greater_int(size_t column_ndx1, size_t column_ndx2)
{
    return greater<int64_t>(column_ndx1, column_ndx2);
}


// column vs column, float
Query& Query::not_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return not_equal<float>(column_ndx1, column_ndx2);
}

Query& Query::less_float(size_t column_ndx1, size_t column_ndx2)
{
    return less<float>(column_ndx1, column_ndx2);
}

Query& Query::greater_float(size_t column_ndx1, size_t column_ndx2)
{
    return greater<float>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<float>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<float>(column_ndx1, column_ndx2);
}

Query& Query::equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return equal<float>(column_ndx1, column_ndx2);
}

// column vs column, double
Query& Query::equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return equal<double>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<double>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<double>(column_ndx1, column_ndx2);
}
Query& Query::greater_double(size_t column_ndx1, size_t column_ndx2)
{
    return greater<double>(column_ndx1, column_ndx2);
}
Query& Query::less_double(size_t column_ndx1, size_t column_ndx2)
{
    return less<double>(column_ndx1, column_ndx2);
}

Query& Query::not_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return not_equal<double>(column_ndx1, column_ndx2);
}


// int constant vs column (we need those because '1234' is ambiguous, can convert to float/double/int64_t)
Query& Query::equal(size_t column_ndx, int value)
{
    return equal(column_ndx, static_cast<int64_t>(value));
}
Query& Query::not_equal(size_t column_ndx, int value)
{
    return not_equal(column_ndx, static_cast<int64_t>(value));
}
Query& Query::greater(size_t column_ndx, int value)
{
    return greater(column_ndx, static_cast<int64_t>(value));
}
Query& Query::greater_equal(size_t column_ndx, int value)
{
    return greater_equal(column_ndx, static_cast<int64_t>(value));
}
Query& Query::less_equal(size_t column_ndx, int value)
{
    return less_equal(column_ndx, static_cast<int64_t>(value));
}
Query& Query::less(size_t column_ndx, int value)
{
    return less(column_ndx, static_cast<int64_t>(value));
}
Query& Query::between(size_t column_ndx, int from, int to)
{
    return between(column_ndx, static_cast<int64_t>(from), static_cast<int64_t>(to));
}

// int64 constant vs column
Query& Query::equal(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, Equal>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, NotEqual>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::greater(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, Greater>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::greater_equal(size_t column_ndx, int64_t value)
{
    if (value > LLONG_MIN) {
        ParentNode* const p = new IntegerNode<int64_t, Greater>(value - 1, column_ndx);
        UpdatePointers(p, &p->m_child);
    }
    // field >= LLONG_MIN has no effect
    return *this;
}
Query& Query::less_equal(size_t column_ndx, int64_t value)
{
    if (value < LLONG_MAX) {
        ParentNode* const p = new IntegerNode<int64_t, Less>(value + 1, column_ndx);
        UpdatePointers(p, &p->m_child);
    }
    // field <= LLONG_MAX has no effect
    return *this;
}
Query& Query::less(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, Less>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::between(size_t column_ndx, int64_t from, int64_t to)
{
    greater_equal(column_ndx, from);
    less_equal(column_ndx, to);
    return *this;
}
Query& Query::equal(size_t column_ndx, bool value)
{
    ParentNode* const p = new IntegerNode<bool, Equal>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}

// ------------- float
Query& Query::equal(size_t column_ndx, float value)
{
    return add_condition<float, FloatDoubleNode<float, Equal> >(column_ndx, value);
}
Query& Query::not_equal(size_t column_ndx, float value)
{
    return add_condition<float, FloatDoubleNode<float, NotEqual> >(column_ndx, value);
}
Query& Query::greater(size_t column_ndx, float value)
{
    return add_condition<float, FloatDoubleNode<float, Greater> >(column_ndx, value);
}
Query& Query::greater_equal(size_t column_ndx, float value)
{
    return add_condition<float, FloatDoubleNode<float, GreaterEqual> >(column_ndx, value);
}
Query& Query::less_equal(size_t column_ndx, float value)
{
    return add_condition<float, FloatDoubleNode<float, LessEqual> >(column_ndx, value);
}
Query& Query::less(size_t column_ndx, float value)
{
    return add_condition<float, FloatDoubleNode<float, Less> >(column_ndx, value);
}
Query& Query::between(size_t column_ndx, float from, float to)
{
    greater_equal(column_ndx, from);
    less_equal(column_ndx, to);
    return *this;
}


// ------------- double
Query& Query::equal(size_t column_ndx, double value)
{
    return add_condition<double, FloatDoubleNode<double, Equal> >(column_ndx, value);
}
Query& Query::not_equal(size_t column_ndx, double value)
{
    return add_condition<double, FloatDoubleNode<double, NotEqual> >(column_ndx, value);
}
Query& Query::greater(size_t column_ndx, double value)
{
    return add_condition<double, FloatDoubleNode<double, Greater> >(column_ndx, value);
}
Query& Query::greater_equal(size_t column_ndx, double value)
{
    return add_condition<double, FloatDoubleNode<double, GreaterEqual> >(column_ndx, value);
}
Query& Query::less_equal(size_t column_ndx, double value)
{
    return add_condition<double, FloatDoubleNode<double, LessEqual> >(column_ndx, value);
}
Query& Query::less(size_t column_ndx, double value)
{
    return add_condition<double, FloatDoubleNode<double, Less> >(column_ndx, value);
}
Query& Query::between(size_t column_ndx, double from, double to)
{
    greater_equal(column_ndx, from);
    less_equal(column_ndx, to);
    return *this;
}


// Strings, StringData()

Query& Query::equal(size_t column_ndx, StringData value, bool case_sensitive)
{
    ParentNode* p;
    if (case_sensitive)
        p = new StringNode<Equal>(value, column_ndx);
    else
        p = new StringNode<EqualIns>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::begins_with(size_t column_ndx, StringData value, bool case_sensitive)
{
    ParentNode* p;
    if (case_sensitive)
        p = new StringNode<BeginsWith>(value, column_ndx);
    else
        p = new StringNode<BeginsWithIns>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::ends_with(size_t column_ndx, StringData value, bool case_sensitive)
{
    ParentNode* p;
    if (case_sensitive)
        p = new StringNode<EndsWith>(value, column_ndx);
    else
        p = new StringNode<EndsWithIns>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::contains(size_t column_ndx, StringData value, bool case_sensitive)
{
    ParentNode* p;
    if (case_sensitive)
        p = new StringNode<Contains>(value, column_ndx);
    else
        p = new StringNode<ContainsIns>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, StringData value, bool case_sensitive)
{
    ParentNode* p;
    if (case_sensitive)
        p = new StringNode<NotEqual>(value, column_ndx);
    else
        p = new StringNode<NotEqualIns>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}


// Aggregates =================================================================================

template <Action action, typename T, typename R, class ColType>
R Query::aggregate(R (ColType::*aggregateMethod)(size_t start, size_t end, size_t limit) const,
                    size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if(limit == 0 || m_table->is_degenerate()) {
        if (resultcount)
            *resultcount = 0;
        return static_cast<R>(0);
    }

    if (end == size_t(-1))
        end = m_table->size();

    const ColType& column =
        m_table->get_column<ColType, ColumnType(ColumnTypeTraits<T>::id)>(column_ndx);

    if (first.size() == 0 || first[0] == 0) {
        // User created query with no criteria; aggregate range
        if (resultcount) {
            *resultcount = limit < (end - start) ? limit : (end - start);            
        }
        // direct aggregate on the column
        return (column.*aggregateMethod)(start, end, limit);
    }

    // Aggregate with criteria
    Init(*m_table);
    size_t matchcount = 0;
    QueryState<R> st;
    st.init(action, NULL, limit);
    R r = first[0]->aggregate<action, R, T>(&st, start, end, column_ndx, &matchcount);
    if (resultcount)
        *resultcount = matchcount;
    return r;
}

// Sum

int64_t Query::sum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Sum, int64_t>(&Column::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Sum, float>(&ColumnFloat::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Sum, double>(&ColumnDouble::sum, column_ndx, resultcount, start, end, limit);
}

// Maximum

int64_t Query::maximum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Max, int64_t>(&Column::maximum, column_ndx, resultcount, start, end, limit);
}
float Query::maximum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Max, float>(&ColumnFloat::maximum, column_ndx, resultcount, start, end, limit);
}
double Query::maximum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Max, double>(&ColumnDouble::maximum, column_ndx, resultcount, start, end, limit);
}

// Minimum

int64_t Query::minimum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Min, int64_t>(&Column::minimum, column_ndx, resultcount, start, end, limit);
}
float Query::minimum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Min, float>(&ColumnFloat::minimum, column_ndx, resultcount, start, end, limit);
}
double Query::minimum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Min, double>(&ColumnDouble::minimum, column_ndx, resultcount, start, end, limit);
}

// Average

template <typename T>
double Query::average(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if(limit == 0 || m_table->is_degenerate()) {
        if (resultcount)
            *resultcount = 0;
        return 0.;
    }

    size_t resultcount2 = 0;
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    typedef typename ColumnTypeTraits<T>::sum_type SumType;
    const SumType sum1 = aggregate<act_Sum, T>(&ColType::sum, column_ndx, &resultcount2, start, end, limit);
    double avg1 = 0;
    if (resultcount2 != 0)
        avg1 = static_cast<double>(sum1) / resultcount2;
    if (resultcount)
        *resultcount = resultcount2;
    return avg1;
}

double Query::average_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return average<int64_t>(column_ndx, resultcount, start, end, limit);
}
double Query::average_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return average<float>(column_ndx, resultcount, start, end, limit);
}
double Query::average_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return average<double>(column_ndx, resultcount, start, end, limit);
}


// Grouping
Query& Query::group()
{
    update.push_back(0);
    update_override.push_back(0);
    first.push_back(0);
    return *this;
}
Query& Query::end_group()
{
    if (first.size() < 2) {
        error_code = "Unbalanced blockBegin/blockEnd";
        return *this;
    }

    if (update[update.size()-2] != 0)
        *update[update.size()-2] = first[first.size()-1];

    if (first[first.size()-2] == 0)
        first[first.size()-2] = first[first.size()-1];

    if (update_override[update_override.size()-1] != 0)
        update[update.size() - 2] = update_override[update_override.size()-1];
    else if (update[update.size()-1] != 0)
        update[update.size() - 2] = update[update.size()-1];

    first.pop_back();
    update.pop_back();
    update_override.pop_back();
    return *this;
}

Query& Query::Or()
{
    ParentNode* const o = new OrNode(first[first.size()-1]);
    all_nodes.push_back(o);

    first[first.size()-1] = o;
    update[update.size()-1] = &((OrNode*)o)->m_cond[1];
    update_override[update_override.size()-1] = &((OrNode*)o)->m_child;
    return *this;
}

void Query::subtable(size_t column)
{
    ParentNode* const p = new SubtableNode(column);
    UpdatePointers(p, &p->m_child);
    // once subtable conditions have been evaluated, resume evaluation from m_child2
    subtables.push_back(&((SubtableNode*)p)->m_child2);
    group();
}

void Query::end_subtable()
{
    end_group();

    if (update[update.size()-1] != 0)
        update[update.size()-1] = subtables[subtables.size()-1];

    subtables.pop_back();
}

// todo, add size_t end? could be useful
size_t Query::find(size_t begin_at_table_row)
{
    if(m_table->is_degenerate())
        return not_found;

    TIGHTDB_ASSERT(begin_at_table_row <= m_table->size());

    Init(*m_table);

    // User created query with no criteria; return first
    if (first.size() == 0 || first[0] == 0) {
        return m_table->size() == 0 ? not_found : begin_at_table_row;
    }

    const size_t end = m_table->size();
    const size_t res = first[0]->find_first(begin_at_table_row, end);

    return (res == end) ? not_found : res;
}

TableView Query::find_all(size_t start, size_t end, size_t limit)
{
    if(limit == 0 || m_table->is_degenerate())
        return TableView(*m_table);

    TIGHTDB_ASSERT(start <= m_table->size());

    Init(*m_table);

    if (end == size_t(-1))
        end = m_table->size();

    // User created query with no criteria; return everything
    if (first.size() == 0 || first[0] == 0) {
        TableView tv(*m_table);
        for (size_t i = start; i < end && i - start < limit; i++)
            tv.get_ref_column().add(i);
        return tv;
    }

#if TIGHTDB_MULTITHREAD_QUERY
    if (m_threadcount > 0) {
        // Use multithreading
        return find_all_multi(start, end);
    }
#endif

    // Use single threading
    TableView tv(*m_table);
    QueryState<int64_t> st;
    st.init(act_FindAll, &tv.get_ref_column(), limit);
    first[0]->aggregate<act_FindAll, int64_t, int64_t>(&st, start, end, not_found, NULL);
    return tv;
}


size_t Query::count(size_t start, size_t end, size_t limit) const
{
    if(limit == 0 || m_table->is_degenerate())
        return 0;

    if (end == size_t(-1))
        end = m_table->size();

    if (first.size() == 0 || first[0] == 0) {
        // User created query with no criteria; count all
        return (limit < end - start ? limit : end - start);
    }

    Init(*m_table);
    QueryState<int64_t> st;
    st.init(act_Count, NULL, limit);
    int64_t r = first[0]->aggregate<act_Count, int64_t, int64_t>(&st, start, end, not_found, NULL);
    return size_t(r);
}


// todo, not sure if start, end and limit could be useful for delete.
size_t Query::remove(size_t start, size_t end, size_t limit)
{
    if(limit == 0 || m_table->is_degenerate())
        return 0;

    if (end == not_found)
        end = m_table->size();

    size_t r = start;
    size_t results = 0;

    for (;;) {
        // Every remove invalidates the array cache in the nodes
        // so we have to re-initialize it before searching
        Init(*m_table);

        r = FindInternal(r, end - results);
        if (r == not_found || r == m_table->size() || results == limit)
            break;
        ++results;
        m_table->remove(r);
    }
    return results;
}

#if TIGHTDB_MULTITHREAD_QUERY
TableView Query::find_all_multi(size_t start, size_t end)
{
    (void)start;
    (void)end;

    // Initialization
    Init(*m_table);
    ts.next_job = start;
    ts.end_job = end;
    ts.done_job = 0;
    ts.count = 0;
    ts.table = &table;
    ts.node = first[0];

    // Signal all threads to start
    pthread_mutex_unlock(&ts.jobs_mutex);
    pthread_cond_broadcast(&ts.jobs_cond);

    // Wait until all threads have completed
    pthread_mutex_lock(&ts.completed_mutex);
    while (ts.done_job < ts.end_job)
        pthread_cond_wait(&ts.completed_cond, &ts.completed_mutex);
    pthread_mutex_lock(&ts.jobs_mutex);
    pthread_mutex_unlock(&ts.completed_mutex);

    TableView tv(*m_table);

    // Sort search results because user expects ascending order
    sort(ts.chunks.begin(), ts.chunks.end(), &Query::comp);
    for (size_t i = 0; i < ts.chunks.size(); ++i) {
        const size_t from = ts.chunks[i].first;
        const size_t upto = (i == ts.chunks.size() - 1) ? size_t(-1) : ts.chunks[i + 1].first;
        size_t first = ts.chunks[i].second;

        while (first < ts.results.size() && ts.results[first] < upto && ts.results[first] >= from) {
            tv.get_ref_column().add(ts.results[first]);
            ++first;
        }
    }

    return move(tv);
}

int Query::set_threads(unsigned int threadcount)
{
#if defined(_WIN32) || defined(__WIN32__) || defined(_WIN64)
    pthread_win32_process_attach_np ();
#endif
    pthread_mutex_init(&ts.result_mutex, NULL);
    pthread_cond_init(&ts.completed_cond, NULL);
    pthread_mutex_init(&ts.jobs_mutex, NULL);
    pthread_mutex_init(&ts.completed_mutex, NULL);
    pthread_cond_init(&ts.jobs_cond, NULL);

    pthread_mutex_lock(&ts.jobs_mutex);

    for (size_t i = 0; i < m_threadcount; ++i)
        pthread_detach(threads[i]);

    for (size_t i = 0; i < threadcount; ++i) {
        int r = pthread_create(&threads[i], NULL, query_thread, (void*)&ts);
        if (r != 0)
            TIGHTDB_ASSERT(false); //todo
    }
    m_threadcount = threadcount;
    return 0;
}


void* Query::query_thread(void* arg)
{
    static_cast<void>(arg);
    thread_state* ts = static_cast<thread_state*>(arg);

    vector<size_t> res;
    vector<pair<size_t, size_t> > chunks;

    for (;;) {
        // Main waiting loop that waits for a query to start
        pthread_mutex_lock(&ts->jobs_mutex);
        while (ts->next_job == ts->end_job)
            pthread_cond_wait(&ts->jobs_cond, &ts->jobs_mutex);
        pthread_mutex_unlock(&ts->jobs_mutex);

        for (;;) {
            // Pick a job
            pthread_mutex_lock(&ts->jobs_mutex);
            if (ts->next_job == ts->end_job)
                break;
            const size_t chunk = min(ts->end_job - ts->next_job, thread_chunk_size);
            const size_t mine = ts->next_job;
            ts->next_job += chunk;
            size_t r = mine - 1;
            const size_t end = mine + chunk;

            pthread_mutex_unlock(&ts->jobs_mutex);

            // Execute job
            for (;;) {
                r = ts->node->find_first(r + 1, end);
                if (r == end)
                    break;
                res.push_back(r);
            }

            // Append result in common queue shared by all threads.
            pthread_mutex_lock(&ts->result_mutex);
            ts->done_job += chunk;
            if (res.size() > 0) {
                ts->chunks.push_back(pair<size_t, size_t>(mine, ts->results.size()));
                ts->count += res.size();
                for (size_t i = 0; i < res.size(); i++) {
                    ts->results.push_back(res[i]);
                }
                res.clear();
            }
            pthread_mutex_unlock(&ts->result_mutex);

            // Signal main thread that we might have compleeted
            pthread_mutex_lock(&ts->completed_mutex);
            pthread_cond_signal(&ts->completed_cond);
            pthread_mutex_unlock(&ts->completed_mutex);
        }
    }
    return 0;
}

#endif // TIGHTDB_MULTITHREADQUERY

string Query::validate()
{
    if (first.size() == 0)
        return "";

    if (error_code != "") // errors detected by QueryInterface
        return error_code;

    if (first[0] == 0)
        return "Syntax error";

    return first[0]->validate(); // errors detected by QueryEngine
}

void Query::Init(const Table& table) const
{
    if (first[0] != NULL) {
        ParentNode* top = first[0];
        top->init(table);
        vector<ParentNode*> v;
        top->gather_children(v);
    }
}

bool Query::is_initialized() const
{
    const ParentNode* top = first[0];
    if (top != NULL) {
        return top->is_initialized();
    }
    return true;
}

size_t Query::FindInternal(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_table->size();
    if (start == end)
        return not_found;

    size_t r;
    if (first[0] != 0)
        r = first[0]->find_first(start, end);
    else
        r = start; // user built an empty query; return any first

    if (r == m_table->size())
        return not_found;
    else
        return r;
}

bool Query::comp(const pair<size_t, size_t>& a, const pair<size_t, size_t>& b)
{
    return a.first < b.first;
}

void Query::UpdatePointers(ParentNode* p, ParentNode** newnode)
{
    all_nodes.push_back(p);
    if (first[first.size()-1] == 0)
        first[first.size()-1] = p;

    if (update[update.size()-1] != 0)
        *update[update.size()-1] = p;

    update[update.size()-1] = newnode;
}

/* ********************************************************************************************************************
*
*  Stuff related to next-generation query syntax
*
******************************************************************************************************************** */

Query& Query::and_query(Query q) 
{
    ParentNode* const p = q.first[0];
    UpdatePointers(p, &p->m_child);

    // The query on which AddQuery() was called is now responsible for destruction of query given as argument. do_delete
    // indicates not to do cleanup in deconstructor, and all_nodes contains a list of all objects to be deleted. So
    // take all objects of argument and copy to this node's all_nodes list.
    q.do_delete = false;
    all_nodes.insert( all_nodes.end(), q.all_nodes.begin(), q.all_nodes.end() );

    return *this;
}


Query Query::operator||(Query q)
{
    Query q2(*this->m_table);
    q2.and_query(*this);
    q2.Or();
    q2.and_query(q);

    return q2;
}
 

Query Query::operator&&(Query q)
{
    if(first[0] == NULL)
        return q;

    if(q.first[0] == NULL)
        return (*this);

    Query q2(*this->m_table);
    q2.and_query(*this);
    q2.and_query(q);

    return q2;
}
 
