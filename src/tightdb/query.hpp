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
#ifndef TIGHTDB_QUERY_HPP
#define TIGHTDB_QUERY_HPP

#include <stdint.h>
#include <cstdio>
#include <climits>
#include <algorithm>
#include <string>
#include <vector>

#define TIGHTDB_MULTITHREAD_QUERY 0

#if TIGHTDB_MULTITHREAD_QUERY
// FIXME: Use our C++ thread abstraction API since it provides a much
// higher level of encapsulation and safety.
#include <pthread.h>
#endif

#include <tightdb/table_ref.hpp>
#include <tightdb/binary_data.hpp>
#include <tightdb/date.hpp>

namespace tightdb {


// Pre-declarations
class ParentNode;
class Table;
class TableView;
class ConstTableView;
class Array;
class Expression;

class Query {
public:
    Query(const Table& table);
    Query();
    Query(const Query& copy); // FIXME: Try to remove this
    ~Query() TIGHTDB_NOEXCEPT;

    Query& expression(Expression* compare, bool auto_delete = false);
    Expression* get_expression();

    // Conditions: Query only rows contained in tv
    Query& tableview(const TableView& tv);
    Query& tableview(const Array& arr);

    // Conditions: int64_t
    Query& equal(size_t column_ndx, int64_t value);
    Query& not_equal(size_t column_ndx, int64_t value);
    Query& greater(size_t column_ndx, int64_t value);
    Query& greater_equal(size_t column_ndx, int64_t value);
    Query& less(size_t column_ndx, int64_t value);
    Query& less_equal(size_t column_ndx, int64_t value);
    Query& between(size_t column_ndx, int64_t from, int64_t to);

    // Conditions: int (we need those because conversion from '1234' is ambiguous with float/double)
    Query& equal(size_t column_ndx, int value);
    Query& not_equal(size_t column_ndx, int value);
    Query& greater(size_t column_ndx, int value);
    Query& greater_equal(size_t column_ndx, int value);
    Query& less(size_t column_ndx, int value);
    Query& less_equal(size_t column_ndx, int value);
    Query& between(size_t column_ndx, int from, int to);

    // Conditions: 2 int columns
    Query& equal_int(size_t column_ndx1, size_t column_ndx2);
    Query& not_equal_int(size_t column_ndx1, size_t column_ndx2);
    Query& greater_int(size_t column_ndx1, size_t column_ndx2);
    Query& less_int(size_t column_ndx1, size_t column_ndx2);
    Query& greater_equal_int(size_t column_ndx1, size_t column_ndx2);
    Query& less_equal_int(size_t column_ndx1, size_t column_ndx2);

    // Conditions: float
    Query& equal(size_t column_ndx, float value);
    Query& not_equal(size_t column_ndx, float value);
    Query& greater(size_t column_ndx, float value);
    Query& greater_equal(size_t column_ndx, float value);
    Query& less(size_t column_ndx, float value);
    Query& less_equal(size_t column_ndx, float value);
    Query& between(size_t column_ndx, float from, float to);

    // Conditions: 2 float columns
    Query& equal_float(size_t column_ndx1, size_t column_ndx2);
    Query& not_equal_float(size_t column_ndx1, size_t column_ndx2);
    Query& greater_float(size_t column_ndx1, size_t column_ndx2);
    Query& greater_equal_float(size_t column_ndx1, size_t column_ndx2);
    Query& less_float(size_t column_ndx1, size_t column_ndx2);
    Query& less_equal_float(size_t column_ndx1, size_t column_ndx2);

     // Conditions: double
    Query& equal(size_t column_ndx, double value);
    Query& not_equal(size_t column_ndx, double value);
    Query& greater(size_t column_ndx, double value);
    Query& greater_equal(size_t column_ndx, double value);
    Query& less(size_t column_ndx, double value);
    Query& less_equal(size_t column_ndx, double value);
    Query& between(size_t column_ndx, double from, double to);

    // Conditions: 2 double columns
    Query& equal_double(size_t column_ndx1, size_t column_ndx2);
    Query& not_equal_double(size_t column_ndx1, size_t column_ndx2);
    Query& greater_double(size_t column_ndx1, size_t column_ndx2);
    Query& greater_equal_double(size_t column_ndx1, size_t column_ndx2);
    Query& less_double(size_t column_ndx1, size_t column_ndx2);
    Query& less_equal_double(size_t column_ndx1, size_t column_ndx2);

    // Conditions: bool
    Query& equal(size_t column_ndx, bool value);

    // Conditions: date
    Query& equal_date(size_t column_ndx, Date value) { return equal(column_ndx, int64_t(value.get_date())); }
    Query& not_equal_date(size_t column_ndx, Date value) { return not_equal(column_ndx, int64_t(value.get_date())); }
    Query& greater_date(size_t column_ndx, Date value) { return greater(column_ndx, int64_t(value.get_date())); }
    Query& greater_equal_date(size_t column_ndx, Date value) { return greater_equal(column_ndx, int64_t(value.get_date())); }
    Query& less_date(size_t column_ndx, Date value) { return less(column_ndx, int64_t(value.get_date())); }
    Query& less_equal_date(size_t column_ndx, Date value) { return less_equal(column_ndx, int64_t(value.get_date())); }
    Query& between_date(size_t column_ndx, Date from, Date to) { return between(column_ndx, int64_t(from.get_date()), int64_t(to.get_date())); }

    // Conditions: strings

    Query& equal(size_t column_ndx, StringData value, bool case_sensitive=true);
    Query& not_equal(size_t column_ndx, StringData value, bool case_sensitive=true);
    Query& begins_with(size_t column_ndx, StringData value, bool case_sensitive=true);
    Query& ends_with(size_t column_ndx, StringData value, bool case_sensitive=true);
    Query& contains(size_t column_ndx, StringData value, bool case_sensitive=true);

    // These are shortcuts for equal(StringData(c_str)) and
    // not_equal(StringData(c_str)), and are needed to avoid unwanted
    // implicit conversion of char* to bool.
    Query& equal(size_t column_ndx, const char* c_str, bool case_sensitive=true);
    Query& not_equal(size_t column_ndx, const char* c_str, bool case_sensitive=true);

    // Conditions: binary data
    Query& equal(size_t column_ndx, BinaryData value);
    Query& not_equal(size_t column_ndx, BinaryData value);
    Query& begins_with(size_t column_ndx, BinaryData value);
    Query& ends_with(size_t column_ndx, BinaryData value);
    Query& contains(size_t column_ndx, BinaryData value);

    // Grouping
    Query& group();
    Query& end_group();
    void subtable(size_t column);
    void end_subtable();
    Query& Or();

    Query& and_query(Query q);
    Query operator||(Query q); 
    Query operator&&(Query q); 



    // Searching
    size_t         find_first(size_t begin_row=size_t(0));
    TableView      find_all(size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1));
    ConstTableView find_all(size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1)) const;

    // Aggregates
    size_t count(size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1)) const;

    int64_t sum(    size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    double average( size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    int64_t maximum(size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    int64_t minimum(size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;

    double sum_float(     size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    double average_float( size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    float maximum_float(  size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    float minimum_float  (size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;

    double sum_double(    size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    double average_double(size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    double maximum_double(size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    double minimum_double(size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;

/*
  TODO:  time_t maximum_date(const Table& table, size_t column, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
  TODO:  time_t minimum_date(const Table& table, size_t column, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
*/

    // Deletion
    size_t  remove(size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1));

#if TIGHTDB_MULTITHREAD_QUERY
    // Multi-threading
    TableView      find_all_multi(size_t start=0, size_t end=size_t(-1));
    ConstTableView find_all_multi(size_t start=0, size_t end=size_t(-1)) const;
    int            set_threads(unsigned int threadcount);
#endif

    TableRef& get_table() {return m_table;}

#ifdef TIGHTDB_DEBUG
    std::string Verify(); // Must be upper case to avoid conflict with macro in ObjC
#endif
   
    mutable bool do_delete;

protected:
    Query(Table& table);
//    Query(const Table& table); // FIXME: This constructor should not exist. We need a ConstQuery class.
    void Create();

    void   Init(const Table& table) const;
    bool   is_initialized() const;
    size_t FindInternal(size_t start=0, size_t end=size_t(-1)) const;
    void   UpdatePointers(ParentNode* p, ParentNode** newnode);

    static bool  comp(const std::pair<size_t, size_t>& a, const std::pair<size_t, size_t>& b);

#if TIGHTDB_MULTITHREAD_QUERY
    static void* query_thread(void* arg);
    struct thread_state {
        pthread_mutex_t result_mutex;
        pthread_cond_t  completed_cond;
        pthread_mutex_t completed_mutex;
        pthread_mutex_t jobs_mutex;
        pthread_cond_t  jobs_cond;
        size_t next_job;
        size_t end_job;
        size_t done_job;
        size_t count;
        ParentNode* node;
        Table* table;
        std::vector<size_t> results;
        std::vector<std::pair<size_t, size_t> > chunks;
    } ts;
    static const size_t max_threads = 128;
    pthread_t threads[max_threads];
#endif

public:
    TableRef m_table;
    std::vector<ParentNode*> first;
    std::vector<ParentNode**> update;
    std::vector<ParentNode**> update_override;
    std::vector<ParentNode**> subtables;
    std::vector<ParentNode*> all_nodes;


private:
    template <class TColumnType> Query& equal(size_t column_ndx1, size_t column_ndx2);
    template <class TColumnType> Query& less(size_t column_ndx1, size_t column_ndx2);
    template <class TColumnType> Query& less_equal(size_t column_ndx1, size_t column_ndx2);
    template <class TColumnType> Query& greater(size_t column_ndx1, size_t column_ndx2);
    template <class TColumnType> Query& greater_equal(size_t column_ndx1, size_t column_ndx2);
    template <class TColumnType> Query& not_equal(size_t column_ndx1, size_t column_ndx2);

    std::string error_code;

#if TIGHTDB_MULTITHREAD_QUERY
    size_t m_threadcount;
#endif

    template <typename T, class N> Query& add_condition(size_t column_ndx, T value);
    template<typename T>
        double average(size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1)) const;
    template <Action action, typename T, typename R, class ColClass>
        R aggregate(R (ColClass::*method)(size_t, size_t, size_t) const,
                    size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const;

    friend class Table;
    template <typename T> friend class BasicTable;
    friend class XQueryAccessorInt;
    friend class XQueryAccessorString;
};




// Implementation:

inline Query& Query::equal(size_t column_ndx, const char* c_str, bool case_sensitive)
{
    return equal(column_ndx, StringData(c_str), case_sensitive);
}

inline Query& Query::not_equal(size_t column_ndx, const char* c_str, bool case_sensitive)
{
    return not_equal(column_ndx, StringData(c_str), case_sensitive);
}

} // namespace tightdb

#endif // TIGHTDB_QUERY_HPP
