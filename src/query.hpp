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
#ifndef __TIGHTDB_QUERY_HPP
#define __TIGHTDB_QUERY_HPP

#include <string>
#include <algorithm>
#include <vector>
#include <stdio.h>
#include <limits.h>
#if defined(_WIN32) || defined(__WIN32__) || defined(_WIN64)
    #include "win32/pthread/pthread.h"
    #include "win32/stdint.h"
#else
    #include <pthread.h>
#endif

namespace tightdb {

// Pre-declarations
class ParentNode;
class Table;
class TableView;

const size_t MAX_THREADS = 128;

class Query {
public:
    Query();
    Query(const Query& copy); // FIXME: Try to remove this
    ~Query();

    // Conditions: int and bool
    Query& equal(size_t column_ndx, int64_t value);
    Query& equal(size_t column_ndx, bool value);
    Query& not_equal(size_t column_ndx, int64_t value);
    Query& greater(size_t column_ndx, int64_t value);
    Query& greater_equal(size_t column_ndx, int64_t value);
    Query& less(size_t column_ndx, int64_t value);
    Query& less_equal(size_t column_ndx, int64_t value);
    Query& between(size_t column_ndx, int64_t from, int64_t to);

    // Conditions: strings
    Query& equal(size_t column_ndx, const char* value, bool caseSensitive=true);
    Query& begins_with(size_t column_ndx, const char* value, bool caseSensitive=true);
    Query& ends_with(size_t column_ndx, const char* value, bool caseSensitive=true);
    Query& contains(size_t column_ndx, const char* value, bool caseSensitive=true);
    Query& not_equal(size_t column_ndx, const char* value, bool caseSensitive=true);

    // Grouping
    void group();
    void end_group();
    void subtable(size_t column);
    void parent();
    void Or();

    // Searching
    size_t    find_next(Table& table, size_t lastmatch=-1);
    TableView find_all(Table& table, size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1));
    void      find_all(Table& table, TableView& tv, size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1));
    
    // Aggregates
    int64_t sum(const Table& table, size_t column, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    int64_t maximum(const Table& table, size_t column, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    int64_t minimum(const Table& table, size_t column, size_t* resultcount=NULL, size_t start=0, size_t end = size_t(-1), size_t limit=size_t(-1)) const;
    double  average(const Table& table, size_t column_ndx, size_t* resultcount=NULL, size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1)) const;
    size_t  count(const Table& table, size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1)) const;
    
    // Deletion
    size_t  remove(Table& table, size_t start=0, size_t end=size_t(-1), size_t limit=size_t(-1)) const;
    
    // Multi-threading
    void FindAllMulti(Table& table, TableView& tv, size_t start=0, size_t end=size_t(-1));
    int  SetThreads(unsigned int threadcount);

    std::string Verify();
    
    std::string error_code;
    
protected:
    friend class XQueryAccessorInt;
    friend class XQueryAccessorString;

    void   Init(const Table& table) const;
    size_t FindInternal(const Table& table, size_t start=0, size_t end=size_t(-1)) const;
    void   UpdatePointers(ParentNode* p, ParentNode** newnode);
    
    static bool  comp(const std::pair<size_t, size_t>& a, const std::pair<size_t, size_t>& b);
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
    pthread_t threads[MAX_THREADS];

    mutable std::vector<ParentNode*> first;
    std::vector<ParentNode**> update;
    std::vector<ParentNode**> update_override;
    std::vector<ParentNode**> subtables;

private:
    size_t m_threadcount;
};


}

#endif // __TIGHTDB_QUERY_HPP
