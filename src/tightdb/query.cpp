#include <tightdb/array.hpp>
#include <tightdb/column_basic.hpp>
#include <tightdb/column_fwd.hpp>
#include <tightdb/query.hpp>
#include <tightdb/query_engine.hpp>


#define MULTITHREAD 0

using namespace tightdb;

const size_t THREAD_CHUNK_SIZE = 1000;

#define MIN(a, b)  (((a) < (b)) ? (a) : (b))
#define MAX(a, b)  (((a) > (b)) ? (a) : (b))

Query::Query(Table& table) : m_table(table.get_table_ref())
{
    Create();
}

Query::Query(const Table& table) : m_table(((Table&)table).get_table_ref())
{
    Create();
}

void Query::Create()
{
    update.push_back(0);
    update_override.push_back(0);
    first.push_back(0);
    m_threadcount = 0;
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
    m_threadcount = copy.m_threadcount;
//    copy.first[0] = 0;
    copy.do_delete = false;
    do_delete = true;
}

Query::~Query()
{
#if MULTITHREAD
    for (size_t i = 0; i < m_threadcount; i++)
        pthread_detach(threads[i]);
#endif
    if (do_delete) {
        for (size_t t = 0; t < all_nodes.size(); t++) {
            ParentNode *p = all_nodes[t];
            delete p;
        }
    }
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
    ParentNode* const p = new ArrayNode(arr);
    UpdatePointers(p, &p->m_child);
    return *this;
}


// Binary
Query& Query::equal(size_t column_ndx, BinaryData b)
{
    ParentNode* const p = new BinaryNode<EQUAL>(b.pointer, b.len, column_ndx);
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

// int64
Query& Query::equal(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, EQUAL>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, NOTEQUAL>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::greater(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, GREATER>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::greater_equal(size_t column_ndx, int64_t value)
{
    if (value > LLONG_MIN) {
        ParentNode* const p = new IntegerNode<int64_t, GREATER>(value - 1, column_ndx);
        UpdatePointers(p, &p->m_child);
    }
    // field >= LLONG_MIN has no effect
    return *this;
}
Query& Query::less_equal(size_t column_ndx, int64_t value)
{
    if (value < LLONG_MAX) {
        ParentNode* const p = new IntegerNode<int64_t, LESS>(value + 1, column_ndx);
        UpdatePointers(p, &p->m_child);
    }
    // field <= LLONG_MAX has no effect
    return *this;
}
Query& Query::less(size_t column_ndx, int64_t value)
{
    ParentNode* const p = new IntegerNode<int64_t, LESS>(value, column_ndx);
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
    ParentNode* const p = new IntegerNode<bool, EQUAL>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}

// ------------- float
Query& Query::equal(size_t column_ndx, float value)
{
    return add_condition<float, BASICNODE<float, EQUAL> >(column_ndx, value);
}
Query& Query::not_equal(size_t column_ndx, float value)
{
    return add_condition<float, BASICNODE<float, NOTEQUAL> >(column_ndx, value);
}
Query& Query::greater(size_t column_ndx, float value)
{
    return add_condition<float, BASICNODE<float, GREATER> >(column_ndx, value);
}
Query& Query::greater_equal(size_t column_ndx, float value)
{
    return add_condition<float, BASICNODE<float, GREATER_EQUAL> >(column_ndx, value);
}
Query& Query::less_equal(size_t column_ndx, float value)
{
    return add_condition<float, BASICNODE<float, LESS_EQUAL> >(column_ndx, value);
}
Query& Query::less(size_t column_ndx, float value)
{
    return add_condition<float, BASICNODE<float, LESS> >(column_ndx, value);
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
    return add_condition<double, BASICNODE<double, EQUAL> >(column_ndx, value);
}
Query& Query::not_equal(size_t column_ndx, double value)
{
    return add_condition<double, BASICNODE<double, NOTEQUAL> >(column_ndx, value);
}
Query& Query::greater(size_t column_ndx, double value)
{
    return add_condition<double, BASICNODE<double, GREATER> >(column_ndx, value);
}
Query& Query::greater_equal(size_t column_ndx, double value)
{
    return add_condition<double, BASICNODE<double, GREATER_EQUAL> >(column_ndx, value);
}
Query& Query::less_equal(size_t column_ndx, double value)
{
    return add_condition<double, BASICNODE<double, LESS_EQUAL> >(column_ndx, value);
}
Query& Query::less(size_t column_ndx, double value)
{
    return add_condition<double, BASICNODE<double, LESS> >(column_ndx, value);
}
Query& Query::between(size_t column_ndx, double from, double to)
{
    greater_equal(column_ndx, from);
    less_equal(column_ndx, to);
    return *this;
}

// STRINGS
Query& Query::equal(size_t column_ndx, const char* value, bool caseSensitive)
{
    ParentNode* p;
    if (caseSensitive)
        p = new StringNode<EQUAL>(value, column_ndx);
    else
        p = new StringNode<EQUAL_INS>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::begins_with(size_t column_ndx, const char* value, bool caseSensitive)
{
    ParentNode* p;
    if (caseSensitive)
        p = new StringNode<BEGINSWITH>(value, column_ndx);
    else
        p = new StringNode<BEGINSWITH_INS>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::ends_with(size_t column_ndx, const char* value, bool caseSensitive)
{
    ParentNode* p;
    if (caseSensitive)
        p = new StringNode<ENDSWITH>(value, column_ndx);
    else
        p = new StringNode<ENDSWITH_INS>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::contains(size_t column_ndx, const char* value, bool caseSensitive)
{
    ParentNode* p;
    if (caseSensitive)
        p = new StringNode<CONTAINS>(value, column_ndx);
    else
        p = new StringNode<CONTAINS_INS>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, const char* value, bool caseSensitive)
{
    ParentNode* p;
    if (caseSensitive)
        p = new StringNode<NOTEQUAL>(value, column_ndx);
    else
        p = new StringNode<NOTEQUAL_INS>(value, column_ndx);
    UpdatePointers(p, &p->m_child);
    return *this;
}


// Aggregates =================================================================================

template <ACTION action, typename T, typename R, class ColType>
R Query::aggregate(R (ColType::*aggregateMethod)(size_t start, size_t end) const,
                    size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if (end == size_t(-1))
        end = m_table->size();

    const ColType& column =
        m_table->GetColumn<ColType, ColumnType(ColumnTypeTraits<T>::id)>(column_ndx);

    if (first.size() == 0 || first[0] == 0) {
        // User created query with no criteria; aggregate range
        if (resultcount)
            *resultcount = end-start;
        // direct aggregate on the column
        return (column.*aggregateMethod)(start, end);
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

int64_t Query::sum(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_SUM, int64_t>(&Column::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_SUM, float>(&ColumnFloat::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_SUM, double>(&ColumnDouble::sum, column_ndx, resultcount, start, end, limit);
}

// Maximum

int64_t Query::maximum(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_MAX, int64_t>(&Column::maximum, column_ndx, resultcount, start, end, limit);
}
float Query::maximum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_MAX, float>(&ColumnFloat::maximum, column_ndx, resultcount, start, end, limit);
}
double Query::maximum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_MAX, double>(&ColumnDouble::maximum, column_ndx, resultcount, start, end, limit);
}

// Minimum

int64_t Query::minimum(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_MIN, int64_t>(&Column::minimum, column_ndx, resultcount, start, end, limit);
}
float Query::minimum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_MIN, float>(&ColumnFloat::minimum, column_ndx, resultcount, start, end, limit);
}
double Query::minimum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<TDB_MIN, double>(&ColumnDouble::minimum, column_ndx, resultcount, start, end, limit);
}

// Average

template <typename T>
double Query::average(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    size_t resultcount2 = 0;
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    typedef typename ColumnTypeTraits<T>::sum_type SumType;
    const SumType sum1 = aggregate<TDB_SUM, T>(&ColType::sum, column_ndx, &resultcount2, start, end, limit);
    double avg1 = 0;
    if (resultcount2 != 0)
        avg1 = static_cast<double>(sum1) / resultcount2;
    if (resultcount)
        *resultcount = resultcount2;
    return avg1;
}

double Query::average(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
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
    ParentNode* const o = new OR_NODE(first[first.size()-1]);
    all_nodes.push_back(o);

    first[first.size()-1] = o;
    update[update.size()-1] = &((OR_NODE*)o)->m_cond[1];
    update_override[update_override.size()-1] = &((OR_NODE*)o)->m_child;
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


size_t Query::find_next(size_t lastmatch)
{
    if (lastmatch == size_t(-1)) Init(*m_table);

    const size_t end = m_table->size();
    const size_t res = first[0]->find_first(lastmatch + 1, end);

    return (res == end) ? not_found : res;
}

TableView Query::find_all(size_t start, size_t end, size_t limit)
{
    Init(*m_table);

    if (end == size_t(-1))
        end = m_table->size();

    // User created query with no criteria; return everything
    if (first.size() == 0 || first[0] == 0) {
        TableView tv(*m_table);
        for (size_t i = start; i < end && i - start < limit; i++)
            tv.get_ref_column().add(i);
        return move(tv);
    }

    if (m_threadcount > 0) {
        // Use multithreading
        return find_all_multi(start, end);
    }

    // Use single threading
    TableView tv(*m_table);
    QueryState<int64_t> st;
    st.init(TDB_FINDALL, &tv.get_ref_column(), limit);
    first[0]->aggregate<TDB_FINDALL, int64_t, int64_t>(&st, start, end, not_found, NULL);
    return move(tv);
}


size_t Query::count(size_t start, size_t end, size_t limit) const
{
    if (end == size_t(-1)) 
        end = m_table->size();

    if (first.size() == 0 || first[0] == 0) {
        // User created query with no criteria; count all
        return (limit < end - start ? limit : end - start);
    }

    Init(*m_table);
    QueryState<int64_t> st;
    st.init(TDB_COUNT, NULL, limit);
    int64_t r = first[0]->aggregate<TDB_COUNT, int64_t, int64_t>(&st, start, end, not_found, NULL);
    return size_t(r);
}

#include <cstdio>

// todo, not sure if start, end and limit could be useful for delete.
size_t Query::remove(size_t start, size_t end, size_t limit)
{
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

TableView Query::find_all_multi(size_t start, size_t end)
{
    (void)start;
    (void)end;

#if MULTITHREAD
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
    std::sort (ts.chunks.begin(), ts.chunks.end(), &Query::comp);
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
#else
    return NULL;
#endif
}

int Query::set_threads(unsigned int threadcount)
{
#if MULTITHREAD
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
#endif
    m_threadcount = threadcount;
    return 0;
}

#ifdef TIGHTDB_DEBUG
std::string Query::Verify()
{
    if (first.size() == 0)
        return "";

    if (error_code != "") // errors detected by QueryInterface
        return error_code;

    if (first[0] == 0)
        return "Syntax error";

    return first[0]->Verify(); // errors detected by QueryEngine
}
#endif // TIGHTDB_DEBUG

void Query::Init(const Table& table) const
{
    if (first[0] != NULL) {
        ParentNode* const top = (ParentNode*)first[0];
        top->Init(table);
        std::vector<ParentNode*>v;
        top->gather_children(v);
    }
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

void Query::UpdatePointers(ParentNode* p, ParentNode** newnode)
{
    all_nodes.push_back(p);
    if (first[first.size()-1] == 0)
        first[first.size()-1] = p;

    if (update[update.size()-1] != 0)
        *update[update.size()-1] = p;

    update[update.size()-1] = newnode;
}

bool Query::comp(const std::pair<size_t, size_t>& a, const std::pair<size_t, size_t>& b)
{
    return a.first < b.first;
}


void* Query::query_thread(void* arg)
{
    (void)arg;
#if MULTITHREAD
    thread_state* ts = (thread_state*)arg;

    std::vector<size_t> res;
    std::vector<std::pair<size_t, size_t> > chunks;

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
            const size_t chunk = MIN(ts->end_job - ts->next_job, THREAD_CHUNK_SIZE);
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
                ts->chunks.push_back(std::pair<size_t, size_t>(mine, ts->results.size()));
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
#endif
    return 0;
}


