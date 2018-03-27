/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/table_view.hpp>
#include <realm/column.hpp>

#include <unordered_set>

using namespace realm;

ConstTableView::ConstTableView(ConstTableView& src, HandoverPatch& patch, MutableSourcePayload mode)
    : ObjList(m_table_view_key_values)
    , m_source_column_key(src.m_source_column_key)
    , m_table_view_key_values(Allocator::get_default())
{
    // move the data payload, but make sure to leave the source array intact or
    // attempts to reuse it for a query rerun will crash (or assert, if lucky)
    // There really *has* to be a way where we don't need to first create an empty
    // array, and then destroy it
    if (src.m_key_values.is_attached()) {
        m_key_values = std::move(src.m_key_values); // Will leave src.m_key_values detached
        src.m_key_values.create();
    }
    else {
        m_key_values.create();
    }

    patch.was_in_sync = src.is_in_sync();
    // m_query must be exported after patch.was_in_sync is updated
    // as exporting m_query will bring src out of sync.
    m_query = Query(src.m_query, patch.query_patch, mode);

    Table::generate_patch(src.m_table, patch.m_table);
    LinkList::generate_patch(src.m_linklist_source.get(), patch.linklist_patch);
    DescriptorOrdering::generate_patch(src.m_descriptor_ordering, patch.descriptors_patch);

    if (src.m_source_column_key) {
        patch.linked_obj.reset(new ObjectHandoverPatch);
        Table::generate_patch(src.m_linked_obj.get_table(), patch.m_table);
        patch.linked_obj->key_value = src.m_linked_obj.get_key().value;
        patch.linked_col = src.m_source_column_key;
    }

    src.m_last_seen_versions.clear(); // bring source out-of-sync, now that it has lost its data
    m_last_seen_versions.clear();
    m_start = src.m_start;
    m_end = src.m_end;
    m_limit = src.m_limit;
}

ConstTableView::ConstTableView(const ConstTableView& src, HandoverPatch& patch, ConstSourcePayload mode)
    : ObjList(m_table_view_key_values)
    , m_source_column_key(src.m_source_column_key)
    , m_query(src.m_query, patch.query_patch, mode)
    , m_table_view_key_values(Allocator::get_default())
{
    if (mode == ConstSourcePayload::Copy && src.m_key_values.is_attached()) {
        m_key_values = src.m_key_values;
    }
    else {
        m_key_values.create();
    }

    if (mode == ConstSourcePayload::Stay)
        patch.was_in_sync = false;
    else
        patch.was_in_sync = src.is_in_sync();
    Table::generate_patch(src.m_table, patch.m_table);
    if (src.m_source_column_key) {
        patch.linked_obj.reset(new ObjectHandoverPatch);
        Table::generate_patch(src.m_linked_obj.get_table(), patch.m_table);
        patch.linked_obj->key_value = src.m_linked_obj.get_key().value;
        patch.linked_col = src.m_source_column_key;
    }
    LinkList::generate_patch(src.m_linklist_source.get(), patch.linklist_patch);
    DescriptorOrdering::generate_patch(src.m_descriptor_ordering, patch.descriptors_patch);

    m_last_seen_versions.clear();
    m_start = src.m_start;
    m_end = src.m_end;
    m_limit = src.m_limit;
}

void ConstTableView::apply_patch(HandoverPatch& patch, Group& group)
{
    m_table = Table::create_from_and_consume_patch(patch.m_table, group);
    m_query.apply_patch(patch.query_patch, group);
    m_linklist_source = LinkList::create_from_and_consume_patch(patch.linklist_patch, group);
    m_descriptor_ordering = DescriptorOrdering::create_from_and_consume_patch(patch.descriptors_patch, *m_table);

    if (patch.linked_obj) {
        TableRef table = Table::create_from_and_consume_patch(patch.m_table, group);
        m_linked_obj = table->get_object(ObjKey(patch.linked_obj->key_value));
        m_source_column_key = patch.linked_col;
    }

    if (patch.was_in_sync)
        m_last_seen_versions = outside_version();
    else
        m_last_seen_versions.clear();
}

// Aggregates ----------------------------------------------------

template <Action action, typename T, typename R>
R ConstTableView::aggregate(ColKey column_key, size_t* result_count, ObjKey* return_key) const
{
    check_cookie();
    size_t non_nulls = 0;

    if (return_key)
        *return_key = null_key;
    if (result_count)
        *result_count = 0;

    REALM_ASSERT(action == act_Sum || action == act_Max || action == act_Min || action == act_Average);
    REALM_ASSERT(m_table);
    REALM_ASSERT(m_table->valid_column(column_key));

    if ((m_key_values.size()) == 0) {
        return {};
    }

    // typedef typename ColTypeTraits::leaf_type ArrType;

    // FIXME: Optimization temporarely removed for stability
    /*
        if (m_num_detached_refs == 0 && m_key_values.size() == column->size()) {
            // direct aggregate on the column
            if (action == act_Count)
                return static_cast<R>(column->count(count_target));
            else
                return (column->*aggregateMethod)(0, size_t(-1), size_t(-1), return_ndx); // end == limit == -1
        }
    */

    // Array object instantiation must NOT allocate initial memory (capacity)
    // with 'new' because it will lead to mem leak. The column keeps ownership
    // of the payload in array and will free it itself later, so we must not call destroy() on array.
    // ArrType arr(column->get_alloc());

    // FIXME: Speed optimization disabled because we need is_null() which is not available on all leaf types.

/*
    const ArrType* arrp = nullptr;
    size_t leaf_start = 0;
    size_t leaf_end = 0;
    size_t row_ndx;
*/
    R res = R{};
    {
        ObjKey key(m_key_values.get(0));
        ConstObj obj = m_table->get_object(key);
        auto first = obj.get<T>(column_key);

        if (!obj.is_null(column_key)) { // cannot just use if(v) on float/double types
            res = static_cast<R>(util::unwrap(first));
            non_nulls++;
            if (return_key) {
                *return_key = key;
            }
        }
    }

    for (size_t tv_index = 1; tv_index < m_key_values.size(); ++tv_index) {

        ObjKey key(m_key_values.get(tv_index));

        // skip detached references:
        if (key == realm::null_key)
            continue;

        // FIXME: Speed optimization disabled because we need is_null() which is not available on all leaf types.
/*
        if (row_ndx < leaf_start || row_ndx >= leaf_end) {
            size_t ndx_in_leaf;
            typename ColType::LeafInfo leaf{&arrp, &arr};
            column->get_leaf(row_ndx, ndx_in_leaf, leaf);
            leaf_start = row_ndx - ndx_in_leaf;
            leaf_end = leaf_start + arrp->size();
        }
*/
        ConstObj obj = m_table->get_object(key);
        auto v = obj.get<T>(column_key);

        if (!obj.is_null(column_key)) {
            non_nulls++;
            R unpacked = static_cast<R>(util::unwrap(v));

            if (action == act_Sum || action == act_Average) {
                res += unpacked;
            }
            else if ((action == act_Max && unpacked > res) || non_nulls == 1) {
                res = unpacked;
                if (return_key)
                    *return_key = key;
            }
            else if ((action == act_Min && unpacked < res) || non_nulls == 1) {
                res = unpacked;
                if (return_key)
                    *return_key = key;
            }
        }
    }

    if (action == act_Average) {
        if (result_count)
            *result_count = non_nulls;
        return res / (non_nulls == 0 ? 1 : non_nulls);
    }

    return res;
}

template <typename T>
size_t ConstTableView::aggregate_count(ColKey column_key, T count_target) const
{
    check_cookie();
    REALM_ASSERT(m_table);
    REALM_ASSERT(m_table->valid_column(column_key));

    if ((m_key_values.size()) == 0) {
        return {};
    }

    size_t cnt = 0;
    for (size_t tv_index = 0; tv_index < m_key_values.size(); ++tv_index) {

        ObjKey key(m_key_values.get(tv_index));

        // skip detached references:
        if (key == realm::null_key)
            continue;

        ConstObj obj = m_table->get_object(key);
        auto v = obj.get<T>(column_key);

        if (v == count_target) {
            cnt++;
        }
    }

    return cnt;
}

// Min, Max and Count on Timestamp cannot utilize existing aggregate() methods, becuase these assume
// numeric types that support arithmetic (+, /, etc).
template <class C>
Timestamp ConstTableView::minmax_timestamp(ColKey column_key, ObjKey* return_key) const
{
    Timestamp best_value;
    ObjKey best_key;
    for_each([&best_key, &best_value, column_key](ConstObj& obj) {
        C compare;
        auto ts = obj.get<Timestamp>(column_key);
        // Because realm::Greater(non-null, null) == false, we need to pick the initial 'best' manually when we see
        // the first non-null entry
        if ((best_key == null_key && !ts.is_null()) || compare(ts, best_value, ts.is_null(), best_value.is_null())) {
            best_value = ts;
            best_key = obj.get_key();
        }
        return false;
    });
    if (return_key)
        *return_key = best_key;

    return best_value;
}

// sum
int64_t ConstTableView::sum_int(ColKey column_key) const
{
    if (m_table->is_nullable(column_key))
        return aggregate<act_Sum, util::Optional<int64_t>, int64_t>(column_key, 0);
    else {
        return aggregate<act_Sum, int64_t, int64_t>(column_key, 0);
    }
}
double ConstTableView::sum_float(ColKey column_key) const
{
    return aggregate<act_Sum, float, double>(column_key);
}
double ConstTableView::sum_double(ColKey column_key) const
{
    return aggregate<act_Sum, double, double>(column_key);
}

// Maximum
int64_t ConstTableView::maximum_int(ColKey column_key, ObjKey* return_key) const
{
    if (m_table->is_nullable(column_key))
        return aggregate<act_Max, util::Optional<int64_t>, int64_t>(column_key, nullptr, return_key);
    else
        return aggregate<act_Max, int64_t, int64_t>(column_key, nullptr, return_key);
}
float ConstTableView::maximum_float(ColKey column_key, ObjKey* return_key) const
{
    return aggregate<act_Max, float, float>(column_key, nullptr, return_key);
}
double ConstTableView::maximum_double(ColKey column_key, ObjKey* return_key) const
{
    return aggregate<act_Max, double, double>(column_key, nullptr, return_key);
}
Timestamp ConstTableView::maximum_timestamp(ColKey column_key, ObjKey* return_key) const
{
    return minmax_timestamp<realm::Greater>(column_key, return_key);
}


// Minimum
int64_t ConstTableView::minimum_int(ColKey column_key, ObjKey* return_key) const
{
    if (m_table->is_nullable(column_key))
        return aggregate<act_Min, util::Optional<int64_t>, int64_t>(column_key, nullptr, return_key);
    else
        return aggregate<act_Min, int64_t, int64_t>(column_key, nullptr, return_key);
}
float ConstTableView::minimum_float(ColKey column_key, ObjKey* return_key) const
{
    return aggregate<act_Min, float, float>(column_key, nullptr, return_key);
}
double ConstTableView::minimum_double(ColKey column_key, ObjKey* return_key) const
{
    return aggregate<act_Min, double, double>(column_key, nullptr, return_key);
}
Timestamp ConstTableView::minimum_timestamp(ColKey column_key, ObjKey* return_key) const
{
    return minmax_timestamp<realm::Less>(column_key, return_key);
}

// Average. The number of values used to compute the result is written to `value_count` by callee
double ConstTableView::average_int(ColKey column_key, size_t* value_count) const
{
    if (m_table->is_nullable(column_key))
        return aggregate<act_Average, util::Optional<int64_t>, double>(column_key, value_count);
    else
        return aggregate<act_Average, int64_t, double>(column_key, value_count);
}
double ConstTableView::average_float(ColKey column_key, size_t* value_count) const
{
    return aggregate<act_Average, float, double>(column_key, value_count);
}
double ConstTableView::average_double(ColKey column_key, size_t* value_count) const
{
    return aggregate<act_Average, double, double>(column_key, value_count);
}

// Count
size_t ConstTableView::count_int(ColKey column_key, int64_t target) const
{
    if (m_table->is_nullable(column_key))
        return aggregate_count<util::Optional<int64_t>>(column_key, target);
    else
        return aggregate_count<int64_t>(column_key, target);
}
size_t ConstTableView::count_float(ColKey column_key, float target) const
{
    return aggregate_count<float>(column_key, target);
}
size_t ConstTableView::count_double(ColKey column_key, double target) const
{
    return aggregate_count<double>(column_key, target);
}

size_t ConstTableView::count_timestamp(ColKey column_key, Timestamp target) const
{
    size_t count = 0;
    for (size_t t = 0; t < size(); t++) {
        try {
            ObjKey key = m_key_values.get(t);
            ConstObj obj = m_table->get_object(key);
            auto ts = obj.get<Timestamp>(column_key);
            realm::Equal e;
            if (e(ts, target, ts.is_null(), target.is_null())) {
                count++;
            }
        }
        catch (const InvalidKey&) {
            // Just skip objects that might have been deleted
        }
    }
    return count;
}

void ConstTableView::to_json(std::ostream& out) const
{
    check_cookie();

    // Represent table as list of objects
    out << "[";

    const size_t row_count = size();
    for (size_t r = 0; r < row_count; ++r) {
        ObjKey key = get_key(r);
        if (key != realm::null_key) {
            if (r > 0)
                out << ",";
            m_table->to_json_row(size_t(key.value), out); // FIXME
        }
    }

    out << "]";
}

void ConstTableView::to_string(std::ostream& out, size_t limit) const
{
    check_cookie();

    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Set limit=-1 to print all rows, otherwise only print to limit
    const size_t row_count = size();
    const size_t out_count = (limit == size_t(-1)) ? row_count : (row_count < limit) ? row_count : limit;

    // Print rows
    size_t i = 0;
    size_t count = out_count;
    while (count) {
        ObjKey key = get_key(count);
        if (key != realm::null_key) {
            m_table->to_string_row(key, out, widths); // FIXME
            --count;
        }
        ++i;
    }

    if (out_count < row_count) {
        const size_t rest = row_count - out_count;
        out << "... and " << rest << " more rows (total " << row_count << ")";
    }
}

void ConstTableView::row_to_string(size_t row_ndx, std::ostream& out) const
{
    check_cookie();

    REALM_ASSERT(row_ndx < m_key_values.size());

    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Print row contents
    ObjKey key = get_key(row_ndx);
    REALM_ASSERT(key != realm::null_key);
    m_table->to_string_row(key, out, widths); // FIXME
}


bool ConstTableView::depends_on_deleted_object() const
{
    // outside_version() will call itself recursively for each TableView in the dependency chain
    // and terminate with `max` if the deepest depends on a deleted LinkList or Row
    return outside_version().empty();
}

// Return version of whatever this TableView depends on
TableVersions ConstTableView::outside_version() const
{
    check_cookie();

    if (m_linklist_source) {
        // m_linkview_source is set when this TableView was created by LinkView::get_as_sorted_view().
        if (m_linklist_source->is_attached()) {
            Table& table = m_linklist_source->get_target_table();
            return {table.get_key(), table.get_content_version()};
        }
        else {
            return {};
        }
    }

    if (m_source_column_key) {
        // m_linked_column is set when this TableView was created by Table::get_backlink_view().
        if (m_linked_obj.is_valid()) {
            auto table = m_linked_obj.get_table();
            return {table->get_key(), table->get_content_version()};
        }
        else {
            return {};
        }
    }
    else if (m_query.m_table) {
        return m_query.get_outside_versions();
    }

    // This TableView was created by Table::get_distinct_view()
    return {m_table->get_key(), m_table->get_content_version()};
}

bool ConstTableView::is_in_sync() const
{
    check_cookie();

    bool table = bool(m_table);
    bool version = bool(m_last_seen_versions == outside_version());
    bool view = bool(m_query.m_view);

    return table && version && (view ? m_query.m_view->is_in_sync() : true);
}

TableVersions ConstTableView::sync_if_needed() const
{
    if (!is_in_sync()) {
        // FIXME: Is this a reasonable handling of constness?
        const_cast<ConstTableView*>(this)->do_sync();
    }
    return m_last_seen_versions;
}


void TableView::remove(size_t row_ndx)
{
    REALM_ASSERT(m_table);
    REALM_ASSERT(row_ndx < m_key_values.size());

    bool sync_to_keep = m_last_seen_versions == outside_version();

    ObjKey key = m_key_values.get(row_ndx);

    // Update refs
    m_key_values.erase(row_ndx);

    // Delete row in origin table
    get_parent().remove_object(key);

    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_versions = outside_version();

    // Adjustment of row indexes greater than the removed index is done by
    // adj_row_acc_move_over or adj_row_acc_erase_row as sideeffect of the actual
    // update of the table, so we don't need to do it here (it has already been done)
}


void TableView::clear()
{
    REALM_ASSERT(m_table);

    bool sync_to_keep = m_last_seen_versions == outside_version();

    _impl::TableFriend::batch_erase_rows(get_parent(), m_key_values); // Throws

    m_key_values.clear();

    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_versions = outside_version();
}

void ConstTableView::distinct(ColKey column)
{
    distinct(DistinctDescriptor(*m_table, {{column}}));
}

/// Remove rows that are duplicated with respect to the column set passed as argument.
/// Will keep original sorting order so that you can both have a distinct and sorted view.
void ConstTableView::distinct(DistinctDescriptor columns)
{
    m_descriptor_ordering.append_distinct(std::move(columns));
    do_sync();
}

void ConstTableView::apply_descriptor_ordering(DescriptorOrdering new_ordering)
{
    m_descriptor_ordering = new_ordering;
    do_sync();
}

std::string ConstTableView::get_descriptor_ordering_description() const
{
    return m_descriptor_ordering.get_description(m_table);
}

// Sort according to one column
void ConstTableView::sort(ColKey column, bool ascending)
{
    sort(SortDescriptor(*m_table, {{column}}, {ascending}));
}

// Sort according to multiple columns, user specified order on each column
void ConstTableView::sort(SortDescriptor order)
{
    m_descriptor_ordering.append_sort(std::move(order));
    do_sort(m_descriptor_ordering);
}


void ConstTableView::do_sync()
{
    // This TableView can be "born" from 4 different sources:
    // - LinkView
    // - Query::find_all()
    // - Table::get_distinct_view()
    // - Table::get_backlink_view()
    // Here we sync with the respective source.
    m_last_seen_versions.clear();

    if (m_linklist_source) {
        m_key_values.clear();
        std::for_each(m_linklist_source->begin(), m_linklist_source->end(),
                      [this](ObjKey key) { m_key_values.add(key); });
    }
    else if (m_distinct_column_source) {
        m_key_values.clear();
        auto index = m_table->get_search_index(m_distinct_column_source);
        REALM_ASSERT(index);
        index->distinct(m_key_values);
    }
    else if (m_source_column_key) {
        m_key_values.clear();
        if (m_linked_obj.is_valid() && m_table) {
            TableKey origin_table_key = m_table->get_key();
            const Table* target_table = m_linked_obj.get_table();
            const Spec& spec = _impl::TableFriend::get_spec(*target_table);
            size_t backlink_col_ndx = spec.find_backlink_column(origin_table_key, m_source_column_key);
            if (backlink_col_ndx != realm::npos) {
                size_t backlink_count = m_linked_obj.get_backlink_count(backlink_col_ndx);
                for (size_t i = 0; i < backlink_count; i++)
                    m_key_values.add(m_linked_obj.get_backlink(backlink_col_ndx, i));
            }
        }
    }
    // FIXME: Unimplemented for link to a column
    else {
        REALM_ASSERT(m_query.m_table);

        // valid query, so clear earlier results and reexecute it.
        if (m_key_values.is_attached())
            m_key_values.clear();
        else
            m_key_values.create();

        if (m_query.m_view)
            m_query.m_view->sync_if_needed();

        m_query.find_all(*const_cast<ConstTableView*>(this), m_start, m_end, m_limit);
    }

    do_sort(m_descriptor_ordering);

    m_last_seen_versions = outside_version();
}

bool ConstTableView::is_in_table_order() const
{
    if (!m_table) {
        return false;
    }
    else if (m_linklist_source) {
        return false;
    }
    else if (m_distinct_column_source) {
        return !m_descriptor_ordering.will_apply_sort();
    }
    else if (m_source_column_key) {
        return false;
    }
    else {
        REALM_ASSERT(m_query.m_table);
        return m_query.produces_results_in_table_order() && !m_descriptor_ordering.will_apply_sort();
    }
}
