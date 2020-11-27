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
#include <realm/column_integer.hpp>
#include <realm/index_string.hpp>
#include <realm/db.hpp>

#include <unordered_set>

using namespace realm;

ConstTableView::ConstTableView(ConstTableView& src, Transaction*, PayloadPolicy)
    : m_source_column_key(src.m_source_column_key)
    , m_key_values(Allocator::get_default())
{
    REALM_ASSERT(false); // unimplemented
}

ConstTableView::ConstTableView(const ConstTableView& src, Transaction* tr, PayloadPolicy mode)
    : m_source_column_key(src.m_source_column_key)
    , m_linked_obj_key(src.m_linked_obj_key)
    , m_key_values(Allocator::get_default())
{
    bool was_in_sync = src.is_in_sync();
    m_query = Query(src.m_query, tr, mode);
    m_table = tr->import_copy_of(src.m_table);

    if (mode == PayloadPolicy::Stay)
        was_in_sync = false;

    /*
    VersionID src_version =
        dynamic_cast<Transaction*>(src.m_table->get_parent_group())->get_version_of_current_transaction();
    if (src_version != tr->get_version_of_current_transaction())
        throw realm::LogicError(LogicError::bad_version);
    */

    if (was_in_sync)
        m_last_seen_versions = get_dependency_versions();
    else
        m_last_seen_versions.clear();
    m_table = tr->import_copy_of(src.m_table);
    m_linklist_source = tr->import_copy_of(src.m_linklist_source);
    m_linkset_source = tr->import_copy_of(src.m_linkset_source);
    if (src.m_source_column_key) {
        m_linked_table = tr->import_copy_of(src.m_linked_table);
    }
    // don't use methods which throw after this point...or m_table_view_key_values will leak
    if (mode == PayloadPolicy::Copy && src.m_key_values.is_attached()) {
        m_key_values = src.m_key_values;
    }
    else if (mode == PayloadPolicy::Move && src.m_key_values.is_attached())
        m_key_values = std::move(src.m_key_values);
    else {
        m_key_values.create();
    }
    if (mode == PayloadPolicy::Move) {
        src.m_last_seen_versions.clear();
    }
    m_descriptor_ordering = src.m_descriptor_ordering;
    m_start = src.m_start;
    m_end = src.m_end;
    m_limit = src.m_limit;
}

// Aggregates ----------------------------------------------------

template <Action action, typename T, typename R>
R ConstTableView::aggregate(ColKey column_key, size_t* result_count, ObjKey* return_key) const
{
    size_t non_nulls = 0;

    if (return_key)
        *return_key = null_key;
    if (result_count)
        *result_count = 0;

    REALM_ASSERT(action == act_Sum || action == act_Max || action == act_Min || action == act_Average);
    REALM_ASSERT(m_table->valid_column(column_key));

    if ((m_key_values.size()) == 0) {
        return {};
    }

    // typedef typename ColTypeTraits::leaf_type ArrType;

    // FIXME: Optimization temporarely removed for stability
    /*
        if (m_num_detached_refs == 0 && m_table_view_key_values.size() == column->size()) {
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
    bool is_first = true;
    for (size_t tv_index = 0; tv_index < m_key_values.size(); ++tv_index) {

        ObjKey key(get_key(tv_index));

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
        // aggregation must be robust in the face of stale keys:
        if (!m_table->is_valid(key))
            continue;

        const Obj obj = m_table->get_object(key);
        auto v = obj.get<T>(column_key);

        if (!obj.is_null(column_key)) {
            non_nulls++;
            R unpacked = static_cast<R>(util::unwrap(v));

            if (is_first) {
                if (return_key) {
                    *return_key = key;
                }
                res = unpacked;
                is_first = false;
            }
            else if (action == act_Sum || action == act_Average) {
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
    REALM_ASSERT(m_table->valid_column(column_key));

    if ((m_key_values.size()) == 0) {
        return {};
    }

    size_t cnt = 0;
    for (size_t tv_index = 0; tv_index < m_key_values.size(); ++tv_index) {

        ObjKey key(get_key(tv_index));

        // skip detached references:
        if (key == realm::null_key)
            continue;

        try {
            const Obj obj = m_table->get_object(key);
            auto v = obj.get<T>(column_key);

            if (v == count_target) {
                cnt++;
            }
        }
        catch (const realm::KeyNotFound&) {
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
    for_each([&best_key, &best_value, column_key](const Obj& obj) {
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

Decimal128 ConstTableView::sum_decimal(ColKey column_key) const
{
    return aggregate<act_Sum, Decimal128, Decimal128>(column_key);
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
Decimal128 ConstTableView::maximum_decimal(ColKey column_key, ObjKey* return_key) const
{
    return aggregate<act_Max, Decimal128, Decimal128>(column_key, nullptr, return_key);
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
Decimal128 ConstTableView::minimum_decimal(ColKey column_key, ObjKey* return_key) const
{
    return aggregate<act_Min, Decimal128, Decimal128>(column_key, nullptr, return_key);
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
Decimal128 ConstTableView::average_decimal(ColKey column_key, size_t* value_count) const
{
    return aggregate<act_Average, Decimal128, Decimal128>(column_key, value_count);
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
            ObjKey key = get_key(t);
            const Obj obj = m_table->get_object(key);
            auto ts = obj.get<Timestamp>(column_key);
            realm::Equal e;
            if (e(ts, target, ts.is_null(), target.is_null())) {
                count++;
            }
        }
        catch (const KeyNotFound&) {
            // Just skip objects that might have been deleted
        }
    }
    return count;
}

size_t ConstTableView::count_decimal(ColKey column_key, Decimal128 target) const
{
    return aggregate_count<Decimal128>(column_key, target);
}

void ConstTableView::to_json(std::ostream& out, size_t link_depth) const
{
    // Represent table as list of objects
    out << "[";

    const size_t row_count = size();
    bool first = true;
    for (size_t r = 0; r < row_count; ++r) {
        if (ObjKey key = get_key(r)) {
            if (first) {
                first = false;
            }
            else {
                out << ",";
            }
            m_table->get_object(key).to_json(out, link_depth, {}, output_mode_json);
        }
    }

    out << "]";
}

bool ConstTableView::depends_on_deleted_object() const
{
    if (m_linklist_source && !m_linklist_source->is_attached()) {
        return true;
    }
    if (m_linkset_source && !m_linkset_source->is_attached()) {
        return true;
    }

    if (m_source_column_key && !(m_linked_table && m_linked_table->is_valid(m_linked_obj_key))) {
        return true;
    }
    else if (m_query.m_source_table_view) {
        return m_query.m_source_table_view->depends_on_deleted_object();
    }
    return false;
}

void ConstTableView::get_dependencies(TableVersions& ret) const
{
    if (m_linklist_source) {
        // m_linkview_source is set when this TableView was created by LinkView::get_as_sorted_view().
        if (m_linklist_source->is_attached()) {
            Table& table = *m_linklist_source->get_target_table();
            ret.emplace_back(table.get_key(), table.get_content_version());
        }
    }
    else if (m_linkset_source) {
        // m_linkview_source is set when this TableView was created by LinkView::get_as_sorted_view().
        if (m_linkset_source->is_attached()) {
            Table& table = *m_linkset_source->get_target_table();
            ret.emplace_back(table.get_key(), table.get_content_version());
        }
    }
    else if (m_source_column_key) {
        // m_source_column_key is set when this TableView was created by Table::get_backlink_view().
        if (m_linked_table) {
            ret.emplace_back(m_linked_table->get_key(), m_linked_table->get_content_version());
        }
    }
    else if (m_query.m_table) {
        m_query.get_outside_versions(ret);
    }
    else {
        // This TableView was created by Table::get_distinct_view()
        ret.emplace_back(m_table->get_key(), m_table->get_content_version());
    }

    // Finally add dependencies from sort/distinct
    if (m_table) {
        m_descriptor_ordering.get_versions(m_table->get_parent_group(), ret);
    }
}

bool ConstTableView::is_in_sync() const
{
    return !m_table ? false : m_last_seen_versions == get_dependency_versions();
}

void ConstTableView::sync_if_needed() const
{
    if (!is_in_sync()) {
        // FIXME: Is this a reasonable handling of constness?
        const_cast<ConstTableView*>(this)->do_sync();
    }
}

void ConstTableView::update_query(const Query& q)
{
    REALM_ASSERT(m_query.m_table);
    REALM_ASSERT(m_query.m_table == q.m_table);

    m_query = q;
    do_sync();
}

void TableView::remove(size_t row_ndx)
{
    m_table.check();
    REALM_ASSERT(row_ndx < m_key_values.size());

    bool sync_to_keep = m_last_seen_versions == get_dependency_versions();

    ObjKey key = get_key(row_ndx);

    // Update refs
    m_key_values.erase(row_ndx);

    // Delete row in origin table
    get_parent()->remove_object(key);

    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_versions = get_dependency_versions();

    // Adjustment of row indexes greater than the removed index is done by
    // adj_row_acc_move_over or adj_row_acc_erase_row as sideeffect of the actual
    // update of the table, so we don't need to do it here (it has already been done)
}


void TableView::clear()
{
    m_table.check();

    bool sync_to_keep = m_last_seen_versions == get_dependency_versions();

    _impl::TableFriend::batch_erase_rows(*get_parent(), m_key_values); // Throws

    m_key_values.clear();

    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_versions = get_dependency_versions();
}

void ConstTableView::distinct(ColKey column)
{
    distinct(DistinctDescriptor({{column}}));
}

/// Remove rows that are duplicated with respect to the column set passed as argument.
/// Will keep original sorting order so that you can both have a distinct and sorted view.
void ConstTableView::distinct(DistinctDescriptor columns)
{
    m_descriptor_ordering.append_distinct(std::move(columns));
    m_descriptor_ordering.collect_dependencies(m_table.unchecked_ptr());

    do_sync();
}

void ConstTableView::limit(LimitDescriptor lim)
{
    m_descriptor_ordering.append_limit(std::move(lim));
    do_sync();
}

void ConstTableView::apply_descriptor_ordering(const DescriptorOrdering& new_ordering)
{
    m_descriptor_ordering = new_ordering;
    m_descriptor_ordering.collect_dependencies(m_table.unchecked_ptr());

    do_sync();
}

void ConstTableView::include(IncludeDescriptor include_paths)
{
    m_descriptor_ordering.append_include(std::move(include_paths));
    do_sync();
}

IncludeDescriptor ConstTableView::get_include_descriptors()
{
    return m_descriptor_ordering.compile_included_backlinks();
}

std::string ConstTableView::get_descriptor_ordering_description() const
{
    return m_descriptor_ordering.get_description(m_table);
}

// Sort according to one column
void ConstTableView::sort(ColKey column, bool ascending)
{
    sort(SortDescriptor({{column}}, {ascending}));
}

// Sort according to multiple columns, user specified order on each column
void ConstTableView::sort(SortDescriptor order)
{
    m_descriptor_ordering.append_sort(std::move(order), SortDescriptor::MergeMode::prepend);
    m_descriptor_ordering.collect_dependencies(m_table.unchecked_ptr());

    do_sort(m_descriptor_ordering);
}


void ConstTableView::do_sync()
{
    util::CriticalSection cs(m_race_detector);
    // This TableView can be "born" from 4 different sources:
    // - LinkView
    // - Query::find_all()
    // - Table::get_distinct_view()
    // - Table::get_backlink_view()
    // Here we sync with the respective source.
    m_last_seen_versions.clear();

    if (m_linklist_source) {
        m_key_values.clear();
        std::for_each(m_linklist_source->begin(), m_linklist_source->end(), [this](ObjKey key) {
            m_key_values.add(key);
        });
    }
    else if (m_linkset_source) {
        m_key_values.clear();
        std::for_each(m_linkset_source->begin(), m_linkset_source->end(), [this](ObjKey key) {
            m_key_values.add(key);
        });
    }
    else if (m_source_column_key) {
        m_key_values.clear();
        if (m_table && m_linked_table->is_valid(m_linked_obj_key)) {
            const Obj m_linked_obj = m_linked_table->get_object(m_linked_obj_key);
            if (m_table->valid_column(m_source_column_key)) { // return empty result, if column has been removed
                ColKey backlink_col = m_table->get_opposite_column(m_source_column_key);
                REALM_ASSERT(backlink_col);
                m_linked_table->report_invalid_key(backlink_col);
                auto backlinks = m_linked_obj.get_all_backlinks(backlink_col);
                for (auto k : backlinks) {
                    m_key_values.add(k);
                }
            }
        }
    }
    // FIXME: Unimplemented for link to a column
    else {
        m_query.m_table.check();

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

    m_last_seen_versions = get_dependency_versions();
}

void ConstTableView::do_sort(const DescriptorOrdering& ordering)
{
    if (ordering.is_empty())
        return;
    size_t sz = size();
    if (sz == 0)
        return;

    // Gather the current rows into a container we can use std algorithms on
    size_t detached_ref_count = 0;
    BaseDescriptor::IndexPairs index_pairs;
    index_pairs.reserve(sz);
    // always put any detached refs at the end of the sort
    // FIXME: reconsider if this is the right thing to do
    // FIXME: consider specialized implementations in derived classes
    // (handling detached refs is not required in linkviews)
    for (size_t t = 0; t < sz; t++) {
        ObjKey key = get_key(t);
        if (m_table->is_valid(key)) {
            index_pairs.emplace_back(key, t);
        }
        else
            ++detached_ref_count;
    }

    const int num_descriptors = int(ordering.size());
    for (int desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
        const BaseDescriptor* base_descr = ordering[desc_ndx];
        const BaseDescriptor* next = ((desc_ndx + 1) < num_descriptors) ? ordering[desc_ndx + 1] : nullptr;
        BaseDescriptor::Sorter predicate = base_descr->sorter(*m_table, index_pairs);

        // Sorting can be specified by multiple columns, so that if two entries in the first column are
        // identical, then the rows are ordered according to the second column, and so forth. For the
        // first column, we cache all the payload of fields of the view in a std::vector<Mixed>
        predicate.cache_first_column(index_pairs);

        base_descr->execute(index_pairs, predicate, next);
    }
    // Apply the results
    m_limit_count = index_pairs.m_removed_by_limit;
    m_key_values.clear();
    for (auto& pair : index_pairs) {
        m_key_values.add(pair.key_for_object);
    }
    for (size_t t = 0; t < detached_ref_count; ++t)
        m_key_values.add(null_key);
}

bool ConstTableView::is_in_table_order() const
{
    if (!m_table) {
        return false;
    }
    else if (m_linklist_source) {
        return false;
    }
    else if (m_linkset_source) {
        return false;
    }
    else if (m_source_column_key) {
        return false;
    }
    else {
        m_query.m_table.check();
        return m_query.produces_results_in_table_order() && !m_descriptor_ordering.will_apply_sort();
    }
}
