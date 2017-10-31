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

#include <realm/query_engine.hpp>

#include <realm/query_expression.hpp>

using namespace realm;

size_t ParentNode::find_first(size_t start, size_t end)
{
    size_t sz = m_children.size();
    size_t current_cond = 0;
    size_t nb_cond_to_test = sz;

    while (REALM_LIKELY(start < end)) {
        size_t m = m_children[current_cond]->find_first_local(start, end);

        if (m != start) {
            // Pointer advanced - we will have to check all other conditions
            nb_cond_to_test = sz;
            start = m;
        }

        nb_cond_to_test--;

        // Optimized for one condition where this will be true first time
        if (REALM_LIKELY(nb_cond_to_test == 0))
            return m;

        current_cond++;

        if (current_cond == sz)
            current_cond = 0;
    }
    return not_found;
}

bool ParentNode::match(ConstObj& obj)
{
    size_t sz = m_children.size();

    while (sz--) {
        if (!m_children[sz]->match_local(obj))
            return false;
    }
    return true;
}

void ParentNode::aggregate_local_prepare(Action TAction, DataType col_id, bool nullable)
{
    if (TAction == act_ReturnFirst) {
        if (nullable)
            m_column_action_specializer = &ThisType::column_action_specialization<act_ReturnFirst, IntNullColumn>;
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_ReturnFirst, IntegerColumn>;
    }
    else if (TAction == act_Count) {
        // For count(), the column below is a dummy and the caller sets it to nullptr. Hence, no data is being read
        // from any column upon each query match (just matchcount++ is performed), and we pass nullable = false simply
        // by convention. FIXME: Clean up all this.
        if (nullable)
            REALM_ASSERT(false);
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_Count, IntegerColumn>;
    }
    else if (TAction == act_Sum && col_id == type_Int) {
        if (nullable)
            m_column_action_specializer = &ThisType::column_action_specialization<act_Sum, IntNullColumn>;
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_Sum, IntegerColumn>;
    }
    else if (TAction == act_Sum && col_id == type_Float) {
        m_column_action_specializer = &ThisType::column_action_specialization<act_Sum, FloatColumn>;
    }
    else if (TAction == act_Sum && col_id == type_Double) {
        m_column_action_specializer = &ThisType::column_action_specialization<act_Sum, DoubleColumn>;
    }
    else if (TAction == act_Max && col_id == type_Int) {
        if (nullable)
            m_column_action_specializer = &ThisType::column_action_specialization<act_Max, IntNullColumn>;
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_Max, IntegerColumn>;
    }
    else if (TAction == act_Max && col_id == type_Float) {
        m_column_action_specializer = &ThisType::column_action_specialization<act_Max, FloatColumn>;
    }
    else if (TAction == act_Max && col_id == type_Double) {
        m_column_action_specializer = &ThisType::column_action_specialization<act_Max, DoubleColumn>;
    }
    else if (TAction == act_Min && col_id == type_Int) {
        if (nullable)
            m_column_action_specializer = &ThisType::column_action_specialization<act_Min, IntNullColumn>;
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_Min, IntegerColumn>;
    }
    else if (TAction == act_Min && col_id == type_Float) {
        m_column_action_specializer = &ThisType::column_action_specialization<act_Min, FloatColumn>;
    }
    else if (TAction == act_Min && col_id == type_Double) {
        m_column_action_specializer = &ThisType::column_action_specialization<act_Min, DoubleColumn>;
    }
    else if (TAction == act_FindAll) {
        // For find_all(), the column below is a dummy and the caller sets it to nullptr. Hence, no data is being read
        // from any column upon each query match (just a TableView.add(size_t index) is performed), and we pass
        // nullable = false simply by convention. FIXME: Clean up all this.
        if (nullable)
            REALM_ASSERT(false);
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_FindAll, IntegerColumn>;
    }
    else if (TAction == act_CallbackIdx) {
        // Future features where for each query match, you want to perform an action that only requires knowlege
        // about the row index, and not the payload there. Examples could be find_all(), however, this code path
        // below is for new features given in a callback method and not yet supported by core.
        if (nullable)
            m_column_action_specializer = &ThisType::column_action_specialization<act_CallbackIdx, IntNullColumn>;
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_CallbackIdx, IntegerColumn>;
    }
    else {
        REALM_ASSERT(false);
    }
}

size_t ParentNode::aggregate_local(QueryStateBase* st, size_t start, size_t end, size_t local_limit,
                                   SequentialGetterBase* source_column)
{
    // aggregate called on non-integer column type. Speed of this function is not as critical as speed of the
    // integer version, because find_first_local() is relatively slower here (because it's non-integers).
    //
    // Todo: Two speedups are possible. Simple: Initially test if there are no sub criterias and run
    // find_first_local()
    // in a tight loop if so (instead of testing if there are sub criterias after each match). Harder: Specialize
    // data type array to make array call match() directly on each match, like for integers.

    size_t local_matches = 0;

    size_t r = start - 1;
    for (;;) {
        if (local_matches == local_limit) {
            m_dD = double(r - start) / (local_matches + 1.1);
            return r + 1;
        }

        // Find first match in this condition node
        r = find_first_local(r + 1, end);
        if (r == not_found) {
            m_dD = double(r - start) / (local_matches + 1.1);
            return end;
        }

        local_matches++;

        // Find first match in remaining condition nodes
        size_t m = r;

        for (size_t c = 1; c < m_children.size(); c++) {
            m = m_children[c]->find_first_local(r, r + 1);
            if (m != r) {
                break;
            }
        }

        // If index of first match in this node equals index of first match in all remaining nodes, we have a final
        // match
        if (m == r) {
            bool cont = (this->*m_column_action_specializer)(st, source_column, r);
            if (!cont) {
                return static_cast<size_t>(-1);
            }
        }
    }
}

void StringNodeEqualBase::deallocate() noexcept
{
    // Must be called after each query execution to free temporary resources used by the execution. Run in
    // destructor, but also in Init because a user could define a query once and execute it multiple times.
    clear_leaf_state();

    if (m_index_matches_destroy)
        m_index_matches->destroy();

    m_index_matches_destroy = false;
    m_index_matches.reset();
    m_index_getter.reset();
}

void StringNodeEqualBase::init()
{
    deallocate();
    m_dD = 10.0;
    StringNodeBase::init();

    if (m_column_type == col_type_StringEnum) {
        m_dT = 1.0;
    }
    else if (m_has_search_index) {
        m_dT = 0.0;
    }
    else {
        m_dT = 10.0;
    }

    if (m_has_search_index) {
        m_index_matches_destroy = false;
        m_last_start = size_t(-1);

        // Will set m_index_matches, m_index_matches_destroy, m_results_start and m_results_end
        _search_index_init();

        if (m_index_matches) {
            m_index_getter.reset(new SequentialGetter<IntegerColumn>(m_index_matches.get()));
        }
    }
}

size_t StringNodeEqualBase::find_first_local(size_t start, size_t end)
{
    REALM_ASSERT(m_table);

    if (m_has_search_index) {
        // Indexed string column
        if (!m_index_getter)
            return not_found; // no matches in the index

        if (m_last_start > start)
            m_last_indexed = m_results_start;
        m_last_start = start;

        while (m_last_indexed < m_results_end) {
            m_index_getter->cache_next(m_last_indexed);
            size_t f = m_index_getter->m_leaf_ptr->find_gte(start, m_last_indexed - m_index_getter->m_leaf_start,
                                                            m_results_end - m_index_getter->m_leaf_start);

            if (f == not_found) {
                // Not found in this leaf - move on to next
                m_last_indexed = m_index_getter->m_leaf_end;
            }
            else if (f >= (m_results_end - m_index_getter->m_leaf_start)) {
                // Found outside valid range
                return not_found;
            }
            else {
                size_t found_index = to_size_t(m_index_getter->m_leaf_ptr->get(f));
                if (found_index >= end)
                    return not_found;
                else {
                    m_last_indexed = f + m_index_getter->m_leaf_start;
                    return found_index;
                }
            }
        }
        return not_found;
    }

    return _find_first_local(start, end);
}

namespace realm {

void StringNode<Equal>::_search_index_init()
{
// TODO: fix when search indexes are supported in clusters
#if 0
    FindRes fr;
    InternalFindResult res;

    if (m_column_type == col_type_StringEnum) {
        fr = static_cast<const StringEnumColumn*>(m_condition_column)->find_all_no_copy(m_value, res);
    }
    else {
        fr = static_cast<const StringColumn*>(m_condition_column)->find_all_no_copy(m_value, res);
    }

    switch (fr) {
        case FindRes_single:
            m_index_matches.reset(
                new IntegerColumn(IntegerColumn::unattached_root_tag(), Allocator::get_default())); // Throws
            m_index_matches->get_root_array()->create(Array::type_Normal);                          // Throws
            m_index_matches->add(res.payload);
            m_index_matches_destroy = true; // we own m_index_matches, so we must destroy it
            m_results_start = 0;
            m_results_end = 1;
            break;
        case FindRes_column:
            // todo: Apparently we can't use m_index.get_alloc() because it uses default allocator which
            // simply makes
            // translate(x) = x. Shouldn't it inherit owner column's allocator?!
            m_index_matches.reset(new IntegerColumn(m_condition_column->get_alloc(), res.payload)); // Throws
            m_results_start = res.start_ndx;
            m_results_end = res.end_ndx;

            // FIXME: handle start and end of find_result!
            break;
        case FindRes_not_found:
            m_index_matches.reset();
            m_index_getter.reset();
            break;
    }
#endif
}

size_t StringNode<Equal>::_find_first_local(size_t start, size_t end)
{
    return m_leaf_ptr->find_first(m_value, start, end);
}

void StringNode<EqualIns>::_search_index_init()
{
// TODO: fix when search indexes are supported in clusters
#if 0
    if (m_column_type == col_type_StringEnum) {
        REALM_ASSERT_RELEASE(false && "Case insensitive searches in StringEnum columns is not yet implemented.");
        // FindRes fr;
        InternalFindResult res;
        static_cast<const StringEnumColumn*>(m_condition_column)->find_all_no_copy(m_value, res);
    }
    else {
        m_index_matches.reset(
            new IntegerColumn(IntegerColumn::unattached_root_tag(), Allocator::get_default())); // Throws
        m_index_matches->get_root_array()->create(Array::type_Normal);                          // Throws
        // m_index_matches->add(res.payload);
        StringData needle(m_value);
        m_condition_column->get_search_index()->find_all(*m_index_matches, needle, true);
    }

    m_index_matches_destroy = true; // we own m_index_matches, so we must destroy it
    m_results_start = 0;
    m_results_end = m_index_matches->size();
#endif
}

size_t StringNode<EqualIns>::_find_first_local(size_t start, size_t end)
{
    EqualIns cond;
    for (size_t s = start; s < end; ++s) {
        StringData t = get_string(s);

        if (cond(StringData(m_value), m_ucase.data(), m_lcase.data(), t))
            return s;
    }

    return not_found;
}

} // namespace realm

size_t NotNode::find_first_local(size_t start, size_t end)
{
    if (start <= m_known_range_start && end >= m_known_range_end) {
        return find_first_covers_known(start, end);
    }
    else if (start >= m_known_range_start && end <= m_known_range_end) {
        return find_first_covered_by_known(start, end);
    }
    else if (start < m_known_range_start && end >= m_known_range_start) {
        return find_first_overlap_lower(start, end);
    }
    else if (start <= m_known_range_end && end > m_known_range_end) {
        return find_first_overlap_upper(start, end);
    }
    else { // start > m_known_range_end || end < m_known_range_start
        return find_first_no_overlap(start, end);
    }
}

bool NotNode::evaluate_at(size_t rowndx)
{
    return m_condition->find_first(rowndx, rowndx + 1) == not_found;
}

void NotNode::update_known(size_t start, size_t end, size_t first)
{
    m_known_range_start = start;
    m_known_range_end = end;
    m_first_in_known_range = first;
}

size_t NotNode::find_first_loop(size_t start, size_t end)
{
    for (size_t i = start; i < end; ++i) {
        if (evaluate_at(i)) {
            return i;
        }
    }
    return not_found;
}

size_t NotNode::find_first_covers_known(size_t start, size_t end)
{
    // CASE: start-end covers the known range
    // [    ######    ]
    REALM_ASSERT_DEBUG(start <= m_known_range_start && end >= m_known_range_end);
    size_t result = find_first_loop(start, m_known_range_start);
    if (result != not_found) {
        update_known(start, m_known_range_end, result);
    }
    else {
        if (m_first_in_known_range != not_found) {
            update_known(start, m_known_range_end, m_first_in_known_range);
            result = m_first_in_known_range;
        }
        else {
            result = find_first_loop(m_known_range_end, end);
            update_known(start, end, result);
        }
    }
    return result;
}

size_t NotNode::find_first_covered_by_known(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG(start >= m_known_range_start && end <= m_known_range_end);
    // CASE: the known range covers start-end
    // ###[#####]###
    if (m_first_in_known_range != not_found) {
        if (m_first_in_known_range > end) {
            return not_found;
        }
        else if (m_first_in_known_range >= start) {
            return m_first_in_known_range;
        }
    }
    // The first known match is before start, so we can't use the results to improve
    // heuristics.
    return find_first_loop(start, end);
}

size_t NotNode::find_first_overlap_lower(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG(start < m_known_range_start && end >= m_known_range_start && end <= m_known_range_end);
    static_cast<void>(end);
    // CASE: partial overlap, lower end
    // [   ###]#####
    size_t result;
    result = find_first_loop(start, m_known_range_start);
    if (result == not_found) {
        result = m_first_in_known_range;
    }
    update_known(start, m_known_range_end, result);
    return result;
}

size_t NotNode::find_first_overlap_upper(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG(start <= m_known_range_end && start >= m_known_range_start && end > m_known_range_end);
    // CASE: partial overlap, upper end
    // ####[###    ]
    size_t result;
    if (m_first_in_known_range != not_found) {
        if (m_first_in_known_range >= start) {
            result = m_first_in_known_range;
            update_known(m_known_range_start, end, result);
        }
        else {
            result = find_first_loop(start, end);
            update_known(m_known_range_start, end, m_first_in_known_range);
        }
    }
    else {
        result = find_first_loop(m_known_range_end, end);
        update_known(m_known_range_start, end, result);
    }
    return result;
}

size_t NotNode::find_first_no_overlap(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG((start < m_known_range_start && end < m_known_range_start) ||
                       (start > m_known_range_end && end > m_known_range_end));
    // CASE: no overlap
    // ### [    ]   or    [    ] ####
    // if input is a larger range, discard and replace with results.
    size_t result = find_first_loop(start, end);
    if (end - start > m_known_range_end - m_known_range_start) {
        update_known(start, end, result);
    }
    return result;
}

ExpressionNode::ExpressionNode(std::unique_ptr<Expression> expression)
: m_expression(std::move(expression))
{
    m_dD = 10.0;
    m_dT = 50.0;
}

void ExpressionNode::table_changed()
{
    m_expression->set_base_table(m_table.get());
}

void ExpressionNode::cluster_changed()
{
    m_expression->set_cluster(m_cluster);
}

std::string ExpressionNode::describe() const
{
    if (m_expression) {
        return m_expression->description();
    }
    else {
        return "empty expression";
    }
}

void ExpressionNode::verify_column() const
{
    m_expression->verify_column();
}

size_t ExpressionNode::find_first_local(size_t start, size_t end)
{
    return m_expression->find_first(start, end);
}

std::unique_ptr<ParentNode> ExpressionNode::clone(QueryNodeHandoverPatches* patches) const
{
    return std::unique_ptr<ParentNode>(new ExpressionNode(*this, patches));
}

void ExpressionNode::apply_handover_patch(QueryNodeHandoverPatches& patches, Group& group)
{
    m_expression->apply_handover_patch(patches, group);
    ParentNode::apply_handover_patch(patches, group);
}

ExpressionNode::ExpressionNode(const ExpressionNode& from, QueryNodeHandoverPatches* patches)
: ParentNode(from, patches)
, m_expression(from.m_expression->clone(patches))
{
}
