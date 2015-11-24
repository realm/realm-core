#include <realm/query_engine.hpp>

namespace realm {
namespace _impl {

const double CostHeuristic<IntegerColumn>::dD = 100.0;
const double CostHeuristic<IntegerColumn>::dT = 1.0 / 4.0;
const double CostHeuristic<IntNullColumn>::dD = 100.0;
const double CostHeuristic<IntNullColumn>::dT = 1.0 / 4.0;

}
}

using namespace realm;

size_t ParentNode::find_first(size_t start, size_t end)
{
    size_t next_cond = 0;
    size_t first_cond = 0;

    while (start < end) {
        size_t m = m_children[next_cond]->find_first_local(start, end);

        next_cond++;
        if (next_cond == m_children.size())
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
    return not_found;
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
        if (nullable)
            m_column_action_specializer = &ThisType::column_action_specialization<act_Count, IntNullColumn>;
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
        if (nullable)
            m_column_action_specializer = &ThisType::column_action_specialization<act_FindAll, IntNullColumn>;
        else
            m_column_action_specializer = &ThisType::column_action_specialization<act_FindAll, IntegerColumn>;
    }
    else if (TAction == act_CallbackIdx) {
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
    // Todo: Two speedups are possible. Simple: Initially test if there are no sub criterias and run find_first_local()
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

        // If index of first match in this node equals index of first match in all remaining nodes, we have a final match
        if (m == r) {
            bool cont = (this->* m_column_action_specializer)(st, source_column, r);
            if (!cont) {
                return static_cast<size_t>(-1);
            }
        }
    }
}

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
    return m_condition->find_first(rowndx, rowndx+1) == not_found;
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
    REALM_ASSERT_DEBUG((start < m_known_range_start && end < m_known_range_start) || (start > m_known_range_end && end > m_known_range_end));
    // CASE: no overlap
    // ### [    ]   or    [    ] ####
    // if input is a larger range, discard and replace with results.
    size_t result = find_first_loop(start, end);
    if (end - start > m_known_range_end - m_known_range_start) {
        update_known(start, end, result);
    }
    return result;
}
