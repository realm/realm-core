#include <cstdio>
#include <algorithm>

#include <realm/array.hpp>
#include <realm/column_basic.hpp>
#include <realm/column_fwd.hpp>
#include <realm/query.hpp>
#include <realm/query_engine.hpp>
#include <realm/descriptor.hpp>
#include <realm/table_view.hpp>
#include <realm/link_view.hpp>

using namespace realm;

Query::Query() : m_view(nullptr), m_source_table_view(nullptr), m_owns_source_table_view(false)
{
    create();
}

Query::Query(Table& table, TableViewBase* tv) 
    : m_table(table.get_table_ref()), m_view(tv), m_source_table_view(tv), m_owns_source_table_view(false)
{
    REALM_ASSERT_DEBUG(m_view == nullptr || m_view->cookie == m_view->cookie_expected);
    create();
}

Query::Query(const Table& table, const LinkViewRef& lv):
    m_table((const_cast<Table&>(table)).get_table_ref()),
    m_view(lv.get()),
    m_source_link_view(lv), m_source_table_view(nullptr), m_owns_source_table_view(false)
{
    REALM_ASSERT_DEBUG(m_view == nullptr || m_view->cookie == m_view->cookie_expected);
    create();
}

Query::Query(const Table& table, TableViewBase* tv) 
    : m_table((const_cast<Table&>(table)).get_table_ref()), m_view(tv), m_source_table_view(tv), m_owns_source_table_view(false)
{
    REALM_ASSERT_DEBUG(m_view == nullptr ||m_view->cookie == m_view->cookie_expected);
    create();
}

Query::Query(const Table& table, std::unique_ptr<TableViewBase> tv)
    : m_table((const_cast<Table&>(table)).get_table_ref()), m_view(tv.get()), m_source_table_view(tv.get()), m_owns_source_table_view(true)
{
    tv.release();

    REALM_ASSERT_DEBUG(m_view == nullptr ||m_view->cookie == m_view->cookie_expected);
    create();
}

void Query::create()
{
    m_groups.emplace_back();
    if (m_table)
        fetch_descriptor();
}

Query::Query(const Query& copy)
{
    m_table = copy.m_table;
    m_groups = copy.m_groups;
    error_code = copy.error_code;
    m_view = copy.m_view;
    m_source_link_view = copy.m_source_link_view;
    m_source_table_view = copy.m_source_table_view;
    m_owns_source_table_view = false;
    m_current_descriptor = copy.m_current_descriptor;
}

Query& Query::operator = (const Query& source)
{
    if (this != &source) {
        m_groups = source.m_groups;
        m_table = source.m_table;
        m_view = source.m_view;
        m_source_link_view = source.m_source_link_view;
        m_source_table_view = source.m_source_table_view;

        if (m_table)
            fetch_descriptor();
    }
    return *this;
}

Query::Query(Query&&) = default;
Query& Query::operator=(Query&&) = default;

Query::~Query() noexcept
{
    if (m_owns_source_table_view)
        delete m_source_table_view;
}

Query::Query(Query& source, Handover_patch& patch, MutableSourcePayload mode)
    : m_table(TableRef()), m_source_link_view(LinkViewRef()), m_source_table_view(nullptr)
{
    patch.m_has_table = bool(source.m_table);
    if (patch.m_has_table) {
        patch.m_table_num = source.m_table.get()->get_index_in_group();
    }
    if (source.m_source_table_view) {
        m_source_table_view = 
            source.m_source_table_view->clone_for_handover(patch.table_view_data, mode).release();
        m_owns_source_table_view = true;
    }
    else { 
        patch.table_view_data = nullptr;
        m_owns_source_table_view = false;
    }
    LinkView::generate_patch(source.m_source_link_view, patch.link_view_data);
    m_view = m_source_link_view.get();

    m_groups = source.m_groups;
}

Query::Query(const Query& source, Handover_patch& patch, ConstSourcePayload mode)
    : m_table(TableRef()), m_source_link_view(LinkViewRef()), m_source_table_view(nullptr)
{
    patch.m_has_table = bool(source.m_table);
    if (patch.m_has_table) {
        patch.m_table_num = source.m_table.get()->get_index_in_group();
    }
    if (source.m_source_table_view) {
        m_source_table_view = 
            source.m_source_table_view->clone_for_handover(patch.table_view_data, mode).release();
        m_owns_source_table_view = true;
    }
    else {
        patch.table_view_data = nullptr;
        m_owns_source_table_view = false;
    }
    LinkView::generate_patch(source.m_source_link_view, patch.link_view_data);
    m_view = m_source_link_view.get();

    m_groups = source.m_groups;
}

Query::Query(Expression* expr) : Query()
{
    add_expression_node(expr);
    if (auto table = const_cast<Table*>(expr->get_table()))
        set_table(table->get_table_ref());
}

void Query::set_table(TableRef tr)
{
    if (tr == m_table) {
        return;
    }

    m_table = tr;
    if (m_table) {
        fetch_descriptor();
    }
    else {
        m_current_descriptor.reset(nullptr);
    }
}


void Query::apply_patch(Handover_patch& patch, Group& group)
{
    if (m_source_table_view) {
        m_source_table_view->apply_and_consume_patch(patch.table_view_data, group);
    }
    m_source_link_view = LinkView::create_from_and_consume_patch(patch.link_view_data, group);
    m_view = m_source_link_view.get();
    if (patch.m_has_table) {
        set_table(group.get_table(patch.m_table_num));
    }
}

void Query::add_expression_node(Expression* compare)
{
    add_node(std::unique_ptr<ParentNode>(new ExpressionNode(compare)));
}

// Binary
Query& Query::equal(size_t column_ndx, BinaryData b)
{
    add_condition<Equal>(column_ndx, b);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, BinaryData b)
{
    add_condition<NotEqual>(column_ndx, b);
    return *this;
}
Query& Query::begins_with(size_t column_ndx, BinaryData b)
{
    add_condition<BeginsWith>(column_ndx, b);
    return *this;
}
Query& Query::ends_with(size_t column_ndx, BinaryData b)
{
    add_condition<EndsWith>(column_ndx, b);
    return *this;
}
Query& Query::contains(size_t column_ndx, BinaryData b)
{
    add_condition<Contains>(column_ndx, b);
    return *this;
}


namespace {

struct MakeConditionNode {
    // make() for Node creates a Node* with either a value type
    // or null.
    //
    // Note that some Realm types (such as Integer) has both a nullable and a non-nullable version 
    // of query nodes, while other Realm types has just a single version that can handle both nullable
    // and non-nullable columns. The special_null_node must reflect that.
    // Regardless of nullability, it throws a LogicError if trying to query for a value of type T on 
    // a column of a different type.

    template <class Node>
    static typename std::enable_if<Node::special_null_node, std::unique_ptr<ParentNode>>::type
    make(size_t col_ndx, typename Node::TConditionValue value)
    {
        return std::unique_ptr<ParentNode>(new Node(std::move(value), col_ndx));
    }

    template <class Node, class T>
    static typename std::enable_if<
        Node::special_null_node
        && !std::is_same<T, typename Node::TConditionValue>::value
        && !std::is_same<T, null>::value
        , std::unique_ptr<ParentNode>>::type
    make(size_t, T)
    {
        throw LogicError{LogicError::type_mismatch};
    }

    template <class Node, class T>
    static typename std::enable_if<
        !Node::special_null_node
        && std::is_same<T, null>::value
        , std::unique_ptr<ParentNode>>::type
    make(size_t col_ndx, T value)
    {
        // value is null
        return std::unique_ptr<ParentNode>(new Node(value, col_ndx));
    }

    template <class Node, class T>
    static typename std::enable_if<
        !Node::special_null_node
        && std::is_same<T, typename Node::TConditionValue>::value
        , std::unique_ptr<ParentNode>>::type
    make(size_t col_ndx, T value)
    {
        return std::unique_ptr<ParentNode>(new Node(value, col_ndx));
    }

    template <class Node, class T>
    static typename std::enable_if<
        !Node::special_null_node
        && !std::is_same<T, null>::value
        && !std::is_same<T, typename Node::TConditionValue>::value
        , std::unique_ptr<ParentNode>>::type
    make(size_t, T)
    {
        throw LogicError{LogicError::type_mismatch};
    }
};

template <class Cond, class T>
std::unique_ptr<ParentNode> make_condition_node(const Descriptor& descriptor, size_t column_ndx, T value)
{
    DataType type = descriptor.get_column_type(column_ndx);
    bool is_nullable = descriptor.is_nullable(column_ndx);
    switch (type) {
        case type_Int:
        case type_Bool:
        case type_DateTime: {
            if (is_nullable) {
                return MakeConditionNode::make<IntegerNode<IntNullColumn, Cond>>(column_ndx, value);
            }
            else {
                return MakeConditionNode::make<IntegerNode<IntegerColumn, Cond>>(column_ndx, value);
            }
        }
        case type_Float: {
            return MakeConditionNode::make<FloatDoubleNode<FloatColumn, Cond>>(column_ndx, value);
        }
        case type_Double: {
            return MakeConditionNode::make<FloatDoubleNode<DoubleColumn, Cond>>(column_ndx, value);
        }
        case type_String: {
            return MakeConditionNode::make<StringNode<Cond>>(column_ndx, value);
        }
        case type_Binary: {
            return MakeConditionNode::make<BinaryNode<Cond>>(column_ndx, value);
        }
        default: {
            throw LogicError{LogicError::type_mismatch};
        }
    }
}

} // anonymous namespace

void Query::fetch_descriptor()
{
    ConstDescriptorRef desc = m_table->get_descriptor();
    for (size_t i = 0; i < m_subtable_path.size(); ++i) {
        desc = desc->get_subdescriptor(m_subtable_path[i]);
    }
    m_current_descriptor = desc;
}


template <typename TConditionFunction, class T>
Query& Query::add_condition(size_t column_ndx, T value)
{
    REALM_ASSERT_DEBUG(m_current_descriptor);
    auto node = make_condition_node<TConditionFunction>(*m_current_descriptor, column_ndx, value);
    add_node(std::move(node));
    return *this;
}


template <class ColumnType> Query& Query::equal(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Equal>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}

// Two column methods, any type
template <class ColumnType> Query& Query::less(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Less>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType> Query& Query::less_equal(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, LessEqual>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType> Query& Query::greater(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Greater>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType> Query& Query::greater_equal(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, GreaterEqual>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType> Query& Query::not_equal(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, NotEqual>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}

// column vs column, integer
Query& Query::equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return equal<IntegerColumn>(column_ndx1, column_ndx2);
}

Query& Query::not_equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return not_equal<IntegerColumn>(column_ndx1, column_ndx2);
}

Query& Query::less_int(size_t column_ndx1, size_t column_ndx2)
{
    return less<IntegerColumn>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<IntegerColumn>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_int(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<IntegerColumn>(column_ndx1, column_ndx2);
}

Query& Query::greater_int(size_t column_ndx1, size_t column_ndx2)
{
    return greater<IntegerColumn>(column_ndx1, column_ndx2);
}


// column vs column, float
Query& Query::not_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return not_equal<BasicColumn<float>>(column_ndx1, column_ndx2);
}

Query& Query::less_float(size_t column_ndx1, size_t column_ndx2)
{
    return less<BasicColumn<float>>(column_ndx1, column_ndx2);
}

Query& Query::greater_float(size_t column_ndx1, size_t column_ndx2)
{
    return greater<BasicColumn<float>>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<BasicColumn<float>>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<BasicColumn<float>>(column_ndx1, column_ndx2);
}

Query& Query::equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return equal<BasicColumn<float>>(column_ndx1, column_ndx2);
}

// column vs column, double
Query& Query::equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return equal<BasicColumn<double>>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<BasicColumn<double>>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<BasicColumn<double>>(column_ndx1, column_ndx2);
}
Query& Query::greater_double(size_t column_ndx1, size_t column_ndx2)
{
    return greater<BasicColumn<double>>(column_ndx1, column_ndx2);
}
Query& Query::less_double(size_t column_ndx1, size_t column_ndx2)
{
    return less<BasicColumn<double>>(column_ndx1, column_ndx2);
}

Query& Query::not_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return not_equal<BasicColumn<double>>(column_ndx1, column_ndx2);
}

// null vs column
Query& Query::equal(size_t column_ndx, null)
{
    add_condition<Equal>(column_ndx, null{});
    return *this;
}

Query& Query::not_equal(size_t column_ndx, null)
{
    add_condition<NotEqual>(column_ndx, null());
    return *this;
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

Query& Query::links_to(size_t origin_column, size_t target_row)
{
    add_node(std::unique_ptr<ParentNode>(new LinksToNode(origin_column, target_row)));
    return *this;
}

// int64 constant vs column
Query& Query::equal(size_t column_ndx, int64_t value)
{
    add_condition<Equal>(column_ndx, value);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, int64_t value)
{
    add_condition<NotEqual>(column_ndx, value);
    return *this;
}
Query& Query::greater(size_t column_ndx, int64_t value)
{
    add_condition<Greater>(column_ndx, value);
    return *this;
}
Query& Query::greater_equal(size_t column_ndx, int64_t value)
{
    if (value > LLONG_MIN) {
        add_condition<Greater>(column_ndx, value - 1);
    }
    // field >= LLONG_MIN has no effect
    return *this;
}
Query& Query::less_equal(size_t column_ndx, int64_t value)
{
    if (value < LLONG_MAX) {
        add_condition<Less>(column_ndx, value + 1);
    }
    // field <= LLONG_MAX has no effect
    return *this;
}
Query& Query::less(size_t column_ndx, int64_t value)
{
    add_condition<Less>(column_ndx, value);
    return *this;
}
Query& Query::between(size_t column_ndx, int64_t from, int64_t to)
{
    group();
    greater_equal(column_ndx, from);
    less_equal(column_ndx, to);
    end_group();
    return *this;
}
Query& Query::equal(size_t column_ndx, bool value)
{
    add_condition<Equal>(column_ndx, int64_t(value));
    return *this;
}

// ------------- float
Query& Query::equal(size_t column_ndx, float value)
{
    return add_condition<Equal>(column_ndx, value);
}
Query& Query::not_equal(size_t column_ndx, float value)
{
    return add_condition<NotEqual>(column_ndx, value);
}
Query& Query::greater(size_t column_ndx, float value)
{
    return add_condition<Greater>(column_ndx, value);
}
Query& Query::greater_equal(size_t column_ndx, float value)
{
    return add_condition<GreaterEqual>(column_ndx, value);
}
Query& Query::less_equal(size_t column_ndx, float value)
{
    return add_condition<LessEqual>(column_ndx, value);
}
Query& Query::less(size_t column_ndx, float value)
{
    return add_condition<Less>(column_ndx, value);
}
Query& Query::between(size_t column_ndx, float from, float to)
{
    group();
    greater_equal(column_ndx, from);
    less_equal(column_ndx, to);
    end_group();
    return *this;
}


// ------------- double
Query& Query::equal(size_t column_ndx, double value)
{
    return add_condition<Equal>(column_ndx, value);
}
Query& Query::not_equal(size_t column_ndx, double value)
{
    return add_condition<NotEqual>(column_ndx, value);
}
Query& Query::greater(size_t column_ndx, double value)
{
    return add_condition<Greater>(column_ndx, value);
}
Query& Query::greater_equal(size_t column_ndx, double value)
{
    return add_condition<GreaterEqual>(column_ndx, value);
}
Query& Query::less_equal(size_t column_ndx, double value)
{
    return add_condition<LessEqual>(column_ndx, value);
}
Query& Query::less(size_t column_ndx, double value)
{
    return add_condition<Less>(column_ndx, value);
}
Query& Query::between(size_t column_ndx, double from, double to)
{
    group();
    greater_equal(column_ndx, from);
    less_equal(column_ndx, to);
    end_group();
    return *this;
}


// Strings, StringData()

Query& Query::equal(size_t column_ndx, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Equal>(column_ndx, value);
    else
        add_condition<EqualIns>(column_ndx, value);
    return *this;
}
Query& Query::begins_with(size_t column_ndx, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<BeginsWith>(column_ndx, value);
    else
        add_condition<BeginsWithIns>(column_ndx, value);
    return *this;
}
Query& Query::ends_with(size_t column_ndx, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<EndsWith>(column_ndx, value);
    else
        add_condition<EndsWithIns>(column_ndx, value);
    return *this;
}
Query& Query::contains(size_t column_ndx, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Contains>(column_ndx, value);
    else
        add_condition<ContainsIns>(column_ndx, value);
    return *this;
}
Query& Query::not_equal(size_t column_ndx, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<NotEqual>(column_ndx, value);
    else
        add_condition<NotEqualIns>(column_ndx, value);
    return *this;
}


// Aggregates =================================================================================

size_t Query::peek_tableview(size_t tv_index) const
{
    REALM_ASSERT(m_view);
    REALM_ASSERT_DEBUG(m_view->cookie == m_view->cookie_expected);
    REALM_ASSERT_3(tv_index, <, m_view->size());

    // Cannot use to_size_t() because the get() may return -1
    size_t tablerow = static_cast<size_t>(m_view->m_row_indexes.get(tv_index));

    if (has_conditions())
        return root_node()->find_first(tablerow, tablerow + 1);

    return tablerow;
}

template <Action action, typename T, typename R, class ColType>
    R Query::aggregate(R(ColType::*aggregateMethod)(size_t start, size_t end, size_t limit,
                                                    size_t* return_ndx) const,
                       size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit, 
                       size_t* return_ndx) const
{
    if(limit == 0 || m_table->is_degenerate()) {
        if (resultcount)
            *resultcount = 0;
        return static_cast<R>(0);
    }

    if (end == size_t(-1))
        end = m_view ? m_view->size() : m_table->size();

    const ColType& column =
        m_table->get_column<ColType, ColumnType(ColumnTypeTraits<T, ColType::nullable>::id)>(column_ndx);

    if (!has_conditions() && !m_view) {
        // No criteria, so call aggregate METHODS directly on columns
        // - this bypasses the query system and is faster
        // User created query with no criteria; aggregate range
        if (resultcount) {
            *resultcount = limit < (end - start) ? limit : (end - start);
        }
        // direct aggregate on the column
        return (column.*aggregateMethod)(start, end, limit, return_ndx);
    }
    else {

        // Aggregate with criteria - goes through the nodes in the query system
        init(*m_table);
        QueryState<R> st;
        st.init(action, nullptr, limit);

        SequentialGetter<ColType> source_column(*m_table, column_ndx);

        if (!m_view) {
            aggregate_internal(action, ColumnTypeTraits<T, ColType::nullable>::id, ColType::nullable, root_node(), &st, start, end, &source_column);
        }
        else {
            for (size_t t = start; t < end && st.m_match_count < limit; t++) {
                size_t r = peek_tableview(t);
                if (r != not_found)
                    st.template match<action, false>(r, 0, source_column.get_next(m_view->m_row_indexes.get(t)));
            }
        }

        if (resultcount) {
            *resultcount = st.m_match_count;
        }

        if (return_ndx) {
            *return_ndx = st.m_minmax_index;
        }

        return st.m_state;
    }
}

    /**************************************************************************************************************
    *                                                                                                             *
    * Main entry point of a query. Schedules calls to aggregate_local                                             *
    * Return value is the result of the query, or Array pointer for FindAll.                                      *
    *                                                                                                             *
    **************************************************************************************************************/

    void Query::aggregate_internal(Action TAction, DataType TSourceColumn, bool nullable,
                                   ParentNode* pn, QueryStateBase* st,
                                   size_t start, size_t end, SequentialGetterBase* source_column) const
    {
        if (end == not_found)
            end = m_table->size();

        for (size_t c = 0; c < pn->m_children.size(); c++)
            pn->m_children[c]->aggregate_local_prepare(TAction, TSourceColumn, nullable);

        size_t td;

        while (start < end) {
            size_t best = std::distance(pn->m_children.begin(),
                                        std::min_element(pn->m_children.begin(), pn->m_children.end(),
                                                         ParentNode::score_compare()));

            // Find a large amount of local matches in best condition
            td = pn->m_children[best]->m_dT == 0.0 ? end : (start + 1000 > end ? end : start + 1000);

            // Executes start...end range of a query and will stay inside the condition loop of the node it was called
            // on. Can be called on any node; yields same result, but different performance. Returns prematurely if
            // condition of called node has evaluated to true local_matches number of times.
            // Return value is the next row for resuming aggregating (next row that caller must call aggregate_local on)
            start = pn->m_children[best]->aggregate_local(st, start, td, findlocals, source_column);

            // Make remaining conditions compute their m_dD (statistics)
            for (size_t c = 0; c < pn->m_children.size() && start < end; c++) {
                if (c == best)
                    continue;

                // Skip test if there is no way its cost can ever be better than best node's
                double cost = pn->m_children[c]->cost();
                if (pn->m_children[c]->m_dT < cost) {

                    // Limit to bestdist in order not to skip too large parts of index nodes
                    size_t maxD = pn->m_children[c]->m_dT == 0.0 ? end - start : bestdist;
                    td = pn->m_children[c]->m_dT == 0.0 ? end : (start + maxD > end ? end : start + maxD);
                    start = pn->m_children[c]->aggregate_local(st, start, td, probe_matches, source_column);
                }
            }
        }
    }


// Sum

int64_t Query::sum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Sum, int64_t>(&IntNullColumn::sum, column_ndx, resultcount, start, end, limit);
    }
    return aggregate<act_Sum, int64_t>(&IntegerColumn::sum, column_ndx, resultcount, start, end, limit);
    return aggregate<act_Sum, int64_t>(&IntegerColumn::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Sum, float>(&FloatColumn::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    return aggregate<act_Sum, double>(&DoubleColumn::sum, column_ndx, resultcount, start, end, limit);
}

// Maximum

int64_t Query::maximum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit, 
                           size_t* return_ndx) const
{
    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Max, int64_t>(&IntNullColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
    }
    return aggregate<act_Max, int64_t>(&IntegerColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
    return aggregate<act_Max, int64_t>(&IntegerColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
}

DateTime Query::maximum_datetime(size_t column_ndx, size_t* resultcount, size_t start, size_t end, 
                                 size_t limit, size_t* return_ndx) const
{
    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Max, int64_t>(&IntNullColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
    }
    return aggregate<act_Max, int64_t>(&IntegerColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
    return aggregate<act_Max, int64_t>(&IntegerColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
}

float Query::maximum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, 
                           size_t limit, size_t* return_ndx) const
{
    return aggregate<act_Max, float>(&FloatColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
}
double Query::maximum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end,
                             size_t limit, size_t* return_ndx) const
{
    return aggregate<act_Max, double>(&DoubleColumn::maximum, column_ndx, resultcount, start, end, limit,
                                      return_ndx);
}


// Minimum

int64_t Query::minimum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, 
                           size_t limit, size_t* return_ndx) const
{
    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Min, int64_t>(&IntNullColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
    }
    return aggregate<act_Min, int64_t>(&IntegerColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
    return aggregate<act_Min, int64_t>(&IntegerColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
}
float Query::minimum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                           size_t* return_ndx) const
{
    return aggregate<act_Min, float>(&FloatColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
}
double Query::minimum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit, 
                             size_t* return_ndx) const
{
    return aggregate<act_Min, double>(&DoubleColumn::minimum, column_ndx, resultcount, start, end, limit, 
                                      return_ndx);
}

DateTime Query::minimum_datetime(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                                 size_t* return_ndx) const
{
    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Min, int64_t>(&IntNullColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
    }
    return aggregate<act_Min, int64_t>(&IntegerColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
    return aggregate<act_Min, int64_t>(&IntegerColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
}


// Average

template <typename T, bool Nullable>
double Query::average(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if(limit == 0 || m_table->is_degenerate()) {
        if (resultcount)
            *resultcount = 0;
        return 0.;
    }

    size_t resultcount2 = 0;
    typedef typename ColumnTypeTraits<T, Nullable>::column_type ColType;
    typedef typename ColumnTypeTraits<T, Nullable>::sum_type SumType;
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
    if (m_table->is_nullable(column_ndx)) {
        return average<int64_t, true>(column_ndx, resultcount, start, end, limit);
    }
    return average<int64_t, false>(column_ndx, resultcount, start, end, limit);
}
double Query::average_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if (m_table->is_nullable(column_ndx)) {
        return average<float, true>(column_ndx, resultcount, start, end, limit);
    }
    return average<float, false>(column_ndx, resultcount, start, end, limit);
}
double Query::average_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
    if (m_table->is_nullable(column_ndx)) {
        return average<double, true>(column_ndx, resultcount, start, end, limit);
    }
    return average<double, false>(column_ndx, resultcount, start, end, limit);
}


// Grouping
Query& Query::group()
{
    m_groups.emplace_back();
    return *this;
}
Query& Query::end_group()
{
    if (m_groups.size() < 2) {
        error_code = "Unbalanced group";
        return *this;
    }

    auto root_node = std::move(m_groups.back().m_root_node);
    m_groups.pop_back();

    if (root_node) {
        add_node(std::move(root_node));
    }

    handle_pending_not();
    return *this;
}

// Not creates an implicit group to capture the term that we want to negate.
Query& Query::Not()
{
    group();
    m_groups.back().m_pending_not = true;

    return *this;
}

// And-terms must end by calling handle_pending_not. This will check if a negation is pending,
// and if so, it will end the implicit group created to hold the term to negate. Note that
// end_group itself will recurse into handle_pending_not if multiple implicit groups are nested
// within each other.
void Query::handle_pending_not()
{
    auto& current_group = m_groups.back();
    if (m_groups.size() > 1 && current_group.m_pending_not) {
        // we are inside group(s) implicitly created to handle a not, so reparent its
        // nodes into a NotNode.
        auto not_node = std::unique_ptr<ParentNode>(new NotNode(std::move(current_group.m_root_node)));
        current_group.m_pending_not = false;

        add_node(std::move(not_node));
        end_group();
    }
}

Query& Query::Or()
{
    auto& current_group = m_groups.back();
    OrNode* or_node = dynamic_cast<OrNode*>(current_group.m_root_node.get());
    if (!or_node) {
        // Reparent the current group's nodes within an OrNode.
        add_node(std::unique_ptr<ParentNode>(new OrNode(std::move(current_group.m_root_node))));
    }
    current_group.m_state = QueryGroup::State::OrCondition;

    return *this;
}

Query& Query::subtable(size_t column)
{
    m_subtable_path.push_back(column);
    fetch_descriptor();
    group();
    m_groups.back().m_subtable_column = column;
    return *this;
}

Query& Query::end_subtable()
{
    auto& current_group = m_groups.back();
    if (current_group.m_subtable_column == not_found) {
        error_code = "Unbalanced subtable";
        return *this;
    }

    auto subtable_node = std::unique_ptr<ParentNode>(new SubtableNode(current_group.m_subtable_column,
                                                                      std::move(current_group.m_root_node)));
    end_group();
    add_node(std::move(subtable_node));

    m_subtable_path.pop_back();
    fetch_descriptor();
    return *this;
}

// todo, add size_t end? could be useful
size_t Query::find(size_t begin)
{
    if (m_table->is_degenerate())
        return not_found;

    REALM_ASSERT_3(begin, <=, m_table->size());

    init(*m_table);

    // User created query with no criteria; return first
    if (!has_conditions()) {
        if (m_view)
            return m_view->size() == 0 ? not_found : begin;
        else
            return m_table->size() == 0 ? not_found : begin;
    }

    if (m_view) {
        size_t end = m_view->size();
        for (; begin < end; begin++) {
            size_t res = peek_tableview(begin);
            if (res != not_found)
                return begin;
        }
        return not_found;
    }
    else {
        size_t end = m_table->size();
        size_t res = root_node()->find_first(begin, end);
        return (res == end) ? not_found : res;
    }
}

void Query::find_all(TableViewBase& ret, size_t start, size_t end, size_t limit) const
{
    if (limit == 0 || m_table->is_degenerate())
        return;

    REALM_ASSERT_3(start, <=, m_table->size());

    init(*m_table);

    if (end == size_t(-1))
        end = m_view ? m_view->size() : m_table->size();

    // User created query with no criteria; return everything
    if (!has_conditions()) {
        IntegerColumn& refs = ret.m_row_indexes;
        size_t end_pos = (limit != size_t(-1)) ? std::min(end, start + limit) : end;

        if (m_view) {
            for (size_t i = start; i < end_pos; ++i)
                refs.add(m_view->m_row_indexes.get(i));
        }
        else {
            for (size_t i = start; i < end_pos; ++i)
                refs.add(i);
        }
        return;
    }

    if (m_view) {
        for (size_t begin = start; begin < end && ret.size() < limit; begin++) {
            size_t res = peek_tableview(begin);
            if (res != not_found)
                ret.m_row_indexes.add(res);
        }
    }
    else {
        QueryState<int64_t> st;
        st.init(act_FindAll, &ret.m_row_indexes, limit);
        aggregate_internal(act_FindAll, ColumnTypeTraits<int64_t, false>::id, false, root_node(), &st, start, end, nullptr);
    }
}

TableView Query::find_all(size_t start, size_t end, size_t limit)
{
    TableView ret(*m_table, *this, start, end, limit);
    find_all(ret, start, end, limit);
    return ret;
}


size_t Query::count(size_t start, size_t end, size_t limit) const
{
    if(limit == 0 || m_table->is_degenerate())
        return 0;

    if (end == size_t(-1))
        end = m_view ? m_view->size() : m_table->size();

    if (!has_conditions()) {
        // User created query with no criteria; count all
        return (limit < end - start ? limit : end - start);
    }

    init(*m_table);
    size_t cnt = 0;

    if (m_view) {
        for (size_t begin = start; begin < end && cnt < limit; begin++) {
            size_t res = peek_tableview(begin);
            if (res != not_found)
                cnt++;
        }
    }
    else {
        QueryState<int64_t> st;
        st.init(act_Count, nullptr, limit);
        aggregate_internal(act_Count, ColumnTypeTraits<int64_t, false>::id, false, root_node(), &st, start, end, nullptr);
        cnt = size_t(st.m_state);
    }

    return cnt;
}


// todo, not sure if start, end and limit could be useful for delete.
size_t Query::remove(size_t start, size_t end, size_t limit)
{
    if(limit == 0 || m_table->is_degenerate())
        return 0;

    if (end == not_found)
        end = m_view ? m_view->size() : m_table->size();

    size_t results = 0;

    if (m_view) {
        for (;;) {
            if (start + results == end || results == limit)
                return results;

            init(*m_table);
            size_t r = peek_tableview(start + results);
            if (r != not_found) {
                m_table->remove(r);
                // new semantics for tableview means that the remove from m_table is automatically reflected
                // m_view->m_row_indexes.adjust_ge(m_view->m_row_indexes.get(start + results), -1);
                results++;
            }
            else {
                return results;
            }
        }
    }
    else {
        size_t r = start;
        for (;;) {
            // Every remove invalidates the array cache in the nodes
            // so we have to re-initialize it before searching
            init(*m_table);

            r = find_internal(r, end - results);
            if (r == not_found || r == m_table->size() || results == limit)
                break;
            ++results;
            m_table->remove(r);
        }
        return results;
    }
}

#if REALM_MULTITHREAD_QUERY
TableView Query::find_all_multi(size_t start, size_t end)
{
    (void)start;
    (void)end;

    // Initialization
    init(*m_table);
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
        const size_t from = ts.chunks[i].m_root_node;
        const size_t upto = (i == ts.chunks.size() - 1) ? size_t(-1) : ts.chunks[i + 1].m_root_node;
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
    pthread_mutex_init(&ts.result_mutex, nullptr);
    pthread_cond_init(&ts.completed_cond, nullptr);
    pthread_mutex_init(&ts.jobs_mutex, nullptr);
    pthread_mutex_init(&ts.completed_mutex, nullptr);
    pthread_cond_init(&ts.jobs_cond, nullptr);

    pthread_mutex_lock(&ts.jobs_mutex);

    for (size_t i = 0; i < m_threadcount; ++i)
        pthread_detach(threads[i]);

    for (size_t i = 0; i < threadcount; ++i) {
        int r = pthread_create(&threads[i], nullptr, query_thread, (void*)&ts);
        if (r != 0)
            REALM_ASSERT(false); //todo
    }
    m_threadcount = threadcount;
    return 0;
}


void* Query::query_thread(void* arg)
{
    static_cast<void>(arg);
    thread_state* ts = static_cast<thread_state*>(arg);

    std::vector<size_t> res;
    std::vector<pair<size_t, size_t>> chunks;

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
    return 0;
}

#endif // REALM_MULTITHREADQUERY

std::string Query::validate()
{
    if (!m_groups.size())
        return "";

    if (error_code != "") // errors detected by QueryInterface
        return error_code;

    if (!root_node())
        return "Syntax error";

    return root_node()->validate(); // errors detected by QueryEngine
}

void Query::init(const Table& table) const
{
    if (ParentNode* root = root_node()) {
        root->init(table);
        std::vector<ParentNode*> v;
        root->gather_children(v);
    }
}

bool Query::is_initialized() const
{
    if (ParentNode* root = root_node()) {
        return root->is_initialized();
    }
    return true;
}

size_t Query::find_internal(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_table->size();
    if (start == end)
        return not_found;

    size_t r;
    if (ParentNode* root = root_node())
        r = root->find_first(start, end);
    else
        r = start; // user built an empty query; return any first

    if (r == m_table->size())
        return not_found;
    else
        return r;
}

bool Query::comp(const std::pair<size_t, size_t>& a, const std::pair<size_t, size_t>& b)
{
    return a.first < b.first;
}

void Query::add_node(std::unique_ptr<ParentNode> node)
{
    REALM_ASSERT(node);
    using State = QueryGroup::State;

    auto& current_group = m_groups.back();
    switch (current_group.m_state) {
        case QueryGroup::State::OrCondition: {
            REALM_ASSERT_DEBUG(dynamic_cast<OrNode*>(current_group.m_root_node.get()));
            OrNode* or_node = static_cast<OrNode*>(current_group.m_root_node.get());
            or_node->m_conditions.emplace_back(std::move(node));
            current_group.m_state = State::OrConditionChildren;
            break;
        }
        case QueryGroup::State::OrConditionChildren: {
            REALM_ASSERT_DEBUG(dynamic_cast<OrNode*>(current_group.m_root_node.get()));
            OrNode* or_node = static_cast<OrNode*>(current_group.m_root_node.get());
            or_node->m_conditions.back()->add_child(std::move(node));
            break;
        }
        default: {
            if (!current_group.m_root_node) {
                current_group.m_root_node = std::move(node);
            } else {
                current_group.m_root_node->add_child(std::move(node));
            }
        }
    }

    handle_pending_not();
}

/* ********************************************************************************************************************
*
*  Stuff related to next-generation query syntax
*
******************************************************************************************************************** */

Query& Query::and_query(const Query& q)
{
    add_node(q.root_node()->clone());

    if (q.m_source_link_view) {
        REALM_ASSERT(!m_source_link_view || m_source_link_view == q.m_source_link_view);
        m_source_link_view = q.m_source_link_view;
    }

    return *this;
}

Query& Query::and_query(Query&& q)
{
    add_node(std::move(q.m_groups[0].m_root_node));

    if (q.m_source_link_view) {
        REALM_ASSERT(!m_source_link_view || m_source_link_view == q.m_source_link_view);
        m_source_link_view = q.m_source_link_view;
    }

    return *this;
}

Query Query::operator||(const Query& q)
{
    Query q2(*m_table);
    q2.and_query(*this);
    q2.Or();
    q2.and_query(q);

    return q2;
}


Query Query::operator&&(const Query& q)
{
    if (!root_node())
        return q;

    if (!q.root_node())
        return *this;

    Query q2(*m_table);
    q2.and_query(*this);
    q2.and_query(q);

    return q2;
}


Query Query::operator!()
{
    if (!root_node())
        throw std::runtime_error("negation of empty query is not supported");
    Query q(*this->m_table);
    q.Not();
    q.and_query(*this);
    return q;
}

QueryGroup::QueryGroup(const QueryGroup& other) :
    m_root_node(other.m_root_node ? other.m_root_node->clone() : nullptr),
    m_pending_not(other.m_pending_not), m_subtable_column(other.m_subtable_column), m_state(other.m_state)
{
}

QueryGroup& QueryGroup::operator=(const QueryGroup& other)
{
    if (this != &other) {
        m_root_node = other.m_root_node ? other.m_root_node->clone() : nullptr;
        m_pending_not = other.m_pending_not;
        m_subtable_column = other.m_subtable_column;
        m_state = other.m_state;
    }
    return *this;
}
