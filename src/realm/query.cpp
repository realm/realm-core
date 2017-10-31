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

#include <realm/query.hpp>

#include <realm/array.hpp>
#include <realm/column_fwd.hpp>
#include <realm/descriptor.hpp>
#include <realm/group_shared.hpp>
#include <realm/link_view.hpp>
#include <realm/query_engine.hpp>
#include <realm/query_expression.hpp>
#include <realm/table_view.hpp>

#include <algorithm>


using namespace realm;
using namespace realm::metrics;

Query::Query()
{
    create();
}

Query::Query(Table& table, TableViewBase* tv)
    : m_table(table.get_table_ref())
    , m_view(tv)
    , m_source_table_view(tv)
{
#ifdef REALM_DEBUG
    if (m_view)
        m_view->check_cookie();
#endif
    create();
}

Query::Query(const Table& table, const LinkViewRef& lv)
    : m_table((const_cast<Table&>(table)).get_table_ref())
    , m_view(lv.get())
    , m_source_link_view(lv)
{
#ifdef REALM_DEBUG
    if (m_view)
        m_view->check_cookie();
#endif
    REALM_ASSERT_DEBUG(&lv->get_target_table() == m_table);
    create();
}

Query::Query(const Table& table, TableViewBase* tv)
    : m_table((const_cast<Table&>(table)).get_table_ref())
    , m_view(tv)
    , m_source_table_view(tv)
{
#ifdef REALM_DEBUG
    if (m_view)
        m_view->check_cookie();
#endif
    create();
}

Query::Query(const Table& table, std::unique_ptr<TableViewBase> tv)
    : m_table((const_cast<Table&>(table)).get_table_ref())
    , m_view(tv.get())
    , m_source_table_view(tv.get())
    , m_owned_source_table_view(std::move(tv))
{
#ifdef REALM_DEBUG
    if (m_view)
        m_view->check_cookie();
#endif
    create();
}

void Query::create()
{
    m_groups.emplace_back();
    if (m_table)
        fetch_descriptor();
}

Query::Query(const Query& source)
    : error_code(source.error_code)
    , m_groups(source.m_groups)
    , m_current_descriptor(source.m_current_descriptor)
    , m_table(source.m_table)
{
    if (source.m_owned_source_table_view) {
        m_owned_source_table_view = source.m_owned_source_table_view->clone();
        m_source_table_view = m_owned_source_table_view.get();
        m_view = m_source_table_view;
    }
    else {
        // FIXME: The lifetime of `m_source_table_view` may be tied to that of `source`, which can easily
        // turn `m_source_table_view` into a dangling reference.
        m_source_table_view = source.m_source_table_view;

        m_source_link_view = source.m_source_link_view;
        m_view = source.m_view;
    }
}

Query& Query::operator=(const Query& source)
{
    if (this != &source) {
        m_groups = source.m_groups;
        m_table = source.m_table;

        if (source.m_owned_source_table_view) {
            m_owned_source_table_view = source.m_owned_source_table_view->clone();
            m_source_table_view = m_owned_source_table_view.get();
            m_view = m_source_table_view;

            m_source_link_view.reset();
        }
        else {
            // FIXME: The lifetime of `m_source_table_view` may be tied to that of `source`, which can easily
            // turn `m_source_table_view` into a dangling reference.
            m_source_table_view = source.m_source_table_view;
            m_owned_source_table_view = nullptr;

            m_source_link_view = source.m_source_link_view;
            m_view = source.m_view;
        }

        if (m_table)
            fetch_descriptor();
    }
    return *this;
}

Query::Query(Query&&) = default;

Query::Query(Query&& other, TableViewBase* tv) : Query(std::move(other))
{
    if (tv) {
        REALM_ASSERT(tv->m_table == m_table);
        m_view = tv;
    }
}

Query& Query::operator=(Query&&) = default;

Query::~Query() noexcept = default;

Query::Query(Query& source, HandoverPatch& patch, MutableSourcePayload mode)
    : m_table(TableRef())
    , m_source_link_view(LinkViewRef())
{
    Table::generate_patch(source.m_table.get(), patch.m_table);
    if (source.m_source_table_view) {
        m_owned_source_table_view = source.m_source_table_view->clone_for_handover(patch.table_view_data, mode);
        m_source_table_view = m_owned_source_table_view.get();
    }
    else {
        patch.table_view_data = nullptr;
    }
    LinkView::generate_patch(source.m_source_link_view, patch.link_view_data);

    m_groups.reserve(source.m_groups.size());
    for (const auto& cur_group : source.m_groups) {
        m_groups.emplace_back(cur_group, patch.m_node_data);
    }
}

Query::Query(const Query& source, HandoverPatch& patch, ConstSourcePayload mode)
    : m_table(TableRef())
    , m_source_link_view(LinkViewRef())
{
    Table::generate_patch(source.m_table.get(), patch.m_table);
    if (source.m_source_table_view) {
        m_owned_source_table_view = source.m_source_table_view->clone_for_handover(patch.table_view_data, mode);
        m_source_table_view = m_owned_source_table_view.get();
    }
    else {
        patch.table_view_data = nullptr;
    }
    LinkView::generate_patch(source.m_source_link_view, patch.link_view_data);

    m_groups.reserve(source.m_groups.size());
    for (const auto& cur_group : source.m_groups) {
        m_groups.emplace_back(cur_group, patch.m_node_data);
    }
}

Query::Query(std::unique_ptr<Expression> expr)
    : Query()
{
    if (auto table = const_cast<Table*>(expr->get_base_table()))
        set_table(table->get_table_ref());

    add_expression_node(std::move(expr));
}

void Query::set_table(TableRef tr)
{
    REALM_ASSERT(!m_table);
    m_table = tr;
    if (m_table) {
        fetch_descriptor();
        ParentNode* root = root_node();
        if (root && !m_table->is_degenerate())
            root->set_table(*m_table);
    }
    else {
        m_current_descriptor.reset();
    }
}


void Query::apply_patch(HandoverPatch& patch, Group& dest_group)
{
    if (m_source_table_view) {
        m_source_table_view->apply_and_consume_patch(patch.table_view_data, dest_group);
    }
    m_source_link_view = LinkView::create_from_and_consume_patch(patch.link_view_data, dest_group);
    if (m_source_link_view)
        m_view = m_source_link_view.get();
    else if (m_source_table_view)
        m_view = m_source_table_view;
    else
        m_view = nullptr;

    for (auto it = m_groups.rbegin(); it != m_groups.rend(); ++it) {
        if (auto& cur_root_node = it->m_root_node)
            cur_root_node->apply_handover_patch(patch.m_node_data, dest_group);
    }
    // not going through Table::create_from_and_consume_patch because we need to use
    // set_table() to update all table references
    if (patch.m_table) {
        if (patch.m_table->m_is_sub_table) {
            auto parent_table = dest_group.get_table(patch.m_table->m_table_num);
            set_table(parent_table->get_subtable(patch.m_table->m_col_ndx, patch.m_table->m_row_ndx));
        }
        else {
            set_table(dest_group.get_table(patch.m_table->m_table_num));
        }
    }
    REALM_ASSERT(patch.m_node_data.empty());
}

void Query::add_expression_node(std::unique_ptr<Expression> expression)
{
    add_node(std::unique_ptr<ParentNode>(new ExpressionNode(std::move(expression))));
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

template <class Node>
struct MakeConditionNode {
    static std::unique_ptr<ParentNode> make(size_t col_ndx, typename Node::TConditionValue value)
    {
        return std::unique_ptr<ParentNode>{new Node(std::move(value), col_ndx)};
    }

    static std::unique_ptr<ParentNode> make(size_t col_ndx, null)
    {
        return std::unique_ptr<ParentNode>{new Node(null{}, col_ndx)};
    }

    template <class T = typename Node::TConditionValue>
    static typename std::enable_if<!std::is_same<typename util::RemoveOptional<T>::type, T>::value,
                                   std::unique_ptr<ParentNode>>::type
    make(size_t col_ndx, typename util::RemoveOptional<T>::type value)
    {
        return std::unique_ptr<ParentNode>{new Node(std::move(value), col_ndx)};
    }

    template <class T>
    static std::unique_ptr<ParentNode> make(size_t, T)
    {
        throw LogicError{LogicError::type_mismatch};
    }
};

template <class Cond>
struct MakeConditionNode<IntegerNode<IntegerColumn, Cond>> {
    static std::unique_ptr<ParentNode> make(size_t col_ndx, int64_t value)
    {
        return std::unique_ptr<ParentNode>{new IntegerNode<IntegerColumn, Cond>(std::move(value), col_ndx)};
    }

    template <class T>
    static std::unique_ptr<ParentNode> make(size_t, T)
    {
        throw LogicError{LogicError::type_mismatch};
    }
};

template <class Cond>
struct MakeConditionNode<StringNode<Cond>> {
    static std::unique_ptr<ParentNode> make(size_t col_ndx, StringData value)
    {
        return std::unique_ptr<ParentNode>{new StringNode<Cond>(std::move(value), col_ndx)};
    }

    static std::unique_ptr<ParentNode> make(size_t col_ndx, null)
    {
        return std::unique_ptr<ParentNode>{new StringNode<Cond>(null{}, col_ndx)};
    }

    template <class T>
    static std::unique_ptr<ParentNode> make(size_t, T)
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
        case type_OldDateTime: {
            if (is_nullable) {
                return MakeConditionNode<IntegerNode<IntNullColumn, Cond>>::make(column_ndx, value);
            }
            else {
                return MakeConditionNode<IntegerNode<IntegerColumn, Cond>>::make(column_ndx, value);
            }
        }
        case type_Float: {
            return MakeConditionNode<FloatDoubleNode<FloatColumn, Cond>>::make(column_ndx, value);
        }
        case type_Double: {
            return MakeConditionNode<FloatDoubleNode<DoubleColumn, Cond>>::make(column_ndx, value);
        }
        case type_String: {
            return MakeConditionNode<StringNode<Cond>>::make(column_ndx, value);
        }
        case type_Binary: {
            return MakeConditionNode<BinaryNode<Cond>>::make(column_ndx, value);
        }
        case type_Timestamp: {
            return MakeConditionNode<TimestampNode<Cond>>::make(column_ndx, value);
        }
        default: {
            throw LogicError{LogicError::type_mismatch};
        }
    }
}

template <class Cond>
std::unique_ptr<ParentNode> make_size_condition_node(const Descriptor& descriptor, size_t column_ndx, int64_t value)
{
    DataType type = descriptor.get_column_type(column_ndx);
    switch (type) {
        case type_String: {
            return std::unique_ptr<ParentNode>{new SizeNode<StringColumn, Cond>(value, column_ndx)};
        }
        case type_Binary: {
            return std::unique_ptr<ParentNode>{new SizeNode<BinaryColumn, Cond>(value, column_ndx)};
        }
        case type_LinkList: {
            return std::unique_ptr<ParentNode>{new SizeNode<LinkListColumn, Cond>(value, column_ndx)};
        }
        case type_Table: {
            return std::unique_ptr<ParentNode>{new SizeNode<SubtableColumn, Cond>(value, column_ndx)};
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


template <typename TConditionFunction>
Query& Query::add_size_condition(size_t column_ndx, int64_t value)
{
    REALM_ASSERT_DEBUG(m_current_descriptor);
    auto node = make_size_condition_node<TConditionFunction>(*m_current_descriptor, column_ndx, value);
    add_node(std::move(node));
    return *this;
}


template <class ColumnType>
Query& Query::equal(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Equal>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}

// Two column methods, any type
template <class ColumnType>
Query& Query::less(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Less>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::less_equal(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, LessEqual>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::greater(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Greater>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::greater_equal(size_t column_ndx1, size_t column_ndx2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, GreaterEqual>(column_ndx1, column_ndx2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::not_equal(size_t column_ndx1, size_t column_ndx2)
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
    return not_equal<FloatColumn>(column_ndx1, column_ndx2);
}

Query& Query::less_float(size_t column_ndx1, size_t column_ndx2)
{
    return less<FloatColumn>(column_ndx1, column_ndx2);
}

Query& Query::greater_float(size_t column_ndx1, size_t column_ndx2)
{
    return greater<FloatColumn>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<FloatColumn>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<FloatColumn>(column_ndx1, column_ndx2);
}

Query& Query::equal_float(size_t column_ndx1, size_t column_ndx2)
{
    return equal<FloatColumn>(column_ndx1, column_ndx2);
}

// column vs column, double
Query& Query::equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return equal<DoubleColumn>(column_ndx1, column_ndx2);
}

Query& Query::less_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return less_equal<DoubleColumn>(column_ndx1, column_ndx2);
}

Query& Query::greater_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return greater_equal<DoubleColumn>(column_ndx1, column_ndx2);
}
Query& Query::greater_double(size_t column_ndx1, size_t column_ndx2)
{
    return greater<DoubleColumn>(column_ndx1, column_ndx2);
}
Query& Query::less_double(size_t column_ndx1, size_t column_ndx2)
{
    return less<DoubleColumn>(column_ndx1, column_ndx2);
}

Query& Query::not_equal_double(size_t column_ndx1, size_t column_ndx2)
{
    return not_equal<DoubleColumn>(column_ndx1, column_ndx2);
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

Query& Query::links_to(size_t origin_column, const ConstRow& target_row)
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


// ------------- Timestamp
Query& Query::greater(size_t column_ndx, Timestamp value)
{
    return add_condition<Greater>(column_ndx, value);
}
Query& Query::equal(size_t column_ndx, Timestamp value)
{
    return add_condition<Equal>(column_ndx, value);
}
Query& Query::not_equal(size_t column_ndx, Timestamp value)
{
    return add_condition<NotEqual>(column_ndx, value);
}
Query& Query::greater_equal(size_t column_ndx, Timestamp value)
{
    return add_condition<GreaterEqual>(column_ndx, value);
}
Query& Query::less_equal(size_t column_ndx, Timestamp value)
{
    return add_condition<LessEqual>(column_ndx, value);
}
Query& Query::less(size_t column_ndx, Timestamp value)
{
    return add_condition<Less>(column_ndx, value);
}

// ------------- size
Query& Query::size_equal(size_t column_ndx, int64_t value)
{
    return add_size_condition<Equal>(column_ndx, value);
}
Query& Query::size_not_equal(size_t column_ndx, int64_t value)
{
    return add_size_condition<NotEqual>(column_ndx, value);
}
Query& Query::size_greater(size_t column_ndx, int64_t value)
{
    return add_size_condition<Greater>(column_ndx, value);
}
Query& Query::size_greater_equal(size_t column_ndx, int64_t value)
{
    return add_size_condition<GreaterEqual>(column_ndx, value);
}
Query& Query::size_less_equal(size_t column_ndx, int64_t value)
{
    return add_size_condition<LessEqual>(column_ndx, value);
}
Query& Query::size_less(size_t column_ndx, int64_t value)
{
    return add_size_condition<Less>(column_ndx, value);
}
Query& Query::size_between(size_t column_ndx, int64_t from, int64_t to)
{
    group();
    size_greater_equal(column_ndx, from);
    size_less_equal(column_ndx, to);
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
Query& Query::like(size_t column_ndx, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Like>(column_ndx, value);
    else
        add_condition<LikeIns>(column_ndx, value);
    return *this;
}


// Aggregates =================================================================================

size_t Query::peek_tablerow(size_t tablerow) const
{
#ifdef REALM_DEBUG
    m_view->check_cookie();
#endif

    if (has_conditions())
        return root_node()->find_first(tablerow, tablerow + 1);

    // Query has no conditions, so all rows match, also the user given argument
    return tablerow;
}

template <Action action, typename T, typename R, class ColType>
R Query::aggregate(R (ColType::*aggregateMethod)(size_t start, size_t end, size_t limit, size_t* return_ndx) const,
                   size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                   size_t* return_ndx) const
{
    if (limit == 0 || m_table->is_degenerate()) {
        if (resultcount)
            *resultcount = 0;
        return static_cast<R>(0);
    }

    if (end == size_t(-1))
        end = m_table->size();

    const ColType& column = m_table->get_column<ColType, ColumnType(ColumnTypeTraits<T>::id)>(column_ndx);

    if (!has_conditions() && !m_view) {
        // No criteria, so call aggregate METHODS directly on columns
        // - this bypasses the query system and is faster
        // User created query with no criteria; aggregate range
        if (resultcount) {
            *resultcount = limit < (end - start) ? limit : (end - start);
        }
        // direct aggregate on the column
        return (column.*aggregateMethod)(start, end, limit, action == act_Sum ? resultcount : return_ndx);
    }
    else {

        // Aggregate with criteria - goes through the nodes in the query system
        init();
        QueryState<R> st;
        st.init(action, nullptr, limit);

        SequentialGetter<ColType> source_column(*m_table, column_ndx);

        if (!m_view) {
            aggregate_internal(action, ColumnTypeTraits<T>::id, ColType::nullable, root_node(), &st, start, end,
                               &source_column);
        }
        else {
            for (size_t t = 0; t < m_view->size(); t++) {
                size_t tablerow = static_cast<size_t>(m_view->m_row_indexes.get(t));
                if (tablerow >= start && tablerow < end && peek_tablerow(tablerow) != not_found) {
                    st.template match<action, false>(tablerow, 0, source_column.get_next(tablerow));
                    if (st.m_match_count >= limit) {
                        break;
                    }
                }
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

void Query::aggregate_internal(Action TAction, DataType TSourceColumn, bool nullable, ParentNode* pn,
                               QueryStateBase* st, size_t start, size_t end,
                               SequentialGetterBase* source_column) const
{
    if (end == not_found)
        end = m_table->size();

    for (size_t c = 0; c < pn->m_children.size(); c++)
        pn->m_children[c]->aggregate_local_prepare(TAction, TSourceColumn, nullable);

    size_t td;

    while (start < end) {
        auto score_compare = [](const ParentNode* a, const ParentNode* b) { return a->cost() < b->cost(); };
        size_t best = std::distance(pn->m_children.begin(),
                                    std::min_element(pn->m_children.begin(), pn->m_children.end(), score_compare));

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
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Sum);
#endif

    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Sum, int64_t>(&IntNullColumn::sum, column_ndx, resultcount, start, end, limit);
    }
    return aggregate<act_Sum, int64_t>(&IntegerColumn::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Sum);
#endif

    return aggregate<act_Sum, float>(&FloatColumn::sum, column_ndx, resultcount, start, end, limit);
}
double Query::sum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Sum);
#endif

    return aggregate<act_Sum, double>(&DoubleColumn::sum, column_ndx, resultcount, start, end, limit);
}

// Maximum

int64_t Query::maximum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                           size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Max, int64_t>(&IntNullColumn::maximum, column_ndx, resultcount, start, end, limit,
                                           return_ndx);
    }
    return aggregate<act_Max, int64_t>(&IntegerColumn::maximum, column_ndx, resultcount, start, end, limit,
                                       return_ndx);
}

OldDateTime Query::maximum_olddatetime(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                                       size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Max, int64_t>(&IntNullColumn::maximum, column_ndx, resultcount, start, end, limit,
                                           return_ndx);
    }
    return aggregate<act_Max, int64_t>(&IntegerColumn::maximum, column_ndx, resultcount, start, end, limit,
                                       return_ndx);
}

float Query::maximum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                           size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    return aggregate<act_Max, float>(&FloatColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
}
double Query::maximum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                             size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    return aggregate<act_Max, double>(&DoubleColumn::maximum, column_ndx, resultcount, start, end, limit, return_ndx);
}


// Minimum

int64_t Query::minimum_int(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                           size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Min, int64_t>(&IntNullColumn::minimum, column_ndx, resultcount, start, end, limit,
                                           return_ndx);
    }
    return aggregate<act_Min, int64_t>(&IntegerColumn::minimum, column_ndx, resultcount, start, end, limit,
                                       return_ndx);
}
float Query::minimum_float(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                           size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    return aggregate<act_Min, float>(&FloatColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
}
double Query::minimum_double(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                             size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    return aggregate<act_Min, double>(&DoubleColumn::minimum, column_ndx, resultcount, start, end, limit, return_ndx);
}

OldDateTime Query::minimum_olddatetime(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit,
                                       size_t* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    if (m_table->is_nullable(column_ndx)) {
        return aggregate<act_Min, int64_t>(&IntNullColumn::minimum, column_ndx, resultcount, start, end, limit,
                                           return_ndx);
    }
    return aggregate<act_Min, int64_t>(&IntegerColumn::minimum, column_ndx, resultcount, start, end, limit,
                                       return_ndx);
}

Timestamp Query::minimum_timestamp(size_t column_ndx, size_t* return_ndx, size_t start, size_t end, size_t limit)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif
    TableView tv(*m_table, *this, start, end, limit);
    find_all(tv, start, end, limit);
    Timestamp ts = tv.minimum_timestamp(column_ndx, return_ndx);
    return ts;
}

Timestamp Query::maximum_timestamp(size_t column_ndx, size_t* return_ndx, size_t start, size_t end, size_t limit)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    TableView tv(*m_table, *this, start, end, limit);
    find_all(tv, start, end, limit);
    Timestamp ts = tv.maximum_timestamp(column_ndx, return_ndx);
    return ts;
}


// Average

template <typename T, bool Nullable>
double Query::average(size_t column_ndx, size_t* resultcount, size_t start, size_t end, size_t limit) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Average);
#endif

    if (limit == 0 || m_table->is_degenerate()) {
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
    if (m_table->is_nullable(column_ndx)) {
        return average<util::Optional<int64_t>, true>(column_ndx, resultcount, start, end, limit);
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

    auto end_root_node = std::move(m_groups.back().m_root_node);
    m_groups.pop_back();

    if (end_root_node) {
        add_node(std::move(end_root_node));
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

    auto subtable_node = std::unique_ptr<ParentNode>(
        new SubtableNode(current_group.m_subtable_column, std::move(current_group.m_root_node)));
    end_group();
    m_subtable_path.pop_back();
    add_node(std::move(subtable_node));

    fetch_descriptor();
    return *this;
}

// todo, add size_t end? could be useful
size_t Query::find(size_t begin)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Find);
#endif

    if (m_table->is_degenerate())
        return not_found;

    REALM_ASSERT_3(begin, <=, m_table->size());

    init();

    // User created query with no criteria; return first
    if (!has_conditions()) {
        if (m_view) {
            for (size_t t = 0; t < m_view->size(); t++) {
                size_t tablerow = static_cast<size_t>(m_view->m_row_indexes.get(t));
                if (tablerow >= begin)
                    return tablerow;
            }
            return not_found;
        }
        else
            return m_table->size() == 0 ? not_found : begin;
    }

    if (m_view) {
        for (size_t t = 0; t < m_view->size(); t++) {
            size_t tablerow = static_cast<size_t>(m_view->m_row_indexes.get(t));
            if (tablerow >= begin && peek_tablerow(tablerow) != not_found)
                return tablerow;
        }
        return not_found;
    }
    else {
        size_t end = m_table->size();
        size_t res = root_node()->find_first(begin, end);
        return (res == end) ? not_found : res;
    }
}

void Query::find_all(TableViewBase& ret, size_t begin, size_t end, size_t limit) const
{
    if (limit == 0 || m_table->is_degenerate())
        return;

    REALM_ASSERT_3(begin, <=, m_table->size());

    init();

    if (end == size_t(-1))
        end = m_table->size();

    if (m_view) {
        for (size_t t = 0; t < m_view->size() && ret.size() < limit; t++) {
            size_t tablerow = static_cast<size_t>(m_view->m_row_indexes.get(t));
            if (tablerow >= begin && tablerow < end && peek_tablerow(tablerow) != not_found) {
                ret.m_row_indexes.add(tablerow);
            }
        }
    }
    else {
        if (!has_conditions()) {
            IntegerColumn& refs = ret.m_row_indexes;
            for (size_t i = begin; i < end && refs.size() < limit; ++i) {
                refs.add(i);
            }
        }
        else {
            QueryState<int64_t> st;
            st.init(act_FindAll, &ret.m_row_indexes, limit);
            aggregate_internal(act_FindAll, ColumnTypeTraits<int64_t>::id, false, root_node(), &st, begin, end,
                               nullptr);
        }
    }
}

TableView Query::find_all(size_t start, size_t end, size_t limit)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_FindAll);
#endif

    TableView ret(*m_table, *this, start, end, limit);
    find_all(ret, start, end, limit);
    return ret;
}


size_t Query::count(size_t start, size_t end, size_t limit) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Count);
#endif

    if (limit == 0 || m_table->is_degenerate())
        return 0;

    if (end == size_t(-1))
        end = m_table->size();

    if (!has_conditions()) {
        // User created query with no criteria; count all
        if (m_view) {
            return (limit < m_view->size() - start ? limit : m_view->size() - start);
        }
        else {
            return (limit < end - start ? limit : end - start);
        }
    }

    init();
    size_t cnt = 0;

    if (m_view) {
        for (size_t t = 0; t < m_view->size() && cnt < limit; t++) {
            size_t tablerow = static_cast<size_t>(m_view->m_row_indexes.get(t));
            if (tablerow >= start && tablerow < end && peek_tablerow(tablerow) != not_found) {
                cnt++;
            }
        }
    }
    else {
        QueryState<int64_t> st;
        st.init(act_Count, nullptr, limit);
        aggregate_internal(act_Count, ColumnTypeTraits<int64_t>::id, false, root_node(), &st, start, end, nullptr);
        cnt = size_t(st.m_state);
    }

    return cnt;
}


// todo, not sure if start, end and limit could be useful for delete.
size_t Query::remove()
{
    if (m_table->is_degenerate())
        return 0;

    TableView tv = find_all();
    size_t rows = tv.size();
    tv.clear();
    return rows;
}

#if REALM_MULTITHREAD_QUERY
TableView Query::find_all_multi(size_t start, size_t end)
{
    static_cast<void>(start);
    static_cast<void>(end);

    // Initialization
    init();
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
    pthread_win32_process_attach_np();
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
            REALM_ASSERT(false); // todo
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

std::string Query::get_description() const
{
    if (root_node()) {
        return root_node()->describe_expression();
    }
    return "";
}

void Query::init() const
{
    REALM_ASSERT(m_table);
    if (ParentNode* root = root_node()) {
        root->init();
        std::vector<ParentNode*> v;
        root->gather_children(v);
    }
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

void Query::add_node(std::unique_ptr<ParentNode> node)
{
    REALM_ASSERT(node);
    using State = QueryGroup::State;

    if (m_table && m_subtable_path.empty() && !m_table->is_degenerate())
        node->set_table(*m_table);

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
            }
            else {
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
********************************************************************************************************************
*/

Query& Query::and_query(const Query& q)
{
    Query copy(q);
    return and_query(std::move(copy));
}

Query& Query::and_query(Query&& q)
{
    if (q.root_node()) {
        add_node(std::move(q.m_groups[0].m_root_node));

        if (q.m_source_link_view) {
            REALM_ASSERT(!m_source_link_view || m_source_link_view == q.m_source_link_view);
            m_source_link_view = q.m_source_link_view;
        }
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

util::Optional<uint_fast64_t> Query::sync_view_if_needed() const
{
    if (m_view)
        return m_view->sync_if_needed();

    if (m_table)
        return m_table->get_version_counter();

    return util::none;
}

QueryGroup::QueryGroup(const QueryGroup& other)
    : m_root_node(other.m_root_node ? other.m_root_node->clone() : nullptr)
    , m_pending_not(other.m_pending_not)
    , m_subtable_column(other.m_subtable_column)
    , m_state(other.m_state)
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

QueryGroup::QueryGroup(const QueryGroup& other, QueryNodeHandoverPatches& patches)
    : m_root_node(other.m_root_node ? other.m_root_node->clone(&patches) : nullptr)
    , m_pending_not(other.m_pending_not)
    , m_subtable_column(other.m_subtable_column)
    , m_state(other.m_state)
{
}
