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
#include <realm/array_integer_tpl.hpp>
#include <realm/transaction.hpp>
#include <realm/dictionary.hpp>
#include <realm/query_conditions_tpl.hpp>
#include <realm/query_engine.hpp>
#include <realm/query_expression.hpp>
#include <realm/table_view.hpp>
#include <realm/set.hpp>

#include <algorithm>

using namespace realm;

Query::Query()
{
    create();
}

Query::Query(ConstTableRef table, const ObjList& list)
    : m_table(table.cast_away_const())
    , m_source_collection(list.clone_obj_list())
{
    m_view = m_source_collection.get();
    REALM_ASSERT_DEBUG(m_view);
    REALM_ASSERT_DEBUG(list.get_target_table() == m_table);
    create();
}

Query::Query(ConstTableRef table, LinkCollectionPtr&& list_ptr)
    : m_table(table.cast_away_const())
    , m_source_collection(std::move(list_ptr))
{
    m_view = m_source_collection.get();
    REALM_ASSERT_DEBUG(m_view);
    REALM_ASSERT_DEBUG(m_view->get_target_table() == m_table);
    create();
}

Query::Query(ConstTableRef table, TableView* tv)
    : m_table(table.cast_away_const())
    , m_view(tv)
    , m_source_table_view(tv)
{
    create();
}

Query::Query(ConstTableRef table, std::unique_ptr<TableView> tv)
    : m_table(table.cast_away_const())
    , m_view(tv.get())
    , m_source_table_view(tv.get())
    , m_owned_source_table_view(std::move(tv))
{
    create();
}

void Query::create()
{
    if (m_table && m_table->is_asymmetric()) {
        throw IllegalOperation{"Query on ephemeral objects not allowed"};
    }
    m_groups.emplace_back();
}

Query::Query(const Query& source)
    : error_code(source.error_code)
    , m_groups(source.m_groups)
    , m_table(source.m_table)
    , m_ordering(source.m_ordering)
{
    if (source.m_owned_source_table_view) {
        m_owned_source_table_view = source.m_owned_source_table_view->clone();
        m_source_table_view = m_owned_source_table_view.get();
    }
    else {
        // FIXME: The lifetime of `m_source_table_view` may be tied to that of `source`, which can easily
        // turn `m_source_table_view` into a dangling reference.
        m_source_table_view = source.m_source_table_view;
        m_source_collection = source.m_source_collection ? source.m_source_collection->clone_obj_list() : nullptr;
    }
    if (m_source_table_view) {
        m_view = m_source_table_view;
    }
    else if (m_source_collection) {
        m_view = m_source_collection.get();
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

            m_source_collection = nullptr;
        }
        else {
            // FIXME: The lifetime of `m_source_table_view` may be tied to that of `source`, which can easily
            // turn `m_source_table_view` into a dangling reference.
            m_source_table_view = source.m_source_table_view;
            m_owned_source_table_view = nullptr;

            m_source_collection = source.m_source_collection ? source.m_source_collection->clone_obj_list() : nullptr;
        }
        if (m_source_table_view) {
            m_view = m_source_table_view;
        }
        else if (m_source_collection) {
            m_view = m_source_collection.get();
        }
        m_ordering = source.m_ordering;
    }
    return *this;
}

Query::Query(Query&&) = default;
Query& Query::operator=(Query&&) = default;

Query::~Query() noexcept = default;

Query::Query(const Query* source, Transaction* tr, PayloadPolicy policy)
{
    if (source->m_source_table_view) {
        m_owned_source_table_view = tr->import_copy_of(*source->m_source_table_view, policy);
        m_source_table_view = m_owned_source_table_view.get();
        m_view = m_source_table_view;
    }
    else {
        // nothing?
    }
    if (source->m_source_collection) {
        m_source_collection = tr->import_copy_of(source->m_source_collection);
        m_view = m_source_collection.get();
        REALM_ASSERT_DEBUG(m_view);
    }
    m_groups = source->m_groups;
    if (source->m_table)
        set_table(tr->import_copy_of(source->m_table));
    // otherwise: empty query.
}

Query::Query(std::unique_ptr<Expression> expr)
    : Query()
{
    if (auto table = expr->get_base_table())
        set_table(table.cast_away_const());

    add_expression_node(std::move(expr));
}

void Query::set_table(TableRef tr)
{
    if (tr == m_table) {
        return;
    }

    if (tr->is_asymmetric()) {
        throw IllegalOperation{"Query on ephemeral objects not allowed"};
    }

    m_table = tr;
    if (m_table) {
        ParentNode* root = root_node();
        if (root)
            root->set_table(m_table);
    }
}


void Query::add_expression_node(std::unique_ptr<Expression> expression)
{
    add_node(std::unique_ptr<ParentNode>(new ExpressionNode(std::move(expression))));
}

// Binary
Query& Query::equal(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<Equal>(column_key, b);
    }
    else {
        add_condition<EqualIns>(column_key, b);
    }
    return *this;
}
Query& Query::not_equal(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<NotEqual>(column_key, b);
    }
    else {
        add_condition<NotEqualIns>(column_key, b);
    }
    return *this;
}
Query& Query::begins_with(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<BeginsWith>(column_key, b);
    }
    else {
        add_condition<BeginsWithIns>(column_key, b);
    }
    return *this;
}
Query& Query::ends_with(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<EndsWith>(column_key, b);
    }
    else {
        add_condition<EndsWithIns>(column_key, b);
    }
    return *this;
}
Query& Query::contains(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<Contains>(column_key, b);
    }
    else {
        add_condition<ContainsIns>(column_key, b);
    }
    return *this;
}
Query& Query::like(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<Like>(column_key, b);
    }
    else {
        add_condition<LikeIns>(column_key, b);
    }
    return *this;
}


namespace {

REALM_NOINLINE REALM_COLD REALM_NORETURN void throw_type_mismatch_error()
{
    throw LogicError(ErrorCodes::TypeMismatch, "Could not build query");
}

template <class Node>
struct MakeConditionNode {
    static std::unique_ptr<ParentNode> make(ColKey col_key, typename Node::TConditionValue value)
    {
        return std::unique_ptr<ParentNode>{new Node(std::move(value), col_key)};
    }

    static std::unique_ptr<ParentNode> make(ColKey col_key, null)
    {
        return std::unique_ptr<ParentNode>{new Node(null{}, col_key)};
    }

    static std::unique_ptr<ParentNode> make(ColKey col_key, Mixed value)
    {
        return std::unique_ptr<ParentNode>{new Node(value.get<typename Node::TConditionValue>(), col_key)};
    }

    // overload for optional types
    template <class T = typename Node::TConditionValue>
    static typename std::enable_if<!std::is_same<typename util::RemoveOptional<T>::type, T>::value,
                                   std::unique_ptr<ParentNode>>::type
    make(ColKey col_key, typename util::RemoveOptional<T>::type value)
    {
        return std::unique_ptr<ParentNode>{new Node(std::move(value), col_key)};
    }

    template <class T>
    REALM_FORCEINLINE static std::unique_ptr<ParentNode> make(ColKey, T&&)
    {
        throw_type_mismatch_error();
    }
};

template <class Cond>
struct MakeConditionNode<IntegerNode<ArrayInteger, Cond>> {
    static std::unique_ptr<ParentNode> make(ColKey col_key, int64_t value)
    {
        return std::unique_ptr<ParentNode>{new IntegerNode<ArrayInteger, Cond>(std::move(value), col_key)};
    }

    template <class T>
    REALM_FORCEINLINE static std::unique_ptr<ParentNode> make(ColKey, T&&)
    {
        throw_type_mismatch_error();
    }
};

template <class Cond>
struct MakeConditionNode<StringNode<Cond>> {
    static std::unique_ptr<ParentNode> make(ColKey col_key, StringData value)
    {
        return std::unique_ptr<ParentNode>{new StringNode<Cond>(std::move(value), col_key)};
    }

    static std::unique_ptr<ParentNode> make(ColKey col_key, null)
    {
        return std::unique_ptr<ParentNode>{new StringNode<Cond>(null{}, col_key)};
    }

    template <class T>
    REALM_FORCEINLINE static std::unique_ptr<ParentNode> make(ColKey, T&&)
    {
        throw_type_mismatch_error();
    }
};

template <class Cond>
struct MakeConditionNode<TimestampNode<Cond>> {
    static std::unique_ptr<ParentNode> make(ColKey col_key, Timestamp value)
    {
        return std::unique_ptr<ParentNode>{new TimestampNode<Cond>(std::move(value), col_key)};
    }

    // only enable certain template conditions of supported timestamp operations
    template <typename... SubstitutionEnabler, typename U = Cond>
    static std::enable_if_t<is_any_v<U, Equal, NotEqual, Greater, Less, GreaterEqual, LessEqual>,
                            std::unique_ptr<ParentNode>>
    make(ColKey col_key, Mixed value)
    {
        static_assert(sizeof...(SubstitutionEnabler) == 0, "Do not specify template arguments");
        return std::unique_ptr<ParentNode>{new TimestampNode<Cond>(value.get<Timestamp>(), col_key)};
    }

    static std::unique_ptr<ParentNode> make(ColKey col_key, null)
    {
        return std::unique_ptr<ParentNode>{new TimestampNode<Cond>(null{}, col_key)};
    }

    template <class T>
    REALM_FORCEINLINE static std::unique_ptr<ParentNode> make(ColKey, T&&)
    {
        throw_type_mismatch_error();
    }
};

template <class Cond>
struct MakeConditionNode<MixedNode<Cond>> {
    template <class T>
    static std::unique_ptr<ParentNode> make(ColKey col_key, T value)
    {
        return std::unique_ptr<ParentNode>{new MixedNode<Cond>(Mixed(value), col_key)};
    }
};

template <class Cond, class T>
std::unique_ptr<ParentNode> make_condition_node(const Table& table, ColKey column_key, T value)
{
    table.check_column(column_key);
    DataType type = DataType(column_key.get_type());
    switch (type) {
        case type_Int: {
            if (column_key.get_attrs().test(col_attr_Nullable)) {
                return MakeConditionNode<IntegerNode<ArrayIntNull, Cond>>::make(column_key, value);
            }
            else {
                return MakeConditionNode<IntegerNode<ArrayInteger, Cond>>::make(column_key, value);
            }
        }
        case type_Bool: {
            return MakeConditionNode<BoolNode<Cond>>::make(column_key, value);
        }
        case type_Float: {
            return MakeConditionNode<FloatDoubleNode<ArrayFloat, Cond>>::make(column_key, value);
        }
        case type_Double: {
            return MakeConditionNode<FloatDoubleNode<ArrayDouble, Cond>>::make(column_key, value);
        }
        case type_String: {
            return MakeConditionNode<StringNode<Cond>>::make(column_key, value);
        }
        case type_Binary: {
            return MakeConditionNode<BinaryNode<Cond>>::make(column_key, value);
        }
        case type_Timestamp: {
            return MakeConditionNode<TimestampNode<Cond>>::make(column_key, value);
        }
        case type_Decimal: {
            return MakeConditionNode<DecimalNode<Cond>>::make(column_key, value);
        }
        case type_ObjectId: {
            return MakeConditionNode<ObjectIdNode<Cond>>::make(column_key, value);
        }
        case type_Mixed: {
            return MakeConditionNode<MixedNode<Cond>>::make(column_key, value);
        }
        case type_UUID: {
            return MakeConditionNode<UUIDNode<Cond>>::make(column_key, value);
        }
        case type_Link:
            if constexpr (std::is_same_v<T, Mixed> && realm::is_any_v<Cond, Equal, NotEqual>) {
                ObjKey key;
                if (value.is_type(type_Link)) {
                    key = value.template get<ObjKey>();
                }
                else if (value.is_type(type_TypedLink)) {
                    ObjLink link = value.get_link();
                    auto target_table = table.get_link_target(column_key);
                    if (target_table->get_key() != link.get_table_key()) {
                        // This will never match
                        return std::unique_ptr<ParentNode>{new ExpressionNode(std::make_unique<FalseExpression>())};
                    }
                    key = link.get_obj_key();
                }
                return std::unique_ptr<ParentNode>{new LinksToNode<Cond>(column_key, key)};
            }
            break;
        default:
            break;
    }
    throw_type_mismatch_error();
}

template <class Cond>
std::unique_ptr<ParentNode> make_size_condition_node(const Table& table, ColKey column_key, int64_t value)
{
    table.check_column(column_key);
    DataType type = DataType(column_key.get_type());
    ColumnAttrMask attr = column_key.get_attrs();

    if (attr.test(col_attr_List)) {
        return std::unique_ptr<ParentNode>{new SizeListNode<Cond>(value, column_key)};
    }
    switch (type) {
        case type_String: {
            return std::unique_ptr<ParentNode>{new SizeNode<StringData, Cond>(value, column_key)};
        }
        case type_Binary: {
            return std::unique_ptr<ParentNode>{new SizeNode<BinaryData, Cond>(value, column_key)};
        }
        default: {
            throw_type_mismatch_error();
        }
    }
}

} // anonymous namespace

template <typename TConditionFunction, class T>
REALM_FORCEINLINE Query& Query::add_condition(ColKey column_key, T value)
{
    auto node = make_condition_node<TConditionFunction>(*m_table, column_key, value);
    add_node(std::move(node));
    return *this;
}


template <typename TConditionFunction>
Query& Query::add_size_condition(ColKey column_key, int64_t value)
{
    auto node = make_size_condition_node<TConditionFunction>(*m_table, column_key, value);
    add_node(std::move(node));
    return *this;
}

// Two column methods, any type
Query& Query::equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<Equal>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
Query& Query::less(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<Less>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
Query& Query::less_equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<LessEqual>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
Query& Query::greater(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<Greater>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
Query& Query::greater_equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<GreaterEqual>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
Query& Query::not_equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<NotEqual>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}

// null vs column
Query& Query::equal(ColKey column_key, null)
{
    add_condition<Equal>(column_key, null{});
    return *this;
}

Query& Query::not_equal(ColKey column_key, null)
{
    add_condition<NotEqual>(column_key, null());
    return *this;
}

// int constant vs column (we need those because '1234' is ambiguous, can convert to float/double/int64_t)
Query& Query::equal(ColKey column_key, int value)
{
    return equal(column_key, static_cast<int64_t>(value));
}
Query& Query::not_equal(ColKey column_key, int value)
{
    return not_equal(column_key, static_cast<int64_t>(value));
}
Query& Query::greater(ColKey column_key, int value)
{
    return greater(column_key, static_cast<int64_t>(value));
}
Query& Query::greater_equal(ColKey column_key, int value)
{
    return greater_equal(column_key, static_cast<int64_t>(value));
}
Query& Query::less_equal(ColKey column_key, int value)
{
    return less_equal(column_key, static_cast<int64_t>(value));
}
Query& Query::less(ColKey column_key, int value)
{
    return less(column_key, static_cast<int64_t>(value));
}
Query& Query::between(ColKey column_key, int from, int to)
{
    return between(column_key, static_cast<int64_t>(from), static_cast<int64_t>(to));
}

Query& Query::links_to(ColKey origin_column_key, ObjKey target_key)
{
    add_node(std::unique_ptr<ParentNode>(new LinksToNode<Equal>(origin_column_key, target_key)));
    return *this;
}

Query& Query::links_to(ColKey origin_column_key, ObjLink target_link)
{
    add_condition<Equal>(origin_column_key, Mixed(target_link));
    return *this;
}

Query& Query::links_to(ColKey origin_column, const std::vector<ObjKey>& target_keys)
{
    add_node(std::unique_ptr<ParentNode>(new LinksToNode<Equal>(origin_column, target_keys)));
    return *this;
}

Query& Query::not_links_to(ColKey origin_column_key, const std::vector<ObjKey>& target_keys)
{
    add_node(std::unique_ptr<ParentNode>(new LinksToNode<NotEqual>(origin_column_key, target_keys)));
    return *this;
}

// int64 constant vs column
Query& Query::equal(ColKey column_key, int64_t value)
{
    add_condition<Equal>(column_key, value);
    return *this;
}
Query& Query::not_equal(ColKey column_key, int64_t value)
{
    add_condition<NotEqual>(column_key, value);
    return *this;
}
Query& Query::greater(ColKey column_key, int64_t value)
{
    add_condition<Greater>(column_key, value);
    return *this;
}
Query& Query::greater_equal(ColKey column_key, int64_t value)
{
    if (value > LLONG_MIN) {
        add_condition<Greater>(column_key, value - 1);
    }
    // field >= LLONG_MIN has no effect
    return *this;
}
Query& Query::less_equal(ColKey column_key, int64_t value)
{
    if (value < LLONG_MAX) {
        add_condition<Less>(column_key, value + 1);
    }
    // field <= LLONG_MAX has no effect
    return *this;
}
Query& Query::less(ColKey column_key, int64_t value)
{
    add_condition<Less>(column_key, value);
    return *this;
}
Query& Query::between(ColKey column_key, int64_t from, int64_t to)
{
    if (column_key.is_nullable()) {
        add_node(std::unique_ptr<realm::ParentNode>(new BetweenNode<ArrayIntNull>(from, to, column_key)));
    }
    else {
        add_node(std::unique_ptr<realm::ParentNode>(new BetweenNode<ArrayInteger>(from, to, column_key)));
    }
    return *this;
}
Query& Query::equal(ColKey column_key, bool value)
{
    add_condition<Equal>(column_key, value);
    return *this;
}
Query& Query::not_equal(ColKey column_key, bool value)
{
    add_condition<NotEqual>(column_key, value);
    return *this;
}

// ------------- float
Query& Query::equal(ColKey column_key, float value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, float value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater(ColKey column_key, float value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, float value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, float value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, float value)
{
    return add_condition<Less>(column_key, value);
}
Query& Query::between(ColKey column_key, float from, float to)
{
    group();
    greater_equal(column_key, from);
    less_equal(column_key, to);
    end_group();
    return *this;
}


// ------------- double
Query& Query::equal(ColKey column_key, double value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, double value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater(ColKey column_key, double value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, double value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, double value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, double value)
{
    return add_condition<Less>(column_key, value);
}
Query& Query::between(ColKey column_key, double from, double to)
{
    group();
    greater_equal(column_key, from);
    less_equal(column_key, to);
    end_group();
    return *this;
}


// ------------- Timestamp
Query& Query::greater(ColKey column_key, Timestamp value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::equal(ColKey column_key, Timestamp value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, Timestamp value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, Timestamp value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, Timestamp value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, Timestamp value)
{
    return add_condition<Less>(column_key, value);
}

Query& Query::between(ColKey column_key, Timestamp from, Timestamp to)
{
    add_node(std::unique_ptr<realm::ParentNode>(new BetweenNode<ArrayTimestamp>(from, to, column_key)));
    return *this;
}
// ------------- ObjectId
Query& Query::greater(ColKey column_key, ObjectId value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::equal(ColKey column_key, ObjectId value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, ObjectId value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, ObjectId value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, ObjectId value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, ObjectId value)
{
    return add_condition<Less>(column_key, value);
}

// ------------- UUID
Query& Query::equal(ColKey column_key, UUID value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, UUID value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater(ColKey column_key, UUID value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, UUID value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, UUID value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, UUID value)
{
    return add_condition<Less>(column_key, value);
}

// ------------- Decimal128
Query& Query::greater(ColKey column_key, Decimal128 value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::equal(ColKey column_key, Decimal128 value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, Decimal128 value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, Decimal128 value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, Decimal128 value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, Decimal128 value)
{
    return add_condition<Less>(column_key, value);
}
Query& Query::between(ColKey column_key, Decimal128 from, Decimal128 to)
{
    group();
    greater_equal(column_key, from);
    less_equal(column_key, to);
    end_group();
    return *this;
}

// ------------- Mixed
Query& Query::greater(ColKey column_key, Mixed value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::equal(ColKey column_key, Mixed value, bool case_sensitive)
{
    if (case_sensitive)
        return add_condition<Equal>(column_key, value);
    else
        return add_condition<EqualIns>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, Mixed value, bool case_sensitive)
{
    if (case_sensitive)
        return add_condition<NotEqual>(column_key, value);
    else
        return add_condition<NotEqualIns>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, Mixed value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, Mixed value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, Mixed value)
{
    return add_condition<Less>(column_key, value);
}
Query& Query::begins_with(ColKey column_key, Mixed value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<BeginsWith>(column_key, value);
    else
        add_condition<BeginsWithIns>(column_key, value);
    return *this;
}
Query& Query::ends_with(ColKey column_key, Mixed value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<EndsWith>(column_key, value);
    else
        add_condition<EndsWithIns>(column_key, value);
    return *this;
}
Query& Query::contains(ColKey column_key, Mixed value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Contains>(column_key, value);
    else
        add_condition<ContainsIns>(column_key, value);
    return *this;
}
Query& Query::like(ColKey column_key, Mixed value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Like>(column_key, value);
    else
        add_condition<LikeIns>(column_key, value);
    return *this;
}
Query& Query::in(ColKey column_key, const Mixed* begin, const Mixed* end)
{
    REALM_ASSERT(!column_key.is_collection());
    ColumnType col_type = column_key.get_type();
    std::unique_ptr<ParentNode> node;
    try {
        if (begin == end) {
            node = std::make_unique<ExpressionNode>(std::make_unique<FalseExpression>());
        }
        else if (col_type == col_type_UUID) {
            node = std::make_unique<UUIDNode<Equal>>(column_key, begin, end);
        }
        else if (col_type == col_type_ObjectId) {
            node = std::make_unique<ObjectIdNode<Equal>>(column_key, begin, end);
        }
        else if (col_type == col_type_String) {
            node = std::make_unique<StringNode<Equal>>(column_key, begin, end);
        }
        else if (col_type == col_type_Int) {
            if (column_key.is_nullable()) {
                node = std::make_unique<IntegerNode<ArrayIntNull, Equal>>(column_key, begin, end);
            }
            else {
                node = std::make_unique<IntegerNode<ArrayInteger, Equal>>(column_key, begin, end);
            }
        }
        else {
            // general path for nodes that don't have this optimization yet
            Query cond = this->m_table->where();
            if (col_type == col_type_Mixed) {
                for (const Mixed* it = begin; it != end; ++it) {
                    cond.add_node(make_condition_node<Equal>(*m_table, column_key, *it));
                    cond.Or();
                }
            }
            else {
                for (const Mixed* it = begin; it != end; ++it) {
                    if (it->is_type(DataType(col_type))) {
                        cond.add_node(make_condition_node<Equal>(*m_table, column_key, *it));
                        cond.Or();
                    }
                    else if (it->is_null() && column_key.is_nullable()) {
                        cond.add_node(make_condition_node<Equal>(*m_table, column_key, realm::null()));
                        cond.Or();
                    }
                }
            }
            this->and_query(cond);
            return *this;
        }
    }
    catch (const InvalidArgument&) {
        // if none of the arguments matched the right type we'd end up with an
        // empty condition node which won't evaluate correctly. The right behaviour
        // is to match nothing, so make a false condition
        node = std::make_unique<ExpressionNode>(std::make_unique<FalseExpression>());
    }
    add_node(std::move(node));
    return *this;
}

// ------------- size
Query& Query::size_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<Equal>(column_key, value);
}
Query& Query::size_not_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<NotEqual>(column_key, value);
}
Query& Query::size_greater(ColKey column_key, int64_t value)
{
    return add_size_condition<Greater>(column_key, value);
}
Query& Query::size_greater_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<GreaterEqual>(column_key, value);
}
Query& Query::size_less_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<LessEqual>(column_key, value);
}
Query& Query::size_less(ColKey column_key, int64_t value)
{
    return add_size_condition<Less>(column_key, value);
}
Query& Query::size_between(ColKey column_key, int64_t from, int64_t to)
{
    group();
    size_greater_equal(column_key, from);
    size_less_equal(column_key, to);
    end_group();
    return *this;
}

// Strings, StringData()

Query& Query::equal(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Equal>(column_key, value);
    else
        add_condition<EqualIns>(column_key, value);
    return *this;
}
Query& Query::begins_with(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<BeginsWith>(column_key, value);
    else
        add_condition<BeginsWithIns>(column_key, value);
    return *this;
}
Query& Query::ends_with(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<EndsWith>(column_key, value);
    else
        add_condition<EndsWithIns>(column_key, value);
    return *this;
}
Query& Query::contains(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Contains>(column_key, value);
    else
        add_condition<ContainsIns>(column_key, value);
    return *this;
}
Query& Query::not_equal(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<NotEqual>(column_key, value);
    else
        add_condition<NotEqualIns>(column_key, value);
    return *this;
}
Query& Query::like(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Like>(column_key, value);
    else
        add_condition<LikeIns>(column_key, value);
    return *this;
}


Query& Query::fulltext(ColKey column_key, StringData value)
{
    auto index = m_table->get_string_index(column_key);
    if (!(index && index->is_fulltext_index())) {
        throw IllegalOperation{"Column has no fulltext index"};
    }

    auto node = std::unique_ptr<ParentNode>{new StringNodeFulltext(std::move(value), column_key)};
    add_node(std::move(node));
    return *this;
}

Query& Query::fulltext(ColKey column_key, StringData value, const LinkMap& link_map)
{
    auto index = link_map.get_target_table()->get_string_index(column_key);
    if (!(index && index->is_fulltext_index())) {
        throw IllegalOperation{"Column has no fulltext index"};
    }

    auto lm = std::make_unique<LinkMap>(link_map);
    auto node = std::unique_ptr<ParentNode>{new StringNodeFulltext(std::move(value), column_key, std::move(lm))};
    add_node(std::move(node));
    return *this;
}

Query& Query::greater(ColKey column_key, StringData value)
{
    add_condition<Greater>(column_key, value);
    return *this;
}

Query& Query::greater_equal(ColKey column_key, StringData value)
{
    add_condition<GreaterEqual>(column_key, value);
    return *this;
}

Query& Query::less(ColKey column_key, StringData value)
{
    add_condition<Less>(column_key, value);
    return *this;
}

Query& Query::less_equal(ColKey column_key, StringData value)
{
    add_condition<LessEqual>(column_key, value);
    return *this;
}

// Aggregates =================================================================================

bool Query::eval_object(const Obj& obj) const
{
    if (has_conditions())
        return obj && root_node()->match(obj);

    // Query has no conditions, so all rows match, also the user given argument
    return true;
}


template <typename T>
void Query::aggregate(QueryStateBase& st, ColKey column_key) const
{
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;

    if (!has_conditions() && !m_view) {
        // use table aggregate
        m_table.unchecked_ptr()->aggregate<T>(st, column_key);
    }
    else {

        // Aggregate with criteria - goes through the nodes in the query system
        init();

        if (!m_view) {
            auto pn = root_node();
            auto best = find_best_node(pn);
            auto node = pn->m_children[best];
            if (auto keys = node->index_based_keys()) {
                // The node having the search index can be removed from the query as we know that
                // all the objects will match this condition
                pn->m_children[best] = pn->m_children.back();
                pn->m_children.pop_back();
                const size_t num_keys = keys->size();
                for (size_t i = 0; i < num_keys; ++i) {
                    auto obj = m_table->get_object(keys->get(i));
                    if (pn->m_children.empty() || eval_object(obj)) {
                        st.m_key_offset = obj.get_key().value;
                        st.match(0, obj.get<T>(column_key));
                    }
                }
            }
            else {
                // no index, traverse cluster tree
                node = pn;
                LeafType leaf(m_table.unchecked_ptr()->get_alloc());

                auto f = [column_key, &leaf, &node, &st, this](const Cluster* cluster) {
                    size_t e = cluster->node_size();
                    node->set_cluster(cluster);
                    cluster->init_leaf(column_key, &leaf);
                    st.m_key_offset = cluster->get_offset();
                    st.m_key_values = cluster->get_key_array();
                    aggregate_internal(node, &st, 0, e, &leaf);
                    return IteratorControl::AdvanceToNext;
                };

                m_table.unchecked_ptr()->traverse_clusters(f);
            }
        }
        else {
            m_view->for_each([&](const Obj& obj) {
                if (eval_object(obj)) {
                    st.m_key_offset = obj.get_key().value;
                    st.match(0, obj.get<T>(column_key));
                }
                return IteratorControl::AdvanceToNext;
            });
        }
    }
}

size_t Query::find_best_node(ParentNode* pn) const
{
    auto score_compare = [](const ParentNode* a, const ParentNode* b) {
        return a->cost() < b->cost();
    };
    size_t best = std::distance(pn->m_children.begin(),
                                std::min_element(pn->m_children.begin(), pn->m_children.end(), score_compare));
    return best;
}

/**************************************************************************************************************
 *                                                                                                             *
 * Main entry point of a query. Schedules calls to aggregate_local                                             *
 * Return value is the result of the query, or Array pointer for FindAll.                                      *
 *                                                                                                             *
 **************************************************************************************************************/

void Query::aggregate_internal(ParentNode* pn, QueryStateBase* st, size_t start, size_t end,
                               ArrayPayload* source_column) const
{
    // Number of matches to find in best condition loop before breaking out to probe other conditions. Too low value
    // gives too many constant time overheads everywhere in the query engine. Too high value makes it adapt less
    // rapidly to changes in match frequencies.
    constexpr size_t findlocals = 64;

    // Average match distance in linear searches where further increase in distance no longer increases query speed
    // (because time spent on handling each match becomes insignificant compared to time spent on the search).
    constexpr size_t bestdist = 512;

    // Minimum number of matches required in a certain condition before it can be used to compute statistics. Too high
    // value can spent too much time in a bad node (with high match frequency). Too low value gives inaccurate
    // statistics.
    constexpr size_t probe_matches = 4;

    while (start < end) {
        // Executes start...end range of a query and will stay inside the condition loop of the node it was called
        // on. Can be called on any node; yields same result, but different performance. Returns prematurely if
        // condition of called node has evaluated to true local_matches number of times.
        // Return value is the next row for resuming aggregating (next row that caller must call aggregate_local on)
        size_t best = find_best_node(pn);
        start = pn->m_children[best]->aggregate_local(st, start, end, findlocals, source_column);
        double current_cost = pn->m_children[best]->cost();

        // Make remaining conditions compute their m_dD (statistics)
        for (size_t c = 0; c < pn->m_children.size() && start < end; c++) {
            if (c == best)
                continue;

            // Skip test if there is no way its cost can ever be better than best node's
            if (pn->m_children[c]->m_dT < current_cost) {

                // Limit to bestdist in order not to skip too large parts of index nodes
                size_t maxD = pn->m_children[c]->m_dT == 0.0 ? end - start : bestdist;
                size_t td = pn->m_children[c]->m_dT == 0.0 ? end : (start + maxD > end ? end : start + maxD);
                start = pn->m_children[c]->aggregate_local(st, start, td, probe_matches, source_column);
            }
        }
    }
}

// Aggregates

std::optional<Mixed> Query::sum(ColKey col_key) const
{
    return AggregateHelper<Query>::sum(*m_table, *this, col_key);
}

std::optional<Mixed> Query::avg(ColKey col_key, size_t* value_count) const
{
    return AggregateHelper<Query>::avg(*m_table, *this, col_key, value_count);
}

std::optional<Mixed> Query::min(ColKey col_key, ObjKey* return_ndx) const
{
    return AggregateHelper<Query>::min(*m_table, *this, col_key, return_ndx);
}

std::optional<Mixed> Query::max(ColKey col_key, ObjKey* return_ndx) const
{
    return AggregateHelper<Query>::max(*m_table, *this, col_key, return_ndx);
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
        // nodes into a NotNode (if not empty).
        current_group.m_pending_not = false;
        if (auto not_root_node = std::move(current_group.m_root_node)) {
            add_node(std::make_unique<NotNode>(std::move(not_root_node)));
        }

        end_group();
    }
}

Query& Query::Or()
{
    auto& current_group = m_groups.back();
    if (current_group.m_state != QueryGroup::State::OrConditionChildren) {
        // Reparent the current group's nodes within an OrNode.
        add_node(std::make_unique<OrNode>(std::move(current_group.m_root_node)));
    }
    current_group.m_state = QueryGroup::State::OrCondition;

    return *this;
}


ObjKey Query::find() const
{
    ObjKey ret;

    if (!m_table)
        return ret;

    auto logger = m_table->get_logger();
    bool do_log = false;
    std::chrono::steady_clock::time_point t1;

    if (logger && logger->would_log(util::LogCategory::query, util::Logger::Level::debug)) {
        logger->log(util::LogCategory::query, util::Logger::Level::debug, "Query find first: '%1'",
                    get_description_safe());
        t1 = std::chrono::steady_clock::now();
        do_log = true;
    }

    init();

    // ordering could change the way in which objects are returned, in this case we need to run find_all()
    if (m_ordering && (m_ordering->will_apply_sort() || m_ordering->will_apply_distinct())) {
        auto table_view = find_all();
        if (table_view.size() > 0) {
            // we just need to find the first.
            ret = table_view.get_key(0);
        }
    }
    else if (!has_conditions()) {
        // User created query with no criteria; return first
        if (m_view) {
            if (m_view->size() > 0) {
                ret = m_view->get_key(0);
            }
        }
        else {
            ret = m_table->size() == 0 ? null_key : m_table.unchecked_ptr()->begin()->get_key();
        }
    }
    else {
        if (m_view) {
            size_t sz = m_view->size();
            for (size_t i = 0; i < sz; i++) {
                const Obj obj = m_view->get_object(i);
                if (eval_object(obj)) {
                    ret = obj.get_key();
                    break;
                }
            }
        }
        else {
            auto node = root_node();
            ObjKey key;
            auto f = [&node, &key](const Cluster* cluster) {
                size_t end = cluster->node_size();
                node->set_cluster(cluster);
                size_t res = node->find_first(0, end);
                if (res != not_found) {
                    key = cluster->get_real_key(res);
                    // We should just find one - we're done
                    return IteratorControl::Stop;
                }
                return IteratorControl::AdvanceToNext;
            };

            m_table->traverse_clusters(f);
            ret = key;
        }
    }

    if (do_log) {
        auto t2 = std::chrono::steady_clock::now();
        logger->log(util::LogCategory::query, util::Logger::Level::debug, "Query first found: %1, Duration: %2 us",
                    ret, std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
    }

    return ret;
}

void Query::do_find_all(QueryStateBase& st) const
{
    auto logger = m_table->get_logger();
    std::chrono::steady_clock::time_point t1;
    bool do_log = false;

    if (st.limit() == 0) {
        if (logger) {
            logger->log(util::LogCategory::query, util::Logger::Level::debug,
                        "Query find all: limit = 0 -> result: 0");
        }
        return;
    }

    if (logger && logger->would_log(util::LogCategory::query, util::Logger::Level::debug)) {
        logger->log(util::LogCategory::query, util::Logger::Level::debug, "Query find all: '%1', limit = %2",
                    get_description_safe(), int64_t(st.limit()));
        t1 = std::chrono::steady_clock::now();
        do_log = true;
    }

    init();

    bool has_cond = has_conditions();

    if (m_view) {
        size_t sz = m_view->size();
        for (size_t t = 0; t < sz; t++) {
            const Obj obj = m_view->get_object(t);
            if (eval_object(obj)) {
                st.m_key_offset = obj.get_key().value;
                if (!st.match(0, Mixed()))
                    break;
            }
        }
    }
    else {
        if (!has_cond) {
            auto f = [&st](const Cluster* cluster) {
                size_t sz = cluster->node_size();
                st.m_key_offset = cluster->get_offset();
                st.m_key_values = cluster->get_key_array();
                for (size_t i = 0; i < sz; i++) {
                    if (!st.match(i, Mixed()))
                        return IteratorControl::Stop;
                }
                return IteratorControl::AdvanceToNext;
            };

            m_table->traverse_clusters(f);
        }
        else {
            auto pn = root_node();
            auto best = find_best_node(pn);
            auto node = pn->m_children[best];
            if (auto keys = node->index_based_keys()) {
                // The node having the search index can be removed from the query as we know that
                // all the objects will match this condition
                pn->m_children[best] = pn->m_children.back();
                pn->m_children.pop_back();

                const size_t num_keys = keys->size();
                for (size_t i = 0; i < num_keys; ++i) {
                    ObjKey key = keys->get(i);
                    st.m_key_offset = key.value;
                    if (pn->m_children.empty()) {
                        // No more conditions - just add key
                        if (!st.match(0, Mixed()))
                            break;
                    }
                    else {
                        auto obj = m_table->get_object(key);
                        if (eval_object(obj)) {
                            if (!st.match(0, Mixed()))
                                break;
                        }
                    }
                }
            }
            else {
                // no index on best node (and likely no index at all), descend B+-tree
                node = pn;

                auto f = [&node, &st, this](const Cluster* cluster) {
                    size_t e = cluster->node_size();
                    node->set_cluster(cluster);
                    st.m_key_offset = cluster->get_offset();
                    st.m_key_values = cluster->get_key_array();
                    aggregate_internal(node, &st, 0, e, nullptr);
                    // Stop if limit is reached
                    return st.match_count() == st.limit() ? IteratorControl::Stop : IteratorControl::AdvanceToNext;
                };

                m_table->traverse_clusters(f);
            }
        }
    }


    if (do_log) {
        auto t2 = std::chrono::steady_clock::now();
        logger->log(util::LogCategory::query, util::Logger::Level::debug, "Query found: %1, Duration: %2 us",
                    st.match_count(), std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
    }
}

TableView Query::find_all(size_t limit) const
{
    TableView ret(*this, limit);
    if (m_ordering) {
        // apply_descriptor_ordering will call do_sync
        ret.apply_descriptor_ordering(*m_ordering);
    }
    else {
        ret.do_sync();
    }
    return ret;
}


size_t Query::do_count(size_t limit) const
{
    auto logger = m_table->get_logger();
    std::chrono::steady_clock::time_point t1;
    bool do_log = false;

    if (limit == 0) {
        if (logger) {
            logger->log(util::LogCategory::query, util::Logger::Level::debug, "Query count: limit = 0 -> result: 0");
        }
        return 0;
    }

    if (!has_conditions()) {
        // User created query with no criteria; count all
        size_t cnt_all;
        if (m_view) {
            cnt_all = std::min(m_view->size(), limit);
        }
        else {
            cnt_all = std::min(m_table->size(), limit);
        }
        if (logger) {
            logger->log(util::LogCategory::query, util::Logger::Level::debug,
                        "Query count (no condition): limit = %1 -> result: %2", int64_t(limit), cnt_all);
        }
        return cnt_all;
    }

    if (logger && logger->would_log(util::LogCategory::query, util::Logger::Level::debug)) {
        logger->log(util::LogCategory::query, util::Logger::Level::debug, "Query count: '%1', limit = %2",
                    get_description_safe(), int64_t(limit));
        t1 = std::chrono::steady_clock::now();
        do_log = true;
    }
    size_t cnt = 0;

    init();

    if (m_view) {
        m_view->for_each([&](const Obj& obj) {
            if (eval_object(obj)) {
                cnt++;
            }
            return IteratorControl::AdvanceToNext;
        });
    }
    else {
        auto pn = root_node();
        auto best = find_best_node(pn);
        auto node = pn->m_children[best];
        if (auto keys = node->index_based_keys()) {
            if (pn->m_children.size() > 1) {
                // The node having the search index can be removed from the query as we know that
                // all the objects will match this condition
                pn->m_children[best] = pn->m_children.back();
                pn->m_children.pop_back();
                const size_t num_keys = keys->size();
                for (size_t i = 0; i < num_keys; ++i) {
                    auto obj = m_table->get_object(keys->get(i));
                    if (eval_object(obj)) {
                        ++cnt;
                        if (cnt == limit)
                            break;
                    }
                }
            }
            else {
                // The node having the search index is the only node
                auto sz = keys->size();
                cnt = std::min(limit, sz);
            }
        }
        else {
            // no index, descend down the B+-tree instead
            node = pn;
            QueryStateCount st(limit);

            auto f = [&node, &st, this](const Cluster* cluster) {
                size_t e = cluster->node_size();
                node->set_cluster(cluster);
                st.m_key_offset = cluster->get_offset();
                st.m_key_values = cluster->get_key_array();
                aggregate_internal(node, &st, 0, e, nullptr);
                // Stop if limit or end is reached
                return st.match_count() == st.limit() ? IteratorControl::Stop : IteratorControl::AdvanceToNext;
            };

            m_table->traverse_clusters(f);

            cnt = st.get_count();
        }
    }

    if (do_log) {
        auto t2 = std::chrono::steady_clock::now();
        logger->log(util::LogCategory::query, util::Logger::Level::debug, "Query matches: %1, Duration: %2 us", cnt,
                    std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
    }

    return cnt;
}

size_t Query::count() const
{
    if (!m_table)
        return 0;
    return do_count();
}

TableView Query::find_all(const DescriptorOrdering& descriptor) const
{
    if (descriptor.is_empty()) {
        return find_all();
    }

    const size_t default_limit = size_t(-1);

    bool only_limit = true;
    size_t min_limit = size_t(-1);
    for (size_t i = 0; i < descriptor.size(); ++i) {
        if (descriptor.get_type(i) != DescriptorType::Limit) {
            only_limit = false;
            break;
        }
        else {
            REALM_ASSERT(dynamic_cast<const LimitDescriptor*>(descriptor[i]));
            const LimitDescriptor* limit = static_cast<const LimitDescriptor*>(descriptor[i]);
            min_limit = std::min(min_limit, limit->get_limit());
        }
    }
    if (only_limit) {
        return find_all(min_limit);
    }

    TableView ret(*this, default_limit);
    ret.apply_descriptor_ordering(descriptor);
    return ret;
}

size_t Query::count(const DescriptorOrdering& descriptor) const
{
    if (!m_table)
        return 0;
    realm::util::Optional<size_t> min_limit = descriptor.get_min_limit();

    if (bool(min_limit) && *min_limit == 0)
        return 0;

    size_t limit = size_t(-1);

    if (!descriptor.will_apply_distinct() && !descriptor.will_apply_filter()) {
        if (bool(min_limit)) {
            limit = *min_limit;
        }
        return do_count(limit);
    }

    TableView ret(*this, limit);
    ret.apply_descriptor_ordering(descriptor);
    return ret.size();
}

// todo, not sure if start, end and limit could be useful for delete.
size_t Query::remove() const
{
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

std::string Query::validate() const
{
    if (!m_groups.size())
        return "";

    if (error_code != "") // errors detected by QueryInterface
        return error_code;

    if (!root_node())
        return "Syntax error";

    return root_node()->validate(); // errors detected by QueryEngine
}

std::string Query::get_description(util::serializer::SerialisationState& state) const
{
    std::string description;
    if (auto root = root_node()) {
        description = root->describe_expression(state);
    }
    if (m_view) {
        description += util::format(" VIEW { %1 element(s) }", m_view->size());
    }
    if (description.length() == 0) {
        // An empty query returns all results and one way to indicate this
        // is to serialise TRUEPREDICATE which is functionally equivalent
        description = "TRUEPREDICATE";
    }
    if (m_ordering) {
        description += " " + m_ordering->get_description(m_table);
    }
    return description;
}

Query& Query::set_ordering(util::bind_ptr<DescriptorOrdering> ordering)
{
    m_ordering = std::move(ordering);
    return *this;
}

util::bind_ptr<DescriptorOrdering> Query::get_ordering()
{
    return std::move(m_ordering);
}

std::string Query::get_description() const
{
    util::serializer::SerialisationState state(m_table->get_parent_group());
    return get_description(state);
}

std::string Query::get_description_safe() const noexcept
{
    try {
        util::serializer::SerialisationState state(m_table->get_parent_group());
        return get_description(state);
    }
    catch (const Exception& e) {
        if (auto logger = m_table->get_logger()) {
            logger->log(util::Logger::Level::warn, "Query::get_description() failed: '%1'", e.what());
        }
    }
    return "Unknown Query";
}

void Query::init() const
{
    m_table.check();
    if (ParentNode* root = root_node()) {
        root->init(m_view == nullptr);
        std::vector<ParentNode*> vec;
        root->gather_children(vec);
    }
}

size_t Query::find_internal(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_table.unchecked_ptr()->size();
    if (start == end)
        return not_found;

    size_t r;
    if (ParentNode* root = root_node())
        r = root->find_first(start, end);
    else
        r = start; // user built an empty query; return any first

    if (r == m_table.unchecked_ptr()->size())
        return not_found;
    else
        return r;
}

void Query::add_node(std::unique_ptr<ParentNode> node)
{
    REALM_ASSERT(node);
    using State = QueryGroup::State;

    if (m_table)
        node->set_table(m_table);

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

        if (q.m_source_collection) {
            REALM_ASSERT(!m_source_collection || (m_source_collection->matches(*q.m_source_collection)));
            m_source_collection = std::move(q.m_source_collection);
            m_view = m_source_collection.get();
        }
    }

    return *this;
}

Query Query::operator||(const Query& q)
{
    Query q2(m_table);
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

    Query q2(m_table);
    q2.and_query(*this);
    q2.and_query(q);

    return q2;
}


Query Query::operator!()
{
    if (!root_node())
        throw Exception(ErrorCodes::InvalidQuery, "negation of empty query is not supported");
    Query q(m_table);
    q.Not();
    q.and_query(*this);
    return q;
}

void Query::get_outside_versions(TableVersions& versions) const
{
    if (m_table) {
        if (m_table_keys.empty()) {
            // Store primary table info
            m_table_keys.push_back(m_table.unchecked_ptr()->get_key());

            if (ParentNode* root = root_node())
                root->get_link_dependencies(m_table_keys);
        }
        versions.emplace_back(m_table.unchecked_ptr()->get_key(), m_table.unchecked_ptr()->get_content_version());

        if (Group* g = m_table.unchecked_ptr()->get_parent_group()) {
            // update table versions for linked tables - first entry is primary table - skip it
            auto end = m_table_keys.end();
            auto it = m_table_keys.begin() + 1;
            while (it != end) {
                versions.emplace_back(*it, g->get_table(*it)->get_content_version());
                ++it;
            }
        }
        if (m_view) {
            m_view->get_dependencies(versions);
        }
    }
}

TableVersions Query::sync_view_if_needed() const
{
    if (m_view) {
        m_view->sync_if_needed();
    }
    TableVersions ret;
    get_outside_versions(ret);
    return ret;
}

QueryGroup::QueryGroup(const QueryGroup& other)
    : m_root_node(other.m_root_node ? other.m_root_node->clone() : nullptr)
    , m_pending_not(other.m_pending_not)
    , m_state(other.m_state)
{
}

QueryGroup& QueryGroup::operator=(const QueryGroup& other)
{
    if (this != &other) {
        m_root_node = other.m_root_node ? other.m_root_node->clone() : nullptr;
        m_pending_not = other.m_pending_not;
    }
    return *this;
}
