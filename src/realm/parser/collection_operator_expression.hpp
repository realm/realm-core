////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_COLLECTION_OPERATOR_EXPRESSION_HPP
#define REALM_COLLECTION_OPERATOR_EXPRESSION_HPP

#include "primitive_list_expression.hpp"
#include "property_expression.hpp"
#include "parser.hpp"
#include "parser_utils.hpp"

#include <utility>

#include <realm/query_expression.hpp>

namespace realm {
namespace parser {

template <typename RetType, parser::Expression::KeyPathOp AggOpType, typename ExpressionType, class Enable = void>
struct CollectionOperatorGetter;

template <parser::Expression::KeyPathOp OpType, typename ExpressionType>
struct CollectionOperatorExpression;

template <parser::Expression::KeyPathOp OpType>
void do_init(CollectionOperatorExpression<OpType, PropertyExpression>& expression, std::string suffix_path,
             parser::KeyPathMapping& mapping);

template <parser::Expression::KeyPathOp OpType>
void do_init(CollectionOperatorExpression<OpType, PrimitiveListExpression>& expression, std::string suffix_path,
             parser::KeyPathMapping& mapping);

template <parser::Expression::KeyPathOp OpType, typename ExpressionType>
struct CollectionOperatorExpression {
    static constexpr parser::Expression::KeyPathOp operation_type = OpType;
    std::function<LinkChain()> link_chain_getter;
    ExpressionType pe;
    ColKey operative_col_key;
    DataType operative_col_type;

    CollectionOperatorExpression(ExpressionType&& exp, std::string suffix_path, parser::KeyPathMapping& mapping)
        : pe(exp)
    {
        link_chain_getter = std::bind(&ExpressionType::link_chain_getter, pe);
        do_init(*this, suffix_path, mapping);
    }

    template <typename T>
    auto value_of_type_for_query() const
    {
        return CollectionOperatorGetter<T, OpType, ExpressionType>::convert(*this);
    }
};

// Certain operations are disabled for some types (eg. a sum of timestamps is invalid).
// The operations that are supported have a specialisation with std::enable_if for that type below
// any type/operation combination that is not specialised will get the runtime error from the following
// default implementation. The return type is just a dummy to make things compile.
template <typename RetType, parser::Expression::KeyPathOp AggOpType, typename ExpressionType, class Enable>
struct CollectionOperatorGetter {
    static Columns<RetType> convert(const CollectionOperatorExpression<AggOpType, ExpressionType>& op)
    {
        throw std::runtime_error(
            util::format("Predicate error: comparison of type '%1' with result of '%2' is not supported.",
                         util::type_to_str<RetType>(), util::collection_operator_to_str(op.operation_type)));
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Min, PropertyExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static SubColumnAggregate<RetType, aggregate_operations::Minimum<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PropertyExpression>& expr)
    {
        if (expr.pe.dest_type_is_backlink()) {
            return expr.link_chain_getter()
                .template column<Link>(*expr.pe.get_dest_table(), expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .min();
        }
        else {
            return expr.link_chain_getter()
                .template column<Link>(expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .min();
        }
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Min, PrimitiveListExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static ListColumnAggregate<RetType, aggregate_operations::Minimum<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PrimitiveListExpression>& expr)
    {
        return expr.link_chain_getter().template column<Lst<RetType>>(expr.pe.get_dest_col_key()).min();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Max, PropertyExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static SubColumnAggregate<RetType, aggregate_operations::Maximum<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PropertyExpression>& expr)
    {
        if (expr.pe.dest_type_is_backlink()) {
            return expr.link_chain_getter()
                .template column<Link>(*expr.pe.get_dest_table(), expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .max();
        }
        else {
            return expr.link_chain_getter()
                .template column<Link>(expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .max();
        }
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Max, PrimitiveListExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static ListColumnAggregate<RetType, aggregate_operations::Maximum<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PrimitiveListExpression>& expr)
    {
        return expr.link_chain_getter().template column<Lst<RetType>>(expr.pe.get_dest_col_key()).max();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Sum, PropertyExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static SubColumnAggregate<RetType, aggregate_operations::Sum<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PropertyExpression>& expr)
    {
        if (expr.pe.dest_type_is_backlink()) {
            return expr.link_chain_getter()
                .template column<Link>(*expr.pe.get_dest_table(), expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .sum();
        }
        else {
            return expr.link_chain_getter()
                .template column<Link>(expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .sum();
        }
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Sum, PrimitiveListExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static ListColumnAggregate<RetType, aggregate_operations::Sum<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PrimitiveListExpression>& expr)
    {
        return expr.link_chain_getter().template column<Lst<RetType>>(expr.pe.get_dest_col_key()).sum();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Avg, PropertyExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static SubColumnAggregate<RetType, aggregate_operations::Average<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PropertyExpression>& expr)
    {
        if (expr.pe.dest_type_is_backlink()) {
            return expr.link_chain_getter()
                .template column<Link>(*expr.pe.get_dest_table(), expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .average();
        }
        else {
            return expr.link_chain_getter()
                .template column<Link>(expr.pe.get_dest_col_key())
                .template column<RetType>(expr.operative_col_key)
                .average();
        }
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Avg, PrimitiveListExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static ListColumnAggregate<RetType, aggregate_operations::Average<RetType>>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PrimitiveListExpression>& expr)
    {
        return expr.link_chain_getter().template column<Lst<RetType>>(expr.pe.get_dest_col_key()).average();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Count, PropertyExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static LinkCount
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PropertyExpression>& expr)
    {
        if (expr.pe.dest_type_is_backlink()) {
            return expr.link_chain_getter()
                .template column<Link>(*expr.pe.get_dest_table(), expr.pe.get_dest_col_key())
                .count();
        }
        else {
            return expr.link_chain_getter().template column<Link>(expr.pe.get_dest_col_key()).count();
        }
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::Count, PrimitiveListExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static SizeOperator<int64_t>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PrimitiveListExpression>& expr)
    {
        return expr.pe.size_of_list<SizeOperator<int64_t>>();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<
    RetType, parser::Expression::KeyPathOp::BacklinkCount, PropertyExpression,
    typename std::enable_if_t<realm::is_any<RetType, Int, Float, Double, Decimal128>::value>> {
    static BacklinkCount<Int> convert(
        const CollectionOperatorExpression<parser::Expression::KeyPathOp::BacklinkCount, PropertyExpression>& expr)
    {
        if (expr.pe.link_chain.empty() || expr.pe.get_dest_col_key() == ColKey()) {
            // here we are operating on the current table from a "@links.@count" query with no link keypath prefix
            return expr.link_chain_getter().template get_backlink_count<Int>();
        }
        else {
            if (expr.pe.dest_type_is_backlink()) {
                return expr.link_chain_getter()
                    .template column<Link>(*expr.pe.get_dest_table(), expr.pe.get_dest_col_key())
                    .template backlink_count<Int>();
            }
            else {
                return expr.link_chain_getter()
                    .template column<Link>(expr.pe.get_dest_col_key())
                    .template backlink_count<Int>();
            }
        }
    }
};

template <>
struct CollectionOperatorGetter<Int, parser::Expression::KeyPathOp::SizeString, PropertyExpression> {
    static SizeOperator<String>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PropertyExpression>& expr)
    {
        return expr.link_chain_getter().template column<String>(expr.pe.get_dest_col_key()).size();
    }
};

template <>
struct CollectionOperatorGetter<Int, parser::Expression::KeyPathOp::SizeBinary, PropertyExpression> {
    static SizeOperator<Binary>
    convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PropertyExpression>& expr)
    {
        return expr.link_chain_getter().template column<Binary>(expr.pe.get_dest_col_key()).size();
    }
};

template <>
struct CollectionOperatorGetter<Int, parser::Expression::KeyPathOp::SizeString, PrimitiveListExpression> {
    static ColumnListElementLength<String> convert(
        const CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PrimitiveListExpression>& expr)
    {
        return expr.link_chain_getter().template column<Lst<String>>(expr.pe.get_dest_col_key()).element_lengths();
    }
};

template <>
struct CollectionOperatorGetter<Int, parser::Expression::KeyPathOp::SizeBinary, PrimitiveListExpression> {
    static ColumnListElementLength<Binary> convert(
        const CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PrimitiveListExpression>& expr)
    {
        return expr.link_chain_getter().template column<Lst<Binary>>(expr.pe.get_dest_col_key()).element_lengths();
    }
};

template <parser::Expression::KeyPathOp OpType>
void do_init(CollectionOperatorExpression<OpType, PropertyExpression>& expression, std::string suffix_path,
             parser::KeyPathMapping& mapping)
{
    using namespace util;

    const bool requires_suffix_path = !(
        OpType == parser::Expression::KeyPathOp::SizeString || OpType == parser::Expression::KeyPathOp::SizeBinary ||
        OpType == parser::Expression::KeyPathOp::Count || OpType == parser::Expression::KeyPathOp::BacklinkCount);

    if (requires_suffix_path) {
        const Table* pre_link_table = expression.pe.link_chain_getter().get_base_table();
        REALM_ASSERT(pre_link_table);
        StringData list_property_name;
        if (expression.pe.dest_type_is_backlink()) {
            list_property_name = "linking object";
        }
        else {
            list_property_name = pre_link_table->get_column_name(expression.pe.get_dest_col_key());
        }
        realm_precondition(expression.pe.get_dest_type() == type_LinkList || expression.pe.dest_type_is_backlink(),
                           util::format("The '%1' operation must be used on a list property, but '%2' is not a list",
                                        util::collection_operator_to_str(OpType), list_property_name));

        ConstTableRef post_link_table;
        if (expression.pe.dest_type_is_backlink()) {
            post_link_table = expression.pe.get_dest_table();
        }
        else {
            post_link_table = expression.pe.get_dest_table()->get_link_target(expression.pe.get_dest_col_key());
        }
        REALM_ASSERT(post_link_table);
        StringData printable_post_link_table_name = util::get_printable_table_name(*post_link_table);

        KeyPath suffix_key_path = key_path_from_string(suffix_path);

        realm_precondition(suffix_path.size() > 0 && suffix_key_path.size() > 0,
                           util::format("A property from object '%1' must be provided to perform operation '%2'",
                                        printable_post_link_table_name, util::collection_operator_to_str(OpType)));
        size_t index = 0;
        KeyPathElement element = mapping.process_next_path(post_link_table, suffix_key_path, index);

        realm_precondition(
            suffix_key_path.size() == 1,
            util::format("Unable to use '%1' because collection aggreate operations are only supported "
                         "for direct properties at this time",
                         suffix_path));

        expression.operative_col_key = element.col_key;
        expression.operative_col_type = DataType(element.col_key.get_type());
    }
    else { // !requires_suffix_path
        if (!expression.pe.link_chain.empty()) {
            expression.operative_col_type = expression.pe.get_dest_type();
        }

        realm_precondition(suffix_path.empty(),
                           util::format("An extraneous property '%1' was found for operation '%2'", suffix_path,
                                        util::collection_operator_to_str(OpType)));
    }
}

template <parser::Expression::KeyPathOp OpType>
void do_init(CollectionOperatorExpression<OpType, PrimitiveListExpression>& expression, std::string suffix_path,
             parser::KeyPathMapping&)
{
    realm_precondition(suffix_path.empty(), util::format("An extraneous property '%1' was found for operation '%2' "
                                                         "when applied to a list of primitive values '%3'",
                                                         suffix_path, util::collection_operator_to_str(OpType),
                                                         expression.pe.get_dest_table()->get_column_name(
                                                             expression.pe.get_dest_col_key())));

    expression.operative_col_type = expression.pe.get_dest_type();
    expression.operative_col_key = expression.pe.get_dest_col_key();
}

} // namespace parser
} // namespace realm

#endif // REALM_COLLECTION_OPERATOR_EXPRESSION_HPP
