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

#include <vector>
#include <string>

#include "expression_container.hpp"

namespace realm {
namespace parser {

using namespace util;

ExpressionComparisonType convert(parser::Expression::ComparisonType type)
{
    switch (type) {
        case Expression::ComparisonType::Unspecified:
            REALM_FALLTHROUGH;
        case Expression::ComparisonType::Any:
            return ExpressionComparisonType::Any;
        case Expression::ComparisonType::All:
            return ExpressionComparisonType::All;
        case Expression::ComparisonType::None:
            return ExpressionComparisonType::None;
    }
    REALM_UNREACHABLE();
}

ExpressionContainer::ExpressionContainer(Query& query, const parser::Expression& e, query_builder::Arguments& args,
                                         parser::KeyPathMapping& mapping)
{
    if (e.type == parser::Expression::Type::KeyPath) {
        std::vector<KeyPathElement> link_chain = parser::generate_link_chain_from_string(query, e.s, mapping);
        if (link_chain.size() > 0 && link_chain.back().is_list_of_primitives()) {
            parser::Expression::KeyPathOp collection_op = e.collection_op;
            if (link_chain.back().operation == KeyPathElement::KeyPathOperation::ListOfPrimitivesElementLength) {
                realm_precondition(
                    collection_op == parser::Expression::KeyPathOp::None,
                    util::format("Invalid combination of aggregate operation '%1' with list of primitives '.length'",
                                 collection_operator_to_str(e.collection_op)));
                if (link_chain.back().col_key.get_type() == col_type_String) {
                    collection_op = parser::Expression::KeyPathOp::SizeString;
                }
                else if (link_chain.back().col_key.get_type() == col_type_Binary) {
                    collection_op = parser::Expression::KeyPathOp::SizeBinary;
                }
                else {
                    REALM_UNREACHABLE();
                }
            }
            PrimitiveListExpression ple(query, std::move(link_chain), convert(e.comparison_type));
            switch (collection_op) {
                case parser::Expression::KeyPathOp::Min:
                    type = ExpressionInternal::exp_OpMinPrimitive;
                    storage =
                        CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PrimitiveListExpression>(
                            std::move(ple), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Max:
                    type = ExpressionInternal::exp_OpMaxPrimitive;
                    storage =
                        CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PrimitiveListExpression>(
                            std::move(ple), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Sum:
                    type = ExpressionInternal::exp_OpSumPrimitive;
                    storage =
                        CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PrimitiveListExpression>(
                            std::move(ple), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Avg:
                    type = ExpressionInternal::exp_OpAvgPrimitive;
                    storage =
                        CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PrimitiveListExpression>(
                            std::move(ple), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::SizeString:
                    type = ExpressionInternal::exp_OpSizeStringPrimitive;
                    storage =
                        CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString,
                                                     PrimitiveListExpression>(std::move(ple), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::SizeBinary:
                    type = ExpressionInternal::exp_OpSizeBinaryPrimitive;
                    storage =
                        CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary,
                                                     PrimitiveListExpression>(std::move(ple), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Count:
                    type = ExpressionInternal::exp_OpCountPrimitive;
                    storage =
                        CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PrimitiveListExpression>(
                            std::move(ple), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::None:
                    type = ExpressionInternal::exp_PrimitiveList;
                    storage = std::move(ple);
                    break;
                default:
                    throw std::runtime_error(util::format(
                        "Invalid query: '%1' is not a valid operator for a list of primitives property '%2'",
                        e.op_suffix, e.s));
            }
        }
        else {
            PropertyExpression pe(query, std::move(link_chain), convert(e.comparison_type));
            switch (e.collection_op) {
                case parser::Expression::KeyPathOp::Min:
                    type = ExpressionInternal::exp_OpMin;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PropertyExpression>(
                        std::move(pe), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Max:
                    type = ExpressionInternal::exp_OpMax;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PropertyExpression>(
                        std::move(pe), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Sum:
                    type = ExpressionInternal::exp_OpSum;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PropertyExpression>(
                        std::move(pe), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Avg:
                    type = ExpressionInternal::exp_OpAvg;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PropertyExpression>(
                        std::move(pe), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::BacklinkCount:
                    type = ExpressionInternal::exp_OpBacklinkCount;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::BacklinkCount,
                                                           PropertyExpression>(std::move(pe), e.op_suffix, mapping);
                    break;
                case parser::Expression::KeyPathOp::Count:
                    REALM_FALLTHROUGH;
                case parser::Expression::KeyPathOp::SizeString:
                    REALM_FALLTHROUGH;
                case parser::Expression::KeyPathOp::SizeBinary:
                    if (pe.get_dest_type() == type_LinkList || pe.get_dest_type() == type_Link) {
                        type = ExpressionInternal::exp_OpCount;
                        storage =
                            CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PropertyExpression>(
                                std::move(pe), e.op_suffix, mapping);
                    }
                    else if (pe.get_dest_type() == type_String) {
                        type = ExpressionInternal::exp_OpSizeString;
                        storage =
                            CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString,
                                                         PropertyExpression>(std::move(pe), e.op_suffix, mapping);
                    }
                    else if (pe.get_dest_type() == type_Binary) {
                        type = ExpressionInternal::exp_OpSizeBinary;
                        storage =
                            CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary,
                                                         PropertyExpression>(std::move(pe), e.op_suffix, mapping);
                    }
                    else {
                        throw std::runtime_error(
                            "Invalid query: @size and @count can only operate on types list, binary, or string");
                    }
                    break;
                case parser::Expression::KeyPathOp::None:
                    type = ExpressionInternal::exp_Property;
                    storage = std::move(pe);
                    break;
            }
        }
    }
    else if (e.type == parser::Expression::Type::SubQuery) {
        REALM_ASSERT_DEBUG(e.subquery);
        type = ExpressionInternal::exp_SubQuery;
        SubqueryExpression exp(query, e.subquery_path, e.subquery_var, mapping);
        // The least invasive way to do the variable substituion is to simply remove the variable prefix
        // from all query keypaths. This only works because core does not support anything else (such as referencing
        // other properties of the parent table).
        // This means that every keypath must start with the variable, we require it to be there and remove it.
        bool did_add = mapping.add_mapping(exp.get_subquery().get_table(), e.subquery_var, "");
        realm_precondition(did_add, util::format("Unable to create a subquery expression with variable '%1' since an "
                                                 "identical variable already exists in this context",
                                                 e.subquery_var));
        query_builder::apply_predicate(exp.get_subquery(), *e.subquery, args, mapping);
        mapping.remove_mapping(exp.get_subquery().get_table(), e.subquery_var);
        storage = std::move(exp);
    }
    else {
        type = ExpressionInternal::exp_Value;
        storage = ValueExpression(&args, &e);
    }
}

PropertyExpression& ExpressionContainer::get_property()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_Property);
    return util::any_cast<PropertyExpression&>(storage);
}

PrimitiveListExpression& ExpressionContainer::get_primitive_list()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_PrimitiveList);
    return util::any_cast<PrimitiveListExpression&>(storage);
}

ValueExpression& ExpressionContainer::get_value()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_Value);
    return util::any_cast<ValueExpression&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PropertyExpression>& ExpressionContainer::get_min()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMin);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PropertyExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PropertyExpression>& ExpressionContainer::get_max()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMax);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PropertyExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PropertyExpression>& ExpressionContainer::get_sum()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSum);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PropertyExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PropertyExpression>& ExpressionContainer::get_avg()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpAvg);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PropertyExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PropertyExpression>&
ExpressionContainer::get_count()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpCount);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PropertyExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PrimitiveListExpression>&
ExpressionContainer::get_primitive_min()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMinPrimitive);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PrimitiveListExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PrimitiveListExpression>&
ExpressionContainer::get_primitive_max()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMaxPrimitive);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PrimitiveListExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PrimitiveListExpression>&
ExpressionContainer::get_primitive_sum()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSumPrimitive);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PrimitiveListExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PrimitiveListExpression>&
ExpressionContainer::get_primitive_avg()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpAvgPrimitive);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PrimitiveListExpression>&>(
        storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PrimitiveListExpression>&
ExpressionContainer::get_primitive_count()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpCountPrimitive);
    return util::any_cast<
        CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PrimitiveListExpression>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PrimitiveListExpression>&
ExpressionContainer::get_primitive_string_length()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeStringPrimitive);
    return util::any_cast<
        CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PrimitiveListExpression>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PrimitiveListExpression>&
ExpressionContainer::get_primitive_binary_length()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeBinaryPrimitive);
    return util::any_cast<
        CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PrimitiveListExpression>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::BacklinkCount, PropertyExpression>&
ExpressionContainer::get_backlink_count()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpBacklinkCount);
    return util::any_cast<
        CollectionOperatorExpression<parser::Expression::KeyPathOp::BacklinkCount, PropertyExpression>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PropertyExpression>&
ExpressionContainer::get_size_string()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeString);
    return util::any_cast<
        CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PropertyExpression>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PropertyExpression>&
ExpressionContainer::get_size_binary()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeBinary);
    return util::any_cast<
        CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PropertyExpression>&>(storage);
}

SubqueryExpression& ExpressionContainer::get_subexpression()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_SubQuery);
    return util::any_cast<SubqueryExpression&>(storage);
}

DataType ExpressionContainer::check_type_compatibility(DataType other_type)
{
    util::Optional<DataType> self_type;
    switch (type) {
        case ExpressionInternal::exp_Value:
            self_type = other_type; // we'll try to parse the value as other_type and fail there if not possible
            break;
        case ExpressionInternal::exp_Property:
            self_type = get_property().get_dest_type(); // must match
            break;
        case ExpressionInternal::exp_OpMin:
            self_type = get_min().operative_col_type;
            break;
        case ExpressionInternal::exp_OpMax:
            self_type = get_max().operative_col_type;
            break;
        case ExpressionInternal::exp_OpSum:
            self_type = get_sum().operative_col_type;
            break;
        case ExpressionInternal::exp_OpAvg:
            self_type = get_avg().operative_col_type;
            break;
        case ExpressionInternal::exp_PrimitiveList:
            self_type = get_primitive_list().get_dest_type();
            break;
        case ExpressionInternal::exp_OpMinPrimitive:
            self_type = get_primitive_min().operative_col_type;
            break;
        case ExpressionInternal::exp_OpMaxPrimitive:
            self_type = get_primitive_max().operative_col_type;
            break;
        case ExpressionInternal::exp_OpSumPrimitive:
            self_type = get_primitive_sum().operative_col_type;
            break;
        case ExpressionInternal::exp_OpAvgPrimitive:
            self_type = get_primitive_avg().operative_col_type;
            break;
        case ExpressionInternal::exp_OpCountPrimitive:
            REALM_FALLTHROUGH;
        case ExpressionInternal::exp_SubQuery:
            REALM_FALLTHROUGH;
        case ExpressionInternal::exp_OpBacklinkCount:
            REALM_FALLTHROUGH;
        case ExpressionInternal::exp_OpCount:
            // linklist count can handle any numeric type
            if (other_type == type_Int || other_type == type_Double || other_type == type_Float) {
                self_type = other_type;
            } // else other_type is unset
            break;
        case ExpressionInternal::exp_OpSizeStringPrimitive:
            REALM_FALLTHROUGH;
        case ExpressionInternal::exp_OpSizeBinaryPrimitive:
            REALM_FALLTHROUGH;
        case ExpressionInternal::exp_OpSizeString:
            REALM_FALLTHROUGH;
        case ExpressionInternal::exp_OpSizeBinary:
            self_type = type_Int; // count/size on string or binary must be an integer due to a core limitation.
            break;
    }
    if (!self_type) {
        throw std::runtime_error(
                util::format("The result of a @count or @size operation must be compaired to a numeric type (found type '%1').",
                        data_type_to_str(other_type)));
    }
    else if (self_type.value() != other_type) {
        throw std::runtime_error(
                                 util::format("Comparison between properties of different types is not supported ('%1' and '%2').",
                                              data_type_to_str(other_type), data_type_to_str(self_type.value())));
    }
    return other_type;
}

bool is_count_type(ExpressionContainer::ExpressionInternal exp_type)
{
    return exp_type == ExpressionContainer::ExpressionInternal::exp_OpCount ||
           exp_type == ExpressionContainer::ExpressionInternal::exp_OpCountPrimitive ||
           exp_type == ExpressionContainer::ExpressionInternal::exp_OpBacklinkCount ||
           exp_type == ExpressionContainer::ExpressionInternal::exp_OpSizeString ||
           exp_type == ExpressionContainer::ExpressionInternal::exp_OpSizeBinary ||
           exp_type == ExpressionContainer::ExpressionInternal::exp_SubQuery ||
           exp_type == ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive ||
           exp_type == ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive;
}

Optional<DataType> primitive_list_property_type(ExpressionContainer& exp)
{
    using ExpType = ExpressionContainer::ExpressionInternal;
    if (exp.type == ExpType::exp_PrimitiveList) {
        return exp.get_primitive_list().get_dest_type();
    }
    else if (exp.type == ExpType::exp_OpMinPrimitive) {
        return exp.get_primitive_min().operative_col_type;
    }
    else if (exp.type == ExpType::exp_OpMaxPrimitive) {
        return exp.get_primitive_max().operative_col_type;
    }
    else if (exp.type == ExpType::exp_OpSumPrimitive) {
        return exp.get_primitive_sum().operative_col_type;
    }
    else if (exp.type == ExpType::exp_OpAvgPrimitive) {
        return exp.get_primitive_avg().operative_col_type;
    }
    return util::none;
}

DataType ExpressionContainer::get_comparison_type(ExpressionContainer& rhs) {
    // check for strongly typed expressions first
    if (type == ExpressionInternal::exp_Property) {
        return rhs.check_type_compatibility(get_property().get_dest_type());
    } else if (rhs.type == ExpressionInternal::exp_Property) {
        return check_type_compatibility(rhs.get_property().get_dest_type());
    }
    else if (auto primitive_type_lhs = primitive_list_property_type(*this)) {
        return rhs.check_type_compatibility(*primitive_type_lhs);
    }
    else if (auto primitive_type_rhs = primitive_list_property_type(rhs)) {
        return check_type_compatibility(*primitive_type_rhs);
    }
    else if (type == ExpressionInternal::exp_OpMin) {
        return rhs.check_type_compatibility(get_min().operative_col_type);
    }
    else if (type == ExpressionInternal::exp_OpMax) {
        return rhs.check_type_compatibility(get_max().operative_col_type);
    }
    else if (type == ExpressionInternal::exp_OpSum) {
        return rhs.check_type_compatibility(get_sum().operative_col_type);
    }
    else if (type == ExpressionInternal::exp_OpAvg) {
        return rhs.check_type_compatibility(get_avg().operative_col_type);
    }
    else if (rhs.type == ExpressionInternal::exp_OpMin) {
        return check_type_compatibility(rhs.get_min().operative_col_type);
    }
    else if (rhs.type == ExpressionInternal::exp_OpMax) {
        return check_type_compatibility(rhs.get_max().operative_col_type);
    }
    else if (rhs.type == ExpressionInternal::exp_OpSum) {
        return check_type_compatibility(rhs.get_sum().operative_col_type);
    }
    else if (rhs.type == ExpressionInternal::exp_OpAvg) {
        return check_type_compatibility(rhs.get_avg().operative_col_type);
    }
    else if (is_count_type(type) && is_count_type(rhs.type)) {
        return type_Int;
        // check weakly typed expressions last, we return type_Int for count/size because at this point the
        // comparison is between a @count/@size and a value which is untyped. The value should be numeric if the query
        // is well formed but we don't know what type it actually is so we will perform int promotion in a conversion.
    }
    else if (is_count_type(type) || is_count_type(rhs.type)) {
        return type_Int;
    }

    throw std::runtime_error(
        "Unsupported query (type undeductable). A comparison must include at least one keypath.");
}

bool ExpressionContainer::is_null() {
    if (type == ExpressionInternal::exp_Value) {
        return get_value().is_null();
    }
    return false;
}

std::vector<KeyPathElement> ExpressionContainer::get_keypaths()
{
    std::vector<KeyPathElement> links;
    switch (type) {
        case ExpressionInternal::exp_Value:
            return {};
        case ExpressionInternal::exp_Property:
            return get_property().link_chain;
        case ExpressionInternal::exp_PrimitiveList:
            return get_primitive_list().link_chain;
        case ExpressionInternal::exp_OpMin:
            return get_min().pe.link_chain;
        case ExpressionInternal::exp_OpMax:
            return get_max().pe.link_chain;
        case ExpressionInternal::exp_OpSum:
            return get_sum().pe.link_chain;
        case ExpressionInternal::exp_OpAvg:
            return get_avg().pe.link_chain;
        case ExpressionInternal::exp_OpCount:
            return get_count().pe.link_chain;
        case ExpressionInternal::exp_OpMinPrimitive:
            return get_primitive_min().pe.link_chain;
        case ExpressionInternal::exp_OpMaxPrimitive:
            return get_primitive_max().pe.link_chain;
        case ExpressionInternal::exp_OpSumPrimitive:
            return get_primitive_sum().pe.link_chain;
        case ExpressionInternal::exp_OpAvgPrimitive:
            return get_primitive_avg().pe.link_chain;
        case ExpressionInternal::exp_OpCountPrimitive:
            return get_primitive_count().pe.link_chain;
        case ExpressionInternal::exp_OpSizeStringPrimitive:
            return get_primitive_string_length().pe.link_chain;
        case ExpressionInternal::exp_OpSizeBinaryPrimitive:
            return get_primitive_binary_length().pe.link_chain;
        case ExpressionInternal::exp_OpSizeString:
            return get_size_string().pe.link_chain;
        case ExpressionInternal::exp_OpSizeBinary:
            return get_size_binary().pe.link_chain;
        case ExpressionInternal::exp_OpBacklinkCount:
            return get_backlink_count().pe.link_chain;
        case ExpressionInternal::exp_SubQuery:
            return get_subexpression().link_chain;
    }
    return {};
}


} // namespace parser
} // namespace realm
