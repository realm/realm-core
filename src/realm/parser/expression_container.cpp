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

ExpressionContainer::ExpressionContainer(Query& query, const parser::Expression& e, query_builder::Arguments& args, parser::KeyPathMapping& mapping)
{
    if (e.type == parser::Expression::Type::KeyPath) {
        PropertyExpression pe(query, e.s, mapping);
        switch (e.collection_op) {
            case parser::Expression::KeyPathOp::Min:
                type = ExpressionInternal::exp_OpMin;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>(std::move(pe), e.op_suffix, mapping);
                break;
            case parser::Expression::KeyPathOp::Max:
                type = ExpressionInternal::exp_OpMax;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>(std::move(pe), e.op_suffix, mapping);
                break;
            case parser::Expression::KeyPathOp::Sum:
                type = ExpressionInternal::exp_OpSum;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>(std::move(pe), e.op_suffix, mapping);
                break;
            case parser::Expression::KeyPathOp::Avg:
                type = ExpressionInternal::exp_OpAvg;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>(std::move(pe), e.op_suffix, mapping);
                break;
            case parser::Expression::KeyPathOp::Count:
                REALM_FALLTHROUGH;
            case parser::Expression::KeyPathOp::SizeString:
                REALM_FALLTHROUGH;
            case parser::Expression::KeyPathOp::SizeBinary:
                if (pe.get_dest_type() == type_LinkList || pe.get_dest_type() == type_Link) {
                    type = ExpressionInternal::exp_OpCount;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>(std::move(pe), e.op_suffix, mapping);
                }
                else if (pe.get_dest_type() == type_String) {
                    type = ExpressionInternal::exp_OpSizeString;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>(std::move(pe), e.op_suffix, mapping);
                }
                else if (pe.get_dest_type() == type_Binary) {
                    type = ExpressionInternal::exp_OpSizeBinary;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>(std::move(pe), e.op_suffix, mapping);
                }
                else {
                    throw std::runtime_error("Invalid query: @size and @count can only operate on types list, binary, or string");
                }
                break;
            case parser::Expression::KeyPathOp::None:
                type = ExpressionInternal::exp_Property;
                storage = std::move(pe);
                break;
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
        realm_precondition(did_add, util::format("Unable to create a subquery expression with variable '%1' since an identical variable already exists in this context", e.subquery_var));
        query_builder::apply_predicate(exp.get_subquery(), *e.subquery, args, mapping);
        mapping.remove_mapping(exp.get_subquery().get_table(), e.subquery_var);
        storage = std::move(exp);
    }
    else {
        type = ExpressionInternal::exp_Value;
        storage = ValueExpression(query, &args, &e);
    }
}

PropertyExpression& ExpressionContainer::get_property()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_Property);
    return util::any_cast<PropertyExpression&>(storage);
}

ValueExpression& ExpressionContainer::get_value()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_Value);
    return util::any_cast<ValueExpression&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>& ExpressionContainer::get_min()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMin);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>& ExpressionContainer::get_max()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMax);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>& ExpressionContainer::get_sum()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSum);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>& ExpressionContainer::get_avg()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpAvg);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>& ExpressionContainer::get_count()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpCount);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>& ExpressionContainer::get_size_string()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeString);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>&>(storage);
}
CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>& ExpressionContainer::get_size_binary()
{
    REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeBinary);
    return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>&>(storage);
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
            self_type = get_min().post_link_col_type;
            break;
        case ExpressionInternal::exp_OpMax:
            self_type = get_max().post_link_col_type;
            break;
        case ExpressionInternal::exp_OpSum:
            self_type = get_sum().post_link_col_type;
            break;
        case ExpressionInternal::exp_OpAvg:
            self_type = get_avg().post_link_col_type;
            break;
        case ExpressionInternal::exp_SubQuery:
            REALM_FALLTHROUGH;
        case ExpressionInternal::exp_OpCount:
            // linklist count can handle any numeric type
            if (other_type == type_Int || other_type == type_Double || other_type == type_Float) {
                self_type = other_type;
            } // else other_type is unset
            break;
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
    return exp_type == ExpressionContainer::ExpressionInternal::exp_OpCount
        || exp_type == ExpressionContainer::ExpressionInternal::exp_OpSizeString
        || exp_type == ExpressionContainer::ExpressionInternal::exp_OpSizeBinary
        || exp_type == ExpressionContainer::ExpressionInternal::exp_SubQuery;
}

DataType ExpressionContainer::get_comparison_type(ExpressionContainer& rhs) {
    // check for strongly typed expressions first
    if (type == ExpressionInternal::exp_Property) {
        return rhs.check_type_compatibility(get_property().get_dest_type());
    } else if (rhs.type == ExpressionInternal::exp_Property) {
        return check_type_compatibility(rhs.get_property().get_dest_type());
    } else if (type == ExpressionInternal::exp_OpMin) {
        return rhs.check_type_compatibility(get_min().post_link_col_type);
    } else if (type == ExpressionInternal::exp_OpMax) {
        return rhs.check_type_compatibility(get_max().post_link_col_type);
    } else if (type == ExpressionInternal::exp_OpSum) {
        return rhs.check_type_compatibility(get_sum().post_link_col_type);
    } else if (type == ExpressionInternal::exp_OpAvg) {
        return rhs.check_type_compatibility(get_avg().post_link_col_type);
    } else if (rhs.type == ExpressionInternal::exp_OpMin) {
        return check_type_compatibility(rhs.get_min().post_link_col_type);
    } else if (rhs.type == ExpressionInternal::exp_OpMax) {
        return check_type_compatibility(rhs.get_max().post_link_col_type);
    } else if (rhs.type == ExpressionInternal::exp_OpSum) {
        return check_type_compatibility(rhs.get_sum().post_link_col_type);
    } else if (rhs.type == ExpressionInternal::exp_OpAvg) {
        return check_type_compatibility(rhs.get_avg().post_link_col_type);
    } else if (is_count_type(type) && is_count_type(rhs.type)) {
        return type_Int;
        // check weakly typed expressions last, we return type_Int for count/size because at this point the
        // comparison is between a @count/@size and a value which is untyped. The value should be numeric if the query
        // is well formed but we don't know what type it actually is so we will perform int promotion in a conversion.
    } else if (is_count_type(type) || is_count_type(rhs.type)) {
        return type_Int;
    }

    throw std::runtime_error("Unsupported query (type undeductable). A comparison must include at lease one keypath.");
}

bool ExpressionContainer::is_null() {
    if (type == ExpressionInternal::exp_Value) {
        return get_value().is_null();
    }
    return false;
}

} // namespace parser
} // namespace realm
