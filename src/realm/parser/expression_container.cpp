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

ExpressionContainer::ExpressionContainer(Query& query, const parser::Expression& e, query_builder::Arguments& args)
{
    if (e.type == parser::Expression::Type::KeyPath) {
        PropertyExpression pe(query, e.s);
        switch (e.collection_op) {
            case parser::Expression::KeyPathOp::Min:
                type = ExpressionInternal::exp_OpMin;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>(std::move(pe), e.op_suffix);
                break;
            case parser::Expression::KeyPathOp::Max:
                type = ExpressionInternal::exp_OpMax;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>(std::move(pe), e.op_suffix);
                break;
            case parser::Expression::KeyPathOp::Sum:
                type = ExpressionInternal::exp_OpSum;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>(std::move(pe), e.op_suffix);
                break;
            case parser::Expression::KeyPathOp::Avg:
                type = ExpressionInternal::exp_OpAvg;
                storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>(std::move(pe), e.op_suffix);
                break;
            case parser::Expression::KeyPathOp::Count:
                REALM_FALLTHROUGH;
            case parser::Expression::KeyPathOp::SizeString:
                REALM_FALLTHROUGH;
            case parser::Expression::KeyPathOp::SizeBinary:
                if (pe.col_type == type_LinkList || pe.col_type == type_Link) {
                    type = ExpressionInternal::exp_OpCount;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>(std::move(pe), e.op_suffix);
                }
                else if (pe.col_type == type_String) {
                    type = ExpressionInternal::exp_OpSizeString;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>(std::move(pe), e.op_suffix);
                }
                else if (pe.col_type == type_Binary) {
                    type = ExpressionInternal::exp_OpSizeBinary;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>(std::move(pe), e.op_suffix);
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

DataType ExpressionContainer::get_comparison_type(ExpressionContainer& rhs) {
    if (type == ExpressionInternal::exp_Property) {
        return get_property().col_type;
    } else if (rhs.type == ExpressionInternal::exp_Property) {
        return rhs.get_property().col_type;
    } else if (type == ExpressionInternal::exp_OpMin) {
        return get_min().post_link_col_type;
    } else if (type == ExpressionInternal::exp_OpMax) {
        return get_max().post_link_col_type;
    } else if (type == ExpressionInternal::exp_OpSum) {
        return get_sum().post_link_col_type;
    } else if (type == ExpressionInternal::exp_OpAvg) {
        return get_avg().post_link_col_type;
    } else if (type == ExpressionInternal::exp_OpCount) {
        return type_Int;
    } else if (type == ExpressionInternal::exp_OpSizeString) {
        return type_Int;
    } else if (type == ExpressionInternal::exp_OpSizeBinary) {
        return type_Int;
    }
    // rhs checks
    else if (rhs.type == ExpressionInternal::exp_OpMin) {
        return rhs.get_min().post_link_col_type;
    } else if (rhs.type == ExpressionInternal::exp_OpMax) {
        return rhs.get_max().post_link_col_type;
    } else if (rhs.type == ExpressionInternal::exp_OpSum) {
        return rhs.get_sum().post_link_col_type;
    } else if (rhs.type == ExpressionInternal::exp_OpAvg) {
        return rhs.get_avg().post_link_col_type;
    } else if (rhs.type == ExpressionInternal::exp_OpCount) {
        return type_Int;
    } else if (rhs.type == ExpressionInternal::exp_OpSizeString) {
        return type_Int;
    } else if (rhs.type == ExpressionInternal::exp_OpSizeBinary) {
        return type_Int;
    }

    throw std::runtime_error("Unsupported query (type undeductable). A comparison must include at lease one keypath");
}

bool ExpressionContainer::is_null() {
    if (type == ExpressionInternal::exp_Value) {
        return get_value().is_null();
    }
    return false;
}

} // namespace parser
} // namespace realm
