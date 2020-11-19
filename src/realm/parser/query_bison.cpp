// A Bison parser, made by GNU Bison 3.7.

// Skeleton implementation for Bison LALR(1) parsers in C++

// Copyright (C) 2002-2015, 2018-2020 Free Software Foundation, Inc.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// As a special exception, you may create a larger work that contains
// part or all of the Bison parser skeleton and distribute that work
// under terms of your choice, so long as that work isn't itself a
// parser generator using the skeleton or a modified version thereof
// as a parser skeleton.  Alternatively, if you modify or redistribute
// the parser skeleton itself, you may (at your option) remove this
// special exception, which will cause the skeleton and the resulting
// Bison output files to be licensed under the GNU General Public
// License without this special exception.

// This special exception was added by the Free Software Foundation in
// version 2.2 of Bison.

// DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
// especially those whose name start with YY_ or yy_.  They are
// private implementation details that can be changed or removed.


#include "query_bison.hpp"


// Unqualified %code blocks.

#include <realm/parser/driver.hpp>
#include <realm/table.hpp>
using namespace realm;
using namespace realm::query_parser;


#ifndef YY_
#if defined YYENABLE_NLS && YYENABLE_NLS
#if ENABLE_NLS
#include <libintl.h> // FIXME: INFRINGES ON USER NAME SPACE.
#define YY_(msgid) dgettext("bison-runtime", msgid)
#endif
#endif
#ifndef YY_
#define YY_(msgid) msgid
#endif
#endif


// Whether we are compiled with exception support.
#ifndef YY_EXCEPTIONS
#if defined __GNUC__ && !defined __EXCEPTIONS
#define YY_EXCEPTIONS 0
#else
#define YY_EXCEPTIONS 1
#endif
#endif


// Enable debugging if requested.
#if YYDEBUG

// A pseudo ostream that takes yydebug_ into account.
#define YYCDEBUG                                                                                                     \
    if (yydebug_)                                                                                                    \
    (*yycdebug_)

#define YY_SYMBOL_PRINT(Title, Symbol)                                                                               \
    do {                                                                                                             \
        if (yydebug_) {                                                                                              \
            *yycdebug_ << Title << ' ';                                                                              \
            yy_print_(*yycdebug_, Symbol);                                                                           \
            *yycdebug_ << '\n';                                                                                      \
        }                                                                                                            \
    } while (false)

#define YY_REDUCE_PRINT(Rule)                                                                                        \
    do {                                                                                                             \
        if (yydebug_)                                                                                                \
            yy_reduce_print_(Rule);                                                                                  \
    } while (false)

#define YY_STACK_PRINT()                                                                                             \
    do {                                                                                                             \
        if (yydebug_)                                                                                                \
            yy_stack_print_();                                                                                       \
    } while (false)

#else // !YYDEBUG

#define YYCDEBUG                                                                                                     \
    if (false)                                                                                                       \
    std::cerr
#define YY_SYMBOL_PRINT(Title, Symbol) YYUSE(Symbol)
#define YY_REDUCE_PRINT(Rule) static_cast<void>(0)
#define YY_STACK_PRINT() static_cast<void>(0)

#endif // !YYDEBUG

#define yyerrok (yyerrstatus_ = 0)
#define yyclearin (yyla.clear())

#define YYACCEPT goto yyacceptlab
#define YYABORT goto yyabortlab
#define YYERROR goto yyerrorlab
#define YYRECOVERING() (!!yyerrstatus_)

namespace yy {

/// Build a parser object.
parser::parser(ParserDriver& drv_yyarg)
#if YYDEBUG
    : yydebug_(false)
    , yycdebug_(&std::cerr)
    ,
#else
    :
#endif
    drv(drv_yyarg)
{
}

parser::~parser() {}

parser::syntax_error::~syntax_error() YY_NOEXCEPT YY_NOTHROW {}

/*---------------.
| symbol kinds.  |
`---------------*/


// by_state.
parser::by_state::by_state() YY_NOEXCEPT : state(empty_state) {}

parser::by_state::by_state(const by_state& that) YY_NOEXCEPT : state(that.state) {}

void parser::by_state::clear() YY_NOEXCEPT
{
    state = empty_state;
}

void parser::by_state::move(by_state& that)
{
    state = that.state;
    that.clear();
}

parser::by_state::by_state(state_type s) YY_NOEXCEPT : state(s) {}

parser::symbol_kind_type parser::by_state::kind() const YY_NOEXCEPT
{
    if (state == empty_state)
        return symbol_kind::SYM_YYEMPTY;
    else
        return YY_CAST(symbol_kind_type, yystos_[+state]);
}

parser::stack_symbol_type::stack_symbol_type() {}

parser::stack_symbol_type::stack_symbol_type(YY_RVREF(stack_symbol_type) that)
    : super_type(YY_MOVE(that.state))
{
    switch (that.kind()) {
        case symbol_kind::SYM_aggr_op: // aggr_op
            value.YY_MOVE_OR_COPY<AggrNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_and_pred: // and_pred
            value.YY_MOVE_OR_COPY<AndNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_atom_pred: // atom_pred
            value.YY_MOVE_OR_COPY<AtomPredNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_constant: // constant
            value.YY_MOVE_OR_COPY<ConstantNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_pred: // pred
            value.YY_MOVE_OR_COPY<OrNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_path: // path
            value.YY_MOVE_OR_COPY<PathNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_post_op: // post_op
            value.YY_MOVE_OR_COPY<PostOpNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_prop: // prop
            value.YY_MOVE_OR_COPY<PropertyNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_boolexpr: // boolexpr
            value.YY_MOVE_OR_COPY<TrueOrFalseNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_value: // value
            value.YY_MOVE_OR_COPY<ValueNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_comp_type:  // comp_type
        case symbol_kind::SYM_equality:   // equality
        case symbol_kind::SYM_relational: // relational
        case symbol_kind::SYM_stringop:   // stringop
            value.YY_MOVE_OR_COPY<int>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_ID:         // "identifier"
        case symbol_kind::SYM_STRING:     // "string"
        case symbol_kind::SYM_INFINITY:   // "infinity"
        case symbol_kind::SYM_NAN:        // "NaN"
        case symbol_kind::SYM_NATURAL0:   // "natural0"
        case symbol_kind::SYM_NUMBER:     // "number"
        case symbol_kind::SYM_FLOAT:      // "float"
        case symbol_kind::SYM_TIMESTAMP:  // "date"
        case symbol_kind::SYM_UUID:       // "UUID"
        case symbol_kind::SYM_OID:        // "ObjectId"
        case symbol_kind::SYM_ARG:        // "argument"
        case symbol_kind::SYM_BEGINSWITH: // "beginswith"
        case symbol_kind::SYM_ENDSWITH:   // "endswith"
        case symbol_kind::SYM_CONTAINS:   // "contains"
        case symbol_kind::SYM_LIKE:       // "like"
        case symbol_kind::SYM_path_elem:  // path_elem
        case symbol_kind::SYM_id:         // id
            value.YY_MOVE_OR_COPY<std::string>(YY_MOVE(that.value));
            break;

        default:
            break;
    }

#if 201103L <= YY_CPLUSPLUS
    // that is emptied.
    that.state = empty_state;
#endif
}

parser::stack_symbol_type::stack_symbol_type(state_type s, YY_MOVE_REF(symbol_type) that)
    : super_type(s)
{
    switch (that.kind()) {
        case symbol_kind::SYM_aggr_op: // aggr_op
            value.move<AggrNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_and_pred: // and_pred
            value.move<AndNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_atom_pred: // atom_pred
            value.move<AtomPredNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_constant: // constant
            value.move<ConstantNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_pred: // pred
            value.move<OrNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_path: // path
            value.move<PathNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_post_op: // post_op
            value.move<PostOpNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_prop: // prop
            value.move<PropertyNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_boolexpr: // boolexpr
            value.move<TrueOrFalseNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_value: // value
            value.move<ValueNode*>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_comp_type:  // comp_type
        case symbol_kind::SYM_equality:   // equality
        case symbol_kind::SYM_relational: // relational
        case symbol_kind::SYM_stringop:   // stringop
            value.move<int>(YY_MOVE(that.value));
            break;

        case symbol_kind::SYM_ID:         // "identifier"
        case symbol_kind::SYM_STRING:     // "string"
        case symbol_kind::SYM_INFINITY:   // "infinity"
        case symbol_kind::SYM_NAN:        // "NaN"
        case symbol_kind::SYM_NATURAL0:   // "natural0"
        case symbol_kind::SYM_NUMBER:     // "number"
        case symbol_kind::SYM_FLOAT:      // "float"
        case symbol_kind::SYM_TIMESTAMP:  // "date"
        case symbol_kind::SYM_UUID:       // "UUID"
        case symbol_kind::SYM_OID:        // "ObjectId"
        case symbol_kind::SYM_ARG:        // "argument"
        case symbol_kind::SYM_BEGINSWITH: // "beginswith"
        case symbol_kind::SYM_ENDSWITH:   // "endswith"
        case symbol_kind::SYM_CONTAINS:   // "contains"
        case symbol_kind::SYM_LIKE:       // "like"
        case symbol_kind::SYM_path_elem:  // path_elem
        case symbol_kind::SYM_id:         // id
            value.move<std::string>(YY_MOVE(that.value));
            break;

        default:
            break;
    }

    // that is emptied.
    that.kind_ = symbol_kind::SYM_YYEMPTY;
}

#if YY_CPLUSPLUS < 201103L
parser::stack_symbol_type& parser::stack_symbol_type::operator=(const stack_symbol_type& that)
{
    state = that.state;
    switch (that.kind()) {
        case symbol_kind::SYM_aggr_op: // aggr_op
            value.copy<AggrNode*>(that.value);
            break;

        case symbol_kind::SYM_and_pred: // and_pred
            value.copy<AndNode*>(that.value);
            break;

        case symbol_kind::SYM_atom_pred: // atom_pred
            value.copy<AtomPredNode*>(that.value);
            break;

        case symbol_kind::SYM_constant: // constant
            value.copy<ConstantNode*>(that.value);
            break;

        case symbol_kind::SYM_pred: // pred
            value.copy<OrNode*>(that.value);
            break;

        case symbol_kind::SYM_path: // path
            value.copy<PathNode*>(that.value);
            break;

        case symbol_kind::SYM_post_op: // post_op
            value.copy<PostOpNode*>(that.value);
            break;

        case symbol_kind::SYM_prop: // prop
            value.copy<PropertyNode*>(that.value);
            break;

        case symbol_kind::SYM_boolexpr: // boolexpr
            value.copy<TrueOrFalseNode*>(that.value);
            break;

        case symbol_kind::SYM_value: // value
            value.copy<ValueNode*>(that.value);
            break;

        case symbol_kind::SYM_comp_type:  // comp_type
        case symbol_kind::SYM_equality:   // equality
        case symbol_kind::SYM_relational: // relational
        case symbol_kind::SYM_stringop:   // stringop
            value.copy<int>(that.value);
            break;

        case symbol_kind::SYM_ID:         // "identifier"
        case symbol_kind::SYM_STRING:     // "string"
        case symbol_kind::SYM_INFINITY:   // "infinity"
        case symbol_kind::SYM_NAN:        // "NaN"
        case symbol_kind::SYM_NATURAL0:   // "natural0"
        case symbol_kind::SYM_NUMBER:     // "number"
        case symbol_kind::SYM_FLOAT:      // "float"
        case symbol_kind::SYM_TIMESTAMP:  // "date"
        case symbol_kind::SYM_UUID:       // "UUID"
        case symbol_kind::SYM_OID:        // "ObjectId"
        case symbol_kind::SYM_ARG:        // "argument"
        case symbol_kind::SYM_BEGINSWITH: // "beginswith"
        case symbol_kind::SYM_ENDSWITH:   // "endswith"
        case symbol_kind::SYM_CONTAINS:   // "contains"
        case symbol_kind::SYM_LIKE:       // "like"
        case symbol_kind::SYM_path_elem:  // path_elem
        case symbol_kind::SYM_id:         // id
            value.copy<std::string>(that.value);
            break;

        default:
            break;
    }

    return *this;
}

parser::stack_symbol_type& parser::stack_symbol_type::operator=(stack_symbol_type& that)
{
    state = that.state;
    switch (that.kind()) {
        case symbol_kind::SYM_aggr_op: // aggr_op
            value.move<AggrNode*>(that.value);
            break;

        case symbol_kind::SYM_and_pred: // and_pred
            value.move<AndNode*>(that.value);
            break;

        case symbol_kind::SYM_atom_pred: // atom_pred
            value.move<AtomPredNode*>(that.value);
            break;

        case symbol_kind::SYM_constant: // constant
            value.move<ConstantNode*>(that.value);
            break;

        case symbol_kind::SYM_pred: // pred
            value.move<OrNode*>(that.value);
            break;

        case symbol_kind::SYM_path: // path
            value.move<PathNode*>(that.value);
            break;

        case symbol_kind::SYM_post_op: // post_op
            value.move<PostOpNode*>(that.value);
            break;

        case symbol_kind::SYM_prop: // prop
            value.move<PropertyNode*>(that.value);
            break;

        case symbol_kind::SYM_boolexpr: // boolexpr
            value.move<TrueOrFalseNode*>(that.value);
            break;

        case symbol_kind::SYM_value: // value
            value.move<ValueNode*>(that.value);
            break;

        case symbol_kind::SYM_comp_type:  // comp_type
        case symbol_kind::SYM_equality:   // equality
        case symbol_kind::SYM_relational: // relational
        case symbol_kind::SYM_stringop:   // stringop
            value.move<int>(that.value);
            break;

        case symbol_kind::SYM_ID:         // "identifier"
        case symbol_kind::SYM_STRING:     // "string"
        case symbol_kind::SYM_INFINITY:   // "infinity"
        case symbol_kind::SYM_NAN:        // "NaN"
        case symbol_kind::SYM_NATURAL0:   // "natural0"
        case symbol_kind::SYM_NUMBER:     // "number"
        case symbol_kind::SYM_FLOAT:      // "float"
        case symbol_kind::SYM_TIMESTAMP:  // "date"
        case symbol_kind::SYM_UUID:       // "UUID"
        case symbol_kind::SYM_OID:        // "ObjectId"
        case symbol_kind::SYM_ARG:        // "argument"
        case symbol_kind::SYM_BEGINSWITH: // "beginswith"
        case symbol_kind::SYM_ENDSWITH:   // "endswith"
        case symbol_kind::SYM_CONTAINS:   // "contains"
        case symbol_kind::SYM_LIKE:       // "like"
        case symbol_kind::SYM_path_elem:  // path_elem
        case symbol_kind::SYM_id:         // id
            value.move<std::string>(that.value);
            break;

        default:
            break;
    }

    // that is emptied.
    that.state = empty_state;
    return *this;
}
#endif

template <typename Base>
void parser::yy_destroy_(const char* yymsg, basic_symbol<Base>& yysym) const
{
    if (yymsg)
        YY_SYMBOL_PRINT(yymsg, yysym);
}

#if YYDEBUG
template <typename Base>
void parser::yy_print_(std::ostream& yyo, const basic_symbol<Base>& yysym) const
{
    std::ostream& yyoutput = yyo;
    YYUSE(yyoutput);
    if (yysym.empty())
        yyo << "empty symbol";
    else {
        symbol_kind_type yykind = yysym.kind();
        yyo << (yykind < YYNTOKENS ? "token" : "nterm") << ' ' << yysym.name() << " (";
        switch (yykind) {
            case symbol_kind::SYM_YYEOF: // "end of file"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_TRUEPREDICATE: // "truepredicate"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_FALSEPREDICATE: // "falsepredicate"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_SORT: // "sort"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_DISTINCT: // "distinct"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_LIMIT: // "limit"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_ASCENDING: // "ascending"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_DESCENDING: // "descending"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_TRUE: // "true"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_FALSE: // "false"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_NULL_VAL: // "null"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_EQUAL: // "=="
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_NOT_EQUAL: // "!="
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_LESS: // "<"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_GREATER: // ">"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_GREATER_EQUAL: // ">="
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_LESS_EQUAL: // "<="
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_CASE: // "[c]"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_ANY: // "any"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_ALL: // "all"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_NONE: // "none"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_SIZE: // "@size"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_COUNT: // "@count"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_MAX: // "@max"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_MIN: // "@min"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_SUM: // "@sun"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_AVG: // "@average"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_AND: // "&&"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_OR: // "||"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_NOT: // "!"
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_ID: // "identifier"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_STRING: // "string"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_INFINITY: // "infinity"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_NAN: // "NaN"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_NATURAL0: // "natural0"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_NUMBER: // "number"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_FLOAT: // "float"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_TIMESTAMP: // "date"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_UUID: // "UUID"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_OID: // "ObjectId"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_ARG: // "argument"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_BEGINSWITH: // "beginswith"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_ENDSWITH: // "endswith"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_CONTAINS: // "contains"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_LIKE: // "like"
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_47_: // '('
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_48_: // ')'
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_49_: // '.'
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_50_: // ','
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_query: // query
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_pred: // pred
            {
                yyo << yysym.value.template as<OrNode*>();
            } break;

            case symbol_kind::SYM_and_pred: // and_pred
            {
                yyo << yysym.value.template as<AndNode*>();
            } break;

            case symbol_kind::SYM_atom_pred: // atom_pred
            {
                yyo << yysym.value.template as<AtomPredNode*>();
            } break;

            case symbol_kind::SYM_value: // value
            {
                yyo << yysym.value.template as<ValueNode*>();
            } break;

            case symbol_kind::SYM_prop: // prop
            {
                yyo << yysym.value.template as<PropertyNode*>();
            } break;

            case symbol_kind::SYM_pred_suffix: // pred_suffix
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_atom_suffix: // atom_suffix
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_distinct: // distinct
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_distinct_param: // distinct_param
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_sort: // sort
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_sort_param: // sort_param
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_limit: // limit
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_direction: // direction
            {
                yyo << "<>";
            } break;

            case symbol_kind::SYM_constant: // constant
            {
                yyo << yysym.value.template as<ConstantNode*>();
            } break;

            case symbol_kind::SYM_boolexpr: // boolexpr
            {
                yyo << yysym.value.template as<TrueOrFalseNode*>();
            } break;

            case symbol_kind::SYM_comp_type: // comp_type
            {
                yyo << yysym.value.template as<int>();
            } break;

            case symbol_kind::SYM_post_op: // post_op
            {
                yyo << yysym.value.template as<PostOpNode*>();
            } break;

            case symbol_kind::SYM_aggr_op: // aggr_op
            {
                yyo << yysym.value.template as<AggrNode*>();
            } break;

            case symbol_kind::SYM_equality: // equality
            {
                yyo << yysym.value.template as<int>();
            } break;

            case symbol_kind::SYM_relational: // relational
            {
                yyo << yysym.value.template as<int>();
            } break;

            case symbol_kind::SYM_stringop: // stringop
            {
                yyo << yysym.value.template as<int>();
            } break;

            case symbol_kind::SYM_path: // path
            {
                yyo << yysym.value.template as<PathNode*>();
            } break;

            case symbol_kind::SYM_path_elem: // path_elem
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            case symbol_kind::SYM_id: // id
            {
                yyo << yysym.value.template as<std::string>();
            } break;

            default:
                break;
        }
        yyo << ')';
    }
}
#endif

void parser::yypush_(const char* m, YY_MOVE_REF(stack_symbol_type) sym)
{
    if (m)
        YY_SYMBOL_PRINT(m, sym);
    yystack_.push(YY_MOVE(sym));
}

void parser::yypush_(const char* m, state_type s, YY_MOVE_REF(symbol_type) sym)
{
#if 201103L <= YY_CPLUSPLUS
    yypush_(m, stack_symbol_type(s, std::move(sym)));
#else
    stack_symbol_type ss(s, sym);
    yypush_(m, ss);
#endif
}

void parser::yypop_(int n)
{
    yystack_.pop(n);
}

#if YYDEBUG
std::ostream& parser::debug_stream() const
{
    return *yycdebug_;
}

void parser::set_debug_stream(std::ostream& o)
{
    yycdebug_ = &o;
}


parser::debug_level_type parser::debug_level() const
{
    return yydebug_;
}

void parser::set_debug_level(debug_level_type l)
{
    yydebug_ = l;
}
#endif // YYDEBUG

parser::state_type parser::yy_lr_goto_state_(state_type yystate, int yysym)
{
    int yyr = yypgoto_[yysym - YYNTOKENS] + yystate;
    if (0 <= yyr && yyr <= yylast_ && yycheck_[yyr] == yystate)
        return yytable_[yyr];
    else
        return yydefgoto_[yysym - YYNTOKENS];
}

bool parser::yy_pact_value_is_default_(int yyvalue)
{
    return yyvalue == yypact_ninf_;
}

bool parser::yy_table_value_is_error_(int yyvalue)
{
    return yyvalue == yytable_ninf_;
}

int parser::operator()()
{
    return parse();
}

int parser::parse()
{
    int yyn;
    /// Length of the RHS of the rule being reduced.
    int yylen = 0;

    // Error handling.
    int yynerrs_ = 0;
    int yyerrstatus_ = 0;

    /// The lookahead symbol.
    symbol_type yyla;

    /// The return value of parse ().
    int yyresult;

#if YY_EXCEPTIONS
    try
#endif // YY_EXCEPTIONS
    {
        YYCDEBUG << "Starting parse\n";


        /* Initialize the stack.  The initial state will be set in
           yynewstate, since the latter expects the semantical and the
           location values to have been already stored, initialize these
           stacks with a primary value.  */
        yystack_.clear();
        yypush_(YY_NULLPTR, 0, YY_MOVE(yyla));

    /*-----------------------------------------------.
    | yynewstate -- push a new symbol on the stack.  |
    `-----------------------------------------------*/
    yynewstate:
        YYCDEBUG << "Entering state " << int(yystack_[0].state) << '\n';
        YY_STACK_PRINT();

        // Accept?
        if (yystack_[0].state == yyfinal_)
            YYACCEPT;

        goto yybackup;


    /*-----------.
    | yybackup.  |
    `-----------*/
    yybackup:
        // Try to take a decision without lookahead.
        yyn = yypact_[+yystack_[0].state];
        if (yy_pact_value_is_default_(yyn))
            goto yydefault;

        // Read a lookahead token.
        if (yyla.empty()) {
            YYCDEBUG << "Reading a token\n";
#if YY_EXCEPTIONS
            try
#endif // YY_EXCEPTIONS
            {
                symbol_type yylookahead(yylex(drv));
                yyla.move(yylookahead);
            }
#if YY_EXCEPTIONS
            catch (const syntax_error& yyexc)
            {
                YYCDEBUG << "Caught exception: " << yyexc.what() << '\n';
                error(yyexc);
                goto yyerrlab1;
            }
#endif // YY_EXCEPTIONS
        }
        YY_SYMBOL_PRINT("Next token is", yyla);

        if (yyla.kind() == symbol_kind::SYM_YYerror) {
            // The scanner already issued an error message, process directly
            // to error recovery.  But do not keep the error token as
            // lookahead, it is too special and may lead us to an endless
            // loop in error recovery. */
            yyla.kind_ = symbol_kind::SYM_YYUNDEF;
            goto yyerrlab1;
        }

        /* If the proper action on seeing token YYLA.TYPE is to reduce or
           to detect an error, take that action.  */
        yyn += yyla.kind();
        if (yyn < 0 || yylast_ < yyn || yycheck_[yyn] != yyla.kind()) {
            goto yydefault;
        }

        // Reduce or error.
        yyn = yytable_[yyn];
        if (yyn <= 0) {
            if (yy_table_value_is_error_(yyn))
                goto yyerrlab;
            yyn = -yyn;
            goto yyreduce;
        }

        // Count tokens shifted since error; after three, turn off error status.
        if (yyerrstatus_)
            --yyerrstatus_;

        // Shift the lookahead token.
        yypush_("Shifting", state_type(yyn), YY_MOVE(yyla));
        goto yynewstate;


    /*-----------------------------------------------------------.
    | yydefault -- do the default action for the current state.  |
    `-----------------------------------------------------------*/
    yydefault:
        yyn = yydefact_[+yystack_[0].state];
        if (yyn == 0)
            goto yyerrlab;
        goto yyreduce;


    /*-----------------------------.
    | yyreduce -- do a reduction.  |
    `-----------------------------*/
    yyreduce:
        yylen = yyr2_[yyn];
        {
            stack_symbol_type yylhs;
            yylhs.state = yy_lr_goto_state_(yystack_[yylen].state, yyr1_[yyn]);
            /* Variants are always initialized to an empty instance of the
               correct type. The default '$$ = $1' action is NOT applied
               when using variants.  */
            switch (yyr1_[yyn]) {
                case symbol_kind::SYM_aggr_op: // aggr_op
                    yylhs.value.emplace<AggrNode*>();
                    break;

                case symbol_kind::SYM_and_pred: // and_pred
                    yylhs.value.emplace<AndNode*>();
                    break;

                case symbol_kind::SYM_atom_pred: // atom_pred
                    yylhs.value.emplace<AtomPredNode*>();
                    break;

                case symbol_kind::SYM_constant: // constant
                    yylhs.value.emplace<ConstantNode*>();
                    break;

                case symbol_kind::SYM_pred: // pred
                    yylhs.value.emplace<OrNode*>();
                    break;

                case symbol_kind::SYM_path: // path
                    yylhs.value.emplace<PathNode*>();
                    break;

                case symbol_kind::SYM_post_op: // post_op
                    yylhs.value.emplace<PostOpNode*>();
                    break;

                case symbol_kind::SYM_prop: // prop
                    yylhs.value.emplace<PropertyNode*>();
                    break;

                case symbol_kind::SYM_boolexpr: // boolexpr
                    yylhs.value.emplace<TrueOrFalseNode*>();
                    break;

                case symbol_kind::SYM_value: // value
                    yylhs.value.emplace<ValueNode*>();
                    break;

                case symbol_kind::SYM_comp_type:  // comp_type
                case symbol_kind::SYM_equality:   // equality
                case symbol_kind::SYM_relational: // relational
                case symbol_kind::SYM_stringop:   // stringop
                    yylhs.value.emplace<int>();
                    break;

                case symbol_kind::SYM_ID:         // "identifier"
                case symbol_kind::SYM_STRING:     // "string"
                case symbol_kind::SYM_INFINITY:   // "infinity"
                case symbol_kind::SYM_NAN:        // "NaN"
                case symbol_kind::SYM_NATURAL0:   // "natural0"
                case symbol_kind::SYM_NUMBER:     // "number"
                case symbol_kind::SYM_FLOAT:      // "float"
                case symbol_kind::SYM_TIMESTAMP:  // "date"
                case symbol_kind::SYM_UUID:       // "UUID"
                case symbol_kind::SYM_OID:        // "ObjectId"
                case symbol_kind::SYM_ARG:        // "argument"
                case symbol_kind::SYM_BEGINSWITH: // "beginswith"
                case symbol_kind::SYM_ENDSWITH:   // "endswith"
                case symbol_kind::SYM_CONTAINS:   // "contains"
                case symbol_kind::SYM_LIKE:       // "like"
                case symbol_kind::SYM_path_elem:  // path_elem
                case symbol_kind::SYM_id:         // id
                    yylhs.value.emplace<std::string>();
                    break;

                default:
                    break;
            }


            // Perform the reduction.
            YY_REDUCE_PRINT(yyn);
#if YY_EXCEPTIONS
            try
#endif // YY_EXCEPTIONS
            {
                switch (yyn) {
                    case 2: // query: pred pred_suffix
                    {
                        drv.result = yystack_[1].value.as<OrNode*>();
                    } break;

                    case 3: // pred: and_pred
                    {
                        yylhs.value.as<OrNode*>() =
                            drv.m_parse_nodes.create<OrNode>(yystack_[0].value.as<AndNode*>());
                    } break;

                    case 4: // pred: pred "||" and_pred
                    {
                        yystack_[2].value.as<OrNode*>()->and_preds.emplace_back(yystack_[0].value.as<AndNode*>());
                        yylhs.value.as<OrNode*>() = yystack_[2].value.as<OrNode*>();
                    } break;

                    case 5: // and_pred: atom_pred
                    {
                        yylhs.value.as<AndNode*>() =
                            drv.m_parse_nodes.create<AndNode>(yystack_[0].value.as<AtomPredNode*>());
                    } break;

                    case 6: // and_pred: and_pred "&&" atom_pred
                    {
                        yystack_[2].value.as<AndNode*>()->atom_preds.emplace_back(
                            yystack_[0].value.as<AtomPredNode*>());
                        yylhs.value.as<AndNode*>() = yystack_[2].value.as<AndNode*>();
                    } break;

                    case 7: // atom_pred: value equality value
                    {
                        yylhs.value.as<AtomPredNode*>() = drv.m_parse_nodes.create<EqualitylNode>(
                            yystack_[2].value.as<ValueNode*>(), yystack_[1].value.as<int>(),
                            yystack_[0].value.as<ValueNode*>());
                    } break;

                    case 8: // atom_pred: value equality "[c]" value
                    {
                        auto tmp = drv.m_parse_nodes.create<EqualitylNode>(yystack_[3].value.as<ValueNode*>(),
                                                                           yystack_[2].value.as<int>(),
                                                                           yystack_[0].value.as<ValueNode*>());
                        tmp->case_sensitive = false;
                        yylhs.value.as<AtomPredNode*>() = tmp;
                    } break;

                    case 9: // atom_pred: value relational value
                    {
                        yylhs.value.as<AtomPredNode*>() = drv.m_parse_nodes.create<RelationalNode>(
                            yystack_[2].value.as<ValueNode*>(), yystack_[1].value.as<int>(),
                            yystack_[0].value.as<ValueNode*>());
                    } break;

                    case 10: // atom_pred: value stringop value
                    {
                        yylhs.value.as<AtomPredNode*>() = drv.m_parse_nodes.create<StringOpsNode>(
                            yystack_[2].value.as<ValueNode*>(), yystack_[1].value.as<int>(),
                            yystack_[0].value.as<ValueNode*>());
                    } break;

                    case 11: // atom_pred: value stringop "[c]" value
                    {
                        auto tmp = drv.m_parse_nodes.create<StringOpsNode>(yystack_[3].value.as<ValueNode*>(),
                                                                           yystack_[2].value.as<int>(),
                                                                           yystack_[0].value.as<ValueNode*>());
                        tmp->case_sensitive = false;
                        yylhs.value.as<AtomPredNode*>() = tmp;
                    } break;

                    case 12: // atom_pred: "!" atom_pred
                    {
                        yylhs.value.as<AtomPredNode*>() =
                            drv.m_parse_nodes.create<NotNode>(yystack_[0].value.as<AtomPredNode*>());
                    } break;

                    case 13: // atom_pred: '(' pred ')'
                    {
                        yylhs.value.as<AtomPredNode*>() =
                            drv.m_parse_nodes.create<ParensNode>(yystack_[1].value.as<OrNode*>());
                    } break;

                    case 14: // atom_pred: boolexpr
                    {
                        yylhs.value.as<AtomPredNode*>() = yystack_[0].value.as<TrueOrFalseNode*>();
                    } break;

                    case 15: // value: constant
                    {
                        yylhs.value.as<ValueNode*>() =
                            drv.m_parse_nodes.create<ValueNode>(yystack_[0].value.as<ConstantNode*>());
                    } break;

                    case 16: // value: prop
                    {
                        yylhs.value.as<ValueNode*>() =
                            drv.m_parse_nodes.create<ValueNode>(yystack_[0].value.as<PropertyNode*>());
                    } break;

                    case 17: // prop: comp_type path id
                    {
                        yylhs.value.as<PropertyNode*>() = drv.m_parse_nodes.create<PropNode>(
                            yystack_[1].value.as<PathNode*>(), yystack_[0].value.as<std::string>(),
                            ExpressionComparisonType(yystack_[2].value.as<int>()));
                    } break;

                    case 18: // prop: path id post_op
                    {
                        yylhs.value.as<PropertyNode*>() = drv.m_parse_nodes.create<PropNode>(
                            yystack_[2].value.as<PathNode*>(), yystack_[1].value.as<std::string>(),
                            yystack_[0].value.as<PostOpNode*>());
                    } break;

                    case 19: // prop: path id '.' aggr_op '.' id
                    {
                        yylhs.value.as<PropertyNode*>() = drv.m_parse_nodes.create<LinkAggrNode>(
                            yystack_[5].value.as<PathNode*>(), yystack_[4].value.as<std::string>(),
                            yystack_[2].value.as<AggrNode*>(), yystack_[0].value.as<std::string>());
                    } break;

                    case 20: // prop: path id '.' aggr_op
                    {
                        yylhs.value.as<PropertyNode*>() = drv.m_parse_nodes.create<ListAggrNode>(
                            yystack_[3].value.as<PathNode*>(), yystack_[2].value.as<std::string>(),
                            yystack_[0].value.as<AggrNode*>());
                    } break;

                    case 35: // constant: "natural0"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::NUMBER, yystack_[0].value.as<std::string>());
                    } break;

                    case 36: // constant: "number"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::NUMBER, yystack_[0].value.as<std::string>());
                    } break;

                    case 37: // constant: "infinity"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::INFINITY_VAL, yystack_[0].value.as<std::string>());
                    } break;

                    case 38: // constant: "NaN"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::NAN_VAL, yystack_[0].value.as<std::string>());
                    } break;

                    case 39: // constant: "string"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::STRING, yystack_[0].value.as<std::string>());
                    } break;

                    case 40: // constant: "float"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::FLOAT, yystack_[0].value.as<std::string>());
                    } break;

                    case 41: // constant: "date"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::TIMESTAMP, yystack_[0].value.as<std::string>());
                    } break;

                    case 42: // constant: "UUID"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::UUID_T, yystack_[0].value.as<std::string>());
                    } break;

                    case 43: // constant: "ObjectId"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::OID, yystack_[0].value.as<std::string>());
                    } break;

                    case 44: // constant: "true"
                    {
                        yylhs.value.as<ConstantNode*>() =
                            drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TRUE, "");
                    } break;

                    case 45: // constant: "false"
                    {
                        yylhs.value.as<ConstantNode*>() =
                            drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FALSE, "");
                    } break;

                    case 46: // constant: "null"
                    {
                        yylhs.value.as<ConstantNode*>() =
                            drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NULL_VAL, "");
                    } break;

                    case 47: // constant: "argument"
                    {
                        yylhs.value.as<ConstantNode*>() = drv.m_parse_nodes.create<ConstantNode>(
                            ConstantNode::ARG, yystack_[0].value.as<std::string>());
                    } break;

                    case 48: // boolexpr: "truepredicate"
                    {
                        yylhs.value.as<TrueOrFalseNode*>() = drv.m_parse_nodes.create<TrueOrFalseNode>(true);
                    } break;

                    case 49: // boolexpr: "falsepredicate"
                    {
                        yylhs.value.as<TrueOrFalseNode*>() = drv.m_parse_nodes.create<TrueOrFalseNode>(false);
                    } break;

                    case 50: // comp_type: "any"
                    {
                        yylhs.value.as<int>() = int(ExpressionComparisonType::Any);
                    } break;

                    case 51: // comp_type: "all"
                    {
                        yylhs.value.as<int>() = int(ExpressionComparisonType::All);
                    } break;

                    case 52: // comp_type: "none"
                    {
                        yylhs.value.as<int>() = int(ExpressionComparisonType::None);
                    } break;

                    case 53: // post_op: %empty
                    {
                        yylhs.value.as<PostOpNode*>() = nullptr;
                    } break;

                    case 54: // post_op: '.' "@count"
                    {
                        yylhs.value.as<PostOpNode*>() = drv.m_parse_nodes.create<PostOpNode>(PostOpNode::COUNT);
                    } break;

                    case 55: // post_op: '.' "@size"
                    {
                        yylhs.value.as<PostOpNode*>() = drv.m_parse_nodes.create<PostOpNode>(PostOpNode::SIZE);
                    } break;

                    case 56: // aggr_op: "@max"
                    {
                        yylhs.value.as<AggrNode*>() = drv.m_parse_nodes.create<AggrNode>(AggrNode::MAX);
                    } break;

                    case 57: // aggr_op: "@min"
                    {
                        yylhs.value.as<AggrNode*>() = drv.m_parse_nodes.create<AggrNode>(AggrNode::MIN);
                    } break;

                    case 58: // aggr_op: "@sun"
                    {
                        yylhs.value.as<AggrNode*>() = drv.m_parse_nodes.create<AggrNode>(AggrNode::SUM);
                    } break;

                    case 59: // aggr_op: "@average"
                    {
                        yylhs.value.as<AggrNode*>() = drv.m_parse_nodes.create<AggrNode>(AggrNode::AVG);
                    } break;

                    case 60: // equality: "=="
                    {
                        yylhs.value.as<int>() = CompareNode::EQUAL;
                    } break;

                    case 61: // equality: "!="
                    {
                        yylhs.value.as<int>() = CompareNode::NOT_EQUAL;
                    } break;

                    case 62: // relational: "<"
                    {
                        yylhs.value.as<int>() = CompareNode::LESS;
                    } break;

                    case 63: // relational: "<="
                    {
                        yylhs.value.as<int>() = CompareNode::LESS_EQUAL;
                    } break;

                    case 64: // relational: ">"
                    {
                        yylhs.value.as<int>() = CompareNode::GREATER;
                    } break;

                    case 65: // relational: ">="
                    {
                        yylhs.value.as<int>() = CompareNode::GREATER_EQUAL;
                    } break;

                    case 66: // stringop: "beginswith"
                    {
                        yylhs.value.as<int>() = CompareNode::BEGINSWITH;
                    } break;

                    case 67: // stringop: "endswith"
                    {
                        yylhs.value.as<int>() = CompareNode::ENDSWITH;
                    } break;

                    case 68: // stringop: "contains"
                    {
                        yylhs.value.as<int>() = CompareNode::CONTAINS;
                    } break;

                    case 69: // stringop: "like"
                    {
                        yylhs.value.as<int>() = CompareNode::LIKE;
                    } break;

                    case 70: // path: %empty
                    {
                        yylhs.value.as<PathNode*>() = drv.m_parse_nodes.create<PathNode>();
                    } break;

                    case 71: // path: path path_elem
                    {
                        yystack_[1].value.as<PathNode*>()->path_elems.push_back(yystack_[0].value.as<std::string>());
                        yylhs.value.as<PathNode*>() = yystack_[1].value.as<PathNode*>();
                    } break;

                    case 72: // path_elem: id '.'
                    {
                        yylhs.value.as<std::string>() = yystack_[1].value.as<std::string>();
                    } break;

                    case 73: // id: "identifier"
                    {
                        yylhs.value.as<std::string>() = yystack_[0].value.as<std::string>();
                    } break;

                    case 74: // id: "beginswith"
                    {
                        yylhs.value.as<std::string>() = yystack_[0].value.as<std::string>();
                    } break;

                    case 75: // id: "endswith"
                    {
                        yylhs.value.as<std::string>() = yystack_[0].value.as<std::string>();
                    } break;

                    case 76: // id: "contains"
                    {
                        yylhs.value.as<std::string>() = yystack_[0].value.as<std::string>();
                    } break;

                    case 77: // id: "like"
                    {
                        yylhs.value.as<std::string>() = yystack_[0].value.as<std::string>();
                    } break;


                    default:
                        break;
                }
            }
#if YY_EXCEPTIONS
            catch (const syntax_error& yyexc)
            {
                YYCDEBUG << "Caught exception: " << yyexc.what() << '\n';
                error(yyexc);
                YYERROR;
            }
#endif // YY_EXCEPTIONS
            YY_SYMBOL_PRINT("-> $$ =", yylhs);
            yypop_(yylen);
            yylen = 0;

            // Shift the result of the reduction.
            yypush_(YY_NULLPTR, YY_MOVE(yylhs));
        }
        goto yynewstate;


    /*--------------------------------------.
    | yyerrlab -- here on detecting error.  |
    `--------------------------------------*/
    yyerrlab:
        // If not already recovering from an error, report this error.
        if (!yyerrstatus_) {
            ++yynerrs_;
            context yyctx(*this, yyla);
            std::string msg = yysyntax_error_(yyctx);
            error(YY_MOVE(msg));
        }


        if (yyerrstatus_ == 3) {
            /* If just tried and failed to reuse lookahead token after an
               error, discard it.  */

            // Return failure if at end of input.
            if (yyla.kind() == symbol_kind::SYM_YYEOF)
                YYABORT;
            else if (!yyla.empty()) {
                yy_destroy_("Error: discarding", yyla);
                yyla.clear();
            }
        }

        // Else will try to reuse lookahead token after shifting the error token.
        goto yyerrlab1;


    /*---------------------------------------------------.
    | yyerrorlab -- error raised explicitly by YYERROR.  |
    `---------------------------------------------------*/
    yyerrorlab:
        /* Pacify compilers when the user code never invokes YYERROR and
           the label yyerrorlab therefore never appears in user code.  */
        if (false)
            YYERROR;

        /* Do not reclaim the symbols of the rule whose action triggered
           this YYERROR.  */
        yypop_(yylen);
        yylen = 0;
        YY_STACK_PRINT();
        goto yyerrlab1;


    /*-------------------------------------------------------------.
    | yyerrlab1 -- common code for both syntax error and YYERROR.  |
    `-------------------------------------------------------------*/
    yyerrlab1:
        yyerrstatus_ = 3; // Each real token shifted decrements this.
        // Pop stack until we find a state that shifts the error token.
        for (;;) {
            yyn = yypact_[+yystack_[0].state];
            if (!yy_pact_value_is_default_(yyn)) {
                yyn += symbol_kind::SYM_YYerror;
                if (0 <= yyn && yyn <= yylast_ && yycheck_[yyn] == symbol_kind::SYM_YYerror) {
                    yyn = yytable_[yyn];
                    if (0 < yyn)
                        break;
                }
            }

            // Pop the current state because it cannot handle the error token.
            if (yystack_.size() == 1)
                YYABORT;

            yy_destroy_("Error: popping", yystack_[0]);
            yypop_();
            YY_STACK_PRINT();
        }
        {
            stack_symbol_type error_token;


            // Shift the error token.
            error_token.state = state_type(yyn);
            yypush_("Shifting", YY_MOVE(error_token));
        }
        goto yynewstate;


    /*-------------------------------------.
    | yyacceptlab -- YYACCEPT comes here.  |
    `-------------------------------------*/
    yyacceptlab:
        yyresult = 0;
        goto yyreturn;


    /*-----------------------------------.
    | yyabortlab -- YYABORT comes here.  |
    `-----------------------------------*/
    yyabortlab:
        yyresult = 1;
        goto yyreturn;


    /*-----------------------------------------------------.
    | yyreturn -- parsing is finished, return the result.  |
    `-----------------------------------------------------*/
    yyreturn:
        if (!yyla.empty())
            yy_destroy_("Cleanup: discarding lookahead", yyla);

        /* Do not reclaim the symbols of the rule whose action triggered
           this YYABORT or YYACCEPT.  */
        yypop_(yylen);
        YY_STACK_PRINT();
        while (1 < yystack_.size()) {
            yy_destroy_("Cleanup: popping", yystack_[0]);
            yypop_();
        }

        return yyresult;
    }
#if YY_EXCEPTIONS
    catch (...)
    {
        YYCDEBUG << "Exception caught: cleaning lookahead and stack\n";
        // Do not try to display the values of the reclaimed symbols,
        // as their printers might throw an exception.
        if (!yyla.empty())
            yy_destroy_(YY_NULLPTR, yyla);

        while (1 < yystack_.size()) {
            yy_destroy_(YY_NULLPTR, yystack_[0]);
            yypop_();
        }
        throw;
    }
#endif // YY_EXCEPTIONS
}

void parser::error(const syntax_error& yyexc)
{
    error(yyexc.what());
}

/* Return YYSTR after stripping away unnecessary quotes and
   backslashes, so that it's suitable for yyerror.  The heuristic is
   that double-quoting is unnecessary unless the string contains an
   apostrophe, a comma, or backslash (other than backslash-backslash).
   YYSTR is taken from yytname.  */
std::string parser::yytnamerr_(const char* yystr)
{
    if (*yystr == '"') {
        std::string yyr;
        char const* yyp = yystr;

        for (;;)
            switch (*++yyp) {
                case '\'':
                case ',':
                    goto do_not_strip_quotes;

                case '\\':
                    if (*++yyp != '\\')
                        goto do_not_strip_quotes;
                    else
                        goto append;

                append:
                default:
                    yyr += *yyp;
                    break;

                case '"':
                    return yyr;
            }
    do_not_strip_quotes:;
    }

    return yystr;
}

std::string parser::symbol_name(symbol_kind_type yysymbol)
{
    return yytnamerr_(yytname_[yysymbol]);
}


// parser::context.
parser::context::context(const parser& yyparser, const symbol_type& yyla)
    : yyparser_(yyparser)
    , yyla_(yyla)
{
}

int parser::context::expected_tokens(symbol_kind_type yyarg[], int yyargn) const
{
    // Actual number of expected tokens
    int yycount = 0;

    int yyn = yypact_[+yyparser_.yystack_[0].state];
    if (!yy_pact_value_is_default_(yyn)) {
        /* Start YYX at -YYN if negative to avoid negative indexes in
           YYCHECK.  In other words, skip the first -YYN actions for
           this state because they are default actions.  */
        int yyxbegin = yyn < 0 ? -yyn : 0;
        // Stay within bounds of both yycheck and yytname.
        int yychecklim = yylast_ - yyn + 1;
        int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
        for (int yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck_[yyx + yyn] == yyx && yyx != symbol_kind::SYM_YYerror &&
                !yy_table_value_is_error_(yytable_[yyx + yyn])) {
                if (!yyarg)
                    ++yycount;
                else if (yycount == yyargn)
                    return 0;
                else
                    yyarg[yycount++] = YY_CAST(symbol_kind_type, yyx);
            }
    }

    if (yyarg && yycount == 0 && 0 < yyargn)
        yyarg[0] = symbol_kind::SYM_YYEMPTY;
    return yycount;
}


int parser::yy_syntax_error_arguments_(const context& yyctx, symbol_kind_type yyarg[], int yyargn) const
{
    /* There are many possibilities here to consider:
       - If this state is a consistent state with a default action, then
         the only way this function was invoked is if the default action
         is an error action.  In that case, don't check for expected
         tokens because there are none.
       - The only way there can be no lookahead present (in yyla) is
         if this state is a consistent state with a default action.
         Thus, detecting the absence of a lookahead is sufficient to
         determine that there is no unexpected or expected token to
         report.  In that case, just report a simple "syntax error".
       - Don't assume there isn't a lookahead just because this state is
         a consistent state with a default action.  There might have
         been a previous inconsistent state, consistent state with a
         non-default action, or user semantic action that manipulated
         yyla.  (However, yyla is currently not documented for users.)
       - Of course, the expected token list depends on states to have
         correct lookahead information, and it depends on the parser not
         to perform extra reductions after fetching a lookahead from the
         scanner and before detecting a syntax error.  Thus, state merging
         (from LALR or IELR) and default reductions corrupt the expected
         token list.  However, the list is correct for canonical LR with
         one exception: it will still contain any token that will not be
         accepted due to an error action in a later state.
    */

    if (!yyctx.lookahead().empty()) {
        if (yyarg)
            yyarg[0] = yyctx.token();
        int yyn = yyctx.expected_tokens(yyarg ? yyarg + 1 : yyarg, yyargn - 1);
        return yyn + 1;
    }
    return 0;
}

// Generate an error message.
std::string parser::yysyntax_error_(const context& yyctx) const
{
    // Its maximum.
    enum { YYARGS_MAX = 5 };
    // Arguments of yyformat.
    symbol_kind_type yyarg[YYARGS_MAX];
    int yycount = yy_syntax_error_arguments_(yyctx, yyarg, YYARGS_MAX);

    char const* yyformat = YY_NULLPTR;
    switch (yycount) {
#define YYCASE_(N, S)                                                                                                \
    case N:                                                                                                          \
        yyformat = S;                                                                                                \
        break
        default: // Avoid compiler warnings.
            YYCASE_(0, YY_("syntax error"));
            YYCASE_(1, YY_("syntax error, unexpected %s"));
            YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
            YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
            YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
            YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

    std::string yyres;
    // Argument number.
    std::ptrdiff_t yyi = 0;
    for (char const* yyp = yyformat; *yyp; ++yyp)
        if (yyp[0] == '%' && yyp[1] == 's' && yyi < yycount) {
            yyres += symbol_name(yyarg[yyi++]);
            ++yyp;
        }
        else
            yyres += *yyp;
    return yyres;
}


const signed char parser::yypact_ninf_ = -50;

const signed char parser::yytable_ninf_ = -1;

const short parser::yypact_[] = {
    52,  -50, -50, -50, -50, -50, -50, -50, -50, 52,  -50, -50, -50, -50, -50, -50, -50, -50, -50, -50, 52,  11,  -11,
    -1,  -50, -8,  -50, -50, -50, -50, -12, -50, -18, -50, 52,  16,  52,  -50, -50, -50, -50, -50, -50, -50, -50, -50,
    -50, 85,  151, 118, -12, -50, -50, -50, -50, -50, -50, -25, -50, -1,  10,  12,  13,  -50, -50, -50, -50, -50, 151,
    -50, -50, 151, -50, -9,  -10, -50, -50, -50, 6,   -50, -50, -50, -50, -50, -50, -50, -50, -50, 17,  2,   -12, 3,
    -12, 20,  -12, -50, -50, -5,  -50, -50, -9,  -50, -50, -12, -50, -50, -50, -12, -5,  -9,  -50};

const signed char parser::yydefact_[] = {
    70, 48, 49, 44, 45, 46, 50, 51, 52, 70, 39, 37, 38, 35, 36, 40, 41, 42, 43, 47, 70, 0,  21, 3,  5,  0,  16, 15,
    14, 70, 0,  12, 0,  1,  70, 2,  70, 60, 61, 62, 64, 65, 63, 66, 67, 68, 69, 70, 70, 70, 0,  73, 74, 75, 76, 77,
    71, 53, 13, 4,  0,  0,  0,  22, 24, 23, 25, 6,  70, 7,  9,  70, 10, 17, 72, 18, 70, 70, 0,  8,  11, 72, 55, 54,
    56, 57, 58, 59, 20, 0,  0,  0,  0,  0,  0,  29, 70, 0,  26, 70, 27, 32, 19, 0,  33, 34, 30, 0,  0,  28, 31};

const signed char parser::yypgoto_[] = {-50, -50, 19,  27,  -7,  -22, -50, -50, -50, -50, -50, -50, -50,
                                        -50, -43, -50, -50, -50, -50, -50, -50, -50, -50, -29, -50, -49};

const signed char parser::yydefgoto_[] = {-1, 21,  22, 23, 24, 25, 26, 35, 63, 64, 91, 65, 89,
                                          66, 106, 27, 28, 29, 75, 88, 47, 48, 49, 30, 56, 57};

const signed char parser::yytable_[] = {
    50, 73, 31, 104, 105, 37, 38, 39, 40,  41, 42, 33, 34, 82, 83, 84,  85, 86,  87,  34, 51,  60, 61, 62, 74,
    69, 70, 72, 36,  67,  58, 52, 53, 54,  55, 43, 44, 45, 46, 32, 81,  97, 93,  100, 81, 102, 79, 90, 92, 80,
    95, 98, 96, 99,  108, 1,  2,  76, 109, 77, 78, 59, 3,  4,  5,  110, 94, 103, 101, 0,  107, 0,  6,  7,  8,
    0,  0,  0,  0,   0,   0,  0,  0,  9,   0,  10, 11, 12, 13, 14, 15,  16, 17,  18,  19, 3,   4,  5,  0,  20,
    0,  0,  0,  0,   68,  6,  7,  8,  0,   0,  0,  0,  0,  0,  0,  0,   0,  0,   10,  11, 12,  13, 14, 15, 16,
    17, 18, 19, 3,   4,   5,  0,  0,  0,   0,  0,  0,  71, 6,  7,  8,   0,  0,   0,   0,  0,   0,  0,  0,  0,
    0,  10, 11, 12,  13,  14, 15, 16, 17,  18, 19, 3,  4,  5,  0,  0,   0,  0,   0,   0,  0,   6,  7,  8,  0,
    0,  0,  0,  0,   0,   0,  0,  0,  0,   10, 11, 12, 13, 14, 15, 16,  17, 18,  19};

const signed char parser::yycheck_[] = {
    29, 50, 9,   8,  9,  13, 14, 15, 16, 17,  18, 0,  30, 23, 24, 25, 26, 27, 28, 30, 32, 5,  6,  7,  49, 47, 48,  49,
    29, 36, 48,  43, 44, 45, 46, 43, 44, 45,  46, 20, 49, 90, 36, 92, 49, 94, 68, 76, 77, 71, 48, 48, 50, 50, 103, 3,
    4,  47, 107, 47, 47, 34, 10, 11, 12, 108, 49, 96, 48, -1, 99, -1, 20, 21, 22, -1, -1, -1, -1, -1, -1, -1, -1,  31,
    -1, 33, 34,  35, 36, 37, 38, 39, 40, 41,  42, 10, 11, 12, -1, 47, -1, -1, -1, -1, 19, 20, 21, 22, -1, -1, -1,  -1,
    -1, -1, -1,  -1, -1, -1, 33, 34, 35, 36,  37, 38, 39, 40, 41, 42, 10, 11, 12, -1, -1, -1, -1, -1, -1, 19, 20,  21,
    22, -1, -1,  -1, -1, -1, -1, -1, -1, -1,  -1, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 10, 11, 12, -1, -1, -1,  -1,
    -1, -1, -1,  20, 21, 22, -1, -1, -1, -1,  -1, -1, -1, -1, -1, -1, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42};

const signed char parser::yystos_[] = {
    0,  3,  4,  10, 11, 12, 20, 21, 22, 31, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 47, 52, 53, 54, 55, 56, 57, 66,
    67, 68, 74, 55, 53, 0,  30, 58, 29, 13, 14, 15, 16, 17, 18, 43, 44, 45, 46, 71, 72, 73, 74, 32, 43, 44, 45, 46,
    75, 76, 48, 54, 5,  6,  7,  59, 60, 62, 64, 55, 19, 56, 56, 19, 56, 76, 49, 69, 47, 47, 47, 56, 56, 49, 23, 24,
    25, 26, 27, 28, 70, 63, 74, 61, 74, 36, 49, 48, 50, 76, 48, 50, 76, 48, 76, 74, 8,  9,  65, 74, 76, 76, 65};

const signed char parser::yyr1_[] = {0,  51, 52, 53, 53, 54, 54, 55, 55, 55, 55, 55, 55, 55, 55, 56, 56, 57, 57, 57,
                                     57, 58, 58, 59, 59, 59, 60, 61, 61, 62, 63, 63, 64, 65, 65, 66, 66, 66, 66, 66,
                                     66, 66, 66, 66, 66, 66, 66, 66, 67, 67, 68, 68, 68, 69, 69, 69, 70, 70, 70, 70,
                                     71, 71, 72, 72, 72, 72, 73, 73, 73, 73, 74, 74, 75, 76, 76, 76, 76, 76};

const signed char parser::yyr2_[] = {0, 2, 2, 1, 3, 1, 3, 3, 4, 3, 3, 4, 2, 3, 1, 1, 1, 3, 3, 6, 4, 0, 2, 1, 1, 1,
                                     4, 2, 4, 4, 3, 5, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                     1, 0, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 2, 2, 1, 1, 1, 1, 1};


#if YYDEBUG || 1
// YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
// First, the terminals, then, starting at \a YYNTOKENS, nonterminals.
const char* const parser::yytname_[] = {"\"end of file\"",
                                        "error",
                                        "\"invalid token\"",
                                        "\"truepredicate\"",
                                        "\"falsepredicate\"",
                                        "\"sort\"",
                                        "\"distinct\"",
                                        "\"limit\"",
                                        "\"ascending\"",
                                        "\"descending\"",
                                        "\"true\"",
                                        "\"false\"",
                                        "\"null\"",
                                        "\"==\"",
                                        "\"!=\"",
                                        "\"<\"",
                                        "\">\"",
                                        "\">=\"",
                                        "\"<=\"",
                                        "\"[c]\"",
                                        "\"any\"",
                                        "\"all\"",
                                        "\"none\"",
                                        "\"@size\"",
                                        "\"@count\"",
                                        "\"@max\"",
                                        "\"@min\"",
                                        "\"@sun\"",
                                        "\"@average\"",
                                        "\"&&\"",
                                        "\"||\"",
                                        "\"!\"",
                                        "\"identifier\"",
                                        "\"string\"",
                                        "\"infinity\"",
                                        "\"NaN\"",
                                        "\"natural0\"",
                                        "\"number\"",
                                        "\"float\"",
                                        "\"date\"",
                                        "\"UUID\"",
                                        "\"ObjectId\"",
                                        "\"argument\"",
                                        "\"beginswith\"",
                                        "\"endswith\"",
                                        "\"contains\"",
                                        "\"like\"",
                                        "'('",
                                        "')'",
                                        "'.'",
                                        "','",
                                        "$accept",
                                        "query",
                                        "pred",
                                        "and_pred",
                                        "atom_pred",
                                        "value",
                                        "prop",
                                        "pred_suffix",
                                        "atom_suffix",
                                        "distinct",
                                        "distinct_param",
                                        "sort",
                                        "sort_param",
                                        "limit",
                                        "direction",
                                        "constant",
                                        "boolexpr",
                                        "comp_type",
                                        "post_op",
                                        "aggr_op",
                                        "equality",
                                        "relational",
                                        "stringop",
                                        "path",
                                        "path_elem",
                                        "id",
                                        YY_NULLPTR};
#endif


#if YYDEBUG
const unsigned char parser::yyrline_[] = {
    0,   120, 120, 123, 124, 127, 128, 131, 132, 137, 138, 139, 144, 145, 146, 149, 150, 153, 154, 155,
    156, 159, 160, 163, 164, 165, 167, 170, 171, 173, 176, 177, 179, 182, 183, 186, 187, 188, 189, 190,
    191, 192, 193, 194, 195, 196, 197, 198, 201, 202, 205, 206, 207, 210, 211, 212, 215, 216, 217, 218,
    221, 222, 225, 226, 227, 228, 231, 232, 233, 234, 237, 238, 241, 244, 245, 246, 247, 248};

void parser::yy_stack_print_() const
{
    *yycdebug_ << "Stack now";
    for (stack_type::const_iterator i = yystack_.begin(), i_end = yystack_.end(); i != i_end; ++i)
        *yycdebug_ << ' ' << int(i->state);
    *yycdebug_ << '\n';
}

void parser::yy_reduce_print_(int yyrule) const
{
    int yylno = yyrline_[yyrule];
    int yynrhs = yyr2_[yyrule];
    // Print the symbols being reduced, and their result.
    *yycdebug_ << "Reducing stack by rule " << yyrule - 1 << " (line " << yylno << "):\n";
    // The symbols being reduced.
    for (int yyi = 0; yyi < yynrhs; yyi++)
        YY_SYMBOL_PRINT("   $" << yyi + 1 << " =", yystack_[(yynrhs) - (yyi + 1)]);
}
#endif // YYDEBUG


} // namespace yy


void yy::parser::error(const std::string& m)
{
    drv.error(m);
}
