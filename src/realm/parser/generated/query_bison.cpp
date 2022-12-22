// A Bison parser, made by GNU Bison 3.8.2.

// Skeleton implementation for Bison LALR(1) parsers in C++

// Copyright (C) 2002-2015, 2018-2021 Free Software Foundation, Inc.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

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

#ifdef _MSC_VER
// ignore msvc warnings in this file (poped at end)
// do this by setting the warning level to 1 (permissive)
#pragma warning( push, 1 )
#endif



#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> // FIXME: INFRINGES ON USER NAME SPACE.
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif


// Whether we are compiled with exception support.
#ifndef YY_EXCEPTIONS
# if defined __GNUC__ && !defined __EXCEPTIONS
#  define YY_EXCEPTIONS 0
# else
#  define YY_EXCEPTIONS 1
# endif
#endif



// Enable debugging if requested.
#if YYDEBUG

// A pseudo ostream that takes yydebug_ into account.
# define YYCDEBUG if (yydebug_) (*yycdebug_)

# define YY_SYMBOL_PRINT(Title, Symbol)         \
  do {                                          \
    if (yydebug_)                               \
    {                                           \
      *yycdebug_ << Title << ' ';               \
      yy_print_ (*yycdebug_, Symbol);           \
      *yycdebug_ << '\n';                       \
    }                                           \
  } while (false)

# define YY_REDUCE_PRINT(Rule)          \
  do {                                  \
    if (yydebug_)                       \
      yy_reduce_print_ (Rule);          \
  } while (false)

# define YY_STACK_PRINT()               \
  do {                                  \
    if (yydebug_)                       \
      yy_stack_print_ ();                \
  } while (false)

#else // !YYDEBUG

# define YYCDEBUG if (false) std::cerr
# define YY_SYMBOL_PRINT(Title, Symbol)  YY_USE (Symbol)
# define YY_REDUCE_PRINT(Rule)           static_cast<void> (0)
# define YY_STACK_PRINT()                static_cast<void> (0)

#endif // !YYDEBUG

#define yyerrok         (yyerrstatus_ = 0)
#define yyclearin       (yyla.clear ())

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYRECOVERING()  (!!yyerrstatus_)

namespace yy {

  /// Build a parser object.
  parser::parser (ParserDriver& drv_yyarg, void* scanner_yyarg)
#if YYDEBUG
    : yydebug_ (false),
      yycdebug_ (&std::cerr),
#else
    :
#endif
      drv (drv_yyarg),
      scanner (scanner_yyarg)
  {}

  parser::~parser ()
  {}

  parser::syntax_error::~syntax_error () YY_NOEXCEPT YY_NOTHROW
  {}

  /*---------.
  | symbol.  |
  `---------*/



  // by_state.
  parser::by_state::by_state () YY_NOEXCEPT
    : state (empty_state)
  {}

  parser::by_state::by_state (const by_state& that) YY_NOEXCEPT
    : state (that.state)
  {}

  void
  parser::by_state::clear () YY_NOEXCEPT
  {
    state = empty_state;
  }

  void
  parser::by_state::move (by_state& that)
  {
    state = that.state;
    that.clear ();
  }

  parser::by_state::by_state (state_type s) YY_NOEXCEPT
    : state (s)
  {}

  parser::symbol_kind_type
  parser::by_state::kind () const YY_NOEXCEPT
  {
    if (state == empty_state)
      return symbol_kind::SYM_YYEMPTY;
    else
      return YY_CAST (symbol_kind_type, yystos_[+state]);
  }

  parser::stack_symbol_type::stack_symbol_type ()
  {}

  parser::stack_symbol_type::stack_symbol_type (YY_RVREF (stack_symbol_type) that)
    : super_type (YY_MOVE (that.state))
  {
    switch (that.kind ())
    {
      case symbol_kind::SYM_aggregate: // aggregate
        value.YY_MOVE_OR_COPY< AggrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.YY_MOVE_OR_COPY< ConstantNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.YY_MOVE_OR_COPY< DescriptorNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.YY_MOVE_OR_COPY< DescriptorOrderingNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_expr: // expr
        value.YY_MOVE_OR_COPY< ExpressionNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.YY_MOVE_OR_COPY< ListNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path_elem: // path_elem
        value.YY_MOVE_OR_COPY< PathElem > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.YY_MOVE_OR_COPY< PathNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.YY_MOVE_OR_COPY< PostOpNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.YY_MOVE_OR_COPY< PropertyNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.YY_MOVE_OR_COPY< QueryNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.YY_MOVE_OR_COPY< SubqueryNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.YY_MOVE_OR_COPY< TrueOrFalseNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_value: // value
        value.YY_MOVE_OR_COPY< ValueNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_direction: // direction
        value.YY_MOVE_OR_COPY< bool > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.YY_MOVE_OR_COPY< int > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_ID: // "identifier"
      case symbol_kind::SYM_STRING: // "string"
      case symbol_kind::SYM_BASE64: // "base64"
      case symbol_kind::SYM_INFINITY: // "infinity"
      case symbol_kind::SYM_NAN: // "NaN"
      case symbol_kind::SYM_NATURAL0: // "natural0"
      case symbol_kind::SYM_NUMBER: // "number"
      case symbol_kind::SYM_FLOAT: // "float"
      case symbol_kind::SYM_TIMESTAMP: // "date"
      case symbol_kind::SYM_UUID: // "UUID"
      case symbol_kind::SYM_OID: // "ObjectId"
      case symbol_kind::SYM_LINK: // "link"
      case symbol_kind::SYM_TYPED_LINK: // "typed link"
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_id: // id
        value.YY_MOVE_OR_COPY< std::string > (YY_MOVE (that.value));
        break;

      default:
        break;
    }

#if 201103L <= YY_CPLUSPLUS
    // that is emptied.
    that.state = empty_state;
#endif
  }

  parser::stack_symbol_type::stack_symbol_type (state_type s, YY_MOVE_REF (symbol_type) that)
    : super_type (s)
  {
    switch (that.kind ())
    {
      case symbol_kind::SYM_aggregate: // aggregate
        value.move< AggrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.move< ConstantNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.move< DescriptorOrderingNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_expr: // expr
        value.move< ExpressionNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.move< ListNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path_elem: // path_elem
        value.move< PathElem > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.move< PropertyNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.move< QueryNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.move< SubqueryNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.move< TrueOrFalseNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_value: // value
        value.move< ValueNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_direction: // direction
        value.move< bool > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.move< int > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_ID: // "identifier"
      case symbol_kind::SYM_STRING: // "string"
      case symbol_kind::SYM_BASE64: // "base64"
      case symbol_kind::SYM_INFINITY: // "infinity"
      case symbol_kind::SYM_NAN: // "NaN"
      case symbol_kind::SYM_NATURAL0: // "natural0"
      case symbol_kind::SYM_NUMBER: // "number"
      case symbol_kind::SYM_FLOAT: // "float"
      case symbol_kind::SYM_TIMESTAMP: // "date"
      case symbol_kind::SYM_UUID: // "UUID"
      case symbol_kind::SYM_OID: // "ObjectId"
      case symbol_kind::SYM_LINK: // "link"
      case symbol_kind::SYM_TYPED_LINK: // "typed link"
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_id: // id
        value.move< std::string > (YY_MOVE (that.value));
        break;

      default:
        break;
    }

    // that is emptied.
    that.kind_ = symbol_kind::SYM_YYEMPTY;
  }

#if YY_CPLUSPLUS < 201103L
  parser::stack_symbol_type&
  parser::stack_symbol_type::operator= (const stack_symbol_type& that)
  {
    state = that.state;
    switch (that.kind ())
    {
      case symbol_kind::SYM_aggregate: // aggregate
        value.copy< AggrNode* > (that.value);
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.copy< ConstantNode* > (that.value);
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.copy< DescriptorNode* > (that.value);
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.copy< DescriptorOrderingNode* > (that.value);
        break;

      case symbol_kind::SYM_expr: // expr
        value.copy< ExpressionNode* > (that.value);
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.copy< ListNode* > (that.value);
        break;

      case symbol_kind::SYM_path_elem: // path_elem
        value.copy< PathElem > (that.value);
        break;

      case symbol_kind::SYM_path: // path
        value.copy< PathNode* > (that.value);
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.copy< PostOpNode* > (that.value);
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.copy< PropertyNode* > (that.value);
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.copy< QueryNode* > (that.value);
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.copy< SubqueryNode* > (that.value);
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.copy< TrueOrFalseNode* > (that.value);
        break;

      case symbol_kind::SYM_value: // value
        value.copy< ValueNode* > (that.value);
        break;

      case symbol_kind::SYM_direction: // direction
        value.copy< bool > (that.value);
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.copy< int > (that.value);
        break;

      case symbol_kind::SYM_ID: // "identifier"
      case symbol_kind::SYM_STRING: // "string"
      case symbol_kind::SYM_BASE64: // "base64"
      case symbol_kind::SYM_INFINITY: // "infinity"
      case symbol_kind::SYM_NAN: // "NaN"
      case symbol_kind::SYM_NATURAL0: // "natural0"
      case symbol_kind::SYM_NUMBER: // "number"
      case symbol_kind::SYM_FLOAT: // "float"
      case symbol_kind::SYM_TIMESTAMP: // "date"
      case symbol_kind::SYM_UUID: // "UUID"
      case symbol_kind::SYM_OID: // "ObjectId"
      case symbol_kind::SYM_LINK: // "link"
      case symbol_kind::SYM_TYPED_LINK: // "typed link"
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_id: // id
        value.copy< std::string > (that.value);
        break;

      default:
        break;
    }

    return *this;
  }

  parser::stack_symbol_type&
  parser::stack_symbol_type::operator= (stack_symbol_type& that)
  {
    state = that.state;
    switch (that.kind ())
    {
      case symbol_kind::SYM_aggregate: // aggregate
        value.move< AggrNode* > (that.value);
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.move< ConstantNode* > (that.value);
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (that.value);
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.move< DescriptorOrderingNode* > (that.value);
        break;

      case symbol_kind::SYM_expr: // expr
        value.move< ExpressionNode* > (that.value);
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.move< ListNode* > (that.value);
        break;

      case symbol_kind::SYM_path_elem: // path_elem
        value.move< PathElem > (that.value);
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (that.value);
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (that.value);
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.move< PropertyNode* > (that.value);
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.move< QueryNode* > (that.value);
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.move< SubqueryNode* > (that.value);
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.move< TrueOrFalseNode* > (that.value);
        break;

      case symbol_kind::SYM_value: // value
        value.move< ValueNode* > (that.value);
        break;

      case symbol_kind::SYM_direction: // direction
        value.move< bool > (that.value);
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.move< int > (that.value);
        break;

      case symbol_kind::SYM_ID: // "identifier"
      case symbol_kind::SYM_STRING: // "string"
      case symbol_kind::SYM_BASE64: // "base64"
      case symbol_kind::SYM_INFINITY: // "infinity"
      case symbol_kind::SYM_NAN: // "NaN"
      case symbol_kind::SYM_NATURAL0: // "natural0"
      case symbol_kind::SYM_NUMBER: // "number"
      case symbol_kind::SYM_FLOAT: // "float"
      case symbol_kind::SYM_TIMESTAMP: // "date"
      case symbol_kind::SYM_UUID: // "UUID"
      case symbol_kind::SYM_OID: // "ObjectId"
      case symbol_kind::SYM_LINK: // "link"
      case symbol_kind::SYM_TYPED_LINK: // "typed link"
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_id: // id
        value.move< std::string > (that.value);
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
  void
  parser::yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const
  {
    if (yymsg)
      YY_SYMBOL_PRINT (yymsg, yysym);
  }

#if YYDEBUG
  template <typename Base>
  void
  parser::yy_print_ (std::ostream& yyo, const basic_symbol<Base>& yysym) const
  {
    std::ostream& yyoutput = yyo;
    YY_USE (yyoutput);
    if (yysym.empty ())
      yyo << "empty symbol";
    else
      {
        symbol_kind_type yykind = yysym.kind ();
        yyo << (yykind < YYNTOKENS ? "token" : "nterm")
            << ' ' << yysym.name () << " (";
        switch (yykind)
    {
      case symbol_kind::SYM_YYEOF: // "end of file"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_TRUEPREDICATE: // "truepredicate"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_FALSEPREDICATE: // "falsepredicate"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_SUBQUERY: // "subquery"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_TRUE: // "true"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_FALSE: // "false"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_NULL_VAL: // "null"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_EQUAL: // "=="
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_NOT_EQUAL: // "!="
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_LESS: // "<"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_GREATER: // ">"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_GREATER_EQUAL: // ">="
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_LESS_EQUAL: // "<="
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_CASE: // "[c]"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_ANY: // "any"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_ALL: // "all"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_NONE: // "none"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_BACKLINK: // "@links"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_MAX: // "@max"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_MIN: // "@min"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_SUM: // "@sun"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_AVG: // "@average"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_AND: // "&&"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_OR: // "||"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_NOT: // "!"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_ID: // "identifier"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_STRING: // "string"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_BASE64: // "base64"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_INFINITY: // "infinity"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_NAN: // "NaN"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_NATURAL0: // "natural0"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_NUMBER: // "number"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_FLOAT: // "float"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_TIMESTAMP: // "date"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_UUID: // "UUID"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_OID: // "ObjectId"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_LINK: // "link"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_TYPED_LINK: // "typed link"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_ARG: // "argument"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_ENDSWITH: // "endswith"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_CONTAINS: // "contains"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_TEXT: // "fulltext"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_LIKE: // "like"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_BETWEEN: // "between"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_IN: // "in"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_OBJ: // "obj"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_SORT: // "sort"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_DISTINCT: // "distinct"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_LIMIT: // "limit"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_ASCENDING: // "ascending"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_DESCENDING: // "descending"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_SIZE: // "@size"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_TYPE: // "@type"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_KEY_VAL: // "key or value"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_57_: // '+'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_58_: // '-'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_59_: // '*'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_60_: // '/'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_61_: // '('
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_62_: // ')'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_63_: // '.'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_64_: // ','
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_65_: // '{'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_66_: // '}'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_67_: // '['
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_68_: // ']'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_final: // final
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_query: // query
                 { yyo << yysym.value.template as < QueryNode* > (); }
        break;

      case symbol_kind::SYM_compare: // compare
                 { yyo << yysym.value.template as < QueryNode* > (); }
        break;

      case symbol_kind::SYM_expr: // expr
                 { yyo << yysym.value.template as < ExpressionNode* > (); }
        break;

      case symbol_kind::SYM_value: // value
                 { yyo << yysym.value.template as < ValueNode* > (); }
        break;

      case symbol_kind::SYM_prop: // prop
                 { yyo << yysym.value.template as < PropertyNode* > (); }
        break;

      case symbol_kind::SYM_aggregate: // aggregate
                 { yyo << yysym.value.template as < AggrNode* > (); }
        break;

      case symbol_kind::SYM_simple_prop: // simple_prop
                 { yyo << yysym.value.template as < PropertyNode* > (); }
        break;

      case symbol_kind::SYM_subquery: // subquery
                 { yyo << yysym.value.template as < SubqueryNode* > (); }
        break;

      case symbol_kind::SYM_post_query: // post_query
                 { yyo << yysym.value.template as < DescriptorOrderingNode* > (); }
        break;

      case symbol_kind::SYM_distinct: // distinct
                 { yyo << yysym.value.template as < DescriptorNode* > (); }
        break;

      case symbol_kind::SYM_distinct_param: // distinct_param
                 { yyo << yysym.value.template as < DescriptorNode* > (); }
        break;

      case symbol_kind::SYM_sort: // sort
                 { yyo << yysym.value.template as < DescriptorNode* > (); }
        break;

      case symbol_kind::SYM_sort_param: // sort_param
                 { yyo << yysym.value.template as < DescriptorNode* > (); }
        break;

      case symbol_kind::SYM_limit: // limit
                 { yyo << yysym.value.template as < DescriptorNode* > (); }
        break;

      case symbol_kind::SYM_direction: // direction
                 { yyo << yysym.value.template as < bool > (); }
        break;

      case symbol_kind::SYM_list: // list
                 { yyo << yysym.value.template as < ListNode* > (); }
        break;

      case symbol_kind::SYM_list_content: // list_content
                 { yyo << yysym.value.template as < ListNode* > (); }
        break;

      case symbol_kind::SYM_constant: // constant
                 { yyo << yysym.value.template as < ConstantNode* > (); }
        break;

      case symbol_kind::SYM_primary_key: // primary_key
                 { yyo << yysym.value.template as < ConstantNode* > (); }
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
                 { yyo << yysym.value.template as < TrueOrFalseNode* > (); }
        break;

      case symbol_kind::SYM_comp_type: // comp_type
                 { yyo << yysym.value.template as < int > (); }
        break;

      case symbol_kind::SYM_post_op: // post_op
                 { yyo << yysym.value.template as < PostOpNode* > (); }
        break;

      case symbol_kind::SYM_aggr_op: // aggr_op
                 { yyo << yysym.value.template as < int > (); }
        break;

      case symbol_kind::SYM_equality: // equality
                 { yyo << yysym.value.template as < int > (); }
        break;

      case symbol_kind::SYM_relational: // relational
                 { yyo << yysym.value.template as < int > (); }
        break;

      case symbol_kind::SYM_stringop: // stringop
                 { yyo << yysym.value.template as < int > (); }
        break;

      case symbol_kind::SYM_path: // path
                 { yyo << yysym.value.template as < PathNode* > (); }
        break;

      case symbol_kind::SYM_path_elem: // path_elem
                 { yyo << yysym.value.template as < PathElem > ().id; }
        break;

      case symbol_kind::SYM_id: // id
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      default:
        break;
    }
        yyo << ')';
      }
  }
#endif

  void
  parser::yypush_ (const char* m, YY_MOVE_REF (stack_symbol_type) sym)
  {
    if (m)
      YY_SYMBOL_PRINT (m, sym);
    yystack_.push (YY_MOVE (sym));
  }

  void
  parser::yypush_ (const char* m, state_type s, YY_MOVE_REF (symbol_type) sym)
  {
#if 201103L <= YY_CPLUSPLUS
    yypush_ (m, stack_symbol_type (s, std::move (sym)));
#else
    stack_symbol_type ss (s, sym);
    yypush_ (m, ss);
#endif
  }

  void
  parser::yypop_ (int n) YY_NOEXCEPT
  {
    yystack_.pop (n);
  }

#if YYDEBUG
  std::ostream&
  parser::debug_stream () const
  {
    return *yycdebug_;
  }

  void
  parser::set_debug_stream (std::ostream& o)
  {
    yycdebug_ = &o;
  }


  parser::debug_level_type
  parser::debug_level () const
  {
    return yydebug_;
  }

  void
  parser::set_debug_level (debug_level_type l)
  {
    yydebug_ = l;
  }
#endif // YYDEBUG

  parser::state_type
  parser::yy_lr_goto_state_ (state_type yystate, int yysym)
  {
    int yyr = yypgoto_[yysym - YYNTOKENS] + yystate;
    if (0 <= yyr && yyr <= yylast_ && yycheck_[yyr] == yystate)
      return yytable_[yyr];
    else
      return yydefgoto_[yysym - YYNTOKENS];
  }

  bool
  parser::yy_pact_value_is_default_ (int yyvalue) YY_NOEXCEPT
  {
    return yyvalue == yypact_ninf_;
  }

  bool
  parser::yy_table_value_is_error_ (int yyvalue) YY_NOEXCEPT
  {
    return yyvalue == yytable_ninf_;
  }

  int
  parser::operator() ()
  {
    return parse ();
  }

  int
  parser::parse ()
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
    yystack_.clear ();
    yypush_ (YY_NULLPTR, 0, YY_MOVE (yyla));

  /*-----------------------------------------------.
  | yynewstate -- push a new symbol on the stack.  |
  `-----------------------------------------------*/
  yynewstate:
    YYCDEBUG << "Entering state " << int (yystack_[0].state) << '\n';
    YY_STACK_PRINT ();

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
    if (yy_pact_value_is_default_ (yyn))
      goto yydefault;

    // Read a lookahead token.
    if (yyla.empty ())
      {
        YYCDEBUG << "Reading a token\n";
#if YY_EXCEPTIONS
        try
#endif // YY_EXCEPTIONS
          {
            symbol_type yylookahead (yylex (scanner));
            yyla.move (yylookahead);
          }
#if YY_EXCEPTIONS
        catch (const syntax_error& yyexc)
          {
            YYCDEBUG << "Caught exception: " << yyexc.what() << '\n';
            error (yyexc);
            goto yyerrlab1;
          }
#endif // YY_EXCEPTIONS
      }
    YY_SYMBOL_PRINT ("Next token is", yyla);

    if (yyla.kind () == symbol_kind::SYM_YYerror)
    {
      // The scanner already issued an error message, process directly
      // to error recovery.  But do not keep the error token as
      // lookahead, it is too special and may lead us to an endless
      // loop in error recovery. */
      yyla.kind_ = symbol_kind::SYM_YYUNDEF;
      goto yyerrlab1;
    }

    /* If the proper action on seeing token YYLA.TYPE is to reduce or
       to detect an error, take that action.  */
    yyn += yyla.kind ();
    if (yyn < 0 || yylast_ < yyn || yycheck_[yyn] != yyla.kind ())
      {
        goto yydefault;
      }

    // Reduce or error.
    yyn = yytable_[yyn];
    if (yyn <= 0)
      {
        if (yy_table_value_is_error_ (yyn))
          goto yyerrlab;
        yyn = -yyn;
        goto yyreduce;
      }

    // Count tokens shifted since error; after three, turn off error status.
    if (yyerrstatus_)
      --yyerrstatus_;

    // Shift the lookahead token.
    yypush_ ("Shifting", state_type (yyn), YY_MOVE (yyla));
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
      yylhs.state = yy_lr_goto_state_ (yystack_[yylen].state, yyr1_[yyn]);
      /* Variants are always initialized to an empty instance of the
         correct type. The default '$$ = $1' action is NOT applied
         when using variants.  */
      switch (yyr1_[yyn])
    {
      case symbol_kind::SYM_aggregate: // aggregate
        yylhs.value.emplace< AggrNode* > ();
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        yylhs.value.emplace< ConstantNode* > ();
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        yylhs.value.emplace< DescriptorNode* > ();
        break;

      case symbol_kind::SYM_post_query: // post_query
        yylhs.value.emplace< DescriptorOrderingNode* > ();
        break;

      case symbol_kind::SYM_expr: // expr
        yylhs.value.emplace< ExpressionNode* > ();
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        yylhs.value.emplace< ListNode* > ();
        break;

      case symbol_kind::SYM_path_elem: // path_elem
        yylhs.value.emplace< PathElem > ();
        break;

      case symbol_kind::SYM_path: // path
        yylhs.value.emplace< PathNode* > ();
        break;

      case symbol_kind::SYM_post_op: // post_op
        yylhs.value.emplace< PostOpNode* > ();
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        yylhs.value.emplace< PropertyNode* > ();
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        yylhs.value.emplace< QueryNode* > ();
        break;

      case symbol_kind::SYM_subquery: // subquery
        yylhs.value.emplace< SubqueryNode* > ();
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        yylhs.value.emplace< TrueOrFalseNode* > ();
        break;

      case symbol_kind::SYM_value: // value
        yylhs.value.emplace< ValueNode* > ();
        break;

      case symbol_kind::SYM_direction: // direction
        yylhs.value.emplace< bool > ();
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        yylhs.value.emplace< int > ();
        break;

      case symbol_kind::SYM_ID: // "identifier"
      case symbol_kind::SYM_STRING: // "string"
      case symbol_kind::SYM_BASE64: // "base64"
      case symbol_kind::SYM_INFINITY: // "infinity"
      case symbol_kind::SYM_NAN: // "NaN"
      case symbol_kind::SYM_NATURAL0: // "natural0"
      case symbol_kind::SYM_NUMBER: // "number"
      case symbol_kind::SYM_FLOAT: // "float"
      case symbol_kind::SYM_TIMESTAMP: // "date"
      case symbol_kind::SYM_UUID: // "UUID"
      case symbol_kind::SYM_OID: // "ObjectId"
      case symbol_kind::SYM_LINK: // "link"
      case symbol_kind::SYM_TYPED_LINK: // "typed link"
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_id: // id
        yylhs.value.emplace< std::string > ();
        break;

      default:
        break;
    }



      // Perform the reduction.
      YY_REDUCE_PRINT (yyn);
#if YY_EXCEPTIONS
      try
#endif // YY_EXCEPTIONS
        {
          switch (yyn)
            {
  case 2: // final: query post_query
                       { drv.result = yystack_[1].value.as < QueryNode* > (); drv.ordering = yystack_[0].value.as < DescriptorOrderingNode* > (); }
    break;

  case 3: // query: compare
                                { yylhs.value.as < QueryNode* > () = yystack_[0].value.as < QueryNode* > (); }
    break;

  case 4: // query: query "||" query
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<OrNode>(yystack_[2].value.as < QueryNode* > (), yystack_[0].value.as < QueryNode* > ()); }
    break;

  case 5: // query: query "&&" query
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<AndNode>(yystack_[2].value.as < QueryNode* > (), yystack_[0].value.as < QueryNode* > ()); }
    break;

  case 6: // query: "!" query
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<NotNode>(yystack_[0].value.as < QueryNode* > ()); }
    break;

  case 7: // query: '(' query ')'
                                { yylhs.value.as < QueryNode* > () = yystack_[1].value.as < QueryNode* > (); }
    break;

  case 8: // query: boolexpr
                                { yylhs.value.as < QueryNode* > () =yystack_[0].value.as < TrueOrFalseNode* > (); }
    break;

  case 9: // compare: expr equality expr
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<EqualityNode>(yystack_[2].value.as < ExpressionNode* > (), yystack_[1].value.as < int > (), yystack_[0].value.as < ExpressionNode* > ()); }
    break;

  case 10: // compare: expr equality "[c]" expr
                                {
                                    auto tmp = drv.m_parse_nodes.create<EqualityNode>(yystack_[3].value.as < ExpressionNode* > (), yystack_[2].value.as < int > (), yystack_[0].value.as < ExpressionNode* > ());
                                    tmp->case_sensitive = false;
                                    yylhs.value.as < QueryNode* > () = tmp;
                                }
    break;

  case 11: // compare: expr relational expr
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<RelationalNode>(yystack_[2].value.as < ExpressionNode* > (), yystack_[1].value.as < int > (), yystack_[0].value.as < ExpressionNode* > ()); }
    break;

  case 12: // compare: value stringop value
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<StringOpsNode>(yystack_[2].value.as < ValueNode* > (), yystack_[1].value.as < int > (), yystack_[0].value.as < ValueNode* > ()); }
    break;

  case 13: // compare: value "fulltext" value
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<StringOpsNode>(yystack_[2].value.as < ValueNode* > (), CompareNode::TEXT, yystack_[0].value.as < ValueNode* > ()); }
    break;

  case 14: // compare: value stringop "[c]" value
                                {
                                    auto tmp = drv.m_parse_nodes.create<StringOpsNode>(yystack_[3].value.as < ValueNode* > (), yystack_[2].value.as < int > (), yystack_[0].value.as < ValueNode* > ());
                                    tmp->case_sensitive = false;
                                    yylhs.value.as < QueryNode* > () = tmp;
                                }
    break;

  case 15: // compare: value "between" list
                                { yylhs.value.as < QueryNode* > () = drv.m_parse_nodes.create<BetweenNode>(yystack_[2].value.as < ValueNode* > (), yystack_[0].value.as < ListNode* > ()); }
    break;

  case 16: // expr: value
                                { yylhs.value.as < ExpressionNode* > () = yystack_[0].value.as < ValueNode* > (); }
    break;

  case 17: // expr: '(' expr ')'
                                { yylhs.value.as < ExpressionNode* > () = yystack_[1].value.as < ExpressionNode* > (); }
    break;

  case 18: // expr: expr '*' expr
                                { yylhs.value.as < ExpressionNode* > () = drv.m_parse_nodes.create<OperationNode>(yystack_[2].value.as < ExpressionNode* > (), '*', yystack_[0].value.as < ExpressionNode* > ()); }
    break;

  case 19: // expr: expr '/' expr
                                { yylhs.value.as < ExpressionNode* > () = drv.m_parse_nodes.create<OperationNode>(yystack_[2].value.as < ExpressionNode* > (), '/', yystack_[0].value.as < ExpressionNode* > ()); }
    break;

  case 20: // expr: expr '+' expr
                                { yylhs.value.as < ExpressionNode* > () = drv.m_parse_nodes.create<OperationNode>(yystack_[2].value.as < ExpressionNode* > (), '+', yystack_[0].value.as < ExpressionNode* > ()); }
    break;

  case 21: // expr: expr '-' expr
                                { yylhs.value.as < ExpressionNode* > () = drv.m_parse_nodes.create<OperationNode>(yystack_[2].value.as < ExpressionNode* > (), '-', yystack_[0].value.as < ExpressionNode* > ()); }
    break;

  case 22: // value: constant
                                { yylhs.value.as < ValueNode* > () = yystack_[0].value.as < ConstantNode* > ();}
    break;

  case 23: // value: prop
                                { yylhs.value.as < ValueNode* > () = yystack_[0].value.as < PropertyNode* > ();}
    break;

  case 24: // value: list
                                { yylhs.value.as < ValueNode* > () = yystack_[0].value.as < ListNode* > ();}
    break;

  case 25: // value: aggregate
                                { yylhs.value.as < ValueNode* > () = yystack_[0].value.as < AggrNode* > ();}
    break;

  case 26: // value: subquery
                                { yylhs.value.as < ValueNode* > () = yystack_[0].value.as < SubqueryNode* > ();}
    break;

  case 27: // prop: path post_op
                                { yylhs.value.as < PropertyNode* > () = drv.m_parse_nodes.create<PropertyNode>(yystack_[1].value.as < PathNode* > ()); yylhs.value.as < PropertyNode* > ()->add_postop(yystack_[0].value.as < PostOpNode* > ()); }
    break;

  case 28: // prop: comp_type path post_op
                                { yylhs.value.as < PropertyNode* > () = drv.m_parse_nodes.create<PropertyNode>(yystack_[1].value.as < PathNode* > (), ExpressionComparisonType(yystack_[2].value.as < int > ())); yylhs.value.as < PropertyNode* > ()->add_postop(yystack_[0].value.as < PostOpNode* > ()); }
    break;

  case 29: // aggregate: path aggr_op '.' id
                                {
                                    auto prop = drv.m_parse_nodes.create<PropertyNode>(yystack_[3].value.as < PathNode* > ());
                                    yylhs.value.as < AggrNode* > () = drv.m_parse_nodes.create<LinkAggrNode>(prop, yystack_[2].value.as < int > (), yystack_[0].value.as < std::string > ());
                                }
    break;

  case 30: // aggregate: path aggr_op
                                {
                                    auto prop = drv.m_parse_nodes.create<PropertyNode>(yystack_[1].value.as < PathNode* > ());
                                    yylhs.value.as < AggrNode* > () = drv.m_parse_nodes.create<ListAggrNode>(prop, yystack_[0].value.as < int > ());
                                }
    break;

  case 31: // simple_prop: path
                                { yylhs.value.as < PropertyNode* > () = drv.m_parse_nodes.create<PropertyNode>(yystack_[0].value.as < PathNode* > ()); }
    break;

  case 32: // subquery: "subquery" '(' simple_prop ',' id ',' query ')' '.' "@size"
                                                               { yylhs.value.as < SubqueryNode* > () = drv.m_parse_nodes.create<SubqueryNode>(yystack_[7].value.as < PropertyNode* > (), yystack_[5].value.as < std::string > (), yystack_[3].value.as < QueryNode* > ()); }
    break;

  case 33: // post_query: %empty
                                { yylhs.value.as < DescriptorOrderingNode* > () = drv.m_parse_nodes.create<DescriptorOrderingNode>();}
    break;

  case 34: // post_query: post_query sort
                                { yystack_[1].value.as < DescriptorOrderingNode* > ()->add_descriptor(yystack_[0].value.as < DescriptorNode* > ()); yylhs.value.as < DescriptorOrderingNode* > () = yystack_[1].value.as < DescriptorOrderingNode* > (); }
    break;

  case 35: // post_query: post_query distinct
                                { yystack_[1].value.as < DescriptorOrderingNode* > ()->add_descriptor(yystack_[0].value.as < DescriptorNode* > ()); yylhs.value.as < DescriptorOrderingNode* > () = yystack_[1].value.as < DescriptorOrderingNode* > (); }
    break;

  case 36: // post_query: post_query limit
                                { yystack_[1].value.as < DescriptorOrderingNode* > ()->add_descriptor(yystack_[0].value.as < DescriptorNode* > ()); yylhs.value.as < DescriptorOrderingNode* > () = yystack_[1].value.as < DescriptorOrderingNode* > (); }
    break;

  case 37: // distinct: "distinct" '(' distinct_param ')'
                                          { yylhs.value.as < DescriptorNode* > () = yystack_[1].value.as < DescriptorNode* > (); }
    break;

  case 38: // distinct_param: path
                                { yylhs.value.as < DescriptorNode* > () = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::DISTINCT); yylhs.value.as < DescriptorNode* > ()->add(yystack_[0].value.as < PathNode* > ());}
    break;

  case 39: // distinct_param: distinct_param ',' path
                                { yystack_[2].value.as < DescriptorNode* > ()->add(yystack_[0].value.as < PathNode* > ()); yylhs.value.as < DescriptorNode* > () = yystack_[2].value.as < DescriptorNode* > (); }
    break;

  case 40: // sort: "sort" '(' sort_param ')'
                                { yylhs.value.as < DescriptorNode* > () = yystack_[1].value.as < DescriptorNode* > (); }
    break;

  case 41: // sort_param: path direction
                                { yylhs.value.as < DescriptorNode* > () = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::SORT); yylhs.value.as < DescriptorNode* > ()->add(yystack_[1].value.as < PathNode* > (), yystack_[0].value.as < bool > ());}
    break;

  case 42: // sort_param: sort_param ',' path direction
                                     { yystack_[3].value.as < DescriptorNode* > ()->add(yystack_[1].value.as < PathNode* > (), yystack_[0].value.as < bool > ()); yylhs.value.as < DescriptorNode* > () = yystack_[3].value.as < DescriptorNode* > (); }
    break;

  case 43: // limit: "limit" '(' "natural0" ')'
                                { yylhs.value.as < DescriptorNode* > () = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::LIMIT, yystack_[1].value.as < std::string > ()); }
    break;

  case 44: // direction: "ascending"
                                { yylhs.value.as < bool > () = true; }
    break;

  case 45: // direction: "descending"
                                { yylhs.value.as < bool > () = false; }
    break;

  case 46: // list: '{' list_content '}'
                                        { yylhs.value.as < ListNode* > () = yystack_[1].value.as < ListNode* > (); }
    break;

  case 47: // list: comp_type '{' list_content '}'
                                        { yystack_[1].value.as < ListNode* > ()->set_comp_type(ExpressionComparisonType(yystack_[3].value.as < int > ())); yylhs.value.as < ListNode* > () = yystack_[1].value.as < ListNode* > (); }
    break;

  case 48: // list_content: constant
                                { yylhs.value.as < ListNode* > () = drv.m_parse_nodes.create<ListNode>(yystack_[0].value.as < ConstantNode* > ()); }
    break;

  case 49: // list_content: %empty
                                { yylhs.value.as < ListNode* > () = drv.m_parse_nodes.create<ListNode>(); }
    break;

  case 50: // list_content: list_content ',' constant
                                { yystack_[2].value.as < ListNode* > ()->add_element(yystack_[0].value.as < ConstantNode* > ()); yylhs.value.as < ListNode* > () = yystack_[2].value.as < ListNode* > (); }
    break;

  case 51: // constant: primary_key
                                { yylhs.value.as < ConstantNode* > () = yystack_[0].value.as < ConstantNode* > (); }
    break;

  case 52: // constant: "infinity"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::INFINITY_VAL, yystack_[0].value.as < std::string > ()); }
    break;

  case 53: // constant: "NaN"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NAN_VAL, yystack_[0].value.as < std::string > ()); }
    break;

  case 54: // constant: "base64"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::BASE64, yystack_[0].value.as < std::string > ()); }
    break;

  case 55: // constant: "float"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FLOAT, yystack_[0].value.as < std::string > ()); }
    break;

  case 56: // constant: "date"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TIMESTAMP, yystack_[0].value.as < std::string > ()); }
    break;

  case 57: // constant: "link"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::LINK, yystack_[0].value.as < std::string > ()); }
    break;

  case 58: // constant: "typed link"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TYPED_LINK, yystack_[0].value.as < std::string > ()); }
    break;

  case 59: // constant: "true"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TRUE, ""); }
    break;

  case 60: // constant: "false"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FALSE, ""); }
    break;

  case 61: // constant: "null"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NULL_VAL, ""); }
    break;

  case 62: // constant: "argument"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::ARG, yystack_[0].value.as < std::string > ()); }
    break;

  case 63: // constant: comp_type "argument"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ExpressionComparisonType(yystack_[1].value.as < int > ()), yystack_[0].value.as < std::string > ()); }
    break;

  case 64: // constant: "obj" '(' "string" ',' primary_key ')'
                                { 
                                    auto tmp = yystack_[1].value.as < ConstantNode* > ();
                                    tmp->add_table(yystack_[3].value.as < std::string > ());
                                    yylhs.value.as < ConstantNode* > () = tmp;
                                }
    break;

  case 65: // primary_key: "natural0"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, yystack_[0].value.as < std::string > ()); }
    break;

  case 66: // primary_key: "number"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, yystack_[0].value.as < std::string > ()); }
    break;

  case 67: // primary_key: "string"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::STRING, yystack_[0].value.as < std::string > ()); }
    break;

  case 68: // primary_key: "UUID"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::UUID_T, yystack_[0].value.as < std::string > ()); }
    break;

  case 69: // primary_key: "ObjectId"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::OID, yystack_[0].value.as < std::string > ()); }
    break;

  case 70: // boolexpr: "truepredicate"
                                { yylhs.value.as < TrueOrFalseNode* > () = drv.m_parse_nodes.create<TrueOrFalseNode>(true); }
    break;

  case 71: // boolexpr: "falsepredicate"
                                { yylhs.value.as < TrueOrFalseNode* > () = drv.m_parse_nodes.create<TrueOrFalseNode>(false); }
    break;

  case 72: // comp_type: "any"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::Any); }
    break;

  case 73: // comp_type: "all"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::All); }
    break;

  case 74: // comp_type: "none"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::None); }
    break;

  case 75: // post_op: %empty
                                { yylhs.value.as < PostOpNode* > () = nullptr; }
    break;

  case 76: // post_op: '.' "@size"
                                { yylhs.value.as < PostOpNode* > () = drv.m_parse_nodes.create<PostOpNode>(yystack_[0].value.as < std::string > (), PostOpNode::SIZE);}
    break;

  case 77: // post_op: '.' "@type"
                                { yylhs.value.as < PostOpNode* > () = drv.m_parse_nodes.create<PostOpNode>(yystack_[0].value.as < std::string > (), PostOpNode::TYPE);}
    break;

  case 78: // aggr_op: '.' "@max"
                                { yylhs.value.as < int > () = int(AggrNode::MAX);}
    break;

  case 79: // aggr_op: '.' "@min"
                                { yylhs.value.as < int > () = int(AggrNode::MIN);}
    break;

  case 80: // aggr_op: '.' "@sun"
                                { yylhs.value.as < int > () = int(AggrNode::SUM);}
    break;

  case 81: // aggr_op: '.' "@average"
                                { yylhs.value.as < int > () = int(AggrNode::AVG);}
    break;

  case 82: // equality: "=="
                                { yylhs.value.as < int > () = CompareNode::EQUAL; }
    break;

  case 83: // equality: "!="
                                { yylhs.value.as < int > () = CompareNode::NOT_EQUAL; }
    break;

  case 84: // equality: "in"
                                { yylhs.value.as < int > () = CompareNode::IN; }
    break;

  case 85: // relational: "<"
                                { yylhs.value.as < int > () = CompareNode::LESS; }
    break;

  case 86: // relational: "<="
                                { yylhs.value.as < int > () = CompareNode::LESS_EQUAL; }
    break;

  case 87: // relational: ">"
                                { yylhs.value.as < int > () = CompareNode::GREATER; }
    break;

  case 88: // relational: ">="
                                { yylhs.value.as < int > () = CompareNode::GREATER_EQUAL; }
    break;

  case 89: // stringop: "beginswith"
                                { yylhs.value.as < int > () = CompareNode::BEGINSWITH; }
    break;

  case 90: // stringop: "endswith"
                                { yylhs.value.as < int > () = CompareNode::ENDSWITH; }
    break;

  case 91: // stringop: "contains"
                                { yylhs.value.as < int > () = CompareNode::CONTAINS; }
    break;

  case 92: // stringop: "like"
                                { yylhs.value.as < int > () = CompareNode::LIKE; }
    break;

  case 93: // path: path_elem
                                { yylhs.value.as < PathNode* > () = drv.m_parse_nodes.create<PathNode>(yystack_[0].value.as < PathElem > ()); }
    break;

  case 94: // path: path '.' path_elem
                                { yystack_[2].value.as < PathNode* > ()->add_element(yystack_[0].value.as < PathElem > ()); yylhs.value.as < PathNode* > () = yystack_[2].value.as < PathNode* > (); }
    break;

  case 95: // path_elem: id
                                { yylhs.value.as < PathElem > () = PathElem{yystack_[0].value.as < std::string > ()}; }
    break;

  case 96: // path_elem: id '[' "natural0" ']'
                                { yylhs.value.as < PathElem > () = PathElem{yystack_[3].value.as < std::string > (), int64_t(strtoll(yystack_[1].value.as < std::string > ().c_str(), nullptr, 0))}; }
    break;

  case 97: // path_elem: id '[' "string" ']'
                                { yylhs.value.as < PathElem > () = PathElem{yystack_[3].value.as < std::string > (), yystack_[1].value.as < std::string > ().substr(1, yystack_[1].value.as < std::string > ().size() - 2)}; }
    break;

  case 98: // path_elem: id '[' "argument" ']'
                                { yylhs.value.as < PathElem > () = PathElem{yystack_[3].value.as < std::string > (), drv.get_arg_for_index(yystack_[1].value.as < std::string > ())}; }
    break;

  case 99: // id: "identifier"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 100: // id: "@links"
                                { yylhs.value.as < std::string > () = std::string("@links"); }
    break;

  case 101: // id: "beginswith"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 102: // id: "endswith"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 103: // id: "contains"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 104: // id: "like"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 105: // id: "between"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 106: // id: "key or value"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 107: // id: "sort"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 108: // id: "distinct"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 109: // id: "limit"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 110: // id: "ascending"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 111: // id: "descending"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 112: // id: "in"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 113: // id: "fulltext"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;



            default:
              break;
            }
        }
#if YY_EXCEPTIONS
      catch (const syntax_error& yyexc)
        {
          YYCDEBUG << "Caught exception: " << yyexc.what() << '\n';
          error (yyexc);
          YYERROR;
        }
#endif // YY_EXCEPTIONS
      YY_SYMBOL_PRINT ("-> $$ =", yylhs);
      yypop_ (yylen);
      yylen = 0;

      // Shift the result of the reduction.
      yypush_ (YY_NULLPTR, YY_MOVE (yylhs));
    }
    goto yynewstate;


  /*--------------------------------------.
  | yyerrlab -- here on detecting error.  |
  `--------------------------------------*/
  yyerrlab:
    // If not already recovering from an error, report this error.
    if (!yyerrstatus_)
      {
        ++yynerrs_;
        context yyctx (*this, yyla);
        std::string msg = yysyntax_error_ (yyctx);
        error (YY_MOVE (msg));
      }


    if (yyerrstatus_ == 3)
      {
        /* If just tried and failed to reuse lookahead token after an
           error, discard it.  */

        // Return failure if at end of input.
        if (yyla.kind () == symbol_kind::SYM_YYEOF)
          YYABORT;
        else if (!yyla.empty ())
          {
            yy_destroy_ ("Error: discarding", yyla);
            yyla.clear ();
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
    yypop_ (yylen);
    yylen = 0;
    YY_STACK_PRINT ();
    goto yyerrlab1;


  /*-------------------------------------------------------------.
  | yyerrlab1 -- common code for both syntax error and YYERROR.  |
  `-------------------------------------------------------------*/
  yyerrlab1:
    yyerrstatus_ = 3;   // Each real token shifted decrements this.
    // Pop stack until we find a state that shifts the error token.
    for (;;)
      {
        yyn = yypact_[+yystack_[0].state];
        if (!yy_pact_value_is_default_ (yyn))
          {
            yyn += symbol_kind::SYM_YYerror;
            if (0 <= yyn && yyn <= yylast_
                && yycheck_[yyn] == symbol_kind::SYM_YYerror)
              {
                yyn = yytable_[yyn];
                if (0 < yyn)
                  break;
              }
          }

        // Pop the current state because it cannot handle the error token.
        if (yystack_.size () == 1)
          YYABORT;

        yy_destroy_ ("Error: popping", yystack_[0]);
        yypop_ ();
        YY_STACK_PRINT ();
      }
    {
      stack_symbol_type error_token;


      // Shift the error token.
      error_token.state = state_type (yyn);
      yypush_ ("Shifting", YY_MOVE (error_token));
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
    if (!yyla.empty ())
      yy_destroy_ ("Cleanup: discarding lookahead", yyla);

    /* Do not reclaim the symbols of the rule whose action triggered
       this YYABORT or YYACCEPT.  */
    yypop_ (yylen);
    YY_STACK_PRINT ();
    while (1 < yystack_.size ())
      {
        yy_destroy_ ("Cleanup: popping", yystack_[0]);
        yypop_ ();
      }

    return yyresult;
  }
#if YY_EXCEPTIONS
    catch (...)
      {
        YYCDEBUG << "Exception caught: cleaning lookahead and stack\n";
        // Do not try to display the values of the reclaimed symbols,
        // as their printers might throw an exception.
        if (!yyla.empty ())
          yy_destroy_ (YY_NULLPTR, yyla);

        while (1 < yystack_.size ())
          {
            yy_destroy_ (YY_NULLPTR, yystack_[0]);
            yypop_ ();
          }
        throw;
      }
#endif // YY_EXCEPTIONS
  }

  void
  parser::error (const syntax_error& yyexc)
  {
    error (yyexc.what ());
  }

  /* Return YYSTR after stripping away unnecessary quotes and
     backslashes, so that it's suitable for yyerror.  The heuristic is
     that double-quoting is unnecessary unless the string contains an
     apostrophe, a comma, or backslash (other than backslash-backslash).
     YYSTR is taken from yytname.  */
  std::string
  parser::yytnamerr_ (const char *yystr)
  {
    if (*yystr == '"')
      {
        std::string yyr;
        char const *yyp = yystr;

        for (;;)
          switch (*++yyp)
            {
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
      do_not_strip_quotes: ;
      }

    return yystr;
  }

  std::string
  parser::symbol_name (symbol_kind_type yysymbol)
  {
    return yytnamerr_ (yytname_[yysymbol]);
  }



  // parser::context.
  parser::context::context (const parser& yyparser, const symbol_type& yyla)
    : yyparser_ (yyparser)
    , yyla_ (yyla)
  {}

  int
  parser::context::expected_tokens (symbol_kind_type yyarg[], int yyargn) const
  {
    // Actual number of expected tokens
    int yycount = 0;

    const int yyn = yypact_[+yyparser_.yystack_[0].state];
    if (!yy_pact_value_is_default_ (yyn))
      {
        /* Start YYX at -YYN if negative to avoid negative indexes in
           YYCHECK.  In other words, skip the first -YYN actions for
           this state because they are default actions.  */
        const int yyxbegin = yyn < 0 ? -yyn : 0;
        // Stay within bounds of both yycheck and yytname.
        const int yychecklim = yylast_ - yyn + 1;
        const int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
        for (int yyx = yyxbegin; yyx < yyxend; ++yyx)
          if (yycheck_[yyx + yyn] == yyx && yyx != symbol_kind::SYM_YYerror
              && !yy_table_value_is_error_ (yytable_[yyx + yyn]))
            {
              if (!yyarg)
                ++yycount;
              else if (yycount == yyargn)
                return 0;
              else
                yyarg[yycount++] = YY_CAST (symbol_kind_type, yyx);
            }
      }

    if (yyarg && yycount == 0 && 0 < yyargn)
      yyarg[0] = symbol_kind::SYM_YYEMPTY;
    return yycount;
  }






  int
  parser::yy_syntax_error_arguments_ (const context& yyctx,
                                                 symbol_kind_type yyarg[], int yyargn) const
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

    if (!yyctx.lookahead ().empty ())
      {
        if (yyarg)
          yyarg[0] = yyctx.token ();
        int yyn = yyctx.expected_tokens (yyarg ? yyarg + 1 : yyarg, yyargn - 1);
        return yyn + 1;
      }
    return 0;
  }

  // Generate an error message.
  std::string
  parser::yysyntax_error_ (const context& yyctx) const
  {
    // Its maximum.
    enum { YYARGS_MAX = 5 };
    // Arguments of yyformat.
    symbol_kind_type yyarg[YYARGS_MAX];
    int yycount = yy_syntax_error_arguments_ (yyctx, yyarg, YYARGS_MAX);

    char const* yyformat = YY_NULLPTR;
    switch (yycount)
      {
#define YYCASE_(N, S)                         \
        case N:                               \
          yyformat = S;                       \
        break
      default: // Avoid compiler warnings.
        YYCASE_ (0, YY_("syntax error"));
        YYCASE_ (1, YY_("syntax error, unexpected %s"));
        YYCASE_ (2, YY_("syntax error, unexpected %s, expecting %s"));
        YYCASE_ (3, YY_("syntax error, unexpected %s, expecting %s or %s"));
        YYCASE_ (4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
        YYCASE_ (5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
      }

    std::string yyres;
    // Argument number.
    std::ptrdiff_t yyi = 0;
    for (char const* yyp = yyformat; *yyp; ++yyp)
      if (yyp[0] == '%' && yyp[1] == 's' && yyi < yycount)
        {
          yyres += symbol_name (yyarg[yyi++]);
          ++yyp;
        }
      else
        yyres += *yyp;
    return yyres;
  }


  const signed char parser::yypact_ninf_ = -104;

  const signed char parser::yytable_ninf_ = -1;

  const short
  parser::yypact_[] =
  {
     137,  -104,  -104,   -41,  -104,  -104,  -104,  -104,  -104,  -104,
    -104,   137,  -104,  -104,  -104,  -104,  -104,  -104,  -104,  -104,
    -104,  -104,  -104,  -104,  -104,  -104,  -104,  -104,  -104,  -104,
    -104,  -104,  -104,   -30,  -104,  -104,  -104,  -104,  -104,  -104,
     137,   431,    23,    27,  -104,    89,    63,  -104,  -104,  -104,
    -104,  -104,  -104,  -104,   380,   -25,  -104,   -19,   495,  -104,
      32,    -8,    15,   -59,  -104,    29,  -104,   137,   137,    61,
    -104,  -104,  -104,  -104,  -104,  -104,  -104,   241,   241,   241,
     241,   189,   241,  -104,  -104,  -104,   345,  -104,    -4,   293,
    -104,   431,     8,   453,  -104,    18,     7,    -9,    22,    26,
    -104,  -104,   431,  -104,  -104,    67,    59,    60,    62,  -104,
    -104,  -104,   241,     6,  -104,     6,  -104,  -104,   241,    37,
      37,  -104,  -104,    64,   345,  -104,   -56,   474,  -104,  -104,
    -104,  -104,  -104,  -104,  -104,  -104,   495,    54,    58,    65,
     495,   495,    51,  -104,   495,   495,    93,    57,    37,  -104,
    -104,  -104,  -104,  -104,  -104,    66,    69,   -28,   -31,    16,
      22,    70,   137,  -104,  -104,   495,  -104,  -104,  -104,  -104,
     495,  -104,    -6,   -31,    22,    71,  -104,    73,  -104
  };

  const signed char
  parser::yydefact_[] =
  {
       0,    70,    71,     0,    59,    60,    61,    72,    73,    74,
     100,     0,    99,    67,    54,    52,    53,    65,    66,    55,
      56,    68,    69,    57,    58,    62,   101,   102,   103,   113,
     104,   105,   112,     0,   107,   108,   109,   110,   111,   106,
       0,    49,     0,    33,     3,     0,    16,    23,    25,    26,
      24,    22,    51,     8,     0,    75,    93,    95,     0,     6,
       0,     0,     0,     0,    48,     0,     1,     0,     0,     2,
      82,    83,    85,    87,    88,    86,    84,     0,     0,     0,
       0,     0,     0,    89,    90,    91,     0,    92,     0,     0,
      63,    49,    75,     0,    27,    30,     0,     0,    31,     0,
       7,    17,     0,    46,     5,     4,     0,     0,     0,    35,
      34,    36,     0,    20,    16,    21,    18,    19,     0,     9,
      11,    13,    15,     0,     0,    12,     0,     0,    28,    78,
      79,    80,    81,    76,    77,    94,     0,     0,     0,     0,
       0,     0,     0,    50,     0,     0,     0,     0,    10,    14,
      47,    29,    97,    96,    98,     0,     0,     0,     0,     0,
      38,     0,     0,    64,    40,     0,    44,    45,    41,    37,
       0,    43,     0,     0,    39,     0,    42,     0,    32
  };

  const signed char
  parser::yypgoto_[] =
  {
    -104,  -104,   -10,  -104,   -36,     0,  -104,  -104,  -104,  -104,
    -104,  -104,  -104,  -104,  -104,  -104,   -45,    47,    46,   -32,
      -3,  -104,   -38,    68,  -104,  -104,  -104,  -104,   -52,   -78,
    -103
  };

  const unsigned char
  parser::yydefgoto_[] =
  {
       0,    42,    43,    44,    45,   114,    47,    48,    97,    49,
      69,   109,   159,   110,   157,   111,   168,    50,    63,    51,
      52,    53,    54,    94,    95,    81,    82,    89,    55,    56,
      57
  };

  const unsigned char
  parser::yytable_[] =
  {
      46,    59,    92,    65,    62,   102,    98,   103,   102,    64,
     150,    46,     7,     8,     9,   135,    67,    68,    67,    68,
      58,   166,   167,    66,    70,    71,    72,    73,    74,    75,
      61,    60,   141,   151,   164,   137,   165,   155,    93,   138,
      46,   113,   115,   116,   117,   119,   120,   139,    96,   135,
     123,    67,    68,    65,   100,   140,   175,   104,   105,    64,
      99,    41,    76,   135,    65,    79,    80,    46,    46,    90,
     143,   127,    77,    78,    79,    80,   147,   101,   169,    13,
     170,   136,   148,    17,    18,   141,   121,    21,    22,   125,
     142,    67,   158,   160,    77,    78,    79,    80,    70,    71,
      72,    73,    74,    75,    83,    84,    85,    86,    87,    88,
     106,   107,   108,   173,    77,    78,    79,    80,   174,   101,
     144,   145,   152,   146,   149,   161,   153,   178,   176,    91,
     162,   163,   171,   154,   177,   122,    76,   126,     0,   156,
       1,     2,     3,     4,     5,     6,    77,    78,    79,    80,
       0,     0,   172,     7,     8,     9,    10,     0,     0,     0,
     128,     0,    46,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,     0,     0,    39,     3,     4,     5,     6,    40,     0,
       0,     0,    41,     0,   118,     7,     8,     9,    10,     0,
       0,     0,     0,     0,     0,     0,    12,    13,    14,    15,
      16,    17,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,     0,     0,    39,     3,     4,     5,     6,
     112,     0,     0,     0,    41,     0,     0,     7,     8,     9,
      10,     0,     0,     0,     0,     0,     0,     0,    12,    13,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,     0,     0,    39,     3,     4,
       5,     6,   112,     0,     0,     0,    41,     0,   124,     7,
       8,     9,    10,     0,     0,     0,     0,     0,     0,     0,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,     0,     0,    39,
       3,     4,     5,     6,     0,     0,     0,     0,    41,     0,
       0,     7,     8,     9,    10,     0,     0,     0,     0,     0,
       0,     0,    12,    13,    14,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    10,
       0,    39,     0,     0,     0,     0,     0,    12,     0,     0,
      41,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      90,    26,    27,    28,    29,    30,    31,    32,     0,    34,
      35,    36,    37,    38,     0,     0,    39,     4,     5,     6,
       0,     0,     0,     0,     0,    91,     0,     7,     8,     9,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    13,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    10,   129,   130,   131,   132,     0,     0,    33,
      12,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    10,    26,    27,    28,    29,    30,    31,
      32,    12,    34,    35,    36,    37,    38,   133,   134,    39,
       0,     0,     0,     0,    10,    26,    27,    28,    29,    30,
      31,    32,    12,    34,    35,    36,    37,    38,   133,   134,
      39,     0,     0,     0,     0,     0,    26,    27,    28,    29,
      30,    31,    32,     0,    34,    35,    36,    37,    38,     0,
       0,    39
  };

  const short
  parser::yycheck_[] =
  {
       0,    11,    54,    41,    40,    64,    58,    66,    64,    41,
      66,    11,    16,    17,    18,    93,    24,    25,    24,    25,
      61,    52,    53,     0,     9,    10,    11,    12,    13,    14,
      40,    61,    63,   136,    62,    28,    64,   140,    63,    32,
      40,    77,    78,    79,    80,    81,    82,    40,    67,   127,
      88,    24,    25,    91,    62,    64,    62,    67,    68,    91,
      28,    65,    47,   141,   102,    59,    60,    67,    68,    40,
     102,    63,    57,    58,    59,    60,   112,    62,    62,    28,
      64,    63,   118,    32,    33,    63,    86,    36,    37,    89,
      64,    24,   144,   145,    57,    58,    59,    60,     9,    10,
      11,    12,    13,    14,    41,    42,    43,    44,    45,    46,
      49,    50,    51,   165,    57,    58,    59,    60,   170,    62,
      61,    61,    68,    61,   124,    32,    68,    54,   173,    65,
      64,    62,    62,    68,    63,    88,    47,    91,    -1,   142,
       3,     4,     5,     6,     7,     8,    57,    58,    59,    60,
      -1,    -1,   162,    16,    17,    18,    19,    -1,    -1,    -1,
      92,    -1,   162,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,    39,    40,    41,    42,
      43,    44,    45,    46,    47,    48,    49,    50,    51,    52,
      53,    -1,    -1,    56,     5,     6,     7,     8,    61,    -1,
      -1,    -1,    65,    -1,    15,    16,    17,    18,    19,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    39,    40,
      41,    42,    43,    44,    45,    46,    47,    48,    49,    50,
      51,    52,    53,    -1,    -1,    56,     5,     6,     7,     8,
      61,    -1,    -1,    -1,    65,    -1,    -1,    16,    17,    18,
      19,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    46,    47,    48,
      49,    50,    51,    52,    53,    -1,    -1,    56,     5,     6,
       7,     8,    61,    -1,    -1,    -1,    65,    -1,    15,    16,
      17,    18,    19,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    39,    40,    41,    42,    43,    44,    45,    46,
      47,    48,    49,    50,    51,    52,    53,    -1,    -1,    56,
       5,     6,     7,     8,    -1,    -1,    -1,    -1,    65,    -1,
      -1,    16,    17,    18,    19,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    19,
      -1,    56,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,
      65,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      40,    41,    42,    43,    44,    45,    46,    47,    -1,    49,
      50,    51,    52,    53,    -1,    -1,    56,     6,     7,     8,
      -1,    -1,    -1,    -1,    -1,    65,    -1,    16,    17,    18,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      39,    40,    19,    20,    21,    22,    23,    -1,    -1,    48,
      27,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    19,    41,    42,    43,    44,    45,    46,
      47,    27,    49,    50,    51,    52,    53,    54,    55,    56,
      -1,    -1,    -1,    -1,    19,    41,    42,    43,    44,    45,
      46,    47,    27,    49,    50,    51,    52,    53,    54,    55,
      56,    -1,    -1,    -1,    -1,    -1,    41,    42,    43,    44,
      45,    46,    47,    -1,    49,    50,    51,    52,    53,    -1,
      -1,    56
  };

  const signed char
  parser::yystos_[] =
  {
       0,     3,     4,     5,     6,     7,     8,    16,    17,    18,
      19,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    56,
      61,    65,    70,    71,    72,    73,    74,    75,    76,    78,
      86,    88,    89,    90,    91,    97,    98,    99,    61,    71,
      61,    71,    73,    87,    88,    91,     0,    24,    25,    79,
       9,    10,    11,    12,    13,    14,    47,    57,    58,    59,
      60,    94,    95,    41,    42,    43,    44,    45,    46,    96,
      40,    65,    97,    63,    92,    93,    67,    77,    97,    28,
      62,    62,    64,    66,    71,    71,    49,    50,    51,    80,
      82,    84,    61,    73,    74,    73,    73,    73,    15,    73,
      73,    74,    86,    91,    15,    74,    87,    63,    92,    20,
      21,    22,    23,    54,    55,    98,    63,    28,    32,    40,
      64,    63,    64,    88,    61,    61,    61,    73,    73,    74,
      66,    99,    68,    68,    68,    99,    89,    83,    97,    81,
      97,    32,    64,    62,    62,    64,    52,    53,    85,    62,
      64,    62,    71,    97,    97,    62,    85,    63,    54
  };

  const signed char
  parser::yyr1_[] =
  {
       0,    69,    70,    71,    71,    71,    71,    71,    71,    72,
      72,    72,    72,    72,    72,    72,    73,    73,    73,    73,
      73,    73,    74,    74,    74,    74,    74,    75,    75,    76,
      76,    77,    78,    79,    79,    79,    79,    80,    81,    81,
      82,    83,    83,    84,    85,    85,    86,    86,    87,    87,
      87,    88,    88,    88,    88,    88,    88,    88,    88,    88,
      88,    88,    88,    88,    88,    89,    89,    89,    89,    89,
      90,    90,    91,    91,    91,    92,    92,    92,    93,    93,
      93,    93,    94,    94,    94,    95,    95,    95,    95,    96,
      96,    96,    96,    97,    97,    98,    98,    98,    98,    99,
      99,    99,    99,    99,    99,    99,    99,    99,    99,    99,
      99,    99,    99,    99
  };

  const signed char
  parser::yyr2_[] =
  {
       0,     2,     2,     1,     3,     3,     2,     3,     1,     3,
       4,     3,     3,     3,     4,     3,     1,     3,     3,     3,
       3,     3,     1,     1,     1,     1,     1,     2,     3,     4,
       2,     1,    10,     0,     2,     2,     2,     4,     1,     3,
       4,     2,     4,     4,     1,     1,     3,     4,     1,     0,
       3,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     6,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     0,     2,     2,     2,     2,
       2,     2,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     3,     1,     4,     4,     4,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1
  };


#if YYDEBUG || 1
  // YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
  // First, the terminals, then, starting at \a YYNTOKENS, nonterminals.
  const char*
  const parser::yytname_[] =
  {
  "\"end of file\"", "error", "\"invalid token\"", "\"truepredicate\"",
  "\"falsepredicate\"", "\"subquery\"", "\"true\"", "\"false\"",
  "\"null\"", "\"==\"", "\"!=\"", "\"<\"", "\">\"", "\">=\"", "\"<=\"",
  "\"[c]\"", "\"any\"", "\"all\"", "\"none\"", "\"@links\"", "\"@max\"",
  "\"@min\"", "\"@sun\"", "\"@average\"", "\"&&\"", "\"||\"", "\"!\"",
  "\"identifier\"", "\"string\"", "\"base64\"", "\"infinity\"", "\"NaN\"",
  "\"natural0\"", "\"number\"", "\"float\"", "\"date\"", "\"UUID\"",
  "\"ObjectId\"", "\"link\"", "\"typed link\"", "\"argument\"",
  "\"beginswith\"", "\"endswith\"", "\"contains\"", "\"fulltext\"",
  "\"like\"", "\"between\"", "\"in\"", "\"obj\"", "\"sort\"",
  "\"distinct\"", "\"limit\"", "\"ascending\"", "\"descending\"",
  "\"@size\"", "\"@type\"", "\"key or value\"", "'+'", "'-'", "'*'", "'/'",
  "'('", "')'", "'.'", "','", "'{'", "'}'", "'['", "']'", "$accept",
  "final", "query", "compare", "expr", "value", "prop", "aggregate",
  "simple_prop", "subquery", "post_query", "distinct", "distinct_param",
  "sort", "sort_param", "limit", "direction", "list", "list_content",
  "constant", "primary_key", "boolexpr", "comp_type", "post_op", "aggr_op",
  "equality", "relational", "stringop", "path", "path_elem", "id", YY_NULLPTR
  };
#endif


#if YYDEBUG
  const short
  parser::yyrline_[] =
  {
       0,   163,   163,   166,   167,   168,   169,   170,   171,   174,
     175,   180,   181,   182,   183,   188,   191,   192,   193,   194,
     195,   196,   199,   200,   201,   202,   203,   206,   207,   210,
     214,   220,   223,   226,   227,   228,   229,   231,   234,   235,
     237,   240,   241,   243,   246,   247,   249,   250,   253,   254,
     255,   258,   259,   260,   261,   262,   263,   264,   265,   266,
     267,   268,   269,   270,   271,   279,   280,   281,   282,   283,
     286,   287,   290,   291,   292,   295,   296,   297,   300,   301,
     302,   303,   306,   307,   308,   311,   312,   313,   314,   317,
     318,   319,   320,   323,   324,   327,   328,   329,   330,   333,
     334,   335,   336,   337,   338,   339,   340,   341,   342,   343,
     344,   345,   346,   347
  };

  void
  parser::yy_stack_print_ () const
  {
    *yycdebug_ << "Stack now";
    for (stack_type::const_iterator
           i = yystack_.begin (),
           i_end = yystack_.end ();
         i != i_end; ++i)
      *yycdebug_ << ' ' << int (i->state);
    *yycdebug_ << '\n';
  }

  void
  parser::yy_reduce_print_ (int yyrule) const
  {
    int yylno = yyrline_[yyrule];
    int yynrhs = yyr2_[yyrule];
    // Print the symbols being reduced, and their result.
    *yycdebug_ << "Reducing stack by rule " << yyrule - 1
               << " (line " << yylno << "):\n";
    // The symbols being reduced.
    for (int yyi = 0; yyi < yynrhs; yyi++)
      YY_SYMBOL_PRINT ("   $" << yyi + 1 << " =",
                       yystack_[(yynrhs) - (yyi + 1)]);
  }
#endif // YYDEBUG


} // yy



void
yy::parser::error (const std::string& m)
{
    drv.error(m);
}

#ifdef _MSC_VER
#pragma warning( pop ) // restore normal warning levels
#endif
