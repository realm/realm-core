// A Bison parser, made by GNU Bison 3.7.6.

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

  /*---------------.
  | symbol kinds.  |
  `---------------*/



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
      case symbol_kind::SYM_direction: // direction
        value.YY_MOVE_OR_COPY< bool > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
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
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_path_elem: // path_elem
      case symbol_kind::SYM_id: // id
        value.YY_MOVE_OR_COPY< std::string > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_aggr_op: // aggr_op
        value.YY_MOVE_OR_COPY< std::unique_ptr<AggrNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.YY_MOVE_OR_COPY< std::unique_ptr<AndNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.YY_MOVE_OR_COPY< std::unique_ptr<AtomPredNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
        value.YY_MOVE_OR_COPY< std::unique_ptr<ConstantNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.YY_MOVE_OR_COPY< std::unique_ptr<DescriptorNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.YY_MOVE_OR_COPY< std::unique_ptr<DescriptorOrderingNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred: // pred
        value.YY_MOVE_OR_COPY< std::unique_ptr<OrNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.YY_MOVE_OR_COPY< std::unique_ptr<PathNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.YY_MOVE_OR_COPY< std::unique_ptr<PostOpNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_simple_prop: // simple_prop
        value.YY_MOVE_OR_COPY< std::unique_ptr<PropNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
        value.YY_MOVE_OR_COPY< std::unique_ptr<PropertyNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.YY_MOVE_OR_COPY< std::unique_ptr<SubqueryNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.YY_MOVE_OR_COPY< std::unique_ptr<TrueOrFalseNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_value: // value
        value.YY_MOVE_OR_COPY< std::unique_ptr<ValueNode> > (YY_MOVE (that.value));
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
      case symbol_kind::SYM_direction: // direction
        value.move< bool > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
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
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_path_elem: // path_elem
      case symbol_kind::SYM_id: // id
        value.move< std::string > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< std::unique_ptr<AggrNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.move< std::unique_ptr<AndNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.move< std::unique_ptr<AtomPredNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
        value.move< std::unique_ptr<ConstantNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< std::unique_ptr<DescriptorNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.move< std::unique_ptr<DescriptorOrderingNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred: // pred
        value.move< std::unique_ptr<OrNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.move< std::unique_ptr<PathNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< std::unique_ptr<PostOpNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_simple_prop: // simple_prop
        value.move< std::unique_ptr<PropNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
        value.move< std::unique_ptr<PropertyNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.move< std::unique_ptr<SubqueryNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.move< std::unique_ptr<TrueOrFalseNode> > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_value: // value
        value.move< std::unique_ptr<ValueNode> > (YY_MOVE (that.value));
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
      case symbol_kind::SYM_direction: // direction
        value.copy< bool > (that.value);
        break;

      case symbol_kind::SYM_comp_type: // comp_type
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
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_path_elem: // path_elem
      case symbol_kind::SYM_id: // id
        value.copy< std::string > (that.value);
        break;

      case symbol_kind::SYM_aggr_op: // aggr_op
        value.copy< std::unique_ptr<AggrNode> > (that.value);
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.copy< std::unique_ptr<AndNode> > (that.value);
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.copy< std::unique_ptr<AtomPredNode> > (that.value);
        break;

      case symbol_kind::SYM_constant: // constant
        value.copy< std::unique_ptr<ConstantNode> > (that.value);
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.copy< std::unique_ptr<DescriptorNode> > (that.value);
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.copy< std::unique_ptr<DescriptorOrderingNode> > (that.value);
        break;

      case symbol_kind::SYM_pred: // pred
        value.copy< std::unique_ptr<OrNode> > (that.value);
        break;

      case symbol_kind::SYM_path: // path
        value.copy< std::unique_ptr<PathNode> > (that.value);
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.copy< std::unique_ptr<PostOpNode> > (that.value);
        break;

      case symbol_kind::SYM_simple_prop: // simple_prop
        value.copy< std::unique_ptr<PropNode> > (that.value);
        break;

      case symbol_kind::SYM_prop: // prop
        value.copy< std::unique_ptr<PropertyNode> > (that.value);
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.copy< std::unique_ptr<SubqueryNode> > (that.value);
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.copy< std::unique_ptr<TrueOrFalseNode> > (that.value);
        break;

      case symbol_kind::SYM_value: // value
        value.copy< std::unique_ptr<ValueNode> > (that.value);
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
      case symbol_kind::SYM_direction: // direction
        value.move< bool > (that.value);
        break;

      case symbol_kind::SYM_comp_type: // comp_type
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
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_path_elem: // path_elem
      case symbol_kind::SYM_id: // id
        value.move< std::string > (that.value);
        break;

      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< std::unique_ptr<AggrNode> > (that.value);
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.move< std::unique_ptr<AndNode> > (that.value);
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.move< std::unique_ptr<AtomPredNode> > (that.value);
        break;

      case symbol_kind::SYM_constant: // constant
        value.move< std::unique_ptr<ConstantNode> > (that.value);
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< std::unique_ptr<DescriptorNode> > (that.value);
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.move< std::unique_ptr<DescriptorOrderingNode> > (that.value);
        break;

      case symbol_kind::SYM_pred: // pred
        value.move< std::unique_ptr<OrNode> > (that.value);
        break;

      case symbol_kind::SYM_path: // path
        value.move< std::unique_ptr<PathNode> > (that.value);
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< std::unique_ptr<PostOpNode> > (that.value);
        break;

      case symbol_kind::SYM_simple_prop: // simple_prop
        value.move< std::unique_ptr<PropNode> > (that.value);
        break;

      case symbol_kind::SYM_prop: // prop
        value.move< std::unique_ptr<PropertyNode> > (that.value);
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.move< std::unique_ptr<SubqueryNode> > (that.value);
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.move< std::unique_ptr<TrueOrFalseNode> > (that.value);
        break;

      case symbol_kind::SYM_value: // value
        value.move< std::unique_ptr<ValueNode> > (that.value);
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

      case symbol_kind::SYM_SORT: // "sort"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_DISTINCT: // "distinct"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_LIMIT: // "limit"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_ASCENDING: // "ascending"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_DESCENDING: // "descending"
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

      case symbol_kind::SYM_IN: // "IN"
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

      case symbol_kind::SYM_LIKE: // "like"
                 { yyo << yysym.value.template as < std::string > (); }
        break;

      case symbol_kind::SYM_BETWEEN: // "between"
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

      case symbol_kind::SYM_55_: // '('
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_56_: // ')'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_57_: // '['
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_58_: // ']'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_59_: // '.'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_60_: // ','
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_61_: // '{'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_62_: // '}'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_query: // query
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_pred: // pred
                 { yyo << yysym.value.template as < std::unique_ptr<OrNode> > (); }
        break;

      case symbol_kind::SYM_and_pred: // and_pred
                 { yyo << yysym.value.template as < std::unique_ptr<AndNode> > (); }
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
                 { yyo << yysym.value.template as < std::unique_ptr<AtomPredNode> > (); }
        break;

      case symbol_kind::SYM_value: // value
                 { yyo << yysym.value.template as < std::unique_ptr<ValueNode> > (); }
        break;

      case symbol_kind::SYM_prop: // prop
                 { yyo << yysym.value.template as < std::unique_ptr<PropertyNode> > (); }
        break;

      case symbol_kind::SYM_simple_prop: // simple_prop
                 { yyo << yysym.value.template as < std::unique_ptr<PropNode> > (); }
        break;

      case symbol_kind::SYM_subquery: // subquery
                 { yyo << yysym.value.template as < std::unique_ptr<SubqueryNode> > (); }
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
                 { yyo << yysym.value.template as < std::unique_ptr<DescriptorOrderingNode> > (); }
        break;

      case symbol_kind::SYM_distinct: // distinct
                 { yyo << yysym.value.template as < std::unique_ptr<DescriptorNode> > (); }
        break;

      case symbol_kind::SYM_distinct_param: // distinct_param
                 { yyo << yysym.value.template as < std::unique_ptr<DescriptorNode> > (); }
        break;

      case symbol_kind::SYM_sort: // sort
                 { yyo << yysym.value.template as < std::unique_ptr<DescriptorNode> > (); }
        break;

      case symbol_kind::SYM_sort_param: // sort_param
                 { yyo << yysym.value.template as < std::unique_ptr<DescriptorNode> > (); }
        break;

      case symbol_kind::SYM_limit: // limit
                 { yyo << yysym.value.template as < std::unique_ptr<DescriptorNode> > (); }
        break;

      case symbol_kind::SYM_direction: // direction
                 { yyo << yysym.value.template as < bool > (); }
        break;

      case symbol_kind::SYM_list: // list
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_list_content: // list_content
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_constant: // constant
                 { yyo << yysym.value.template as < std::unique_ptr<ConstantNode> > (); }
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
                 { yyo << yysym.value.template as < std::unique_ptr<TrueOrFalseNode> > (); }
        break;

      case symbol_kind::SYM_comp_type: // comp_type
                 { yyo << yysym.value.template as < int > (); }
        break;

      case symbol_kind::SYM_post_op: // post_op
                 { yyo << yysym.value.template as < std::unique_ptr<PostOpNode> > (); }
        break;

      case symbol_kind::SYM_aggr_op: // aggr_op
                 { yyo << yysym.value.template as < std::unique_ptr<AggrNode> > (); }
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
                 { yyo << yysym.value.template as < std::unique_ptr<PathNode> > (); }
        break;

      case symbol_kind::SYM_path_elem: // path_elem
                 { yyo << yysym.value.template as < std::string > (); }
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
  parser::yypop_ (int n)
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
  parser::yy_pact_value_is_default_ (int yyvalue)
  {
    return yyvalue == yypact_ninf_;
  }

  bool
  parser::yy_table_value_is_error_ (int yyvalue)
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
      case symbol_kind::SYM_direction: // direction
        yylhs.value.emplace< bool > ();
        break;

      case symbol_kind::SYM_comp_type: // comp_type
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
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_path_elem: // path_elem
      case symbol_kind::SYM_id: // id
        yylhs.value.emplace< std::string > ();
        break;

      case symbol_kind::SYM_aggr_op: // aggr_op
        yylhs.value.emplace< std::unique_ptr<AggrNode> > ();
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        yylhs.value.emplace< std::unique_ptr<AndNode> > ();
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        yylhs.value.emplace< std::unique_ptr<AtomPredNode> > ();
        break;

      case symbol_kind::SYM_constant: // constant
        yylhs.value.emplace< std::unique_ptr<ConstantNode> > ();
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        yylhs.value.emplace< std::unique_ptr<DescriptorNode> > ();
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        yylhs.value.emplace< std::unique_ptr<DescriptorOrderingNode> > ();
        break;

      case symbol_kind::SYM_pred: // pred
        yylhs.value.emplace< std::unique_ptr<OrNode> > ();
        break;

      case symbol_kind::SYM_path: // path
        yylhs.value.emplace< std::unique_ptr<PathNode> > ();
        break;

      case symbol_kind::SYM_post_op: // post_op
        yylhs.value.emplace< std::unique_ptr<PostOpNode> > ();
        break;

      case symbol_kind::SYM_simple_prop: // simple_prop
        yylhs.value.emplace< std::unique_ptr<PropNode> > ();
        break;

      case symbol_kind::SYM_prop: // prop
        yylhs.value.emplace< std::unique_ptr<PropertyNode> > ();
        break;

      case symbol_kind::SYM_subquery: // subquery
        yylhs.value.emplace< std::unique_ptr<SubqueryNode> > ();
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        yylhs.value.emplace< std::unique_ptr<TrueOrFalseNode> > ();
        break;

      case symbol_kind::SYM_value: // value
        yylhs.value.emplace< std::unique_ptr<ValueNode> > ();
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
  case 2: // query: pred pred_suffix
                       { drv.result = std::move(yystack_[1].value.as < std::unique_ptr<OrNode> > ()); drv.ordering = std::move(yystack_[0].value.as < std::unique_ptr<DescriptorOrderingNode> > ()); }
    break;

  case 3: // pred: and_pred
                                { yylhs.value.as < std::unique_ptr<OrNode> > () = std::make_unique<OrNode>(std::move(yystack_[0].value.as < std::unique_ptr<AndNode> > ())); }
    break;

  case 4: // pred: pred "||" and_pred
                                { yystack_[2].value.as < std::unique_ptr<OrNode> > ()->atom_preds.emplace_back(std::move(yystack_[0].value.as < std::unique_ptr<AndNode> > ())); yylhs.value.as < std::unique_ptr<OrNode> > () = std::move(yystack_[2].value.as < std::unique_ptr<OrNode> > ()); }
    break;

  case 5: // and_pred: atom_pred
                                { yylhs.value.as < std::unique_ptr<AndNode> > () = std::make_unique<AndNode>(std::move(yystack_[0].value.as < std::unique_ptr<AtomPredNode> > ())); }
    break;

  case 6: // and_pred: and_pred "&&" atom_pred
                                { yystack_[2].value.as < std::unique_ptr<AndNode> > ()->atom_preds.emplace_back(std::move(yystack_[0].value.as < std::unique_ptr<AtomPredNode> > ())); yylhs.value.as < std::unique_ptr<AndNode> > () = std::move(yystack_[2].value.as < std::unique_ptr<AndNode> > ()); }
    break;

  case 7: // atom_pred: value equality value
                                { yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::make_unique<EqualityNode>(std::move(yystack_[2].value.as < std::unique_ptr<ValueNode> > ()),yystack_[1].value.as < int > (),std::move(yystack_[0].value.as < std::unique_ptr<ValueNode> > ())); }
    break;

  case 8: // atom_pred: value equality "[c]" value
                                {
                                    auto tmp = std::make_unique<EqualityNode>(std::move(yystack_[3].value.as < std::unique_ptr<ValueNode> > ()),yystack_[2].value.as < int > (),std::move(yystack_[0].value.as < std::unique_ptr<ValueNode> > ()));
                                    tmp->case_sensitive = false;
                                    yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::move(tmp);
                                }
    break;

  case 9: // atom_pred: value relational value
                                { yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::make_unique<RelationalNode>(std::move(yystack_[2].value.as < std::unique_ptr<ValueNode> > ()),yystack_[1].value.as < int > (),std::move(yystack_[0].value.as < std::unique_ptr<ValueNode> > ())); }
    break;

  case 10: // atom_pred: value stringop value
                                { yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::make_unique<StringOpsNode>(std::move(yystack_[2].value.as < std::unique_ptr<ValueNode> > ()),yystack_[1].value.as < int > (),std::move(yystack_[0].value.as < std::unique_ptr<ValueNode> > ())); }
    break;

  case 11: // atom_pred: value stringop "[c]" value
                                {
                                    auto tmp = std::make_unique<StringOpsNode>(std::move(yystack_[3].value.as < std::unique_ptr<ValueNode> > ()),yystack_[2].value.as < int > (),std::move(yystack_[0].value.as < std::unique_ptr<ValueNode> > ()));
                                    tmp->case_sensitive = false;
                                    yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::move(tmp);
                                }
    break;

  case 12: // atom_pred: value "between" list
                                {
                                    error("The 'between' operator is not supported yet, please rewrite the expression using '>' and '<'.");
                                    YYERROR;
                                }
    break;

  case 13: // atom_pred: "!" atom_pred
                                { yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::make_unique<NotNode>(std::move(yystack_[0].value.as < std::unique_ptr<AtomPredNode> > ())); }
    break;

  case 14: // atom_pred: '(' pred ')'
                                { yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::make_unique<ParensNode>(std::move(yystack_[1].value.as < std::unique_ptr<OrNode> > ())); }
    break;

  case 15: // atom_pred: boolexpr
                                { yylhs.value.as < std::unique_ptr<AtomPredNode> > () = std::move(yystack_[0].value.as < std::unique_ptr<TrueOrFalseNode> > ()); }
    break;

  case 16: // value: constant
                                { yylhs.value.as < std::unique_ptr<ValueNode> > () = std::make_unique<ValueNode>(std::move(yystack_[0].value.as < std::unique_ptr<ConstantNode> > ()));}
    break;

  case 17: // value: prop
                                { yylhs.value.as < std::unique_ptr<ValueNode> > () = std::make_unique<ValueNode>(std::move(yystack_[0].value.as < std::unique_ptr<PropertyNode> > ()));}
    break;

  case 18: // prop: path id post_op
                                { yylhs.value.as < std::unique_ptr<PropertyNode> > () = std::make_unique<PropNode>(std::move(yystack_[2].value.as < std::unique_ptr<PathNode> > ()),yystack_[1].value.as < std::string > (),std::move(yystack_[0].value.as < std::unique_ptr<PostOpNode> > ())); }
    break;

  case 19: // prop: path id '[' constant ']' post_op
                                       { yylhs.value.as < std::unique_ptr<PropertyNode> > () = std::make_unique<PropNode>(std::move(yystack_[5].value.as < std::unique_ptr<PathNode> > ()),yystack_[4].value.as < std::string > (),std::move(yystack_[2].value.as < std::unique_ptr<ConstantNode> > ()), std::move(yystack_[0].value.as < std::unique_ptr<PostOpNode> > ())); }
    break;

  case 20: // prop: comp_type path id post_op
                                { yylhs.value.as < std::unique_ptr<PropertyNode> > () = std::make_unique<PropNode>(std::move(yystack_[2].value.as < std::unique_ptr<PathNode> > ()), yystack_[1].value.as < std::string > (), std::move(yystack_[0].value.as < std::unique_ptr<PostOpNode> > ()),ExpressionComparisonType(yystack_[3].value.as < int > ()));}
    break;

  case 21: // prop: path "@links" post_op
                                { yylhs.value.as < std::unique_ptr<PropertyNode> > () = std::make_unique<PropNode>(std::move(yystack_[2].value.as < std::unique_ptr<PathNode> > ()), "@links" , std::move(yystack_[0].value.as < std::unique_ptr<PostOpNode> > ())); }
    break;

  case 22: // prop: path id '.' aggr_op '.' id
                                    { yylhs.value.as < std::unique_ptr<PropertyNode> > () = std::make_unique<LinkAggrNode>(std::move(yystack_[5].value.as < std::unique_ptr<PathNode> > ()), yystack_[4].value.as < std::string > (), std::move(yystack_[2].value.as < std::unique_ptr<AggrNode> > ()), yystack_[0].value.as < std::string > ()); }
    break;

  case 23: // prop: path id '.' aggr_op
                                { yylhs.value.as < std::unique_ptr<PropertyNode> > () = std::make_unique<ListAggrNode>(std::move(yystack_[3].value.as < std::unique_ptr<PathNode> > ()), yystack_[2].value.as < std::string > (), std::move(yystack_[0].value.as < std::unique_ptr<AggrNode> > ())); }
    break;

  case 24: // prop: subquery
                                { yylhs.value.as < std::unique_ptr<PropertyNode> > () = std::move(yystack_[0].value.as < std::unique_ptr<SubqueryNode> > ()); }
    break;

  case 25: // simple_prop: path id
                                { yylhs.value.as < std::unique_ptr<PropNode> > () = std::make_unique<PropNode>(std::move(yystack_[1].value.as < std::unique_ptr<PathNode> > ()),yystack_[0].value.as < std::string > ()); }
    break;

  case 26: // subquery: "subquery" '(' simple_prop ',' id ',' pred ')' '.' "@size"
                                                              { yylhs.value.as < std::unique_ptr<SubqueryNode> > () = std::make_unique<SubqueryNode>(std::move(yystack_[7].value.as < std::unique_ptr<PropNode> > ()), yystack_[5].value.as < std::string > (), std::move(yystack_[3].value.as < std::unique_ptr<OrNode> > ())); }
    break;

  case 27: // pred_suffix: %empty
                                { yylhs.value.as < std::unique_ptr<DescriptorOrderingNode> > () = std::make_unique<DescriptorOrderingNode>();}
    break;

  case 28: // pred_suffix: pred_suffix sort
                                { yystack_[1].value.as < std::unique_ptr<DescriptorOrderingNode> > ()->add_descriptor(std::move(yystack_[0].value.as < std::unique_ptr<DescriptorNode> > ())); yylhs.value.as < std::unique_ptr<DescriptorOrderingNode> > () = std::move(yystack_[1].value.as < std::unique_ptr<DescriptorOrderingNode> > ()); }
    break;

  case 29: // pred_suffix: pred_suffix distinct
                                { yystack_[1].value.as < std::unique_ptr<DescriptorOrderingNode> > ()->add_descriptor(std::move(yystack_[0].value.as < std::unique_ptr<DescriptorNode> > ())); yylhs.value.as < std::unique_ptr<DescriptorOrderingNode> > () = std::move(yystack_[1].value.as < std::unique_ptr<DescriptorOrderingNode> > ()); }
    break;

  case 30: // pred_suffix: pred_suffix limit
                                { yystack_[1].value.as < std::unique_ptr<DescriptorOrderingNode> > ()->add_descriptor(std::move(yystack_[0].value.as < std::unique_ptr<DescriptorNode> > ())); yylhs.value.as < std::unique_ptr<DescriptorOrderingNode> > () = std::move(yystack_[1].value.as < std::unique_ptr<DescriptorOrderingNode> > ()); }
    break;

  case 31: // distinct: "distinct" '(' distinct_param ')'
                                          { yylhs.value.as < std::unique_ptr<DescriptorNode> > () = std::move(yystack_[1].value.as < std::unique_ptr<DescriptorNode> > ()); }
    break;

  case 32: // distinct_param: path id
                                { yylhs.value.as < std::unique_ptr<DescriptorNode> > () = std::make_unique<DescriptorNode>(DescriptorNode::DISTINCT); yylhs.value.as < std::unique_ptr<DescriptorNode> > ()->add(yystack_[1].value.as < std::unique_ptr<PathNode> > ()->path_elems, yystack_[0].value.as < std::string > ());}
    break;

  case 33: // distinct_param: distinct_param ',' path id
                                 { yystack_[3].value.as < std::unique_ptr<DescriptorNode> > ()->add(yystack_[1].value.as < std::unique_ptr<PathNode> > ()->path_elems, yystack_[0].value.as < std::string > ()); yylhs.value.as < std::unique_ptr<DescriptorNode> > () = std::move(yystack_[3].value.as < std::unique_ptr<DescriptorNode> > ()); }
    break;

  case 34: // sort: "sort" '(' sort_param ')'
                                 { yylhs.value.as < std::unique_ptr<DescriptorNode> > () = std::move(yystack_[1].value.as < std::unique_ptr<DescriptorNode> > ()); }
    break;

  case 35: // sort_param: path id direction
                                { yylhs.value.as < std::unique_ptr<DescriptorNode> > () = std::make_unique<DescriptorNode>(DescriptorNode::SORT); yylhs.value.as < std::unique_ptr<DescriptorNode> > ()->add(yystack_[2].value.as < std::unique_ptr<PathNode> > ()->path_elems, yystack_[1].value.as < std::string > (), yystack_[0].value.as < bool > ());}
    break;

  case 36: // sort_param: sort_param ',' path id direction
                                        { yystack_[4].value.as < std::unique_ptr<DescriptorNode> > ()->add(yystack_[2].value.as < std::unique_ptr<PathNode> > ()->path_elems, yystack_[1].value.as < std::string > (), yystack_[0].value.as < bool > ()); yylhs.value.as < std::unique_ptr<DescriptorNode> > () = std::move(yystack_[4].value.as < std::unique_ptr<DescriptorNode> > ()); }
    break;

  case 37: // limit: "limit" '(' "natural0" ')'
                                { yylhs.value.as < std::unique_ptr<DescriptorNode> > () = std::make_unique<DescriptorNode>(DescriptorNode::LIMIT, yystack_[1].value.as < std::string > ()); }
    break;

  case 38: // direction: "ascending"
                                { yylhs.value.as < bool > () = true; }
    break;

  case 39: // direction: "descending"
                                { yylhs.value.as < bool > () = false; }
    break;

  case 43: // constant: "natural0"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::NUMBER, yystack_[0].value.as < std::string > ()); }
    break;

  case 44: // constant: "number"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::NUMBER, yystack_[0].value.as < std::string > ()); }
    break;

  case 45: // constant: "infinity"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::INFINITY_VAL, yystack_[0].value.as < std::string > ()); }
    break;

  case 46: // constant: "NaN"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::NAN_VAL, yystack_[0].value.as < std::string > ()); }
    break;

  case 47: // constant: "string"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::STRING, yystack_[0].value.as < std::string > ()); }
    break;

  case 48: // constant: "base64"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::BASE64, yystack_[0].value.as < std::string > ()); }
    break;

  case 49: // constant: "float"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::FLOAT, yystack_[0].value.as < std::string > ()); }
    break;

  case 50: // constant: "date"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::TIMESTAMP, yystack_[0].value.as < std::string > ()); }
    break;

  case 51: // constant: "UUID"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::UUID_T, yystack_[0].value.as < std::string > ()); }
    break;

  case 52: // constant: "ObjectId"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::OID, yystack_[0].value.as < std::string > ()); }
    break;

  case 53: // constant: "link"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::LINK, yystack_[0].value.as < std::string > ()); }
    break;

  case 54: // constant: "typed link"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::TYPED_LINK, yystack_[0].value.as < std::string > ()); }
    break;

  case 55: // constant: "true"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::TRUE, ""); }
    break;

  case 56: // constant: "false"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::FALSE, ""); }
    break;

  case 57: // constant: "null"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::NULL_VAL, ""); }
    break;

  case 58: // constant: "argument"
                                { yylhs.value.as < std::unique_ptr<ConstantNode> > () = std::make_unique<ConstantNode>(ConstantNode::ARG, yystack_[0].value.as < std::string > ()); }
    break;

  case 59: // boolexpr: "truepredicate"
                                { yylhs.value.as < std::unique_ptr<TrueOrFalseNode> > () = std::make_unique<TrueOrFalseNode>(true); }
    break;

  case 60: // boolexpr: "falsepredicate"
                                { yylhs.value.as < std::unique_ptr<TrueOrFalseNode> > () = std::make_unique<TrueOrFalseNode>(false); }
    break;

  case 61: // comp_type: "any"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::Any); }
    break;

  case 62: // comp_type: "all"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::All); }
    break;

  case 63: // comp_type: "none"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::None); }
    break;

  case 64: // post_op: %empty
                                { yylhs.value.as < std::unique_ptr<PostOpNode> > () = nullptr; }
    break;

  case 65: // post_op: '.' "@size"
                                { yylhs.value.as < std::unique_ptr<PostOpNode> > () = std::make_unique<PostOpNode>(yystack_[0].value.as < std::string > (), PostOpNode::SIZE);}
    break;

  case 66: // post_op: '.' "@type"
                                { yylhs.value.as < std::unique_ptr<PostOpNode> > () = std::make_unique<PostOpNode>(yystack_[0].value.as < std::string > (), PostOpNode::TYPE);}
    break;

  case 67: // aggr_op: "@max"
                                { yylhs.value.as < std::unique_ptr<AggrNode> > () = std::make_unique<AggrNode>(AggrNode::MAX);}
    break;

  case 68: // aggr_op: "@min"
                                { yylhs.value.as < std::unique_ptr<AggrNode> > () = std::make_unique<AggrNode>(AggrNode::MIN);}
    break;

  case 69: // aggr_op: "@sun"
                                { yylhs.value.as < std::unique_ptr<AggrNode> > () = std::make_unique<AggrNode>(AggrNode::SUM);}
    break;

  case 70: // aggr_op: "@average"
                                { yylhs.value.as < std::unique_ptr<AggrNode> > () = std::make_unique<AggrNode>(AggrNode::AVG);}
    break;

  case 71: // equality: "=="
                                { yylhs.value.as < int > () = CompareNode::EQUAL; }
    break;

  case 72: // equality: "!="
                                { yylhs.value.as < int > () = CompareNode::NOT_EQUAL; }
    break;

  case 73: // equality: "IN"
                                { yylhs.value.as < int > () = CompareNode::IN; }
    break;

  case 74: // relational: "<"
                                { yylhs.value.as < int > () = CompareNode::LESS; }
    break;

  case 75: // relational: "<="
                                { yylhs.value.as < int > () = CompareNode::LESS_EQUAL; }
    break;

  case 76: // relational: ">"
                                { yylhs.value.as < int > () = CompareNode::GREATER; }
    break;

  case 77: // relational: ">="
                                { yylhs.value.as < int > () = CompareNode::GREATER_EQUAL; }
    break;

  case 78: // stringop: "beginswith"
                                { yylhs.value.as < int > () = CompareNode::BEGINSWITH; }
    break;

  case 79: // stringop: "endswith"
                                { yylhs.value.as < int > () = CompareNode::ENDSWITH; }
    break;

  case 80: // stringop: "contains"
                                { yylhs.value.as < int > () = CompareNode::CONTAINS; }
    break;

  case 81: // stringop: "like"
                                { yylhs.value.as < int > () = CompareNode::LIKE; }
    break;

  case 82: // path: %empty
                                { yylhs.value.as < std::unique_ptr<PathNode> > () = std::make_unique<PathNode>(); }
    break;

  case 83: // path: path path_elem
                                { yystack_[1].value.as < std::unique_ptr<PathNode> > ()->add_element(std::move(yystack_[0].value.as < std::string > ())); yylhs.value.as < std::unique_ptr<PathNode> > () = std::move(yystack_[1].value.as < std::unique_ptr<PathNode> > ()); }
    break;

  case 84: // path_elem: id '.'
                                { yylhs.value.as < std::string > () = yystack_[1].value.as < std::string > (); }
    break;

  case 85: // id: "identifier"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 86: // id: "@links" '.' "identifier" '.' "identifier"
                                { yylhs.value.as < std::string > () = std::string("@links.") + yystack_[2].value.as < std::string > () + "." + yystack_[0].value.as < std::string > (); }
    break;

  case 87: // id: "beginswith"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 88: // id: "endswith"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 89: // id: "contains"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 90: // id: "like"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 91: // id: "between"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 92: // id: "key or value"
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

    int yyn = yypact_[+yyparser_.yystack_[0].state];
    if (!yy_pact_value_is_default_ (yyn))
      {
        /* Start YYX at -YYN if negative to avoid negative indexes in
           YYCHECK.  In other words, skip the first -YYN actions for
           this state because they are default actions.  */
        int yyxbegin = yyn < 0 ? -yyn : 0;
        // Stay within bounds of both yycheck and yytname.
        int yychecklim = yylast_ - yyn + 1;
        int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
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


  const signed char parser::yypact_ninf_ = -77;

  const signed char parser::yytable_ninf_ = -1;

  const short
  parser::yypact_[] =
  {
      94,   -77,   -77,   -37,   -77,   -77,   -77,   -77,   -77,   -77,
      94,   -77,   -77,   -77,   -77,   -77,   -77,   -77,   -77,   -77,
     -77,   -77,   -77,   -77,    94,    23,    -4,    30,   -77,    33,
     -77,   -77,   -77,   -77,   -77,   -14,   -77,   -77,   -18,   -77,
      94,    38,    94,   -77,   -77,   -77,   -77,   -77,   -77,   -77,
     -77,   -77,   -77,   -77,     1,   131,   205,   168,    61,     2,
     -77,   -77,   -77,   -77,   -77,   -77,   -77,   -77,    -3,     6,
      61,   -77,    30,    13,    14,    15,   -77,   -77,   -77,   -77,
     219,   -77,   205,   -77,   -77,   205,   -77,    12,    19,   -21,
     -77,   219,    48,   -77,    61,    20,   -77,   -77,    34,   -36,
     -77,   -77,   -77,    52,   -44,   -77,    28,   -77,   -77,    31,
     -77,   -77,   -77,   -77,    32,    35,   -77,   -40,    61,   -35,
      61,    37,   219,   -77,    57,    43,    61,    94,   -77,   -77,
      -2,   -77,   -77,    20,   -77,   -77,   -77,   -44,   -77,   -77,
     -17,    61,   -77,   -77,   -77,    61,    54,    -2,    20,    62,
     -77,   -77
  };

  const signed char
  parser::yydefact_[] =
  {
      82,    59,    60,     0,    55,    56,    57,    61,    62,    63,
      82,    47,    48,    45,    46,    43,    44,    49,    50,    51,
      52,    53,    54,    58,    82,     0,    27,     3,     5,     0,
      17,    24,    16,    15,    82,     0,    82,    13,     0,     1,
      82,     2,    82,    71,    72,    73,    74,    76,    77,    75,
      78,    79,    80,    81,     0,    82,    82,    82,     0,    64,
      85,    87,    88,    89,    90,    91,    92,    83,    64,     0,
       0,    14,     4,     0,     0,     0,    29,    28,    30,     6,
       0,    12,    82,     7,     9,    82,    10,     0,    64,     0,
      21,     0,    84,    18,     0,    25,    82,    82,     0,     0,
      41,     8,    11,     0,    84,    20,     0,    65,    66,     0,
      67,    68,    69,    70,    23,     0,    84,     0,     0,     0,
       0,     0,     0,    40,     0,    64,     0,    82,    34,    82,
       0,    31,    82,    32,    37,    42,    86,     0,    19,    22,
       0,     0,    38,    39,    35,     0,     0,     0,    33,     0,
      36,    26
  };

  const signed char
  parser::yypgoto_[] =
  {
     -77,   -77,   -24,    79,     0,   -27,   -77,   -77,   -77,   -77,
     -77,   -77,   -77,   -77,   -77,   -26,   -77,   -77,   -76,   -77,
     -77,   -66,   -77,   -77,   -77,   -77,   -33,   -77,   -53
  };

  const unsigned char
  parser::yydefgoto_[] =
  {
       0,    25,    26,    27,    28,    29,    30,    69,    31,    41,
      76,   119,    77,   117,    78,   144,    81,    99,    32,    33,
      34,    90,   114,    55,    56,    57,    35,    67,    68
  };

  const unsigned char
  parser::yytable_[] =
  {
      38,    58,    93,    70,   100,    88,   142,   143,   107,   108,
      37,    59,   106,    40,    40,   109,   128,    95,    36,    60,
     129,   131,   105,    39,   122,   132,   123,    40,    83,    84,
      86,   107,   108,    61,    62,    63,    64,    65,    71,   146,
      66,   115,    79,    73,    74,    75,   135,    43,    44,    45,
      46,    47,    48,    49,    91,   101,    92,   116,   102,   138,
      42,    89,    80,   118,   120,   130,    94,   133,    96,    97,
      98,   103,   121,   139,   110,   111,   112,   113,   104,   116,
      50,    51,    52,    53,    54,   106,    87,   124,   147,   125,
     136,   126,   148,   134,    60,   127,   141,     1,     2,   145,
     107,   108,   137,   140,     3,     4,     5,     6,    61,    62,
      63,    64,    65,   149,   151,    66,     7,     8,     9,    72,
       0,   150,     0,     0,     0,     0,    10,     0,    11,    12,
      13,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,     3,     4,     5,     6,     0,     0,     0,     0,    24,
       0,     0,    82,     7,     8,     9,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    11,    12,    13,    14,    15,
      16,    17,    18,    19,    20,    21,    22,    23,     3,     4,
       5,     6,     0,     0,     0,     0,     0,     0,     0,    85,
       7,     8,     9,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    11,    12,    13,    14,    15,    16,    17,    18,
      19,    20,    21,    22,    23,     3,     4,     5,     6,     0,
       0,     0,     0,     0,     0,     0,     0,     7,     8,     9,
       4,     5,     6,     0,     0,     0,     0,     0,     0,    11,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,     0,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23
  };

  const short
  parser::yycheck_[] =
  {
      24,    34,    68,    36,    80,    58,     8,     9,    52,    53,
      10,    25,    33,    31,    31,    91,    56,    70,    55,    33,
      60,    56,    88,     0,    60,    60,    62,    31,    55,    56,
      57,    52,    53,    47,    48,    49,    50,    51,    56,    56,
      54,    94,    42,     5,     6,     7,   122,    14,    15,    16,
      17,    18,    19,    20,    57,    82,    59,    59,    85,   125,
      30,    59,    61,    96,    97,   118,    60,   120,    55,    55,
      55,    59,    38,   126,    26,    27,    28,    29,    59,    59,
      47,    48,    49,    50,    51,    33,    25,    59,   141,    58,
      33,    59,   145,    56,    33,    60,   129,     3,     4,   132,
      52,    53,    59,   127,    10,    11,    12,    13,    47,    48,
      49,    50,    51,    59,    52,    54,    22,    23,    24,    40,
      -1,   147,    -1,    -1,    -1,    -1,    32,    -1,    34,    35,
      36,    37,    38,    39,    40,    41,    42,    43,    44,    45,
      46,    10,    11,    12,    13,    -1,    -1,    -1,    -1,    55,
      -1,    -1,    21,    22,    23,    24,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    34,    35,    36,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    46,    10,    11,
      12,    13,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    21,
      22,    23,    24,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    34,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,    45,    46,    10,    11,    12,    13,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    22,    23,    24,
      11,    12,    13,    -1,    -1,    -1,    -1,    -1,    -1,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    -1,    34,    35,    36,    37,    38,    39,    40,
      41,    42,    43,    44,    45,    46
  };

  const signed char
  parser::yystos_[] =
  {
       0,     3,     4,    10,    11,    12,    13,    22,    23,    24,
      32,    34,    35,    36,    37,    38,    39,    40,    41,    42,
      43,    44,    45,    46,    55,    64,    65,    66,    67,    68,
      69,    71,    81,    82,    83,    89,    55,    67,    65,     0,
      31,    72,    30,    14,    15,    16,    17,    18,    19,    20,
      47,    48,    49,    50,    51,    86,    87,    88,    89,    25,
      33,    47,    48,    49,    50,    51,    54,    90,    91,    70,
      89,    56,    66,     5,     6,     7,    73,    75,    77,    67,
      61,    79,    21,    68,    68,    21,    68,    25,    91,    59,
      84,    57,    59,    84,    60,    91,    55,    55,    55,    80,
      81,    68,    68,    59,    59,    84,    33,    52,    53,    81,
      26,    27,    28,    29,    85,    91,    59,    76,    89,    74,
      89,    38,    60,    62,    59,    58,    59,    60,    56,    60,
      91,    56,    60,    91,    56,    81,    33,    59,    84,    91,
      65,    89,     8,     9,    78,    89,    56,    91,    91,    59,
      78,    52
  };

  const signed char
  parser::yyr1_[] =
  {
       0,    63,    64,    65,    65,    66,    66,    67,    67,    67,
      67,    67,    67,    67,    67,    67,    68,    68,    69,    69,
      69,    69,    69,    69,    69,    70,    71,    72,    72,    72,
      72,    73,    74,    74,    75,    76,    76,    77,    78,    78,
      79,    80,    80,    81,    81,    81,    81,    81,    81,    81,
      81,    81,    81,    81,    81,    81,    81,    81,    81,    82,
      82,    83,    83,    83,    84,    84,    84,    85,    85,    85,
      85,    86,    86,    86,    87,    87,    87,    87,    88,    88,
      88,    88,    89,    89,    90,    91,    91,    91,    91,    91,
      91,    91,    91
  };

  const signed char
  parser::yyr2_[] =
  {
       0,     2,     2,     1,     3,     1,     3,     3,     4,     3,
       3,     4,     3,     2,     3,     1,     1,     1,     3,     6,
       4,     3,     6,     4,     1,     2,    10,     0,     2,     2,
       2,     4,     2,     4,     4,     3,     5,     4,     1,     1,
       3,     1,     3,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     0,     2,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     0,     2,     2,     1,     5,     1,     1,     1,
       1,     1,     1
  };


#if YYDEBUG || 1
  // YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
  // First, the terminals, then, starting at \a YYNTOKENS, nonterminals.
  const char*
  const parser::yytname_[] =
  {
  "\"end of file\"", "error", "\"invalid token\"", "\"truepredicate\"",
  "\"falsepredicate\"", "\"sort\"", "\"distinct\"", "\"limit\"",
  "\"ascending\"", "\"descending\"", "\"subquery\"", "\"true\"",
  "\"false\"", "\"null\"", "\"==\"", "\"!=\"", "\"IN\"", "\"<\"", "\">\"",
  "\">=\"", "\"<=\"", "\"[c]\"", "\"any\"", "\"all\"", "\"none\"",
  "\"@links\"", "\"@max\"", "\"@min\"", "\"@sun\"", "\"@average\"",
  "\"&&\"", "\"||\"", "\"!\"", "\"identifier\"", "\"string\"",
  "\"base64\"", "\"infinity\"", "\"NaN\"", "\"natural0\"", "\"number\"",
  "\"float\"", "\"date\"", "\"UUID\"", "\"ObjectId\"", "\"link\"",
  "\"typed link\"", "\"argument\"", "\"beginswith\"", "\"endswith\"",
  "\"contains\"", "\"like\"", "\"between\"", "\"@size\"", "\"@type\"",
  "\"key or value\"", "'('", "')'", "'['", "']'", "'.'", "','", "'{'",
  "'}'", "$accept", "query", "pred", "and_pred", "atom_pred", "value",
  "prop", "simple_prop", "subquery", "pred_suffix", "distinct",
  "distinct_param", "sort", "sort_param", "limit", "direction", "list",
  "list_content", "constant", "boolexpr", "comp_type", "post_op",
  "aggr_op", "equality", "relational", "stringop", "path", "path_elem",
  "id", YY_NULLPTR
  };
#endif


#if YYDEBUG
  const short
  parser::yyrline_[] =
  {
       0,   144,   144,   147,   148,   151,   152,   155,   156,   161,
     162,   163,   168,   172,   173,   174,   177,   178,   181,   182,
     183,   184,   185,   186,   187,   190,   193,   196,   197,   198,
     199,   201,   204,   205,   207,   210,   211,   213,   216,   217,
     219,   222,   223,   226,   227,   228,   229,   230,   231,   232,
     233,   234,   235,   236,   237,   238,   239,   240,   241,   244,
     245,   248,   249,   250,   253,   254,   255,   258,   259,   260,
     261,   264,   265,   266,   269,   270,   271,   272,   275,   276,
     277,   278,   281,   282,   285,   288,   289,   290,   291,   292,
     293,   294,   295
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
