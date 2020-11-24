// A Bison parser, made by GNU Bison 3.7.4.

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
# define YY_SYMBOL_PRINT(Title, Symbol)  YYUSE (Symbol)
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
  parser::parser (ParserDriver& drv_yyarg)
#if YYDEBUG
    : yydebug_ (false),
      yycdebug_ (&std::cerr),
#else
    :
#endif
      drv (drv_yyarg)
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
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.YY_MOVE_OR_COPY< AggrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.YY_MOVE_OR_COPY< AndNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.YY_MOVE_OR_COPY< AtomPredNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
        value.YY_MOVE_OR_COPY< ConstantNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.YY_MOVE_OR_COPY< DescriptorNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.YY_MOVE_OR_COPY< DescriptorOrderingNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred: // pred
        value.YY_MOVE_OR_COPY< OrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.YY_MOVE_OR_COPY< PathNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.YY_MOVE_OR_COPY< PostOpNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
        value.YY_MOVE_OR_COPY< PropertyNode* > (YY_MOVE (that.value));
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
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_path_elem: // path_elem
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
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< AggrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.move< AndNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.move< AtomPredNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
        value.move< ConstantNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.move< DescriptorOrderingNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred: // pred
        value.move< OrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
        value.move< PropertyNode* > (YY_MOVE (that.value));
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
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_path_elem: // path_elem
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
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.copy< AggrNode* > (that.value);
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.copy< AndNode* > (that.value);
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.copy< AtomPredNode* > (that.value);
        break;

      case symbol_kind::SYM_constant: // constant
        value.copy< ConstantNode* > (that.value);
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.copy< DescriptorNode* > (that.value);
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.copy< DescriptorOrderingNode* > (that.value);
        break;

      case symbol_kind::SYM_pred: // pred
        value.copy< OrNode* > (that.value);
        break;

      case symbol_kind::SYM_path: // path
        value.copy< PathNode* > (that.value);
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.copy< PostOpNode* > (that.value);
        break;

      case symbol_kind::SYM_prop: // prop
        value.copy< PropertyNode* > (that.value);
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
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_path_elem: // path_elem
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
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< AggrNode* > (that.value);
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.move< AndNode* > (that.value);
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.move< AtomPredNode* > (that.value);
        break;

      case symbol_kind::SYM_constant: // constant
        value.move< ConstantNode* > (that.value);
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (that.value);
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.move< DescriptorOrderingNode* > (that.value);
        break;

      case symbol_kind::SYM_pred: // pred
        value.move< OrNode* > (that.value);
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (that.value);
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (that.value);
        break;

      case symbol_kind::SYM_prop: // prop
        value.move< PropertyNode* > (that.value);
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
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_path_elem: // path_elem
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
    YYUSE (yyoutput);
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

      case symbol_kind::SYM_BACKLINK: // "@link"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_SIZE: // "@size"
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_COUNT: // "@count"
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

      case symbol_kind::SYM_49_: // '('
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_50_: // ')'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_51_: // '.'
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_52_: // ','
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_query: // query
                 { yyo << "<>"; }
        break;

      case symbol_kind::SYM_pred: // pred
                 { yyo << yysym.value.template as < OrNode* > (); }
        break;

      case symbol_kind::SYM_and_pred: // and_pred
                 { yyo << yysym.value.template as < AndNode* > (); }
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
                 { yyo << yysym.value.template as < AtomPredNode* > (); }
        break;

      case symbol_kind::SYM_value: // value
                 { yyo << yysym.value.template as < ValueNode* > (); }
        break;

      case symbol_kind::SYM_prop: // prop
                 { yyo << yysym.value.template as < PropertyNode* > (); }
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
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

      case symbol_kind::SYM_constant: // constant
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
                 { yyo << yysym.value.template as < AggrNode* > (); }
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
            symbol_type yylookahead (yylex (drv));
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
      case symbol_kind::SYM_aggr_op: // aggr_op
        yylhs.value.emplace< AggrNode* > ();
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        yylhs.value.emplace< AndNode* > ();
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        yylhs.value.emplace< AtomPredNode* > ();
        break;

      case symbol_kind::SYM_constant: // constant
        yylhs.value.emplace< ConstantNode* > ();
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        yylhs.value.emplace< DescriptorNode* > ();
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        yylhs.value.emplace< DescriptorOrderingNode* > ();
        break;

      case symbol_kind::SYM_pred: // pred
        yylhs.value.emplace< OrNode* > ();
        break;

      case symbol_kind::SYM_path: // path
        yylhs.value.emplace< PathNode* > ();
        break;

      case symbol_kind::SYM_post_op: // post_op
        yylhs.value.emplace< PostOpNode* > ();
        break;

      case symbol_kind::SYM_prop: // prop
        yylhs.value.emplace< PropertyNode* > ();
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
      case symbol_kind::SYM_ARG: // "argument"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_path_elem: // path_elem
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
  case 2: // query: pred pred_suffix
                       { drv.result = yystack_[1].value.as < OrNode* > (); drv.ordering = yystack_[0].value.as < DescriptorOrderingNode* > (); }
    break;

  case 3: // pred: and_pred
                                { yylhs.value.as < OrNode* > () = drv.m_parse_nodes.create<OrNode>(yystack_[0].value.as < AndNode* > ()); }
    break;

  case 4: // pred: pred "||" and_pred
                                { yystack_[2].value.as < OrNode* > ()->and_preds.emplace_back(yystack_[0].value.as < AndNode* > ()); yylhs.value.as < OrNode* > () = yystack_[2].value.as < OrNode* > (); }
    break;

  case 5: // and_pred: atom_pred
                                { yylhs.value.as < AndNode* > () = drv.m_parse_nodes.create<AndNode>(yystack_[0].value.as < AtomPredNode* > ()); }
    break;

  case 6: // and_pred: and_pred "&&" atom_pred
                                { yystack_[2].value.as < AndNode* > ()->atom_preds.emplace_back(yystack_[0].value.as < AtomPredNode* > ()); yylhs.value.as < AndNode* > () = yystack_[2].value.as < AndNode* > (); }
    break;

  case 7: // atom_pred: value equality value
                                { yylhs.value.as < AtomPredNode* > () = drv.m_parse_nodes.create<EqualitylNode>(yystack_[2].value.as < ValueNode* > (), yystack_[1].value.as < int > (), yystack_[0].value.as < ValueNode* > ()); }
    break;

  case 8: // atom_pred: value equality "[c]" value
                                {
                                    auto tmp = drv.m_parse_nodes.create<EqualitylNode>(yystack_[3].value.as < ValueNode* > (), yystack_[2].value.as < int > (), yystack_[0].value.as < ValueNode* > ());
                                    tmp->case_sensitive = false;
                                    yylhs.value.as < AtomPredNode* > () = tmp;
                                }
    break;

  case 9: // atom_pred: value relational value
                                { yylhs.value.as < AtomPredNode* > () = drv.m_parse_nodes.create<RelationalNode>(yystack_[2].value.as < ValueNode* > (), yystack_[1].value.as < int > (), yystack_[0].value.as < ValueNode* > ()); }
    break;

  case 10: // atom_pred: value stringop value
                                { yylhs.value.as < AtomPredNode* > () = drv.m_parse_nodes.create<StringOpsNode>(yystack_[2].value.as < ValueNode* > (), yystack_[1].value.as < int > (), yystack_[0].value.as < ValueNode* > ()); }
    break;

  case 11: // atom_pred: value stringop "[c]" value
                                {
                                    auto tmp = drv.m_parse_nodes.create<StringOpsNode>(yystack_[3].value.as < ValueNode* > (), yystack_[2].value.as < int > (), yystack_[0].value.as < ValueNode* > ());
                                    tmp->case_sensitive = false;
                                    yylhs.value.as < AtomPredNode* > () = tmp;
                                }
    break;

  case 12: // atom_pred: "!" atom_pred
                                { yylhs.value.as < AtomPredNode* > () = drv.m_parse_nodes.create<NotNode>(yystack_[0].value.as < AtomPredNode* > ()); }
    break;

  case 13: // atom_pred: '(' pred ')'
                                { yylhs.value.as < AtomPredNode* > () = drv.m_parse_nodes.create<ParensNode>(yystack_[1].value.as < OrNode* > ()); }
    break;

  case 14: // atom_pred: boolexpr
                                { yylhs.value.as < AtomPredNode* > () =yystack_[0].value.as < TrueOrFalseNode* > (); }
    break;

  case 15: // value: constant
                                { yylhs.value.as < ValueNode* > () = drv.m_parse_nodes.create<ValueNode>(yystack_[0].value.as < ConstantNode* > ());}
    break;

  case 16: // value: prop
                                { yylhs.value.as < ValueNode* > () = drv.m_parse_nodes.create<ValueNode>(yystack_[0].value.as < PropertyNode* > ());}
    break;

  case 17: // prop: comp_type path id
                                { yylhs.value.as < PropertyNode* > () = drv.m_parse_nodes.create<PropNode>(yystack_[1].value.as < PathNode* > (), yystack_[0].value.as < std::string > (), ExpressionComparisonType(yystack_[2].value.as < int > ())); }
    break;

  case 18: // prop: path id post_op
                                { yylhs.value.as < PropertyNode* > () = drv.m_parse_nodes.create<PropNode>(yystack_[2].value.as < PathNode* > (), yystack_[1].value.as < std::string > (), yystack_[0].value.as < PostOpNode* > ()); }
    break;

  case 19: // prop: path id '.' aggr_op '.' id
                                    { yylhs.value.as < PropertyNode* > () = drv.m_parse_nodes.create<LinkAggrNode>(yystack_[5].value.as < PathNode* > (), yystack_[4].value.as < std::string > (), yystack_[2].value.as < AggrNode* > (), yystack_[0].value.as < std::string > ()); }
    break;

  case 20: // prop: path id '.' aggr_op
                                { yylhs.value.as < PropertyNode* > () = drv.m_parse_nodes.create<ListAggrNode>(yystack_[3].value.as < PathNode* > (), yystack_[2].value.as < std::string > (), yystack_[0].value.as < AggrNode* > ()); }
    break;

  case 21: // pred_suffix: %empty
                                { yylhs.value.as < DescriptorOrderingNode* > () = drv.m_parse_nodes.create<DescriptorOrderingNode>();}
    break;

  case 22: // pred_suffix: pred_suffix sort
                                { yystack_[1].value.as < DescriptorOrderingNode* > ()->add_descriptor(yystack_[0].value.as < DescriptorNode* > ()); yylhs.value.as < DescriptorOrderingNode* > () = yystack_[1].value.as < DescriptorOrderingNode* > (); }
    break;

  case 23: // pred_suffix: pred_suffix distinct
                                { yystack_[1].value.as < DescriptorOrderingNode* > ()->add_descriptor(yystack_[0].value.as < DescriptorNode* > ()); yylhs.value.as < DescriptorOrderingNode* > () = yystack_[1].value.as < DescriptorOrderingNode* > (); }
    break;

  case 24: // pred_suffix: pred_suffix limit
                                { yystack_[1].value.as < DescriptorOrderingNode* > ()->add_descriptor(yystack_[0].value.as < DescriptorNode* > ()); yylhs.value.as < DescriptorOrderingNode* > () = yystack_[1].value.as < DescriptorOrderingNode* > (); }
    break;

  case 25: // distinct: "distinct" '(' distinct_param ')'
                                          { yylhs.value.as < DescriptorNode* > () = yystack_[1].value.as < DescriptorNode* > (); }
    break;

  case 26: // distinct_param: path id
                                { yylhs.value.as < DescriptorNode* > () = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::DISTINCT); yylhs.value.as < DescriptorNode* > ()->add(yystack_[1].value.as < PathNode* > ()->path_elems, yystack_[0].value.as < std::string > ());}
    break;

  case 27: // distinct_param: distinct_param ',' path id
                                 { yystack_[3].value.as < DescriptorNode* > ()->add(yystack_[1].value.as < PathNode* > ()->path_elems, yystack_[0].value.as < std::string > ()); yylhs.value.as < DescriptorNode* > () = yystack_[3].value.as < DescriptorNode* > (); }
    break;

  case 28: // sort: "sort" '(' sort_param ')'
                                 { yylhs.value.as < DescriptorNode* > () = yystack_[1].value.as < DescriptorNode* > (); }
    break;

  case 29: // sort_param: path id direction
                                { yylhs.value.as < DescriptorNode* > () = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::SORT); yylhs.value.as < DescriptorNode* > ()->add(yystack_[2].value.as < PathNode* > ()->path_elems, yystack_[1].value.as < std::string > (), yystack_[0].value.as < bool > ());}
    break;

  case 30: // sort_param: sort_param ',' path id direction
                                        { yystack_[4].value.as < DescriptorNode* > ()->add(yystack_[2].value.as < PathNode* > ()->path_elems, yystack_[1].value.as < std::string > (), yystack_[0].value.as < bool > ()); yylhs.value.as < DescriptorNode* > () = yystack_[4].value.as < DescriptorNode* > (); }
    break;

  case 31: // limit: "limit" '(' "natural0" ')'
                                { yylhs.value.as < DescriptorNode* > () = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::LIMIT, yystack_[1].value.as < std::string > ()); }
    break;

  case 32: // direction: "ascending"
                                { yylhs.value.as < bool > () = true; }
    break;

  case 33: // direction: "descending"
                                { yylhs.value.as < bool > () = false; }
    break;

  case 34: // constant: "natural0"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, yystack_[0].value.as < std::string > ()); }
    break;

  case 35: // constant: "number"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, yystack_[0].value.as < std::string > ()); }
    break;

  case 36: // constant: "infinity"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::INFINITY_VAL, yystack_[0].value.as < std::string > ()); }
    break;

  case 37: // constant: "NaN"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NAN_VAL, yystack_[0].value.as < std::string > ()); }
    break;

  case 38: // constant: "string"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::STRING, yystack_[0].value.as < std::string > ()); }
    break;

  case 39: // constant: "base64"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::BASE64, yystack_[0].value.as < std::string > ()); }
    break;

  case 40: // constant: "float"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FLOAT, yystack_[0].value.as < std::string > ()); }
    break;

  case 41: // constant: "date"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TIMESTAMP, yystack_[0].value.as < std::string > ()); }
    break;

  case 42: // constant: "UUID"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::UUID_T, yystack_[0].value.as < std::string > ()); }
    break;

  case 43: // constant: "ObjectId"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::OID, yystack_[0].value.as < std::string > ()); }
    break;

  case 44: // constant: "true"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TRUE, ""); }
    break;

  case 45: // constant: "false"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FALSE, ""); }
    break;

  case 46: // constant: "null"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NULL_VAL, ""); }
    break;

  case 47: // constant: "argument"
                                { yylhs.value.as < ConstantNode* > () = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::ARG, yystack_[0].value.as < std::string > ()); }
    break;

  case 48: // boolexpr: "truepredicate"
                                { yylhs.value.as < TrueOrFalseNode* > () = drv.m_parse_nodes.create<TrueOrFalseNode>(true); }
    break;

  case 49: // boolexpr: "falsepredicate"
                                { yylhs.value.as < TrueOrFalseNode* > () = drv.m_parse_nodes.create<TrueOrFalseNode>(false); }
    break;

  case 50: // comp_type: "any"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::Any); }
    break;

  case 51: // comp_type: "all"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::All); }
    break;

  case 52: // comp_type: "none"
                                { yylhs.value.as < int > () = int(ExpressionComparisonType::None); }
    break;

  case 53: // post_op: %empty
                                { yylhs.value.as < PostOpNode* > () = nullptr; }
    break;

  case 54: // post_op: '.' "@count"
                                { yylhs.value.as < PostOpNode* > () = drv.m_parse_nodes.create<PostOpNode>(PostOpNode::COUNT);}
    break;

  case 55: // post_op: '.' "@size"
                                { yylhs.value.as < PostOpNode* > () = drv.m_parse_nodes.create<PostOpNode>(PostOpNode::SIZE);}
    break;

  case 56: // aggr_op: "@max"
                                { yylhs.value.as < AggrNode* > () = drv.m_parse_nodes.create<AggrNode>(AggrNode::MAX);}
    break;

  case 57: // aggr_op: "@min"
                                { yylhs.value.as < AggrNode* > () = drv.m_parse_nodes.create<AggrNode>(AggrNode::MIN);}
    break;

  case 58: // aggr_op: "@sun"
                                { yylhs.value.as < AggrNode* > () = drv.m_parse_nodes.create<AggrNode>(AggrNode::SUM);}
    break;

  case 59: // aggr_op: "@average"
                                { yylhs.value.as < AggrNode* > () = drv.m_parse_nodes.create<AggrNode>(AggrNode::AVG);}
    break;

  case 60: // equality: "=="
                                { yylhs.value.as < int > () = CompareNode::EQUAL; }
    break;

  case 61: // equality: "!="
                                { yylhs.value.as < int > () = CompareNode::NOT_EQUAL; }
    break;

  case 62: // relational: "<"
                                { yylhs.value.as < int > () = CompareNode::LESS; }
    break;

  case 63: // relational: "<="
                                { yylhs.value.as < int > () = CompareNode::LESS_EQUAL; }
    break;

  case 64: // relational: ">"
                                { yylhs.value.as < int > () = CompareNode::GREATER; }
    break;

  case 65: // relational: ">="
                                { yylhs.value.as < int > () = CompareNode::GREATER_EQUAL; }
    break;

  case 66: // stringop: "beginswith"
                                { yylhs.value.as < int > () = CompareNode::BEGINSWITH; }
    break;

  case 67: // stringop: "endswith"
                                { yylhs.value.as < int > () = CompareNode::ENDSWITH; }
    break;

  case 68: // stringop: "contains"
                                { yylhs.value.as < int > () = CompareNode::CONTAINS; }
    break;

  case 69: // stringop: "like"
                                { yylhs.value.as < int > () = CompareNode::LIKE; }
    break;

  case 70: // path: %empty
                                { yylhs.value.as < PathNode* > () = drv.m_parse_nodes.create<PathNode>(); }
    break;

  case 71: // path: path path_elem
                                { yystack_[1].value.as < PathNode* > ()->add_element(yystack_[0].value.as < std::string > ()); yylhs.value.as < PathNode* > () = yystack_[1].value.as < PathNode* > (); }
    break;

  case 72: // path_elem: id '.'
                                { yylhs.value.as < std::string > () = yystack_[1].value.as < std::string > (); }
    break;

  case 73: // id: "identifier"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 74: // id: "@link" '.' "identifier" '.' "identifier"
                                { yylhs.value.as < std::string > () = std::string("@links.") + yystack_[2].value.as < std::string > () + "." + yystack_[0].value.as < std::string > (); }
    break;

  case 75: // id: "beginswith"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 76: // id: "endswith"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 77: // id: "contains"
                                { yylhs.value.as < std::string > () = yystack_[0].value.as < std::string > (); }
    break;

  case 78: // id: "like"
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


  const signed char parser::yypact_ninf_ = -49;

  const signed char parser::yytable_ninf_ = -1;

  const short
  parser::yypact_[] =
  {
      55,   -49,   -49,   -49,   -49,   -49,   -49,   -49,   -49,    55,
     -49,   -49,   -49,   -49,   -49,   -49,   -49,   -49,   -49,   -49,
     -49,    55,    28,    10,    12,   -49,    -9,   -49,   -49,   -49,
     -49,   -13,   -49,   -20,   -49,    55,    20,    55,   -49,   -49,
     -49,   -49,   -49,   -49,   -49,   -49,   -49,   -49,    90,   160,
     125,   -13,     0,   -49,   -49,   -49,   -49,   -49,   -49,     1,
     -49,    12,     4,     5,     6,   -49,   -49,   -49,   -49,   160,
     -49,   -49,   160,   -49,    11,    23,   -11,   -49,   -49,   -49,
      19,   -49,   -49,   -49,    17,   -49,   -49,   -49,   -49,   -49,
     -49,    18,   -31,   -13,   -21,   -13,    13,    38,   -13,   -49,
     -49,    -7,   -49,   -49,    11,   -49,   -49,   -49,   -13,   -49,
     -49,   -49,   -13,    -7,    11,   -49
  };

  const signed char
  parser::yydefact_[] =
  {
      70,    48,    49,    44,    45,    46,    50,    51,    52,    70,
      38,    39,    36,    37,    34,    35,    40,    41,    42,    43,
      47,    70,     0,    21,     3,     5,     0,    16,    15,    14,
      70,     0,    12,     0,     1,    70,     2,    70,    60,    61,
      62,    64,    65,    63,    66,    67,    68,    69,    70,    70,
      70,     0,     0,    73,    75,    76,    77,    78,    71,    53,
      13,     4,     0,     0,     0,    23,    22,    24,     6,    70,
       7,     9,    70,    10,    17,     0,    72,    18,    70,    70,
       0,     8,    11,    72,     0,    55,    54,    56,    57,    58,
      59,    20,     0,     0,     0,     0,     0,     0,     0,    28,
      70,     0,    25,    70,    26,    31,    74,    19,     0,    32,
      33,    29,     0,     0,    27,    30
  };

  const signed char
  parser::yypgoto_[] =
  {
     -49,   -49,    40,    37,     3,   -26,   -49,   -49,   -49,   -49,
     -49,   -49,   -49,   -39,   -49,   -49,   -49,   -49,   -49,   -49,
     -49,   -49,   -30,   -49,   -48
  };

  const signed char
  parser::yydefgoto_[] =
  {
      -1,    22,    23,    24,    25,    26,    27,    36,    65,    94,
      66,    92,    67,   111,    28,    29,    30,    77,    91,    48,
      49,    50,    31,    58,    59
  };

  const signed char
  parser::yytable_[] =
  {
      51,   109,   110,    74,    38,    39,    40,    41,    42,    43,
      52,    35,    32,    85,    86,    87,    88,    89,    90,    99,
      53,   100,    70,    71,    73,    62,    63,    64,    34,   102,
      60,   103,    54,    55,    56,    57,    44,    45,    46,    47,
      68,    35,    37,    81,    83,   101,    82,   104,    93,    95,
     107,    75,    76,    78,    79,    80,    84,    96,     1,     2,
     113,    33,    83,   105,   114,     3,     4,     5,    97,    98,
     108,   106,    61,   112,   115,     6,     7,     8,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     9,     0,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    20,
       3,     4,     5,     0,    21,     0,     0,     0,     0,    69,
       6,     7,     8,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    10,    11,    12,    13,    14,    15,
      16,    17,    18,    19,    20,     3,     4,     5,     0,     0,
       0,     0,     0,     0,    72,     6,     7,     8,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    20,
       3,     4,     5,     0,     0,     0,     0,     0,     0,     0,
       6,     7,     8,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    10,    11,    12,    13,    14,    15,
      16,    17,    18,    19,    20
  };

  const signed char
  parser::yycheck_[] =
  {
      30,     8,     9,    51,    13,    14,    15,    16,    17,    18,
      23,    31,     9,    24,    25,    26,    27,    28,    29,    50,
      33,    52,    48,    49,    50,     5,     6,     7,     0,    50,
      50,    52,    45,    46,    47,    48,    45,    46,    47,    48,
      37,    31,    30,    69,    51,    93,    72,    95,    78,    79,
      98,    51,    51,    49,    49,    49,    33,    38,     3,     4,
     108,    21,    51,    50,   112,    10,    11,    12,    51,    51,
     100,    33,    35,   103,   113,    20,    21,    22,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    -1,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      10,    11,    12,    -1,    49,    -1,    -1,    -1,    -1,    19,
      20,    21,    22,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    34,    35,    36,    37,    38,    39,
      40,    41,    42,    43,    44,    10,    11,    12,    -1,    -1,
      -1,    -1,    -1,    -1,    19,    20,    21,    22,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      10,    11,    12,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      20,    21,    22,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    34,    35,    36,    37,    38,    39,
      40,    41,    42,    43,    44
  };

  const signed char
  parser::yystos_[] =
  {
       0,     3,     4,    10,    11,    12,    20,    21,    22,    32,
      34,    35,    36,    37,    38,    39,    40,    41,    42,    43,
      44,    49,    54,    55,    56,    57,    58,    59,    67,    68,
      69,    75,    57,    55,     0,    31,    60,    30,    13,    14,
      15,    16,    17,    18,    45,    46,    47,    48,    72,    73,
      74,    75,    23,    33,    45,    46,    47,    48,    76,    77,
      50,    56,     5,     6,     7,    61,    63,    65,    57,    19,
      58,    58,    19,    58,    77,    51,    51,    70,    49,    49,
      49,    58,    58,    51,    33,    24,    25,    26,    27,    28,
      29,    71,    64,    75,    62,    75,    38,    51,    51,    50,
      52,    77,    50,    52,    77,    50,    33,    77,    75,     8,
       9,    66,    75,    77,    77,    66
  };

  const signed char
  parser::yyr1_[] =
  {
       0,    53,    54,    55,    55,    56,    56,    57,    57,    57,
      57,    57,    57,    57,    57,    58,    58,    59,    59,    59,
      59,    60,    60,    60,    60,    61,    62,    62,    63,    64,
      64,    65,    66,    66,    67,    67,    67,    67,    67,    67,
      67,    67,    67,    67,    67,    67,    67,    67,    68,    68,
      69,    69,    69,    70,    70,    70,    71,    71,    71,    71,
      72,    72,    73,    73,    73,    73,    74,    74,    74,    74,
      75,    75,    76,    77,    77,    77,    77,    77,    77
  };

  const signed char
  parser::yyr2_[] =
  {
       0,     2,     2,     1,     3,     1,     3,     3,     4,     3,
       3,     4,     2,     3,     1,     1,     1,     3,     3,     6,
       4,     0,     2,     2,     2,     4,     2,     4,     4,     3,
       5,     4,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     0,     2,     2,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       0,     2,     2,     1,     5,     1,     1,     1,     1
  };


#if YYDEBUG || 1
  // YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
  // First, the terminals, then, starting at \a YYNTOKENS, nonterminals.
  const char*
  const parser::yytname_[] =
  {
  "\"end of file\"", "error", "\"invalid token\"", "\"truepredicate\"",
  "\"falsepredicate\"", "\"sort\"", "\"distinct\"", "\"limit\"",
  "\"ascending\"", "\"descending\"", "\"true\"", "\"false\"", "\"null\"",
  "\"==\"", "\"!=\"", "\"<\"", "\">\"", "\">=\"", "\"<=\"", "\"[c]\"",
  "\"any\"", "\"all\"", "\"none\"", "\"@link\"", "\"@size\"", "\"@count\"",
  "\"@max\"", "\"@min\"", "\"@sun\"", "\"@average\"", "\"&&\"", "\"||\"",
  "\"!\"", "\"identifier\"", "\"string\"", "\"base64\"", "\"infinity\"",
  "\"NaN\"", "\"natural0\"", "\"number\"", "\"float\"", "\"date\"",
  "\"UUID\"", "\"ObjectId\"", "\"argument\"", "\"beginswith\"",
  "\"endswith\"", "\"contains\"", "\"like\"", "'('", "')'", "'.'", "','",
  "$accept", "query", "pred", "and_pred", "atom_pred", "value", "prop",
  "pred_suffix", "distinct", "distinct_param", "sort", "sort_param",
  "limit", "direction", "constant", "boolexpr", "comp_type", "post_op",
  "aggr_op", "equality", "relational", "stringop", "path", "path_elem",
  "id", YY_NULLPTR
  };
#endif


#if YYDEBUG
  const unsigned char
  parser::yyrline_[] =
  {
       0,   127,   127,   130,   131,   134,   135,   138,   139,   144,
     145,   146,   151,   152,   153,   156,   157,   160,   161,   162,
     163,   166,   167,   168,   169,   171,   174,   175,   177,   180,
     181,   183,   186,   187,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   206,   207,
     210,   211,   212,   215,   216,   217,   220,   221,   222,   223,
     226,   227,   230,   231,   232,   233,   236,   237,   238,   239,
     242,   243,   246,   249,   250,   251,   252,   253,   254
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
