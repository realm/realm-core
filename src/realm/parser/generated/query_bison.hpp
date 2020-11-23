// A Bison parser, made by GNU Bison 3.7.4.

// Skeleton interface for Bison LALR(1) parsers in C++

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


/**
 ** \file generated/query_bison.hpp
 ** Define the yy::parser class.
 */

// C++ LALR(1) parser skeleton written by Akim Demaille.

// DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
// especially those whose name start with YY_ or yy_.  They are
// private implementation details that can be changed or removed.

#ifndef YY_YY_GENERATED_QUERY_BISON_HPP_INCLUDED
# define YY_YY_GENERATED_QUERY_BISON_HPP_INCLUDED
// "%code requires" blocks.

  # include <string>
  # include "realm/query_expression.hpp"
  namespace realm::query_parser {
    class ParserDriver;
    class ConstantNode;
    class PropertyNode;
    class PostOpNode;
    class AggrNode;
    class ValueNode;
    class TrueOrFalseNode;
    class OrNode;
    class AndNode;
    class AtomPredNode;
    class PathNode;
    class DescriptorOrderingNode;
    class DescriptorNode;
  }  
  using namespace realm::query_parser;


# include <cassert>
# include <cstdlib> // std::abort
# include <iostream>
# include <stdexcept>
# include <string>
# include <vector>

#if defined __cplusplus
# define YY_CPLUSPLUS __cplusplus
#else
# define YY_CPLUSPLUS 199711L
#endif

// Support move semantics when possible.
#if 201103L <= YY_CPLUSPLUS
# define YY_MOVE           std::move
# define YY_MOVE_OR_COPY   move
# define YY_MOVE_REF(Type) Type&&
# define YY_RVREF(Type)    Type&&
# define YY_COPY(Type)     Type
#else
# define YY_MOVE
# define YY_MOVE_OR_COPY   copy
# define YY_MOVE_REF(Type) Type&
# define YY_RVREF(Type)    const Type&
# define YY_COPY(Type)     const Type&
#endif

// Support noexcept when possible.
#if 201103L <= YY_CPLUSPLUS
# define YY_NOEXCEPT noexcept
# define YY_NOTHROW
#else
# define YY_NOEXCEPT
# define YY_NOTHROW throw ()
#endif

// Support constexpr when possible.
#if 201703 <= YY_CPLUSPLUS
# define YY_CONSTEXPR constexpr
#else
# define YY_CONSTEXPR
#endif

#include <typeinfo>
#ifndef YY_ASSERT
# include <cassert>
# define YY_ASSERT assert
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                            \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 1
#endif

namespace yy {




  /// A Bison parser.
  class parser
  {
  public:
#ifndef YYSTYPE
  /// A buffer to store and retrieve objects.
  ///
  /// Sort of a variant, but does not keep track of the nature
  /// of the stored data, since that knowledge is available
  /// via the current parser state.
  class semantic_type
  {
  public:
    /// Type of *this.
    typedef semantic_type self_type;

    /// Empty construction.
    semantic_type () YY_NOEXCEPT
      : yybuffer_ ()
      , yytypeid_ (YY_NULLPTR)
    {}

    /// Construct and fill.
    template <typename T>
    semantic_type (YY_RVREF (T) t)
      : yytypeid_ (&typeid (T))
    {
      YY_ASSERT (sizeof (T) <= size);
      new (yyas_<T> ()) T (YY_MOVE (t));
    }

#if 201103L <= YY_CPLUSPLUS
    /// Non copyable.
    semantic_type (const self_type&) = delete;
    /// Non copyable.
    self_type& operator= (const self_type&) = delete;
#endif

    /// Destruction, allowed only if empty.
    ~semantic_type () YY_NOEXCEPT
    {
      YY_ASSERT (!yytypeid_);
    }

# if 201103L <= YY_CPLUSPLUS
    /// Instantiate a \a T in here from \a t.
    template <typename T, typename... U>
    T&
    emplace (U&&... u)
    {
      YY_ASSERT (!yytypeid_);
      YY_ASSERT (sizeof (T) <= size);
      yytypeid_ = & typeid (T);
      return *new (yyas_<T> ()) T (std::forward <U>(u)...);
    }
# else
    /// Instantiate an empty \a T in here.
    template <typename T>
    T&
    emplace ()
    {
      YY_ASSERT (!yytypeid_);
      YY_ASSERT (sizeof (T) <= size);
      yytypeid_ = & typeid (T);
      return *new (yyas_<T> ()) T ();
    }

    /// Instantiate a \a T in here from \a t.
    template <typename T>
    T&
    emplace (const T& t)
    {
      YY_ASSERT (!yytypeid_);
      YY_ASSERT (sizeof (T) <= size);
      yytypeid_ = & typeid (T);
      return *new (yyas_<T> ()) T (t);
    }
# endif

    /// Instantiate an empty \a T in here.
    /// Obsolete, use emplace.
    template <typename T>
    T&
    build ()
    {
      return emplace<T> ();
    }

    /// Instantiate a \a T in here from \a t.
    /// Obsolete, use emplace.
    template <typename T>
    T&
    build (const T& t)
    {
      return emplace<T> (t);
    }

    /// Accessor to a built \a T.
    template <typename T>
    T&
    as () YY_NOEXCEPT
    {
      YY_ASSERT (yytypeid_);
      YY_ASSERT (*yytypeid_ == typeid (T));
      YY_ASSERT (sizeof (T) <= size);
      return *yyas_<T> ();
    }

    /// Const accessor to a built \a T (for %printer).
    template <typename T>
    const T&
    as () const YY_NOEXCEPT
    {
      YY_ASSERT (yytypeid_);
      YY_ASSERT (*yytypeid_ == typeid (T));
      YY_ASSERT (sizeof (T) <= size);
      return *yyas_<T> ();
    }

    /// Swap the content with \a that, of same type.
    ///
    /// Both variants must be built beforehand, because swapping the actual
    /// data requires reading it (with as()), and this is not possible on
    /// unconstructed variants: it would require some dynamic testing, which
    /// should not be the variant's responsibility.
    /// Swapping between built and (possibly) non-built is done with
    /// self_type::move ().
    template <typename T>
    void
    swap (self_type& that) YY_NOEXCEPT
    {
      YY_ASSERT (yytypeid_);
      YY_ASSERT (*yytypeid_ == *that.yytypeid_);
      std::swap (as<T> (), that.as<T> ());
    }

    /// Move the content of \a that to this.
    ///
    /// Destroys \a that.
    template <typename T>
    void
    move (self_type& that)
    {
# if 201103L <= YY_CPLUSPLUS
      emplace<T> (std::move (that.as<T> ()));
# else
      emplace<T> ();
      swap<T> (that);
# endif
      that.destroy<T> ();
    }

# if 201103L <= YY_CPLUSPLUS
    /// Move the content of \a that to this.
    template <typename T>
    void
    move (self_type&& that)
    {
      emplace<T> (std::move (that.as<T> ()));
      that.destroy<T> ();
    }
#endif

    /// Copy the content of \a that to this.
    template <typename T>
    void
    copy (const self_type& that)
    {
      emplace<T> (that.as<T> ());
    }

    /// Destroy the stored \a T.
    template <typename T>
    void
    destroy ()
    {
      as<T> ().~T ();
      yytypeid_ = YY_NULLPTR;
    }

  private:
#if YY_CPLUSPLUS < 201103L
    /// Non copyable.
    semantic_type (const self_type&);
    /// Non copyable.
    self_type& operator= (const self_type&);
#endif

    /// Accessor to raw memory as \a T.
    template <typename T>
    T*
    yyas_ () YY_NOEXCEPT
    {
      void *yyp = yybuffer_.yyraw;
      return static_cast<T*> (yyp);
     }

    /// Const accessor to raw memory as \a T.
    template <typename T>
    const T*
    yyas_ () const YY_NOEXCEPT
    {
      const void *yyp = yybuffer_.yyraw;
      return static_cast<const T*> (yyp);
     }

    /// An auxiliary type to compute the largest semantic type.
    union union_type
    {
      // aggr_op
      char dummy1[sizeof (AggrNode*)];

      // and_pred
      char dummy2[sizeof (AndNode*)];

      // atom_pred
      char dummy3[sizeof (AtomPredNode*)];

      // constant
      char dummy4[sizeof (ConstantNode*)];

      // distinct
      // distinct_param
      // sort
      // sort_param
      // limit
      char dummy5[sizeof (DescriptorNode*)];

      // pred_suffix
      char dummy6[sizeof (DescriptorOrderingNode*)];

      // pred
      char dummy7[sizeof (OrNode*)];

      // path
      char dummy8[sizeof (PathNode*)];

      // post_op
      char dummy9[sizeof (PostOpNode*)];

      // prop
      char dummy10[sizeof (PropertyNode*)];

      // boolexpr
      char dummy11[sizeof (TrueOrFalseNode*)];

      // value
      char dummy12[sizeof (ValueNode*)];

      // direction
      char dummy13[sizeof (bool)];

      // comp_type
      // equality
      // relational
      // stringop
      char dummy14[sizeof (int)];

      // "identifier"
      // "string"
      // "base64"
      // "infinity"
      // "NaN"
      // "natural0"
      // "number"
      // "float"
      // "date"
      // "UUID"
      // "ObjectId"
      // "argument"
      // "beginswith"
      // "endswith"
      // "contains"
      // "like"
      // path_elem
      // id
      char dummy15[sizeof (std::string)];
    };

    /// The size of the largest semantic type.
    enum { size = sizeof (union_type) };

    /// A buffer to store semantic values.
    union
    {
      /// Strongest alignment constraints.
      long double yyalign_me;
      /// A buffer large enough to store any of the semantic values.
      char yyraw[size];
    } yybuffer_;

    /// Whether the content is built: if defined, the name of the stored type.
    const std::type_info *yytypeid_;
  };

#else
    typedef YYSTYPE semantic_type;
#endif

    /// Syntax errors thrown from user actions.
    struct syntax_error : std::runtime_error
    {
      syntax_error (const std::string& m)
        : std::runtime_error (m)
      {}

      syntax_error (const syntax_error& s)
        : std::runtime_error (s.what ())
      {}

      ~syntax_error () YY_NOEXCEPT YY_NOTHROW;
    };

    /// Token kinds.
    struct token
    {
      enum token_kind_type
      {
        TOK_YYEMPTY = -2,
    TOK_END = 0,                   // "end of file"
    TOK_YYerror = 256,             // error
    TOK_YYUNDEF = 257,             // "invalid token"
    TOK_TRUEPREDICATE = 258,       // "truepredicate"
    TOK_FALSEPREDICATE = 259,      // "falsepredicate"
    TOK_SORT = 260,                // "sort"
    TOK_DISTINCT = 261,            // "distinct"
    TOK_LIMIT = 262,               // "limit"
    TOK_ASCENDING = 263,           // "ascending"
    TOK_DESCENDING = 264,          // "descending"
    TOK_TRUE = 265,                // "true"
    TOK_FALSE = 266,               // "false"
    TOK_NULL_VAL = 267,            // "null"
    TOK_EQUAL = 268,               // "=="
    TOK_NOT_EQUAL = 269,           // "!="
    TOK_LESS = 270,                // "<"
    TOK_GREATER = 271,             // ">"
    TOK_GREATER_EQUAL = 272,       // ">="
    TOK_LESS_EQUAL = 273,          // "<="
    TOK_CASE = 274,                // "[c]"
    TOK_ANY = 275,                 // "any"
    TOK_ALL = 276,                 // "all"
    TOK_NONE = 277,                // "none"
    TOK_BACKLINK = 278,            // "@link"
    TOK_SIZE = 279,                // "@size"
    TOK_COUNT = 280,               // "@count"
    TOK_MAX = 281,                 // "@max"
    TOK_MIN = 282,                 // "@min"
    TOK_SUM = 283,                 // "@sun"
    TOK_AVG = 284,                 // "@average"
    TOK_AND = 285,                 // "&&"
    TOK_OR = 286,                  // "||"
    TOK_NOT = 287,                 // "!"
    TOK_ID = 288,                  // "identifier"
    TOK_STRING = 289,              // "string"
    TOK_BASE64 = 290,              // "base64"
    TOK_INFINITY = 291,            // "infinity"
    TOK_NAN = 292,                 // "NaN"
    TOK_NATURAL0 = 293,            // "natural0"
    TOK_NUMBER = 294,              // "number"
    TOK_FLOAT = 295,               // "float"
    TOK_TIMESTAMP = 296,           // "date"
    TOK_UUID = 297,                // "UUID"
    TOK_OID = 298,                 // "ObjectId"
    TOK_ARG = 299,                 // "argument"
    TOK_BEGINSWITH = 300,          // "beginswith"
    TOK_ENDSWITH = 301,            // "endswith"
    TOK_CONTAINS = 302,            // "contains"
    TOK_LIKE = 303                 // "like"
      };
      /// Backward compatibility alias (Bison 3.6).
      typedef token_kind_type yytokentype;
    };

    /// Token kind, as returned by yylex.
    typedef token::yytokentype token_kind_type;

    /// Backward compatibility alias (Bison 3.6).
    typedef token_kind_type token_type;

    /// Symbol kinds.
    struct symbol_kind
    {
      enum symbol_kind_type
      {
        YYNTOKENS = 53, ///< Number of tokens.
        SYM_YYEMPTY = -2,
        SYM_YYEOF = 0,                           // "end of file"
        SYM_YYerror = 1,                         // error
        SYM_YYUNDEF = 2,                         // "invalid token"
        SYM_TRUEPREDICATE = 3,                   // "truepredicate"
        SYM_FALSEPREDICATE = 4,                  // "falsepredicate"
        SYM_SORT = 5,                            // "sort"
        SYM_DISTINCT = 6,                        // "distinct"
        SYM_LIMIT = 7,                           // "limit"
        SYM_ASCENDING = 8,                       // "ascending"
        SYM_DESCENDING = 9,                      // "descending"
        SYM_TRUE = 10,                           // "true"
        SYM_FALSE = 11,                          // "false"
        SYM_NULL_VAL = 12,                       // "null"
        SYM_EQUAL = 13,                          // "=="
        SYM_NOT_EQUAL = 14,                      // "!="
        SYM_LESS = 15,                           // "<"
        SYM_GREATER = 16,                        // ">"
        SYM_GREATER_EQUAL = 17,                  // ">="
        SYM_LESS_EQUAL = 18,                     // "<="
        SYM_CASE = 19,                           // "[c]"
        SYM_ANY = 20,                            // "any"
        SYM_ALL = 21,                            // "all"
        SYM_NONE = 22,                           // "none"
        SYM_BACKLINK = 23,                       // "@link"
        SYM_SIZE = 24,                           // "@size"
        SYM_COUNT = 25,                          // "@count"
        SYM_MAX = 26,                            // "@max"
        SYM_MIN = 27,                            // "@min"
        SYM_SUM = 28,                            // "@sun"
        SYM_AVG = 29,                            // "@average"
        SYM_AND = 30,                            // "&&"
        SYM_OR = 31,                             // "||"
        SYM_NOT = 32,                            // "!"
        SYM_ID = 33,                             // "identifier"
        SYM_STRING = 34,                         // "string"
        SYM_BASE64 = 35,                         // "base64"
        SYM_INFINITY = 36,                       // "infinity"
        SYM_NAN = 37,                            // "NaN"
        SYM_NATURAL0 = 38,                       // "natural0"
        SYM_NUMBER = 39,                         // "number"
        SYM_FLOAT = 40,                          // "float"
        SYM_TIMESTAMP = 41,                      // "date"
        SYM_UUID = 42,                           // "UUID"
        SYM_OID = 43,                            // "ObjectId"
        SYM_ARG = 44,                            // "argument"
        SYM_BEGINSWITH = 45,                     // "beginswith"
        SYM_ENDSWITH = 46,                       // "endswith"
        SYM_CONTAINS = 47,                       // "contains"
        SYM_LIKE = 48,                           // "like"
        SYM_49_ = 49,                            // '('
        SYM_50_ = 50,                            // ')'
        SYM_51_ = 51,                            // '.'
        SYM_52_ = 52,                            // ','
        SYM_YYACCEPT = 53,                       // $accept
        SYM_query = 54,                          // query
        SYM_pred = 55,                           // pred
        SYM_and_pred = 56,                       // and_pred
        SYM_atom_pred = 57,                      // atom_pred
        SYM_value = 58,                          // value
        SYM_prop = 59,                           // prop
        SYM_pred_suffix = 60,                    // pred_suffix
        SYM_distinct = 61,                       // distinct
        SYM_distinct_param = 62,                 // distinct_param
        SYM_sort = 63,                           // sort
        SYM_sort_param = 64,                     // sort_param
        SYM_limit = 65,                          // limit
        SYM_direction = 66,                      // direction
        SYM_constant = 67,                       // constant
        SYM_boolexpr = 68,                       // boolexpr
        SYM_comp_type = 69,                      // comp_type
        SYM_post_op = 70,                        // post_op
        SYM_aggr_op = 71,                        // aggr_op
        SYM_equality = 72,                       // equality
        SYM_relational = 73,                     // relational
        SYM_stringop = 74,                       // stringop
        SYM_path = 75,                           // path
        SYM_path_elem = 76,                      // path_elem
        SYM_id = 77                              // id
      };
    };

    /// (Internal) symbol kind.
    typedef symbol_kind::symbol_kind_type symbol_kind_type;

    /// The number of tokens.
    static const symbol_kind_type YYNTOKENS = symbol_kind::YYNTOKENS;

    /// A complete symbol.
    ///
    /// Expects its Base type to provide access to the symbol kind
    /// via kind ().
    ///
    /// Provide access to semantic value.
    template <typename Base>
    struct basic_symbol : Base
    {
      /// Alias to Base.
      typedef Base super_type;

      /// Default constructor.
      basic_symbol ()
        : value ()
      {}

#if 201103L <= YY_CPLUSPLUS
      /// Move constructor.
      basic_symbol (basic_symbol&& that)
        : Base (std::move (that))
        , value ()
      {
        switch (this->kind ())
    {
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< AggrNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.move< AndNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.move< AtomPredNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
        value.move< ConstantNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.move< DescriptorOrderingNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_pred: // pred
        value.move< OrNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
        value.move< PropertyNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.move< TrueOrFalseNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_value: // value
        value.move< ValueNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_direction: // direction
        value.move< bool > (std::move (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.move< int > (std::move (that.value));
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
        value.move< std::string > (std::move (that.value));
        break;

      default:
        break;
    }

      }
#endif

      /// Copy constructor.
      basic_symbol (const basic_symbol& that);

      /// Constructors for typed symbols.
#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t)
        : Base (t)
      {}
#else
      basic_symbol (typename Base::kind_type t)
        : Base (t)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, AggrNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const AggrNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, AndNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const AndNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, AtomPredNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const AtomPredNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, ConstantNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const ConstantNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, DescriptorNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const DescriptorNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, DescriptorOrderingNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const DescriptorOrderingNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, OrNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const OrNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, PathNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const PathNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, PostOpNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const PostOpNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, PropertyNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const PropertyNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, TrueOrFalseNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const TrueOrFalseNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, ValueNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const ValueNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, bool&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const bool& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, int&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const int& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, std::string&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const std::string& v)
        : Base (t)
        , value (v)
      {}
#endif

      /// Destroy the symbol.
      ~basic_symbol ()
      {
        clear ();
      }

      /// Destroy contents, and record that is empty.
      void clear ()
      {
        // User destructor.
        symbol_kind_type yykind = this->kind ();
        basic_symbol<Base>& yysym = *this;
        (void) yysym;
        switch (yykind)
        {
      case symbol_kind::SYM_comp_type: // comp_type
                    { }
        break;

      case symbol_kind::SYM_equality: // equality
                    { }
        break;

      case symbol_kind::SYM_relational: // relational
                    { }
        break;

      case symbol_kind::SYM_stringop: // stringop
                    { }
        break;

       default:
          break;
        }

        // Value type destructor.
switch (yykind)
    {
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.template destroy< AggrNode* > ();
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.template destroy< AndNode* > ();
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.template destroy< AtomPredNode* > ();
        break;

      case symbol_kind::SYM_constant: // constant
        value.template destroy< ConstantNode* > ();
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.template destroy< DescriptorNode* > ();
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.template destroy< DescriptorOrderingNode* > ();
        break;

      case symbol_kind::SYM_pred: // pred
        value.template destroy< OrNode* > ();
        break;

      case symbol_kind::SYM_path: // path
        value.template destroy< PathNode* > ();
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.template destroy< PostOpNode* > ();
        break;

      case symbol_kind::SYM_prop: // prop
        value.template destroy< PropertyNode* > ();
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.template destroy< TrueOrFalseNode* > ();
        break;

      case symbol_kind::SYM_value: // value
        value.template destroy< ValueNode* > ();
        break;

      case symbol_kind::SYM_direction: // direction
        value.template destroy< bool > ();
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.template destroy< int > ();
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
        value.template destroy< std::string > ();
        break;

      default:
        break;
    }

        Base::clear ();
      }

      /// The user-facing name of this symbol.
      std::string name () const YY_NOEXCEPT
      {
        return parser::symbol_name (this->kind ());
      }

      /// Backward compatibility (Bison 3.6).
      symbol_kind_type type_get () const YY_NOEXCEPT;

      /// Whether empty.
      bool empty () const YY_NOEXCEPT;

      /// Destructive move, \a s is emptied into this.
      void move (basic_symbol& s);

      /// The semantic value.
      semantic_type value;

    private:
#if YY_CPLUSPLUS < 201103L
      /// Assignment operator.
      basic_symbol& operator= (const basic_symbol& that);
#endif
    };

    /// Type access provider for token (enum) based symbols.
    struct by_kind
    {
      /// Default constructor.
      by_kind ();

#if 201103L <= YY_CPLUSPLUS
      /// Move constructor.
      by_kind (by_kind&& that);
#endif

      /// Copy constructor.
      by_kind (const by_kind& that);

      /// The symbol kind as needed by the constructor.
      typedef token_kind_type kind_type;

      /// Constructor from (external) token numbers.
      by_kind (kind_type t);

      /// Record that this symbol is empty.
      void clear ();

      /// Steal the symbol kind from \a that.
      void move (by_kind& that);

      /// The (internal) type number (corresponding to \a type).
      /// \a empty when empty.
      symbol_kind_type kind () const YY_NOEXCEPT;

      /// Backward compatibility (Bison 3.6).
      symbol_kind_type type_get () const YY_NOEXCEPT;

      /// The symbol kind.
      /// \a SYM_YYEMPTY when empty.
      symbol_kind_type kind_;
    };

    /// Backward compatibility for a private implementation detail (Bison 3.6).
    typedef by_kind by_type;

    /// "External" symbols: returned by the scanner.
    struct symbol_type : basic_symbol<by_kind>
    {
      /// Superclass.
      typedef basic_symbol<by_kind> super_type;

      /// Empty symbol.
      symbol_type () {}

      /// Constructor for valueless symbols, and symbols from each type.
#if 201103L <= YY_CPLUSPLUS
      symbol_type (int tok)
        : super_type(token_type (tok))
#else
      symbol_type (int tok)
        : super_type(token_type (tok))
#endif
      {
        YY_ASSERT (tok == token::TOK_END
                   || (token::TOK_YYerror <= tok && tok <= token::TOK_NOT)
                   || (40 <= tok && tok <= 41)
                   || tok == 46
                   || tok == 44);
      }
#if 201103L <= YY_CPLUSPLUS
      symbol_type (int tok, std::string v)
        : super_type(token_type (tok), std::move (v))
#else
      symbol_type (int tok, const std::string& v)
        : super_type(token_type (tok), v)
#endif
      {
        YY_ASSERT ((token::TOK_ID <= tok && tok <= token::TOK_LIKE));
      }
    };

    /// Build a parser object.
    parser (ParserDriver& drv_yyarg);
    virtual ~parser ();

#if 201103L <= YY_CPLUSPLUS
    /// Non copyable.
    parser (const parser&) = delete;
    /// Non copyable.
    parser& operator= (const parser&) = delete;
#endif

    /// Parse.  An alias for parse ().
    /// \returns  0 iff parsing succeeded.
    int operator() ();

    /// Parse.
    /// \returns  0 iff parsing succeeded.
    virtual int parse ();

#if YYDEBUG
    /// The current debugging stream.
    std::ostream& debug_stream () const YY_ATTRIBUTE_PURE;
    /// Set the current debugging stream.
    void set_debug_stream (std::ostream &);

    /// Type for debugging levels.
    typedef int debug_level_type;
    /// The current debugging level.
    debug_level_type debug_level () const YY_ATTRIBUTE_PURE;
    /// Set the current debugging level.
    void set_debug_level (debug_level_type l);
#endif

    /// Report a syntax error.
    /// \param msg    a description of the syntax error.
    virtual void error (const std::string& msg);

    /// Report a syntax error.
    void error (const syntax_error& err);

    /// The user-facing name of the symbol whose (internal) number is
    /// YYSYMBOL.  No bounds checking.
    static std::string symbol_name (symbol_kind_type yysymbol);

    // Implementation of make_symbol for each symbol type.
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_END ()
      {
        return symbol_type (token::TOK_END);
      }
#else
      static
      symbol_type
      make_END ()
      {
        return symbol_type (token::TOK_END);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_YYerror ()
      {
        return symbol_type (token::TOK_YYerror);
      }
#else
      static
      symbol_type
      make_YYerror ()
      {
        return symbol_type (token::TOK_YYerror);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_YYUNDEF ()
      {
        return symbol_type (token::TOK_YYUNDEF);
      }
#else
      static
      symbol_type
      make_YYUNDEF ()
      {
        return symbol_type (token::TOK_YYUNDEF);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_TRUEPREDICATE ()
      {
        return symbol_type (token::TOK_TRUEPREDICATE);
      }
#else
      static
      symbol_type
      make_TRUEPREDICATE ()
      {
        return symbol_type (token::TOK_TRUEPREDICATE);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_FALSEPREDICATE ()
      {
        return symbol_type (token::TOK_FALSEPREDICATE);
      }
#else
      static
      symbol_type
      make_FALSEPREDICATE ()
      {
        return symbol_type (token::TOK_FALSEPREDICATE);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_SORT ()
      {
        return symbol_type (token::TOK_SORT);
      }
#else
      static
      symbol_type
      make_SORT ()
      {
        return symbol_type (token::TOK_SORT);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_DISTINCT ()
      {
        return symbol_type (token::TOK_DISTINCT);
      }
#else
      static
      symbol_type
      make_DISTINCT ()
      {
        return symbol_type (token::TOK_DISTINCT);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_LIMIT ()
      {
        return symbol_type (token::TOK_LIMIT);
      }
#else
      static
      symbol_type
      make_LIMIT ()
      {
        return symbol_type (token::TOK_LIMIT);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_ASCENDING ()
      {
        return symbol_type (token::TOK_ASCENDING);
      }
#else
      static
      symbol_type
      make_ASCENDING ()
      {
        return symbol_type (token::TOK_ASCENDING);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_DESCENDING ()
      {
        return symbol_type (token::TOK_DESCENDING);
      }
#else
      static
      symbol_type
      make_DESCENDING ()
      {
        return symbol_type (token::TOK_DESCENDING);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_TRUE ()
      {
        return symbol_type (token::TOK_TRUE);
      }
#else
      static
      symbol_type
      make_TRUE ()
      {
        return symbol_type (token::TOK_TRUE);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_FALSE ()
      {
        return symbol_type (token::TOK_FALSE);
      }
#else
      static
      symbol_type
      make_FALSE ()
      {
        return symbol_type (token::TOK_FALSE);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_NULL_VAL ()
      {
        return symbol_type (token::TOK_NULL_VAL);
      }
#else
      static
      symbol_type
      make_NULL_VAL ()
      {
        return symbol_type (token::TOK_NULL_VAL);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_EQUAL ()
      {
        return symbol_type (token::TOK_EQUAL);
      }
#else
      static
      symbol_type
      make_EQUAL ()
      {
        return symbol_type (token::TOK_EQUAL);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_NOT_EQUAL ()
      {
        return symbol_type (token::TOK_NOT_EQUAL);
      }
#else
      static
      symbol_type
      make_NOT_EQUAL ()
      {
        return symbol_type (token::TOK_NOT_EQUAL);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_LESS ()
      {
        return symbol_type (token::TOK_LESS);
      }
#else
      static
      symbol_type
      make_LESS ()
      {
        return symbol_type (token::TOK_LESS);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_GREATER ()
      {
        return symbol_type (token::TOK_GREATER);
      }
#else
      static
      symbol_type
      make_GREATER ()
      {
        return symbol_type (token::TOK_GREATER);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_GREATER_EQUAL ()
      {
        return symbol_type (token::TOK_GREATER_EQUAL);
      }
#else
      static
      symbol_type
      make_GREATER_EQUAL ()
      {
        return symbol_type (token::TOK_GREATER_EQUAL);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_LESS_EQUAL ()
      {
        return symbol_type (token::TOK_LESS_EQUAL);
      }
#else
      static
      symbol_type
      make_LESS_EQUAL ()
      {
        return symbol_type (token::TOK_LESS_EQUAL);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_CASE ()
      {
        return symbol_type (token::TOK_CASE);
      }
#else
      static
      symbol_type
      make_CASE ()
      {
        return symbol_type (token::TOK_CASE);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_ANY ()
      {
        return symbol_type (token::TOK_ANY);
      }
#else
      static
      symbol_type
      make_ANY ()
      {
        return symbol_type (token::TOK_ANY);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_ALL ()
      {
        return symbol_type (token::TOK_ALL);
      }
#else
      static
      symbol_type
      make_ALL ()
      {
        return symbol_type (token::TOK_ALL);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_NONE ()
      {
        return symbol_type (token::TOK_NONE);
      }
#else
      static
      symbol_type
      make_NONE ()
      {
        return symbol_type (token::TOK_NONE);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_BACKLINK ()
      {
        return symbol_type (token::TOK_BACKLINK);
      }
#else
      static
      symbol_type
      make_BACKLINK ()
      {
        return symbol_type (token::TOK_BACKLINK);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_SIZE ()
      {
        return symbol_type (token::TOK_SIZE);
      }
#else
      static
      symbol_type
      make_SIZE ()
      {
        return symbol_type (token::TOK_SIZE);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_COUNT ()
      {
        return symbol_type (token::TOK_COUNT);
      }
#else
      static
      symbol_type
      make_COUNT ()
      {
        return symbol_type (token::TOK_COUNT);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_MAX ()
      {
        return symbol_type (token::TOK_MAX);
      }
#else
      static
      symbol_type
      make_MAX ()
      {
        return symbol_type (token::TOK_MAX);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_MIN ()
      {
        return symbol_type (token::TOK_MIN);
      }
#else
      static
      symbol_type
      make_MIN ()
      {
        return symbol_type (token::TOK_MIN);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_SUM ()
      {
        return symbol_type (token::TOK_SUM);
      }
#else
      static
      symbol_type
      make_SUM ()
      {
        return symbol_type (token::TOK_SUM);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_AVG ()
      {
        return symbol_type (token::TOK_AVG);
      }
#else
      static
      symbol_type
      make_AVG ()
      {
        return symbol_type (token::TOK_AVG);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_AND ()
      {
        return symbol_type (token::TOK_AND);
      }
#else
      static
      symbol_type
      make_AND ()
      {
        return symbol_type (token::TOK_AND);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_OR ()
      {
        return symbol_type (token::TOK_OR);
      }
#else
      static
      symbol_type
      make_OR ()
      {
        return symbol_type (token::TOK_OR);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_NOT ()
      {
        return symbol_type (token::TOK_NOT);
      }
#else
      static
      symbol_type
      make_NOT ()
      {
        return symbol_type (token::TOK_NOT);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_ID (std::string v)
      {
        return symbol_type (token::TOK_ID, std::move (v));
      }
#else
      static
      symbol_type
      make_ID (const std::string& v)
      {
        return symbol_type (token::TOK_ID, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_STRING (std::string v)
      {
        return symbol_type (token::TOK_STRING, std::move (v));
      }
#else
      static
      symbol_type
      make_STRING (const std::string& v)
      {
        return symbol_type (token::TOK_STRING, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_BASE64 (std::string v)
      {
        return symbol_type (token::TOK_BASE64, std::move (v));
      }
#else
      static
      symbol_type
      make_BASE64 (const std::string& v)
      {
        return symbol_type (token::TOK_BASE64, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_INFINITY (std::string v)
      {
        return symbol_type (token::TOK_INFINITY, std::move (v));
      }
#else
      static
      symbol_type
      make_INFINITY (const std::string& v)
      {
        return symbol_type (token::TOK_INFINITY, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_NAN (std::string v)
      {
        return symbol_type (token::TOK_NAN, std::move (v));
      }
#else
      static
      symbol_type
      make_NAN (const std::string& v)
      {
        return symbol_type (token::TOK_NAN, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_NATURAL0 (std::string v)
      {
        return symbol_type (token::TOK_NATURAL0, std::move (v));
      }
#else
      static
      symbol_type
      make_NATURAL0 (const std::string& v)
      {
        return symbol_type (token::TOK_NATURAL0, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_NUMBER (std::string v)
      {
        return symbol_type (token::TOK_NUMBER, std::move (v));
      }
#else
      static
      symbol_type
      make_NUMBER (const std::string& v)
      {
        return symbol_type (token::TOK_NUMBER, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_FLOAT (std::string v)
      {
        return symbol_type (token::TOK_FLOAT, std::move (v));
      }
#else
      static
      symbol_type
      make_FLOAT (const std::string& v)
      {
        return symbol_type (token::TOK_FLOAT, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_TIMESTAMP (std::string v)
      {
        return symbol_type (token::TOK_TIMESTAMP, std::move (v));
      }
#else
      static
      symbol_type
      make_TIMESTAMP (const std::string& v)
      {
        return symbol_type (token::TOK_TIMESTAMP, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_UUID (std::string v)
      {
        return symbol_type (token::TOK_UUID, std::move (v));
      }
#else
      static
      symbol_type
      make_UUID (const std::string& v)
      {
        return symbol_type (token::TOK_UUID, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_OID (std::string v)
      {
        return symbol_type (token::TOK_OID, std::move (v));
      }
#else
      static
      symbol_type
      make_OID (const std::string& v)
      {
        return symbol_type (token::TOK_OID, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_ARG (std::string v)
      {
        return symbol_type (token::TOK_ARG, std::move (v));
      }
#else
      static
      symbol_type
      make_ARG (const std::string& v)
      {
        return symbol_type (token::TOK_ARG, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_BEGINSWITH (std::string v)
      {
        return symbol_type (token::TOK_BEGINSWITH, std::move (v));
      }
#else
      static
      symbol_type
      make_BEGINSWITH (const std::string& v)
      {
        return symbol_type (token::TOK_BEGINSWITH, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_ENDSWITH (std::string v)
      {
        return symbol_type (token::TOK_ENDSWITH, std::move (v));
      }
#else
      static
      symbol_type
      make_ENDSWITH (const std::string& v)
      {
        return symbol_type (token::TOK_ENDSWITH, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_CONTAINS (std::string v)
      {
        return symbol_type (token::TOK_CONTAINS, std::move (v));
      }
#else
      static
      symbol_type
      make_CONTAINS (const std::string& v)
      {
        return symbol_type (token::TOK_CONTAINS, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_LIKE (std::string v)
      {
        return symbol_type (token::TOK_LIKE, std::move (v));
      }
#else
      static
      symbol_type
      make_LIKE (const std::string& v)
      {
        return symbol_type (token::TOK_LIKE, v);
      }
#endif


    class context
    {
    public:
      context (const parser& yyparser, const symbol_type& yyla);
      const symbol_type& lookahead () const { return yyla_; }
      symbol_kind_type token () const { return yyla_.kind (); }
      /// Put in YYARG at most YYARGN of the expected tokens, and return the
      /// number of tokens stored in YYARG.  If YYARG is null, return the
      /// number of expected tokens (guaranteed to be less than YYNTOKENS).
      int expected_tokens (symbol_kind_type yyarg[], int yyargn) const;

    private:
      const parser& yyparser_;
      const symbol_type& yyla_;
    };

  private:
#if YY_CPLUSPLUS < 201103L
    /// Non copyable.
    parser (const parser&);
    /// Non copyable.
    parser& operator= (const parser&);
#endif


    /// Stored state numbers (used for stacks).
    typedef signed char state_type;

    /// The arguments of the error message.
    int yy_syntax_error_arguments_ (const context& yyctx,
                                    symbol_kind_type yyarg[], int yyargn) const;

    /// Generate an error message.
    /// \param yyctx     the context in which the error occurred.
    virtual std::string yysyntax_error_ (const context& yyctx) const;
    /// Compute post-reduction state.
    /// \param yystate   the current state
    /// \param yysym     the nonterminal to push on the stack
    static state_type yy_lr_goto_state_ (state_type yystate, int yysym);

    /// Whether the given \c yypact_ value indicates a defaulted state.
    /// \param yyvalue   the value to check
    static bool yy_pact_value_is_default_ (int yyvalue);

    /// Whether the given \c yytable_ value indicates a syntax error.
    /// \param yyvalue   the value to check
    static bool yy_table_value_is_error_ (int yyvalue);

    static const signed char yypact_ninf_;
    static const signed char yytable_ninf_;

    /// Convert a scanner token kind \a t to a symbol kind.
    /// In theory \a t should be a token_kind_type, but character literals
    /// are valid, yet not members of the token_type enum.
    static symbol_kind_type yytranslate_ (int t);

    /// Convert the symbol name \a n to a form suitable for a diagnostic.
    static std::string yytnamerr_ (const char *yystr);

    /// For a symbol, its name in clear.
    static const char* const yytname_[];


    // Tables.
    // YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
    // STATE-NUM.
    static const short yypact_[];

    // YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
    // Performed when YYTABLE does not specify something else to do.  Zero
    // means the default is an error.
    static const signed char yydefact_[];

    // YYPGOTO[NTERM-NUM].
    static const signed char yypgoto_[];

    // YYDEFGOTO[NTERM-NUM].
    static const signed char yydefgoto_[];

    // YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
    // positive, shift that token.  If negative, reduce the rule whose
    // number is the opposite.  If YYTABLE_NINF, syntax error.
    static const signed char yytable_[];

    static const signed char yycheck_[];

    // YYSTOS[STATE-NUM] -- The (internal number of the) accessing
    // symbol of state STATE-NUM.
    static const signed char yystos_[];

    // YYR1[YYN] -- Symbol number of symbol that rule YYN derives.
    static const signed char yyr1_[];

    // YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.
    static const signed char yyr2_[];


#if YYDEBUG
    // YYRLINE[YYN] -- Source line where rule number YYN was defined.
    static const unsigned char yyrline_[];
    /// Report on the debug stream that the rule \a r is going to be reduced.
    virtual void yy_reduce_print_ (int r) const;
    /// Print the state stack on the debug stream.
    virtual void yy_stack_print_ () const;

    /// Debugging level.
    int yydebug_;
    /// Debug stream.
    std::ostream* yycdebug_;

    /// \brief Display a symbol kind, value and location.
    /// \param yyo    The output stream.
    /// \param yysym  The symbol.
    template <typename Base>
    void yy_print_ (std::ostream& yyo, const basic_symbol<Base>& yysym) const;
#endif

    /// \brief Reclaim the memory associated to a symbol.
    /// \param yymsg     Why this token is reclaimed.
    ///                  If null, print nothing.
    /// \param yysym     The symbol.
    template <typename Base>
    void yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const;

  private:
    /// Type access provider for state based symbols.
    struct by_state
    {
      /// Default constructor.
      by_state () YY_NOEXCEPT;

      /// The symbol kind as needed by the constructor.
      typedef state_type kind_type;

      /// Constructor.
      by_state (kind_type s) YY_NOEXCEPT;

      /// Copy constructor.
      by_state (const by_state& that) YY_NOEXCEPT;

      /// Record that this symbol is empty.
      void clear () YY_NOEXCEPT;

      /// Steal the symbol kind from \a that.
      void move (by_state& that);

      /// The symbol kind (corresponding to \a state).
      /// \a symbol_kind::SYM_YYEMPTY when empty.
      symbol_kind_type kind () const YY_NOEXCEPT;

      /// The state number used to denote an empty symbol.
      /// We use the initial state, as it does not have a value.
      enum { empty_state = 0 };

      /// The state.
      /// \a empty when empty.
      state_type state;
    };

    /// "Internal" symbol: element of the stack.
    struct stack_symbol_type : basic_symbol<by_state>
    {
      /// Superclass.
      typedef basic_symbol<by_state> super_type;
      /// Construct an empty symbol.
      stack_symbol_type ();
      /// Move or copy construction.
      stack_symbol_type (YY_RVREF (stack_symbol_type) that);
      /// Steal the contents from \a sym to build this.
      stack_symbol_type (state_type s, YY_MOVE_REF (symbol_type) sym);
#if YY_CPLUSPLUS < 201103L
      /// Assignment, needed by push_back by some old implementations.
      /// Moves the contents of that.
      stack_symbol_type& operator= (stack_symbol_type& that);

      /// Assignment, needed by push_back by other implementations.
      /// Needed by some other old implementations.
      stack_symbol_type& operator= (const stack_symbol_type& that);
#endif
    };

    /// A stack with random access from its top.
    template <typename T, typename S = std::vector<T> >
    class stack
    {
    public:
      // Hide our reversed order.
      typedef typename S::iterator iterator;
      typedef typename S::const_iterator const_iterator;
      typedef typename S::size_type size_type;
      typedef typename std::ptrdiff_t index_type;

      stack (size_type n = 200)
        : seq_ (n)
      {}

#if 201103L <= YY_CPLUSPLUS
      /// Non copyable.
      stack (const stack&) = delete;
      /// Non copyable.
      stack& operator= (const stack&) = delete;
#endif

      /// Random access.
      ///
      /// Index 0 returns the topmost element.
      const T&
      operator[] (index_type i) const
      {
        return seq_[size_type (size () - 1 - i)];
      }

      /// Random access.
      ///
      /// Index 0 returns the topmost element.
      T&
      operator[] (index_type i)
      {
        return seq_[size_type (size () - 1 - i)];
      }

      /// Steal the contents of \a t.
      ///
      /// Close to move-semantics.
      void
      push (YY_MOVE_REF (T) t)
      {
        seq_.push_back (T ());
        operator[] (0).move (t);
      }

      /// Pop elements from the stack.
      void
      pop (std::ptrdiff_t n = 1) YY_NOEXCEPT
      {
        for (; 0 < n; --n)
          seq_.pop_back ();
      }

      /// Pop all elements from the stack.
      void
      clear () YY_NOEXCEPT
      {
        seq_.clear ();
      }

      /// Number of elements on the stack.
      index_type
      size () const YY_NOEXCEPT
      {
        return index_type (seq_.size ());
      }

      /// Iterator on top of the stack (going downwards).
      const_iterator
      begin () const YY_NOEXCEPT
      {
        return seq_.begin ();
      }

      /// Bottom of the stack.
      const_iterator
      end () const YY_NOEXCEPT
      {
        return seq_.end ();
      }

      /// Present a slice of the top of a stack.
      class slice
      {
      public:
        slice (const stack& stack, index_type range)
          : stack_ (stack)
          , range_ (range)
        {}

        const T&
        operator[] (index_type i) const
        {
          return stack_[range_ - i];
        }

      private:
        const stack& stack_;
        index_type range_;
      };

    private:
#if YY_CPLUSPLUS < 201103L
      /// Non copyable.
      stack (const stack&);
      /// Non copyable.
      stack& operator= (const stack&);
#endif
      /// The wrapped container.
      S seq_;
    };


    /// Stack type.
    typedef stack<stack_symbol_type> stack_type;

    /// The stack.
    stack_type yystack_;

    /// Push a new state on the stack.
    /// \param m    a debug message to display
    ///             if null, no trace is output.
    /// \param sym  the symbol
    /// \warning the contents of \a s.value is stolen.
    void yypush_ (const char* m, YY_MOVE_REF (stack_symbol_type) sym);

    /// Push a new look ahead token on the state on the stack.
    /// \param m    a debug message to display
    ///             if null, no trace is output.
    /// \param s    the state
    /// \param sym  the symbol (for its value and location).
    /// \warning the contents of \a sym.value is stolen.
    void yypush_ (const char* m, state_type s, YY_MOVE_REF (symbol_type) sym);

    /// Pop \a n symbols from the stack.
    void yypop_ (int n = 1);

    /// Constants.
    enum
    {
      yylast_ = 204,     ///< Last index in yytable_.
      yynnts_ = 25,  ///< Number of nonterminal symbols.
      yyfinal_ = 34 ///< Termination state number.
    };


    // User arguments.
    ParserDriver& drv;

  };

  inline
  parser::symbol_kind_type
  parser::yytranslate_ (int t)
  {
    // YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to
    // TOKEN-NUM as returned by yylex.
    static
    const signed char
    translate_table[] =
    {
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      49,    50,     2,     2,    52,     2,    51,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48
    };
    // Last valid token kind.
    const int code_max = 303;

    if (t <= 0)
      return symbol_kind::SYM_YYEOF;
    else if (t <= code_max)
      return YY_CAST (symbol_kind_type, translate_table[t]);
    else
      return symbol_kind::SYM_YYUNDEF;
  }

  // basic_symbol.
  template <typename Base>
  parser::basic_symbol<Base>::basic_symbol (const basic_symbol& that)
    : Base (that)
    , value ()
  {
    switch (this->kind ())
    {
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.copy< AggrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.copy< AndNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.copy< AtomPredNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
        value.copy< ConstantNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.copy< DescriptorNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.copy< DescriptorOrderingNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_pred: // pred
        value.copy< OrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.copy< PathNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.copy< PostOpNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
        value.copy< PropertyNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.copy< TrueOrFalseNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_value: // value
        value.copy< ValueNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_direction: // direction
        value.copy< bool > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.copy< int > (YY_MOVE (that.value));
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
        value.copy< std::string > (YY_MOVE (that.value));
        break;

      default:
        break;
    }

  }



  template <typename Base>
  parser::symbol_kind_type
  parser::basic_symbol<Base>::type_get () const YY_NOEXCEPT
  {
    return this->kind ();
  }

  template <typename Base>
  bool
  parser::basic_symbol<Base>::empty () const YY_NOEXCEPT
  {
    return this->kind () == symbol_kind::SYM_YYEMPTY;
  }

  template <typename Base>
  void
  parser::basic_symbol<Base>::move (basic_symbol& s)
  {
    super_type::move (s);
    switch (this->kind ())
    {
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< AggrNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_and_pred: // and_pred
        value.move< AndNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_atom_pred: // atom_pred
        value.move< AtomPredNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_constant: // constant
        value.move< ConstantNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_pred_suffix: // pred_suffix
        value.move< DescriptorOrderingNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_pred: // pred
        value.move< OrNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_prop: // prop
        value.move< PropertyNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_boolexpr: // boolexpr
        value.move< TrueOrFalseNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_value: // value
        value.move< ValueNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_direction: // direction
        value.move< bool > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.move< int > (YY_MOVE (s.value));
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
        value.move< std::string > (YY_MOVE (s.value));
        break;

      default:
        break;
    }

  }

  // by_kind.
  inline
  parser::by_kind::by_kind ()
    : kind_ (symbol_kind::SYM_YYEMPTY)
  {}

#if 201103L <= YY_CPLUSPLUS
  inline
  parser::by_kind::by_kind (by_kind&& that)
    : kind_ (that.kind_)
  {
    that.clear ();
  }
#endif

  inline
  parser::by_kind::by_kind (const by_kind& that)
    : kind_ (that.kind_)
  {}

  inline
  parser::by_kind::by_kind (token_kind_type t)
    : kind_ (yytranslate_ (t))
  {}

  inline
  void
  parser::by_kind::clear ()
  {
    kind_ = symbol_kind::SYM_YYEMPTY;
  }

  inline
  void
  parser::by_kind::move (by_kind& that)
  {
    kind_ = that.kind_;
    that.clear ();
  }

  inline
  parser::symbol_kind_type
  parser::by_kind::kind () const YY_NOEXCEPT
  {
    return kind_;
  }

  inline
  parser::symbol_kind_type
  parser::by_kind::type_get () const YY_NOEXCEPT
  {
    return this->kind ();
  }

} // yy




#endif // !YY_YY_GENERATED_QUERY_BISON_HPP_INCLUDED
