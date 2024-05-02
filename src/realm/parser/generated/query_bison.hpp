// A Bison parser, made by GNU Bison 3.8.2.

// Skeleton interface for Bison LALR(1) parsers in C++

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

  #include <memory>
  #include <string>
  #include <realm/mixed.hpp>
  #include <realm/geospatial.hpp>
  #include <array>
  #include <optional>
  using realm::GeoPoint;
  namespace realm::query_parser {
    class ParserDriver;
    class ConstantNode;
    class GeospatialNode;
    class ListNode;
    class PostOpNode;
    class AggrNode;
    class ExpressionNode;
    class ValueNode;
    class OperationNode;
    class TrueOrFalseNode;
    class OrNode;
    class AndNode;
    class QueryNode;
    class PathNode;
    class DescriptorOrderingNode;
    class DescriptorNode;
    class PropertyNode;
    class SubqueryNode;

    enum class CompareType: char;
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
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
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
#ifdef YYSTYPE
# ifdef __GNUC__
#  pragma GCC message "bison: do not #define YYSTYPE in C++, use %define api.value.type"
# endif
    typedef YYSTYPE value_type;
#else
  /// A buffer to store and retrieve objects.
  ///
  /// Sort of a variant, but does not keep track of the nature
  /// of the stored data, since that knowledge is available
  /// via the current parser state.
  class value_type
  {
  public:
    /// Type of *this.
    typedef value_type self_type;

    /// Empty construction.
    value_type () YY_NOEXCEPT
      : yyraw_ ()
      , yytypeid_ (YY_NULLPTR)
    {}

    /// Construct and fill.
    template <typename T>
    value_type (YY_RVREF (T) t)
      : yytypeid_ (&typeid (T))
    {
      YY_ASSERT (sizeof (T) <= size);
      new (yyas_<T> ()) T (YY_MOVE (t));
    }

#if 201103L <= YY_CPLUSPLUS
    /// Non copyable.
    value_type (const self_type&) = delete;
    /// Non copyable.
    self_type& operator= (const self_type&) = delete;
#endif

    /// Destruction, allowed only if empty.
    ~value_type () YY_NOEXCEPT
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
    value_type (const self_type&);
    /// Non copyable.
    self_type& operator= (const self_type&);
#endif

    /// Accessor to raw memory as \a T.
    template <typename T>
    T*
    yyas_ () YY_NOEXCEPT
    {
      void *yyp = yyraw_;
      return static_cast<T*> (yyp);
     }

    /// Const accessor to raw memory as \a T.
    template <typename T>
    const T*
    yyas_ () const YY_NOEXCEPT
    {
      const void *yyp = yyraw_;
      return static_cast<const T*> (yyp);
     }

    /// An auxiliary type to compute the largest semantic type.
    union union_type
    {
      // aggregate
      char dummy1[sizeof (AggrNode*)];

      // equality
      // relational
      // stringop
      char dummy2[sizeof (CompareType)];

      // constant
      // primary_key
      char dummy3[sizeof (ConstantNode*)];

      // distinct
      // distinct_param
      // sort
      // sort_param
      // limit
      char dummy4[sizeof (DescriptorNode*)];

      // post_query
      char dummy5[sizeof (DescriptorOrderingNode*)];

      // expr
      char dummy6[sizeof (ExpressionNode*)];

      // geoloop_content
      // geoloop
      // geopoly_content
      // geospatial
      char dummy7[sizeof (GeospatialNode*)];

      // list
      // list_content
      char dummy8[sizeof (ListNode*)];

      // path
      char dummy9[sizeof (PathNode*)];

      // post_op
      char dummy10[sizeof (PostOpNode*)];

      // prop
      // simple_prop
      char dummy11[sizeof (PropertyNode*)];

      // query
      // compare
      char dummy12[sizeof (QueryNode*)];

      // subquery
      char dummy13[sizeof (SubqueryNode*)];

      // boolexpr
      char dummy14[sizeof (TrueOrFalseNode*)];

      // value
      char dummy15[sizeof (ValueNode*)];

      // direction
      char dummy16[sizeof (bool)];

      // coordinate
      char dummy17[sizeof (double)];

      // comp_type
      // aggr_op
      char dummy18[sizeof (int)];

      // geopoint
      char dummy19[sizeof (std::optional<GeoPoint>)];

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
      // "link"
      // "typed link"
      // "argument"
      // "keypath"
      // "beginswith"
      // "endswith"
      // "contains"
      // "fulltext"
      // "like"
      // "between"
      // "in"
      // "geowithin"
      // "obj"
      // "sort"
      // "distinct"
      // "limit"
      // "binary"
      // "ascending"
      // "descending"
      // "FIRST"
      // "LAST"
      // "SIZE"
      // "@size"
      // "@type"
      // "key or value"
      // "@links"
      // id
      char dummy20[sizeof (std::string)];
    };

    /// The size of the largest semantic type.
    enum { size = sizeof (union_type) };

    /// A buffer to store semantic values.
    union
    {
      /// Strongest alignment constraints.
      long double yyalign_me_;
      /// A buffer large enough to store any of the semantic values.
      char yyraw_[size];
    };

    /// Whether the content is built: if defined, the name of the stored type.
    const std::type_info *yytypeid_;
  };

#endif
    /// Backward compatibility (Bison 3.8).
    typedef value_type semantic_type;


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
    TOK_SUBQUERY = 260,            // "subquery"
    TOK_TRUE = 261,                // "true"
    TOK_FALSE = 262,               // "false"
    TOK_NULL_VAL = 263,            // "null"
    TOK_EQUAL = 264,               // "=="
    TOK_NOT_EQUAL = 265,           // "!="
    TOK_LESS = 266,                // "<"
    TOK_GREATER = 267,             // ">"
    TOK_GREATER_EQUAL = 268,       // ">="
    TOK_LESS_EQUAL = 269,          // "<="
    TOK_CASE = 270,                // "[c]"
    TOK_ANY = 271,                 // "any"
    TOK_ALL = 272,                 // "all"
    TOK_NONE = 273,                // "none"
    TOK_MAX = 274,                 // "@max"
    TOK_MIN = 275,                 // "@min"
    TOK_SUM = 276,                 // "@sum"
    TOK_AVG = 277,                 // "@average"
    TOK_AND = 278,                 // "&&"
    TOK_OR = 279,                  // "||"
    TOK_NOT = 280,                 // "!"
    TOK_GEOBOX = 281,              // "geobox"
    TOK_GEOPOLYGON = 282,          // "geopolygon"
    TOK_GEOCIRCLE = 283,           // "geocircle"
    TOK_ID = 284,                  // "identifier"
    TOK_STRING = 285,              // "string"
    TOK_BASE64 = 286,              // "base64"
    TOK_INFINITY = 287,            // "infinity"
    TOK_NAN = 288,                 // "NaN"
    TOK_NATURAL0 = 289,            // "natural0"
    TOK_NUMBER = 290,              // "number"
    TOK_FLOAT = 291,               // "float"
    TOK_TIMESTAMP = 292,           // "date"
    TOK_UUID = 293,                // "UUID"
    TOK_OID = 294,                 // "ObjectId"
    TOK_LINK = 295,                // "link"
    TOK_TYPED_LINK = 296,          // "typed link"
    TOK_ARG = 297,                 // "argument"
    TOK_KP_ARG = 298,              // "keypath"
    TOK_BEGINSWITH = 299,          // "beginswith"
    TOK_ENDSWITH = 300,            // "endswith"
    TOK_CONTAINS = 301,            // "contains"
    TOK_TEXT = 302,                // "fulltext"
    TOK_LIKE = 303,                // "like"
    TOK_BETWEEN = 304,             // "between"
    TOK_IN = 305,                  // "in"
    TOK_GEOWITHIN = 306,           // "geowithin"
    TOK_OBJ = 307,                 // "obj"
    TOK_SORT = 308,                // "sort"
    TOK_DISTINCT = 309,            // "distinct"
    TOK_LIMIT = 310,               // "limit"
    TOK_BINARY = 311,              // "binary"
    TOK_ASCENDING = 312,           // "ascending"
    TOK_DESCENDING = 313,          // "descending"
    TOK_INDEX_FIRST = 314,         // "FIRST"
    TOK_INDEX_LAST = 315,          // "LAST"
    TOK_INDEX_SIZE = 316,          // "SIZE"
    TOK_SIZE = 317,                // "@size"
    TOK_TYPE = 318,                // "@type"
    TOK_KEY_VAL = 319,             // "key or value"
    TOK_BACKLINK = 320             // "@links"
      };
      /// Backward compatibility alias (Bison 3.6).
      typedef token_kind_type yytokentype;
    };

    /// Token kind, as returned by yylex.
    typedef token::token_kind_type token_kind_type;

    /// Backward compatibility alias (Bison 3.6).
    typedef token_kind_type token_type;

    /// Symbol kinds.
    struct symbol_kind
    {
      enum symbol_kind_type
      {
        YYNTOKENS = 78, ///< Number of tokens.
        SYM_YYEMPTY = -2,
        SYM_YYEOF = 0,                           // "end of file"
        SYM_YYerror = 1,                         // error
        SYM_YYUNDEF = 2,                         // "invalid token"
        SYM_TRUEPREDICATE = 3,                   // "truepredicate"
        SYM_FALSEPREDICATE = 4,                  // "falsepredicate"
        SYM_SUBQUERY = 5,                        // "subquery"
        SYM_TRUE = 6,                            // "true"
        SYM_FALSE = 7,                           // "false"
        SYM_NULL_VAL = 8,                        // "null"
        SYM_EQUAL = 9,                           // "=="
        SYM_NOT_EQUAL = 10,                      // "!="
        SYM_LESS = 11,                           // "<"
        SYM_GREATER = 12,                        // ">"
        SYM_GREATER_EQUAL = 13,                  // ">="
        SYM_LESS_EQUAL = 14,                     // "<="
        SYM_CASE = 15,                           // "[c]"
        SYM_ANY = 16,                            // "any"
        SYM_ALL = 17,                            // "all"
        SYM_NONE = 18,                           // "none"
        SYM_MAX = 19,                            // "@max"
        SYM_MIN = 20,                            // "@min"
        SYM_SUM = 21,                            // "@sum"
        SYM_AVG = 22,                            // "@average"
        SYM_AND = 23,                            // "&&"
        SYM_OR = 24,                             // "||"
        SYM_NOT = 25,                            // "!"
        SYM_GEOBOX = 26,                         // "geobox"
        SYM_GEOPOLYGON = 27,                     // "geopolygon"
        SYM_GEOCIRCLE = 28,                      // "geocircle"
        SYM_ID = 29,                             // "identifier"
        SYM_STRING = 30,                         // "string"
        SYM_BASE64 = 31,                         // "base64"
        SYM_INFINITY = 32,                       // "infinity"
        SYM_NAN = 33,                            // "NaN"
        SYM_NATURAL0 = 34,                       // "natural0"
        SYM_NUMBER = 35,                         // "number"
        SYM_FLOAT = 36,                          // "float"
        SYM_TIMESTAMP = 37,                      // "date"
        SYM_UUID = 38,                           // "UUID"
        SYM_OID = 39,                            // "ObjectId"
        SYM_LINK = 40,                           // "link"
        SYM_TYPED_LINK = 41,                     // "typed link"
        SYM_ARG = 42,                            // "argument"
        SYM_KP_ARG = 43,                         // "keypath"
        SYM_BEGINSWITH = 44,                     // "beginswith"
        SYM_ENDSWITH = 45,                       // "endswith"
        SYM_CONTAINS = 46,                       // "contains"
        SYM_TEXT = 47,                           // "fulltext"
        SYM_LIKE = 48,                           // "like"
        SYM_BETWEEN = 49,                        // "between"
        SYM_IN = 50,                             // "in"
        SYM_GEOWITHIN = 51,                      // "geowithin"
        SYM_OBJ = 52,                            // "obj"
        SYM_SORT = 53,                           // "sort"
        SYM_DISTINCT = 54,                       // "distinct"
        SYM_LIMIT = 55,                          // "limit"
        SYM_BINARY = 56,                         // "binary"
        SYM_ASCENDING = 57,                      // "ascending"
        SYM_DESCENDING = 58,                     // "descending"
        SYM_INDEX_FIRST = 59,                    // "FIRST"
        SYM_INDEX_LAST = 60,                     // "LAST"
        SYM_INDEX_SIZE = 61,                     // "SIZE"
        SYM_SIZE = 62,                           // "@size"
        SYM_TYPE = 63,                           // "@type"
        SYM_KEY_VAL = 64,                        // "key or value"
        SYM_BACKLINK = 65,                       // "@links"
        SYM_66_ = 66,                            // '+'
        SYM_67_ = 67,                            // '-'
        SYM_68_ = 68,                            // '*'
        SYM_69_ = 69,                            // '/'
        SYM_70_ = 70,                            // '('
        SYM_71_ = 71,                            // ')'
        SYM_72_ = 72,                            // '.'
        SYM_73_ = 73,                            // ','
        SYM_74_ = 74,                            // '['
        SYM_75_ = 75,                            // ']'
        SYM_76_ = 76,                            // '{'
        SYM_77_ = 77,                            // '}'
        SYM_YYACCEPT = 78,                       // $accept
        SYM_final = 79,                          // final
        SYM_query = 80,                          // query
        SYM_compare = 81,                        // compare
        SYM_expr = 82,                           // expr
        SYM_value = 83,                          // value
        SYM_prop = 84,                           // prop
        SYM_aggregate = 85,                      // aggregate
        SYM_simple_prop = 86,                    // simple_prop
        SYM_subquery = 87,                       // subquery
        SYM_coordinate = 88,                     // coordinate
        SYM_geopoint = 89,                       // geopoint
        SYM_geoloop_content = 90,                // geoloop_content
        SYM_geoloop = 91,                        // geoloop
        SYM_geopoly_content = 92,                // geopoly_content
        SYM_geospatial = 93,                     // geospatial
        SYM_post_query = 94,                     // post_query
        SYM_distinct = 95,                       // distinct
        SYM_distinct_param = 96,                 // distinct_param
        SYM_sort = 97,                           // sort
        SYM_sort_param = 98,                     // sort_param
        SYM_limit = 99,                          // limit
        SYM_direction = 100,                     // direction
        SYM_list = 101,                          // list
        SYM_list_content = 102,                  // list_content
        SYM_constant = 103,                      // constant
        SYM_primary_key = 104,                   // primary_key
        SYM_boolexpr = 105,                      // boolexpr
        SYM_comp_type = 106,                     // comp_type
        SYM_post_op = 107,                       // post_op
        SYM_aggr_op = 108,                       // aggr_op
        SYM_equality = 109,                      // equality
        SYM_relational = 110,                    // relational
        SYM_stringop = 111,                      // stringop
        SYM_path = 112,                          // path
        SYM_id = 113                             // id
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
      basic_symbol () YY_NOEXCEPT
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
      case symbol_kind::SYM_aggregate: // aggregate
        value.move< AggrNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.move< CompareType > (std::move (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.move< ConstantNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.move< DescriptorOrderingNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_expr: // expr
        value.move< ExpressionNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_geoloop_content: // geoloop_content
      case symbol_kind::SYM_geoloop: // geoloop
      case symbol_kind::SYM_geopoly_content: // geopoly_content
      case symbol_kind::SYM_geospatial: // geospatial
        value.move< GeospatialNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.move< ListNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.move< PropertyNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.move< QueryNode* > (std::move (that.value));
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.move< SubqueryNode* > (std::move (that.value));
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

      case symbol_kind::SYM_coordinate: // coordinate
        value.move< double > (std::move (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< int > (std::move (that.value));
        break;

      case symbol_kind::SYM_geopoint: // geopoint
        value.move< std::optional<GeoPoint> > (std::move (that.value));
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
      case symbol_kind::SYM_KP_ARG: // "keypath"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_GEOWITHIN: // "geowithin"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_BINARY: // "binary"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_INDEX_FIRST: // "FIRST"
      case symbol_kind::SYM_INDEX_LAST: // "LAST"
      case symbol_kind::SYM_INDEX_SIZE: // "SIZE"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_BACKLINK: // "@links"
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
      basic_symbol (typename Base::kind_type t, CompareType&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const CompareType& v)
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
      basic_symbol (typename Base::kind_type t, ExpressionNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const ExpressionNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, GeospatialNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const GeospatialNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, ListNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const ListNode*& v)
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
      basic_symbol (typename Base::kind_type t, QueryNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const QueryNode*& v)
        : Base (t)
        , value (v)
      {}
#endif

#if 201103L <= YY_CPLUSPLUS
      basic_symbol (typename Base::kind_type t, SubqueryNode*&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const SubqueryNode*& v)
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
      basic_symbol (typename Base::kind_type t, double&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const double& v)
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
      basic_symbol (typename Base::kind_type t, std::optional<GeoPoint>&& v)
        : Base (t)
        , value (std::move (v))
      {}
#else
      basic_symbol (typename Base::kind_type t, const std::optional<GeoPoint>& v)
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
      void clear () YY_NOEXCEPT
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

      case symbol_kind::SYM_aggr_op: // aggr_op
                    { }
        break;

       default:
          break;
        }

        // Value type destructor.
switch (yykind)
    {
      case symbol_kind::SYM_aggregate: // aggregate
        value.template destroy< AggrNode* > ();
        break;

      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.template destroy< CompareType > ();
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.template destroy< ConstantNode* > ();
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.template destroy< DescriptorNode* > ();
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.template destroy< DescriptorOrderingNode* > ();
        break;

      case symbol_kind::SYM_expr: // expr
        value.template destroy< ExpressionNode* > ();
        break;

      case symbol_kind::SYM_geoloop_content: // geoloop_content
      case symbol_kind::SYM_geoloop: // geoloop
      case symbol_kind::SYM_geopoly_content: // geopoly_content
      case symbol_kind::SYM_geospatial: // geospatial
        value.template destroy< GeospatialNode* > ();
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.template destroy< ListNode* > ();
        break;

      case symbol_kind::SYM_path: // path
        value.template destroy< PathNode* > ();
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.template destroy< PostOpNode* > ();
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.template destroy< PropertyNode* > ();
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.template destroy< QueryNode* > ();
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.template destroy< SubqueryNode* > ();
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

      case symbol_kind::SYM_coordinate: // coordinate
        value.template destroy< double > ();
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.template destroy< int > ();
        break;

      case symbol_kind::SYM_geopoint: // geopoint
        value.template destroy< std::optional<GeoPoint> > ();
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
      case symbol_kind::SYM_KP_ARG: // "keypath"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_GEOWITHIN: // "geowithin"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_BINARY: // "binary"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_INDEX_FIRST: // "FIRST"
      case symbol_kind::SYM_INDEX_LAST: // "LAST"
      case symbol_kind::SYM_INDEX_SIZE: // "SIZE"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_BACKLINK: // "@links"
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
      value_type value;

    private:
#if YY_CPLUSPLUS < 201103L
      /// Assignment operator.
      basic_symbol& operator= (const basic_symbol& that);
#endif
    };

    /// Type access provider for token (enum) based symbols.
    struct by_kind
    {
      /// The symbol kind as needed by the constructor.
      typedef token_kind_type kind_type;

      /// Default constructor.
      by_kind () YY_NOEXCEPT;

#if 201103L <= YY_CPLUSPLUS
      /// Move constructor.
      by_kind (by_kind&& that) YY_NOEXCEPT;
#endif

      /// Copy constructor.
      by_kind (const by_kind& that) YY_NOEXCEPT;

      /// Constructor from (external) token numbers.
      by_kind (kind_type t) YY_NOEXCEPT;



      /// Record that this symbol is empty.
      void clear () YY_NOEXCEPT;

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
      symbol_type () YY_NOEXCEPT {}

      /// Constructor for valueless symbols, and symbols from each type.
#if 201103L <= YY_CPLUSPLUS
      symbol_type (int tok)
        : super_type (token_kind_type (tok))
#else
      symbol_type (int tok)
        : super_type (token_kind_type (tok))
#endif
      {
#if !defined _MSC_VER || defined __clang__
        YY_ASSERT (tok == token::TOK_END
                   || (token::TOK_YYerror <= tok && tok <= token::TOK_GEOCIRCLE)
                   || tok == 43
                   || tok == 45
                   || tok == 42
                   || tok == 47
                   || (40 <= tok && tok <= 41)
                   || tok == 46
                   || tok == 44
                   || tok == 91
                   || tok == 93
                   || tok == 123
                   || tok == 125);
#endif
      }
#if 201103L <= YY_CPLUSPLUS
      symbol_type (int tok, std::string v)
        : super_type (token_kind_type (tok), std::move (v))
#else
      symbol_type (int tok, const std::string& v)
        : super_type (token_kind_type (tok), v)
#endif
      {
#if !defined _MSC_VER || defined __clang__
        YY_ASSERT ((token::TOK_ID <= tok && tok <= token::TOK_BACKLINK));
#endif
      }
    };

    /// Build a parser object.
    parser (ParserDriver& drv_yyarg, void* scanner_yyarg);
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

    // Implementation of make_symbol for each token kind.
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
      make_SUBQUERY ()
      {
        return symbol_type (token::TOK_SUBQUERY);
      }
#else
      static
      symbol_type
      make_SUBQUERY ()
      {
        return symbol_type (token::TOK_SUBQUERY);
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
      make_GEOBOX ()
      {
        return symbol_type (token::TOK_GEOBOX);
      }
#else
      static
      symbol_type
      make_GEOBOX ()
      {
        return symbol_type (token::TOK_GEOBOX);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_GEOPOLYGON ()
      {
        return symbol_type (token::TOK_GEOPOLYGON);
      }
#else
      static
      symbol_type
      make_GEOPOLYGON ()
      {
        return symbol_type (token::TOK_GEOPOLYGON);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_GEOCIRCLE ()
      {
        return symbol_type (token::TOK_GEOCIRCLE);
      }
#else
      static
      symbol_type
      make_GEOCIRCLE ()
      {
        return symbol_type (token::TOK_GEOCIRCLE);
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
      make_LINK (std::string v)
      {
        return symbol_type (token::TOK_LINK, std::move (v));
      }
#else
      static
      symbol_type
      make_LINK (const std::string& v)
      {
        return symbol_type (token::TOK_LINK, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_TYPED_LINK (std::string v)
      {
        return symbol_type (token::TOK_TYPED_LINK, std::move (v));
      }
#else
      static
      symbol_type
      make_TYPED_LINK (const std::string& v)
      {
        return symbol_type (token::TOK_TYPED_LINK, v);
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
      make_KP_ARG (std::string v)
      {
        return symbol_type (token::TOK_KP_ARG, std::move (v));
      }
#else
      static
      symbol_type
      make_KP_ARG (const std::string& v)
      {
        return symbol_type (token::TOK_KP_ARG, v);
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
      make_TEXT (std::string v)
      {
        return symbol_type (token::TOK_TEXT, std::move (v));
      }
#else
      static
      symbol_type
      make_TEXT (const std::string& v)
      {
        return symbol_type (token::TOK_TEXT, v);
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
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_BETWEEN (std::string v)
      {
        return symbol_type (token::TOK_BETWEEN, std::move (v));
      }
#else
      static
      symbol_type
      make_BETWEEN (const std::string& v)
      {
        return symbol_type (token::TOK_BETWEEN, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_IN (std::string v)
      {
        return symbol_type (token::TOK_IN, std::move (v));
      }
#else
      static
      symbol_type
      make_IN (const std::string& v)
      {
        return symbol_type (token::TOK_IN, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_GEOWITHIN (std::string v)
      {
        return symbol_type (token::TOK_GEOWITHIN, std::move (v));
      }
#else
      static
      symbol_type
      make_GEOWITHIN (const std::string& v)
      {
        return symbol_type (token::TOK_GEOWITHIN, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_OBJ (std::string v)
      {
        return symbol_type (token::TOK_OBJ, std::move (v));
      }
#else
      static
      symbol_type
      make_OBJ (const std::string& v)
      {
        return symbol_type (token::TOK_OBJ, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_SORT (std::string v)
      {
        return symbol_type (token::TOK_SORT, std::move (v));
      }
#else
      static
      symbol_type
      make_SORT (const std::string& v)
      {
        return symbol_type (token::TOK_SORT, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_DISTINCT (std::string v)
      {
        return symbol_type (token::TOK_DISTINCT, std::move (v));
      }
#else
      static
      symbol_type
      make_DISTINCT (const std::string& v)
      {
        return symbol_type (token::TOK_DISTINCT, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_LIMIT (std::string v)
      {
        return symbol_type (token::TOK_LIMIT, std::move (v));
      }
#else
      static
      symbol_type
      make_LIMIT (const std::string& v)
      {
        return symbol_type (token::TOK_LIMIT, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_BINARY (std::string v)
      {
        return symbol_type (token::TOK_BINARY, std::move (v));
      }
#else
      static
      symbol_type
      make_BINARY (const std::string& v)
      {
        return symbol_type (token::TOK_BINARY, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_ASCENDING (std::string v)
      {
        return symbol_type (token::TOK_ASCENDING, std::move (v));
      }
#else
      static
      symbol_type
      make_ASCENDING (const std::string& v)
      {
        return symbol_type (token::TOK_ASCENDING, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_DESCENDING (std::string v)
      {
        return symbol_type (token::TOK_DESCENDING, std::move (v));
      }
#else
      static
      symbol_type
      make_DESCENDING (const std::string& v)
      {
        return symbol_type (token::TOK_DESCENDING, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_INDEX_FIRST (std::string v)
      {
        return symbol_type (token::TOK_INDEX_FIRST, std::move (v));
      }
#else
      static
      symbol_type
      make_INDEX_FIRST (const std::string& v)
      {
        return symbol_type (token::TOK_INDEX_FIRST, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_INDEX_LAST (std::string v)
      {
        return symbol_type (token::TOK_INDEX_LAST, std::move (v));
      }
#else
      static
      symbol_type
      make_INDEX_LAST (const std::string& v)
      {
        return symbol_type (token::TOK_INDEX_LAST, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_INDEX_SIZE (std::string v)
      {
        return symbol_type (token::TOK_INDEX_SIZE, std::move (v));
      }
#else
      static
      symbol_type
      make_INDEX_SIZE (const std::string& v)
      {
        return symbol_type (token::TOK_INDEX_SIZE, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_SIZE (std::string v)
      {
        return symbol_type (token::TOK_SIZE, std::move (v));
      }
#else
      static
      symbol_type
      make_SIZE (const std::string& v)
      {
        return symbol_type (token::TOK_SIZE, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_TYPE (std::string v)
      {
        return symbol_type (token::TOK_TYPE, std::move (v));
      }
#else
      static
      symbol_type
      make_TYPE (const std::string& v)
      {
        return symbol_type (token::TOK_TYPE, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_KEY_VAL (std::string v)
      {
        return symbol_type (token::TOK_KEY_VAL, std::move (v));
      }
#else
      static
      symbol_type
      make_KEY_VAL (const std::string& v)
      {
        return symbol_type (token::TOK_KEY_VAL, v);
      }
#endif
#if 201103L <= YY_CPLUSPLUS
      static
      symbol_type
      make_BACKLINK (std::string v)
      {
        return symbol_type (token::TOK_BACKLINK, std::move (v));
      }
#else
      static
      symbol_type
      make_BACKLINK (const std::string& v)
      {
        return symbol_type (token::TOK_BACKLINK, v);
      }
#endif


    class context
    {
    public:
      context (const parser& yyparser, const symbol_type& yyla);
      const symbol_type& lookahead () const YY_NOEXCEPT { return yyla_; }
      symbol_kind_type token () const YY_NOEXCEPT { return yyla_.kind (); }
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
    typedef unsigned char state_type;

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
    static bool yy_pact_value_is_default_ (int yyvalue) YY_NOEXCEPT;

    /// Whether the given \c yytable_ value indicates a syntax error.
    /// \param yyvalue   the value to check
    static bool yy_table_value_is_error_ (int yyvalue) YY_NOEXCEPT;

    static const short yypact_ninf_;
    static const signed char yytable_ninf_;

    /// Convert a scanner token kind \a t to a symbol kind.
    /// In theory \a t should be a token_kind_type, but character literals
    /// are valid, yet not members of the token_kind_type enum.
    static symbol_kind_type yytranslate_ (int t) YY_NOEXCEPT;

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
    static const unsigned char yydefact_[];

    // YYPGOTO[NTERM-NUM].
    static const short yypgoto_[];

    // YYDEFGOTO[NTERM-NUM].
    static const unsigned char yydefgoto_[];

    // YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
    // positive, shift that token.  If negative, reduce the rule whose
    // number is the opposite.  If YYTABLE_NINF, syntax error.
    static const unsigned char yytable_[];

    static const short yycheck_[];

    // YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
    // state STATE-NUM.
    static const signed char yystos_[];

    // YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.
    static const signed char yyr1_[];

    // YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.
    static const signed char yyr2_[];


#if YYDEBUG
    // YYRLINE[YYN] -- Source line where rule number YYN was defined.
    static const short yyrline_[];
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

      stack (size_type n = 200) YY_NOEXCEPT
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
        slice (const stack& stack, index_type range) YY_NOEXCEPT
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
    void yypop_ (int n = 1) YY_NOEXCEPT;

    /// Constants.
    enum
    {
      yylast_ = 633,     ///< Last index in yytable_.
      yynnts_ = 36,  ///< Number of nonterminal symbols.
      yyfinal_ = 72 ///< Termination state number.
    };


    // User arguments.
    ParserDriver& drv;
    void* scanner;

  };

  inline
  parser::symbol_kind_type
  parser::yytranslate_ (int t) YY_NOEXCEPT
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
      70,    71,    68,    66,    73,    67,    72,    69,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    74,     2,    75,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    76,     2,    77,     2,     2,     2,     2,
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
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65
    };
    // Last valid token kind.
    const int code_max = 320;

    if (t <= 0)
      return symbol_kind::SYM_YYEOF;
    else if (t <= code_max)
      return static_cast <symbol_kind_type> (translate_table[t]);
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
      case symbol_kind::SYM_aggregate: // aggregate
        value.copy< AggrNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.copy< CompareType > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.copy< ConstantNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.copy< DescriptorNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.copy< DescriptorOrderingNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_expr: // expr
        value.copy< ExpressionNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_geoloop_content: // geoloop_content
      case symbol_kind::SYM_geoloop: // geoloop
      case symbol_kind::SYM_geopoly_content: // geopoly_content
      case symbol_kind::SYM_geospatial: // geospatial
        value.copy< GeospatialNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.copy< ListNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_path: // path
        value.copy< PathNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.copy< PostOpNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.copy< PropertyNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.copy< QueryNode* > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.copy< SubqueryNode* > (YY_MOVE (that.value));
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

      case symbol_kind::SYM_coordinate: // coordinate
        value.copy< double > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.copy< int > (YY_MOVE (that.value));
        break;

      case symbol_kind::SYM_geopoint: // geopoint
        value.copy< std::optional<GeoPoint> > (YY_MOVE (that.value));
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
      case symbol_kind::SYM_KP_ARG: // "keypath"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_GEOWITHIN: // "geowithin"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_BINARY: // "binary"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_INDEX_FIRST: // "FIRST"
      case symbol_kind::SYM_INDEX_LAST: // "LAST"
      case symbol_kind::SYM_INDEX_SIZE: // "SIZE"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_BACKLINK: // "@links"
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
      case symbol_kind::SYM_aggregate: // aggregate
        value.move< AggrNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_equality: // equality
      case symbol_kind::SYM_relational: // relational
      case symbol_kind::SYM_stringop: // stringop
        value.move< CompareType > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_constant: // constant
      case symbol_kind::SYM_primary_key: // primary_key
        value.move< ConstantNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_distinct: // distinct
      case symbol_kind::SYM_distinct_param: // distinct_param
      case symbol_kind::SYM_sort: // sort
      case symbol_kind::SYM_sort_param: // sort_param
      case symbol_kind::SYM_limit: // limit
        value.move< DescriptorNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_post_query: // post_query
        value.move< DescriptorOrderingNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_expr: // expr
        value.move< ExpressionNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_geoloop_content: // geoloop_content
      case symbol_kind::SYM_geoloop: // geoloop
      case symbol_kind::SYM_geopoly_content: // geopoly_content
      case symbol_kind::SYM_geospatial: // geospatial
        value.move< GeospatialNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_list: // list
      case symbol_kind::SYM_list_content: // list_content
        value.move< ListNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_path: // path
        value.move< PathNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_post_op: // post_op
        value.move< PostOpNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_prop: // prop
      case symbol_kind::SYM_simple_prop: // simple_prop
        value.move< PropertyNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_query: // query
      case symbol_kind::SYM_compare: // compare
        value.move< QueryNode* > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_subquery: // subquery
        value.move< SubqueryNode* > (YY_MOVE (s.value));
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

      case symbol_kind::SYM_coordinate: // coordinate
        value.move< double > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_comp_type: // comp_type
      case symbol_kind::SYM_aggr_op: // aggr_op
        value.move< int > (YY_MOVE (s.value));
        break;

      case symbol_kind::SYM_geopoint: // geopoint
        value.move< std::optional<GeoPoint> > (YY_MOVE (s.value));
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
      case symbol_kind::SYM_KP_ARG: // "keypath"
      case symbol_kind::SYM_BEGINSWITH: // "beginswith"
      case symbol_kind::SYM_ENDSWITH: // "endswith"
      case symbol_kind::SYM_CONTAINS: // "contains"
      case symbol_kind::SYM_TEXT: // "fulltext"
      case symbol_kind::SYM_LIKE: // "like"
      case symbol_kind::SYM_BETWEEN: // "between"
      case symbol_kind::SYM_IN: // "in"
      case symbol_kind::SYM_GEOWITHIN: // "geowithin"
      case symbol_kind::SYM_OBJ: // "obj"
      case symbol_kind::SYM_SORT: // "sort"
      case symbol_kind::SYM_DISTINCT: // "distinct"
      case symbol_kind::SYM_LIMIT: // "limit"
      case symbol_kind::SYM_BINARY: // "binary"
      case symbol_kind::SYM_ASCENDING: // "ascending"
      case symbol_kind::SYM_DESCENDING: // "descending"
      case symbol_kind::SYM_INDEX_FIRST: // "FIRST"
      case symbol_kind::SYM_INDEX_LAST: // "LAST"
      case symbol_kind::SYM_INDEX_SIZE: // "SIZE"
      case symbol_kind::SYM_SIZE: // "@size"
      case symbol_kind::SYM_TYPE: // "@type"
      case symbol_kind::SYM_KEY_VAL: // "key or value"
      case symbol_kind::SYM_BACKLINK: // "@links"
      case symbol_kind::SYM_id: // id
        value.move< std::string > (YY_MOVE (s.value));
        break;

      default:
        break;
    }

  }

  // by_kind.
  inline
  parser::by_kind::by_kind () YY_NOEXCEPT
    : kind_ (symbol_kind::SYM_YYEMPTY)
  {}

#if 201103L <= YY_CPLUSPLUS
  inline
  parser::by_kind::by_kind (by_kind&& that) YY_NOEXCEPT
    : kind_ (that.kind_)
  {
    that.clear ();
  }
#endif

  inline
  parser::by_kind::by_kind (const by_kind& that) YY_NOEXCEPT
    : kind_ (that.kind_)
  {}

  inline
  parser::by_kind::by_kind (token_kind_type t) YY_NOEXCEPT
    : kind_ (yytranslate_ (t))
  {}



  inline
  void
  parser::by_kind::clear () YY_NOEXCEPT
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
