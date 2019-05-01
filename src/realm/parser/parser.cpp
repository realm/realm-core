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

#include "parser.hpp"

#include <iostream>

#include <pegtl.hpp>
#include <pegtl/analyze.hpp>
#include <pegtl/contrib/tracer.hpp>

#include <realm/util/assert.hpp>

#include <realm/parser/parser_utils.hpp>

// String tokens can't be followed by [A-z0-9_].
#define string_token_t(s) seq< TAOCPP_PEGTL_ISTRING(s), not_at< identifier_other > >

using namespace tao::pegtl;

namespace realm {
namespace parser {

// forward declarations
struct expr;
struct string_oper;
struct symbolic_oper;
struct pred;

// strings
struct unicode : list< seq< one< 'u' >, rep< 4, must< xdigit > > >, one< '\\' > > {};
struct escaped_char : one< '"', '\'', '\\', '/', 'b', 'f', 'n', 'r', 't', '0' > {};
struct escaped : sor< escaped_char, unicode > {};
struct unescaped : utf8::range< 0x20, 0x10FFFF > {};
struct chars : if_then_else< one< '\\' >, must< escaped >, unescaped > {};
struct dq_string_content : until< at< one< '"' > >, must< chars > > {};
struct dq_string : seq< one< '"' >, must< dq_string_content >, any > {};

struct sq_string_content : until< at< one< '\'' > >, must< chars > > {};
struct sq_string : seq< one< '\'' >, must< sq_string_content >, any > {};

// base64 encoded data
struct b64_allowed : sor< disable< alnum >, one< '/' >, one< '+' >, one< '=' > > {};
struct b64_content : until< at< one< '"' > >, must< b64_allowed > > {};
struct base64 : seq< TAOCPP_PEGTL_ISTRING("B64\""), must< b64_content >, any > {};

// numbers
struct minus : opt< one< '-' > > {};
struct dot : one< '.' > {};

struct float_num : sor<
    seq< plus< digit >, dot, star< digit > >,
    seq< star< digit >, dot, plus< digit > >
> {};
struct hex_num : seq< one< '0' >, one< 'x', 'X' >, plus< xdigit > > {};
struct int_num : plus< digit > {};

struct number : seq< minus, sor< float_num, hex_num, int_num > > {};

struct timestamp_number : disable< number > {};
struct first_timestamp_number : disable< timestamp_number > {};
// Tseconds:nanoseconds
struct internal_timestamp : seq< one< 'T' >, first_timestamp_number, one< ':' >, timestamp_number > {};
// T2017-09-28@23:12:60:288833
// YYYY-MM-DD@HH:MM:SS:NANOS nanos optional
// or the above with 'T' in place of '@'
struct readable_timestamp : seq< first_timestamp_number, one< '-' >, timestamp_number, one< '-' >,
    timestamp_number, one< '@', 'T' >, timestamp_number, one< ':' >, timestamp_number, one< ':' >,
    timestamp_number, opt< seq< one< ':' >, timestamp_number > > > {};
struct timestamp : sor< internal_timestamp, readable_timestamp > {};

struct true_value : string_token_t("true") {};
struct false_value : string_token_t("false") {};
struct null_value : seq<sor<string_token_t("null"), string_token_t("nil")>> {
};

// following operators must allow proceeding string characters
struct min : TAOCPP_PEGTL_ISTRING(".@min.") {};
struct max : TAOCPP_PEGTL_ISTRING(".@max.") {};
struct sum : TAOCPP_PEGTL_ISTRING(".@sum.") {};
struct avg : TAOCPP_PEGTL_ISTRING(".@avg.") {};
// these operators are normal strings (no proceeding string characters)
struct count : string_token_t(".@count") {};
struct size : string_token_t(".@size") {};
struct backlinks : string_token_t("@links") {
};
struct one_key_path : seq<sor<alpha, one<'_', '$'>>, star<sor<alnum, one<'_', '-', '$'>>>> {
};
struct backlink_path : seq<backlinks, dot, one_key_path, dot, one_key_path> {
};
struct backlink_count : seq<disable<backlinks>, sor<disable<count>, disable<size>>> {
};

struct single_collection_operators : sor< count, size > {};
struct key_collection_operators : sor< min, max, sum, avg > {};

// key paths
struct key_path : list<sor<backlink_path, one_key_path>, one<'.'>> {
};

struct key_path_prefix : disable< key_path > {};
struct key_path_suffix : disable< key_path > {};
struct collection_operator_match
    : sor<seq<key_path_prefix, key_collection_operators, key_path_suffix>,
          seq<opt<key_path_prefix, dot>, backlink_count>, seq<key_path_prefix, single_collection_operators>> {
};

// argument
struct argument_index : plus< digit > {};
struct argument : seq<one<'$'>, argument_index> {
};

// subquery eg: SUBQUERY(items, $x, $x.name CONTAINS 'a' && $x.price > 5).@count
struct subq_prefix : seq<string_token_t("subquery"), star<blank>, one<'('>> {
};
struct subq_suffix : one<')'> {
};
struct sub_path : disable<key_path> {
};
struct sub_var_name : seq<one<'$'>, seq<sor<alpha, one<'_', '$'>>, star<sor<alnum, one<'_', '-', '$'>>>>> {
};
struct sub_result_op : sor<count, size> {
};
struct sub_preamble : seq<subq_prefix, pad<sub_path, blank>, one<','>, pad<sub_var_name, blank>, one<','>> {
};
struct subquery : seq<sub_preamble, pad<pred, blank>, pad<subq_suffix, blank>, sub_result_op> {
};

// list aggregate operations
struct agg_target : seq<key_path> {
};
struct agg_any : seq<sor<string_token_t("any"), string_token_t("some")>, plus<blank>, agg_target,
                     pad<sor<string_oper, symbolic_oper>, blank>, expr> {
};
struct agg_all
    : seq<string_token_t("all"), plus<blank>, agg_target, pad<sor<string_oper, symbolic_oper>, blank>, expr> {
};
struct agg_none
    : seq<string_token_t("none"), plus<blank>, agg_target, pad<sor<string_oper, symbolic_oper>, blank>, expr> {
};
struct agg_shortcut_pred : sor<agg_any, agg_all, agg_none> {
};

// expressions and operators
struct expr : sor<dq_string, sq_string, timestamp, number, argument, true_value, false_value, null_value, base64,
                  collection_operator_match, subquery, key_path> {
};
struct case_insensitive : TAOCPP_PEGTL_ISTRING("[c]") {};

struct eq : seq< sor< two< '=' >, one< '=' > >, star< blank >, opt< case_insensitive > >{};
struct noteq : seq< sor< tao::pegtl::string< '!', '=' >, tao::pegtl::string< '<', '>' > >, star< blank >, opt< case_insensitive > > {};
struct in : seq<string_token_t("in"), star<blank>, opt<case_insensitive>> {
};
struct lteq : sor< tao::pegtl::string< '<', '=' >, tao::pegtl::string< '=', '<' > > {};
struct lt : one< '<' > {};
struct gteq : sor< tao::pegtl::string< '>', '=' >, tao::pegtl::string< '=', '>' > > {};
struct gt : one< '>' > {};
struct contains : string_token_t("contains") {};
struct begins : string_token_t("beginswith") {};
struct ends : string_token_t("endswith") {};
struct like : string_token_t("like") {};
struct between : string_token_t("between") {};

struct sort_prefix : seq< string_token_t("sort"), star< blank >, one< '(' > > {};
struct distinct_prefix : seq< string_token_t("distinct"), star< blank >, one< '(' > > {};
struct ascending : seq< sor< string_token_t("ascending"), string_token_t("asc") > > {};
struct descending : seq< sor< string_token_t("descending"), string_token_t("desc") > > {};
struct descriptor_property : disable< key_path > {};
struct sort_param : seq< star< blank >, descriptor_property, plus< blank >, sor< ascending, descending >, star< blank > > {};
struct sort : seq< sort_prefix, sort_param, star< seq< one< ',' >, sort_param > >, one< ')' > > {};
struct distinct_param : seq< star< blank >, descriptor_property, star< blank > > {};
struct distinct : seq < distinct_prefix, distinct_param, star< seq< one< ',' >, distinct_param > >, one< ')' > > {};
struct limit_param : disable<int_num> {
};
struct limit : seq<string_token_t("limit"), star<blank>, one<'('>, star<blank>, limit_param, star<blank>, one<')'>> {
};
struct predicate_suffix_modifier : sor<sort, distinct, limit> {
};

struct string_oper : seq< sor< contains, begins, ends, like>, star< blank >, opt< case_insensitive > > {};
// "=" is equality and since other operators can start with "=" we must check equal last
struct symbolic_oper : sor< noteq, lteq, lt, gteq, gt, eq, in, between > {};

// predicates
struct comparison_pred : seq< expr, pad< sor< string_oper, symbolic_oper >, blank >, expr > {};

// we need to alias the group tokens because these are also used in other expressions above and we have to match
// the predicate group tokens without also matching () in other expressions.
struct begin_pred_group : one<'('> {
};
struct end_pred_group : one<')'> {
};

struct group_pred : if_must<begin_pred_group, pad<pred, blank>, end_pred_group> {
};
struct true_pred : string_token_t("truepredicate") {};
struct false_pred : string_token_t("falsepredicate") {};

struct not_pre : seq< sor< one< '!' >, string_token_t("not") > > {};
struct atom_pred
    : seq<opt<not_pre>, pad<sor<group_pred, true_pred, false_pred, agg_shortcut_pred, comparison_pred>, blank>,
          star<pad<predicate_suffix_modifier, blank>>> {
};

struct and_op : pad< sor< two< '&' >, string_token_t("and") >, blank > {};
struct or_op : pad< sor< two< '|' >, string_token_t("or") >, blank > {};

struct or_ext : if_must< or_op, pred > {};
struct and_ext : if_must< and_op, pred > {};
struct and_pred : seq< atom_pred, star< and_ext > > {};

struct pred : seq< and_pred, star< or_ext > > {};

// state
struct ParserState
{
    std::vector<Predicate *> group_stack;
    std::vector<std::string> timestamp_input_buffer;
    std::string collection_key_path_prefix, collection_key_path_suffix;
    Expression::KeyPathOp pending_op;
    DescriptorOrderingState ordering_state;
    DescriptorOrderingState::SingleOrderingState temp_ordering;
    std::string subquery_path, subquery_var;
    std::vector<Predicate> subqueries;
    Predicate::ComparisonType pending_comparison_type;

    Predicate *current_group()
    {
        return group_stack.back();
    }

    Predicate *last_predicate()
    {
        Predicate *pred = current_group();
        while (pred->type != Predicate::Type::Comparison && pred->cpnd.sub_predicates.size()) {
            pred = &pred->cpnd.sub_predicates.back();
        }
        return pred;
    }

    void add_predicate_to_current_group(Predicate::Type type)
    {
        current_group()->cpnd.sub_predicates.emplace_back(type, negate_next);
        negate_next = false;

        if (current_group()->cpnd.sub_predicates.size() > 1) {
            if (next_type == Predicate::Type::Or) {
                apply_or();
            }
            else {
                apply_and();
            }
        }
    }

    bool negate_next = false;
    Predicate::Type next_type = Predicate::Type::And;
    Expression* last_expression = nullptr;

    void add_collection_aggregate_expression()
    {
        add_expression(Expression(collection_key_path_prefix, pending_op, collection_key_path_suffix));
        collection_key_path_prefix = "";
        collection_key_path_suffix = "";
        pending_op = Expression::KeyPathOp::None;
    }

    void apply_list_aggregate_operation()
    {
        last_predicate()->cmpr.compare_type = pending_comparison_type;
        pending_comparison_type = Predicate::ComparisonType::Unspecified;
    }

    void add_expression(Expression && exp)
    {
        Predicate *current = last_predicate();
        if (current->type == Predicate::Type::Comparison && current->cmpr.expr[1].type == parser::Expression::Type::None) {
            current->cmpr.expr[1] = std::move(exp);
            last_expression = &(current->cmpr.expr[1]);
        }
        else {
            add_predicate_to_current_group(Predicate::Type::Comparison);
            last_predicate()->cmpr.expr[0] = std::move(exp);
            last_expression = &(last_predicate()->cmpr.expr[0]);
        }
    }

    void add_timestamp_from_buffer()
    {
        add_expression(Expression(std::move(timestamp_input_buffer))); // moving contents clears buffer
    }

    void apply_or()
    {
        Predicate *group = current_group();
        if (group->type == Predicate::Type::Or) {
            return;
        }

        // convert to OR
        group->type = Predicate::Type::Or;
        if (group->cpnd.sub_predicates.size() > 2) {
            // split the current group into an AND group ORed with the last subpredicate
            Predicate new_sub(Predicate::Type::And);
            new_sub.cpnd.sub_predicates = std::move(group->cpnd.sub_predicates);

            group->cpnd.sub_predicates = { new_sub, std::move(new_sub.cpnd.sub_predicates.back()) };
            group->cpnd.sub_predicates[0].cpnd.sub_predicates.pop_back();
        }
    }

    void apply_and()
    {
        if (current_group()->type == Predicate::Type::And) {
            return;
        }

        auto &sub_preds = current_group()->cpnd.sub_predicates;
        auto second_last = sub_preds.end() - 2;
        if (second_last->type == Predicate::Type::And && !second_last->negate) {
            // make a new and group populated with the last two predicates
            second_last->cpnd.sub_predicates.push_back(std::move(sub_preds.back()));
            sub_preds.pop_back();
        }
        else {
            // otherwise combine last two into a new AND group
            Predicate pred(Predicate::Type::And);
            pred.cpnd.sub_predicates.insert(pred.cpnd.sub_predicates.begin(), second_last, sub_preds.end());
            sub_preds.erase(second_last, sub_preds.end());
            sub_preds.emplace_back(std::move(pred));
        }
    }
};

// rules
template< typename Rule >
struct action : nothing< Rule > {};

#ifdef REALM_PARSER_PRINT_TOKENS
    #define DEBUG_PRINT_TOKEN(string) do { std::cout << string << std::endl; } while (0)
#else
    #define DEBUG_PRINT_TOKEN(string) do { static_cast<void>(string); } while (0)
#endif

template<> struct action< and_op >
{
    template< typename Input >
    static void apply(const Input&, ParserState& state)
    {
        DEBUG_PRINT_TOKEN("<and>");
        state.next_type = Predicate::Type::And;
    }
};

template<> struct action< or_op >
{
    template< typename Input >
    static void apply(const Input&, ParserState & state)
    {
        DEBUG_PRINT_TOKEN("<or>");
        state.next_type = Predicate::Type::Or;
    }
};


#define EXPRESSION_ACTION(rule, type)                                                                                \
    template <>                                                                                                      \
    struct action<rule> {                                                                                            \
        template <typename Input>                                                                                    \
        static void apply(const Input& in, ParserState& state)                                                       \
        {                                                                                                            \
            DEBUG_PRINT_TOKEN("expression:" + in.string() + #rule);                                                  \
            state.add_expression(Expression(type, in.string()));                                                     \
        }                                                                                                            \
    };

EXPRESSION_ACTION(dq_string_content, Expression::Type::String)
EXPRESSION_ACTION(sq_string_content, Expression::Type::String)
EXPRESSION_ACTION(key_path, Expression::Type::KeyPath)
EXPRESSION_ACTION(number, Expression::Type::Number)
EXPRESSION_ACTION(true_value, Expression::Type::True)
EXPRESSION_ACTION(false_value, Expression::Type::False)
EXPRESSION_ACTION(null_value, Expression::Type::Null)
EXPRESSION_ACTION(argument_index, Expression::Type::Argument)
EXPRESSION_ACTION(base64, Expression::Type::Base64)

template<> struct action< timestamp >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        state.add_timestamp_from_buffer();
    }
};

template<> struct action< first_timestamp_number >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        // the grammer might attempt to match a timestamp and get part way and fail,
        // so everytime we start again we need to clear the buffer.
        state.timestamp_input_buffer.clear();
        state.timestamp_input_buffer.push_back(in.string());
    }
};

template<> struct action< timestamp_number >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        state.timestamp_input_buffer.push_back(in.string());
    }
};


template<> struct action< descriptor_property >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        DescriptorOrderingState::PropertyState p;
        p.key_path = in.string();
        p.ascending = false; // will be overwritten by the required order specification next
        state.temp_ordering.properties.push_back(p);
    }
};

template<> struct action< ascending >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        REALM_ASSERT_DEBUG(state.temp_ordering.properties.size() > 0);
        state.temp_ordering.properties.back().ascending = true;
    }
};

template<> struct action< descending >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        REALM_ASSERT_DEBUG(state.temp_ordering.properties.size() > 0);
        state.temp_ordering.properties.back().ascending = false;
    }
};

template<> struct action< sort_prefix >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        // This clears the temp buffer when a distinct clause starts. It makes sure there is no temp properties that
        // were added if previous properties started matching but it wasn't actuallly a full distinct/sort match
        state.temp_ordering.properties.clear();
    }
};

template<> struct action< distinct_prefix >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        // This clears the temp buffer when a distinct clause starts. It makes sure there is no temp properties that
        // were added if previous properties started matching but it wasn't actuallly a full distinct/sort match
        state.temp_ordering.properties.clear();
    }
};

template<> struct action< sort >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        state.temp_ordering.type = DescriptorOrderingState::SingleOrderingState::DescriptorType::Sort;
        state.ordering_state.orderings.push_back(state.temp_ordering);
        state.temp_ordering.properties.clear();
    }
};

template<> struct action< distinct >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        state.temp_ordering.type = DescriptorOrderingState::SingleOrderingState::DescriptorType::Distinct;
        state.ordering_state.orderings.push_back(state.temp_ordering);
        state.temp_ordering.properties.clear();
    }
};

template <>
struct action<limit_param> {
    template <typename Input>
    static void apply(const Input& in, ParserState& state)
    {
        DEBUG_PRINT_TOKEN(in.string() + " LIMIT PARAM");
        try {
            DescriptorOrderingState::SingleOrderingState limit_state;
            limit_state.type = DescriptorOrderingState::SingleOrderingState::DescriptorType::Limit;
            limit_state.limit = realm::util::stot<size_t>(in.string());
            state.ordering_state.orderings.push_back(limit_state);
        }
        catch (...) {
            const static std::string message =
                "Invalid Predicate. 'LIMIT' accepts a positive integer parameter eg: 'LIMIT(10)'";
            throw tao::pegtl::parse_error(message, in);
        }
    }
};

template <>
struct action<sub_path> {
    template <typename Input>
    static void apply(const Input& in, ParserState& state)
    {
        DEBUG_PRINT_TOKEN(in.string() + " SUB PATH");
        state.subquery_path = in.string();
    }
};

template <>
struct action<sub_var_name> {
    template <typename Input>
    static void apply(const Input& in, ParserState& state)
    {
        DEBUG_PRINT_TOKEN(in.string() + " SUB VAR NAME");
        state.subquery_var = in.string();
    }
};

// the preamble acts as the opening for a sub predicate group which is the subquery conditions
template <>
struct action<sub_preamble> {
    template <typename Input>
    static void apply(const Input& in, ParserState& state)
    {
        DEBUG_PRINT_TOKEN(in.string() + "<BEGIN SUBQUERY CONDITIONS>");

        Expression exp(Expression::Type::SubQuery);
        exp.subquery_path = state.subquery_path;
        exp.subquery_var = state.subquery_var;
        exp.subquery = std::make_shared<Predicate>(Predicate::Type::And);
        REALM_ASSERT_DEBUG(!state.subquery_var.empty() && !state.subquery_path.empty());
        Predicate* sub_pred = exp.subquery.get();
        state.add_expression(std::move(exp));
        state.group_stack.push_back(sub_pred);
    }
};


// once the whole subquery syntax is matched, we close the subquery group and add the expression
template <>
struct action<subquery> {
    template <typename Input>
    static void apply(const Input& in, ParserState& state)
    {
        DEBUG_PRINT_TOKEN(in.string() + "<END SUBQUERY CONDITIONS>");
        state.group_stack.pop_back();
    }
};

#define COLLECTION_OPERATION_ACTION(rule, type)                                                                      \
    template <>                                                                                                      \
    struct action<rule> {                                                                                            \
        template <typename Input>                                                                                    \
        static void apply(const Input& in, ParserState& state)                                                       \
        {                                                                                                            \
            DEBUG_PRINT_TOKEN("operation: " + in.string());                                                          \
            state.pending_op = type;                                                                                 \
        }                                                                                                            \
    };


COLLECTION_OPERATION_ACTION(min, Expression::KeyPathOp::Min)
COLLECTION_OPERATION_ACTION(max, Expression::KeyPathOp::Max)
COLLECTION_OPERATION_ACTION(sum, Expression::KeyPathOp::Sum)
COLLECTION_OPERATION_ACTION(avg, Expression::KeyPathOp::Avg)
COLLECTION_OPERATION_ACTION(count, Expression::KeyPathOp::Count)
COLLECTION_OPERATION_ACTION(size, Expression::KeyPathOp::SizeString)
COLLECTION_OPERATION_ACTION(backlink_count, Expression::KeyPathOp::BacklinkCount)

template<> struct action< key_path_prefix > {
    template< typename Input >
    static void apply(const Input& in, ParserState& state) {
        DEBUG_PRINT_TOKEN("key_path_prefix: " + in.string());
        state.collection_key_path_prefix = in.string();
    }
};

template<> struct action< key_path_suffix > {
    template< typename Input >
    static void apply(const Input& in, ParserState& state) {
        DEBUG_PRINT_TOKEN("key_path_suffix: " + in.string());
        state.collection_key_path_suffix = in.string();
    }
};

template<> struct action< collection_operator_match > {
    template< typename Input >
    static void apply(const Input& in, ParserState& state) {
        DEBUG_PRINT_TOKEN(in.string());
        state.add_collection_aggregate_expression();
    }
};

#define LIST_AGG_OP_TYPE_ACTION(rule, type)                                                                          \
    template <>                                                                                                      \
    struct action<rule> {                                                                                            \
        template <typename Input>                                                                                    \
        static void apply(const Input& in, ParserState& state)                                                       \
        {                                                                                                            \
            DEBUG_PRINT_TOKEN(in.string() + #rule);                                                                  \
            state.pending_comparison_type = type;                                                                    \
        }                                                                                                            \
    };

LIST_AGG_OP_TYPE_ACTION(agg_any, Predicate::ComparisonType::Any)
LIST_AGG_OP_TYPE_ACTION(agg_all, Predicate::ComparisonType::All)
LIST_AGG_OP_TYPE_ACTION(agg_none, Predicate::ComparisonType::None)

template <>
struct action<agg_shortcut_pred> {
    template <typename Input>
    static void apply(const Input& in, ParserState& state)
    {
        DEBUG_PRINT_TOKEN(in.string() + " Aggregate shortcut matched");
        state.apply_list_aggregate_operation();
    }
};

template<> struct action< true_pred >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        state.add_predicate_to_current_group(Predicate::Type::True);
    }
};

template<> struct action< false_pred >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        state.add_predicate_to_current_group(Predicate::Type::False);
    }
};

#define OPERATOR_ACTION(rule, oper)                                                                                  \
    template <>                                                                                                      \
    struct action<rule> {                                                                                            \
        template <typename Input>                                                                                    \
        static void apply(const Input& in, ParserState& state)                                                       \
        {                                                                                                            \
            DEBUG_PRINT_TOKEN(in.string() + #oper);                                                                  \
            state.last_predicate()->cmpr.op = oper;                                                                  \
        }                                                                                                            \
    };

OPERATOR_ACTION(eq, Predicate::Operator::Equal)
OPERATOR_ACTION(noteq, Predicate::Operator::NotEqual)
OPERATOR_ACTION(in, Predicate::Operator::In)
OPERATOR_ACTION(gteq, Predicate::Operator::GreaterThanOrEqual)
OPERATOR_ACTION(gt, Predicate::Operator::GreaterThan)
OPERATOR_ACTION(lteq, Predicate::Operator::LessThanOrEqual)
OPERATOR_ACTION(lt, Predicate::Operator::LessThan)
OPERATOR_ACTION(begins, Predicate::Operator::BeginsWith)
OPERATOR_ACTION(ends, Predicate::Operator::EndsWith)
OPERATOR_ACTION(contains, Predicate::Operator::Contains)
OPERATOR_ACTION(like, Predicate::Operator::Like)

template<> struct action< between >
{
    template< typename Input >
    static void apply(const Input& in, ParserState&)
    {
        const static std::string message = "Invalid Predicate. The 'between' operator is not supported yet, please rewrite the expression using '>' and '<'.";
        throw tao::pegtl::parse_error(message, in);
    }
};

template<> struct action< case_insensitive >
{
    template< typename Input >
    static void apply(const Input& in, ParserState & state)
    {
        DEBUG_PRINT_TOKEN(in.string());
        state.last_predicate()->cmpr.option = Predicate::OperatorOption::CaseInsensitive;
    }
};

template <>
struct action<begin_pred_group> {
    template< typename Input >
    static void apply(const Input&, ParserState & state)
    {
        DEBUG_PRINT_TOKEN("<begin_group>");
        state.add_predicate_to_current_group(Predicate::Type::And);
        state.group_stack.push_back(state.last_predicate());
    }
};

template<> struct action< group_pred >
{
    template< typename Input >
    static void apply(const Input&, ParserState & state)
    {
        DEBUG_PRINT_TOKEN("<end_group>");
        state.group_stack.pop_back();
    }
};

template<> struct action< not_pre >
{
    template< typename Input >
    static void apply(const Input&, ParserState & state)
    {
        DEBUG_PRINT_TOKEN("<not>");
        state.negate_next = true;
    }
};

template< typename Rule >
struct error_message_control : tao::pegtl::normal< Rule >
{
    static const std::string error_message;

    template< typename Input, typename ... States >
    static void raise(const Input& in, States&&...)
    {
        throw tao::pegtl::parse_error(error_message, in);
    }
};

template<>
const std::string error_message_control< chars >::error_message = "Invalid characters in string constant.";

template< typename Rule>
const std::string error_message_control< Rule >::error_message = "Invalid predicate.";

ParserResult parse(const char* query)
{
    StringData sd(query); // assumes c-style null termination
    return parse(sd);
}

ParserResult parse(const std::string& query)
{
    StringData sd(query);
    return parse(sd);
}

ParserResult parse(const StringData& query)
{
    DEBUG_PRINT_TOKEN(query);

    Predicate out_predicate(Predicate::Type::And);

    ParserState state;
    state.group_stack.push_back(&out_predicate);

    // pegtl::memory_input does not make a copy, it operates on pointers to the contiguous data
    tao::pegtl::memory_input<> input(query.data(), query.size(), query);
    tao::pegtl::parse< must< pred, eof >, action, error_message_control >(input, state);
    if (out_predicate.type == Predicate::Type::And && out_predicate.cpnd.sub_predicates.size() == 1) {
        return ParserResult{ std::move(out_predicate.cpnd.sub_predicates.back()), state.ordering_state };
    }

    return ParserResult{std::move(out_predicate), state.ordering_state};
}

size_t analyze_grammar()
{
    return analyze<pred>();
}

}}
