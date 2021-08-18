%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.7.4"
%defines
%no-lines

%define api.token.constructor
%define api.value.type variant
%define parse.assert

%code requires {
  # include <memory>
  # include <string>
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
    class PropNode;
    class SubqueryNode;
  }
  using namespace realm::query_parser;
}

// The parsing context.
%parse-param { ParserDriver& drv }
%param { void* scanner }

// %locations

%define parse.trace
%define parse.error verbose

%code {
#include <realm/parser/driver.hpp>
#include <realm/table.hpp>
using namespace realm;
using namespace realm::query_parser;

#ifdef _MSC_VER
// ignore msvc warnings in this file (poped at end)
// do this by setting the warning level to 1 (permissive)
#pragma warning( push, 1 )
#endif
}
%define api.symbol.prefix {SYM_}
%define api.token.prefix {TOK_}
%token
  END  0  "end of file"
  TRUEPREDICATE "truepredicate"
  FALSEPREDICATE "falsepredicate"
  SORT "sort"
  DISTINCT "distinct"
  LIMIT "limit"
  ASCENDING "ascending"
  DESCENDING "descending"
  SUBQUERY "subquery"
  TRUE    "true"
  FALSE   "false"
  NULL_VAL "null"
  EQUAL   "=="
  NOT_EQUAL   "!="
  IN      "IN"
  LESS    "<"
  GREATER ">"
  GREATER_EQUAL ">="
  LESS_EQUAL    "<="
  CASE     "[c]"
  ANY     "any"
  ALL     "all"
  NONE    "none"
  BACKLINK "@links"
  MAX     "@max"
  MIN     "@min"
  SUM     "@sun"
  AVG     "@average"
  AND     "&&"
  OR      "||"
  NOT     "!"
;

%token <std::string> ID "identifier"
%token <std::string> STRING "string"
%token <std::string> BASE64 "base64"
%token <std::string> INFINITY "infinity"
%token <std::string> NAN "NaN"
%token <std::string> NATURAL0 "natural0"
%token <std::string> NUMBER "number"
%token <std::string> FLOAT "float"
%token <std::string> TIMESTAMP "date"
%token <std::string> UUID "UUID"
%token <std::string> OID "ObjectId"
%token <std::string> LINK "link"
%token <std::string> TYPED_LINK "typed link"
%token <std::string> ARG "argument"
%token <std::string> BEGINSWITH "beginswith"
%token <std::string> ENDSWITH "endswith"
%token <std::string> CONTAINS "contains"
%token <std::string> LIKE    "like"
%token <std::string> BETWEEN "between"
%token <std::string> SIZE "@size"
%token <std::string> TYPE "@type"
%token <std::string> KEY_VAL "key or value"
%type  <bool> direction
%type  <int> equality relational stringop
%type  <std::unique_ptr<ConstantNode>> constant
%type  <std::unique_ptr<PropertyNode>> prop
%type  <std::unique_ptr<PostOpNode>> post_op
%type  <std::unique_ptr<AggrNode>> aggr_op
%type  <std::unique_ptr<ValueNode>> value
%type  <std::unique_ptr<TrueOrFalseNode>> boolexpr
%type  <int> comp_type
%type  <std::unique_ptr<OrNode>> pred
%type  <std::unique_ptr<AtomPredNode>> atom_pred
%type  <std::unique_ptr<AndNode>> and_pred
%type  <std::unique_ptr<PathNode>> path
%type  <std::unique_ptr<DescriptorOrderingNode>> pred_suffix
%type  <std::unique_ptr<DescriptorNode>> sort sort_param distinct distinct_param limit
%type  <std::unique_ptr<SubqueryNode>> subquery
%type  <std::string> path_elem id
%type  <std::unique_ptr<PropNode>> simple_prop

%destructor { } <int>

%printer { yyo << $$; } <*>;
%printer { yyo << "<>"; } <>;

%%
%start query;

%left AND;
%left OR;
%right NOT;

query
    : pred pred_suffix { drv.result = std::move($1); drv.ordering = std::move($2); };

pred
    : and_pred                  { $$ = std::make_unique<OrNode>(std::move($1)); }
    | pred "||" and_pred        { $1->atom_preds.emplace_back(std::move($3)); $$ = std::move($1); }

and_pred
    : atom_pred                 { $$ = std::make_unique<AndNode>(std::move($1)); }
    | and_pred "&&" atom_pred   { $1->atom_preds.emplace_back(std::move($3)); $$ = std::move($1); }

atom_pred
    : value equality value      { $$ = std::make_unique<EqualityNode>(std::move($1),$2,std::move($3)); }
    | value equality CASE value {
                                    auto tmp = std::make_unique<EqualityNode>(std::move($1),$2,std::move($4));
                                    tmp->case_sensitive = false;
                                    $$ = std::move(tmp);
                                }
    | value relational value    { $$ = std::make_unique<RelationalNode>(std::move($1),$2,std::move($3)); }
    | value stringop value      { $$ = std::make_unique<StringOpsNode>(std::move($1),$2,std::move($3)); }
    | value stringop CASE value {
                                    auto tmp = std::make_unique<StringOpsNode>(std::move($1),$2,std::move($4));
                                    tmp->case_sensitive = false;
                                    $$ = std::move(tmp);
                                }
    | value BETWEEN list        {
                                    error("The 'between' operator is not supported yet, please rewrite the expression using '>' and '<'.");
                                    YYERROR;
                                }
    | NOT atom_pred             { $$ = std::make_unique<NotNode>(std::move($2)); }
    | '(' pred ')'              { $$ = std::make_unique<ParensNode>(std::move($2)); }
    | boolexpr                  { $$ = std::move($1); }

value
    : constant                  { $$ = std::make_unique<ValueNode>(std::move($1));}
    | prop                      { $$ = std::make_unique<ValueNode>(std::move($1));}

prop
    : path id post_op           { $$ = std::make_unique<PropNode>(std::move($1),$2,std::move($3)); }
    | path id '[' constant ']' post_op { $$ = std::make_unique<PropNode>(std::move($1),$2,std::move($4), std::move($6)); }
    | comp_type path id post_op { $$ = std::make_unique<PropNode>(std::move($2), $3, std::move($4),ExpressionComparisonType($1));}
    | path BACKLINK post_op     { $$ = std::make_unique<PropNode>(std::move($1), "@links" , std::move($3)); }
    | path id '.' aggr_op '.'  id   { $$ = std::make_unique<LinkAggrNode>(std::move($1), $2, std::move($4), $6); }
    | path id '.' aggr_op       { $$ = std::make_unique<ListAggrNode>(std::move($1), $2, std::move($4)); }
    | subquery                  { $$ = std::move($1); }

simple_prop
    : path id                   { $$ = std::make_unique<PropNode>(std::move($1),$2); }

subquery
    : SUBQUERY '(' simple_prop ',' id ',' pred ')' '.' SIZE   { $$ = std::make_unique<SubqueryNode>(std::move($3), $5, std::move($7)); }

pred_suffix
    : %empty                    { $$ = std::make_unique<DescriptorOrderingNode>();}
    | pred_suffix sort          { $1->add_descriptor(std::move($2)); $$ = std::move($1); }
    | pred_suffix distinct      { $1->add_descriptor(std::move($2)); $$ = std::move($1); }
    | pred_suffix limit         { $1->add_descriptor(std::move($2)); $$ = std::move($1); }

distinct: DISTINCT '(' distinct_param ')' { $$ = std::move($3); }

distinct_param
    : path id                   { $$ = std::make_unique<DescriptorNode>(DescriptorNode::DISTINCT); $$->add($1->path_elems, $2);}
    | distinct_param ',' path id { $1->add($3->path_elems, $4); $$ = std::move($1); }

sort: SORT '(' sort_param ')'    { $$ = std::move($3); }

sort_param
    : path id direction         { $$ = std::make_unique<DescriptorNode>(DescriptorNode::SORT); $$->add($1->path_elems, $2, $3);}
    | sort_param ',' path id direction  { $1->add($3->path_elems, $4, $5); $$ = std::move($1); }

limit: LIMIT '(' NATURAL0 ')'   { $$ = std::make_unique<DescriptorNode>(DescriptorNode::LIMIT, $3); }

direction
    : ASCENDING                 { $$ = true; }
    | DESCENDING                { $$ = false; }

list : '{' list_content '}'

list_content
    : constant
    | list_content ',' constant

constant
    : NATURAL0                  { $$ = std::make_unique<ConstantNode>(ConstantNode::NUMBER, $1); }
    | NUMBER                    { $$ = std::make_unique<ConstantNode>(ConstantNode::NUMBER, $1); }
    | INFINITY                  { $$ = std::make_unique<ConstantNode>(ConstantNode::INFINITY_VAL, $1); }
    | NAN                       { $$ = std::make_unique<ConstantNode>(ConstantNode::NAN_VAL, $1); }
    | STRING                    { $$ = std::make_unique<ConstantNode>(ConstantNode::STRING, $1); }
    | BASE64                    { $$ = std::make_unique<ConstantNode>(ConstantNode::BASE64, $1); }
    | FLOAT                     { $$ = std::make_unique<ConstantNode>(ConstantNode::FLOAT, $1); }
    | TIMESTAMP                 { $$ = std::make_unique<ConstantNode>(ConstantNode::TIMESTAMP, $1); }
    | UUID                      { $$ = std::make_unique<ConstantNode>(ConstantNode::UUID_T, $1); }
    | OID                       { $$ = std::make_unique<ConstantNode>(ConstantNode::OID, $1); }
    | LINK                      { $$ = std::make_unique<ConstantNode>(ConstantNode::LINK, $1); }
    | TYPED_LINK                { $$ = std::make_unique<ConstantNode>(ConstantNode::TYPED_LINK, $1); }
    | TRUE                      { $$ = std::make_unique<ConstantNode>(ConstantNode::TRUE, ""); }
    | FALSE                     { $$ = std::make_unique<ConstantNode>(ConstantNode::FALSE, ""); }
    | NULL_VAL                  { $$ = std::make_unique<ConstantNode>(ConstantNode::NULL_VAL, ""); }
    | ARG                       { $$ = std::make_unique<ConstantNode>(ConstantNode::ARG, $1); }

boolexpr
    : "truepredicate"           { $$ = std::make_unique<TrueOrFalseNode>(true); }
    | "falsepredicate"          { $$ = std::make_unique<TrueOrFalseNode>(false); }

comp_type
    : ANY                       { $$ = int(ExpressionComparisonType::Any); }
    | ALL                       { $$ = int(ExpressionComparisonType::All); }
    | NONE                      { $$ = int(ExpressionComparisonType::None); }

post_op
    : %empty                    { $$ = nullptr; }
    | '.' SIZE                  { $$ = std::make_unique<PostOpNode>($2, PostOpNode::SIZE);}
    | '.' TYPE                  { $$ = std::make_unique<PostOpNode>($2, PostOpNode::TYPE);}

aggr_op
    : MAX                       { $$ = std::make_unique<AggrNode>(AggrNode::MAX);}
    | MIN                       { $$ = std::make_unique<AggrNode>(AggrNode::MIN);}
    | SUM                       { $$ = std::make_unique<AggrNode>(AggrNode::SUM);}
    | AVG                       { $$ = std::make_unique<AggrNode>(AggrNode::AVG);}

equality
    : EQUAL                     { $$ = CompareNode::EQUAL; }
    | NOT_EQUAL                 { $$ = CompareNode::NOT_EQUAL; }
    | IN                        { $$ = CompareNode::IN; }

relational
    : LESS                      { $$ = CompareNode::LESS; }
    | LESS_EQUAL                { $$ = CompareNode::LESS_EQUAL; }
    | GREATER                   { $$ = CompareNode::GREATER; }
    | GREATER_EQUAL             { $$ = CompareNode::GREATER_EQUAL; }

stringop
    : BEGINSWITH                { $$ = CompareNode::BEGINSWITH; }
    | ENDSWITH                  { $$ = CompareNode::ENDSWITH; }
    | CONTAINS                  { $$ = CompareNode::CONTAINS; }
    | LIKE                      { $$ = CompareNode::LIKE; }

path
    : %empty                    { $$ = std::make_unique<PathNode>(); }
    | path path_elem            { $1->add_element(std::move($2)); $$ = std::move($1); }

path_elem
    : id '.'                    { $$ = $1; }

id  
    : ID                        { $$ = $1; }
    | BACKLINK '.' ID '.' ID    { $$ = std::string("@links.") + $3 + "." + $5; }
    | BEGINSWITH                { $$ = $1; }
    | ENDSWITH                  { $$ = $1; }
    | CONTAINS                  { $$ = $1; }
    | LIKE                      { $$ = $1; }
    | BETWEEN                   { $$ = $1; }
    | KEY_VAL                   { $$ = $1; }
%%

void
yy::parser::error (const std::string& m)
{
    drv.error(m);
}

#ifdef _MSC_VER
#pragma warning( pop ) // restore normal warning levels
#endif
