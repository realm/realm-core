%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.7.4"
%defines
%no-lines

%define api.token.constructor
%define api.value.type variant
%define parse.assert

%code requires {
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
%type  <ConstantNode*> constant
%type  <PropertyNode*> prop
%type  <PostOpNode*> post_op
%type  <AggrNode*> aggr_op
%type  <ValueNode*> value
%type  <TrueOrFalseNode*> boolexpr
%type  <int> comp_type
%type  <OrNode*> pred
%type  <AtomPredNode*> atom_pred
%type  <AndNode*> and_pred
%type  <PathNode*> path
%type  <DescriptorOrderingNode*> pred_suffix
%type  <DescriptorNode*> sort sort_param distinct distinct_param limit
%type  <SubqueryNode*> subquery
%type  <std::string> path_elem id
%type  <PropNode*> simple_prop

%destructor { } <int>

%printer { yyo << $$; } <*>;
%printer { yyo << "<>"; } <>;

%%
%start query;

%left AND;
%left OR;
%right NOT;

query
    : pred pred_suffix { drv.result = $1; drv.ordering = $2; };

pred
    : and_pred                  { $$ = drv.m_parse_nodes.create<OrNode>($1); }
    | pred "||" and_pred        { $1->and_preds.emplace_back($3); $$ = $1; }

and_pred
    : atom_pred                 { $$ = drv.m_parse_nodes.create<AndNode>($1); }
    | and_pred "&&" atom_pred   { $1->atom_preds.emplace_back($3); $$ = $1; }

atom_pred
    : value equality value      { $$ = drv.m_parse_nodes.create<EqualityNode>($1, $2, $3); }
    | value equality CASE value {
                                    auto tmp = drv.m_parse_nodes.create<EqualityNode>($1, $2, $4);
                                    tmp->case_sensitive = false;
                                    $$ = tmp;
                                }
    | value relational value    { $$ = drv.m_parse_nodes.create<RelationalNode>($1, $2, $3); }
    | value stringop value      { $$ = drv.m_parse_nodes.create<StringOpsNode>($1, $2, $3); }
    | value stringop CASE value {
                                    auto tmp = drv.m_parse_nodes.create<StringOpsNode>($1, $2, $4);
                                    tmp->case_sensitive = false;
                                    $$ = tmp;
                                }
    | value BETWEEN list        {
                                    error("The 'between' operator is not supported yet, please rewrite the expression using '>' and '<'.");
                                    YYERROR;
                                }
    | NOT atom_pred             { $$ = drv.m_parse_nodes.create<NotNode>($2); }
    | '(' pred ')'              { $$ = drv.m_parse_nodes.create<ParensNode>($2); }
    | boolexpr                  { $$ =$1; }

value
    : constant                  { $$ = drv.m_parse_nodes.create<ValueNode>($1);}
    | prop                      { $$ = drv.m_parse_nodes.create<ValueNode>($1);}

prop
    : path id post_op           { $$ = drv.m_parse_nodes.create<PropNode>($1, $2, $3); }
    | path id '[' constant ']' post_op { $$ = drv.m_parse_nodes.create<PropNode>($1, $2, $4, $6); }
    | comp_type path id post_op { $$ = drv.m_parse_nodes.create<PropNode>($2, $3, $4, ExpressionComparisonType($1)); }
    | path BACKLINK post_op     { $$ = drv.m_parse_nodes.create<PropNode>($1, "@links", $3); }
    | path id '.' aggr_op '.'  id   { $$ = drv.m_parse_nodes.create<LinkAggrNode>($1, $2, $4, $6); }
    | path id '.' aggr_op       { $$ = drv.m_parse_nodes.create<ListAggrNode>($1, $2, $4); }
    | subquery                  { $$ = $1; }

simple_prop
    : path id                   { $$ = drv.m_parse_nodes.create<PropNode>($1, $2); }

subquery
    : SUBQUERY '(' simple_prop ',' id ',' pred ')' '.' SIZE   { $$ = drv.m_parse_nodes.create<SubqueryNode>($3, $5, $7); }

pred_suffix
    : %empty                    { $$ = drv.m_parse_nodes.create<DescriptorOrderingNode>();}
    | pred_suffix sort          { $1->add_descriptor($2); $$ = $1; }
    | pred_suffix distinct      { $1->add_descriptor($2); $$ = $1; }
    | pred_suffix limit         { $1->add_descriptor($2); $$ = $1; }

distinct: DISTINCT '(' distinct_param ')' { $$ = $3; }

distinct_param
    : path id                   { $$ = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::DISTINCT); $$->add($1->path_elems, $2);}
    | distinct_param ',' path id { $1->add($3->path_elems, $4); $$ = $1; }

sort: SORT '(' sort_param ')'    { $$ = $3; }

sort_param
    : path id direction         { $$ = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::SORT); $$->add($1->path_elems, $2, $3);}
    | sort_param ',' path id direction  { $1->add($3->path_elems, $4, $5); $$ = $1; }

limit: LIMIT '(' NATURAL0 ')'   { $$ = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::LIMIT, $3); }

direction
    : ASCENDING                 { $$ = true; }
    | DESCENDING                { $$ = false; }

list : '{' list_content '}'

list_content
    : constant
    | list_content ',' constant

constant
    : NATURAL0                  { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, $1); }
    | NUMBER                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, $1); }
    | INFINITY                  { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::INFINITY_VAL, $1); }
    | NAN                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NAN_VAL, $1); }
    | STRING                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::STRING, $1); }
    | BASE64                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::BASE64, $1); }
    | FLOAT                     { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FLOAT, $1); }
    | TIMESTAMP                 { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TIMESTAMP, $1); }
    | UUID                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::UUID_T, $1); }
    | OID                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::OID, $1); }
    | LINK                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::LINK, $1); }
    | TYPED_LINK                { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TYPED_LINK, $1); }
    | TRUE                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TRUE, ""); }
    | FALSE                     { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FALSE, ""); }
    | NULL_VAL                  { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NULL_VAL, ""); }
    | ARG                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::ARG, $1); }

boolexpr
    : "truepredicate"           { $$ = drv.m_parse_nodes.create<TrueOrFalseNode>(true); }
    | "falsepredicate"          { $$ = drv.m_parse_nodes.create<TrueOrFalseNode>(false); }

comp_type
    : ANY                       { $$ = int(ExpressionComparisonType::Any); }
    | ALL                       { $$ = int(ExpressionComparisonType::All); }
    | NONE                      { $$ = int(ExpressionComparisonType::None); }

post_op
    : %empty                    { $$ = nullptr; }
    | '.' SIZE                  { $$ = drv.m_parse_nodes.create<PostOpNode>($2, PostOpNode::SIZE);}
    | '.' TYPE                  { $$ = drv.m_parse_nodes.create<PostOpNode>($2, PostOpNode::TYPE);}

aggr_op
    : MAX                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::MAX);}
    | MIN                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::MIN);}
    | SUM                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::SUM);}
    | AVG                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::AVG);}

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
    : %empty                    { $$ = drv.m_parse_nodes.create<PathNode>(); }
    | path path_elem            { $1->add_element($2); $$ = $1; }

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
