%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.7.4"
%defines
%no-lines

%define api.token.constructor
%define api.value.type variant
%define parse.assert

%code requires {
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
    struct PathElem {
        std::string id;
        Mixed index;
        std::string buffer;
        PathElem() {}
        PathElem(const PathElem& other);
        PathElem& operator=(const PathElem& other);
        PathElem(std::string s) : id(s) {}
        PathElem(std::string s, Mixed i) : id(s), index(i) { index.use_buffer(buffer); }
    };

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
  SUBQUERY "subquery"
  TRUE    "true"
  FALSE   "false"
  NULL_VAL "null"
  EQUAL   "=="
  NOT_EQUAL   "!="
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
  GEOBOX        "geobox"
  GEOPOLYGON    "geopolygon"
  GEOSPHERE     "geosphere"
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
%token <std::string> TEXT "fulltext"
%token <std::string> LIKE    "like"
%token <std::string> BETWEEN "between"
%token <std::string> IN "in"
%token <std::string> GEOWITHIN "geowithin"
%token <std::string> OBJ "obj"
%token <std::string> SORT "sort"
%token <std::string> DISTINCT "distinct"
%token <std::string> LIMIT "limit"
%token <std::string> ASCENDING "ascending"
%token <std::string> DESCENDING "descending"
%token <std::string> SIZE "@size"
%token <std::string> TYPE "@type"
%token <std::string> KEY_VAL "key or value"
%type  <bool> direction
%type  <int> equality relational stringop aggr_op
%type  <double> coordinate
%type  <ConstantNode*> constant primary_key
%type  <GeospatialNode*> geospatial geoloop geoloop_content geopoly_content
// std::optional<GeoPoint> is necessary because GeoPoint has deleted its default constructor
// but bison must push a default value to the stack, even though it will be overwritten by a real value
%type  <std::optional<GeoPoint>> geopoint
%type  <ListNode*> list list_content
%type  <AggrNode*> aggregate
%type  <SubqueryNode*> subquery 
%type  <PropertyNode*> prop
%type  <PostOpNode*> post_op
%type  <ValueNode*> value
%type  <ExpressionNode*> expr
%type  <TrueOrFalseNode*> boolexpr
%type  <int> comp_type
%type  <QueryNode*> query compare
%type  <PathNode*> path
%type  <DescriptorOrderingNode*> post_query
%type  <DescriptorNode*> sort sort_param distinct distinct_param limit
%type  <std::string> id
%type  <PathElem> path_elem
%type  <PropertyNode*> simple_prop

%destructor { } <int>

%printer {
           if (!$$) {
               yyo << "null";
           } else {
             yyo << "['" << $$->longitude << "', '" << $$->latitude;
             if (auto alt = $$->get_altitude())
               yyo << "', '" << *alt; 
             yyo << "']"; }}  <std::optional<GeoPoint>>;
%printer { yyo << $$.id; } <PathElem>;
%printer { yyo << $$; } <*>;
%printer { yyo << "<>"; } <>;

%%
%start final;

%left OR;
%left AND;
%left '+' '-';
%left '*' '/';
%right NOT;

final
    : query post_query { drv.result = $1; drv.ordering = $2; };

query
    : compare                   { $$ = $1; }
    | query "||" query          { $$ = drv.m_parse_nodes.create<OrNode>($1, $3); }
    | query "&&" query          { $$ = drv.m_parse_nodes.create<AndNode>($1, $3); }
    | NOT query                 { $$ = drv.m_parse_nodes.create<NotNode>($2); }
    | '(' query ')'             { $$ = $2; }
    | boolexpr                  { $$ =$1; }

compare
    : expr equality expr        { $$ = drv.m_parse_nodes.create<EqualityNode>($1, $2, $3); }
    | expr equality CASE expr   {
                                    auto tmp = drv.m_parse_nodes.create<EqualityNode>($1, $2, $4);
                                    tmp->case_sensitive = false;
                                    $$ = tmp;
                                }
    | expr relational expr      { $$ = drv.m_parse_nodes.create<RelationalNode>($1, $2, $3); }
    | value stringop value      { $$ = drv.m_parse_nodes.create<StringOpsNode>($1, $2, $3); }
    | value TEXT value          { $$ = drv.m_parse_nodes.create<StringOpsNode>($1, CompareNode::TEXT, $3); }
    | value stringop CASE value {
                                    auto tmp = drv.m_parse_nodes.create<StringOpsNode>($1, $2, $4);
                                    tmp->case_sensitive = false;
                                    $$ = tmp;
                                }
    | value BETWEEN list        { $$ = drv.m_parse_nodes.create<BetweenNode>($1, $3); }
    | prop GEOWITHIN geospatial { $$ = drv.m_parse_nodes.create<GeoWithinNode>($1, $3); }
    | prop GEOWITHIN ARG        { $$ = drv.m_parse_nodes.create<GeoWithinNode>($1, $3); }

expr
    : value                     { $$ = $1; }
    | '(' expr ')'              { $$ = $2; }
    | expr '*' expr             { $$ = drv.m_parse_nodes.create<OperationNode>($1, '*', $3); }
    | expr '/' expr             { $$ = drv.m_parse_nodes.create<OperationNode>($1, '/', $3); }
    | expr '+' expr             { $$ = drv.m_parse_nodes.create<OperationNode>($1, '+', $3); }
    | expr '-' expr             { $$ = drv.m_parse_nodes.create<OperationNode>($1, '-', $3); }

value
    : constant                  { $$ = $1;}
    | prop                      { $$ = $1;}
    | list                      { $$ = $1;}
    | aggregate                 { $$ = $1;}
    | subquery                  { $$ = $1;}

prop
    : path post_op              { $$ = drv.m_parse_nodes.create<PropertyNode>($1); $$->add_postop($2); }
    | comp_type path post_op    { $$ = drv.m_parse_nodes.create<PropertyNode>($2, ExpressionComparisonType($1)); $$->add_postop($3); }

aggregate
    : path aggr_op '.'  id      {
                                    auto prop = drv.m_parse_nodes.create<PropertyNode>($1);
                                    $$ = drv.m_parse_nodes.create<LinkAggrNode>(prop, $2, $4);
                                }
    | path aggr_op              {
                                    auto prop = drv.m_parse_nodes.create<PropertyNode>($1);
                                    $$ = drv.m_parse_nodes.create<ListAggrNode>(prop, $2);
                                }

simple_prop
    : path                      { $$ = drv.m_parse_nodes.create<PropertyNode>($1); }

subquery
    : SUBQUERY '(' simple_prop ',' id ',' query ')' '.' SIZE   { $$ = drv.m_parse_nodes.create<SubqueryNode>($3, $5, $7); }

coordinate
    : FLOAT         { $$ = strtod($1.c_str(), nullptr); }
    | NATURAL0      { $$ = double(strtoll($1.c_str(), nullptr, 0)); }

geopoint
    : '[' coordinate ',' coordinate ']' { $$ = GeoPoint{$2, $4}; }
    | '[' coordinate ',' coordinate ',' FLOAT ']' { $$ = GeoPoint{$2, $4, strtod($6.c_str(), nullptr)}; }

geoloop_content
    : geopoint { $$ = drv.m_parse_nodes.create<GeospatialNode>(GeospatialNode::Loop{}, *$1); }
    | geoloop_content ',' geopoint { $1->add_point_to_loop(*$3); $$ = $1; }

geoloop : '{' geoloop_content '}' { $$ = $2; }

geopoly_content
    : geoloop { $$ = $1; }
    | geopoly_content ',' geoloop { $1->add_loop_to_polygon($3); $$ = $1; }

geospatial
    : GEOBOX '(' geopoint ',' geopoint ')'  { $$ = drv.m_parse_nodes.create<GeospatialNode>(GeospatialNode::Box{}, *$3, *$5); }
    | GEOSPHERE '(' geopoint ',' coordinate ')' { $$ = drv.m_parse_nodes.create<GeospatialNode>(GeospatialNode::Sphere{}, *$3, $5); }
    | GEOPOLYGON '(' geopoly_content ')'    { $$ = $3; }

post_query
    : %empty                    { $$ = drv.m_parse_nodes.create<DescriptorOrderingNode>();}
    | post_query sort           { $1->add_descriptor($2); $$ = $1; }
    | post_query distinct       { $1->add_descriptor($2); $$ = $1; }
    | post_query limit          { $1->add_descriptor($2); $$ = $1; }

distinct: DISTINCT '(' distinct_param ')' { $$ = $3; }

distinct_param
    : path                      { $$ = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::DISTINCT); $$->add($1);}
    | distinct_param ',' path   { $1->add($3); $$ = $1; }

sort: SORT '(' sort_param ')'   { $$ = $3; }

sort_param
    : path direction            { $$ = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::SORT); $$->add($1, $2);}
    | sort_param ',' path direction  { $1->add($3, $4); $$ = $1; }

limit: LIMIT '(' NATURAL0 ')'   { $$ = drv.m_parse_nodes.create<DescriptorNode>(DescriptorNode::LIMIT, $3); }

direction
    : ASCENDING                 { $$ = true; }
    | DESCENDING                { $$ = false; }

list : '{' list_content '}'             { $$ = $2; }
     | comp_type '{' list_content '}'   { $3->set_comp_type(ExpressionComparisonType($1)); $$ = $3; }

list_content
    : constant                  { $$ = drv.m_parse_nodes.create<ListNode>($1); }
    | %empty                    { $$ = drv.m_parse_nodes.create<ListNode>(); }
    | list_content ',' constant { $1->add_element($3); $$ = $1; } 

constant
    : primary_key               { $$ = $1; }
    | INFINITY                  { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::INFINITY_VAL, $1); }
    | NAN                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NAN_VAL, $1); }
    | BASE64                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::BASE64, $1); }
    | FLOAT                     { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FLOAT, $1); }
    | TIMESTAMP                 { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TIMESTAMP, $1); }
    | LINK                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::LINK, $1); }
    | TYPED_LINK                { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TYPED_LINK, $1); }
    | TRUE                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TRUE, ""); }
    | FALSE                     { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FALSE, ""); }
    | NULL_VAL                  { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NULL_VAL, ""); }
    | ARG                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::ARG, $1); }
    | comp_type ARG             { $$ = drv.m_parse_nodes.create<ConstantNode>(ExpressionComparisonType($1), $2); }
    | OBJ '(' STRING ',' primary_key ')'
                                { 
                                    auto tmp = $5;
                                    tmp->add_table($3);
                                    $$ = tmp;
                                }

primary_key
    : NATURAL0                  { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, $1); }
    | NUMBER                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, $1); }
    | STRING                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::STRING, $1); }
    | UUID                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::UUID_T, $1); }
    | OID                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::OID, $1); }

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
    : '.' MAX                   { $$ = int(AggrNode::MAX);}
    | '.' MIN                   { $$ = int(AggrNode::MIN);}
    | '.' SUM                   { $$ = int(AggrNode::SUM);}
    | '.' AVG                   { $$ = int(AggrNode::AVG);}

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
    : path_elem                 { $$ = drv.m_parse_nodes.create<PathNode>($1); }
    | path '.' path_elem        { $1->add_element($3); $$ = $1; }

path_elem
    : id                        { $$ = PathElem{$1}; }
    | id '[' NATURAL0 ']'       { $$ = PathElem{$1, int64_t(strtoll($3.c_str(), nullptr, 0))}; }
    | id '[' STRING ']'         { $$ = PathElem{$1, $3.substr(1, $3.size() - 2)}; }
    | id '[' ARG ']'            { $$ = PathElem{$1, drv.get_arg_for_index($3)}; }

id  
    : ID                        { $$ = $1; }
    | BACKLINK                  { $$ = std::string("@links"); }
    | BEGINSWITH                { $$ = $1; }
    | ENDSWITH                  { $$ = $1; }
    | CONTAINS                  { $$ = $1; }
    | LIKE                      { $$ = $1; }
    | BETWEEN                   { $$ = $1; }
    | KEY_VAL                   { $$ = $1; }
    | SORT                      { $$ = $1; }
    | DISTINCT                  { $$ = $1; }
    | LIMIT                     { $$ = $1; }
    | ASCENDING                 { $$ = $1; }
    | DESCENDING                { $$ = $1; }
    | IN                        { $$ = $1; }
    | TEXT                      { $$ = $1; }
%%

void
yy::parser::error (const std::string& m)
{
    drv.error(m);
}

#ifdef _MSC_VER
#pragma warning( pop ) // restore normal warning levels
#endif
