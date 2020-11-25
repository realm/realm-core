%top{
#include <stdint.h>
}
%{ /* -*- C++ -*- */
# include <cerrno>
# include <climits>
# include <cstdlib>
# include <cstring> // strerror
# include <string>
# include "realm/parser/driver.hpp"
# include "realm/parser/generated/query_bison.hpp"
%}

%option nounistd never-interactive noyywrap nounput noinput batch debug noyylineno

hex     [0-9a-fA-F]
unicode "\\u"{hex}{4}
escape  "\\"[\"\'/bfnrt0\\]
chars   [^\"\'\\]
id      [a-zA-Z_$][a-zA-Z_\-$0-9]*
digit   [0-9]
int     {digit}+
sint    [+-]?{digit}+
optint  {digit}*
exp     [eE]{sint}
blank   [ \t\r]

%%
{blank}+   ;
\n+        ;

("=="|"="|"in"|"IN")        return yy::parser::make_EQUAL  ();
("!="|"<>")                 return yy::parser::make_NOT_EQUAL();
"<"                         return yy::parser::make_LESS   ();
">"                         return yy::parser::make_GREATER();
[,()\.]                     return yytext[0];
("<="|"=<")                 return yy::parser::make_LESS_EQUAL ();
(">="|"=>")                 return yy::parser::make_GREATER_EQUAL ();
&&|(?i:and)                 return yy::parser::make_AND    ();
"||"|(?i:or)                return yy::parser::make_OR     ();
("!"|"not"|"NOT")           return yy::parser::make_NOT();
"any"|"ANY"|"some"|"SOME"   return yy::parser::make_ANY();
"all"|"ALL"                 return yy::parser::make_ALL();
"none"|"NONE"               return yy::parser::make_NONE();
(?i:beginswith)             return yy::parser::make_BEGINSWITH(yytext);
(?i:endswith)               return yy::parser::make_ENDSWITH(yytext);
(?i:contains)               return yy::parser::make_CONTAINS(yytext);
(?i:like)                   return yy::parser::make_LIKE(yytext);
(?i:truepredicate)          return yy::parser::make_TRUEPREDICATE (); 
(?i:falsepredicate)         return yy::parser::make_FALSEPREDICATE (); 
(?i:sort)                   return yy::parser::make_SORT();
(?i:distinct)               return yy::parser::make_DISTINCT();
(?i:limit)                  return yy::parser::make_LIMIT();
(?i:ascending)|(?i:asc)     return yy::parser::make_ASCENDING();
(?i:descending)|(?i:desc)   return yy::parser::make_DESCENDING();
"@size"                     return yy::parser::make_SIZE    ();
"@count"                    return yy::parser::make_COUNT    ();
"@max"                      return yy::parser::make_MAX    ();
"@min"                      return yy::parser::make_MIN    ();
"@sum"                      return yy::parser::make_SUM    ();
"@avg"                      return yy::parser::make_AVG    ();
"@links"                    return yy::parser::make_BACKLINK();
"[c]"                       return yy::parser::make_CASE    ();
(true|TRUE)                 return yy::parser::make_TRUE    ();
(false|FALSE)               return yy::parser::make_FALSE    ();
[+-]?((?i:inf)|(?i:infinity)) return yy::parser::make_INFINITY(yytext);
[+-]?(?i:nan)               return  yy::parser::make_NAN(yytext);
(?i:null)|(?i:nil)          return yy::parser::make_NULL_VAL ();
"uuid("{hex}{8}"-"{hex}{4}"-"{hex}{4}"-"{hex}{4}"-"{hex}{12}")" return yy::parser::make_UUID(yytext); 
"oid("{hex}{24}")"          return yy::parser::make_OID(yytext); 
("T"{sint}":"{sint})|({int}"-"{int}"-"{int}[@T]{int}":"{int}":"{int}(":"{int})?) return yy::parser::make_TIMESTAMP(yytext);
{int}                       return yy::parser::make_NATURAL0 (yytext);
"$"{int}                    return yy::parser::make_ARG(yytext); 
[+-]?{int}                  return yy::parser::make_NUMBER (yytext);
"0"[xX]{hex}+               return yy::parser::make_NUMBER (yytext);
[+-]?{int}{exp}?            return yy::parser::make_FLOAT (yytext);
[+-]?(({int}"."{optint})|({optint}"."{int})){exp}? return yy::parser::make_FLOAT (yytext);
("B64\""[a-zA-Z0-9/\+=]*\")  return yy::parser::make_BASE64(yytext);
('({chars}|{escape}|{unicode})*')            return yy::parser::make_STRING (yytext);
(\"({chars}|{escape}|{unicode})*\")            return yy::parser::make_STRING (yytext);
{id}                        return yy::parser::make_ID (yytext);

.          {
             throw yy::parser::syntax_error
               ("invalid character: " + std::string(yytext));
           }

<<EOF>>    return yy::parser::make_END ();
%%

void realm::query_parser::ParserDriver::scan_begin (bool trace_scanning)
{
    yy_flex_debug = trace_scanning;
    YY_BUFFER_STATE bp;
    bp = yy_scan_bytes(parse_string.c_str(), int(parse_string.size()));
    yy_switch_to_buffer(bp);
    scan_buffer = (void *)bp;
}

void realm::query_parser::ParserDriver::scan_end ()
{
   yy_delete_buffer((YY_BUFFER_STATE)scan_buffer);
}
