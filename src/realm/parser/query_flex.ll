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
%}

%option nounistd never-interactive noyywrap nounput noinput batch debug noyylineno reentrant

hex     [0-9a-fA-F]
unicode "\\u"{hex}{4}
escape  "\\"[\"\'/bfnrt0\\]
char1    [^\"\\]
char2    [^\'\\]
utf8    [\xC2-\xDF][\x80-\xBF]|[\xE0-\xEF][\x80-\xBF]{2}|[\xF0-\xF7][\x80-\xBF]{3}
letter  [a-zA-Z_$]
ws      "\\"[ nrt]
id_char [a-zA-Z_\-$0-9]
digit   [0-9]
int     {digit}+
sint    [+-]?{digit}+
optint  {digit}*
exp     [eE]{sint}
blank   [ \t\r]

%%
{blank}+   ;
\n+        ;

"+"|"-"|"*"|"/"             return yytext[0];
("=="|"=")                  return yy::parser::make_EQUAL  ();
("in"|"IN")                 return yy::parser::make_IN  ();
("!="|"<>")                 return yy::parser::make_NOT_EQUAL();
"<"                         return yy::parser::make_LESS   ();
">"                         return yy::parser::make_GREATER();
[\[\],(){}\.]               return yytext[0];
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
(?i:between)                return yy::parser::make_BETWEEN(yytext);
(?i:truepredicate)          return yy::parser::make_TRUEPREDICATE (); 
(?i:falsepredicate)         return yy::parser::make_FALSEPREDICATE (); 
(?i:sort)                   return yy::parser::make_SORT(yytext);
(?i:distinct)               return yy::parser::make_DISTINCT(yytext);
(?i:limit)                  return yy::parser::make_LIMIT(yytext);
(?i:ascending)|(?i:asc)     return yy::parser::make_ASCENDING();
(?i:descending)|(?i:desc)   return yy::parser::make_DESCENDING();
(?i:subquery)               return yy::parser::make_SUBQUERY();
("@size"|"@count")          return yy::parser::make_SIZE(yytext);
"@max"                      return yy::parser::make_MAX    ();
"@min"                      return yy::parser::make_MIN    ();
"@sum"                      return yy::parser::make_SUM    ();
"@avg"                      return yy::parser::make_AVG    ();
"@links"                    return yy::parser::make_BACKLINK();
"@type"                     return yy::parser::make_TYPE    (yytext);
"@keys"                     return yy::parser::make_KEY_VAL (yytext);
"@values"                   return yy::parser::make_KEY_VAL (yytext);
"[c]"                       return yy::parser::make_CASE    ();
(true|TRUE)                 return yy::parser::make_TRUE    ();
(false|FALSE)               return yy::parser::make_FALSE    ();
[+-]?((?i:inf)|(?i:infinity)) return yy::parser::make_INFINITY(yytext);
[+-]?(?i:nan)               return  yy::parser::make_NAN(yytext);
(?i:null)|(?i:nil)          return yy::parser::make_NULL_VAL ();
"uuid("{hex}{8}"-"{hex}{4}"-"{hex}{4}"-"{hex}{4}"-"{hex}{12}")" return yy::parser::make_UUID(yytext); 
"oid("{hex}{24}")"          return yy::parser::make_OID(yytext); 
("T"{sint}":"{sint})|({int}"-"{int}"-"{int}[@T]{int}":"{int}":"{int}(":"{int})?) return yy::parser::make_TIMESTAMP(yytext);
"O"{int}                    return yy::parser::make_LINK (yytext);
"L"{int}":"{int}            return yy::parser::make_TYPED_LINK (yytext);
{int}                       return yy::parser::make_NATURAL0 (yytext);
"$"{int}                    return yy::parser::make_ARG(yytext); 
[+-]?{int}                  return yy::parser::make_NUMBER (yytext);
"0"[xX]{hex}+               return yy::parser::make_NUMBER (yytext);
[+-]?{int}{exp}?f?          return yy::parser::make_FLOAT (yytext);
[+-]?(({int}"."{optint})|({optint}"."{int})){exp}?f? return yy::parser::make_FLOAT (yytext);
("B64\""[a-zA-Z0-9/\+=]*\")         return yy::parser::make_BASE64(yytext);
(\"({char1}|{escape}|{unicode})*\") return yy::parser::make_STRING (yytext);
('({char2}|{escape}|{unicode})*')   return yy::parser::make_STRING (yytext);
({letter}|{utf8})({id_char}|{utf8}|{ws})*           return yy::parser::make_ID (check_escapes(yytext));

.          {
             throw yy::parser::syntax_error
               ("invalid character: " + std::string(yytext));
           }

<<EOF>>    return yy::parser::make_END ();
%%

void realm::query_parser::ParserDriver::scan_begin (yyscan_t yyscanner, bool trace_scanning)
{
    struct yyguts_t * yyg = (struct yyguts_t*)yyscanner;
    yy_flex_debug = trace_scanning;
    yy_scan_buffer(parse_buffer.data(), int(parse_buffer.size()), yyscanner);
}
