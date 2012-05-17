import sys
from Cheetah.Template import Template

templateDef = """#slurp
#compiler-settings
commentStartToken = %%
directiveStartToken = %
#end compiler-settings
/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_HPP
#define TIGHTDB_HPP

#include "../src/table_basic.hpp"


%for $i in range($max_cols)
%set $num_cols = $i + 1
#define TIGHTDB_TABLE_${num_cols}(Table%slurp
%for $j in range($num_cols)
, name${j+1}, type${j+1}%slurp
%end for
) \\
struct Table##Spec: tightdb::SpecBase { \\
%for $j in range($num_cols)
    typedef tightdb::TypeAppend< %slurp
%if $j == 0
void%slurp
%else
ColTypes$j%slurp
%end if
, type${j+1} >::type %slurp
%if $j < $num_cols-1
ColTypes${j+1}%slurp
%else
ColTypes%slurp
%end if
; \\
%end for
 \\
    template<template<int> class Col, class Init> struct Columns { \\
%for $j in range($num_cols)
        typename Col<$j>::type name${j+1}; \\
%end for
        Columns(Init i): %slurp
%for $j in range($num_cols)
%if 0 < $j
, %slurp
%end if
name${j+1}%slurp
(i, #name${j+1})%slurp
%end for
 {} \\
    }; \\
 \\
    template<class C%slurp
%for $j in range($num_cols)
, class T${j+1}%slurp
%end for
> \\
    static void insert(std::size_t i, const C& cols%slurp
%for $j in range($num_cols)
, const T${j+1}& v${j+1}%slurp
%end for
) \\
    { \\
%for $j in range($num_cols)
        cols.name${j+1}._insert(i, v${j+1}); \\
%end for
    } \\
}; \\
typedef tightdb::BasicTable<Table##Spec> Table;


%end for
#endif // TIGHTDB_HPP
"""

args = sys.argv[1:]
if len(args) != 1:
    sys.stderr.write("Please specify the maximum number of table columns\n")
    sys.exit(1)
max_cols = int(args[0])
t = Template(templateDef, searchList=[{'max_cols': max_cols}])
sys.stdout.write(str(t))
