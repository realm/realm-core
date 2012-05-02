import sys
from Cheetah.Template import Template

templateDef = """#slurp
#compiler-settings
commentStartToken = %%
directiveStartToken = %
#end compiler-settings
#ifndef TIGHTDB_H
#define TIGHTDB_H

#include "basic_table.hpp"


%for $i in range($max_cols)
%set $num_cols = $i + 1
#define TDB_TABLE_${num_cols}(Table%slurp
%for $j in range($num_cols)
, type${j+1}, name${j+1}%slurp
%end for
) \\
struct Table##Spec: tightdb::SpecBase { \\
    template<template<int, class> class Column, class Init> \\
    class Columns { \\
    public: \\
%for $j in range($num_cols)
        Column<$j, type${j+1}> name${j+1}; \\
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
#endif // TIGHTDB_H
"""

args = sys.argv[1:]
if len(args) != 1:
    sys.stderr.write("Please specify the maximum number of table columns\n")
    sys.exit(1)
max_cols = int(args[0])
t = Template(templateDef, searchList=[{'max_cols': max_cols}])
sys.stdout.write(str(t))
