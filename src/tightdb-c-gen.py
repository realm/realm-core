import sys
from Cheetah.Template import Template
templateDef = """#slurp
#compiler-settings
commentStartToken = %%
directiveStartToken = %
#end compiler-settings
#ifndef __C_TIGHTDB_H__
#define __C_TIGHTDB_H__

#include "c-table.hpp"
#include "query_interface.h"

%for $col in range($max_cols)
%set $num_cols = $col + 1

#define TDB_TABLE_${num_cols}(TableName%slurp
%for $j in range($num_cols)
, CType$j, CName$j%slurp
%end for
) \\
\\
Table* TableName##_new(void) { \\
    Table *tbl = table_new(); \\
    Spec* spec = table_get_spec(tbl); \\
%for $j in range($num_cols)
	spec_add_column(spec, COLUMN_TYPE_##CType$j, #CName$j); \\
%end for
    table_update_from_spec(tbl, spec_get_ref(spec)); \\
    spec_delete(spec); \\
    return tbl; \\
} \\
\\
void TableName##_add(Table* tbl, %slurp
%for $j in range($num_cols)
%if 0 < $j
, %slurp
%end if
tdb_type_##CType$j value$j%slurp
%end for
) { \\
	table_add(tbl, %slurp
%for $j in range($num_cols)
%if 0 < $j
, %slurp
%end if
value$j%slurp
%end for
); \\
} \\
\\
void TableName##_insert(Table* tbl, size_t row_ndx, %slurp
%for $j in range($num_cols)
%if 0 < $j
, %slurp
%end if
tdb_type_##CType$j value$j%slurp
%end for
) { \\
	table_insert(tbl, row_ndx, %slurp
%for $j in range($num_cols)
%if 0 < $j
, %slurp
%end if
value$j%slurp
%end for
); \\
} \\
\\
%for $j in range($num_cols)
tdb_type_##CType$j TableName##_get_##CName${j}(Table* tbl, size_t row_ndx) { \\
	return table_get_##CType${j}(tbl, $j, row_ndx); \\
} \\
void TableName##_set_##CName${j}(Table* tbl, size_t row_ndx, tdb_type_##CType$j value) { \\
	return table_set_##CType${j}(tbl, $j, row_ndx, value); \\
} \\
%end for


%end for

#endif //__C_TIGHTDB_H__
"""

args = sys.argv[1:]
if len(args) != 1:
	sys.stderr.write("Please specify the maximum number of table columns\n")
	sys.exit(1)
max_cols = int(args[0])
t = Template(templateDef, searchList=[{'max_cols': max_cols}])
sys.stdout.write(str(t))
