import sys
from Cheetah.Template import Template

templateDef = """#slurp
#compiler-settings
commentStartToken = %%
directiveStartToken = %
#end compiler-settings
#ifndef __TIGHTDB_H__
#define __TIGHTDB_H__

#include "Table.h"
#include <vector>

#include "query/QueryInterface.h"

using namespace std;

#define TDB_QUERY(QueryName, TableName) \\
class QueryName : public TableName##Query { \\
public: \\
QueryName()

#define TDB_QUERY_OPT(QueryName, TableName) \\
class QueryName : public TableName##Query { \\
public: \\
QueryName

#define TDB_QUERY_END }; \\
%set $max_cols = 5
%for $i in range($max_cols)
%set $num_cols = $i + 1



#define TDB_TABLE_${num_cols}(TableName%slurp
%for $j in range($num_cols)
, CType${j+1}, CName${j+1}%slurp
%end for
) \\
class TableName##Query { \\
protected: \\
%for $j in range($num_cols)
	QueryAccessor##CType${j+1} CName${j+1}; \\
%end for
}; \\
\\
class TableName : public TopLevelTable { \\
public: \\
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \\
%for $j in range($num_cols)
		RegisterColumn(Accessor##CType${j+1}::type, #CName${j+1}); \\
%end for
\\
%for $j in range($num_cols)
		CName${j+1}.Create(this, $j); \\
%end for
	}; \\
\\
	class Cursor : public CursorBase { \\
	public: \\
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \\
%for $j in range($num_cols)
			CName${j+1}.Create(this, $j); \\
%end for
		} \\
		Cursor(const Cursor& v) : CursorBase(v) { \\
%for $j in range($num_cols)
			CName${j+1}.Create(this, $j); \\
%end for
		} \\
%for $j in range($num_cols)
		Accessor##CType${j+1} CName${j+1}; \\
%end for
	}; \\
\\
	void Add(%slurp
%for $j in range($num_cols)
%if 0 < $j
, %slurp
%end if
tdbType##CType${j+1} v${j+1}%slurp
%end for
) { \\
		const size_t ndx = GetSize(); \\
%for $j in range($num_cols)
		Insert##CType${j+1} ($j, ndx, v${j+1}); \\
%end for
		InsertDone(); \\
	} \\
\\
	void Insert(size_t ndx%slurp
%for $j in range($num_cols)
, tdbType##CType${j+1} v${j+1}%slurp
%end for
) { \\
%for $j in range($num_cols)
		Insert##CType${j+1} ($j, ndx, v${j+1}); \\
%end for
		InsertDone(); \\
	} \\
\\
	Cursor Add() {return Cursor(*this, AddRow());} \\
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);} \\
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);} \\
	Cursor operator[](int ndx) {return Cursor(*this, (ndx < 0) ? GetSize() + ndx : ndx);} \\
\\
	size_t Find(const TableName##Query&) const {return (size_t)-1;} \\
	TableName FindAll(const TableName##Query&) const {return TableName();} \\
	TableName Sort() const {return TableName();} \\
	TableName Range(int, int) const {return TableName();} \\
	TableName Limit(size_t) const {return TableName();} \\
\\
%for $j in range($num_cols)
	ColumnProxy##CType${j+1} CName${j+1}; \\
%end for
\\
protected: \\
	friend class Group; \\
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \\
\\
private: \\
	TableName(const TableName&) {} \\
	TableName& operator=(const TableName&) {return *this;} \\
};
%end for
"""

t = Template(templateDef)
sys.stdout.write(str(t))
