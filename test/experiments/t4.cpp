#include <vector>
#include <iostream>
#include "tightdb.h"
#include "Group.h"

int main()
{
	Group g;
	TopLevelTable &table = g.GetTable("test");
	Spec s = table.GetSpec();
	Spec sub = s.AddColumnTable("sub");
	sub.AddColumn(COLUMN_TYPE_INT, "foo");
	table.UpdateFromSpec(s.GetRef());

	for (int i=0; i<10000; ++i) {
		if (i%500 == 0) cerr << i << endl;
		table.AddRow();
		Table st = table.GetTable(0, i);
		st.AddRow();
	}
	return 0;
}
