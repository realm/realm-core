#include "tightdb.h"
#include "Group.h"

int main()
{
	Group g;
	TopLevelTable& table = g.GetTable("test");
	Spec s = table.GetSpec();
	Spec sub = s.AddColumnTable("sub");
	sub.AddColumn(COLUMN_TYPE_INT, "foo");
	table.UpdateFromSpec(s.GetRef());

	for (int i=0; i<10; ++i) {
		table.AddRow();
	}
	g.Write("/tmp/subtables.tightdb");

	Group g2("/tmp/subtables.tightdb");
	TopLevelTable& table2 = g2.GetTable("test");
	Table t = table2.GetTable(0,0);
	t.AddRow();
	return 0;
}
