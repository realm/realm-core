#include <sstream>
#include <iostream>
#include <stdexcept>
#include "tightdb.h"
#include "Group.h"

int main()
{
	Group g;
	TopLevelTable &table = g.GetTable("test");
	Spec s = table.GetSpec();
	s.AddColumn(COLUMN_TYPE_INT, "foo");
	Spec sub = s.AddColumnTable("sub");
	sub.AddColumn(COLUMN_TYPE_INT, "bar");
	table.UpdateFromSpec(s.GetRef());

	for (int i=0; i<15000; ++i) {
		table.AddRow();
		table.Set(0, i, 100+i);
		if (i%2 == 0) {
			Table st = table.GetTable(1, i);
			st.AddRow();
			st.Set(0, 0, 200+i);
		}
	}

	cout << table.GetSize() << endl;

	for (int i=0; i<15000; ++i) {
		if (table.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table.Get(0, i) << " at " << i;
			throw runtime_error(o.str());
		}
		Table st = table.GetTable(1, i);
		if (st.GetSize() != (i%2 == 0 ? 1 : 0)) throw runtime_error("Bad subtable size");
		if (i%2 == 0) {
			if (st.Get(0,0) != 200+i) {
				ostringstream o;
				o << "Bad bar " << st.Get(0,0) << " at 0";
				throw runtime_error(o.str());
			}
		}
		if (i%3 == 0) {
			st.AddRow();
			st.Set(0, st.GetSize()-1, 300+i);
		}
	}

	for (int i=0; i<15000; ++i) {
		if (table.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table.Get(0, i) << " at " << i << " in second run";
			throw runtime_error(o.str());
		}
		Table st = table.GetTable(1, i);
		size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0);
		if (st.GetSize() != expected_size) throw runtime_error("Bad subtable size in second run");
		size_t idx = 0;
		if (i%2 == 0) {
			if (st.Get(0, idx) != 200+i) {
				ostringstream o;
				o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in second run";
				throw runtime_error(o.str());
			}
			++idx;
		}
		if (i%3 == 0) {
			if (st.Get(0, idx) != 300+i) {
				ostringstream o;
				o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in second run";
				throw runtime_error(o.str());
			}
			++idx;
		}
	}

	g.Write("subtables.tightdb");

	// Read back tables
	Group g2("subtables.tightdb");
	TopLevelTable &table2 = g2.GetTable("test");

	for (int i=0; i<15000; ++i) {
		if (table2.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table2.Get(0, i) << " at " << i << " in third run";
			throw runtime_error(o.str());
		}
		Table st = table2.GetTable(1, i);
		size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0);
		if (st.GetSize() != expected_size) throw runtime_error("Bad subtable size in third run");
		size_t idx = 0;
		if (i%2 == 0) {
			if (st.Get(0, idx) != 200+i) {
				ostringstream o;
				o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in third run";
				throw runtime_error(o.str());
			}
			++idx;
		}
		if (i%3 == 0) {
			if (st.Get(0, idx) != 300+i) {
				ostringstream o;
				o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in third run";
				throw runtime_error(o.str());
			}
			++idx;
		}
	}

	return 0;
}
