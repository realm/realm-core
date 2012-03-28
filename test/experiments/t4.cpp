#include <sstream>
#include <iostream>
#include <stdexcept>
#include "tightdb.h"
#include "Group.h"

int main()
{
	int n = 15000;

	Group g;
	TopLevelTable &table = g.GetTable("test");
	Spec s = table.GetSpec();
	s.AddColumn(COLUMN_TYPE_INT, "foo");
	Spec sub = s.AddColumnTable("sub");
	sub.AddColumn(COLUMN_TYPE_INT, "bar");
	s.AddColumn(COLUMN_TYPE_MIXED, "baz");
	table.UpdateFromSpec(s.GetRef());

	for (int i=0; i<n; ++i) {
		table.AddRow();
		table.Set(0, i, 100+i);
		if (i%2 == 0) {
			Table st = table.GetTable(1, i);
			st.AddRow();
			st.Set(0, 0, 200+i);
		}
		if (i%3 == 1) {
			table.SetMixed(2, i, Mixed(COLUMN_TYPE_TABLE));
			TopLevelTable st = table.GetMixedTable(2, i);
			st.RegisterColumn(COLUMN_TYPE_INT, "banach");
			st.AddRow();
			st.Set(0, 0, 700+i);
		}
	}

	if (table.GetSize() != n) throw runtime_error("Bad table size");

	for (int i=0; i<n; ++i) {
		if (table.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table.Get(0, i) << " at " << i;
			throw runtime_error(o.str());
		}
		{
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
		if (table.GetMixedType(2,i) != (i%3 == 1 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT)) throw runtime_error("Bad mixed type");
		if (i%3 == 1) {
			TopLevelTable st = table.GetMixedTable(2, i);
			if (st.GetSize() != 1) throw runtime_error("Bad subtable size in mixed column");
			if (st.Get(0,0) != 700+i) {
				ostringstream o;
				o << "Bad banach " << st.Get(0,0) << " at i = " << i;
				throw runtime_error(o.str());
			}
		}
		if (i%8 == 3) {
			if (i%3 != 1) table.SetMixed(2, i, Mixed(COLUMN_TYPE_TABLE));
			TopLevelTable st = table.GetMixedTable(2, i);
			if (i%3 != 1) st.RegisterColumn(COLUMN_TYPE_INT, "banach");
			st.AddRow();
			st.Set(0, st.GetSize()-1, 800+i);
		}
	}

	for (int i=0; i<n; ++i) {
		if (table.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table.Get(0, i) << " at " << i << " in second run";
			throw runtime_error(o.str());
		}
		{
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
		if (table.GetMixedType(2,i) != (i%3 == 1 || i%8 == 3 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT)) throw runtime_error("Bad mixed type in second run");
		if (i%3 == 1 || i%8 == 3) {
			TopLevelTable st = table.GetMixedTable(2, i);
			size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0);
			if (st.GetSize() != expected_size) throw runtime_error("Bad subtable size in mixed column in second run");
			size_t idx = 0;
			if (i%3 == 1) {
				if (st.Get(0, idx) != 700+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in second run, should have been " << (700+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%8 == 3) {
				if (st.Get(0, idx) != 800+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in second run, should have been " << (800+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
		}
	}

	g.Write("subtables.tdb");

	// Read back tables
	Group g2("subtables.tdb");
	TopLevelTable &table2 = g2.GetTable("test");

	for (int i=0; i<n; ++i) {
		if (table2.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table2.Get(0, i) << " at " << i << " in third run";
			throw runtime_error(o.str());
		}
		{
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
			if (i%5 == 0) {
				st.AddRow();
				st.Set(0, st.GetSize()-1, 400+i);
			}
		}
		if (table2.GetMixedType(2,i) != (i%3 == 1 || i%8 == 3 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT)) throw runtime_error("Bad mixed type in third run");
		if (i%3 == 1 || i%8 == 3) {
			TopLevelTable st = table2.GetMixedTable(2, i);
			size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0);
			if (st.GetSize() != expected_size) throw runtime_error("Bad subtable size in mixed column in third run");
			size_t idx = 0;
			if (i%3 == 1) {
				if (st.Get(0, idx) != 700+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in third run, should have been " << (700+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%8 == 3) {
				if (st.Get(0, idx) != 800+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in third run, should have been " << (800+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
		}
		if (i%7 == 4) {
			if (i%3 != 1 && i%8 != 3) table2.SetMixed(2, i, Mixed(COLUMN_TYPE_TABLE));
			TopLevelTable st = table2.GetMixedTable(2, i);
			if (i%3 != 1 && i%8 != 3) st.RegisterColumn(COLUMN_TYPE_INT, "banach");
			st.AddRow();
			st.Set(0, st.GetSize()-1, 900+i);
		}
	}

	for (int i=0; i<n; ++i) {
		if (table2.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table2.Get(0, i) << " at " << i << " in fourth run";
			throw runtime_error(o.str());
		}
		{
			Table st = table2.GetTable(1, i);
			size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0) + (i%5 == 0 ? 1 : 0);
			if (st.GetSize() != expected_size) throw runtime_error("Bad subtable size in fourth run");
			size_t idx = 0;
			if (i%2 == 0) {
				if (st.Get(0, idx) != 200+i) {
					ostringstream o;
					o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in fourth run";
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%3 == 0) {
				if (st.Get(0, idx) != 300+i) {
					ostringstream o;
					o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in fourth run";
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%5 == 0) {
				if (st.Get(0, idx) != 400+i) {
					ostringstream o;
					o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in fourth run";
					throw runtime_error(o.str());
				}
				++idx;
			}
		}
		if (table2.GetMixedType(2,i) != (i%3 == 1 || i%8 == 3 || i%7 == 4 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT)) throw runtime_error("Bad mixed type in fourth run");
		if (i%3 == 1 || i%8 == 3 || i%7 == 4) {
			TopLevelTable st = table2.GetMixedTable(2, i);
			size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0) + (i%7 == 4 ? 1 : 0);
			if (st.GetSize() != expected_size) {
				ostringstream o;
				o << "Bad subtable size " << st.GetSize() << " in mixed column in fourth run at i = " << i << ", expecting " << expected_size;
				throw runtime_error(o.str());
			}
			size_t idx = 0;
			if (i%3 == 1) {
				if (st.Get(0, idx) != 700+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in fourth run, should have been " << (700+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%8 == 3) {
				if (st.Get(0, idx) != 800+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in fourth run, should have been " << (800+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%7 == 4) {
				if (st.Get(0, idx) != 900+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in fourth run, should have been " << (900+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
		}
	}

	g2.Write("subtables2.tdb");

	// Read back tables
	Group g3("subtables2.tdb");
	TopLevelTable &table3 = g2.GetTable("test");

	for (int i=0; i<n; ++i) {
		if (table3.Get(0, i) != 100+i) {
			ostringstream o;
			o << "Bad foo " << table3.Get(0, i) << " at " << i << " in fifth run";
			throw runtime_error(o.str());
		}
		{
			Table st = table3.GetTable(1, i);
			size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0) + (i%5 == 0 ? 1 : 0);
			if (st.GetSize() != expected_size) throw runtime_error("Bad subtable size in fifth run");
			size_t idx = 0;
			if (i%2 == 0) {
				if (st.Get(0, idx) != 200+i) {
					ostringstream o;
					o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in fifth run";
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%3 == 0) {
				if (st.Get(0, idx) != 300+i) {
					ostringstream o;
					o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in fifth run";
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%5 == 0) {
				if (st.Get(0, idx) != 400+i) {
					ostringstream o;
					o << "Bad bar " << st.Get(0, idx) << " at " << idx << " in fifth run";
					throw runtime_error(o.str());
				}
				++idx;
			}
		}
		if (table3.GetMixedType(2,i) != (i%3 == 1 || i%8 == 3 || i%7 == 4 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT)) throw runtime_error("Bad mixed type in fifth run");
		if (i%3 == 1 || i%8 == 3 || i%7 == 4) {
			TopLevelTable st = table3.GetMixedTable(2, i);
			size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0) + (i%7 == 4 ? 1 : 0);
			if (st.GetSize() != expected_size) throw runtime_error("Bad subtable size in mixed column in fifth run");
			size_t idx = 0;
			if (i%3 == 1) {
				if (st.Get(0, idx) != 700+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in fifth run, should have been " << (700+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%8 == 3) {
				if (st.Get(0, idx) != 800+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in fifth run, should have been " << (800+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
			if (i%7 == 4) {
				if (st.Get(0, idx) != 900+i) {
					ostringstream o;
					o << "Bad banach " << st.Get(0, idx) << " at i = " << i << " and j = " << idx << " in fifth run, should have been " << (900+i);
					throw runtime_error(o.str());
				}
				++idx;
			}
		}
	}

	return 0;
}
