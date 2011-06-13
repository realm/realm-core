#include <cstdio>
#include "tightdb.h"

enum Days {
	Mon,
	Tue,
	Wed,
	Thu,
	Fri,
	Sat,
	Sun
};

// Define new table with 4 columns
TDB_TABLE_4(MyTable,
//      Type:       Name:
		Bool,       active,
		Int,        count,
		Enum<Days>, weekday,
		String,     desc)

void test() {
	// Create a new table instance
	MyTable table;

	// Add a row of data
	table.Add(true, 47, Tue, "Hello");

	// Add a row and set data individually
	MyTable::Cursor r = table.Add();
	r.active  = false;
	r.count   = 15;
	r.weekday = Wed;
	r.desc    = "Hello again";

	// Access values directly
	for (size_t i = 0; i < table.GetSize(); ++i) {
		const char* str = table[i].desc;
		printf("fourth: %s\n", str);
	}

	// Direct find (or lookup)
	size_t res = table.weekday.Find(Mon);
	if (res == -1) printf("not found");

	// Create advanced query
	TDB_QUERY_OPT(TestQuery, MyTable) (int v) {
		count <= v;
		desc == "Hello" || 
			(desc == "Hey" && weekday.between(Mon, Thu));
	}};

	// Run query with modifiers (single result)
	size_t result2 = table.Range(10, 200).Find(TestQuery(12));
	if (result2 != -1) printf("found match at %d\n", result2);
	
	// Run query with modifiers (all results)
	MyTable result = table.FindAll(TestQuery(2)).Sort().Limit(10);
}
