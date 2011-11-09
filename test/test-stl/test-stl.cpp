#include <stdio.h>
#include <vector>
#include <map>
#include <string>
#include <algorithm>
#include <UnitTest++.h>
#include "../Support/mem.h"
#include "../Support/number_names.h"

#ifdef _MSC_VER
#include "../../src/win32/stdint.h"
#endif

using namespace std;

enum Days {
	Mon,
	Tue,
	Wed,
	Thu,
	Fri,
	Sat,
	Sun
};

struct TestTable {
	int first;
	string second;
	int third;
	Days fourth;
};

class match_second {
public:
	match_second(const string& target) : m_target(target) {}
	bool operator()(const TestTable& v) const {
		return v.second == m_target;
	}
private:
	const string& m_target;
};

class match_third {
public:
	match_third(int target) : m_target(target) {}
	bool operator()(const TestTable& v) const {return v.third == m_target;}
private:
	const int m_target;
};

class match_fourth {
public:
	match_fourth(Days target) : m_target(target) {}
	bool operator()(const TestTable& v) const {return v.fourth == m_target;}
private:
	const Days m_target;
};

// Get and Set are too fast (50ms/M) for normal 64-bit rand*rand*rand*rand*rand (5-10ms/M)
uint64_t rand2() { 
	return (uint64_t)rand() * (uint64_t)rand() * (uint64_t)rand() * (uint64_t)rand() * (uint64_t)rand();


	static int64_t seed = 2862933555777941757ULL; 
	static int64_t seed2 = 0;
	seed = (2862933555777941757ULL * seed + 3037000493ULL); 
	seed2++;
	return seed * seed2 + seed2; 
}

int main() {
	vector<TestTable> table;

	for (size_t i = 0; i < 250000; ++i) {
		// create random string
		const int n = rand() % 1000;// * 10 + rand();
		const string s = number_name(n);

		TestTable t = {n, s, 100, Wed};
		table.push_back(t);
	}

	// Last entry for verification
	TestTable t = {0, "abcde", 100, Wed};
	table.push_back(t);

	const size_t memUsed = GetMemUsage();
	printf("Memory usage: %lld bytes\n", (int64_t)memUsed);

	UnitTest::Timer timer;
	
	// Search small integer column
	{
		timer.Start();

		// Do a search over entire column (value not found)
		for (size_t i = 0; i < 100; ++i) {
			vector<TestTable>::const_iterator res = find_if(table.begin(), table.end(), match_fourth(Tue));
			if (res != table.end()) {
				printf("error");
			}
		}

		const int search_time = timer.GetTimeInMs();
		printf("Search (small integer): %dms\n", search_time);
	}

	// Search byte-sized integer column
	{
		timer.Start();

		// Do a search over entire column (value not found)
		for (size_t i = 0; i < 100; ++i) {
			vector<TestTable>::const_iterator res = find_if(table.begin(), table.end(), match_third(50));
			if (res != table.end()) {
				printf("error");
			}
		}

		const int search_time = timer.GetTimeInMs();
		printf("Search (byte-sized integer): %dms\n", search_time);
	}
	
	// Search string column
	{
		timer.Start();

		// Do a search over entire column (value not found)
		const string target = "abcde";
		for (size_t i = 0; i < 100; ++i) {
			vector<TestTable>::const_iterator res = find_if(table.begin(), table.end(), match_second(target));
			if (res == table.end()) {
				printf("error");
			}
		}

		const int search_time = timer.GetTimeInMs();
		printf("Search (string): %dms\n", search_time);
	}

	// Add index
	multimap<int, TestTable> mapTable;
	{
		timer.Start();
		
		// Copy data to map
		for (vector<TestTable>::const_iterator p = table.begin(); p != table.end(); ++p) {
			mapTable.insert(pair<int,TestTable>(p->first,*p));
		}

		// free memory used by table
		vector<TestTable>().swap(table);

		const int search_time = timer.GetTimeInMs();
		printf("Add index: %dms\n", search_time);

		printf("Memory usage2: %lld bytes\n", (int64_t)GetMemUsage());
	}

	// Search with index
	{
		timer.Start();

		for (size_t i = 0; i < 1000; ++i) {
			const size_t n = rand() % 1000;
			multimap<int, TestTable>::const_iterator p = mapTable.find(n);
			if (p->second.fourth == Fri) { // to avoid above find being optimized away
				printf("error");
			}
		}

		const int search_time = timer.GetTimeInMs();
		printf("Search index: %dms\n", search_time);
	}

	getchar(); // wait for key
	//return 1;
}
