#include "UnitTest++.h"
#include <string>

int main() {
	/*
	char hay[] = "this is a test on substring search\0";
	char needle[] = "sub\0";
	size_t lens[] = {1, 1, 1};

	volatile bool b = case_strstr(needle, needle, lens, hay, true);


	return 0;
	*/

	const int res = UnitTest::RunAllTests();
#ifdef _MSC_VER
	getchar(); // wait for key
#endif
	return res;
}
