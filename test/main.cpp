#include "UnitTest++.h"

int main() {
	const int res = UnitTest::RunAllTests();

#ifdef _MSC_VER
	getchar(); // wait for key
#endif
	return res;
}
