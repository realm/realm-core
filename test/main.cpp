
#include "UnitTest++.h"
#include <string>
#include <math.h>
#include "Column.h"

int main() {
	const int res = UnitTest::RunAllTests();
#ifdef _MSC_VER
	getchar(); // wait for key
#endif
	return res;
}
