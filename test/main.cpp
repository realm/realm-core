#include "UnitTest++.h"
#include <string>

int main() {

	wchar_t c = L'Å';

	volatile wchar_t a = towlower(c);

	const int res = UnitTest::RunAllTests();
#ifdef _MSC_VER
	getchar(); // wait for key
#endif
	return res;
}
