#ifdef _MSC_VER
	// includes for memory leak detection
	#define _CRTDBG_MAP_ALLOC
	#include <crtdbg.h>
	#include <cstdlib>
	#include <cstdio>
#endif

#include "UnitTest++.h"

int main() {
	const int res = UnitTest::RunAllTests();

	//getchar(); // wait for key
	return res;
}
