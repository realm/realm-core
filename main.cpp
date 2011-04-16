// includes for memory leak detection
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>

#include <stdio.h>
#include <UnitTest++.h>

int main() {
	const int res = UnitTest::RunAllTests();

	getchar(); // wait for key
	return res;
}