
#include "UnitTest++.h"
#include <string>
#include <math.h>
#include "Column.h"

#define hello hej \
	hj3 \\\
	efe

int main() {

	int a = rand();
	int b = rand();

//	volatile int c = a > b ? a : b; // cmovg
/*
	Column c;
	
	for(int i = 0; i < 20000; i++)
		c.Add(rand() % 100);

	c.arrays(0, -1);

	size_t siz = c.Size();
	for(int i = 0; i < siz; i++)
		printf("%d ", c.Get(i));
		*/

	const int res = UnitTest::RunAllTests();
#ifdef _MSC_VER
	getchar(); // wait for key
#endif
	return res;
}
