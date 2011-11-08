#include "UnitTest++.h"

int main() {

	/*
unsigned int v = 61;

for(int i = 0; i < 64; i++)
{
	v = i;

	v--;
	v |= v >> 1;
	v |= v >> 2;
	v++;



	printf("%d\t%d\n", i, v);

}

	getchar();
*/

	const int res = UnitTest::RunAllTests();

#ifdef _MSC_VER
	getchar(); // wait for key
#endif
	return res;
}
