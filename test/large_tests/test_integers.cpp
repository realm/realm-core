
#include <UnitTest++.h>
#include "../testsettings.h"
#include "Column.h"

#include "verified_integer.h"

TEST(large1)
{
	VerifiedInteger a;
	Column c;

	for(int i = 0; i < 5000; i++) {
		a.Add(i);
	}

	for(int i = 0; i < 5000; i++) {
		a.Delete(rand() % 4000);
		a.Insert(rand() % 4000, rand());
	}
	
	c.Clear(); a.FindAll(c, 1);
}
