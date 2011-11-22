
#include <UnitTest++.h>
#include "../testsettings.h"
#include "../Support/number_names.h"
#include "Column.h"

#include "verified_string.h"

TEST(large2)
{
	VerifiedString a;
	Column c;

	for(int i = 0; i < 5000; i++) {
		a.Add(number_name(i).c_str());
	}

	for(int i = 0; i < 5000; i++) {
		a.Delete(rand() % 4000);
		a.Insert(rand() % 4000, number_name(i).c_str());
	}

	c.Clear(); a.FindAll(c, "five");

	c.Clear();	a.FindAll(c, "foo");

}

