#include "Checks.h"
#include <cstring>

namespace UnitTest {
void CheckEqual(TestResults& results, int expected, size_t actual, TestDetails const& details)
{
	CheckEqual(results, expected, (int)actual, details);
}

void CheckEqual(TestResults& results, size_t expected, int actual, TestDetails const& details)
{
	CheckEqual(results, (int)expected, actual, details);
}

namespace {

void CheckStringsEqual(TestResults& results, char const* expected, char const* actual, 
                       TestDetails const& details)
{
	using namespace std;

    if (strcmp(expected, actual))
    {
        UnitTest::MemoryOutStream stream;
        stream << "Expected " << expected << " but was " << actual;

        results.OnTestFailure(details, stream.GetText());
    }
}

}


void CheckEqual(TestResults& results, char const* expected, char const* actual,
                TestDetails const& details)
{
    CheckStringsEqual(results, expected, actual, details);
}

void CheckEqual(TestResults& results, char* expected, char* actual,
                TestDetails const& details)
{
    CheckStringsEqual(results, expected, actual, details);
}

void CheckEqual(TestResults& results, char* expected, char const* actual,
                TestDetails const& details)
{
    CheckStringsEqual(results, expected, actual, details);
}

void CheckEqual(TestResults& results, char const* expected, char* actual,
                TestDetails const& details)
{
    CheckStringsEqual(results, expected, actual, details);
}


}
