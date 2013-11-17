#ifndef UNITTEST_TESTRUNNER_H
#define UNITTEST_TESTRUNNER_H

#include "Test.h"
#include "TestList.h"
#include "CurrentTest.h"
#include <iostream>

namespace UnitTest {

class TestReporter;
class TestResults;
class Timer;

int RunAllTests();

struct True
{
	bool operator()(const Test* const) const
	{
		return true;	
	}
};

class TestRunner
{
public:
	explicit TestRunner(TestReporter& reporter);
	~TestRunner();

	template <class Predicate>
	int RunTestsIf(TestList const& list, char const* suiteName, 
				   const Predicate& predicate, int maxTestTimeInMs) const
	{

        bool only = false;

        // See if there are any unit tests whose name ends with _ONLY suffix which means that we must only 
        // execute that single test and none else
        {
    	    Test* curTest = list.GetHead();
	        while (curTest != 0)
	        {
		        if (IsTestInSuite(curTest,suiteName) && predicate(curTest))
			    {
                    std::string n = std::string(curTest->m_details.testName);
                    if(n.size() >= 5 && n.substr(n.size() - 5, 5) == "_ONLY")
                        only = true;
			    }
			    curTest = curTest->next;
	        }
        }
        
        if(only)
            std::cerr << "\n *** BE AWARE THAT MULTIPLE UNIT TESTS ARE DISABLED DUE TO USING 'ONLY' MACRO *** \n\n";

        Test* curTest = list.GetHead();

	    while (curTest != 0)
	    {
		    if (IsTestInSuite(curTest,suiteName) && predicate(curTest))
			{
                std::string n = std::string(curTest->m_details.testName);
                if(!only || (only && n.size() >= 5 && n.substr(n.size() - 5, 5) == "_ONLY"))
    				RunTest(m_result, curTest, maxTestTimeInMs);
			}

			curTest = curTest->next;
	    }

	    return Finish();
	}	

private:
	TestReporter* m_reporter;
	TestResults* m_result;
	Timer* m_timer;

	int Finish() const;
	bool IsTestInSuite(const Test* const curTest, char const* suiteName) const;
	void RunTest(TestResults* const result, Test* const curTest, int const maxTestTimeInMs) const;
};

}

#endif
