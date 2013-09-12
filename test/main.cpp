#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/column.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/query_expression.hpp>
#include <assert.h>

#define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

using namespace std;
using namespace UnitTest;
using namespace tightdb;

namespace {



struct CustomTestReporter: TestReporter {
    void ReportTestStart(TestDetails const& test)
    {
        static_cast<void>(test);
//        cerr << test.filename << ":" << test.lineNumber << ": Begin " << test.testName << "\n";
    }

    void ReportFailure(TestDetails const& test, char const* failure)
    {
        cerr << test.filename << ":" << test.lineNumber << ": error: "
            "Failure in " << test.testName << ": " << failure << "\n";
    }

    void ReportTestFinish(TestDetails const& test, float seconds_elapsed)
    {
        static_cast<void>(test);
        static_cast<void>(seconds_elapsed);
//        cerr << test.filename << ":" << test.lineNumber << ": End\n";
    }

    void ReportSummary(int total_test_count, int failed_test_count, int failure_count, float seconds_elapsed)
    {
        if (0 < failure_count)
            cerr << "FAILURE: " << failed_test_count << " "
                "out of " << total_test_count << " tests failed "
                "(" << failure_count << " failures).\n";
        else
            cerr << "Success: " << total_test_count << " tests passed.\n";

        const streamsize orig_prec = cerr.precision();
        cerr.precision(2);
        cerr << "Test time: " << seconds_elapsed << " seconds.\n";
        cerr.precision(orig_prec);
    }
};

} // anonymous namespace


TIGHTDB_TABLE_3(ThreeColTable,
    first,  Int,
    second, Float,
    third, Double)


int main(int argc, char* argv[])
{
    size_t match;

    // Setup untyped table
    Table untyped;
    untyped.add_column(type_Int, "firs1");
    untyped.add_column(type_Float, "second");
    untyped.add_column(type_Double, "third");
    untyped.add_empty_row(2);
    untyped.set_int(0, 0, 20);
    untyped.set_float(1, 0, 19.9f);
    untyped.set_double(2, 0, 3.0);
    untyped.set_int(0, 1, 20);
    untyped.set_float(1, 1, 20.1f);
    untyped.set_double(2, 1, 4.0);


    // Setup typed table, same contents as untyped
    ThreeColTable typed;
    typed.add(20, 19.9f, 3.0);
    typed.add(20, 20.1f, 4.0);

    // Small typed table test:
    match = (typed.column().second + 100 > 120 && typed.column().first > 2).find_next();
    assert(match == 1);

    // Untyped &&

    // Left condition makes first row non-match
    match = (untyped.column<float>(1) + 1 > 21 && untyped.column<double>(2) > 2).find_next();
    assert(match == 1);

    // Right condition makes first row a non-match
    match = (untyped.column<float>(1) > 10 && untyped.column<double>(2) > 3.5).find_next();
    assert(match == 1);

    // Both make first row match
    match = (untyped.column<float>(1) < 20 && untyped.column<double>(2) > 2).find_next();
    assert(match == 0);

    // Both make first row non-match
    match = (untyped.column<float>(1) > 20 && untyped.column<double>(2) > 3.5).find_next();
    assert(match == 1);

    // Left cond match 0, right match 1
    match = (untyped.column<float>(1) < 20 && untyped.column<double>(2) > 3.5).find_next();
    assert(match == not_found);

    // Left match 1, right match 0
    match = (untyped.column<float>(1) > 20 && untyped.column<double>(2) < 3.5).find_next();
    assert(match == not_found);

    // Untyped ||

    // Left match 0
    match = (untyped.column<float>(1) < 20 || untyped.column<double>(2) < 3.5).find_next();
    assert(match == 0);

    // Right match 0
    match = (untyped.column<float>(1) > 20 || untyped.column<double>(2) < 3.5).find_next();
    assert(match == 0);

    // Left match 1

    match = (untyped.column<float>(1) > 20 || untyped.column<double>(2) > 9.5).find_next();
    
    assert(match == 1);


    




    // Tons of other tests. More in test_query.cpp


    Query q4 = untyped.column<float>(1) + untyped.column<int64_t>(0) > 40;
    Query q5 = 20 < untyped.column<float>(1);

    match = q4.expression(  q5.get_expression()  ).find_next();
    assert(match == 1);


    // Untyped, direct column addressing
    Value<int64_t> uv1(1);

    Columns<float> uc1 = untyped.column<float>(1);

    Query q2 = uv1 <= uc1;
    match = q2.find_next();
    assert(match == 0);


    Query q0 = uv1 <= uc1;
    match = q0.find_next();
    assert(match == 0);

    Query q99 = uv1 <= untyped.column<float>(1);
    match = q99.find_next();
    assert(match == 0);


    Query q8 = 1 > untyped.column<float>(1) + 5;
    match = q8.find_next();
    assert(match == not_found);



    Query q3 = untyped.column<float>(1) + untyped.column<int64_t>(0) > 10 + untyped.column<int64_t>(0);
    match = q3.find_next();

    match = q2.find_next();
    assert(match == 0);    


    // Typed, direct column addressing
    Query q1 = typed.column().second + typed.column().first > 40;
    match = q1.find_next();
    assert(match == 1);   


    match = (typed.column().first + typed.column().second > 40).find_next();
    assert(match == 1);   


    Query tq1 = typed.column().first + typed.column().second >= typed.column().first + typed.column().second;
    match = tq1.find_next();
    assert(match == 0);   


    // Typed, column objects
    Columns<int64_t> t0 = typed.column().first;
    Columns<float> t1 = typed.column().second;

    match = (t0 + t1 > 40).find_next();
    assert(match == 1);

    match = q1.find_next();
    assert(match == 1);   

    match = (untyped.column<int64_t>(0) + untyped.column<float>(1) > 40).find_next();
    assert(match == 1);    

    match = (untyped.column<int64_t>(0) + untyped.column<float>(1) < 40).find_next();
    assert(match == 0);    

    match = (untyped.column<float>(1) <= untyped.column<int64_t>(0)).find_next();
    assert(match == 0);    

    match = (untyped.column<int64_t>(0) + untyped.column<float>(1) >= untyped.column<int64_t>(0) + untyped.column<float>(1)).find_next();
    assert(match == 0);    

    // Untyped, column objects
    Columns<int64_t> u0 = untyped.column<int64_t>(0);
    Columns<float> u1 = untyped.column<float>(1);

    match = (u0 + u1 > 40).find_next();
    assert(match == 1);    
    
    
    // Flexible language binding style
    Subexpr* first = new Columns<int64_t>(0);
    Subexpr* second = new Columns<float>(1);
    Subexpr* third = new Columns<double>(2);
    Subexpr* constant = new Value<int64_t>(40);    
    Subexpr* plus = new Operator<Plus<float> >(*first, *second);  
    Expression *e = new Compare<Greater, float>(*plus, *constant);


    // Bind table and do search
    match = untyped.where().expression(e).find_next();
    assert(match == 1);    

    Query q9 = untyped.where().expression(e);
    match = q9.find_next();
    assert(match == 1);    


    Subexpr* first2 = new Columns<int64_t>(0);
    Subexpr* second2 = new Columns<float>(1);
    Subexpr* third2 = new Columns<double>(2);
    Subexpr* constant2 = new Value<int64_t>(40);    
    Subexpr* plus2 = new Operator<Plus<float> >(*first, *second);  
    Expression *e2 = new Compare<Greater, float>(*plus, *constant);

    match = untyped.where().expression(e).expression(e2).find_next();
    assert(match == 1);    

    Query q10 = untyped.where().expression(q9.get_expression()).expression(e2);
    match = q10.find_next();
    assert(match == 1);    


    Query tq3 = tq1;
    match = tq3.find_next();
    assert(match == 0);   
 
    
    delete e;
    delete plus;
    delete constant;
    delete third;
    delete second;
    delete first;


    delete e2;
    delete plus2;
    delete constant2;
    delete third2;
    delete second2;
    delete first2;
    
    CustomTestReporter reporter;
    TestRunner runner(reporter);
    const int res = runner.RunTestsIf(Test::GetTestList(), 0, True(), 0);

    
    return 0;
}
 

// 638

// 545   568