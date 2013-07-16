#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/column.hpp>
#include <tightdb/utilities.hpp>

//#define USE_VLD
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


struct plus2 { int64_t operator()(int64_t a, int64_t b) {return a + b;}; };
struct minus2 { int64_t operator()(int64_t a, int64_t b) {return a - b;}; };

struct quad
{
    quad() {};

    int64_t ma;
    int64_t mb;
    int64_t mc;
    int64_t md;
    int64_t me;
    int64_t mf;
    int64_t mg;
    int64_t mh;

    template <class TOperator> TIGHTDB_FORCEINLINE void fun(quad& q1, quad& q2) {
        TOperator o;
        ma = o(q1.ma, q2.ma);
        mb = o(q1.mb, q2.mb);
        mc = o(q1.mc, q2.mc);
        md = o(q1.md, q2.md);
        me = o(q1.me, q2.me);
        mf = o(q1.mf, q2.mf);
        mg = o(q1.mg, q2.mg);
        mh = o(q1.mh, q2.mh);
    }

    TIGHTDB_FORCEINLINE void fetch(Array& arr, size_t i) {
        ma = arr.Get(i + 0);
        mb = arr.Get(i + 1);
        mc = arr.Get(i + 2);
        md = arr.Get(i + 3);
        me = arr.Get(i + 4);
        mf = arr.Get(i + 5);
        mg = arr.Get(i + 6);
        mh = arr.Get(i + 7);
    }

};

struct CExpression
{
    TIGHTDB_FORCEINLINE virtual void evaluate(size_t i, quad& result) = 0;
};

struct CConst
{
};

template <int64_t v> struct CConstant : public CExpression, public CConst
{
    TIGHTDB_FORCEINLINE void evaluate(size_t i, quad& result) {
        result.ma = v;
        result.mb = v;
        result.mc = v;
        result.md = v;
        result.me = v;
        result.mf = v;
        result.mg = v;
        result.mh = v;
    }

};


struct CDynConstant : public CExpression, public CConst
{
    CDynConstant(int64_t v) {
        mq.ma = v;
        mq.mb = v;
        mq.mc = v;
        mq.md = v;
        mq.me = v;
        mq.mf = v;
        mq.mg = v;
        mq.mh = v;
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, quad& result) {
        result = mq;
    }
    int64_t c;
    quad mq;
};


struct CColumn : public CExpression
{
    TIGHTDB_FORCEINLINE void evaluate(size_t i, quad& result) {
        result.fetch(*arr, i);
    }
    Array* arr;
};




template <class oper, class TLeft = CExpression, class TRight = CExpression> struct COperator : public CExpression
{
    COperator(TLeft* left, TRight* right) {
        m_left = left;
        m_right = right;
    };

    TIGHTDB_FORCEINLINE void evaluate(size_t i, quad& result) {
        quad q;

        if(SameType<TLeft, CDynConstant>::value && SameType<TRight, CColumn>::value) {
            reinterpret_cast<CColumn*>(m_right)->CColumn::evaluate(i, result); 
            result.fun<oper>(reinterpret_cast<CDynConstant*>(m_left)->CDynConstant::mq, result);
        }
        else if(SameType<TLeft, CColumn>::value && SameType<TRight, CDynConstant>::value) {
            reinterpret_cast<CColumn*>(m_left)->CColumn::evaluate(i, result); 
            result.fun<oper>(reinterpret_cast<CDynConstant*>(m_right)->CDynConstant::mq, result);
        }
        else if(SameType<TLeft, CDynConstant>::value && SameType<TRight, CDynConstant>::value) {
            result.fun<oper>(reinterpret_cast<CDynConstant*>(m_right)->CDynConstant::mq, reinterpret_cast<CDynConstant*>(m_left)->CDynConstant::mq);
        }
        else {
            quad q1;
            quad q2;
            m_right->TRight::evaluate(i, q1);
            m_left->TLeft::evaluate(i, q2); 
            result.fun<oper>(q1, q2);
        }
    }

    TLeft* m_left;
    TRight* m_right;
};




struct CCompareBase
{
    virtual size_t Compare(CExpression* e1, CExpression* e2) = 0;
};


template <class TCond> struct CCompare : public CCompareBase
{
    TCond c;
    size_t Compare(CExpression* e1, CExpression* e2) {
        for(size_t i = 0; i < 1010 - 8; i += 8) {
            quad q1;
            quad q2;
            e1->evaluate(i, q1);
            e2->evaluate(i, q2);
            bool match = c(q1.ma, q2.ma) || c(q1.mb, q2.mb) || c(q1.mc, q2.mc) || c(q1.md, q2.md) || c(q1.me, q2.me) || c(q1.mf, q2.mf) || c(q1.mg, q2.mg) || c(q1.mh, q2.mh);
            if(match)
                return i;
        }
        return -1;
    }
};




int main(int argc, char* argv[])
{
        bool volatile decide = true;

        Array a;

        for(size_t t = 0; t < 1010; t++)
            a.add(rand() % 10);

        a.Set(999, 30);

        CColumn* col = new CColumn;
        col->arr = &a;

        CDynConstant* cc1 = new CDynConstant(20);

        CExpression* cc2 = new CDynConstant(50);

        CCompareBase *e = new CCompare<Equal>();


//        CExpression* ck = new COperator<plus2>(cc1, col);



  //      CExpression* ck = new COperator<plus2, CDynConstant, CColumn>(cc1, col);          //





   //     CExpression* ck = new COperator<plus2, CColumn, CDynConstant>(col, cc1);           // slow
        CExpression* ck = new COperator<plus2>(col, cc1);           // slow



        {
            volatile size_t m;
            unsigned int best = -1;
            for(int y = 0; y < 40; y++)
            {
                UnitTest::Timer timer;
                timer.Start();
                for(int i = 0; i < 100000; i++)
                    m = e->Compare(ck, cc2);
                
                int t = timer.GetTimeInMs() ;
                if (t < best)
                    best = t;

            }
            cerr << best << "  " << m << "\n";
        }






        {
            volatile size_t m;
            unsigned int best = -1;
            for(int y = 0; y < 3; y++)
            {
                UnitTest::Timer timer;
                timer.Start();
                for(int i = 0; i < 100000; i++) {
                    for(size_t t = 0; t < 1010; t += 8) {
                        if(a.Get(t) + 20 == 50 || a.Get(t + 1) + 20 == 50 || a.Get(t + 2) + 20 == 50 || a.Get(t + 3) + 20 == 50 ||
                           a.Get(t + 4) + 20 == 50 || a.Get(t + 5) + 20 == 50 || a.Get(t + 6) + 20 == 50 || a.Get(t + 7) + 20 == 50) {
                            m = t;
                            break;
                        }
                    }
                }
                int t = timer.GetTimeInMs() ;
                if (t < best)
                    best = t;

            }
            cerr << best << "  " << m << "\n";
        }


        
        return 0;
}
 