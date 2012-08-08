#include <stdio.h>
#include "tightdb.hpp"

#ifndef _MSC_VER
#include "timer.h"
#endif

#include <assert.h>

#ifndef _MSC_VER
#include "../Support/mem.hpp"
#include "../Support/number_names.hpp"
#else
#include <UnitTest++.h>
#endif

#include <string>
#include <algorithm>
#include <sstream>

using namespace std;
using namespace tightdb;

TIGHTDB_TABLE_11(TestTable,
                bits_0,    Int,
                bits_1,    Int,
                bits_2,    Int,
                bits_4,    Int,
                bits_8,    Int,
                bits_16,   Int,
                bits_32,   Int,
                bits_64,   Int,
                short_str, String,
                long_str,  String,
                enum_str,  String)

int main()
{
    const size_t row_count = 250000;
    const size_t rounds = 100;
#ifndef _MSC_VER
    Timer timer;
#else
    UnitTest::Timer timer;
#endif
    
#ifdef _DEBUG
    printf("Running Debug Build\n");
#else
    printf("Running Release Build\n");
#endif
    printf("  Row count: %d\n", (int)row_count);
    printf("  Rounds:    %d\n", (int)rounds);
    printf("\n");

    // TightDB tests
    {
        TestTable table;

        // Build large table
        for (size_t i = 0; i < row_count; ++i) {
            std::stringstream ss;
            
            // Create short unique string
            ss << "s" << i;
            const string short_str = ss.str();
            
            // Create long unique string
            ss << " very long string...............";
            const string long_str = ss.str();
            
            // Create strings that can be auto-enumerated
            const string enum_str = (i % 2) ? "monday" : "tuesday";

            table.add(0, 1, 3, 15, 0x7FLL, 0x7FFFLL, 0x7FFFFFFFLL, 0x7FFFFFFFFFFFFFFFLL, short_str.c_str(), long_str.c_str(), enum_str.c_str());
        }
        table.add(0, 0, 0, 0, 0, 0, 0, 0, "bottom", "long bottom", "saturday");
        
        table.optimize(); // auto-enumerate last string column

        
        // Search over integer columns
        for (size_t i = 0; i < 8; ++i) {
            // Do a search over entire column (sparse, only last value matches)
            {
                TestTable::Query q = table.where();
                if      (i == 0) q.bits_0.equal(1);
                else if (i == 1) q.bits_1.equal(0);
                else if (i == 2) q.bits_2.equal(0);
                else if (i == 3) q.bits_4.equal(0);
                else if (i == 4) q.bits_8.equal(0);
                else if (i == 5) q.bits_16.equal(0);
                else if (i == 6) q.bits_32.equal(0);
                else if (i == 7) q.bits_64.equal(0);
                
                const size_t expected = (i == 0) ? 0 : 1;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count(table);
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("TightDB: Column %d: Sparse: %fs\n", (int)i, search_time);
            }
            
            // Do a search over entire column (all matches)
            {
                TestTable::Query q = table.where();
                if      (i == 0) q.bits_0.equal(0);
                else if (i == 1) q.bits_1.equal(1);
                else if (i == 2) q.bits_2.equal(3);
                else if (i == 3) q.bits_4.equal(15);
                else if (i == 4) q.bits_8.equal(0x7FLL);
                else if (i == 5) q.bits_16.equal(0x7FFFLL);
                else if (i == 6) q.bits_32.equal(0x7FFFFFFFLL);
                else if (i == 7) q.bits_64.equal(0x7FFFFFFFFFFFFFFFLL);
                
                size_t expected = row_count;
                if (i == 0) ++expected;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count(table);
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("TightDB: Column %d: Many:   %fs\n", (int)i, search_time);
            }
            
            // Do a sum over entire column (all matches)
            {
                TestTable::Query q = table.where();
                size_t expected;
                if      (i == 0) expected = 0;
                else if (i == 1) expected = row_count * 1;
                else if (i == 2) expected = row_count * 3;
                else if (i == 3) expected = row_count * 15;
                else if (i == 4) expected = row_count * 0x7FLL;
                else if (i == 5) expected = row_count * 0x7FFFLL;
                else if (i == 6) expected = row_count * 0x7FFFFFFFLL;
                else if (i == 7) expected = row_count * 0x7FFFFFFFFFFFFFFFLL;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = q.bits_0.sum(table);
                        else if (i == 1) res = q.bits_1.sum(table);
                        else if (i == 2) res = q.bits_2.sum(table);
                        else if (i == 3) res = q.bits_4.sum(table);
                        else if (i == 4) res = q.bits_8.sum(table);
                        else if (i == 5) res = q.bits_16.sum(table);
                        else if (i == 6) res = q.bits_32.sum(table);
                        else if (i == 7) res = q.bits_64.sum(table);
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("TightDB: Column %d: Sum:    %fs\n", (int)i, search_time);
            }
            
            // Do a sum over entire column (all matches)
            {
                int64_t expected;
                if      (i == 0) expected = 0;
                else if (i == 1) expected = row_count * 1;
                else if (i == 2) expected = row_count * 3;
                else if (i == 3) expected = row_count * 15;
                else if (i == 4) expected = row_count * 0x7FLL;
                else if (i == 5) expected = row_count * 0x7FFFLL;
                else if (i == 6) expected = row_count * 0x7FFFFFFFLL;
                else if (i == 7) expected = row_count * 0x7FFFFFFFFFFFFFFFLL;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        int64_t res;
                        if      (i == 0) res = table.cols().bits_0.sum();
                        else if (i == 1) res = table.cols().bits_1.sum();
                        else if (i == 2) res = table.cols().bits_2.sum();
                        else if (i == 3) res = table.cols().bits_4.sum();
                        else if (i == 4) res = table.cols().bits_8.sum();
                        else if (i == 5) res = table.cols().bits_16.sum();
                        else if (i == 6) res = table.cols().bits_32.sum();
                        else if (i == 7) res = table.cols().bits_64.sum();
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("TightDB: Column %d: Sum2:   %fs\n", (int)i, search_time);
            }
        }
        
        for (size_t i = 0; i < 3; ++i) {
            // Do a search over entire column (sparse, only last value matches)
            {
                TestTable::Query q = table.where();
                if      (i == 0) q.short_str.equal("bottom");
                else if (i == 1) q.long_str.equal("long bottom");
                else if (i == 2) q.enum_str.equal("saturday");
                
                const size_t expected = 1;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count(table);
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("TightDB: StringColumn %d: Sparse: %fs\n", (int)i, search_time);
            }
            
            // Do a search over entire column (all but last value matches)
            {
                TestTable::Query q = table.where();
                if      (i == 0) q.short_str.not_equal("bottom");
                else if (i == 1) q.long_str.not_equal("long bottom");
                else if (i == 2) q.enum_str.not_equal("saturday");
                
                const size_t expected = row_count;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count(table);
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("TightDB: StringColumn %d: Many: %fs\n", (int)i, search_time);
            }
        }
    }
    
    // STL tests
    {
        struct TestStruct {
            bool    field1;
            bool    field2;
            int     field3;
            int     field4;
            int     field5;
            int     field6;
            int     field7;
            int64_t field8;
            string  field9;
            string  field10;
            string  field11;
        };
        
        vector<TestStruct> table;
        
        class match1 {
        public:
            match1(bool target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field1 == m_target;}
        private:
            const bool m_target;
        };
        class match2 {
        public:
            match2(bool target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field2 == m_target;}
        private:
            const bool m_target;
        };
        class match3 {
        public:
            match3(int target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field3 == m_target;}
        private:
            const int m_target;
        };
        class match4 {
        public:
            match4(int target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field4 == m_target;}
        private:
            const int m_target;
        };
        class match5 {
        public:
            match5(int target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field5 == m_target;}
        private:
            const int m_target;
        };
        class match6 {
        public:
            match6(int target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field6 == m_target;}
        private:
            const int m_target;
        };
        class match7 {
        public:
            match7(int target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field7 == m_target;}
        private:
            const int m_target;
        };
        class match8 {
        public:
            match8(int64_t target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field8 == m_target;}
        private:
            const int64_t m_target;
        };
        class match9 {
        public:
            match9(const string& target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field9 == m_target;}
        private:
            const string& m_target;
        };
        class match10 {
        public:
            match10(const string& target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field10 == m_target;}
        private:
            const string& m_target;
        };
        class match11 {
        public:
            match11(const string& target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field11 == m_target;}
        private:
            const string& m_target;
        };
        class match9n {
        public:
            match9n(const string& target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field9 != m_target;}
        private:
            const string& m_target;
        };
        class match10n {
        public:
            match10n(const string& target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field10 != m_target;}
        private:
            const string& m_target;
        };
        class match11n {
        public:
            match11n(const string& target) : m_target(target) {}
            bool operator()(const TestStruct& v) const {return v.field11 != m_target;}
        private:
            const string& m_target;
        };
        
                
        // Build large table
        for (size_t i = 0; i < row_count; ++i) {
            std::stringstream ss;
            
            // Create short unique string
            ss << "s" << i;
            const string short_str = ss.str();
            
            // Create long unique string
            ss << " very long string...............";
            const string long_str = ss.str();
            
            // Create strings that can be auto-enumerated
            const string enum_str = (i % 2) ? "monday" : "tuesday";
            
            const TestStruct ts = {0, 1, 3, 15, 0x7FLL, 0x7FFFLL, 0x7FFFFFFFLL, 0x7FFFFFFFFFFFFFFFLL, short_str, long_str, enum_str};
            table.push_back(ts);
        }
        const TestStruct ts2 = {0, 0, 0, 0, 0, 0, 0, 0, "bottom", "long bottom", "saturday"};
        table.push_back(ts2);
        
        // Search over integer columns
        for (size_t i = 0; i < 8; ++i) {
            // Do a search over entire column (sparse, only last value matches)
            {
                const size_t expected = (i == 0) ? 0 : 1;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = count_if(table.begin(), table.end(), match1(true));
                        else if (i == 1) res = count_if(table.begin(), table.end(), match2(false));
                        else if (i == 2) res = count_if(table.begin(), table.end(), match3(0));
                        else if (i == 3) res = count_if(table.begin(), table.end(), match4(0));
                        else if (i == 4) res = count_if(table.begin(), table.end(), match5(0));
                        else if (i == 5) res = count_if(table.begin(), table.end(), match6(0));
                        else if (i == 6) res = count_if(table.begin(), table.end(), match7(0));
                        else if (i == 7) res = count_if(table.begin(), table.end(), match8(0));
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("STL: Column %d: Sparse: %fs\n", (int)i, search_time);
            }
            
            // Do a search over entire column (all matches)
            {
                size_t expected = row_count;
                if (i == 0) ++expected;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = count_if(table.begin(), table.end(), match1(false));
                        else if (i == 1) res = count_if(table.begin(), table.end(), match2(true));
                        else if (i == 2) res = count_if(table.begin(), table.end(), match3(3));
                        else if (i == 3) res = count_if(table.begin(), table.end(), match4(15));
                        else if (i == 4) res = count_if(table.begin(), table.end(), match5(0x7FLL));
                        else if (i == 5) res = count_if(table.begin(), table.end(), match6(0x7FFFLL));
                        else if (i == 6) res = count_if(table.begin(), table.end(), match7(0x7FFFFFFFLL));
                        else if (i == 7) res = count_if(table.begin(), table.end(), match8(0x7FFFFFFFFFFFFFFFLL));
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("STL: Column %d: Many:   %fs\n", (int)i, search_time);
            }
            
            // Do a sum over entire column (all matches)
            {
	       	timer.Start();
                for (size_t n = 0; n < rounds; ++n) {
                    int64_t expected;
                    if      (i == 0) expected = 0;
                    else if (i == 1) expected = row_count * 1;
                    else if (i == 2) expected = row_count * 3;
                    else if (i == 3) expected = row_count * 15;
                    else if (i == 4) expected = row_count * 0x7FLL;
                    else if (i == 5) expected = row_count * 0x7FFFLL;
                    else if (i == 6) expected = row_count * 0x7FFFFFFFLL;
                    else if (i == 7) expected = row_count * 0x7FFFFFFFFFFFFFFFLL;
                    {
                        int64_t res = 0;
                        if (i == 0) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += (int)p->field1;
                            }
                        }
                        else if (i == 1) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += (int)p->field2;
                            }
                        }
                        else if (i == 2) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += p->field3;
                            }
                        }
                        else if (i == 3) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += p->field4;
                            }
                        }
                        else if (i == 4) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += p->field5;
                            }
                        }
                        else if (i == 5) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += p->field6;
                            }
                        }
                        else if (i == 6) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += p->field7;
                            }
                        }
                        else if (i == 7) {
                            for (vector<TestStruct>::const_iterator p = table.begin(); p != table.end(); ++p) {
                                res += p->field8;
                            }
                        }
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("STL: Column %d: Sum:    %fs\n", (int)i, search_time);
            }
        }
        
        for (size_t i = 0; i < 3; ++i) {
            // Do a search over entire column (sparse, only last value matches)
            {
                const size_t expected = 1;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = count_if(table.begin(), table.end(), match9("bottom"));
                        else if (i == 1) res = count_if(table.begin(), table.end(), match10("long bottom"));
                        else if (i == 2) res = count_if(table.begin(), table.end(), match11("saturday"));
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("STL: StringColumn %d: Sparse: %fs\n", (int)i, search_time);
            }
            
            // Do a search over entire column (all but last value matches)
            {
                const size_t expected = row_count;
                
                timer.Start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = count_if(table.begin(), table.end(), match9n("bottom"));
                        else if (i == 1) res = count_if(table.begin(), table.end(), match10n("long bottom"));
                        else if (i == 2) res = count_if(table.begin(), table.end(), match11n("saturday"));
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
#ifndef _MSC_VER
                const double search_time = timer.GetTime();
#else
                const double search_time = timer.GetTimeInMs() / 1000.0;
#endif
                printf("STL: StringColumn %d: Many: %fs\n", (int)i, search_time);
            }
        }
        
    }

    return 0;
}
