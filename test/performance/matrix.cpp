#include <cstdio>
#include <string>
#include <algorithm>
#include <vector>
#include <sstream>

#include <tightdb.hpp>

#include "performance/timer.hpp"

#define ONLY_CN_TESTS
const size_t row_count = 250112; // should be dividable with 128
const size_t rounds = 1000;

//const size_t row_count = 128*10; // should be dividable with 128
//const size_t rounds = 1;

using namespace std;
using namespace tightdb;

namespace {

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

struct Timer {
    void start() { m_start = get_timer_millis(); }
    double get_elapsed_millis() const { return get_timer_millis() - m_start; }
private:
    long m_start;
};

Timer timer;


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
class columns2 {
public:
    columns2() {}
    bool operator()(const TestStruct& v) const {return v.field2 == 1 && v.field3 == 3;}
};
class columns3 {
public:
    columns3() {}
    bool operator()(const TestStruct& v) const {return v.field2 == 1 && v.field3 == 3 && v.field4 == 15;}
};
class columns4 {
public:
    columns4() {}
    bool operator()(const TestStruct& v) const {return v.field2 == 1 && v.field3 == 3 && v.field4 == 15 && v.field5 == 0x7FLL;}
};
class columns5 {
public:
    columns5() {}
    bool operator()(const TestStruct& v) const {return v.field2 == 1 && v.field3 == 3 && v.field4 == 15 && v.field5 == 0x7FLL && v.field6 == 0x7FFFLL;}
};
class columns6 {
public:
    columns6() {}
    bool operator()(const TestStruct& v) const {return v.field2 == 1 && v.field3 == 3 && v.field4 == 15 && v.field5 == 0x7FLL && v.field6 == 0x7FFFLL && v.field7 == 0x7FFFFFFFLL;}
};
class columns7 {
public:
    columns7() {}
    bool operator()(const TestStruct& v) const {return v.field2 == 1 && v.field3 == 3 && v.field4 == 15 && v.field5 == 0x7FLL && v.field6 == 0x7FFFLL && v.field7 == 0x7FFFFFFFLL && v.field8 == 0x7FFFFFFFFFFFFFFFLL;}
};

} // anonymous namespace

int main()
{
#ifdef TIGHTDB_DEBUG
    printf("Running Debug Build\n");
#else
    printf("Running Release Build\n");
#endif
    printf("  Row count: %d\n", (int)row_count);
    printf("  Rounds:    %d\n", (int)rounds);
    printf("\n");


#ifndef ONLY_CN_TESTS        
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
                // Search with query engine
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
                
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count();
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time = timer.get_elapsed_millis();
                printf("TightDB: Column %d: Sparse: %fs\n", (int)i, search_time);
                
                // Search with column intrinsic functions
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = table.column().bits_0.count(1);
                        else if (i == 1) res = table.column().bits_1.count(0);
                        else if (i == 2) res = table.column().bits_2.count(0);
                        else if (i == 3) res = table.column().bits_4.count(0);
                        else if (i == 4) res = table.column().bits_8.count(0);
                        else if (i == 5) res = table.column().bits_16.count(0);
                        else if (i == 6) res = table.column().bits_32.count(0);
                        else if (i == 7) res = table.column().bits_64.count(0);
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time2 = timer.get_elapsed_millis();
                printf("TightDB: Column %d: Sparse2: %fs\n", (int)i, search_time2);
            }
            
            // Do a search over entire column (all matches)
            {
                // Search with query engine
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
                
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count();
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time = timer.get_elapsed_millis();
                printf("TightDB: Column %d: Many:   %fs\n", (int)i, search_time);
                
                // Search with column intrinsic functions
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = table.column().bits_0.count(0);
                        else if (i == 1) res = table.column().bits_1.count(1);
                        else if (i == 2) res = table.column().bits_2.count(3);
                        else if (i == 3) res = table.column().bits_4.count(15);
                        else if (i == 4) res = table.column().bits_8.count(0x7FLL);
                        else if (i == 5) res = table.column().bits_16.count(0x7FFFLL);
                        else if (i == 6) res = table.column().bits_32.count(0x7FFFFFFFLL);
                        else if (i == 7) res = table.column().bits_64.count(0x7FFFFFFFFFFFFFFFLL);
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time2 = timer.get_elapsed_millis();
                printf("TightDB: Column %d: Many2: %fs\n", (int)i, search_time2);
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
                
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = q.bits_0.sum();
                        else if (i == 1) res = q.bits_1.sum();
                        else if (i == 2) res = q.bits_2.sum();
                        else if (i == 3) res = q.bits_4.sum();
                        else if (i == 4) res = q.bits_8.sum();
                        else if (i == 5) res = q.bits_16.sum();
                        else if (i == 6) res = q.bits_32.sum();
                        else if (i == 7) res = q.bits_64.sum();
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time = timer.get_elapsed_millis();
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
                
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        int64_t res;
                        if      (i == 0) res = table.column().bits_0.sum();
                        else if (i == 1) res = table.column().bits_1.sum();
                        else if (i == 2) res = table.column().bits_2.sum();
                        else if (i == 3) res = table.column().bits_4.sum();
                        else if (i == 4) res = table.column().bits_8.sum();
                        else if (i == 5) res = table.column().bits_16.sum();
                        else if (i == 6) res = table.column().bits_32.sum();
                        else if (i == 7) res = table.column().bits_64.sum();
                        
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time = timer.get_elapsed_millis();
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
                
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count();
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time = timer.get_elapsed_millis();
                printf("TightDB: StringColumn %d: Sparse: %fs\n", (int)i, search_time);
            }
            
            // Do a search over entire column (all but last value matches)
            {
                TestTable::Query q = table.where();
                if      (i == 0) q.short_str.not_equal("bottom");
                else if (i == 1) q.long_str.not_equal("long bottom");
                else if (i == 2) q.enum_str.not_equal("saturday");
                
                const size_t expected = row_count;
                
                timer.start();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count();
                        if (res != expected) {
                            printf("error");
                        }
                    }
                }
                const double search_time = timer.get_elapsed_millis();
                printf("TightDB: StringColumn %d: Many: %fs\n", (int)i, search_time);
            }
        }
    }
#endif    

#ifndef ONLY_CN_TESTS        
    // STL tests
    {
        vector<TestStruct> table;
        
        
                
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
                
                timer.start();
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
                const double search_time = timer.get_elapsed_millis();
                printf("STL: Column %d: Sparse: %fs\n", (int)i, search_time);
            }
            
            // Do a search over entire column (all matches)
            {
                size_t expected = row_count;
                if (i == 0) ++expected;
                
                timer.start();
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
                const double search_time = timer.get_elapsed_millis();
                printf("STL: Column %d: Many:   %fs\n", (int)i, search_time);
            }
            
            // Do a sum over entire column (all matches)
            {
                timer.start();
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
                const double search_time = timer.get_elapsed_millis();
                printf("STL: Column %d: Sum:    %fs\n", (int)i, search_time);
            }
        }
        
        for (size_t i = 0; i < 3; ++i) {
            // Do a search over entire column (sparse, only last value matches)
            {
                const size_t expected = 1;
                
                timer.start();
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
                const double search_time = timer.get_elapsed_millis();
                printf("STL: StringColumn %d: Sparse: %fs\n", (int)i, search_time);
            }
            
            // Do a search over entire column (all but last value matches)
            {
                const size_t expected = row_count;
                
                timer.start();
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
                const double search_time = timer.get_elapsed_millis();
                printf("STL: StringColumn %d: Many: %fs\n", (int)i, search_time);
            }
        }
        
    }

#endif

    // TightDB Multi-column tests
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
            
            const int64_t v1 = (i % 2) ? 0 : 1;
            const int64_t v2 = (i % 4) ? 0 : 3;
            const int64_t v3 = (i % 8) ? 0 : 15;
            const int64_t v4 = (i % 16) ? 0 : 0x7FLL;
            const int64_t v5 = (i % 32) ? 0 : 0x7FFFLL;
            const int64_t v6 = (i % 64) ? 0 : 0x7FFFFFFFLL;
            const int64_t v7 = (i % 128) ? 0 : 0x7FFFFFFFFFFFFFFFLL;
            
            table.add(0, v1, v2, v3, v4, v5, v6, v7, short_str.c_str(), long_str.c_str(), enum_str.c_str());
        }
        //table.add(0, 0, 0, 0, 0, 0, 0, 0, "bottom", "long bottom", "saturday");
        
        table.optimize(); // auto-enumerate last string column
        
        // Search over two columns
        {
            TestTable::Query q = table.where().bits_1.equal(1).bits_2.equal(3);
            const size_t expected = row_count / 4;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("TightDB: c2: %fs\n", search_time);
        }
        
        // Search over three columns
        {
            TestTable::Query q = table.where().bits_1.equal(1).bits_2.equal(3).bits_4.equal(15);
            const size_t expected = row_count / 8;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("TightDB: c3: %fs\n", search_time);
        }
        
        // Search over four columns
        {
            TestTable::Query q = table.where().bits_1.equal(1)
                                              .bits_2.equal(3)
                                              .bits_4.equal(15)
                                              .bits_8.equal(0x7FLL);
            const size_t expected = row_count / 16;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("TightDB: c4: %fs\n", search_time);
        }
        
        // Search over five columns
        {
            TestTable::Query q = table.where().bits_1.equal(1)
                                              .bits_2.equal(3)
                                              .bits_4.equal(15)
                                              .bits_8.equal(0x7FLL)
                                              .bits_16.equal(0x7FFFLL);
            const size_t expected = row_count / 32;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        printf("error %d %d", (int)expected, (int)res);
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("TightDB: c5: %fs\n", search_time);
        }
        
        // Search over six columns
        {
            TestTable::Query q = table.where().bits_1.equal(1)
                                              .bits_2.equal(3)
                                              .bits_4.equal(15)
                                              .bits_8.equal(0x7FLL)
                                              .bits_16.equal(0x7FFFLL)
                                              .bits_32.equal(0x7FFFFFFFLL);
            const size_t expected = row_count / 64;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("TightDB: c6: %fs\n", search_time);
        }
        
        // Search over six columns
        {
            TestTable::Query q = table.where().bits_1.equal(1)
                                              .bits_2.equal(3)
                                              .bits_4.equal(15)
                                              .bits_8.equal(0x7FLL)
                                              .bits_16.equal(0x7FFFLL)
                                              .bits_32.equal(0x7FFFFFFFLL)
                                              .bits_64.equal(0x7FFFFFFFFFFFFFFFLL);
            const size_t expected = row_count / 128;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("TightDB: c7: %fs\n", search_time);
        }
    }
    
    // STL Multi-column tests
    {
        vector<TestStruct> table;
        

        
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
            
            const int64_t v1 = (i % 2) ? 0 : 1;
            const int64_t v2 = (i % 4) ? 0 : 3;
            const int64_t v3 = (i % 8) ? 0 : 15;
            const int64_t v4 = (i % 16) ? 0 : 0x7FLL;
            const int64_t v5 = (i % 32) ? 0 : 0x7FFFLL;
            const int64_t v6 = (i % 64) ? 0 : 0x7FFFFFFFLL;
            const int64_t v7 = (i % 128) ? 0 : 0x7FFFFFFFFFFFFFFFLL;
            
            const TestStruct ts = {0, v1, v2, v3, v4, v5, v6, v7, short_str, long_str, enum_str};
            table.push_back(ts);
        }
        const TestStruct ts2 = {0, 0, 0, 0, 0, 0, 0, 0, "bottom", "long bottom", "saturday"};
        table.push_back(ts2);
        
        // Search over two columns
        {
            const size_t expected = row_count / 4;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns2());
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("STL: c2: %fs\n", search_time);
        }
        
        // Search over three columns
        {
            const size_t expected = row_count / 8;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns3());
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("STL: c3: %fs\n", search_time);
        }
        
        // Search over four columns
        {
            const size_t expected = row_count / 16;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns4());
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("STL: c4: %fs\n", search_time);
        }
        
        // Search over five columns
        {
            const size_t expected = row_count / 32;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns5());
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("STL: c5: %fs\n", search_time);
        }
        
        // Search over six columns
        {
            const size_t expected = row_count / 64;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns6());
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("STL: c6: %fs\n", search_time);
        }
        
        // Search over seven columns
        {
            const size_t expected = row_count / 128;
            
            timer.start();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns7());
                    if (res != expected) {
                        printf("error");
                    }
                }
            }
            const double search_time = timer.get_elapsed_millis();
            printf("STL: c7: %fs\n", search_time);
        }
    }

    return 0;
}
