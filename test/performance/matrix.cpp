#include <algorithm>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>

#include <realm.hpp>

#include "../util/timer.hpp"

//#define ONLY_CN_TESTS
/************************************************************************************

// In Visual Studio, enable this matrix.cpp in compilation and disable test.cpp, else
// compilation will fail (two main() functions).

***********************************************************************************/

using namespace std;
using namespace realm;

namespace {

const size_t row_count = 250112; // should be divisible by 128
const size_t rounds = 1000;

//const size_t row_count = 128*10; // should be divisible by 128
//const size_t rounds = 1;

REALM_TABLE_11(TestTable,
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


test_util::Timer timer;


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
#ifdef REALM_DEBUG
    cout << "Running Debug Build\n";
#else
    cout << "Running Release Build\n";
#endif
    cout << "  Row count: "<<row_count<<"\n";
    cout << "  Rounds:    "<<rounds<<"\n";
    cout << "\n";


#ifndef ONLY_CN_TESTS
    // Realm tests
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

                timer.reset();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count();
                        if (res != expected) {
                            cout << "error\n";
                        }
                    }
                }
                cout << "Realm: Column "<<i<<": Sparse:  "<<timer<<"\n";

                // Search with column intrinsic functions
                timer.reset();
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
                            cout << "error\n";
                        }
                    }
                }
                cout << "Realm: Column "<<i<<": Sparse2: "<<timer<<"\n";
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

                timer.reset();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        const size_t res = q.count();
                        if (res != expected) {
                            cout << "error\n";
                        }
                    }
                }
                cout << "Realm: Column "<<i<<": Many:    "<<timer<<"\n";

                // Search with column intrinsic functions
                timer.reset();
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
                            cout << "error\n";
                        }
                    }
                }
                cout << "Realm: Column "<<i<<": Many2:   "<<timer<<"\n";
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

                timer.reset();
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
                            cout << "error\n";
                        }
                    }
                }
                cout << "Realm: Column "<<i<<": Sum:     "<<timer<<"\n";
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

                timer.reset();
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
                            cout << "error\n";
                        }
                    }
                }
                cout << "Realm: Column "<<i<<": Sum2:    "<<timer<<"\n";
            }
        }

        for (size_t k = 0; k < 2; ++k) {
            const char* const run = k == 0 ? "String" : "Index";

            for (size_t i = 0; i < 3; ++i) {
                // ColumnDirect: Do a search over entire column (sparse, only last value matches)
                {
                    const size_t expected = 1;

                    timer.reset();
                    {
                        for (size_t n = 0; n < rounds; ++n) {
                            size_t res;
                            if      (i == 0) res = table.column().short_str.count("bottom");
                            else if (i == 1) res = table.column().long_str.count("long bottom");
                            else if (i == 2) res = table.column().enum_str.count("saturday");
                            if (res != expected) {
                                cout << "error\n";
                            }
                        }
                    }
                    cout << "Realm: "<<run<<"Column c "<<i<<": Sparse: "<<timer<<"\n";
                }

                // Query: Do a search over entire column (sparse, only last value matches)
                {
                    TestTable::Query q = table.where();
                    if      (i == 0) q.short_str.equal("bottom");
                    else if (i == 1) q.long_str.equal("long bottom");
                    else if (i == 2) q.enum_str.equal("saturday");

                    const size_t expected = 1;

                    timer.reset();
                    {
                        for (size_t n = 0; n < rounds; ++n) {
                            const size_t res = q.count();
                            if (res != expected) {
                                cout << "error\n";
                            }
                        }
                    }
                    cout << "Realm: "<<run<<"Column q "<<i<<": Sparse: "<<timer<<"\n";
                }

                // Do a search over entire column (many matches)
                {
                    TestTable::Query q = table.where();
                    if      (i == 0) q.short_str.not_equal("bottom");
                    else if (i == 1) q.long_str.not_equal("long bottom");
                    else if (i == 2) q.enum_str.not_equal("saturday");

                    const size_t expected = i == 2 ? row_count / 2 : row_count;
                    const size_t len = table.size();

                    timer.reset();
                    {
                        for (size_t n = 0; n < rounds; ++n) {
                            size_t res;
                            if      (i == 0) res = len -table.column().short_str.count("bottom");
                            else if (i == 1) res = len -table.column().long_str.count("long bottom");
                            else if (i == 2) res = table.column().enum_str.count("monday");
                            if (res != expected) {
                                cout << "error\n";
                            }
                        }
                    }
                    cout << "Realm: "<<run<<"Column c "<<i<<": Many:   "<<timer<<"\n";
                }

                // Query: Do a search over entire column (many matches)
                {
                    TestTable::Query q = table.where();
                    if      (i == 0) q.short_str.not_equal("bottom");
                    else if (i == 1) q.long_str.not_equal("long bottom");
                    else if (i == 2) q.enum_str.equal("monday"); // every second entry matches

                    const size_t expected = i == 2 ? row_count / 2 : row_count;

                    timer.reset();
                    {
                        for (size_t n = 0; n < rounds; ++n) {
                            const size_t res = q.count();
                            if (res != expected) {
                                cout << "error\n";
                            }
                        }
                    }
                    cout << "Realm: "<<run<<"Column q "<<i<<": Many:   "<<timer<<"\n";
                }
            }

            // Set index on string columns for next run
            table.column().short_str.set_index();
            table.column().long_str.set_index();
            table.column().enum_str.set_index();
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

                timer.reset();
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
                            cout << "error\n";
                        }
                    }
                }
                cout << "STL: Column "<<i<<": Sparse: "<<timer<<"\n";
            }

            // Do a search over entire column (all matches)
            {
                size_t expected = row_count;
                if (i == 0) ++expected;

                timer.reset();
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
                            cout << "error\n";
                        }
                    }
                }
                cout << "STL: Column "<<i<<": Many:   "<<timer<<"\n";
            }

            // Do a sum over entire column (all matches)
            {
                timer.reset();
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
                            cout << "error\n";
                        }
                    }
                }
                cout << "STL: Column "<<i<<": Sum:    "<<timer<<"\n";
            }
        }

        for (size_t i = 0; i < 3; ++i) {
            // Do a search over entire column (sparse, only last value matches)
            {
                const size_t expected = 1;

                timer.reset();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = count_if(table.begin(), table.end(), match9("bottom"));
                        else if (i == 1) res = count_if(table.begin(), table.end(), match10("long bottom"));
                        else if (i == 2) res = count_if(table.begin(), table.end(), match11("saturday"));

                        if (res != expected) {
                            cout << "error\n";
                        }
                    }
                }
                cout << "STL: StringColumn "<<i<<": Sparse: "<<timer<<"\n";
            }

            // Do a search over entire column (all but last value matches)
            {
                const size_t expected = row_count;

                timer.reset();
                {
                    for (size_t n = 0; n < rounds; ++n) {
                        size_t res;
                        if      (i == 0) res = count_if(table.begin(), table.end(), match9n("bottom"));
                        else if (i == 1) res = count_if(table.begin(), table.end(), match10n("long bottom"));
                        else if (i == 2) res = count_if(table.begin(), table.end(), match11n("saturday"));

                        if (res != expected) {
                            cout << "error\n";
                        }
                    }
                }
                cout << "STL: StringColumn "<<i<<": Many: "<<timer<<"\n";
            }
        }

    }

#endif

    // Realm Multi-column tests
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

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "Realm: c2: "<<timer<<"\n";
        }

        // Search over three columns
        {
            TestTable::Query q = table.where().bits_1.equal(1).bits_2.equal(3).bits_4.equal(15);
            const size_t expected = row_count / 8;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "Realm: c3: "<<timer<<"\n";
        }

        // Search over four columns
        {
            TestTable::Query q = table.where().bits_1.equal(1)
                                              .bits_2.equal(3)
                                              .bits_4.equal(15)
                                              .bits_8.equal(0x7FLL);
            const size_t expected = row_count / 16;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "Realm: c4: "<<timer<<"\n";
        }

        // Search over five columns
        {
            TestTable::Query q = table.where().bits_1.equal(1)
                                              .bits_2.equal(3)
                                              .bits_4.equal(15)
                                              .bits_8.equal(0x7FLL)
                                              .bits_16.equal(0x7FFFLL);
            const size_t expected = row_count / 32;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        cout << "error "<<expected<<" "<<res<<"\n";
                    }
                }
            }
            cout << "Realm: c5: "<<timer<<"\n";
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

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "Realm: c6: "<<timer<<"\n";
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

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = q.count();
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "Realm: c7: "<<timer<<"\n";
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

            const bool    v1 = (i %   2) ? false : true;
            const int     v2 = (i %   4) ? 0 : 0x3;
            const int     v3 = (i %   8) ? 0 : 0xF;
            const int     v4 = (i %  16) ? 0 : 0x7F;
            const int     v5 = (i %  32) ? 0 : 0x7FFF;
            const int     v6 = (i %  64) ? 0 : 0x7FFFFFFF;
            const int64_t v7 = (i % 128) ? 0 : 0x7FFFFFFFFFFFFFFFLL;

            const TestStruct ts = {0, v1, v2, v3, v4, v5, v6, v7, short_str, long_str, enum_str};
            table.push_back(ts);
        }
        const TestStruct ts2 = {0, 0, 0, 0, 0, 0, 0, 0, "bottom", "long bottom", "saturday"};
        table.push_back(ts2);

        // Search over two columns
        {
            const size_t expected = row_count / 4;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns2());
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "STL: c2: "<<timer<<"\n";
        }

        // Search over three columns
        {
            const size_t expected = row_count / 8;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns3());
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "STL: c3: "<<timer<<"\n";
        }

        // Search over four columns
        {
            const size_t expected = row_count / 16;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns4());
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "STL: c4: "<<timer<<"\n";
        }

        // Search over five columns
        {
            const size_t expected = row_count / 32;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns5());
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "STL: c5: "<<timer<<"\n";
        }

        // Search over six columns
        {
            const size_t expected = row_count / 64;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns6());
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "STL: c6: "<<timer<<"\n";
        }

        // Search over seven columns
        {
            const size_t expected = row_count / 128;

            timer.reset();
            {
                for (size_t n = 0; n < rounds; ++n) {
                    const size_t res = count_if(table.begin(), table.end(), columns7());
                    if (res != expected) {
                        cout << "error\n";
                    }
                }
            }
            cout << "STL: c7: "<<timer<<"\n";
        }
    }

    return 0;
}
