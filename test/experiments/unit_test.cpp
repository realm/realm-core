#include <cstdlib>
#include <typeinfo>
#include <exception>
#include <vector>
#include <string>
#include <iostream>

#include <unistd.h>

#if __GNUC__ > 3 || __GNUC__ == 3 && __GNUC_MINOR__ >= 2
#  define TIGHTDB_HAVE_CXXABI_DEMANGLE
#  include <cxxabi.h>
#endif

#include <tightdb/util/thread.hpp>

#include "../util/timer.hpp"
#include "unit_test.hpp"

using namespace std;
using namespace tightdb;


namespace {

struct Test {
    const char* m_file;
    long m_line;
    const char* m_name;
    void (*m_func)();
    Test(const char* file, long line, const char* name, void (*func)()):
        m_file(file), m_line(line), m_name(name), m_func(func) {}
};

struct Registry {
    Mutex m_mutex;
    vector<Test> m_tests;
    Test* m_current_test;
    bool m_errors_seen;
    long m_checks_failed;
    long m_checks_completed;
    Registry():
        m_current_test(0), m_errors_seen(false),
        m_checks_failed(0), m_checks_completed(0) {}
};

Registry& get_registry()
{
    static Registry reg;
    return reg;
}

void emit(ostream& out, const char* file, long line, string message)
{
    out << file << ":" << line << ": " << message << "\n";
}

void emit(ostream& out, Test& test, string message)
{
    emit(out, test.m_file, test.m_line,  message);
}

void check_failed(const char* file, long line, const string& message)
{
    Registry& reg = get_registry();
    Mutex::Lock lock(reg.m_mutex);
    Test& test = *reg.m_current_test;
    string name = test.m_name;
    emit(cerr, file, line, "ERROR in " + name + ": " + message);
    reg.m_errors_seen = true;
    ++reg.m_checks_failed;
    ++reg.m_checks_completed;
}

// See http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/namespaceabi.html
//
// FIXME: Could use the Autoconf macro 'ax_cxx_gcc_abi_demangle'. See
// http://autoconf-archive.cryp.to.
string demangle(string n)
{
#ifdef TIGHTDB_HAVE_CXXABI_DEMANGLE
    int s = 0;
    char *r = abi::__cxa_demangle(n.c_str(), 0, 0, &s);
    if(!r) return n;
    string m = r;
    free(r);
    return m;
#else
    return n;
#endif
}

template<class T> inline string rtti_name()
{
    return demangle(typeid(T).name());
}

template<typename T> inline string rtti_name(T const &v)
{
    return demangle(typeid(v).name());
}

} // anonymous namespace



namespace tightdb {
namespace unit_test {

void register_test(const char* file, long line, const char* name, void (*func)())
{
    Registry& reg = get_registry();
    Mutex::Lock lock(reg.m_mutex);
    reg.m_tests.push_back(Test(file, line, name, func));
}

void check_succeeded()
{
    Registry& reg = get_registry();
    Mutex::Lock lock(reg.m_mutex);
    ++reg.m_checks_completed;
}

void cond_failed(const char* file, long line, const char* cond_text)
{
    string msg = "CHECK("+string(cond_text)+") failed";
    check_failed(file, line, msg);
}

void equal_failed(const char* file, long line, const char* a_text, const char* b_text,
                  const string& a_val, const string& b_val)
{
    string msg = "CHECK_EQUAL("+string(a_text)+", "+b_text+") failed with ("+a_val+", "+b_val+")";
    check_failed(file, line, msg);
}

void throw_failed(const char* file, long line, const char* expr_text, const char* exception)
{
    string msg = "CHECK_THROW("+std::string(expr_text)+") failed: Expected exception "+exception;
    check_failed(file, line, msg);
}


} // namespace unit_test
} // namespace tightdb



int main()
{
    // FIXME: Add meta unit test to test that all checks work - it is
    // important to trigger all variations of the implementation of
    // the checks.
    // FIXME: Timing
    // FIXME: Run unit test by name
    // FIXME: Write quoted strings with escaped nonprintables
    // FIXME: Multi-threaded
    // FIXME: Fixtures maybe?

    test_util::Timer timer;
    Registry& reg = get_registry();
    size_t num_tests = reg.m_tests.size();
    size_t num_failed_tests = 0;
    for (size_t i = 0; i < num_tests; ++i) {
        reg.m_errors_seen = false;
        Test& test = reg.m_tests[i];
        reg.m_current_test = &test;
        try {
            emit(cout, test, "Running "+string(test.m_name));
            (*test.m_func)();
        }
        catch (exception& ex) {
            reg.m_errors_seen = true;
            emit(cerr, test, "ERROR in "+string(test.m_name)+": "
                 "Unhandled exception "+rtti_name(ex)+": "+ex.what());
        }
        catch (...) {
            reg.m_errors_seen = true;
            emit(cerr, test, "ERROR in "+string(test.m_name)+": "
                 "Unhandled exception of unknown type");
        }
        if (reg.m_errors_seen)
            ++num_failed_tests;
    }
    if (num_failed_tests == 0) {
        cout << "Success: "<<num_tests<<" tests passed "
            "("<<reg.m_checks_completed<<" checks).\n";
    }
    else {
        cerr << "FAILURE: "<<num_failed_tests<<" out of "<<num_tests<<" tests failed "
            "("<<reg.m_checks_failed<<" failures out of "<<reg.m_checks_completed<<" checks).\n";
    }
    cout << "Test time: "<<timer<<"\n";
    return num_failed_tests == 0 ? 0 : 1;
}
