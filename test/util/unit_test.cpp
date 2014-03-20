#include <exception>
#include <vector>
#include <string>
#include <iostream>

#include <tightdb/util/thread.hpp>

#include "demangle.hpp"
#include "timer.hpp"
#include "wildcard.hpp"
#include "unit_test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;
using namespace tightdb::test_util::unit_test;



// Threading: Need a job-scheduler based on a thread pool as opposed
// to a process pool.

// Feedback from checks must be associated with current test. This
// must be based either on thread-specific-data or on explicit
// reference from check-macros to a variable in scope.

// FIXME: Add meta unit test to test that all checks work - it is
// important to trigger all variations of the implementation of the
// checks.

// FIXME: Think about order of tests during execution.
// FIXME: Write quoted strings with escaped nonprintables
// FIXME: Multi-threaded



namespace {

class TempUnlockGuard {
public:
    TempUnlockGuard(UniqueLock& lock) TIGHTDB_NOEXCEPT:
        m_lock(lock)
    {
        m_lock.unlock();
    }

    ~TempUnlockGuard() TIGHTDB_NOEXCEPT
    {
        m_lock.lock();
    }

private:
    UniqueLock& m_lock;
};

struct Test {
    Location m_loc;
    void (*m_func)();
    Test(const char* file, long line, const char* name, void (*func)())
    {
        m_loc.test_name   = name;
        m_loc.file_name   = file;
        m_loc.line_number = line;
        m_func = func;
    }
};

struct Registry {
    Mutex m_mutex;
    vector<Test> m_tests;
    Test* m_current_test;
    bool m_errors_seen;
    long long m_checks_failed;
    long long m_checks_completed;
    Reporter* m_reporter;
    Registry():
        m_current_test(0),
        m_errors_seen(false),
        m_checks_failed(0),
        m_checks_completed(0),
        m_reporter(0)
    {
    }
};

Registry& get_registry()
{
    static Registry reg;
    return reg;
}

void check_failed(const char* file, long line, const string& message)
{
    Registry& reg = get_registry();
    LockGuard lock(reg.m_mutex);
    Test& test = *reg.m_current_test;
    if (reg.m_reporter) {
        Location loc = test.m_loc;
        loc.file_name   = file;
        loc.line_number = line;
        reg.m_reporter->fail(loc, message);
    }
    reg.m_errors_seen = true;
    ++reg.m_checks_failed;
    ++reg.m_checks_completed;
}


void replace_char(string& str, char c, const string& replacement)
{
    for (size_t pos = str.find(c); pos != string::npos; pos = str.find(c, pos + 1))
        str.replace(pos, 1, replacement);
}

string xml_escape(const string& value)
{
    string value_2 = value;
    replace_char(value_2, '&',  "&amp;");
    replace_char(value_2, '<',  "&lt;");
    replace_char(value_2, '>',  "&gt;");
    replace_char(value_2, '\'', "&apos;");
    replace_char(value_2, '\"', "&quot;");
    return value_2;
}

class XmlReporter: public Reporter {
public:
    XmlReporter(ostream& out):
        m_out(out)
    {
    }

    ~XmlReporter() TIGHTDB_NOEXCEPT
    {
    }

    void begin(const Location& loc) TIGHTDB_OVERRIDE
    {
        test t;
        t.m_loc = loc;
        m_tests.push_back(t);
    }

    void fail(const Location& loc, const string& message) TIGHTDB_OVERRIDE
    {
        failure f;
        f.m_loc     = loc;
        f.m_message = message;
        m_tests.back().m_failures.push_back(f);
    }

    void end(const Location&, double elapsed_seconds) TIGHTDB_OVERRIDE
    {
        m_tests.back().m_elapsed_seconds = elapsed_seconds;
    }

    void summary(const Summary& summary) TIGHTDB_OVERRIDE
    {
        m_out <<
            "<?xml version=\"1.0\"?>\n"
            "<unittest-results "
            "tests=\"" << summary.num_included_tests << "\" "
            "failedtests=\"" << summary.num_failed_tests << "\" "
            "checks=\"" << summary.num_checks << "\" "
            "failures=\"" << summary.num_failed_checks << "\" "
            "time=\"" << summary.elapsed_seconds << "\">\n";
        typedef vector<test>::const_iterator test_iter;
        test_iter tests_end = m_tests.end();
        for (test_iter i_1 = m_tests.begin(); i_1 != tests_end; ++i_1) {
            m_out <<
                "  <test suite=\"default\" "
                "name=\"" << i_1->m_loc.test_name << "\" "
                "time=\"" << i_1->m_elapsed_seconds << "\"";
            if (i_1->m_failures.empty()) {
                m_out << "/>\n";
                continue;
            }
            m_out << ">\n";
            typedef vector<failure>::const_iterator fail_iter;
            fail_iter fails_end = i_1->m_failures.end();
            for (fail_iter i_2 = i_1->m_failures.begin(); i_2 != fails_end; ++i_2) {
                string msg = xml_escape(i_2->m_message);
                m_out << "    <failure message=\"" << i_2->m_loc.file_name << ""
                    "(" << i_2->m_loc.line_number << ") : " << msg << "\"/>\n";
            }
            m_out << "  </test>\n";
        }
        m_out <<
            "</unittest-results>\n";
    }

protected:
    struct failure {
        Location m_loc;
        string m_message;
    };

    struct test {
        Location m_loc;
        vector<failure> m_failures;
        double m_elapsed_seconds;
    };

    vector<test> m_tests;

    ostream& m_out;
};


class WildcardFilter: public Filter {
public:
    WildcardFilter(const std::string& filter)
    {
        bool exclude = false;
        typedef string::const_iterator iter;
        iter i = filter.begin(), end = filter.end();
        for (;;) {
            // Skip space
            while (i != end) {
                if (*i != ' ')
                    break;
                ++i;
            }

            // End of input?
            if (i == end)
                break;

            iter word_begin = i;

            // Find end of word
            while (i != end) {
                if (*i == ' ')
                    break;
                ++i;
            }

            iter word_end = i;
            size_t word_size = word_end - word_begin;
            if (word_size == 1 && *word_begin == '-') {
                exclude = true;
                continue;
            }

            string word(word_begin, word_end);
            patterns& p = exclude ? m_exclude : m_include;
            p.push_back(wildcard_pattern(word));
        }

        // Include everything if no includes are specified.
        if (m_include.empty())
            m_include.push_back(wildcard_pattern("*"));
    }

    ~WildcardFilter() TIGHTDB_NOEXCEPT
    {
    }

    bool include(const Location& loc) TIGHTDB_OVERRIDE
    {
        const char* name = loc.test_name;
        typedef patterns::const_iterator iter;

        // Say "no" if it matches an exclude pattern
        {
            iter end = m_exclude.end();
            for (iter i = m_exclude.begin(); i != end; ++i) {
                if (i->match(name))
                    return false;
            }
        }

        // Say "yes" if it matches an include pattern
        {
            iter end = m_include.end();
            for (iter i = m_include.begin(); i != end; ++i) {
                if (i->match(name))
                    return true;
            }
        }

        // Not included
        return false;
    }

private:
    typedef vector<wildcard_pattern> patterns;
    patterns m_include, m_exclude;
};


} // anonymous namespace



namespace tightdb {
namespace test_util {
namespace unit_test {


RegisterTest::RegisterTest(const char* file, long line, const char* name, void (*func)())
{
    Registry& reg = get_registry();
    LockGuard lock(reg.m_mutex);
    reg.m_tests.push_back(Test(file, line, name, func));
}

void check_succeeded()
{
    Registry& reg = get_registry();
    LockGuard lock(reg.m_mutex);
    ++reg.m_checks_completed;
}

void cond_failed(const char* file, long line, const char* cond_text)
{
    string msg = "CHECK("+string(cond_text)+") failed";
    check_failed(file, line, msg);
}

void compare_failed(const char* file, long line, const char* macro_name,
                    const char* a_text, const char* b_text,
                    const string& a_val, const string& b_val)
{
    string msg = string(macro_name)+"("+a_text+", "+b_text+") failed with ("+a_val+", "+b_val+")";
    check_failed(file, line, msg);
}

void throw_failed(const char* file, long line, const char* expr_text, const char* exception)
{
    string msg = "CHECK_THROW("+string(expr_text)+") failed: Expected exception "+exception;
    check_failed(file, line, msg);
}


void Reporter::begin(const Location&)
{
}

void Reporter::fail(const Location&, const string&)
{
}

void Reporter::end(const Location&, double)
{
}

void Reporter::summary(const Summary&)
{
}


bool run(Reporter* reporter, Filter* filter)
{
    Timer timer(Timer::type_UserTime);
    double prev_time = 0;
    Registry& reg = get_registry();
    UniqueLock lock(reg.m_mutex);
    reg.m_reporter = reporter;
    long num_tests = long(reg.m_tests.size());
    long num_excluded_tests = 0, num_failed_tests = 0;
    for (long i = 0; i != num_tests; ++i) {
        Test& test = reg.m_tests[i];
        if (filter && !filter->include(test.m_loc)) {
            ++num_excluded_tests;
            continue;
        }
        reg.m_current_test = &test;
        reg.m_errors_seen = false;
        if (reporter)
            reporter->begin(test.m_loc);
        try {
            TempUnlockGuard unlock(lock);
            (*test.m_func)();
        }
        catch (exception& ex) {
            reg.m_errors_seen = true;
            if (reporter) {
                string message = "Unhandled exception "+get_type_name(ex)+": "+ex.what();
                reporter->fail(test.m_loc, message);
            }
        }
        catch (...) {
            reg.m_errors_seen = true;
            if (reporter) {
                string message = "Unhandled exception of unknown type";
                reporter->fail(test.m_loc, message);
            }
        }
        if (reporter) {
            double time = timer.get_elapsed_time();
            reporter->end(test.m_loc, time - prev_time);
            prev_time = time;
        }
        if (reg.m_errors_seen)
            ++num_failed_tests;
    }
    if (reporter) {
        Summary summary;
        summary.num_included_tests = num_tests - num_excluded_tests;
        summary.num_failed_tests   = num_failed_tests;
        summary.num_excluded_tests = num_excluded_tests;
        summary.num_checks         = reg.m_checks_completed;
        summary.num_failed_checks  = reg.m_checks_failed;
        summary.elapsed_seconds    = timer.get_elapsed_time();
        reporter->summary(summary);
    }
    return num_failed_tests == 0;
}



SimpleReporter::SimpleReporter(bool report_progress)
{
    m_report_progress = report_progress;
}

void SimpleReporter::begin(const Location& loc)
{
    if (!m_report_progress)
        return;

    cout << loc.file_name << ":" << loc.line_number << ": "
        "Begin " << loc.test_name << "\n";
}

void SimpleReporter::fail(const Location& loc, const string& message)
{
    cerr << loc.file_name << ":" << loc.line_number << ": "
        "ERROR in " << loc.test_name << ": " << message << "\n";
}

void SimpleReporter::summary(const Summary& summary)
{
    cout << "\n";
    if (summary.num_failed_tests == 0) {
        cout << "Success: "<<summary.num_included_tests<<" tests passed "
            "("<<summary.num_checks<<" checks).\n";
    }
    else {
        cerr << "FAILURE: "<<summary.num_failed_tests<<" "
            "out of "<<summary.num_included_tests<<" tests failed "
            "("<<summary.num_failed_checks<<" "
            "out of "<<summary.num_checks<<" checks failed).\n";
    }
    cout << "Test time: "<<Timer::format(summary.elapsed_seconds)<<"\n";
    if (summary.num_excluded_tests != 0)
        cout << "\nNote: "<<summary.num_excluded_tests<<" tests were excluded!\n";
}


Reporter* create_xml_reporter(ostream& out)
{
    return new XmlReporter(out);
}


Filter* create_wildcard_filter(const string& filter)
{
    return new WildcardFilter(filter);
}


} // namespace unit_test
} // namespace test_util
} // namespace tightdb
