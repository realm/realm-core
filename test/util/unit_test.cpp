#include <exception>
#include <vector>
#include <map>
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

    void begin(const TestDetails& details) TIGHTDB_OVERRIDE
    {
        test& t = m_tests[details.test_index];
        t.m_details = details;
    }

    void fail(const TestDetails& details, const string& message) TIGHTDB_OVERRIDE
    {
        failure f;
        f.m_details = details;
        f.m_message = message;
        test& t = m_tests[details.test_index];
        t.m_failures.push_back(f);
    }

    void end(const TestDetails& details, double elapsed_seconds) TIGHTDB_OVERRIDE
    {
        test& t = m_tests[details.test_index];
        t.m_elapsed_seconds = elapsed_seconds;
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
        typedef tests::const_iterator test_iter;
        test_iter tests_end = m_tests.end();
        for (test_iter i_1 = m_tests.begin(); i_1 != tests_end; ++i_1) {
            const test& t = i_1->second;
            m_out <<
                "  <test suite=\"default\" "
                "name=\"" << t.m_details.test_name << "\" "
                "time=\"" << t.m_elapsed_seconds << "\"";
            if (t.m_failures.empty()) {
                m_out << "/>\n";
                continue;
            }
            m_out << ">\n";
            typedef vector<failure>::const_iterator fail_iter;
            fail_iter fails_end = t.m_failures.end();
            for (fail_iter i_2 = t.m_failures.begin(); i_2 != fails_end; ++i_2) {
                string msg = xml_escape(i_2->m_message);
                m_out << "    <failure message=\"" << i_2->m_details.file_name << ""
                    "(" << i_2->m_details.line_number << ") : " << msg << "\"/>\n";
            }
            m_out << "  </test>\n";
        }
        m_out <<
            "</unittest-results>\n";
    }

protected:
    struct failure {
        TestDetails m_details;
        string m_message;
    };

    struct test {
        TestDetails m_details;
        vector<failure> m_failures;
        double m_elapsed_seconds;
    };

    typedef map<long, test> tests; // Key is test index
    tests m_tests;

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

    bool include(const TestDetails& details) TIGHTDB_OVERRIDE
    {
        const char* name = details.test_name;
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


class TestList::Impl {
public:
    vector<Test*> m_tests;
    Mutex m_mutex;
    long long m_num_checks;
    long long m_num_checks_failed;
};


class TestResults::ExecContext {
public:
    Reporter* m_reporter;
    Test* m_current_test;
    bool m_errors_seen;
};


TestList::TestList()
{
    m_impl.reset(new Impl);
}


TestList::~TestList() TIGHTDB_NOEXCEPT
{
}


void TestList::add(Test& test, const char* name, const char* file, long line)
{
    test.test_results.m_list = this;
    TestDetails& details = test.test_details;
    long index = long(m_impl->m_tests.size());
    details.test_index  = index;
    details.test_name   = name;
    details.file_name   = file;
    details.line_number = line;
    m_impl->m_tests.push_back(&test);
}


bool TestList::run(Reporter* reporter, Filter* filter)
{
    Timer timer(Timer::type_UserTime);
    double prev_time = 0;
    TestResults::ExecContext context;
    context.m_reporter = reporter;
    UniqueLock lock(m_impl->m_mutex);
    m_impl->m_num_checks = 0;
    m_impl->m_num_checks_failed = 0;
    long num_tests = long(m_impl->m_tests.size());
    long num_excluded_tests = 0, num_failed_tests = 0;
    for (long i = 0; i != num_tests; ++i) {
        Test* test = m_impl->m_tests[i];
        if (filter && !filter->include(test->test_details)) {
            ++num_excluded_tests;
            continue;
        }
        context.m_current_test = test;
        context.m_errors_seen = false;
        test->test_results.m_context = &context;
        if (reporter)
            reporter->begin(test->test_details);
        try {
            TempUnlockGuard unlock(lock);
            test->test_run();
        }
        catch (exception& ex) {
            context.m_errors_seen = true;
            if (reporter) {
                string message = "Unhandled exception "+get_type_name(ex)+": "+ex.what();
                reporter->fail(test->test_details, message);
            }
        }
        catch (...) {
            context.m_errors_seen = true;
            if (reporter) {
                string message = "Unhandled exception of unknown type";
                reporter->fail(test->test_details, message);
            }
        }
        if (reporter) {
            double time = timer.get_elapsed_time();
            reporter->end(test->test_details, time - prev_time);
            prev_time = time;
        }
        if (context.m_errors_seen)
            ++num_failed_tests;
        test->test_results.m_context = 0;
    }
    if (reporter) {
        Summary summary;
        summary.num_included_tests = num_tests - num_excluded_tests;
        summary.num_failed_tests   = num_failed_tests;
        summary.num_excluded_tests = num_excluded_tests;
        summary.num_checks         = m_impl->m_num_checks;
        summary.num_failed_checks  = m_impl->m_num_checks_failed;
        summary.elapsed_seconds    = timer.get_elapsed_time();
        reporter->summary(summary);
    }
    return num_failed_tests == 0;
}


TestList& get_default_test_list()
{
    static TestList list;
    return list;
}


TestResults::TestResults():
    m_list(0),
    m_context(0)
{
}


void TestResults::check_succeeded()
{
    TestList::Impl& list_impl = *m_list->m_impl;
    LockGuard lock(list_impl.m_mutex);
    TIGHTDB_ASSERT(m_context);
    ++list_impl.m_num_checks;
}


void TestResults::check_failed(const char* file, long line, const string& message)
{
    TestList::Impl& list_impl = *m_list->m_impl;
    LockGuard lock(list_impl.m_mutex);
    TIGHTDB_ASSERT(m_context);
    if (m_context->m_reporter) {
        Test& test = *m_context->m_current_test;
        TestDetails details = test.test_details; // Copy
        details.file_name   = file;
        details.line_number = line;
        m_context->m_reporter->fail(details, message);
    }
    m_context->m_errors_seen = true;
    ++list_impl.m_num_checks;
    ++list_impl.m_num_checks_failed;
}


void TestResults::cond_failed(const char* file, long line, const char* cond_text)
{
    string msg = "CHECK("+string(cond_text)+") failed";
    check_failed(file, line, msg);
}


void TestResults::compare_failed(const char* file, long line, const char* macro_name,
                                 const char* a_text, const char* b_text,
                                 const string& a_val, const string& b_val)
{
    string msg = string(macro_name)+"("+a_text+", "+b_text+") failed with ("+a_val+", "+b_val+")";
    check_failed(file, line, msg);
}


void TestResults::inexact_compare_failed(const char* file, long line, const char* macro_name,
                                         const char* a_text, const char* b_text,
                                         const char* eps_text, long double a, long double b,
                                         long double eps)
{
    ostringstream out;
    out.precision(std::numeric_limits<long double>::digits10 + 1);
    out << macro_name<<"("<<a_text<<", "<<b_text<<", "<<eps_text<<") "
        "failed with ("<<a<<", "<<b<<", "<<eps<<")";
    check_failed(file, line, out.str());
}


void TestResults::throw_failed(const char* file, long line,
                               const char* expr_text, const char* exception)
{
    ostringstream out;
    out << "CHECK_THROW("<<expr_text<<", "<<exception<<") failed: Did not throw";
    check_failed(file, line, out.str());
}


void Reporter::begin(const TestDetails&)
{
}

void Reporter::fail(const TestDetails&, const string&)
{
}

void Reporter::end(const TestDetails&, double)
{
}

void Reporter::summary(const Summary&)
{
}


SimpleReporter::SimpleReporter(bool report_progress)
{
    m_report_progress = report_progress;
}

void SimpleReporter::begin(const TestDetails& details)
{
    if (!m_report_progress)
        return;

    cout << details.file_name << ":" << details.line_number << ": "
        "Begin " << details.test_name << "\n";
}

void SimpleReporter::fail(const TestDetails& details, const string& message)
{
    cerr << details.file_name << ":" << details.line_number << ": "
        "ERROR in " << details.test_name << ": " << message << "\n";
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
