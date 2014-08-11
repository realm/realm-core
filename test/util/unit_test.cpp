#include <stdexcept>
#include <map>
#include <string>
#include <iostream>

#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/bind.hpp>
#include <tightdb/util/thread.hpp>

#include "demangle.hpp"
#include "timer.hpp"
#include "random.hpp"
#include "wildcard.hpp"
#include "unit_test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;
using namespace tightdb::test_util::unit_test;



// FIXME: Think about order of tests during execution.
// FIXME: Write quoted strings with escaped nonprintables



namespace {


struct SharedContext {
    Reporter& m_reporter;
    vector<Test*> m_tests;
    Mutex m_mutex;
    size_t m_next_test;

    SharedContext(Reporter& reporter):
        m_reporter(reporter),
        m_next_test(0)
    {
    }
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
                "  <test suite=\""<< t.m_details.suite_name <<"\" "
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
    WildcardFilter(const string& filter)
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


class TestList::ExecContext {
public:
    SharedContext* m_shared;
    Mutex m_mutex;
    long long m_num_checks;
    long long m_num_failed_checks;
    long m_num_failed_tests;
    bool m_errors_seen;

    ExecContext():
        m_shared(0),
        m_num_checks(0),
        m_num_failed_checks(0),
        m_num_failed_tests(0)
    {
    }

    void run();
};


void TestList::add(Test& test, const char* suite, const char* name, const char* file, long line)
{
    test.test_results.m_test = &test;
    test.test_results.m_list = this;
    long index = long(m_tests.size());
    TestDetails& details = test.test_details;
    details.test_index  = index;
    details.suite_name  = suite;
    details.test_name   = name;
    details.file_name   = file;
    details.line_number = line;
    m_tests.push_back(&test);
}

void TestList::reassign_indexes()
{
    long n = long(m_tests.size());
    for (long i = 0; i != n; ++i) {
        Test* test = m_tests[i];
        test->test_details.test_index = i;
    }
}

void TestList::ExecContext::run()
{
    Timer timer;
    double time = 0;
    Test* test = 0;
    for (;;) {
        double prev_time = time;
        time = timer.get_elapsed_time();

        // Next test
        {
            SharedContext& shared = *m_shared;
            Reporter& reporter = shared.m_reporter;
            LockGuard lock(shared.m_mutex);
            if (test)
                reporter.end(test->test_details, time - prev_time);
            if (shared.m_next_test == shared.m_tests.size())
                break;
            test = shared.m_tests[shared.m_next_test++];
            reporter.begin(test->test_details);
        }

        m_errors_seen = false;
        test->test_results.m_context = this;

        try {
            test->test_run();
        }
        catch (exception& ex) {
            string message = "Unhandled exception "+get_type_name(ex)+": "+ex.what();
            test->test_results.test_failed(message);
        }
        catch (...) {
            m_errors_seen = true;
            string message = "Unhandled exception of unknown type";
            test->test_results.test_failed(message);
        }

        test->test_results.m_context = 0;
        if (m_errors_seen)
            ++m_num_failed_tests;
    }
}

bool TestList::run(Reporter* reporter, Filter* filter, int num_threads, bool shuffle)
{
    Timer timer;
    Reporter fallback_reporter;
    Reporter& reporter_2 = reporter ? *reporter : fallback_reporter;
    if (num_threads < 1 || num_threads > 1024)
        throw runtime_error("Bad number of threads");

    SharedContext shared(reporter_2);
    size_t num_tests = m_tests.size(), num_disabled = 0;
    for (size_t i = 0; i != num_tests; ++i) {
        Test* test = m_tests[i];
        if (!test->test_enabled()) {
            ++num_disabled;
            continue;
        }
        if (filter && !filter->include(test->test_details))
            continue;
        shared.m_tests.push_back(test);
    }

    if (shuffle) {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        random.shuffle(shared.m_tests.begin(), shared.m_tests.end());
    }

    UniquePtr<ExecContext[]> thread_contexts(new ExecContext[num_threads]);
    for (int i = 0; i != num_threads; ++i)
        thread_contexts[i].m_shared = &shared;

    if (num_threads == 1) {
        thread_contexts[0].run();
    }
    else {
        UniquePtr<Thread[]> threads(new Thread[num_threads]);
        for (int i = 0; i != num_threads; ++i)
            threads[i].start(bind(&ExecContext::run, &thread_contexts[i]));
        for (int i = 0; i != num_threads; ++i)
            threads[i].join();
    }

    long num_failed_tests = 0;
    long long num_checks = 0;
    long long num_failed_checks = 0;

    for (int i = 0; i != num_threads; ++i) {
        ExecContext& thread_context = thread_contexts[i];
        num_failed_tests  += thread_context.m_num_failed_tests;
        num_checks        += thread_context.m_num_checks;
        num_failed_checks += thread_context.m_num_failed_checks;
    }

    Summary summary;
    summary.num_included_tests = long(shared.m_tests.size());
    summary.num_failed_tests   = num_failed_tests;
    summary.num_excluded_tests = long(num_tests - num_disabled) - summary.num_included_tests;
    summary.num_disabled_tests = long(num_disabled);
    summary.num_checks         = num_checks;
    summary.num_failed_checks  = num_failed_checks;
    summary.elapsed_seconds    = timer.get_elapsed_time();
    reporter_2.summary(summary);

    return num_failed_tests == 0;
}


TestList& get_default_test_list()
{
    static TestList list;
    return list;
}


TestResults::TestResults():
    m_test(0),
    m_list(0),
    m_context(0)
{
}


void TestResults::check_succeeded()
{
    LockGuard lock(m_context->m_mutex);
    ++m_context->m_num_checks;
}


void TestResults::check_failed(const char* file, long line, const string& message)
{
    {
        LockGuard lock(m_context->m_mutex);
        ++m_context->m_num_checks;
        ++m_context->m_num_failed_checks;
        m_context->m_errors_seen = true;
    }
    SharedContext& shared = *m_context->m_shared;
    TestDetails details = m_test->test_details; // Copy
    details.file_name   = file;
    details.line_number = line;
    {
        LockGuard lock(shared.m_mutex);
        shared.m_reporter.fail(details, message);
    }
}


void TestResults::test_failed(const string& message)
{
    {
        LockGuard lock(m_context->m_mutex);
        m_context->m_errors_seen = true;
    }
    SharedContext& shared = *m_context->m_shared;
    {
        LockGuard lock(shared.m_mutex);
        shared.m_reporter.fail(m_test->test_details, message);
    }
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
    out.precision(numeric_limits<long double>::digits10 + 1);
    out << macro_name<<"("<<a_text<<", "<<b_text<<", "<<eps_text<<") "
        "failed with ("<<a<<", "<<b<<", "<<eps<<")";
    check_failed(file, line, out.str());
}


void TestResults::throw_failed(const char* file, long line,
                               const char* expr_text, const char* exception)
{
    ostringstream out;
    out << "CHECK_THROW(" << expr_text << ", " << (exception ? exception : "''") << ") failed: Did not throw";
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


class PatternBasedFileOrder::state: public RefCountBase {
public:
    typedef map<TestDetails*, int> major_map;
    major_map m_major_map;

    typedef vector<wildcard_pattern> patterns;
    patterns m_patterns;

    state(const char** patterns_begin, const char** patterns_end)
    {
        for (const char** i = patterns_begin; i != patterns_end; ++i)
            m_patterns.push_back(wildcard_pattern(*i));
    }

    ~state() TIGHTDB_NOEXCEPT
    {
    }

    int get_major(TestDetails* details)
    {
        major_map::const_iterator i = m_major_map.find(details);
        if (i != m_major_map.end())
            return i->second;
        patterns::const_iterator j = m_patterns.begin(), end = m_patterns.end();
        while (j != end && !j->match(details->file_name))
            ++j;
        int major = int(j - m_patterns.begin());
        m_major_map[details] = major;
        return major;
    }
};

bool PatternBasedFileOrder::operator()(TestDetails* a, TestDetails* b)
{
    int major_a = m_wrap.m_state->get_major(a);
    int major_b = m_wrap.m_state->get_major(b);
    if (major_a < major_b)
        return true;
    if (major_a > major_b)
        return false;
    int i = strcmp(a->file_name, b->file_name);
    return i < 0 || (i == 0 && a->test_index < b->test_index);
}

PatternBasedFileOrder::wrap::wrap(const char** patterns_begin, const char** patterns_end):
    m_state(new state(patterns_begin, patterns_end))
{
}

PatternBasedFileOrder::wrap::~wrap()
{
}

PatternBasedFileOrder::wrap::wrap(const wrap& w):
    m_state(w.m_state)
{
}

PatternBasedFileOrder::wrap& PatternBasedFileOrder::wrap::operator=(const wrap& w)
{
    m_state = w.m_state;
    return *this;
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
    if (summary.num_excluded_tests == 1) {
        cout << "\nNote: One test was excluded!\n";
    }
    else if (summary.num_excluded_tests > 1) {
        cout << "\nNote: "<<summary.num_excluded_tests<<" tests were excluded!\n";
    }
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
