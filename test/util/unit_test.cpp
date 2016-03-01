#include <functional>
#include <memory>
#include <stdexcept>
#include <map>
#include <string>
#include <iostream>

#include <realm/util/thread.hpp>

#include "demangle.hpp"
#include "timer.hpp"
#include "random.hpp"
#include "wildcard.hpp"
#include "unit_test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using namespace realm::test_util::unit_test;



// FIXME: Think about order of tests during execution.
// FIXME: Write quoted strings with escaped nonprintables



namespace {

void replace_char(std::string& str, char c, const std::string& replacement)
{
    for (size_t pos = str.find(c); pos != std::string::npos; pos = str.find(c, pos + 1))
        str.replace(pos, 1, replacement);
}


std::string xml_escape(const std::string& value)
{
    std::string value_2 = value;
    replace_char(value_2, '&',  "&amp;");
    replace_char(value_2, '<',  "&lt;");
    replace_char(value_2, '>',  "&gt;");
    replace_char(value_2, '\'', "&apos;");
    replace_char(value_2, '\"', "&quot;");
    return value_2;
}


class XmlReporter: public Reporter {
public:
    XmlReporter(std::ostream& out):
        m_out(out)
    {
    }

    ~XmlReporter() noexcept
    {
    }

    void begin(const TestContext& context) override
    {
        auto key = key_type(context.test_index, context.recurrence_index);
        m_tests.emplace(key, test());
    }

    void fail(const TestContext& context, const char* file_name, long line_number,
              const std::string& message) override
    {
        failure f;
        f.file_name   = file_name;
        f.line_number = line_number;
        f.message     = message;
        auto key = key_type(context.test_index, context.recurrence_index);
        auto i = m_tests.find(key);
        i->second.failures.push_back(f);
    }

    void end(const TestContext& context, double elapsed_seconds) override
    {
        auto key = key_type(context.test_index, context.recurrence_index);
        auto i = m_tests.find(key);
        i->second.elapsed_seconds = elapsed_seconds;
    }

    void summary(const ExecContext& context, const Summary& summary) override
    {
        m_out <<
            "<?xml version=\"1.0\"?>\n"
            "<unittest-results "
            "tests=\"" << summary.num_executed_tests << "\" "
            "failedtests=\"" << summary.num_failed_tests << "\" "
            "checks=\"" << summary.num_executed_checks << "\" "
            "failures=\"" << summary.num_failed_checks << "\" "
            "time=\"" << summary.elapsed_seconds << "\">\n";
        std::ostringstream out;
        out.imbue(std::locale::classic());
        for (const auto& p: m_tests) {
            auto key = p.first;
            const test& t = p.second;
            size_t test_index    = key.first;
            int recurrence_index = key.second;
            const TestDetails details = context.test_list.get_test_details(test_index);
            out.str(std::string());
            out << details.test_name;
            if (context.num_recurrences > 1)
                out << '#' << (recurrence_index+1);
            std::string test_name = out.str();
            m_out <<
                "  <test suite=\""<< xml_escape(details.suite_name) <<"\" "
                "name=\"" << xml_escape(test_name) << "\" "
                "time=\"" << t.elapsed_seconds << "\"";
            if (t.failures.empty()) {
                m_out << "/>\n";
                continue;
            }
            m_out << ">\n";
            typedef std::vector<failure>::const_iterator fail_iter;
            fail_iter fails_end = t.failures.end();
            for (fail_iter i_2 = t.failures.begin(); i_2 != fails_end; ++i_2) {
                std::string msg = xml_escape(i_2->message);
                m_out << "    <failure message=\"" << i_2->file_name << ""
                    "(" << i_2->line_number << ") : " << msg << "\"/>\n";
            }
            m_out << "  </test>\n";
        }
        m_out <<
            "</unittest-results>\n";
    }

protected:
    struct failure {
        const char* file_name;
        long line_number;
        std::string message;
    };

    struct test {
        std::vector<failure> failures;
        double elapsed_seconds = 0;
    };

    using key_type = std::pair<size_t, int>; // (test index, recurrence index)
    std::map<key_type, test> m_tests;

    std::ostream& m_out;
};


class WildcardFilter: public Filter {
public:
    WildcardFilter(const std::string& filter)
    {
        bool exclude = false;
        typedef std::string::const_iterator iter;
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

            std::string word(word_begin, word_end);
            patterns& p = exclude ? m_exclude : m_include;
            p.push_back(wildcard_pattern(word));
        }

        // Include everything if no includes are specified.
        if (m_include.empty())
            m_include.push_back(wildcard_pattern("*"));
    }

    ~WildcardFilter() noexcept
    {
    }

    bool include(const TestDetails& details) override
    {
        const char* name_begin = details.test_name.data();
        const char* name_end   = name_begin + details.test_name.size();
        typedef patterns::const_iterator iter;

        // Say "no" if it matches an exclude pattern
        {
            iter end = m_exclude.end();
            for (iter i = m_exclude.begin(); i != end; ++i) {
                if (i->match(name_begin, name_end))
                    return false;
            }
        }

        // Say "yes" if it matches an include pattern
        {
            iter end = m_include.end();
            for (iter i = m_include.begin(); i != end; ++i) {
                if (i->match(name_begin, name_end))
                    return true;
            }
        }

        // Not included
        return false;
    }

private:
    typedef std::vector<wildcard_pattern> patterns;
    patterns m_include, m_exclude;
};


} // anonymous namespace



namespace realm {
namespace test_util {
namespace unit_test {


class TestList::SharedContext: public ExecContext {
public:
    Reporter& reporter;
    Mutex mutex;
    struct Entry {
        const Test* test;
        size_t test_index;
        int recurrence_index;
    };
    std::vector<Entry> concur_tests, no_concur_tests;
    size_t next_concur_test = 0; // Index into `concur_tests`
    long num_failed_tests = 0;
    long long num_checks = 0;
    long long num_failed_checks = 0;
    int num_ended_threads = 0;

    SharedContext(const TestList& test_list, int num_recurrences, int num_threads, Reporter& r):
        ExecContext(test_list, num_recurrences, num_threads),
        reporter(r)
    {
    }

    void add(Entry entry)
    {
        const Test& test = *entry.test;
        auto& tests = (test.allow_concur && num_threads > 1 ? concur_tests : no_concur_tests);
        tests.push_back(entry);
    }

    void shuffle()
    {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        random.shuffle(concur_tests.begin(), concur_tests.end());
        random.shuffle(no_concur_tests.begin(), no_concur_tests.end());
    }
};



class TestList::ThreadContext {
public:
    const int thread_index;
    SharedContext& shared_context;
    Mutex mutex;
    std::atomic<long long> num_checks{0};
    long long num_failed_checks = 0;
    long num_failed_tests = 0;
    bool errors_seen;

    ThreadContext(int ti, SharedContext& sc):
        thread_index(ti),
        shared_context(sc)
    {
    }

    void run();

    void run(SharedContext::Entry, UniqueLock&);
};


void TestList::add(RunFunc run_func, IsEnabledFunc is_enabled_func, bool allow_concur,
                   const char* suite, const std::string& name, const char* file, long line)
{
    Test test;
    test.run_func        = run_func;
    test.is_enabled_func = is_enabled_func;
    test.allow_concur    = allow_concur;
    test.details.suite_name  = suite;
    test.details.test_name   = name;
    test.details.file_name   = file;
    test.details.line_number = line;
    m_tests.reserve(m_tests.size() + 1); // Throws
    m_test_storage.push_back(test); // Throws
    m_tests.push_back(&m_test_storage.back());
}


bool TestList::run(Reporter* reporter, Filter* filter, int num_repetitions, int num_threads,
                   bool shuffle)
{
    Timer timer;
    Reporter fallback_reporter;
    Reporter& reporter_2 = reporter ? *reporter : fallback_reporter;
    if (num_repetitions < 0)
        throw std::runtime_error("Bad number of repetitions");
    if (num_threads < 1)
        throw std::runtime_error("Bad number of threads");

    // Filter
    std::vector<std::pair<const Test*, size_t>> included_tests; // Second component is test index
    size_t num_enabled = 0, num_disabled = 0;
    for (size_t i = 0; i < m_tests.size(); ++i) {
        const Test* test = m_tests[i];
        if (!(*test->is_enabled_func)()) {
            ++num_disabled;
            continue;
        }
        ++num_enabled;
        if (filter && !filter->include(test->details))
            continue;
        included_tests.emplace_back(test, i);
    }

    // Repeat
    SharedContext shared_context(*this, num_repetitions, num_threads, reporter_2);
    size_t num_executed_tests = 0;
    for (int i = 0; i < num_repetitions; ++i) {
        for (auto p: included_tests) {
            SharedContext::Entry entry;
            entry.test = p.first;
            entry.test_index = p.second;
            entry.recurrence_index = i;
            shared_context.add(entry);
            ++num_executed_tests;
        }
    }

    // Shuffle
    if (shuffle)
        shared_context.shuffle();

    // Execute
    if (num_threads == 1) {
        ThreadContext thread_context(0, shared_context);
        thread_context.run();
    }
    else {
        auto thread = [&](int i) {
            ThreadContext thread_context(i, shared_context);
            thread_context.run();
        };
        std::unique_ptr<Thread[]> threads(new Thread[num_threads]);
        for (int i = 0; i != num_threads; ++i)
            threads[i].start([=] { thread(i); });
        for (int i = 0; i != num_threads; ++i)
            threads[i].join();
    }

    // Summarize
    Summary summary;
    summary.num_disabled_tests  = long(num_disabled);
    summary.num_excluded_tests  = long(num_enabled - included_tests.size());
    summary.num_included_tests  = long(included_tests.size());
    summary.num_executed_tests  = long(num_executed_tests);
    summary.num_failed_tests    = shared_context.num_failed_tests;
    summary.num_executed_checks = shared_context.num_checks;
    summary.num_failed_checks   = shared_context.num_failed_checks;
    summary.elapsed_seconds     = timer.get_elapsed_time();
    reporter_2.summary(shared_context, summary);

    return shared_context.num_failed_tests == 0;
}


void TestList::ThreadContext::run()
{
    UniqueLock lock(shared_context.mutex);

    // First run the tests that can safely run concurrently with other threads
    // and with itself.
    while (shared_context.next_concur_test < shared_context.concur_tests.size()) {
        auto entry = shared_context.concur_tests[shared_context.next_concur_test++];
        run(entry, lock);
    }

    // When only the last test thread running, we can run the tests that cannot
    // safely run concurrently with other threads or with itself.
    int num_remaining_threads = shared_context.num_threads - shared_context.num_ended_threads;
    if (num_remaining_threads == 1) {
        for (auto entry: shared_context.no_concur_tests)
            run(entry, lock);
    }

    shared_context.num_failed_tests  += num_failed_tests;
    shared_context.num_checks        += num_checks;
    shared_context.num_failed_checks += num_failed_checks;

    ++shared_context.num_ended_threads;
    shared_context.reporter.end_of_thread(shared_context, thread_index);
}


void TestList::ThreadContext::run(SharedContext::Entry entry, UniqueLock& lock)
{
    const Test& test = *entry.test;
    TestContext test_context(*this, test.details, entry.test_index, entry.recurrence_index);
    shared_context.reporter.begin(test_context);
    lock.unlock();

    errors_seen = false;
    Timer timer;
    try {
        (*test.run_func)(test_context);
    }
    catch (std::exception& ex) {
        std::string message = "Unhandled exception "+get_type_name(ex)+": "+ex.what();
        test_context.test_failed(message);
    }
    catch (...) {
        std::string message = "Unhandled exception of unknown type";
        test_context.test_failed(message);
    }
    double elapsed_time = timer.get_elapsed_time();
    if (errors_seen)
        ++num_failed_tests;

    lock.lock();
    shared_context.reporter.end(test_context, elapsed_time);
}


TestList& get_default_test_list()
{
    static TestList list;
    return list;
}


TestContext::TestContext(TestList::ThreadContext& tc, const TestDetails& td, size_t ti, int ri):
    exec_context(tc.shared_context),
    test_details(td),
    test_index(ti),
    recurrence_index(ri),
    thread_index(tc.thread_index),
    m_thread_context(tc)
{
}


void TestContext::check_succeeded()
{
    ++m_thread_context.num_checks;
}


void TestContext::check_failed(const char* file, long line, const std::string& message)
{
    {
        LockGuard lock(m_thread_context.mutex);
        ++m_thread_context.num_checks;
        ++m_thread_context.num_failed_checks;
        m_thread_context.errors_seen = true;
    }
    {
        TestList::SharedContext& shared = m_thread_context.shared_context;
        LockGuard lock(shared.mutex);
        shared.reporter.fail(*this, file, line, message);
    }
}


void TestContext::test_failed(const std::string& message)
{
    {
        LockGuard lock(m_thread_context.mutex);
        m_thread_context.errors_seen = true;
    }
    {
        TestList::SharedContext& shared = m_thread_context.shared_context;
        LockGuard lock(shared.mutex);
        shared.reporter.fail(*this, test_details.file_name, test_details.line_number, message);
    }
}


void TestContext::cond_failed(const char* file, long line, const char* macro_name,
                              const char* cond_text)
{
    std::string msg = std::string(macro_name)+"("+cond_text+") failed";
    check_failed(file, line, msg);
}


void TestContext::compare_failed(const char* file, long line, const char* macro_name,
                                 const char* a_text, const char* b_text,
                                 const std::string& a_val, const std::string& b_val)
{
    std::string msg = std::string(macro_name)+"("+a_text+", "+b_text+") failed with ("+a_val+", "+b_val+")";
    check_failed(file, line, msg);
}


void TestContext::inexact_compare_failed(const char* file, long line, const char* macro_name,
                                         const char* a_text, const char* b_text,
                                         const char* eps_text, long double a, long double b,
                                         long double eps)
{
    std::ostringstream out;
    out.precision(std::numeric_limits<long double>::digits10 + 1);
    out << macro_name<<"("<<a_text<<", "<<b_text<<", "<<eps_text<<") "
        "failed with ("<<a<<", "<<b<<", "<<eps<<")";
    check_failed(file, line, out.str());
}


void TestContext::throw_failed(const char* file, long line, const char* expr_text,
                               const char* exception_name)
{
    std::ostringstream out;
    out << "CHECK_THROW("<<expr_text<<", "<<exception_name<<") failed: Did not throw";
    check_failed(file, line, out.str());
}


void TestContext::throw_ex_failed(const char* file, long line, const char* expr_text,
                                  const char* exception_name, const char* exception_cond_text)
{
    std::ostringstream out;
    out << "CHECK_THROW_EX("<<expr_text<<", "<<exception_name<<", "<<
        exception_cond_text<<") failed: Did not throw";
    check_failed(file, line, out.str());
}


void TestContext::throw_ex_cond_failed(const char* file, long line, const char* expr_text,
                                       const char* exception_name, const char* exception_cond_text)
{
    std::ostringstream out;
    out << "CHECK_THROW_EX("<<expr_text<<", "<<exception_name<<", "<<
        exception_cond_text<<") failed: Did throw, but condition failed";
    check_failed(file, line, out.str());
}


void TestContext::throw_any_failed(const char* file, long line, const char* expr_text)
{
    std::ostringstream out;
    out << "CHECK_THROW_ANY("<<expr_text<<") failed: Did not throw";
    check_failed(file, line, out.str());
}


void Reporter::begin(const TestContext&)
{
}

void Reporter::fail(const TestContext&, const char*, long, const std::string&)
{
}

void Reporter::end(const TestContext&, double)
{
}

void Reporter::summary(const ExecContext&, const Summary&)
{
}

void Reporter::end_of_thread(const ExecContext&, int)
{
}


class PatternBasedFileOrder::state: public RefCountBase {
public:
    typedef std::map<const void*, int> major_map; // Key is address of TestDetails object
    major_map m_major_map;

    typedef std::vector<wildcard_pattern> patterns;
    patterns m_patterns;

    state(const char** patterns_begin, const char** patterns_end)
    {
        for (const char** i = patterns_begin; i != patterns_end; ++i)
            m_patterns.push_back(wildcard_pattern(*i));
    }

    ~state() noexcept
    {
    }

    int get_major(const TestDetails& details)
    {
        major_map::const_iterator i = m_major_map.find(&details);
        if (i != m_major_map.end())
            return i->second;
        patterns::const_iterator j = m_patterns.begin(), end = m_patterns.end();
        while (j != end && !j->match(details.file_name))
            ++j;
        int major = int(j - m_patterns.begin());
        m_major_map[&details] = major;
        return major;
    }
};

bool PatternBasedFileOrder::operator()(const TestDetails& a, const TestDetails& b)
{
    int major_a = m_wrap.m_state->get_major(a);
    int major_b = m_wrap.m_state->get_major(b);
    if (major_a < major_b)
        return true;
    if (major_a > major_b)
        return false;
    int i = strcmp(a.file_name, b.file_name);
    return i < 0;
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

void SimpleReporter::begin(const TestContext& context)
{
    if (!m_report_progress)
        return;

    const TestDetails& details = context.test_details;
    std::cout << details.file_name << ":" << details.line_number << ": ";
    std::cout << "Begin " << details.test_name;
    if (context.exec_context.num_recurrences > 1)
        std::cout << '#' << (context.recurrence_index+1);
    if (context.exec_context.num_threads > 1)
        std::cout << " [test thread "<<(context.thread_index+1)<<"] ";
    std::cout << "\n";
}

void SimpleReporter::fail(const TestContext& context, const char* file_name, long line_number,
                          const std::string& message)
{
    const TestDetails& details = context.test_details;
    std::cerr << file_name << ":" << line_number << ": ";
    std::cerr << "ERROR in " << details.test_name;
    if (context.exec_context.num_recurrences > 1)
        std::cout << '#' << (context.recurrence_index+1);
    std::cerr << ": " << message << "\n";
}

void SimpleReporter::summary(const ExecContext&, const Summary& summary)
{
    std::cout << "\n";
    if (summary.num_failed_tests == 0) {
        std::cout << "Success: All "<<summary.num_executed_tests<<" tests passed "
            "("<<summary.num_executed_checks<<" checks).\n";
    }
    else {
        std::cerr << "FAILURE: "<<summary.num_failed_tests<<" "
            "out of "<<summary.num_executed_tests<<" tests failed "
            "("<<summary.num_failed_checks<<" "
            "out of "<<summary.num_executed_checks<<" checks failed).\n";
    }
    std::cout << "Test time: "<<Timer::format(summary.elapsed_seconds)<<"\n";
    if (summary.num_excluded_tests == 1) {
        std::cout << "\nNote: One test was excluded!\n";
    }
    else if (summary.num_excluded_tests > 1) {
        std::cout << "\nNote: "<<summary.num_excluded_tests<<" tests were excluded!\n";
    }
}

void SimpleReporter::end_of_thread(const ExecContext& context, int thread_index)
{
    if (!m_report_progress)
        return;

    if (context.num_threads > 1)
        std::cout << "End of test thread "<<(thread_index+1)<<"\n";
}


Reporter* create_xml_reporter(std::ostream& out)
{
    return new XmlReporter(out);
}


Filter* create_wildcard_filter(const std::string& filter)
{
    return new WildcardFilter(filter);
}


} // namespace unit_test
} // namespace test_util
} // namespace realm
