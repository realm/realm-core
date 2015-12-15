// #define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
#  include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <stdexcept>
#include <vector>
#include <locale>
#include <sstream>
#include <fstream>
#include <iostream>
#include <iomanip>

#include <realm/util/features.h>
#include <memory>
#include <realm/util/features.h>
#include <realm.hpp>
#include <realm/utilities.hpp>
#include <realm/version.hpp>
#include <realm/disable_sync_to_disk.hpp>

#include "test_all.hpp"
#include "util/timer.hpp"
#include "util/resource_limits.hpp"

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using namespace realm::test_util::unit_test;


namespace {

const char* file_order[] = {
    // When choosing order, please try to use these guidelines:
    //
    //  - If feature A depends on feature B, test feature B first.
    //
    //  - If feature A has a more central role in the API than feature B, test
    //    feature A first.
    //
    "test_self.cpp",

    // realm/util/
    "test_safe_int_ops.cpp",
    "test_basic_utils.cpp",
    "test_file*.cpp",
    "test_thread.cpp",
    "test_util_network.cpp",
    "test_utf8.cpp",

    // /realm/ (helpers)
    "test_string_data.cpp",
    "test_binary_data.cpp",

    // /realm/impl/ (detail)
    "test_alloc*.cpp",
    "test_array*.cpp",
    "test_column*.cpp",
    "test_index*.cpp",
    "test_destroy_guard.cpp",

    // /realm/ (main API)
    "test_version.cpp",
    "test_table*.cpp",
    "test_descriptor*.cpp",
    "test_group*.cpp",
    "test_shared*.cpp",
    "test_transactions*.cpp",
    "test_query*.cpp",
    "test_links.cpp",
    "test_link_query_view.cpp",
    "test_json.cpp",
    "test_replication*.cpp",
    "test_transform.cpp",
    "test_sync.cpp",

    "test_lang_bind_helper.cpp",

    "large_tests*.cpp"
};


void fix_max_open_files()
{
    if (system_has_rlimit(resource_NumOpenFiles)) {
        long soft_limit = get_soft_rlimit(resource_NumOpenFiles);
        if (soft_limit >= 0) {
            long hard_limit = get_hard_rlimit(resource_NumOpenFiles);
            long new_soft_limit = hard_limit < 0 ? 4096 : hard_limit;
            if (new_soft_limit > soft_limit) {
                set_soft_rlimit(resource_NumOpenFiles, new_soft_limit);
/*
                std::cout << "\n"
                    "MaxOpenFiles: "<<soft_limit<<" --> "<<new_soft_limit<<"\n";
*/
            }
        }
    }
}


void fix_async_daemon_path()
{
    // `setenv()` is POSIX. _WIN32 has `_putenv_s()` instead.
#ifndef _WIN32
    const char* async_daemon;
    // When running the unit-tests in Xcode, it runs them
    // in its own temporary directory. So we have to make sure we
    // look for the daemon there
    const char* xcode_env = getenv("__XCODE_BUILT_PRODUCTS_DIR_PATHS");
    if (xcode_env) {
#  ifdef REALM_DEBUG
        async_daemon = "realmd-dbg-noinst";
#  else
        async_daemon = "realmd-noinst";
#  endif
    }
    else {
#  ifdef REALM_COVER
        async_daemon = "../src/realm/realmd-cov-noinst";
#  else
#    ifdef REALM_DEBUG
        async_daemon = "../src/realm/realmd-dbg-noinst";
#    else
        async_daemon = "../src/realm/realmd-noinst";
#    endif
#  endif
    }
    setenv("REALM_ASYNC_DAEMON", async_daemon, 0);
#endif // _WIN32
}


void display_build_config()
{
    const char* with_debug =
        Version::has_feature(feature_Debug) ? "Enabled" : "Disabled";

#ifdef REALM_COMPILER_SSE
    const char* compiler_sse = "Yes";
#else
    const char* compiler_sse = "No";
#endif

#ifdef REALM_COMPILER_AVX
    const char* compiler_avx = "Yes";
#else
    const char* compiler_avx = "No";
#endif

    const char* cpu_sse = realm::sseavx<42>() ? "4.2" :
        (realm::sseavx<30>() ? "3.0" : "None");

    const char* cpu_avx = realm::sseavx<1>() ? "Yes" : "No";

    std::cout <<
        "\n"
        "Realm version: "<<Version::get_version()<<"\n"
        "  with Debug "<<with_debug<<"\n"
        "\n"
        "REALM_MAX_BPNODE_SIZE = "<<REALM_MAX_BPNODE_SIZE<<"\n"
        "\n"
        // Be aware that ps3/xbox have sizeof (void*) = 4 && sizeof (size_t) == 8
        // We decide to print size_t here
        "sizeof (size_t) * 8 = " << (sizeof (size_t) * 8) << "\n"
        "\n"
        "Compiler supported SSE (auto detect):       "<<compiler_sse<<"\n"
        "This CPU supports SSE (auto detect):        "<<cpu_sse<<"\n"
        "Compiler supported AVX (auto detect):       "<<compiler_avx<<"\n"
        "This CPU supports AVX (AVX1) (auto detect): "<<cpu_avx<<"\n"
        "\n";
}


// Records elapsed time for each test and shows a "Top 5" at the end.
class CustomReporter: public SimpleReporter {
public:
    explicit CustomReporter(bool report_progress):
        SimpleReporter(report_progress)
    {
    }

    ~CustomReporter() noexcept
    {
    }

    void end(const TestDetails& details, double elapsed_seconds) override
    {
        result r;
        r.m_test_name = details.test_name;
        r.m_elapsed_seconds = elapsed_seconds;
        m_results.push_back(r);
        SimpleReporter::end(details, elapsed_seconds);
    }

    void summary(const Summary& summary) override
    {
        SimpleReporter::summary(summary);

        size_t max_n = 5;
        size_t n = std::min<size_t>(max_n, m_results.size());
        if (n < 2)
            return;

        partial_sort(m_results.begin(), m_results.begin() + n, m_results.end());
        size_t name_col_width = 0, time_col_width = 0;
        for(size_t i = 0; i != n; ++i) {
            const result& r = m_results[i];
            size_t size = r.m_test_name.size();
            if (size > name_col_width)
                name_col_width = size;
            size = Timer::format(r.m_elapsed_seconds).size();
            if (size > time_col_width)
                time_col_width = size;
        }
        name_col_width += 2;
        size_t full_width = name_col_width + time_col_width;
        std::cout.fill('-');
        std::cout << "\nTop " << n << " time usage:\n" << std::setw(int(full_width)) << "" << "\n";
        std::cout.fill(' ');
        for(size_t i = 0; i != n; ++i) {
            const result& r = m_results[i];
            std::cout <<
                std::left  << std::setw(int(name_col_width)) << r.m_test_name <<
                std::right << std::setw(int(time_col_width)) << Timer::format(r.m_elapsed_seconds) << "\n";
        }
    }

private:
    struct result {
        std::string m_test_name;
        double m_elapsed_seconds;
        bool operator<(const result& r) const
        {
            return m_elapsed_seconds > r.m_elapsed_seconds; // Descending order
        }
    };

    std::vector<result> m_results;
};


bool run_tests()
{
    {
        const char* str = getenv("UNITTEST_RANDOM_SEED");
        if (str && strlen(str) != 0) {
            unsigned long seed;
            if (strcmp(str, "random") == 0) {
                seed = produce_nondeterministic_random_seed();
            }
            else {
                std::istringstream in(str);
                in.imbue(std::locale::classic());
                in.flags(in.flags() & ~std::ios_base::skipws); // Do not accept white space
                in >> seed;
                bool bad = !in || in.get() != std::char_traits<char>::eof();
                if (bad)
                    throw std::runtime_error("Bad random seed");
            }
            std::cout << "Random seed: "<<seed<<"\n\n";
            random_seed(seed);
        }
    }

    {
        const char* str = getenv("UNITTEST_KEEP_FILES");
        if (str && strlen(str) != 0)
            keep_test_files();
    }

    std::unique_ptr<Reporter> reporter;
    std::unique_ptr<Filter> filter;

    // Set up reporter
    std::ofstream xml_file;
    bool xml;
#ifdef REALM_MOBILE
    xml = true;
#else
    const char* xml_str = getenv("UNITTEST_XML");
    xml = (xml_str && strlen(xml_str) != 0);
#endif
    if (xml) {
        std::string path = get_test_path_prefix();
        std::string xml_path = path + "unit-test-report.xml";
        xml_file.open(xml_path.c_str());
        reporter.reset(create_xml_reporter(xml_file));
    }
    else {
        const char* str = getenv("UNITTEST_PROGRESS");
        bool report_progress = str && strlen(str) != 0;
        reporter.reset(new CustomReporter(report_progress));
    }

    // Set up filter
    const char* filter_str = getenv("UNITTEST_FILTER");
    const char* test_only = get_test_only();
    if (test_only)
        filter_str = test_only;
    if (filter_str && strlen(filter_str) != 0)
        filter.reset(create_wildcard_filter(filter_str));

    int num_threads = 1;
    {
        const char* str = getenv("UNITTEST_THREADS");
        if (str && strlen(str) != 0) {
            std::istringstream in(str);
            in.imbue(std::locale::classic());
            in.flags(in.flags() & ~std::ios_base::skipws); // Do not accept white space
            in >> num_threads;
            bool bad = !in || in.get() != std::char_traits<char>::eof() ||
                num_threads < 1 || num_threads > 1024;
            if (bad)
                throw std::runtime_error("Bad number of threads");
            if (num_threads > 1)
                std::cout << "Number of test threads: "<<num_threads<<"\n\n";
        }
    }

    bool shuffle = false;
    {
        const char* str = getenv("UNITTEST_SHUFFLE");
        if (str && strlen(str) != 0)
            shuffle = true;
    }

    // Run
    TestList& list = get_default_test_list();
    list.sort(PatternBasedFileOrder(file_order));
    bool success = list.run(reporter.get(), filter.get(), num_threads, shuffle);

    if (test_only)
        std::cout << "\n*** BE AWARE THAT MOST TESTS WERE EXCLUDED DUE TO USING 'ONLY' MACRO ***\n";

    if (!xml)
        std::cout << "\n";

    return success;
}


} // anonymous namespace

int test_all(int argc, char* argv[])
{
    // Disable buffering on std::cout so that progress messages can be related to
    // error messages.
    std::cout.setf(std::ios::unitbuf);

    // No need to synchronize file changes to physical medium in the test suite,
    // as that would only make a difference if the entire system crashes,
    // e.g. due to power off.
    disable_sync_to_disk();

    bool no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exitcode") == 0;

#ifdef _MSC_VER
    // we're in /build/ on Windows if we're in the Visual Studio IDE
    set_test_resource_path("../../test/");
    set_test_path_prefix("../../test/");
#endif

    fix_max_open_files();
    fix_async_daemon_path();
    display_build_config();

    bool success = run_tests();

#ifdef _MSC_VER
    getchar(); // wait for key
#endif

    return success || no_error_exit_staus ? EXIT_SUCCESS : EXIT_FAILURE;
}
