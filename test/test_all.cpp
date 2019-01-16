/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

// #define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
#include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

#ifdef _WIN32
// Using GetModuleFileName() and PathRemoveFileSpec()
#include <Shlwapi.h>
#pragma comment(lib, "shlwapi.lib")
#endif

#include <ctime>
#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <stdexcept>
#include <tuple>
#include <memory>
#include <iterator>
#include <vector>
#include <locale>
#include <sstream>
#include <fstream>
#include <iostream>
#include <iomanip>

#include <realm/util/features.h>
#include <realm.hpp>
#include <realm/utilities.hpp>
#include <realm/version.hpp>
#include <realm/disable_sync_to_disk.hpp>

#include "util/timer.hpp"
#include "util/resource_limits.hpp"

#include "test.hpp"
#include "test_all.hpp"

// Need to disable file descriptor leak checks on Apple platforms, as it seems
// like an unknown number of file descriptors can be left behind, presumably due
// the way asynchronous DNS lookup is implemented.
#if !defined _WIN32 && !REALM_PLATFORM_APPLE
#define ENABLE_FILE_DESCRIPTOR_LEAK_CHECK
#endif

#ifdef ENABLE_FILE_DESCRIPTOR_LEAK_CHECK
#include <unistd.h>
#include <fcntl.h>
#endif

using namespace realm;
using namespace realm::test_util;
using namespace realm::test_util::unit_test;

// Random seed for various random number generators used by fuzzying unit tests.
unsigned int unit_test_random_seed;

namespace {

// clang-format off
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

    "test_lang_bind_helper.cpp",

    "large_tests*.cpp"
};
// clang-format on

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


long get_num_open_files()
{
#ifdef ENABLE_FILE_DESCRIPTOR_LEAK_CHECK
    if (system_has_rlimit(resource_NumOpenFiles)) {
        long soft_limit = get_soft_rlimit(resource_NumOpenFiles);
        if (soft_limit >= 0) {
            long num_open_files = 0;
            for (long i = 0; i < soft_limit; ++i) {
                int fildes = int(i);
                int ret = fcntl(fildes, F_GETFD);
                if (ret != -1) {
                    ++num_open_files;
                    continue;
                }
                if (errno != EBADF)
                    throw std::runtime_error("fcntl() failed");
            }
            return num_open_files;
        }
    }
#endif
    return -1;
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
#ifdef REALM_DEBUG
        async_daemon = "realmd-dbg-noinst";
#else
        async_daemon = "realmd-noinst";
#endif
    }
    else {
#ifdef REALM_COVER
        async_daemon = "../src/realm/realmd-cov-noinst";
#else
#ifdef REALM_DEBUG
        async_daemon = "../src/realm/realmd-dbg-noinst";
#else
        async_daemon = "../src/realm/realmd-noinst";
#endif
#endif
    }
    setenv("REALM_ASYNC_DAEMON", async_daemon, 0);
#endif // _WIN32
}

void set_random_seed()
{
    // Select random seed for the random generator that some of our unit tests are using
    unit_test_random_seed = 1234567;
    const char* str = getenv("UNITTEST_RANDOM_SEED");
    if (str && strlen(str) != 0) {
        if (strcmp(str, "random") == 0) {
            unit_test_random_seed = produce_nondeterministic_random_seed();
        }
        else {
            std::istringstream in(str);
            in.imbue(std::locale::classic());
            in.flags(in.flags() & ~std::ios_base::skipws); // Do not accept white space
            in >> unit_test_random_seed;
            bool bad = !in || in.get() != std::char_traits<char>::eof();
            if (bad)
                throw std::runtime_error("Bad random seed");
        }
        random_seed(unit_test_random_seed);
    }
}

class AggressiveGovernor : public PageReclaimGovernor {
public:
    int64_t get_current_target(size_t) override
    {
        return 4096;
    }
};

AggressiveGovernor aggressive_governor;

void set_always_encrypt()
{
    const char* str = getenv("UNITTEST_ENCRYPT_ALL");
    if (str && strlen(str) != 0) {
        enable_always_encrypt();
        // ask for a very aggressive page reclaimer to maximize chance of triggering a bug.
        realm::util::set_page_reclaim_governor(&aggressive_governor);
    }
}

void display_build_config()
{
    const char* with_debug = Version::has_feature(feature_Debug) ? "Enabled" : "Disabled";

#if REALM_ENABLE_MEMDEBUG
    const char* memdebug = "Enabled";
#else
    const char* memdebug = "Disabled";
#endif

#if REALM_ENABLE_ENCRYPTION
    bool always_encrypt = is_always_encrypt_enabled();
    const char* encryption = always_encrypt ? "Enabled at compile-time (always encrypt = yes)"
                                            : "Enabled at compile-time (always encrypt = no)";
#else
    const char* encryption = "Disabled at compile-time";
#endif

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

    const char* cpu_sse = realm::sseavx<42>() ? "4.2" : (realm::sseavx<30>() ? "3.0" : "None");

    const char* cpu_avx = realm::sseavx<1>() ? "Yes" : "No";

    std::cout << std::endl
              << "Realm version: " << Version::get_version() << " with Debug " << with_debug << "\n"
              << "Encryption: " << encryption << "\n"
              << "\n"
              << "REALM_MAX_BPNODE_SIZE = " << REALM_MAX_BPNODE_SIZE << "\n"
              << "REALM_MEMDEBUG = " << memdebug << "\n"
              << "\n"
              // Be aware that ps3/xbox have sizeof (void*) = 4 && sizeof (size_t) == 8
              // We decide to print size_t here
              << "sizeof (size_t) * 8 = " << (sizeof(size_t) * 8) << "\n"
              << "\n"
              << "Compiler supported SSE (auto detect):       " << compiler_sse << "\n"
              << "This CPU supports SSE (auto detect):        " << cpu_sse << "\n"
              << "Compiler supported AVX (auto detect):       " << compiler_avx << "\n"
              << "This CPU supports AVX (AVX1) (auto detect): " << cpu_avx << "\n"
              << "\n"
              << "Unit test random seed:                      " << unit_test_random_seed << "\n"
              << std::endl;
}


// Records elapsed time for each test and shows a "Top 5" at the end.
class CustomReporter : public SimpleReporter {
public:
    explicit CustomReporter(bool report_progress)
        : SimpleReporter(report_progress)
    {
    }

    ~CustomReporter() noexcept
    {
    }

    void end(const TestContext& context, double elapsed_seconds) override
    {
        result r;
        r.test_index = context.test_index;
        r.recurrence_index = context.recurrence_index;
        r.elapsed_seconds = elapsed_seconds;
        m_results.push_back(r);
        SimpleReporter::end(context, elapsed_seconds);
    }

    void summary(const SharedContext& context, const Summary& results_summary) override
    {
        SimpleReporter::summary(context, results_summary);

        size_t max_n = 5;
        size_t n = std::min<size_t>(max_n, m_results.size());
        if (n < 2)
            return;

        partial_sort(m_results.begin(), m_results.begin() + n, m_results.end());
        std::vector<std::tuple<std::string, std::string>> rows;
        size_t name_col_width = 0, time_col_width = 0;
        for (size_t i = 0; i < n; ++i) {
            const result& r = m_results[i];
            const TestDetails& details = context.test_list.get_test_details(r.test_index);
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << details.test_name;
            if (context.num_recurrences > 1)
                out << '#' << (r.recurrence_index + 1);
            std::string name = out.str();
            std::string time = Timer::format(r.elapsed_seconds);
            rows.emplace_back(name, time);
            if (name.size() > name_col_width)
                name_col_width = name.size();
            if (time.size() > time_col_width)
                time_col_width = time.size();
        }

        name_col_width += 2;
        size_t full_width = name_col_width + time_col_width;
        std::cout.fill('-');
        std::cout << "\nTop " << n << " time usage:\n"
                  << std::setw(int(full_width)) << ""
                  << "\n";
        std::cout.fill(' ');
        for (const auto& row : rows) {
            std::cout << std::left << std::setw(int(name_col_width)) << std::get<0>(row) << std::right
                      << std::setw(int(time_col_width)) << std::get<1>(row) << "\n";
        }
    }

private:
    struct result {
        size_t test_index;
        int recurrence_index;
        double elapsed_seconds;
        bool operator<(const result& r) const
        {
            return elapsed_seconds > r.elapsed_seconds; // Descending order
        }
    };

    std::vector<result> m_results;
};


void put_time(std::ostream& out, const std::tm& tm, const char* format)
{
    const std::time_put<char>& facet = std::use_facet<std::time_put<char>>(out.getloc());
    facet.put(std::ostreambuf_iterator<char>(out), out, ' ', &tm, format, format + strlen(format));
}


bool run_tests(util::Logger* logger)
{
    {
        const char* str = getenv("UNITTEST_KEEP_FILES");
        if (str && strlen(str) != 0)
            keep_test_files();
    }

    TestList::Config config;
    config.logger = logger;

    // Set number of threads
    {
        const char* str = getenv("UNITTEST_THREADS");
        if (str && strlen(str) != 0) {
            std::istringstream in(str);
            in.imbue(std::locale::classic());
            in.flags(in.flags() & ~std::ios_base::skipws); // Do not accept white space
            in >> config.num_threads;
            bool bad = !in || in.get() != std::char_traits<char>::eof() || config.num_threads < 1;
            if (bad)
                throw std::runtime_error("Bad number of threads");
            if (config.num_threads > 1)
                std::cout << "Number of test threads: " << config.num_threads << "\n\n";
        }
    }

    // Set number of repetitions
    {
        const char* str = getenv("UNITTEST_REPEAT");
        if (str && strlen(str) != 0) {
            std::istringstream in(str);
            in.imbue(std::locale::classic());
            in.flags(in.flags() & ~std::ios_base::skipws); // Do not accept white space
            in >> config.num_repetitions;
            bool bad = !in || in.get() != std::char_traits<char>::eof() || config.num_repetitions < 0;
            if (bad)
                throw std::runtime_error("Bad number of repetitions");
        }
    }

    // Shuffle
    {
        const char* str = getenv("UNITTEST_SHUFFLE");
        if (str && strlen(str) != 0)
            config.shuffle = true;
    }

    // Set up reporter
    std::ofstream xml_file;
    bool xml;
#if REALM_MOBILE
    xml = true;
#else
    const char* xml_str = getenv("UNITTEST_XML");
    xml = (xml_str && strlen(xml_str) != 0);
#endif
    std::vector<std::unique_ptr<Reporter>> reporters;
    {
        const char* str = getenv("UNITTEST_PROGRESS");
        bool report_progress = str && strlen(str) != 0;
        std::unique_ptr<Reporter> reporter = std::make_unique<CustomReporter>(report_progress);
        reporters.push_back(std::move(reporter));
    }
    if (xml) {
        std::string path = get_test_path_prefix();
        std::string xml_path = path + "unit-test-report.xml";
        xml_file.open(xml_path.c_str());
        std::unique_ptr<Reporter> reporter_1 = create_junit_reporter(xml_file);
        std::unique_ptr<Reporter> reporter_2 = create_twofold_reporter(*reporters.back(), *reporter_1);
        reporters.push_back(std::move(reporter_1));
        reporters.push_back(std::move(reporter_2));
    }
    config.reporter = reporters.back().get();

    // Set up filter
    const char* filter_str = getenv("UNITTEST_FILTER");
    const char* test_only = get_test_only();
    if (test_only)
        filter_str = test_only;
    std::unique_ptr<Filter> filter;
    if (filter_str && strlen(filter_str) != 0)
        filter = create_wildcard_filter(filter_str);
    config.filter = filter.get();

    // Set intra test log level threshold
    {
        const char* str = getenv("UNITTEST_LOG_LEVEL");
        if (str && strlen(str) != 0) {
            std::istringstream in(str);
            in.imbue(std::locale::classic());
            in.flags(in.flags() & ~std::ios_base::skipws); // Do not accept white space
            in >> config.intra_test_log_level;
            bool bad = !in || in.get() != std::char_traits<char>::eof();
            if (bad)
                throw std::runtime_error("Bad intra test log level");
        }
    }

    // Set up per-thread file logging
    {
        const char* str = getenv("UNITTEST_LOG_TO_FILES");
        if (str && strlen(str) != 0) {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            time_t now = time(nullptr);
            tm tm = *localtime(&now);
            out << "test_logs_";
            put_time(out, tm, "%Y%m%d_%H%M%S");
            std::string dir_path = get_test_path_prefix() + out.str();
            util::make_dir(dir_path);
            config.per_thread_log_path = util::File::resolve("thread_%.log", dir_path);
        }
    }

    // Enable abort on failure
    {
        const char* str = getenv("UNITTEST_ABORT_ON_FAILURE");
        if (str && strlen(str) != 0) {
            config.abort_on_failure = true;
        }
    }

    // Run
    TestList& list = get_default_test_list();
    list.sort(PatternBasedFileOrder(file_order));
    bool success = list.run(config);

    if (test_only)
        std::cout << "\n*** BE AWARE THAT MOST TESTS WERE EXCLUDED DUE TO USING 'ONLY' MACRO ***\n";

    std::cout << "\n";

    return success;
}

} // anonymous namespace


int test_all(int argc, char* argv[], util::Logger* logger)
{
    // General note: Some Github clients on Windows will interfere with the .realm files created by unit tests (the
    // git client will attempt to access the files when it sees that new files have been created). This may cause
    // very rare/sporadic segfaults and asserts. If the temporary directory path is outside revision control, there
    // is no problem. Otherwise we need two things fulfilled: 1) The directory must be in .gitignore, and also 2) 
    // The directory must be newly created and not added to git.

    // Disable buffering on std::cout so that progress messages can be related to
    // error messages.
    std::cout.setf(std::ios::unitbuf);

#ifndef REALM_COVER
    // No need to synchronize file changes to physical medium in the test suite,
    // as that would only make a difference if the entire system crashes,
    // e.g. due to power off.
    // NOTE: This is not strictly true. If encryption is enabled, a crash of the testsuite
    // (not the whole platform) may produce corrupt realm files.
    disable_sync_to_disk();
#endif

    bool no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exitcode") == 0;

#ifdef _MSC_VER
    // Set CurrentDirectory to the same directory as the binary, so that we can run the unit test suite with no
    // problems regardless if we use the Visual Studio IDE or command line
    char dest[MAX_PATH];
    DWORD length = GetModuleFileNameA(NULL, dest, MAX_PATH);
    PathRemoveFileSpec(dest);
    SetCurrentDirectory(dest);

    set_test_resource_path("../");
    set_test_path_prefix("../");
#endif

    set_random_seed();
    set_always_encrypt();

    fix_max_open_files();
    fix_async_daemon_path();

    display_build_config();

    long num_open_files = get_num_open_files();

    bool success = run_tests(logger);

    if (num_open_files >= 0) {
        long num_open_files_2 = get_num_open_files();
        REALM_ASSERT(num_open_files_2 >= 0);
        if (num_open_files_2 > num_open_files) {
            long n = num_open_files_2 - num_open_files;
            std::cerr << "ERROR: " << n << " file descriptors were leaked\n";
            success = false;
        }
    }

#ifdef _MSC_VER
    // We don't want forked processes (see winfork() to require user interaction).
    if(!getenv("REALM_FORKED"))
        getchar(); // wait for key
#endif

    return success || no_error_exit_staus ? EXIT_SUCCESS : EXIT_FAILURE;
}
