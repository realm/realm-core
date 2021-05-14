////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#define CATCH_CONFIG_RUNNER
#include "realm/util/features.h"
#if REALM_PLATFORM_APPLE
#define CATCH_CONFIG_NO_CPP17_UNCAUGHT_EXCEPTIONS
#endif
#include <catch2/catch.hpp>
#include <external/json/json.hpp>
#include <realm/util/to_string.hpp>

#include <limits.h>

#ifdef _MSC_VER
#include <Windows.h>

// PathCchRemoveFileSpec()
#include <pathcch.h>
#pragma comment(lib, "Pathcch.lib")
#else
#include <libgen.h>
#endif

int main(int argc, char** argv)
{
#ifdef _MSC_VER
    wchar_t path[MAX_PATH];
    if (GetModuleFileName(NULL, path, MAX_PATH) == 0) {
        fprintf(stderr, "Failed to retrieve path to exectuable.\n");
        return 1;
    }
    PathCchRemoveFileSpec(path, MAX_PATH);
    SetCurrentDirectory(path);
#else
    char executable[PATH_MAX];
    if (realpath(argv[0], executable) == NULL) {
        fprintf(stderr, "Failed to resolve path to exectuable.\n");
        return 1;
    }
    const char* directory = dirname(executable);
    if (chdir(directory) < 0) {
        fprintf(stderr, "Failed to change directory.\n");
        return 1;
    }
#endif
    int result = -1;
    if (const char* str = getenv("UNITTEST_EVERGREEN_TEST_RESULTS"); str && strlen(str) != 0) {
        std::cout << "Configuring evergreen reporter to store test results in " << str << std::endl;
        Catch::ConfigData config;
        config.reporterName = "evergreen";
        config.outputFilename = str;
        Catch::Session session;
        session.useConfigData(config);
        result = session.run();
    }
    else {
        result = Catch::Session().run(argc, argv);
    }
    return result < 0xff ? result : 0xff;
}

namespace Catch {
class EvergreenReporter : public CumulativeReporterBase<EvergreenReporter> {
public:
    using Base = CumulativeReporterBase<EvergreenReporter>;
    explicit EvergreenReporter(ReporterConfig const& config)
        : Base(config)
    {
    }
    ~EvergreenReporter() = default;
    static std::string getDescription()
    {
        return "Reports test results in a format consumable by Evergreen.";
    }
    void noMatchingTestCases(std::string const& /*spec*/) override {}
    using Base::testGroupEnded;
    using Base::testGroupStarting;
    using Base::testRunStarting;

    bool assertionEnded(AssertionStats const& assertionStats) override
    {
        if (!assertionStats.assertionResult.isOk()) {
            std::cerr << "Assertion failure: " << assertionStats.assertionResult.getSourceInfo() << std::endl;
            std::cerr << "\t from expresion: '" << assertionStats.assertionResult.getExpression() << "'" << std::endl;
            std::cerr << "\t with expansion: '" << assertionStats.assertionResult.getExpandedExpression() << "'"
                      << std::endl;
            for (auto& message : assertionStats.infoMessages) {
                std::cerr << "\t message: " << message.message << std::endl;
            }
            std::cerr << std::endl;
        }
        return true;
    }
    void testCaseStarting(TestCaseInfo const& testCaseInfo) override
    {
        m_results.emplace(std::make_pair(testCaseInfo.name, TestResult{}));
        Base::testCaseStarting(testCaseInfo);
    }
    void testCaseEnded(TestCaseStats const& testCaseStats) override
    {
        auto it = m_results.find(testCaseStats.testInfo.name);
        if (it == m_results.end()) {
            throw std::runtime_error("logic error in Evergreen section reporter, could not end test case '" +
                                     testCaseStats.testInfo.name + "' which was never tracked as started.");
        }
        if (testCaseStats.totals.assertions.allPassed()) {
            it->second.status = "pass";
        }
        else {
            it->second.status = "fail";
        }
        it->second.end_time = std::chrono::system_clock::now();
        Base::testCaseEnded(testCaseStats);
    }
    void sectionStarting(SectionInfo const& sectionInfo) override
    {
        if (m_pending_name.empty()) {
            m_pending_name = sectionInfo.name;
        }
        else {
            m_pending_name += "::" + sectionInfo.name;
        }
        m_pending_test = {};
        Base::sectionStarting(sectionInfo);
    }
    void sectionEnded(SectionStats const& sectionStats) override
    {
        if (!m_pending_name.empty()) {
            if (sectionStats.assertions.allPassed()) {
                m_pending_test.status = "pass";
            }
            else {
                m_pending_test.status = "fail";
            }
            m_pending_test.end_time = std::chrono::system_clock::now();
            m_results.emplace(std::make_pair(m_pending_name, m_pending_test));
            m_pending_name = "";
        }
        Base::sectionEnded(sectionStats);
    }
    void testRunEndedCumulative() override
    {
        auto results_arr = nlohmann::json::array();
        for (const auto& [test_name, cur_result] : m_results) {
            auto to_millis = [](const auto& tp) -> double {
                return static_cast<double>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count());
            };
            double start_secs = to_millis(cur_result.start_time) / 1000;
            double end_secs = to_millis(cur_result.end_time) / 1000;
            int exit_code = 0;
            if (cur_result.status != "pass") {
                exit_code = 1;
            }

            nlohmann::json cur_result_obj = {{"test_file", test_name}, {"status", cur_result.status},
                                             {"exit_code", exit_code}, {"start", start_secs},
                                             {"end", end_secs},        {"elapsed", end_secs - start_secs}};
            results_arr.push_back(std::move(cur_result_obj));
        }
        auto result_file_obj = nlohmann::json{{"results", std::move(results_arr)}};
        stream << result_file_obj << std::endl;
    }

    struct TestResult {
        TestResult()
            : start_time{std::chrono::system_clock::now()}
            , end_time{}
            , status{"unknown"}
        {
        }
        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
        std::string status;
    };
    TestResult m_pending_test;
    std::string m_pending_name;
    std::vector<std::pair<std::string, TestResult>> m_current_stack;
    std::map<std::string, TestResult> m_results;
};

CATCH_REGISTER_REPORTER("evergreen", EvergreenReporter)

} // end namespace Catch
