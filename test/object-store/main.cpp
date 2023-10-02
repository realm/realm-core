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

#include <util/crypt_key.hpp>
#include <util/test_path.hpp>

#include <realm/util/features.h>
#include <realm/util/to_string.hpp>

#if TEST_SCHEDULER_UV
#include <realm/object-store/util/uv/scheduler.hpp>
#endif

#include <catch2/catch_all.hpp>
#include <catch2/reporters/catch_reporter_cumulative_base.hpp>
#include <catch2/catch_test_case_info.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include <external/json/json.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits.h>


int main(int argc, const char** argv)
{
    realm::test_util::initialize_test_path(1, argv);

    Catch::ConfigData config;

    if (const char* str = getenv("UNITTEST_EVERGREEN_TEST_RESULTS"); str && strlen(str) != 0) {
        std::cout << "Configuring evergreen reporter to store test results in " << str << std::endl;
        // If the output file already exists, make a copy so these results can be appended to it
        std::map<std::string, std::string> custom_options;
        if (std::filesystem::exists(str)) {
            std::string results_copy = realm::util::format("%1.bak", str);
            std::filesystem::copy(str, results_copy, std::filesystem::copy_options::overwrite_existing);
            custom_options["json_file"] = results_copy;
            std::cout << "Existing results file copied to " << results_copy << std::endl;
        }
        config.showDurations = Catch::ShowDurations::Always; // this is to help debug hangs in Evergreen
        config.reporterSpecifications.push_back(Catch::ReporterSpec{"console", {}, {}, {}});
        config.reporterSpecifications.push_back(
            Catch::ReporterSpec{"evergreen", {str}, {}, std::move(custom_options)});
    }
    else if (const char* str = getenv("UNITTEST_XML"); str && strlen(str) != 0) {
        std::cout << "Configuring jUnit reporter to store test results in " << str << std::endl;
        config.showDurations = Catch::ShowDurations::Always; // this is to help debug hangs in Jenkins
        config.reporterSpecifications.push_back(Catch::ReporterSpec{"console", {}, {}, {}});
        config.reporterSpecifications.push_back(Catch::ReporterSpec{"junit", {str}, {}, {}});
    }

    if (const char* env = getenv("UNITTEST_ENCRYPT_ALL")) {
        std::string str(env);
        for (auto& c : str) {
            c = tolower(c);
        }
        if (str == "1" || str == "on" || str == "yes") {
            realm::test_util::enable_always_encrypt();
        }
    }

#if TEST_SCHEDULER_UV
    realm::util::Scheduler::set_default_factory([]() {
        return std::make_shared<realm::util::UvMainLoopScheduler>();
    });
#endif

    Catch::Session session;
    session.useConfigData(config);
    int result = session.run(argc, argv);
    return result < 0xff ? result : 0xff;
}

namespace Catch {
class EvergreenReporter : public CumulativeReporterBase {
public:
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

    using Base = CumulativeReporterBase;
    using CumulativeReporterBase::CumulativeReporterBase;
    static std::string getDescription()
    {
        return "Reports test results in a format consumable by Evergreen.";
    }

    void assertionEnded(AssertionStats const& assertionStats) override
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
    }
    void testCaseStarting(TestCaseInfo const& testCaseInfo) override
    {
        m_results.emplace(std::make_pair(testCaseInfo.name, TestResult{}));
        Base::testCaseStarting(testCaseInfo);
    }
    void testCaseEnded(TestCaseStats const& testCaseStats) override
    {
        auto it = m_results.find(testCaseStats.testInfo->name);
        if (it == m_results.end()) {
            throw std::runtime_error("logic error in Evergreen section reporter, could not end test case '" +
                                     testCaseStats.testInfo->name + "' which was never tracked as started.");
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
        auto& options = m_customOptions;
        auto& json_file = options["json_file"];
        nlohmann::json results_arr = nlohmann::json::array();
        // If the results file already exists, include the results from that file
        try {
            if (!json_file.empty() && std::filesystem::exists(json_file)) {
                std::ifstream f(json_file);
                // Make sure the file was successfully opened and is not empty
                if (f.is_open() && !f.eof()) {
                    nlohmann::json existing_data = nlohmann::json::parse(f);
                    auto results = existing_data.find("results");
                    if (results != existing_data.end() && results->is_array()) {
                        std::cout << "Appending tests from previous results" << std::endl;
                        results_arr = *results;
                    }
                }
            }
        }
        catch (nlohmann::json::exception&) {
            // json parse error, ignore the entries
        }
        catch (std::exception&) {
            // unable to open/read file
        }
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
        m_stream << result_file_obj << std::endl;
        if (!json_file.empty() && std::filesystem::exists(json_file)) {
            // Delete the old results file
            std::filesystem::remove(json_file);
        }
    }

    TestResult m_pending_test;
    std::string m_pending_name;
    std::vector<std::pair<std::string, TestResult>> m_current_stack;
    std::map<std::string, TestResult> m_results;
};

CATCH_REGISTER_REPORTER("evergreen", EvergreenReporter)

} // end namespace Catch
