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

#define CATCH_CONFIG_EXTERNAL_INTERFACES
#include <catch2/catch.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include "test_utils.hpp"

#include <realm/util/base64.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/util/file.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/time.hpp>
#include <realm/string_data.hpp>

#include <external/json/json.hpp>

#include <iomanip>
#include <iostream>
#include <fstream>

namespace realm {

bool create_dummy_realm(std::string path)
{
    Realm::Config config;
    config.path = path;
    try {
        _impl::RealmCoordinator::get_coordinator(path)->get_realm(config, none);
        REQUIRE_REALM_EXISTS(path);
        return true;
    }
    catch (std::exception&) {
        return false;
    }
}

void reset_test_directory(const std::string& base_path)
{
    util::try_remove_dir_recursive(base_path);
    util::make_dir(base_path);
}

std::vector<char> make_test_encryption_key(const char start)
{
    std::vector<char> vector;
    vector.reserve(64);
    for (int i = 0; i < 64; i++) {
        vector.emplace_back((start + i) % 128);
    }
    return vector;
}

// FIXME: Catch2 limitation on old compilers (currently our android CI)
// https://github.com/catchorg/Catch2/blob/master/docs/limitations.md#clangg----skipping-leaf-sections-after-an-exception
void catch2_ensure_section_run_workaround(bool did_run_a_section, std::string section_name,
                                          util::FunctionRef<void()> func)
{
    if (did_run_a_section) {
        func();
    }
    else {
        std::cout << "Skipping test section '" << section_name << "' on this run." << std::endl;
    }
}

std::string encode_fake_jwt(const std::string& in, util::Optional<int64_t> exp, util::Optional<int64_t> iat)
{
    // by default make a valid expiry time so that the sync session pre check
    // doesn't trigger a token refresh on first open
    using namespace std::chrono_literals;
    if (!exp) {
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        exp = std::chrono::system_clock::to_time_t(now + 30min);
    }
    if (!iat) {
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        iat = std::chrono::system_clock::to_time_t(now - 1s);
    }

    std::string unencoded_prefix = nlohmann::json({"alg", "HS256"}).dump();
    std::string unencoded_body =
        nlohmann::json(
            {{"user_data", {{"token", in}}}, {"exp", *exp}, {"iat", *iat}, {"access", {"download", "upload"}}})
            .dump();

    std::string encoded_prefix, encoded_body;
    encoded_prefix.resize(util::base64_encoded_size(unencoded_prefix.size()));
    encoded_body.resize(util::base64_encoded_size(unencoded_body.size()));
    util::base64_encode(unencoded_prefix.data(), unencoded_prefix.size(), &encoded_prefix[0], encoded_prefix.size());
    util::base64_encode(unencoded_body.data(), unencoded_body.size(), &encoded_body[0], encoded_body.size());
    std::string suffix = "Et9HFtf9R3GEMA0IICOfFMVXY7kkTX1wr4qCyhIf58U";
    return encoded_prefix + "." + encoded_body + "." + suffix;
}

class EvergreenLogger : public util::RootLogger, public util::AtomicRefCountBase {
public:
    EvergreenLogger(std::ofstream stream)
        : m_out_file(std::move(stream))
    {
    }

    uint64_t cur_line() const
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        return m_lines_written;
    }

    void flush()
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        do_flush();
    }

protected:
    void do_log(Level level, const std::string& message) final
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        ++m_lines_written;
        m_out_file << get_level_prefix(level) << message << '\n';
    }

private:
    void do_flush()
    {
        m_out_file.flush();
    }

    mutable std::mutex m_mutex;
    uint64_t m_lines_written = 0;
    std::ofstream m_out_file;
};

class ForwardingLogger : public util::RootLogger {
public:
    explicit ForwardingLogger(util::bind_ptr<EvergreenLogger> logger)
        : m_wrapped_logger(logger)
    {
        set_level_threshold(Level::all);
    }

protected:
    void do_log(Level level, const std::string& message) final
    {
        Logger::do_log(*m_wrapped_logger, level, message);
    }

private:
    util::bind_ptr<EvergreenLogger> m_wrapped_logger;
};

using TestReporterFactory = std::function<std::unique_ptr<util::Logger>(util::Logger::Level)>;
using MaybeTestReporterFactory = util::Optional<TestReporterFactory>;

MaybeTestReporterFactory update_and_get_logger_factory(util::Optional<util::bind_ptr<EvergreenLogger>> maybe_logger)
{
    static std::mutex g_reporter_factory_mutex;
    static util::Optional<TestReporterFactory> g_reporter_factory = util::none;
    std::lock_guard<std::mutex> lk(g_reporter_factory_mutex);
    if (maybe_logger) {
        auto logger = *maybe_logger;
        if (logger) {
            g_reporter_factory.emplace([logger = std::move(logger)](util::Logger::Level) {
                return std::make_unique<ForwardingLogger>(logger);
            });
        }
        else {
            g_reporter_factory = util::none;
        }
    }
    return g_reporter_factory;
}

MaybeTestReporterFactory get_test_reporter_logger()
{
    return update_and_get_logger_factory(util::none);
}

} // namespace realm

namespace Catch {
class EvergreenReporter : public CumulativeReporterBase<EvergreenReporter> {
public:
    using Base = CumulativeReporterBase<EvergreenReporter>;
    explicit EvergreenReporter(ReporterConfig const& config)
        : Base(config)
    {
        auto getenv_sv = [](auto name) {
            auto ptr = ::getenv(name);
            if (!ptr) {
                return std::string_view{};
            }
            return std::string_view(ptr);
        };
        if (auto should_log = getenv_sv("UNITTEST_LOG_TO_FILES"); !should_log.empty()) {
            std::string log_file_name(getenv_sv("UNITTEST_LOG_FILE_PREFIX"));
            if (log_file_name.empty()) {
                log_file_name = "realm-object-store-tests.log";
            }
            else {
                log_file_name += ".log";
            }

            std::ostringstream out;
            out.imbue(std::locale::classic());
            auto tm = realm::util::localtime(::time(nullptr));
            out << "./test_logs_" << std::put_time(&tm, "%Y%m%d_%H%M%S");

            auto dir_name = out.str();
            realm::util::make_dir(dir_name);

            auto log_file_path = realm::util::File::resolve(log_file_name, dir_name);
            std::ofstream log_file(log_file_path);
            if (log_file.fail()) {
                throw std::runtime_error("Cannot open log file at " + log_file_path);
            }
            m_logger = realm::util::bind_ptr<realm::EvergreenLogger>(new realm::EvergreenLogger(std::move(log_file)));
            m_logger->set_level_threshold(realm::util::Logger::Level::all);
            realm::update_and_get_logger_factory(m_logger);
        }
    }
    ~EvergreenReporter()
    {
        realm::update_and_get_logger_factory(realm::util::bind_ptr<realm::EvergreenLogger>(nullptr));
    }

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
        if (m_logger) {
            m_logger->info("Beginning test case \"%1\"", testCaseInfo.name);
            m_logger->flush();
            m_results.emplace(std::make_pair(testCaseInfo.name, TestResult{m_logger->cur_line()}));
        }
        else {
            m_results.emplace(std::make_pair(testCaseInfo.name, TestResult{}));
        }
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
        if (m_logger) {
            m_logger->info("Ending test case \"%1\"", testCaseStats.testInfo.name);
            m_logger->flush();
        }
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
        if (m_logger) {
            m_logger->info("Beginning test section \"%1\"", m_pending_name);
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
        if (m_logger) {
            m_logger->info("Ending test section %1", sectionStats.sectionInfo.name);
        }
        Base::sectionEnded(sectionStats);
    }
    void testRunEndedCumulative() override
    {
        if (m_logger) {
            m_logger->flush();
        }

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

            nlohmann::json cur_result_obj = {{"test_file", test_name},
                                             {"status", cur_result.status},
                                             {"exit_code", exit_code},
                                             {"start", start_secs},
                                             {"end", end_secs},
                                             {"elapsed", end_secs - start_secs},
                                             {"line_num", cur_result.start_line}};
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

        explicit TestResult(uint64_t start_line)
            : start_time{std::chrono::system_clock::now()}
            , end_time{}
            , start_line(start_line)
            , status{"unknown"}
        {
        }

        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
        uint64_t start_line = 0;
        std::string status;
    };
    TestResult m_pending_test;
    std::string m_pending_name;
    std::vector<std::pair<std::string, TestResult>> m_current_stack;
    std::map<std::string, TestResult> m_results;
    realm::util::bind_ptr<realm::EvergreenLogger> m_logger;
};

CATCH_REGISTER_REPORTER("evergreen", EvergreenReporter)

} // end namespace Catch
