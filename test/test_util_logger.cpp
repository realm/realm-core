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

#include <memory>
#include <vector>
#include <locale>

#include <realm/util/logger.hpp>
#include <realm/util/hex_dump.hpp>
#include <realm/binary_data.hpp>

#include "test.hpp"

using namespace realm;

// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.

namespace {

TEST(Util_Logger_LevelToFromString)
{
    auto check = [&test_context = test_context](util::Logger::Level level, const char* name) {
        std::ostringstream out;
        out.imbue(std::locale::classic());
        out << level;
        CHECK_EQUAL(name, out.str());
        util::Logger::Level level_2;
        std::istringstream in(name);
        in.imbue(std::locale::classic());
        in.flags(in.flags() & ~std::ios_base::skipws); // Do not accept white space
        in >> level_2;
        CHECK(in && in.get() == std::char_traits<char>::eof());
        CHECK_EQUAL(level, level_2);
    };
    check(util::Logger::Level::all, "all");
    check(util::Logger::Level::trace, "trace");
    check(util::Logger::Level::debug, "debug");
    check(util::Logger::Level::detail, "detail");
    check(util::Logger::Level::info, "info");
    check(util::Logger::Level::warn, "warn");
    check(util::Logger::Level::error, "error");
    check(util::Logger::Level::fatal, "fatal");
    check(util::Logger::Level::off, "off");
}


TEST(Util_Logger_LevelThreshold)
{
    using namespace realm::util;
    auto base_logger = std::make_shared<StderrLogger>();
    auto threadsafe_logger = std::make_shared<ThreadSafeLogger>(base_logger);
    auto prefix_logger =
        PrefixLogger(util::LogCategory::realm, "test", threadsafe_logger); // created using Logger shared_ptr
    auto prefix_logger2 = PrefixLogger("test2", prefix_logger);            // created using PrefixLogger

    auto default_log_level = util::LogCategory::realm.get_default_level_threshold();
    CHECK(base_logger->get_level_threshold() == default_log_level);
    CHECK(threadsafe_logger->get_level_threshold() == default_log_level);
    CHECK(prefix_logger.get_level_threshold() == default_log_level);
    CHECK(prefix_logger2.get_level_threshold() == default_log_level);

    PrefixLogger storage_logger(LogCategory::storage, "test", threadsafe_logger);
    PrefixLogger query_logger(LogCategory::query, "test", threadsafe_logger);
    PrefixLogger sync_logger(LogCategory::sync, "test", threadsafe_logger);
    base_logger->set_level_threshold("Realm.Storage", Logger::Level::debug);
    CHECK(storage_logger.get_level_threshold() == Logger::Level::debug);
    CHECK(query_logger.get_level_threshold() == Logger::Level::debug);
    CHECK(sync_logger.get_level_threshold() == default_log_level);

    base_logger->set_level_threshold(Logger::Level::error);
    CHECK(base_logger->get_level_threshold() == Logger::Level::error);
    CHECK(threadsafe_logger->get_level_threshold() == Logger::Level::error);
    CHECK(prefix_logger.get_level_threshold() == Logger::Level::error);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::error);

    threadsafe_logger->set_level_threshold(Logger::Level::trace);
    CHECK(base_logger->get_level_threshold() == Logger::Level::trace);
    CHECK(threadsafe_logger->get_level_threshold() == Logger::Level::trace);
    CHECK(prefix_logger.get_level_threshold() == Logger::Level::trace);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::trace);

    prefix_logger.set_level_threshold(Logger::Level::debug);
    CHECK(base_logger->get_level_threshold() == Logger::Level::debug);
    CHECK(threadsafe_logger->get_level_threshold() == Logger::Level::debug);
    CHECK(prefix_logger.get_level_threshold() == Logger::Level::debug);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::debug);

    prefix_logger2.set_level_threshold(Logger::Level::info);
    CHECK(base_logger->get_level_threshold() == Logger::Level::info);
    CHECK(threadsafe_logger->get_level_threshold() == Logger::Level::info);
    CHECK(prefix_logger.get_level_threshold() == Logger::Level::info);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::info);

    auto ll_logger = std::make_shared<LocalThresholdLogger>(base_logger);
    auto ll_logger2 = std::make_shared<LocalThresholdLogger>(base_logger, Logger::Level::trace);
    CHECK(base_logger->get_level_threshold() == Logger::Level::info);
    CHECK(ll_logger->get_level_threshold() == Logger::Level::info);
    CHECK(ll_logger2->get_level_threshold() == Logger::Level::trace);

    ll_logger->set_level_threshold(Logger::Level::error);
    ll_logger2->set_level_threshold(Logger::Level::debug);
    CHECK(base_logger->get_level_threshold() == Logger::Level::info);
    CHECK(ll_logger->get_level_threshold() == Logger::Level::error);
    CHECK(ll_logger2->get_level_threshold() == Logger::Level::debug);
}


TEST(Util_Logger_LocalThresholdLogger)
{
    using namespace realm::util;
    // Get the original level
    auto orig_level = LogCategory::realm.get_default_level_threshold();

    auto base_logger = std::make_shared<StderrLogger>();
    auto lt_logger = std::make_shared<LocalThresholdLogger>(base_logger);
    auto lt_logger2 = std::make_shared<LocalThresholdLogger>(base_logger, Logger::Level::trace);
    auto prefix_logger = PrefixLogger(util::LogCategory::realm, "test", lt_logger);
    auto prefix_logger2 = PrefixLogger(util::LogCategory::realm, "test2", lt_logger2);

    CHECK(base_logger->get_level_threshold() == orig_level);
    CHECK(lt_logger->get_level_threshold() == orig_level);
    CHECK(lt_logger2->get_level_threshold() == Logger::Level::trace);
    CHECK(prefix_logger.get_level_threshold() == orig_level);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::trace);

    lt_logger->set_level_threshold(Logger::Level::error);
    lt_logger2->set_level_threshold(Logger::Level::debug);
    CHECK(base_logger->get_level_threshold() == orig_level);
    CHECK(lt_logger->get_level_threshold() == Logger::Level::error);
    CHECK(prefix_logger.get_level_threshold() == Logger::Level::error);
    CHECK(lt_logger2->get_level_threshold() == Logger::Level::debug);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::debug);

    prefix_logger.set_level_threshold(Logger::Level::off);
    prefix_logger2.set_level_threshold(Logger::Level::all);
    CHECK(base_logger->get_level_threshold() == orig_level);
    CHECK(lt_logger->get_level_threshold() == Logger::Level::off);
    CHECK(prefix_logger.get_level_threshold() == Logger::Level::off);
    CHECK(lt_logger2->get_level_threshold() == Logger::Level::all);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::all);

    base_logger->set_level_threshold(Logger::Level::error);
    CHECK(base_logger->get_level_threshold() == Logger::Level::error);
    CHECK(lt_logger->get_level_threshold() == Logger::Level::off);
    CHECK(prefix_logger.get_level_threshold() == Logger::Level::off);
    CHECK(lt_logger2->get_level_threshold() == Logger::Level::all);
    CHECK(prefix_logger2.get_level_threshold() == Logger::Level::all);

    CHECK_EQUAL(orig_level, LogCategory::realm.get_default_level_threshold());
}


TEST(Util_Logger_Categories)
{
    class StringLogger : public util::Logger {
    public:
        std::string operator()()
        {
            return std::move(message);
        }

    protected:
        void do_log(const util::LogCategory&, util::Logger::Level, const std::string& m) override
        {
            message += m;
        }
        std::string message;
    } logger;

    logger.log(util::LogCategory::query, util::Logger::Level::debug, "Query");
    CHECK(logger().empty());
    logger.set_level_threshold("Realm.Storage.Query", util::Logger::Level::debug);
    logger.log(util::LogCategory::object, util::Logger::Level::debug, "Object");
    logger.log(util::LogCategory::query, util::Logger::Level::debug, "Query");
    CHECK_EQUAL(logger(), "Query");
    logger.set_level_threshold("Realm.Storage.Object", util::Logger::Level::debug);
    logger.log(util::LogCategory::object, util::Logger::Level::debug, "Object");
    logger.log(util::LogCategory::query, util::Logger::Level::debug, "Query");
    CHECK_EQUAL(logger(), "ObjectQuery");
}


TEST(Util_Logger_Stream)
{
    std::ostringstream out_1;
    std::ostringstream out_2;
    out_2.imbue(std::locale::classic());
    {
        util::StreamLogger logger(out_1);
        for (int i = 0; i < 10; ++i) {
            std::ostringstream formatter;
            formatter << "Foo " << i;
            logger.info(formatter.str().c_str());
            out_2 << "Foo " << i << "\n";
        }
    }
    CHECK_EQUAL(out_1.str(), out_2.str());
}


TEST(Util_Logger_Formatting)
{
    std::ostringstream out_1;
    std::ostringstream out_2;
    out_2.imbue(std::locale::classic());
    {
        util::StreamLogger logger(out_1);
        logger.info("Foo %1", 1);
        out_2 << "Foo 1\n";
        logger.info("Foo %1 bar %2", "x", 2);
        out_2 << "Foo x bar 2\n";
        logger.info("Foo %2 bar %1", 3, "y");
        out_2 << "Foo y bar 3\n";
        logger.info("%3 foo %1 bar %2", 4.1, 4, "z");
        out_2 << "z foo 4.1 bar 4\n";
        logger.info("%1 foo %1 bar %1", "a");
        out_2 << "a foo a bar a\n";
    }
    CHECK(out_1.str() == out_2.str());
}


TEST(Util_Logger_File_1)
{
    TEST_PATH(path);

    std::ostringstream out;
    out.imbue(std::locale::classic());
    {
        util::FileLogger logger(path);
        for (int i = 0; i < 10; ++i) {
            logger.info("Foo %1", i);
            out << "Foo " << i << "\n";
        }
    }
    std::string str = out.str();
    size_t size = str.size();
    std::unique_ptr<char[]> buffer(new char[size]);
    util::File file(path);
    if (CHECK_EQUAL(size, file.get_size())) {
        file.read(0, buffer.get(), size);
        CHECK(str == std::string(buffer.get(), size));
    }
}


TEST(Util_Logger_File_2)
{
    TEST_PATH(path);

    std::ostringstream out;
    out.imbue(std::locale::classic());
    {
        util::FileLogger logger(util::File(path, util::File::mode_Write));
        for (int i = 0; i < 10; ++i) {
            logger.info("Foo %1", i);
            out << "Foo " << i << "\n";
        }
    }
    std::string str = out.str();
    size_t size = str.size();
    std::unique_ptr<char[]> buffer(new char[size]);
    util::File file(path);
    if (CHECK_EQUAL(size, file.get_size())) {
        file.read(0, buffer.get(), size);
        CHECK(str == std::string(buffer.get(), size));
    }
}

TEST(Util_Logger_Prefix)
{
    std::ostringstream out_1;
    std::ostringstream out_2;
    {
        auto root_logger = std::make_shared<util::StreamLogger>(out_1);
        util::PrefixLogger logger1(util::LogCategory::realm, "Prefix: ", root_logger);
        util::PrefixLogger logger2("Prefix2: ", logger1);
        logger1.info("Foo");
        out_2 << "Prefix: Foo\n";
        logger1.info("Bar");
        out_2 << "Prefix: Bar\n";
        logger2.info("Foo");
        out_2 << "Prefix: Prefix2: Foo\n";
        logger2.info("Bar");
        out_2 << "Prefix: Prefix2: Bar\n";
    }
    CHECK(out_1.str() == out_2.str());
}


TEST(Util_Logger_ThreadSafe)
{
    struct BalloonLogger : public util::Logger {
        std::vector<std::string> messages;

    protected:
        void do_log(const util::LogCategory&, util::Logger::Level, const std::string& message) override
        {
            messages.push_back(std::move(message));
        }
    };
    auto root_logger = std::make_shared<BalloonLogger>();
    util::ThreadSafeLogger logger(root_logger);

    const long num_iterations = 10000;
    auto func = [&](int i) {
        for (long j = 0; j < num_iterations; ++j)
            logger.info("%1:%2", i, j);
    };

    const int num_threads = 8;
    std::unique_ptr<test_util::ThreadWrapper[]> threads(new test_util::ThreadWrapper[num_threads]);
    for (int i = 0; i < num_threads; ++i)
        threads[i].start([&func, i] {
            func(i);
        });
    for (int i = 0; i < num_threads; ++i)
        CHECK_NOT(threads[i].join());

    std::vector<std::string> messages_1(std::move(root_logger->messages)), messages_2;
    for (int i = 0; i < num_threads; ++i) {
        for (long j = 0; j < num_iterations; ++j) {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << i << ":" << j;
            messages_2.push_back(out.str());
        }
    }

    std::sort(messages_1.begin(), messages_1.end());
    std::sort(messages_2.begin(), messages_2.end());
    CHECK(messages_1 == messages_2);
}

TEST(Util_HexDump)
{
    const unsigned char u_char_data[] = {0x00, 0x05, 0x10, 0x17, 0xff};
    const signed char s_char_data[] = {0, 5, 10, -5, -1};
    const char char_data[] = {0, 5, 10, char(-5), char(-1)};

    std::string str1 = util::hex_dump(u_char_data, sizeof(u_char_data));
    CHECK_EQUAL(str1, "00 05 10 17 FF");
    // std::cout << str1 << std::endl;

    std::string str2 = util::hex_dump(s_char_data, sizeof(s_char_data));
    CHECK_EQUAL(str2, "00 05 0A FB FF");
    // std::cout << str2 << std::endl;

    std::string str3 = util::hex_dump(char_data, sizeof(char_data));
    CHECK_EQUAL(str3, "00 05 0A FB FF");
    // std::cout << str3 << std::endl;
}

} // unnamed namespace
