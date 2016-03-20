#include <memory>
#include <vector>
#include <locale>

#include <realm/util/logger.hpp>

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

TEST(Util_Logger_Stream)
{
    std::ostringstream out_1;
    std::ostringstream out_2;
    out_2.imbue(std::locale::classic());
    {
        util::StreamLogger logger(out_1);
        for (int i = 0; i < 10; ++i) {
            std::ostringstream formatter;
            formatter << "Foo "<<i;
            logger.log(formatter.str().c_str());
            out_2 << "Foo "<<i<<"\n";
        }
    }
    CHECK(out_1.str() == out_2.str());
}


TEST(Util_Logger_Formatting)
{
    std::ostringstream out_1;
    std::ostringstream out_2;
    out_2.imbue(std::locale::classic());
    {
        util::StreamLogger logger(out_1);
        logger.log("Foo %1", 1);
        out_2 << "Foo 1\n";
        logger.log("Foo %1 bar %2", "x", 2);
        out_2 << "Foo x bar 2\n";
        logger.log("Foo %2 bar %1", 3, "y");
        out_2 << "Foo y bar 3\n";
        logger.log("%3 foo %1 bar %2", 4.1, 4, "z");
        out_2 << "z foo 4.1 bar 4\n";
        logger.log("Foo %1");
        out_2 << "Foo %1\n";
        logger.log("Foo %1 bar %2", "x");
        out_2 << "Foo x bar %2\n";
        logger.log("Foo %2 bar %1", "x");
        out_2 << "Foo %2 bar x\n";
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
            logger.log("Foo %1", i);
            out << "Foo "<<i<<"\n";
        }
    }
    std::string str = out.str();
    size_t size = str.size();
    std::unique_ptr<char[]> buffer(new char[size]);
    util::File file(path);
    if (CHECK_EQUAL(size, file.get_size())) {
        file.read(buffer.get(), size);
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
            logger.log("Foo %1", i);
            out << "Foo "<<i<<"\n";
        }
    }
    std::string str = out.str();
    size_t size = str.size();
    std::unique_ptr<char[]> buffer(new char[size]);
    util::File file(path);
    if (CHECK_EQUAL(size, file.get_size())) {
        file.read(buffer.get(), size);
        CHECK(str == std::string(buffer.get(), size));
    }
}


TEST(Util_Logger_Prefix)
{
    std::ostringstream out_1;
    std::ostringstream out_2;
    {
        util::StreamLogger base_logger(out_1);
        util::PrefixLogger logger("Prefix: ", base_logger);
        logger.log("Foo");
        out_2 << "Prefix: Foo\n";
        logger.log("Bar");
        out_2 << "Prefix: Bar\n";
    }
    CHECK(out_1.str() == out_2.str());
}


TEST(Util_Logger_ThreadSafe)
{
    struct BaseLogger: public util::Logger {
        std::vector<std::string> messages;
        void do_log(std::string message) override
        {
            messages.push_back(std::move(message));
        }
    };
    BaseLogger base_logger;
    util::ThreadSafeLogger logger(base_logger);

    const long num_iterations = 10000;
    auto func = [&](int i) {
        for (long j = 0; j < num_iterations; ++j)
            logger.log("%1:%2", i, j);
    };

    const int num_threads = 8;
    std::unique_ptr<test_util::ThreadWrapper[]> threads(new test_util::ThreadWrapper[num_threads]);
    for (int i = 0; i < num_threads; ++i)
        threads[i].start([&func, i] { func(i); });
    for (int i = 0; i < num_threads; ++i)
        CHECK_NOT(threads[i].join());

    std::vector<std::string> messages_1(std::move(base_logger.messages)), messages_2;
    for (int i = 0; i < num_threads; ++i) {
        for (long j = 0; j < num_iterations; ++j) {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << i<<":"<<j;
            messages_2.push_back(out.str());
        }
    }

    std::sort(messages_1.begin(), messages_1.end());
    std::sort(messages_2.begin(), messages_2.end());
    CHECK(messages_1 == messages_2);
}

} // unnamed namespace
