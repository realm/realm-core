#include "testsettings.hpp"
#ifdef TEST_FILE

#include <array>

#include "realm/util/cli_args.hpp"

#include "test.hpp"

using namespace realm::util;

TEST(CliArgs_Basic)
{
    CliArgumentParser arg_parser;
    CliFlag flag(arg_parser, "foo", 'f');
    CliArgument arg(arg_parser, "bar", 'b');
    CliFlag missing(arg_parser, "missing");
    auto to_parse = std::array<const char*, 5>({"yolo", "--foo", "--bar", "bizz", "buzz"});

    auto results = arg_parser.parse(static_cast<int>(to_parse.size()), to_parse.data());
    CHECK_EQUAL(results.program_name, "yolo");
    CHECK_EQUAL(results.unmatched_arguments.size(), 1);
    CHECK_EQUAL(results.unmatched_arguments[0], "buzz");
    CHECK(flag);
    CHECK(arg);
    CHECK(!missing);
    CHECK_EQUAL(arg.value(), "bizz");
}

TEST(CliArgs_Short)
{
    CliArgumentParser arg_parser;
    CliFlag flag(arg_parser, "foo", 'f');
    CliArgument arg(arg_parser, "bar", 'b');
    auto to_parse = std::array<const char*, 5>({"yolo", "-f", "-b", "bizz", "buzz"});

    auto results = arg_parser.parse(static_cast<int>(to_parse.size()), to_parse.data());
    CHECK_EQUAL(results.program_name, "yolo");
    CHECK_EQUAL(results.unmatched_arguments.size(), 1);
    CHECK_EQUAL(results.unmatched_arguments[0], "buzz");
    CHECK(flag);
    CHECK(arg);
    CHECK_EQUAL(arg.value(), "bizz");
}

TEST(CliArgs_Mixed)
{
    CliArgumentParser arg_parser;
    CliFlag flag(arg_parser, "foo", 'f');
    CliArgument arg(arg_parser, "bar", 'b');
    auto to_parse = std::array<const char*, 5>({"yolo", "-f", "--bar", "bizz", "buzz"});

    auto results = arg_parser.parse(static_cast<int>(to_parse.size()), to_parse.data());
    CHECK_EQUAL(results.program_name, "yolo");
    CHECK_EQUAL(results.unmatched_arguments.size(), 1);
    CHECK_EQUAL(results.unmatched_arguments[0], "buzz");
    CHECK(flag);
    CHECK(arg);
    CHECK_EQUAL(arg.value(), "bizz");
}

TEST(CliArgs_EqAssign)
{
    CliArgumentParser arg_parser;
    CliArgument arg_int(arg_parser, "bar");
    CliArgument arg_str(arg_parser, "bizz");
    auto to_parse = std::array<const char*, 4>({"yolo", "--bar=6", "--bizz", "buzz"});

    auto results = arg_parser.parse(static_cast<int>(to_parse.size()), to_parse.data());
    CHECK_EQUAL(results.program_name, "yolo");
    CHECK_EQUAL(results.unmatched_arguments.size(), 0);
    CHECK(arg_int);
    CHECK(arg_str);
    CHECK_EQUAL(arg_str.as<std::string>(), "buzz");
    CHECK_EQUAL(arg_int.as<std::string>(), "6");
    CHECK_EQUAL(arg_int.as<int64_t>(), 6);
}

TEST(CliArgs_IntegersMixed)
{
    CliArgumentParser arg_parser;
    CliFlag flag(arg_parser, "foo", 'f');
    CliArgument arg_int(arg_parser, "bar");
    CliArgument arg_str(arg_parser, "bizz");
    auto to_parse = std::array<const char*, 6>({"yolo", "-f", "--bar", "6", "--bizz", "buzz"});

    auto results = arg_parser.parse(static_cast<int>(to_parse.size()), to_parse.data());
    CHECK_EQUAL(results.program_name, "yolo");
    CHECK_EQUAL(results.unmatched_arguments.size(), 0);
    CHECK(flag);
    CHECK(arg_int);
    CHECK(arg_str);
    CHECK_THROW(arg_str.as<int64_t>(), CliParseException);
    CHECK_NOTHROW(arg_str.as<std::string>());
    CHECK_EQUAL(arg_str.as<std::string>(), "buzz");
    CHECK_NOTHROW(arg_int.as<std::string>());
    CHECK_EQUAL(arg_int.as<std::string>(), "6");
    CHECK_NOTHROW(arg_int.as<int64_t>());
    CHECK_EQUAL(arg_int.as<int64_t>(), 6);
}
#endif // TEST_FILE
