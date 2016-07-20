#include <realm/util/memory_stream.hpp>

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

TEST(util_Memory_stream_input)
{
	const char buf[] = "123 4567";
	realm::util::MemoryInputStream in;
	in.set_buffer(buf, buf + sizeof(buf) - 1);
	in.unsetf(std::ios_base::skipws);

	CHECK_EQUAL(in.eof(), false);
	CHECK_EQUAL(in.tellg(), 0);

	int number;
	char sp;

 	in >> number;
	CHECK_EQUAL(number, 123);
	CHECK_EQUAL(in.eof(), false);
	CHECK_EQUAL(in.tellg(), 3);

	in >> sp;
	CHECK_EQUAL(sp, ' ');
	CHECK_EQUAL(in.eof(), false);
	CHECK_EQUAL(in.tellg(), 4);

	in.seekg(1);
	in >> number;
	CHECK_EQUAL(number, 23);
	CHECK_EQUAL(in.eof(), false);
	CHECK_EQUAL(in.tellg(), 3);

	in.seekg(5);
	in >> number;
	CHECK_EQUAL(number, 567);
	CHECK_EQUAL(in.eof(), true);
	CHECK_EQUAL(in.tellg(), -1);
}

} // unnamed namespace

