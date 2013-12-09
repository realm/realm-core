#include "testsettings.hpp"
#ifdef TEST_FILE

#include <sstream>
#include <ostream>

#include <UnitTest++.h>

#include <tightdb/util/file.hpp>

using namespace std;
using namespace tightdb;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

TEST(File_ExistsAndRemove)
{
    util::File("test_file", util::File::mode_Write);
    CHECK(util::File::exists("test_file"));
    CHECK(util::File::try_remove("test_file"));
    CHECK(!util::File::exists("test_file"));
    CHECK(!util::File::try_remove("test_file"));
}

TEST(File_IsSame)
{
    {
        util::File f1("test_file_1", util::File::mode_Write);
        util::File f2("test_file_1", util::File::mode_Read);
        util::File f3("test_file_2", util::File::mode_Write);

        CHECK(f1.is_same_file(f1));
        CHECK(f1.is_same_file(f2));
        CHECK(!f1.is_same_file(f3));
        CHECK(!f2.is_same_file(f3));
    }
    util::File::try_remove("test_file_1");
    util::File::try_remove("test_file_2");
}

TEST(File_Streambuf)
{
    {
        util::File f("test_file", util::File::mode_Write);
        util::File::Streambuf b(&f);
        ostream out(&b);
        out << "Line " << 1 << endl;
        out << "Line " << 2 << endl;
    }
    {
        util::File f("test_file", util::File::mode_Read);
        char buffer[256];
        size_t n = f.read(buffer);
        string s_1(buffer, buffer+n);
        ostringstream out;
        out << "Line " << 1 << endl;
        out << "Line " << 2 << endl;
        string s_2 = out.str();
        CHECK(s_1 == s_2);
    }
    util::File::try_remove("test_file");
}

#endif // TEST_FILE
