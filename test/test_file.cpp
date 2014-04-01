#include "testsettings.hpp"
#ifdef TEST_FILE

#include <sstream>
#include <ostream>

#include <tightdb/util/file.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb::util;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

TEST(File_ExistsAndRemove)
{
    TEST_PATH(path);
    File(path, File::mode_Write);
    CHECK(File::exists(path));
    CHECK(File::try_remove(path));
    CHECK(!File::exists(path));
    CHECK(!File::try_remove(path));
}

TEST(File_IsSame)
{
    TEST_PATH(path_1);
    TEST_PATH(path_2);
    {
        File f1(path_1, File::mode_Write);
        File f2(path_1, File::mode_Read);
        File f3(path_2, File::mode_Write);

        CHECK(f1.is_same_file(f1));
        CHECK(f1.is_same_file(f2));
        CHECK(!f1.is_same_file(f3));
        CHECK(!f2.is_same_file(f3));
    }
}

TEST(File_Streambuf)
{
    TEST_PATH(path);
    {
        File f(path, File::mode_Write);
        File::Streambuf b(&f);
        ostream out(&b);
        out << "Line " << 1 << endl;
        out << "Line " << 2 << endl;
    }
    {
        File f(path, File::mode_Read);
        char buffer[256];
        size_t n = f.read(buffer);
        string s_1(buffer, buffer+n);
        ostringstream out;
        out << "Line " << 1 << endl;
        out << "Line " << 2 << endl;
        string s_2 = out.str();
        CHECK(s_1 == s_2);
    }
}

#endif // TEST_FILE
