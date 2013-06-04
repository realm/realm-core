#include <UnitTest++.h>

#include <tightdb/file.hpp>

using namespace tightdb;

TEST(File_ExistsAndRemove)
{
    File("test_file", File::mode_Write);
    CHECK(File::exists("test_file"));
    CHECK(File::try_remove("test_file"));
    CHECK(!File::exists("test_file"));
    CHECK(!File::try_remove("test_file"));
}

TEST(File_IsSame)
{
    {
        File f1("test_file_1", File::mode_Write);
        File f2("test_file_1", File::mode_Read);
        File f3("test_file_2", File::mode_Write);

        CHECK(f1.is_same_file(f1));
        CHECK(f1.is_same_file(f2));
        CHECK(!f1.is_same_file(f3));
        CHECK(!f2.is_same_file(f3));
    }
    File::try_remove("test_file_1");
    File::try_remove("test_file_2");
}
