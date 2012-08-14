#include <UnitTest++.h>
#include <tightdb/column_mixed.hpp>

using namespace tightdb;

TEST(ColumnMixed_Int)
{
    ColumnMixed c;

    c.insert_int(0,     2);
    c.insert_int(1,   100);
    c.insert_int(2, 20000);
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_INT, c.GetType(i));
    }

    CHECK_EQUAL(    2, c.GetInt(0));
    CHECK_EQUAL(  100, c.GetInt(1));
    CHECK_EQUAL(20000, c.GetInt(2));

    c.SetInt(0,   400);
    c.SetInt(1,     0);
    c.SetInt(2, 99999);

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_INT, c.GetType(i));
    }

    CHECK_EQUAL(  400, c.GetInt(0));
    CHECK_EQUAL(    0, c.GetInt(1));
    CHECK_EQUAL(99999, c.GetInt(2));
    CHECK_EQUAL(3, c.Size());

    c.Destroy();
}

TEST(ColumnMixed_Bool)
{
    ColumnMixed c;

    c.insert_bool(0, true);
    c.insert_bool(1, false);
    c.insert_bool(2, true);
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BOOL, c.GetType(i));
    }

    CHECK_EQUAL(true,  c.get_bool(0));
    CHECK_EQUAL(false, c.get_bool(1));
    CHECK_EQUAL(true,  c.get_bool(2));

    c.set_bool(0, false);
    c.set_bool(1, true);
    c.set_bool(2, false);
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BOOL, c.GetType(i));
    }

    CHECK_EQUAL(false, c.get_bool(0));
    CHECK_EQUAL(true,  c.get_bool(1));
    CHECK_EQUAL(false, c.get_bool(2));

    c.Destroy();
}

TEST(ColumnMixed_Date)
{
    ColumnMixed c;

    c.insert_date(0,     2);
    c.insert_date(1,   100);
    c.insert_date(2, 20000);
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_DATE, c.GetType(i));
    }

    CHECK_EQUAL(    2, c.get_date(0));
    CHECK_EQUAL(  100, c.get_date(1));
    CHECK_EQUAL(20000, c.get_date(2));

    c.set_date(0,   400);
    c.set_date(1,     0);
    c.set_date(2, 99999);

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_DATE, c.GetType(i));
    }

    CHECK_EQUAL(  400, c.get_date(0));
    CHECK_EQUAL(    0, c.get_date(1));
    CHECK_EQUAL(99999, c.get_date(2));
    CHECK_EQUAL(3, c.Size());

    c.Destroy();
}

TEST(ColumnMixed_String)
{
    ColumnMixed c;

    c.insert_string(0, "aaa");
    c.insert_string(1, "bbbbb");
    c.insert_string(2, "ccccccc");
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(i));
    }

    CHECK_EQUAL("aaa",     c.get_string(0));
    CHECK_EQUAL("bbbbb",   c.get_string(1));
    CHECK_EQUAL("ccccccc", c.get_string(2));

    c.set_string(0, "dd");
    c.set_string(1, "");
    c.set_string(2, "eeeeeeeee");
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(i));
    }

    CHECK_EQUAL("dd",        c.get_string(0));
    CHECK_EQUAL("",          c.get_string(1));
    CHECK_EQUAL("eeeeeeeee", c.get_string(2));

    c.Destroy();
}

TEST(ColumnMixed_Binary)
{
    ColumnMixed c;

    c.insert_binary(0, "aaa", 4);
    c.insert_binary(1, "bbbbb", 6);
    c.insert_binary(2, "ccccccc", 8);
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(i));
    }

    CHECK_EQUAL("aaa",     (const char*)c.get_binary(0).pointer);
    CHECK_EQUAL("bbbbb",   (const char*)c.get_binary(1).pointer);
    CHECK_EQUAL("ccccccc", (const char*)c.get_binary(2).pointer);

    c.set_binary(0, "dd", 3);
    c.set_binary(1, "", 1);
    c.set_binary(2, "eeeeeeeee", 10);
    CHECK_EQUAL(3, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(i));
    }

    CHECK_EQUAL("dd",        (const char*)c.get_binary(0).pointer);
    CHECK_EQUAL("",          (const char*)c.get_binary(1).pointer);
    CHECK_EQUAL("eeeeeeeee", (const char*)c.get_binary(2).pointer);

    c.Destroy();
}

TEST(ColumnMixed_Table)
{
    ColumnMixed c;

    c.insert_subtable(0);
    c.insert_subtable(1);
    CHECK_EQUAL(2, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_TABLE, c.GetType(i));
    }

    Table* const t1 = c.get_subtable_ptr(0);
    Table* const t2 = c.get_subtable_ptr(1);
    CHECK(t1->is_empty());
    CHECK(t2->is_empty());
    delete t1;
    delete t2;

    c.Destroy();
}

TEST(ColumnMixed_Mixed)
{
    ColumnMixed c;

    // Insert mixed types
    c.insert_int(0, 23);
    c.insert_bool(0, false);
    c.insert_date(0, 23423);
    c.insert_string(0, "Hello");
    c.insert_binary(0, "binary", 7);
    c.insert_subtable(0);
    CHECK_EQUAL(6, c.Size());

    CHECK_EQUAL(COLUMN_TYPE_TABLE,  c.GetType(0));
    CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(1));
    CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(2));
    CHECK_EQUAL(COLUMN_TYPE_DATE,   c.GetType(3));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   c.GetType(4));
    CHECK_EQUAL(COLUMN_TYPE_INT,    c.GetType(5));

    // Change all entries to new types
    c.SetInt(0, 23);
    c.set_bool(1, false);
    c.set_date(2, 23423);
    c.set_string(3, "Hello");
    c.set_binary(4, "binary", 7);
    c.set_subtable(5);
    CHECK_EQUAL(6, c.Size());

    CHECK_EQUAL(COLUMN_TYPE_TABLE,  c.GetType(5));
    CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(4));
    CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(3));
    CHECK_EQUAL(COLUMN_TYPE_DATE,   c.GetType(2));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   c.GetType(1));
    CHECK_EQUAL(COLUMN_TYPE_INT,    c.GetType(0));

    c.Destroy();
}
