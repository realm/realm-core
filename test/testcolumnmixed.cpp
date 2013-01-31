#include <UnitTest++.h>
#include <tightdb/column_mixed.hpp>

#include <limits>

using namespace tightdb;

TEST(ColumnMixed_Int)
{
    ColumnMixed c;
    const int64_t maxval = std::numeric_limits<int64_t>::max();
    const int64_t minval = std::numeric_limits<int64_t>::min();
    const int64_t allbit = 0xFFFFFFFFFFFFFFFF;

    c.insert_int(0,     2);
    c.insert_int(1, minval);
    c.insert_int(2, maxval);
    c.insert_int(3, allbit);
    CHECK_EQUAL(4, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_INT, c.GetType(i));
    }

    CHECK_EQUAL(     2, c.get_int(0));
    CHECK_EQUAL(minval, c.get_int(1));
    CHECK_EQUAL(maxval, c.get_int(2));
    CHECK_EQUAL(allbit, c.get_int(3));

    c.set_int(0,    400);
    c.set_int(1,      0);
    c.set_int(2, -99999);
    c.set_int(3,      1);

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_INT, c.GetType(i));
    }

    CHECK_EQUAL(   400, c.get_int(0));
    CHECK_EQUAL(     0, c.get_int(1));
    CHECK_EQUAL(-99999, c.get_int(2));
    CHECK_EQUAL(     1, c.get_int(3));
    CHECK_EQUAL(4, c.Size());

    c.Destroy();
}


TEST(ColumnMixed_Float)
{
    ColumnMixed c;

    const uint32_t v = 0xFFFFFFFF;
    const float f = static_cast<float>(v);
    float fval1[] = {0.0f, 100.123f, -111.222f, f};
    float fval2[] = {-0.0f, -100.123f, std::numeric_limits<float>::max(), std::numeric_limits<float>::min()};
    
    // Test insert
    for (size_t i=0; i<4; ++i)
        c.insert_float(i, fval1[i]);
    CHECK_EQUAL(4, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_FLOAT, c.GetType(i));
        CHECK_EQUAL( fval1[i], c.get_float(i));
    }

    // Set to new values - ensure sign is changed
    for (size_t i=0; i<4; ++i)
        c.set_float(i, fval2[i]);

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_FLOAT, c.GetType(i));
        CHECK_EQUAL( fval2[i], c.get_float(i));
    }
    CHECK_EQUAL(4, c.Size());

    c.Destroy();
}


TEST(ColumnMixed_Double)
{
    ColumnMixed c;

    const uint64_t v = 0xFFFFFFFFFFFFFFFF;
    const double d = static_cast<double>(v);
    double fval1[] = {1.0, 200.123, -111.222, d};
    double fval2[] = {-1.0, -100.123, std::numeric_limits<double>::max(), std::numeric_limits<double>::min()};
    
    // Test insert
    for (size_t i=0; i<4; ++i) {
        c.insert_double(i, fval1[i]);
    }
    CHECK_EQUAL(4, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_DOUBLE, c.GetType(i));
        double v = c.get_double(i);
        CHECK_EQUAL( fval1[i], v);
    }

    // Set to new values - ensure sign is changed
    for (size_t i=0; i<4; ++i)
        c.set_double(i, fval2[i]);

    CHECK_EQUAL(4, c.Size());
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_DOUBLE, c.GetType(i));
        CHECK_EQUAL( fval2[i], c.get_double(i));
    }

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
    c.insert_float(0, 1.124f);
    c.insert_double(0, 1234.124);
    CHECK_EQUAL(8, c.Size());

    CHECK_EQUAL(COLUMN_TYPE_DOUBLE, c.GetType(0));
    CHECK_EQUAL(COLUMN_TYPE_FLOAT,  c.GetType(1));
    CHECK_EQUAL(COLUMN_TYPE_TABLE,  c.GetType(2));
    CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(3));
    CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(4));
    CHECK_EQUAL(COLUMN_TYPE_DATE,   c.GetType(5));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   c.GetType(6));
    CHECK_EQUAL(COLUMN_TYPE_INT,    c.GetType(7));

    // Change all entries to new types
    c.set_int(0, 23);
    c.set_bool(1, false);
    c.set_date(2, 23423);
    c.set_string(3, "Hello");
    c.set_binary(4, "binary", 7);
    c.set_subtable(5);
    c.set_float(6, 1.124f);
    c.set_double(7, 1234.124);
    CHECK_EQUAL(8, c.Size());

    CHECK_EQUAL(COLUMN_TYPE_DOUBLE, c.GetType(7));
    CHECK_EQUAL(COLUMN_TYPE_FLOAT,  c.GetType(6));
    CHECK_EQUAL(COLUMN_TYPE_TABLE,  c.GetType(5));
    CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(4));
    CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(3));
    CHECK_EQUAL(COLUMN_TYPE_DATE,   c.GetType(2));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   c.GetType(1));
    CHECK_EQUAL(COLUMN_TYPE_INT,    c.GetType(0));

    c.Destroy();
}
