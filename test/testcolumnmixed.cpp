#include "ColumnMixed.hpp"
#include <UnitTest++.h>

using namespace tightdb;

TEST(ColumnMixed_Int) {
    ColumnMixed c;
    
    c.InsertInt(0,     2);
    c.InsertInt(1,   100);
    c.InsertInt(2, 20000);
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

TEST(ColumnMixed_Bool) {
    ColumnMixed c;
    
    c.InsertBool(0, true);
    c.InsertBool(1, false);
    c.InsertBool(2, true);
    CHECK_EQUAL(3, c.Size());
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BOOL, c.GetType(i));
    }
    
    CHECK_EQUAL(true,  c.GetBool(0));
    CHECK_EQUAL(false, c.GetBool(1));
    CHECK_EQUAL(true,  c.GetBool(2));
    
    c.SetBool(0, false);
    c.SetBool(1, true);
    c.SetBool(2, false);
    CHECK_EQUAL(3, c.Size());
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BOOL, c.GetType(i));
    }
    
    CHECK_EQUAL(false, c.GetBool(0));
    CHECK_EQUAL(true,  c.GetBool(1));
    CHECK_EQUAL(false, c.GetBool(2));
    
    c.Destroy();
}

TEST(ColumnMixed_Date) {
    ColumnMixed c;
    
    c.InsertDate(0,     2);
    c.InsertDate(1,   100);
    c.InsertDate(2, 20000);
    CHECK_EQUAL(3, c.Size());
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_DATE, c.GetType(i));
    }
    
    CHECK_EQUAL(    2, c.GetDate(0));
    CHECK_EQUAL(  100, c.GetDate(1));
    CHECK_EQUAL(20000, c.GetDate(2));
    
    c.SetDate(0,   400);
    c.SetDate(1,     0);
    c.SetDate(2, 99999);
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_DATE, c.GetType(i));
    }
    
    CHECK_EQUAL(  400, c.GetDate(0));
    CHECK_EQUAL(    0, c.GetDate(1));
    CHECK_EQUAL(99999, c.GetDate(2));
    CHECK_EQUAL(3, c.Size());
    
    c.Destroy();
}

TEST(ColumnMixed_String) {
    ColumnMixed c;
    
    c.InsertString(0, "aaa");
    c.InsertString(1, "bbbbb");
    c.InsertString(2, "ccccccc");
    CHECK_EQUAL(3, c.Size());
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(i));
    }
    
    CHECK_EQUAL("aaa",     c.GetString(0));
    CHECK_EQUAL("bbbbb",   c.GetString(1));
    CHECK_EQUAL("ccccccc", c.GetString(2));
    
    c.SetString(0, "dd");
    c.SetString(1, "");
    c.SetString(2, "eeeeeeeee");
    CHECK_EQUAL(3, c.Size());
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(i));
    }
    
    CHECK_EQUAL("dd",        c.GetString(0));
    CHECK_EQUAL("",          c.GetString(1));
    CHECK_EQUAL("eeeeeeeee", c.GetString(2));
    
    c.Destroy();
}

TEST(ColumnMixed_Binary) {
    ColumnMixed c;
    
    c.InsertBinary(0, "aaa", 4);
    c.InsertBinary(1, "bbbbb", 6);
    c.InsertBinary(2, "ccccccc", 8);
    CHECK_EQUAL(3, c.Size());
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(i));
    }
    
    CHECK_EQUAL("aaa",     (const char*)c.GetBinary(0).pointer);
    CHECK_EQUAL("bbbbb",   (const char*)c.GetBinary(1).pointer);
    CHECK_EQUAL("ccccccc", (const char*)c.GetBinary(2).pointer);
    
    c.SetBinary(0, "dd", 3);
    c.SetBinary(1, "", 1);
    c.SetBinary(2, "eeeeeeeee", 10);
    CHECK_EQUAL(3, c.Size());
    
    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(i));
    }
    
    CHECK_EQUAL("dd",        (const char*)c.GetBinary(0).pointer);
    CHECK_EQUAL("",          (const char*)c.GetBinary(1).pointer);
    CHECK_EQUAL("eeeeeeeee", (const char*)c.GetBinary(2).pointer);
    
    c.Destroy();
}

TEST(ColumnMixed_Table) {
    ColumnMixed c;

    c.InsertTable(0);
    c.InsertTable(1);
    CHECK_EQUAL(2, c.Size());

    for (size_t i = 0; i < c.Size(); ++i) {
        CHECK_EQUAL(COLUMN_TYPE_TABLE, c.GetType(i));
    }

    Table* const t1 = c.get_subtable_ptr(0);
    Table* const t2 = c.get_subtable_ptr(1);
    CHECK(t1->IsEmpty());
    CHECK(t2->IsEmpty());
    delete t1;
    delete t2;

    c.Destroy();
}

TEST(ColumnMixed_Mixed) {
    ColumnMixed c;
    
    // Insert mixed types
    c.InsertInt(0, 23);
    c.InsertBool(0, false);
    c.InsertDate(0, 23423);
    c.InsertString(0, "Hello");
    c.InsertBinary(0, "binary", 7);
    c.InsertTable(0);
    CHECK_EQUAL(6, c.Size());
    
    CHECK_EQUAL(COLUMN_TYPE_TABLE,  c.GetType(0));
    CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(1));
    CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(2));
    CHECK_EQUAL(COLUMN_TYPE_DATE,   c.GetType(3));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   c.GetType(4));
    CHECK_EQUAL(COLUMN_TYPE_INT,    c.GetType(5));
    
    // Change all entries to new types
    c.SetInt(0, 23);
    c.SetBool(1, false);
    c.SetDate(2, 23423);
    c.SetString(3, "Hello");
    c.SetBinary(4, "binary", 7);
    c.SetTable(5);
    CHECK_EQUAL(6, c.Size());
    
    CHECK_EQUAL(COLUMN_TYPE_TABLE,  c.GetType(5));
    CHECK_EQUAL(COLUMN_TYPE_BINARY, c.GetType(4));
    CHECK_EQUAL(COLUMN_TYPE_STRING, c.GetType(3));
    CHECK_EQUAL(COLUMN_TYPE_DATE,   c.GetType(2));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   c.GetType(1));
    CHECK_EQUAL(COLUMN_TYPE_INT,    c.GetType(0));
    
    c.Destroy();
}
