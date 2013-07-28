#include <cstring>
#include <fstream>
#include <iostream>

#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/array_binary.hpp>
#include <tightdb/array_string_long.hpp>

#define CHECK(v) do { if (v) break; cerr << __LINE__ << ": CHECK failed" << endl; } while (false)
#define CHECK_EQUAL(a, b) do { if (check_equal((a),(b))) break; cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl; } while(false)
#define CHECK_THROW(v, e) do { try { v; } catch (e&) { break; } cerr << __LINE__ << ": CHECK_THROW failed: Expected " # e << endl; } while(false)

using namespace tightdb;
using namespace std;


namespace {

template<class A, class B> inline bool check_equal(const A& a, const B& b) { return a == b; }
inline bool check_equal(const char* a, const char* b) { return strcmp(a, b) == 0; }

} // anonymous namespace


namespace {
} // anonymous namespace


int main()
{
    Table table;

    table.add_column(type_Table,  "third");

    vector<size_t> column_path;
    column_path.push_back(0);
    table.add_subcolumn(column_path, type_Int,    "sub_first");

    table.insert_subtable(0, 0);
    table.insert_done();

    {
        TableRef subtable = table.get_subtable(0, 0);
        subtable->insert_int(0, 0, 42);
        subtable->insert_done();
    }

    column_path.clear();
    column_path.push_back(0);
    table.add_subcolumn(column_path, type_Int, "sub_third");

    table.Verify();

    TableRef subtable = table.get_subtable(0, 0);
}
