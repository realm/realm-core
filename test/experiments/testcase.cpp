#include <cstring>
#include <iostream>
#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

#define CHECK(v) do { if (v) break; cerr << __LINE__ << ": CHECK failed" << endl; } while(false)
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
    Column c;
    c.add(0);
    c.add(1);
    c.add(2);
    c.add(3);
    c.add(4);
    CHECK_EQUAL(c.Get(4), 4);
}
