#include <cstring>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>

#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/array_binary.hpp>
#include <tightdb/array_string_long.hpp>

#define CHECK(v) do { if (v) break; cerr << __LINE__ << ": CHECK failed" << endl; } while (false)
#define CHECK_EQUAL(a, b) do { if (check_equal((a),(b))) break; cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl; } while(false)
#define CHECK_THROW(v, e) do { try { v; } catch (e&) { break; } cerr << __LINE__ << ": CHECK_THROW failed: Expected " # e << endl; } while(false)

using namespace tightdb;
using namespace std;

