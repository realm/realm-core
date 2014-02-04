#include "testsettings.hpp"

#include <string>
#include <ostream>
#include <UnitTest++.h>

#include <tightdb/version.hpp>

using namespace std;
using namespace tightdb;


ONLY(VERSION)
{
#if 1
    cout << "TightDB version: " << Version::get_version();
    cout << "\n  with Debug " << 
        (Version::has_feature(feature_Debug) ? "Enabled" : "Disabled"); 
    cout << "\n  with Replication " << 
        (Version::has_feature(feature_Replication) ? "Enabled" : "Disabled"); 
    cout << endl << endl;
#endif

    CHECK_EQUAL(TIGHTDB_VER_MAJOR, Version::get_major());
    CHECK_EQUAL(TIGHTDB_VER_MINOR, Version::get_minor());
    CHECK_EQUAL(TIGHTDB_VER_PATCH, Version::get_patch());
    CHECK_EQUAL(TIGHTDB_VER_PATCH, Version::get_patch());

    CHECK_EQUAL(true, Version::is_at_least(0,0,0));
    CHECK_EQUAL(true, Version::is_at_least(0,1,5));
    CHECK_EQUAL(true, Version::is_at_least(0,1,6));
    // Below might have to be updated when the version is incremented
    CHECK_EQUAL(false, Version::is_at_least(0,1,9));
    CHECK_EQUAL(false, Version::is_at_least(1,0,0));
    CHECK_EQUAL(false, Version::is_at_least(0,2,0));
}