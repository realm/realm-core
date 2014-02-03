/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_VERSION_HPP
#define TIGHTDB_VERSION_HPP

#include <string>
#include <sstream>

#define TIGHTDB_VER_MAJOR   0
#define TIGHTDB_VER_MINOR   1
#define TIGHTDB_VER_PATCH   6

namespace tightdb {

class Version
{
public:
    static int get_major() { return TIGHTDB_VER_MAJOR; }
    static int get_minor() { return TIGHTDB_VER_MINOR; }
    static int get_patch() { return TIGHTDB_VER_PATCH; }
    static std::string get_version();
    static bool is_at_least(int major, int minor, int patch);
    
    // TODO: bool has_feature(feature)
};


// Implementation:

std::string Version::get_version() 
{
    std::stringstream ss;
    ss << get_major() << "." << get_minor() << "." << get_patch();
    return ss.str();
}

bool Version::is_at_least(int major, int minor, int patch)
{
    if (get_major() < major)
        return false;
    if (get_minor() < minor)
        return false;
    if (get_patch() < patch)
        return false;
}



} // namespace tigthdb

#endif // TIGHTDB_VERSION_HPP