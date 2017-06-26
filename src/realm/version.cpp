/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/features.h>
#include <realm/version.hpp>
#include <realm/string_data.hpp>
#include <sstream>

using namespace realm;

std::string Version::get_version()
{
    std::stringstream ss;
    ss << get_major() << "." << get_minor() << "." << get_patch();
    return ss.str();
}

StringData Version::get_extra()
{
    return REALM_VERSION_EXTRA;
}

bool Version::is_at_least(int major, int minor, int patch, StringData extra)
{
    if (get_major() < major)
        return false;
    if (get_major() > major)
        return true;

    if (get_minor() < minor)
        return false;
    if (get_minor() > minor)
        return true;

    if (get_patch() > patch)
        return true;
    if (get_patch() < patch)
        return false;

    return (get_extra() >= extra);
}

bool Version::is_at_least(int major, int minor, int patch)
{
    return is_at_least(major, minor, patch, "");
}

bool Version::has_feature(Feature feature)
{
    switch (feature) {
        case feature_Debug:
#ifdef REALM_DEBUG
            return true;
#else
            return false;
#endif

        case feature_Replication:
            return true;
    }
    return false; // LCOV_EXCL_LINE
}
