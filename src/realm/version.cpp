#include <realm/util/features.h>
#include <realm/version.hpp>

using namespace realm;


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
    if (get_major() > major)
        return true;

    if (get_minor() < minor)
        return false;
    if (get_minor() > minor)
        return true;

    return (get_patch() >= patch);
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
    return false;
}
