#include <stdexcept>

#include <realm/util/features.h>
#include <realm/util/parent_dir.hpp>

#if defined(_MSC_VER) && _MSC_VER >= 1900 // compiling with at least Visual Studio 2015
#include <filesystem>
#define HAVE_STD_FILESYSTEM 1
#else
#define HAVE_STD_FILESYSTEM 0
#endif

using namespace realm;


std::string util::parent_dir(const std::string& path)
{
#ifndef _WIN32
    // Find end of last directory separator sequence
    auto begin = path.begin();
    auto i = path.end();
    while (i != begin) {
        auto j = i;
        --i;
        if (*i == '/') {
            // Find beginning of last directory separator sequence
            while (i != begin) {
                auto k = i - 1;
                if (*k != '/')
                    return {begin, i}; // Throws
                i = k;
            }
            return {begin, j}; // Throws
        }
    }
    return {};
#elif HAVE_STD_FILESYSTEM
    namespace fs = std::filesystem;
    return fs::path(path).parent_path().string(); // Throws
#else
    static_cast<void>(path);
    throw util::runtime_error{"Not yet supported"};
#endif
}
