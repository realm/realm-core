#include <locale>

#include <realm/sync/noinst/server/server_dir.hpp>

using namespace realm;

namespace {

bool valid_virt_path_segment(const std::string& seg)
{
    if (seg.empty())
        return false;
    // Prevent `.`, `..`, and `.foo` (hidden files)
    if (seg.front() == '.')
        return false;
    // Prevent spurious clashes between directory names and file names
    // created by appending `.realm`, `.realm.lock`, or `.realm.management`
    // to the last component of client specified virtual paths.
    bool possible_clash = (StringData(seg).ends_with(".realm") || StringData(seg).ends_with(".realm.lock") ||
                           StringData(seg).ends_with(".realm.management"));
    if (possible_clash)
        return false;
    std::locale c_loc = std::locale::classic();
    for (char ch : seg) {
        if (std::isalnum(ch, c_loc)) // A-Za-z0-9
            continue;
        if (ch == '_' || ch == '-' || ch == '.' || ch == '"')
            continue;
        return false;
    }
    return true;
}

} // unnamed namespace


_impl::VirtualPathComponents _impl::parse_virtual_path(const std::string& root_path, const std::string& virt_path)
{
    VirtualPathComponents result;
    if (virt_path.empty())
        return result;

    std::string real_path = root_path;
    size_t prev_pos = 0;
    if (virt_path.front() != '/') {
        --prev_pos;
        real_path += '/';
    }
    for (;;) {
        ++prev_pos; // Skip previous slash
        size_t pos = virt_path.find('/', prev_pos);
        bool last = (pos == std::string::npos);
        if (last)
            pos = virt_path.size();
        std::string segment = virt_path.substr(prev_pos, pos - prev_pos);
        // Parition key style paths will be surrounded in quotes, which Windows
        // doesn't allow in paths.
        segment.erase(std::remove(segment.begin(), segment.end(), '"'), segment.end());

        if (!valid_virt_path_segment(segment))
            return result;

        real_path = util::File::resolve(segment, real_path);
        if (last)
            break;
        prev_pos = pos;
    }
    result.is_valid = true;
    result.real_realm_path = real_path + ".realm";
    return result;
}


bool _impl::map_virt_to_real_realm_path(const std::string& root_path, const std::string& virt_path,
                                        std::string& real_path)
{
    VirtualPathComponents result = parse_virtual_path(root_path, virt_path); // Throws
    if (result.is_valid) {
        real_path = std::move(result.real_realm_path);
        return true;
    }
    return false;
}


bool _impl::map_partial_to_reference_virt_path(const std::string& partial_path, std::string& reference_path)
{
    std::string root_path = "";                                                 // Immaterial
    VirtualPathComponents result = parse_virtual_path(root_path, partial_path); // Throws
    if (result.is_valid && result.is_partial_view) {
        reference_path = std::move(result.reference_path);
        return true;
    }
    return false;
}


void _impl::make_dirs(const std::string& root_path, const std::string& virt_path)
{
    REALM_ASSERT(!virt_path.empty());
    size_t prev_pos = 0;
    std::string real_path = root_path;
    if (virt_path.front() != '/') {
        real_path += '/';
        --prev_pos;
    }
    for (;;) {
        ++prev_pos; // Skip previous slash
        size_t pos = virt_path.find('/', prev_pos);
        if (pos == std::string::npos)
            break;
        std::string name = virt_path.substr(prev_pos, pos - prev_pos);
        REALM_ASSERT(valid_virt_path_segment(name));
        real_path = util::File::resolve(name, real_path);
        util::try_make_dir(real_path);
        prev_pos = pos;
    }
}
