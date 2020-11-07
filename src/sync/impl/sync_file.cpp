////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "sync/impl/sync_file.hpp"

#include <realm/util/file.hpp>
#include <realm/util/hex_dump.hpp>
#include <realm/util/sha_crypto.hpp>
#include <realm/util/time.hpp>
#include <realm/util/scope_exit.hpp>

#include <iomanip>
#include <sstream>
#include <system_error>
#include <fstream>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>

inline static int mkstemp(char* _template) { return _open(_mktemp(_template), _O_CREAT | _O_TEMPORARY, _S_IREAD | _S_IWRITE); }
#else
#include <unistd.h>
#endif


using File = realm::util::File;

namespace realm {

namespace {

uint8_t value_of_hex_digit(char hex_digit)
{
    if (hex_digit >= '0' && hex_digit <= '9') {
        return hex_digit - '0';
    } else if (hex_digit >= 'A' && hex_digit <= 'F') {
        return 10 + hex_digit - 'A';
    } else if (hex_digit >= 'a' && hex_digit <= 'f') {
        return 10 + hex_digit - 'a';
    } else {
        throw std::invalid_argument("Cannot get the value of a character that isn't a hex digit.");
    }
}

bool filename_is_reserved(const std::string& filename) {
    return (filename == "." || filename == "..");
}

bool character_is_unreserved(char character)
{
    bool is_capital_letter = (character >= 'A' && character <= 'Z');
    bool is_lowercase_letter = (character >= 'a' && character <= 'z');
    bool is_number = (character >= '0' && character <= '9');
    bool is_allowed_symbol = (character == '-' || character == '_' || character == '.');
    return is_capital_letter || is_lowercase_letter || is_number || is_allowed_symbol;
}

char decoded_char_for(const std::string& percent_encoding, size_t index)
{
    if (index+2 >= percent_encoding.length()) {
        throw std::invalid_argument("Malformed string: not enough characters after '%' before end of string.");
    }
    REALM_ASSERT(percent_encoding[index] == '%');
    return (16*value_of_hex_digit(percent_encoding[index + 1])) + value_of_hex_digit(percent_encoding[index + 2]);
}

} // (anonymous namespace)

namespace util {

std::string make_percent_encoded_string(const std::string& raw_string)
{
    std::string buffer;
    buffer.reserve(raw_string.size());
    for (size_t i=0; i<raw_string.size(); i++) {
        unsigned char character = raw_string[i];
        if (character_is_unreserved(character)) {
            buffer.push_back(character);
        } else {
            buffer.resize(buffer.size() + 3);
            // Format string must resolve to exactly 3 characters.
            sprintf(&buffer.back() - 2, "%%%2X", character);
        }
    }
    return buffer;
}

std::string make_raw_string(const std::string& percent_encoded_string)
{
    std::string buffer;
    size_t input_len = percent_encoded_string.length();
    buffer.reserve(input_len);
    size_t idx = 0;
    while (idx < input_len) {
        char current = percent_encoded_string[idx];
        if (current == '%') {
            // Decode. +3.
            buffer.push_back(decoded_char_for(percent_encoded_string, idx));
            idx += 3;
        } else {
            // No need to decode. +1.
            if (!character_is_unreserved(current)) {
                throw std::invalid_argument("Input string is invalid: contains reserved characters.");
            }
            buffer.push_back(current);
            idx++;
        }
    }
    return buffer;
}

std::string file_path_by_appending_component(const std::string& path, const std::string& component, FilePathType path_type)
{
    // FIXME: Does this have to be changed to accomodate Windows platforms?
    std::string buffer;
    buffer.reserve(2 + path.length() + component.length());
    buffer.append(path);
    std::string terminal = "";
    if (path_type == FilePathType::Directory && component[component.length() - 1] != '/') {
        terminal = "/";
    }
    char path_last = path[path.length() - 1];
    char component_first = component[0];
    if (path_last == '/' && component_first == '/') {
        buffer.append(component.substr(1));
        buffer.append(terminal);
    } else if (path_last == '/' || component_first == '/') {
        buffer.append(component);
        buffer.append(terminal);
    } else {
        buffer.append("/");
        buffer.append(component);
        buffer.append(terminal);
    }
    return buffer;
}

std::string file_path_by_appending_extension(const std::string& path, const std::string& extension)
{
    std::string buffer;
    buffer.reserve(1 + path.length() + extension.length());
    buffer.append(path);
    char path_last = path[path.length() - 1];
    char extension_first = extension[0];
    if (path_last == '.' && extension_first == '.') {
        buffer.append(extension.substr(1));
    } else if (path_last == '.' || extension_first == '.') {
        buffer.append(extension);
    } else {
        buffer.append(".");
        buffer.append(extension);
    }
    return buffer;
}

std::string create_timestamped_template(const std::string& prefix, int wildcard_count)
{
    constexpr int WILDCARD_MAX = 20;
    constexpr int WILDCARD_MIN = 6;
    wildcard_count = std::min(WILDCARD_MAX, std::max(WILDCARD_MIN, wildcard_count));
    std::time_t time = std::time(nullptr);
    std::stringstream stream;
    stream << prefix << "-" << util::format_local_time(time, "%Y%m%d-%H%M%S") << "-" << std::string(wildcard_count, 'X');
    return stream.str();
}

std::string reserve_unique_file_name(const std::string& path, const std::string& template_string)
{
    REALM_ASSERT_DEBUG(template_string.find("XXXXXX") != std::string::npos);
    std::string path_buffer = file_path_by_appending_component(path, template_string, FilePathType::File);
    int fd = mkstemp(&path_buffer[0]);
    if (fd < 0) {
        int err = errno;
        throw std::system_error(err, std::system_category());
    }
    // Remove the file so we can use the name for our own file.
#ifdef _WIN32
    _close(fd);
    _unlink(path_buffer.c_str());
#else
    close(fd);
    unlink(path_buffer.c_str());
#endif
    return path_buffer;
}

static std::string validate_and_clean_path(const std::string& path)
{
    REALM_ASSERT(path.length() > 0);
    std::string escaped_path = util::make_percent_encoded_string(path);
    if (filename_is_reserved(escaped_path))
        throw std::invalid_argument(util::format("A path can't have an identifier reserved by the filesystem: '%1'", escaped_path));
    return escaped_path;
}

} // util

SyncFileManager::SyncFileManager(const std::string& base_path, const std::string& app_id)
: m_base_path(util::file_path_by_appending_component(base_path,
                                                     c_sync_directory,
                                                     util::FilePathType::Directory))
, m_app_path(util::file_path_by_appending_component(m_base_path,
                                                    util::validate_and_clean_path(app_id),
                                                    util::FilePathType::Directory))
{
    util::try_make_dir(m_base_path);
    util::try_make_dir(m_app_path);
}

std::string SyncFileManager::get_special_directory(std::string directory_name) const
{
    auto dir_path = file_path_by_appending_component(m_app_path,
                                                     directory_name,
                                                     util::FilePathType::Directory);
    util::try_make_dir(dir_path);
    return dir_path;
}

std::string SyncFileManager::user_directory(const std::string& user_identity) const
{
    std::string user_path = get_user_directory_path(user_identity);
    util::try_make_dir(user_path);
    return user_path;
}

void SyncFileManager::remove_user_directory(const std::string& user_identity) const
{
    std::string user_path = get_user_directory_path(user_identity);
    util::try_remove_dir_recursive(user_path);
}

bool SyncFileManager::try_rename_user_directory(const std::string& old_name, const std::string& new_name) const
{
    const auto& old_name_escaped = util::validate_and_clean_path(old_name);
    const auto& new_name_escaped = util::validate_and_clean_path(new_name);
    const std::string& base = m_app_path;
    const auto& old_path = file_path_by_appending_component(base, old_name_escaped, util::FilePathType::Directory);
    const auto& new_path = file_path_by_appending_component(base, new_name_escaped, util::FilePathType::Directory);

    try {
        File::move(old_path, new_path);
    } catch (File::NotFound const&) {
        return false;
    }
    return true;
}

bool SyncFileManager::remove_realm(const std::string& absolute_path) const
{
    REALM_ASSERT(absolute_path.length() > 0);
    bool success = true;
    // Remove the Realm file (e.g. "example.realm").
    success = File::try_remove(absolute_path);
    // Remove the lock file (e.g. "example.realm.lock").
    auto lock_path = util::file_path_by_appending_extension(absolute_path, "lock");
    success = File::try_remove(lock_path);
    // Remove the management directory (e.g. "example.realm.management").
    auto management_path = util::file_path_by_appending_extension(absolute_path, "management");
    try {
        util::try_remove_dir_recursive(management_path);
    }
    catch (File::AccessError const&) {
        success = false;
    }
    return success;
}

bool SyncFileManager::copy_realm_file(const std::string& old_path, const std::string& new_path) const
{
    REALM_ASSERT(old_path.length() > 0);
    try {
        if (File::exists(new_path)) {
            return false;
        }
        File::copy(old_path, new_path);
    }
    catch (File::NotFound const&) {
        return false;
    }
    catch (File::AccessError const&) {
        return false;
    }
    return true;
}

bool SyncFileManager::remove_realm(const std::string& user_identity, const std::string& raw_realm_path) const
{
    auto escaped = util::validate_and_clean_path(raw_realm_path);
    auto realm_path = util::file_path_by_appending_component(user_directory(user_identity), escaped);
    return remove_realm(realm_path);
}

bool SyncFileManager::try_file_exists(const std::string& path) noexcept
{
    try {
        // May throw; for example when the path is too long
        return util::File::exists(path);
    }
    catch (const std::exception&) {
        return false;
    }
}

static bool try_file_remove(const std::string& path) noexcept
{
    try {
        return util::File::try_remove(path);
    }
    catch (const std::exception&) {
        return false;
    }
}

std::string SyncFileManager::realm_file_path(const std::string& user_identity, const std::string& local_user_identity, const std::string& realm_file_name) const
{
    auto escaped_file_name = util::validate_and_clean_path(realm_file_name);
    std::string preferred_name = util::file_path_by_appending_component(user_directory(user_identity), escaped_file_name);
    std::string preferred_path = preferred_name + c_realm_file_suffix;

    if (!try_file_exists(preferred_path)) {
        // Shorten the Realm path to just `<rootDir>/<hashedAbsolutePath>.realm`
        // If that also fails, give up and report error to user.
        std::string hashed_name = fallback_hashed_realm_file_path(preferred_name);
        std::string hashed_path = hashed_name + c_realm_file_suffix;
        if (try_file_exists(hashed_path)) {
            // detected that the hashed fallback has been used previously
            // it was created for a reason so keep using it
            return hashed_path;
        }

        // retain support for legacy paths
        std::string old_path = legacy_realm_file_path(local_user_identity, realm_file_name);
        if (try_file_exists(old_path)) {
            return old_path;
        }

        // retain support for legacy local identity paths
        std::string old_local_identity_path = legacy_local_identity_path(local_user_identity, realm_file_name);
        if (try_file_exists(old_local_identity_path)) {
            return old_local_identity_path;
        }

        // since this appears to be a new file, test the normal location
        // we use a test file with the same name and a suffix of the
        // same length so we can catch "filename too long" errors on windows
        try {
            std::string test_path = preferred_name + c_realm_file_test_suffix;
            auto defer = util::make_scope_exit([test_path]() noexcept {
                try_file_remove(test_path);
            });
            util::File f(test_path, util::File::Mode::mode_Write);
            // if the test file succeeds, delete it and return the preferred location
        }
        catch (const File::AccessError& e_absolute) {
            // the preferred test failed, test the hashed path
            try {
                std::string test_hashed_path = hashed_name + c_realm_file_test_suffix;
                auto defer = util::make_scope_exit([test_hashed_path]() noexcept {
                    try_file_remove(test_hashed_path);
                });
                util::File f(test_hashed_path, util::File::Mode::mode_Write);
                // at this point the create succeeded, clean up the test file and return the hashed path
                return hashed_path;
            }
            catch (const File::AccessError& e_hashed) {
                // hashed test path also failed, give up and report error to user.
                throw std::logic_error(util::format("A valid realm path cannot be created for the "
                                                    "Realm identity '%1' at neither '%2' nor '%3'. %4",
                                                    realm_file_name, preferred_path, hashed_path, e_hashed.what()));
            }
        }
    }

    return preferred_path;
}

std::string SyncFileManager::metadata_path() const
{
    auto dir_path = file_path_by_appending_component(get_utility_directory(),
                                                     c_metadata_directory,
                                                     util::FilePathType::Directory);
    util::try_make_dir(dir_path);
    return util::file_path_by_appending_component(dir_path, c_metadata_realm);
}

bool SyncFileManager::remove_metadata_realm() const
{
    auto dir_path = file_path_by_appending_component(get_utility_directory(),
                                                     c_metadata_directory,
                                                     util::FilePathType::Directory);
    try {
        util::try_remove_dir_recursive(dir_path);
        return true;
    }
    catch (File::AccessError const&) {
        return false;
    }
}

std::string SyncFileManager::fallback_hashed_realm_file_path(const std::string& preferred_path) const
{
    std::array<unsigned char, 32> hash;
    util::sha256(preferred_path.data(), preferred_path.size(), hash.data());
    std::string hashed_name = util::file_path_by_appending_component(m_app_path, util::hex_dump(hash.data(), hash.size(), ""));
    return hashed_name;
}

std::string SyncFileManager::legacy_realm_file_path(const std::string& local_user_identity, const std::string& realm_file_name) const
{
    auto path = util::file_path_by_appending_component(m_app_path, c_legacy_sync_directory, util::FilePathType::Directory);
    path = util::file_path_by_appending_component(path, util::validate_and_clean_path(local_user_identity), util::FilePathType::Directory);
    path = util::file_path_by_appending_component(path, util::validate_and_clean_path(realm_file_name));
    return path;
}

std::string SyncFileManager::legacy_local_identity_path(const std::string& local_user_identity, const std::string& realm_file_name) const
{
    auto escaped_file_name = util::validate_and_clean_path(realm_file_name);
    std::string user_path = get_user_directory_path(local_user_identity);
    std::string path_name = util::file_path_by_appending_component(user_path, escaped_file_name);
    std::string path = path_name + c_realm_file_suffix;

    return path;
}

std::string SyncFileManager::get_user_directory_path(const std::string& user_identity) const {
    return file_path_by_appending_component(m_app_path,
                                            util::validate_and_clean_path(user_identity),
                                            util::FilePathType::Directory);
}

} // realm
