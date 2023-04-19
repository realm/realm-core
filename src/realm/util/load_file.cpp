#include <realm/util/buffer.hpp>
#include <realm/util/file.hpp>
#include <realm/util/load_file.hpp>

using namespace realm;


std::string util::load_file(const std::string& path)
{
    util::File file{path}; // Throws
    util::Buffer<char> buffer;
    std::size_t used_size = 0;
    for (;;) {
        std::size_t min_extra_capacity = 256;
        buffer.reserve_extra(used_size, min_extra_capacity);                             // Throws
        std::size_t n = file.read(buffer.data() + used_size, buffer.size() - used_size); // Throws
        if (n == 0)
            break;
        used_size += n;
    }
    return std::string(buffer.data(), used_size); // Throws
}


std::string util::load_file_and_chomp(const std::string& path)
{
    std::string contents = load_file(path); // Throws
    if (!contents.empty() && contents.back() == '\n')
        contents.pop_back();
    return contents;
}
