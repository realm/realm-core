#include <realm/group_shared.hpp>

#include <iostream>
#include <algorithm>
#include <fstream>

using namespace realm;
using namespace realm::util;

static void help(const char* program) {
    std::cerr << "Usage: " << program << " <OUTPUT>\n";
    exit(1);
}

int main(int argc, const char** argv)
{
    if (argc != 2) {
        help(argv[0]);
    }

    _impl::TransactLogBufferStream stream;
    _impl::TransactLogEncoder e{stream};

    e.insert_group_level_table(0, 0, "foo");
    e.select_table(0, 0, nullptr);
    e.insert_column(0, type_Int, "integer_column", false);
    e.insert_empty_rows(0, 1, 0, false);
    // FIXME: Add more instructions.


    const char* buffer_begin = stream.transact_log_data();
    const char* buffer_end = e.write_position();

    std::ofstream out{argv[1]};
    if (out.is_open()) {
        out.write(buffer_begin, buffer_end - buffer_begin);
        return 0;
    }

    std::cerr << "Error writing to file: " << argv[1] << "\n";
    return 1;
}
