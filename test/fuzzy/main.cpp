#include <realm/group_shared.hpp>
#include <realm/commit_log.hpp>
#include "../test.hpp"

#include <stdio.h>
#include <fstream>

using namespace realm;
using namespace realm::util;

struct InputStreamAdapter : _impl::InputStream {
    InputStreamAdapter(std::ifstream& s) : stream_(s) {}

    size_t read(char* buffer, size_t size) final
    {
        return stream_.read(buffer, size).gcount();
    }

    std::ifstream& stream_;
};

int main(int argc, char const *argv[])
{
    if (argc == 1) {
        fprintf(stderr, "Usage: %s <LOGFILE>\n(where <LOGFILE> is a transaction log file that will be replayed.)", argv[0]);
        exit(1);
    }

    std::ifstream in{argv[1]};
    if (!in.is_open()) {
        fprintf(stderr, "Could not open file for reading: %s\n", argv[1]);
        exit(1);
    }

    InputStreamAdapter in_a{in};
    std::vector<char> buffer;
    buffer.resize(1024);
    _impl::NoCopyInputStreamAdaptor in_aa{in_a, buffer.data(), buffer.size()};

    test_util::unit_test::TestDetails test_details;
    test_details.test_index = 0;
    test_details.suite_name = "FuzzyTest";
    test_details.test_name = "TransactLogApplier";
    test_details.file_name = __FILE__;
    test_details.line_number = __LINE__;

    Group group;

    try {
        Replication::apply_changeset(in_aa, group);
    }
    catch (_impl::TransactLogParser::BadTransactLog) {
        return 0;
    }

    return 0;
}
