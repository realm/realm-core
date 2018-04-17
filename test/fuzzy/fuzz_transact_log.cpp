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

#include <realm/db.hpp>
#include "../test.hpp"

#include <cstdio>
#include <fstream>

using namespace realm;
using namespace realm::util;

struct InputStreamAdapter : _impl::InputStream {
    InputStreamAdapter(std::ifstream& s)
        : m_stream(s)
    {
    }

    size_t read(char* buffer, size_t size) final
    {
        return m_stream.read(buffer, size).gcount();
    }

    std::ifstream& m_stream;
};

int main(int argc, const char* argv[])
{
    if (argc == 1) {
        fprintf(stderr, "Usage: %s <LOGFILE>\n(where <LOGFILE> is a transaction log file that will be replayed.)",
                argv[0]);
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

    Group group;

    try {
        Replication::apply_changeset(in_aa, group);
    }
    catch (_impl::TransactLogParser::BadTransactLog) {
        return 0;
    }

    return 0;
}
