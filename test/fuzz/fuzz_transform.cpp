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


#include "../test.hpp"
#include "../fuzz_tester.hpp"

#include "../test_all.hpp"

#include <fstream>

using namespace realm;
using namespace realm::test_util;
using namespace realm::test_util::unit_test;


struct FileSource {
    struct EndOfStream : std::runtime_error {
        EndOfStream()
            : std::runtime_error("end of stream")
        {
        }
    };

    std::ifstream& m_in;
    FileSource(std::ifstream& in)
        : m_in(in)
    {
    }

    // Emulate realm::test_util::Random interface

    unsigned int get_byte()
    {
        // std::ifstream is buffered, so in theory this should be reasonably fast.
        if (!m_in.good())
            throw EndOfStream{};

        unsigned char b;
        m_in >> b;
        return b;
    }

    template <class T>
    T draw_float()
    {
        using I = std::conditional_t<std::is_same<T, float>::value, uint32_t, uint64_t>;
        I r0 = draw_int<I>();
        return T(r0) / std::numeric_limits<T>::max();
    }

    template <class T>
    T draw_int()
    {
        union {
            T i;
            unsigned char p[sizeof(T)];
        } result;
        for (size_t i = 0; i < sizeof(T); ++i) {
            result.p[i] = get_byte();
        }
        return result.i;
    }

    template <class T>
    T draw_int(T min, T max)
    {
        // FIXME: Assuming benign over-/underflow.
        T range = max - min;
        T n = draw_int<std::make_unsigned_t<T>>() % (range + 1);
        return n + min;
    }

    template <class T>
    T draw_int_max(T max)
    {
        return draw_int<T>(T{}, max);
    }

    template <class T>
    T draw_int_mod(T mod)
    {
        return draw_int_max<T>(mod - 1);
    }

    bool draw_bool()
    {
        return (get_byte() & 1) == 1;
    }

    bool chance(int n, int m)
    {
        return draw_int_mod(m) < n;
    }
};

static int g_argc;
static char** g_argv;

TEST(Fuzz_Transform)
{
    std::ifstream in{g_argv[1]};
    if (!in.is_open()) {
        fprintf(stderr, "Could not open file for reading: %s\n", g_argv[1]);
        exit(1);
    }

    std::string pseudo_pid = g_argv[2];

    FileSource source{in};

    const char* trace_p = ::getenv("UNITTEST_RANDOMIZED_TRACE");
    bool trace = trace_p && (StringData{trace_p} != "no");

    try {
        FuzzTester<FileSource> fuzzer{source, trace};
        while (true) { // Will eventually terminate with an exception
            fuzzer.round(test_context, pseudo_pid);
        }
    }
    catch (const FileSource::EndOfStream&) {
        // Ignore, it's fine
    }
}

int main(int argc, char* argv[])
{
    if (argc != 3) {
        fprintf(stderr,
                "Usage: %s <INPUT> <N>\n(where <INPUT> is the path to a file containing a sequence of bytes "
                "indicating operations to be replayed, and <N> is a number unique to the process being started in "
                "order to prevent collisions with parallel fuzzers.)\n",
                argv[0]);
        exit(1);
    }

    g_argc = argc;
    g_argv = argv;

    realm::disable_sync_to_disk();

    TestList::Config config;
    config.logger = nullptr;
    config.intra_test_log_level = util::Logger::Level::fatal;
    TestList& list = get_default_test_list();
    bool success = list.run(config);
    REALM_ASSERT_RELEASE(success == true);
    return 0;
}
