/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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
#include "fuzz_configurator.hpp"
#include "fuzz_object.hpp"
#include "../util/test_path.hpp"
#include <iostream>

FuzzConfigurator::FuzzConfigurator(FuzzObject& fuzzer, int argc, const char* argv[])
    : m_file_path_passed(true)
    , m_fuzzer(fuzzer)
    , m_fuzz_name("afl++")
{
    if (argc < 2)
        throw;
    realm::disable_sync_to_disk();
    init(argv[1]);
    setup_realm_config();
    print_cnf();
}

FuzzConfigurator::FuzzConfigurator(FuzzObject& fuzzer, const std::string& input)
    : m_file_path_passed(false)
    , m_fuzzer(fuzzer)
    , m_fuzz_name("libfuzz")
{
    realm::disable_sync_to_disk();
    init(input);
    setup_realm_config();
    print_cnf();
}

void FuzzConfigurator::setup_realm_config()
{
    m_config.path = m_path;
    m_config.schema_version = 0;
    if (m_use_encryption) {
        const char* key = m_fuzzer.get_encryption_key();
        const char* i = key;
        while (*i != '\0') {
            m_config.encryption_key.push_back(*i);
            i++;
        }
    }
}

const realm::Realm::Config& FuzzConfigurator::get_config() const
{
    return m_config;
}

FuzzObject& FuzzConfigurator::get_fuzzer()
{
    return m_fuzzer;
}

const std::string& FuzzConfigurator::get_realm_path() const
{
    return m_path;
}

FuzzLog& FuzzConfigurator::get_logger()
{
    return m_log;
}

State& FuzzConfigurator::get_state()
{
    return m_state;
}

void FuzzConfigurator::init(const std::string& input)
{
    std::string name = "fuzz-test";
    realm::test_util::RealmPathInfo test_context{name};
    m_prefix = "./";
    SHARED_GROUP_TEST_PATH(path);
    m_path = path.c_str();
    if (m_file_path_passed) {
        std::ifstream in(input, std::ios::in | std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Could not open file for reading: " << input << "\n";
            throw;
        }
        std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
        set_state(contents);
    }
    else {
        set_state(input);
    }
}

void FuzzConfigurator::set_state(const std::string& input)
{
    m_state = State{input, 0};
    m_use_encryption = m_fuzzer.get_next_token(m_state) % 2 == 0;
}

const std::string& FuzzConfigurator::get_prefix() const
{
    return m_prefix;
}

void FuzzConfigurator::print_cnf()
{
    if (m_logging) {
        m_log << "// Test case generated in " REALM_VER_CHUNK " on " << m_fuzzer.get_current_time_stamp() << ".\n";
        m_log << "// REALM_MAX_BPNODE_SIZE is " << REALM_MAX_BPNODE_SIZE << "\n";
        m_log << "// ----------------------------------------------------------------------\n";
        const auto& printable_key =
            !m_use_encryption ? "nullptr" : std::string("\"") + m_config.encryption_key.data() + "\"";
        m_log << "const char* key = " << printable_key << ";\n";
        m_log << "\n";
    }
}