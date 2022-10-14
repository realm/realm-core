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
    : m_fuzzer(fuzzer)
{
    realm::disable_sync_to_disk();
    init(argc, argv);
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

bool FuzzConfigurator::is_stdin_filename_enabled() const
{
    return m_file_names_from_stdin;
}

std::ostream* FuzzConfigurator::get_logger()
{
    return m_logging ? &(m_log) : nullptr;
}

State& FuzzConfigurator::get_state()
{
    return m_state;
}

void FuzzConfigurator::usage(const char* argv[])
{
    fprintf(stderr,
            "Usage: %s {FILE | --} [--log] [--name NAME] [--prefix PATH]\n"
            "Where FILE is a instruction file that will be replayed.\n"
            "Pass -- without argument to read filenames from stdin\n"
            "Pass --log to have code printed to stdout producing the same instructions.\n"
            "Pass --name NAME with distinct values when running on multiple threads,\n"
            "                 to make sure the test don't use the same Realm file\n"
            "Pass --prefix PATH to supply a path that should be prepended to all filenames\n"
            "                 read from stdin.\n",
            argv[0]);
    throw;
}

void FuzzConfigurator::init(int argc, const char* argv[])
{
    std::string name = "fuzz-test";
    realm::test_util::RealmPathInfo test_context{name};
    m_prefix = "./";
    SHARED_GROUP_TEST_PATH(path);
    size_t file_arg = size_t(-1);
    for (size_t i = 1; i < size_t(argc); ++i) {
        std::string arg = argv[i];
        if (arg == "--log") {
            m_log.open("fuzz_log.txt");
            m_log << path.c_str() << std::endl;
            m_log << "Init realm " << std::endl;
            m_logging = true;
        }
        else if (arg == "--") {
            m_file_names_from_stdin = true;
        }
        else if (arg == "--prefix") {
            m_prefix = argv[++i];
        }
        else if (arg == "--name") {
            name = argv[++i];
        }
        else {
            file_arg = i;
        }
    }
    if (!m_file_names_from_stdin && file_arg == size_t(-1)) {
        usage(argv);
    }

    m_path = path.c_str();
    if (!m_file_names_from_stdin) {
        std::ifstream in(argv[file_arg], std::ios::in | std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Could not open file for reading: " << argv[file_arg] << "\n";
            throw;
        }
        std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
        set_state(contents);
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
        std::string printable_key;
        if (!m_use_encryption) {
            printable_key = "nullptr";
        }
        else {
            printable_key = std::string("\"") + m_config.encryption_key.data() + "\"";
        }
        m_log << "const char* key = " << printable_key << ";\n";
        m_log << "\n";
    }
}