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

#include "<realm/disable_sync_to_disk.hpp>
#include "<util/test_path.hpp>
#include <iostream>

FuzzConfigurator::FuzzConfigurator(int argc, const char* argv[])
{
    std::string name = "fuzz-test";
    std::string prefix = "./";
    bool file_names_from_stdin = false;

    realm::test_util::RealmPathInfo test_context{name};
    SHARED_GROUP_TEST_PATH(path);

    size_t file_arg = size_t(-1);
    for (size_t i = 1; i < size_t(argc); ++i) {
        std::string arg = argv[i];
        if (arg == "--log") {
            m_log.open("fuzz_log.txt");
            m_log << path.c_str() << std::endl;
            m_log << "Init realm " << std::endl;
            logging = true;
        }
        else if (arg == "--") {
            file_names_from_stdin = true;
        }
        else if (arg == "--prefix") {
            prefix = argv[++i];
        }
        else if (arg == "--name") {
            name = argv[++i];
        }
        else {
            file_arg = i;
        }
    }

    if (!file_names_from_stdin && file_arg == size_t(-1)) {
        usage(argv);
    }

    disable_sync_to_disk();

    std::ifstream in(argv[file_arg], std::ios::in | std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "Could not open file for reading: " << argv[file_arg] << "\n";
        exit(1);
    }

    std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));

    m_path = path.c_str();
    m_contents.swap(contents);
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
    exit(1);
}