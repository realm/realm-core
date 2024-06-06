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

#ifndef FUZZ_CONFIG_HPP
#define FUZZ_CONFIG_HPP

#include "util.hpp"
#include "fuzz_logger.hpp"

#include <realm/object-store/shared_realm.hpp>

#include <string>
#include <vector>

class FuzzObject;
class FuzzConfigurator {
public:
    FuzzConfigurator(FuzzObject& fuzzer, const std::string& input, bool use_input_file, const std::string& name);
    const realm::Realm::Config& get_config() const;
    FuzzObject& get_fuzzer();
    const std::string& get_realm_path() const;
    FuzzLog& get_logger();
    State& get_state();
    void set_state(const std::string& input);
    void print_cnf();

private:
    void init(const std::string&);
    void setup_realm_config();

    realm::Realm::Config m_config;
    std::string m_path;
    FuzzLog m_log;
    bool m_use_encryption{false};
    bool m_used_input_file{false};
    FuzzObject& m_fuzzer;
    State m_state;
    std::string m_fuzz_name;
};
#endif
