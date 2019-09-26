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

#ifndef REALM_SYNC_ADAPTER_HPP
#define REALM_SYNC_ADAPTER_HPP

#include "shared_realm.hpp"
#include "sync/sync_config.hpp"

#include <regex>

namespace realm {

class SyncUser;
class SyncLoggerFactory;

class Adapter {
public:
    Adapter(std::function<void(std::string)> realm_changed, std::function<bool(const std::string&)> should_watch_realm_predicate,
            std::string local_root_dir, SyncConfig sync_config_template);

    enum class InstructionType {
        Insert,
        Delete,
        Set,
        Clear,
        ListSet,
        ListInsert,
        ListErase,
        ListClear,
        AddType,
        AddProperties,
    };

    static std::string instruction_type_string(InstructionType type) {
        switch (type) {
            case InstructionType::Insert:           return "INSERT";
            case InstructionType::Delete:           return "DELETE";
            case InstructionType::Set:              return "SET";
            case InstructionType::Clear:            return "CLEAR";
            case InstructionType::ListSet:          return "LIST_SET";
            case InstructionType::ListInsert:       return "LIST_INSERT";
            case InstructionType::ListErase:        return "LIST_ERASE";
            case InstructionType::ListClear:        return "LIST_CLEAR";
            case InstructionType::AddType:          return "ADD_TYPE";
            case InstructionType::AddProperties:    return "ADD_PROPERTIES";
        }
        REALM_COMPILER_HINT_UNREACHABLE();
    }

    util::Optional<util::AppendBuffer<char>> current(std::string realm_path);
    void advance(std::string realm_path);

    Realm::Config get_config(std::string path, util::Optional<Schema> schema = util::none);

    void close() { m_impl.reset(); }

private:
    class Impl;
    std::shared_ptr<Impl> m_impl;
};

} // namespace realm

#endif // REALM_SYNC_ADAPTER_HPP
