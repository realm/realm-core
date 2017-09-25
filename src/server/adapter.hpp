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

#include "global_notifier.hpp"
#include "property.hpp"

#include <json.hpp>
#include <set>
#include <regex>

namespace realm {

class SyncUser;

class Adapter {
public:
    Adapter(std::function<void(std::string)> realm_changed, std::string local_root_dir,
            std::string server_base_url, std::shared_ptr<SyncUser> user, std::function<SyncBindSessionHandler> bind_callback, std::regex regex);

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
        switch(type) {
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
    }

    class ChangeSet {
    public:
        ChangeSet(nlohmann::json instructions, SharedRealm realm) : json(instructions), realm(realm) {}
        const nlohmann::json json;
        const SharedRealm realm;
    };

    util::Optional<ChangeSet> current(std::string realm_path);
    void advance(std::string realm_path);

    realm::Realm::Config get_config(std::string path, util::Optional<Schema> schema = util::none);

    void close() { m_global_notifier.reset(); }

private:
    std::shared_ptr<GlobalNotifier> m_global_notifier;

    class Callback : public GlobalNotifier::Callback {
    public:
        Callback(std::function<void(std::string)> changed, std::regex regex) : m_realm_changed(changed), m_regex(regex) {}
        virtual std::vector<bool> available(const std::vector<std::string>& realms);
        virtual void realm_changed(GlobalNotifier::ChangeNotification changes);

    private:
        std::function<void(std::string)> m_realm_changed;
        std::regex m_regex;
    };
};

} // namespace realm

#endif // REALM_SYNC_ADAPTER_HPP
