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

namespace realm {

using json = nlohmann::json;

class SyncUser;

class Adapter {
public:
    Adapter(std::function<void(std::string)> realm_changed, std::string local_root_dir,
            std::string server_base_url, std::shared_ptr<SyncUser> user);

    enum class InstructionType {
        Insert,
        Delete,
        SetProperty,
        Clear,
        ListSet,
        ListInsert,
        ListErase,
        ListClear,
        AddType,
        AddProperty,
        AddPrimaryKey,
        ChangeIdentity,
        SwapIdentity,
    };

    static std::string instruction_type_string(InstructionType type) {
        switch(type) {
            case InstructionType::Insert:           return "Insert";
            case InstructionType::Delete:           return "Delete";
            case InstructionType::SetProperty:      return "Set";
            case InstructionType::Clear:            return "Clear";
            case InstructionType::ListSet:          return "ListSet";
            case InstructionType::ListInsert:       return "ListInsert";
            case InstructionType::ListErase:        return "ListErase";
            case InstructionType::ListClear:        return "ListClear";
            case InstructionType::AddType:          return "AddType";
            case InstructionType::AddProperty:      return "AddProperty";
            case InstructionType::AddPrimaryKey:    return "AddPrimaryKey";
            case InstructionType::ChangeIdentity:   return "ChangeIdentity";
            case InstructionType::SwapIdentity:     return "SwapIdentity";
        }
    }

    class ChangeSet {
    public:
        ChangeSet(json instructions, SharedRealm realm) : json(instructions), realm(realm) {}
        const json json;
        const SharedRealm realm;
    };

    util::Optional<ChangeSet> current(std::string realm_path);
    void advance(std::string realm_path);

    SharedRealm realm_at_path(std::string path);

    void close() { m_global_notifier.reset(); }

private:
    std::shared_ptr<GlobalNotifier> m_global_notifier;

    class Callback : public GlobalNotifier::Callback {
    public:
        Callback(std::function<void(GlobalNotifier::RealmInfo)> changed) : m_realm_changed(changed) {}
        virtual std::vector<bool> available(std::vector<GlobalNotifier::RealmInfo> realms);
        virtual void realm_changed(GlobalNotifier::ChangeNotification changes);

    private:
        std::function<void(GlobalNotifier::RealmInfo)> m_realm_changed;
    };
};

} // namespace realm

#endif // REALM_SYNC_ADAPTER_HPP
