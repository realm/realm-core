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
#include <mpark/variant.hpp>

namespace realm {

class SyncUser;

class Adapter {
public:
    Adapter(std::function<void(std::string)> realm_changed, std::string local_root_dir,
            std::string server_base_url, std::shared_ptr<SyncUser> user);

    class Instruction {
    public:
        enum class Type {
            Insert,
            Delete,
            SetProperty,
            Clear,
            ListSet,
            ListInsert,
            ListMove,
            ListSwap,
            ListNullify,
            ListClear,
            AddType,
            AddProperty,
        };

        static std::string type_string(Type type) {
            switch(type) {
                case Type::Insert:      return "Insert";
                case Type::Delete:      return "Delete";
                case Type::SetProperty: return "Set";
                case Type::Clear:       return "Clear";
                case Type::ListSet:     return "ListSet";
                case Type::ListInsert:  return "ListInsert";
                case Type::ListMove:    return "ListMove";
                case Type::ListSwap:    return "ListSwap";
                case Type::ListNullify: return "ListNullify";
                case Type::ListClear:   return "ListClear";
                case Type::AddType:     return "AddType";
                case Type::AddProperty: return "AddProperty";
            }
        }

        const Type type;
        const std::string object_type;

        const size_t row = -1;

        const std::string property;
        const bool is_null = false;
        const mpark::variant<bool, int64_t, double, std::string, Timestamp, size_t> value;
        const PropertyType data_type = PropertyType::Int;

        const std::string target_object_type = "";
        const bool nullable = false;

        const size_t list_index = -1;

        Instruction(Type t, std::string o) 
        : type(t), object_type(o), value() {}

        Instruction(Type t, std::string o, size_t r) 
        : type(t), object_type(o), row(r), value() {}

        Instruction(Type t, std::string o, size_t r, std::string p) 
        : type(t), object_type(o), row(r), property(p), value() {}

        Instruction(Type t, std::string o, size_t r, std::string p, size_t i, size_t l) 
        : type(t), object_type(o), row(r), property(p), value(i), list_index(l) {}

        Instruction(std::string o, std::string p, PropertyType t, bool n, std::string l = "") 
        : type(Type::AddProperty), object_type(o), property(p), value(), data_type(t), target_object_type(l) {}

        template<typename T>
        Instruction(std::string o, size_t r, std::string p, PropertyType t, bool n, T v)
        : type(Type::SetProperty), object_type(o), row(r), property(p), is_null(n), value(v), data_type(t) {}
    };

    class ChangeSet : public std::vector<Instruction> {
    public:
        ChangeSet(SharedRealm realm) : std::vector<Instruction>(), m_realm(realm) {}
    private:
        SharedRealm m_realm;
    };

    util::Optional<ChangeSet> current(std::string realm_path);
    void advance(std::string realm_path);

    SharedRealm realm_at_path(std::string path);

private:
    std::shared_ptr<GlobalNotifier> m_global_notifier;

    class Callback : public GlobalNotifier::Callback {
    public:
        Callback(std::function<void(GlobalNotifier::RealmInfo)> changed) : m_realm_changed(changed) {}
        virtual std::vector<bool> available(std::vector<GlobalNotifier::RealmInfo> realms,
                                            std::vector<bool> new_realms,
                                            bool all);
        virtual void realm_changed(GlobalNotifier::ChangeNotification changes);

    private:
        std::function<void(GlobalNotifier::RealmInfo)> m_realm_changed;
    };
};

} // namespace realm

#endif // REALM_SYNC_ADAPTER_HPP
