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

namespace realm {

class SyncUser;

class Adapter {
public:
    Adapter(std::function<void(std::string)> realm_changed, std::string local_root_dir,
            std::string server_base_url, std::shared_ptr<SyncUser> user);

    class Instruction {
    public:
        enum class Type {
            Insertion,
            SetProperty,
            ListSet,
            ListInsert,
            ListMove,
            ListSwap,
            ListNullify,
            ListClear,
        };

        static std::string type_string(Type type) {
            switch(type) {
                case Type::Insertion:   return "Insert";
                case Type::SetProperty: return "Set";
                case Type::ListSet:     return "ListSet";
                case Type::ListInsert:  return "ListInsert";
                case Type::ListMove:    return "ListMove";
                case Type::ListSwap:    return "ListSwap";
                case Type::ListNullify: return "ListNullify";
                case Type::ListClear:   return "ListClear";
            }
        }

        const Type type;
        const std::string object_type;
        const size_t row;

        const std::string property;
        const bool is_null;
        const Mixed value;

        const size_t list_index = -1;

        Instruction(Type t, std::string o, size_t r) 
        : type(t), object_type(o), row(r), is_null(false), value() {}

        Instruction(Type t, std::string o, size_t r, std::string p) 
        : type(t), object_type(o), row(r), property(p), is_null(false), value() {}

        Instruction(Type t, std::string o, size_t r, std::string p, size_t i, size_t l) 
        : type(t), object_type(o), row(r), property(p), is_null(false), value((int64_t)i), list_index(l) {}

        template<typename T>
        Instruction(std::string o, size_t r, std::string p, bool n, T v)
        : type(Type::SetProperty), object_type(o), row(r), property(p), is_null(n), value(v) {}
    };

    std::vector<Instruction> current(std::string realm_path);
    void advance(std::string realm_path);

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
