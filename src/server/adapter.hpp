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
    Adapter(std::function<void(SharedRealm)> realm_changed, std::string local_root_dir,
            std::string server_base_url, std::shared_ptr<SyncUser> user);
private:
    std::shared_ptr<GlobalNotifier> m_global_notifier;

    class Callback : public GlobalNotifier::Callback {
    public:
        Callback(std::function<void(SharedRealm)> changed) : m_realm_changed(changed) {}
        virtual std::vector<bool> available(std::vector<std::pair<std::string, std::string>> realms,
                                            std::vector<bool> new_realms,
                                            bool all);
        virtual void realm_changed(GlobalNotifier::ChangeNotification changes);

    private:
        std::function<void(SharedRealm)> m_realm_changed;
    };
};

} // namespace realm

#endif // REALM_SYNC_ADAPTER_HPP
