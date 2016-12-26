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

#include "adapter.hpp"

using namespace realm;

Adapter::Adapter(std::function<void(SharedRealm)> realm_changed,
                 std::string local_root_dir, std::string server_base_url,
                 std::shared_ptr<SyncUser> user)
: m_global_notifier(GlobalNotifier::shared_notifier(std::make_unique<Adapter::Callback>(realm_changed), 
    local_root_dir, server_base_url, user, false))
{
    m_global_notifier->start();
    //m_global_notifier->add_realm("7df151ce2479057c21fe28d6830f5a17", "/7df151ce2479057c21fe28d6830f5a17/test");
}

std::vector<bool> Adapter::Callback::available(std::vector<std::pair<std::string, std::string>> realms,
                                               std::vector<bool> new_realms,
                                               bool all) {
    std::cout << "AVAILABLE" << std::endl;
    std::vector<bool> watch;
    for (auto realm : realms) {
        std::cout << realm.second << std::endl;
        watch.push_back(true);
    }
    return watch;
}

void Adapter::Callback::realm_changed(GlobalNotifier::ChangeNotification changes) {
    std::cout << "CHANGED " << changes.get_path() << std::endl;
    m_realm_changed(changes.get_new_realm());
}
