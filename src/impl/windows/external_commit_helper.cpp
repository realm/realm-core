////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include "impl/external_commit_helper.hpp"
#include "impl/realm_coordinator.hpp"

#include <algorithm>

using namespace realm;
using namespace realm::_impl;
using namespace realm::util;

static std::string normalize_realm_path_for_windows_kernel_object_name(std::string realm_path) {
    // windows named objects names should not contain backslash
    std::replace(realm_path.begin(), realm_path.end(), '\\', '/');

    // always use lowercase for the drive letter as a win32 named objects name
    auto position = realm_path.find(':');
    if (position != std::string::npos && position > 0) {
        realm_path[position - 1] = tolower(realm_path[position - 1]);
    }

    return realm_path;
}

static std::string create_condvar_sharedmemory_name(std::string realm_path) {
    realm_path = normalize_realm_path_for_windows_kernel_object_name(realm_path);

    std::string name("Local\\Realm_ObjectStore_ExternalCommitHelper_SharedCondVar_");
    name.append(realm_path);
    return name;
}

ExternalCommitHelper::ExternalCommitHelper(RealmCoordinator& parent)
: m_parent(parent)
, m_condvar_shared(create_condvar_sharedmemory_name(parent.get_path()))
{
    m_mutex.set_shared_part(InterprocessMutex::SharedPart(), 
        normalize_realm_path_for_windows_kernel_object_name(parent.get_path()), 
        "ExternalCommitHelper_ControlMutex");

    m_commit_available.set_shared_part(m_condvar_shared.get(), 
        normalize_realm_path_for_windows_kernel_object_name(parent.get_path()),
        "ExternalCommitHelper_CommitCondVar",
        normalize_realm_path_for_windows_kernel_object_name(std::filesystem::temp_directory_path().u8string()));

    m_thread = std::async(std::launch::async, [this]() { listen(); });
}

ExternalCommitHelper::~ExternalCommitHelper()
{
    {
        std::lock_guard<InterprocessMutex> lock(m_mutex);
        m_keep_listening = false;
        m_commit_available.notify_all();
    }
    m_thread.wait();

    m_commit_available.release_shared_part();
}

void ExternalCommitHelper::notify_others()
{
    std::lock_guard<InterprocessMutex> lock(m_mutex);
    m_commit_available.notify_all();
}

void ExternalCommitHelper::listen()
{
    std::lock_guard<InterprocessMutex> lock(m_mutex);
    while (m_keep_listening) {
        m_commit_available.wait(m_mutex, nullptr);
        if (m_keep_listening) {
            m_mutex.unlock();
			m_parent.on_change();
            m_mutex.lock();
        }
    }
}
