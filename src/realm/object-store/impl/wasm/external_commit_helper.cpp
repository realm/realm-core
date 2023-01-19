////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#include <realm/object-store/impl/external_commit_helper.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>

#include <emscripten/eventloop.h>

using namespace realm;
using namespace realm::_impl;

ExternalCommitHelper::ExternalCommitHelper(RealmCoordinator& parent, const RealmConfig&)
    : m_parent(parent)
{
}

ExternalCommitHelper::~ExternalCommitHelper()
{
    if (m_timeout) {
        emscripten_clear_timeout(*m_timeout);
    }
}

void ExternalCommitHelper::timeout_callback(void* user_data) {
   auto helper = reinterpret_cast<ExternalCommitHelper*>(user_data);
   helper->m_timeout.reset();
   helper->m_parent.on_change();
}

void ExternalCommitHelper::notify_others() {
    if (m_timeout) {
        emscripten_clear_timeout(*m_timeout);
    }

    m_timeout = emscripten_set_timeout(timeout_callback, 0, this);
}
