////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
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

#include "common_utils.hpp"

#include <iostream>

std::error_code wait_for_upload(realm::Realm& realm, std::chrono::seconds timeout)
{
    return wait_for_session(realm, &realm::SyncSession::wait_for_upload_completion, timeout);
}

std::error_code wait_for_download(realm::Realm& realm, std::chrono::seconds timeout)
{
    return wait_for_session(realm, &realm::SyncSession::wait_for_download_completion, timeout);
}

namespace realm {

void timed_sleeping_wait_for(util::FunctionRef<bool()> condition, std::chrono::milliseconds max_ms)
{
    const auto wait_start = std::chrono::steady_clock::now();
    while (!condition()) {
        if (std::chrono::steady_clock::now() - wait_start > max_ms) {
            throw std::runtime_error(util::format("timed_sleeping_wait_for exceeded %1 ms", max_ms.count()));
        }
        millisleep(1);
    }
}

namespace reset_utils {

Obj create_object(Realm& realm, StringData object_type, util::Optional<ObjectId> primary_key,
                  util::Optional<Partition> partition)
{
    auto table = realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
    REALM_ASSERT(table);
    FieldValues values = {};
    if (partition) {
        ColKey col = table->get_column_key(partition->property_name);
        REALM_ASSERT(col);
        values.insert(col, Mixed{partition->value});
    }
    return table->create_object_with_primary_key(primary_key ? *primary_key : ObjectId::gen(), std::move(values));
}

TestClientReset::TestClientReset(const Realm::Config& local_config, const Realm::Config& remote_config)
    : m_local_config(local_config)
    , m_remote_config(remote_config)
{
}
TestClientReset::~TestClientReset()
{
    // make sure we didn't forget to call run()
    REALM_ASSERT(m_did_run || !(m_make_local_changes || m_make_remote_changes || m_on_post_local || m_on_post_reset));
}

TestClientReset* TestClientReset::setup(Callback&& on_setup)
{
    m_on_setup = std::move(on_setup);
    return this;
}
TestClientReset* TestClientReset::make_local_changes(Callback&& changes_local)
{
    m_make_local_changes = std::move(changes_local);
    return this;
}
TestClientReset* TestClientReset::make_remote_changes(Callback&& changes_remote)
{
    m_make_remote_changes = std::move(changes_remote);
    return this;
}
TestClientReset* TestClientReset::on_post_local_changes(Callback&& post_local)
{
    m_on_post_local = std::move(post_local);
    return this;
}
TestClientReset* TestClientReset::on_post_reset(Callback&& post_reset)
{
    m_on_post_reset = std::move(post_reset);
    return this;
}

void TestClientReset::set_pk_of_object_driving_reset(const ObjectId& pk)
{
    m_pk_driving_reset = pk;
}

ObjectId TestClientReset::get_pk_of_object_driving_reset() const
{
    return m_pk_driving_reset;
}

void TestClientReset::disable_wait_for_reset_completion()
{
    m_wait_for_reset_completion = false;
}

} // namespace reset_utils
} // namespace realm
