/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef TEST_TEST_TABLE_HELPER_HPP_
#define TEST_TEST_TABLE_HELPER_HPP_

namespace realm {

class ObjKeyVector : public std::vector<ObjKey> {
public:
    ObjKeyVector(const std::vector<int64_t>& init)
    {
        reserve(init.size());
        for (auto i : init) {
            emplace_back(i);
        }
    }
};

class MyTrivialReplication : public Replication {
public:
    HistoryType get_history_type() const noexcept override
    {
        return hist_None;
    }

    int get_history_schema_version() const noexcept override
    {
        return 0;
    }

    bool is_upgradable_history_schema(int) const noexcept override
    {
        REALM_ASSERT(false);
        return false;
    }

    void upgrade_history_schema(int) override
    {
        REALM_ASSERT(false);
    }

    _impl::History* _get_history_write() override
    {
        return nullptr;
    }

    std::unique_ptr<_impl::History> _create_history_read() override
    {
        return {};
    }

    void do_initiate_transact(Group& group, version_type version, bool hist_updated) override
    {
        Replication::do_initiate_transact(group, version, hist_updated);
        m_group = &group;
    }

protected:
    version_type prepare_changeset(const char* data, size_t size, version_type orig_version) override
    {
        m_incoming_changeset = util::Buffer<char>(size); // Throws
        std::copy(data, data + size, m_incoming_changeset.data());
        // Make space for the new changeset in m_changesets such that we can be
        // sure no exception will be thrown whan adding the changeset in
        // finalize_changeset().
        m_changesets.reserve(m_changesets.size() + 1); // Throws
        return orig_version + 1;
    }

    void finalize_changeset() noexcept override
    {
        // The following operation will not throw due to the space reservation
        // carried out in prepare_new_changeset().
        m_changesets.push_back(std::move(m_incoming_changeset));
    }

    util::Buffer<char> m_incoming_changeset;
    std::vector<util::Buffer<char>> m_changesets;
    Group* m_group;
};

class ReplSyncClient : public MyTrivialReplication {
public:
    ReplSyncClient(int history_schema_version, uint64_t file_ident = 0)
        : m_history_schema_version(history_schema_version)
        , m_file_ident(file_ident)
    {
    }

    void initialize(DB& sg) override
    {
        Replication::initialize(sg);
    }

    version_type prepare_changeset(const char*, size_t, version_type version) override
    {
        if (!m_arr) {
            using gf = _impl::GroupFriend;
            Allocator& alloc = gf::get_alloc(*m_group);
            m_arr = std::make_unique<BinaryColumn>(alloc);
            gf::prepare_history_parent(*m_group, *m_arr, hist_SyncClient, m_history_schema_version, 0);
            m_arr->create();
            m_arr->add(BinaryData("Changeset"));
        }
        return version + 1;
    }

    bool is_upgraded() const
    {
        return m_upgraded;
    }

    bool is_upgradable_history_schema(int) const noexcept override
    {
        return true;
    }

    void upgrade_history_schema(int) override
    {
        m_group->set_sync_file_id(m_file_ident);
        m_upgraded = true;
    }

    HistoryType get_history_type() const noexcept override
    {
        return hist_SyncClient;
    }

    int get_history_schema_version() const noexcept override
    {
        return m_history_schema_version;
    }

private:
    int m_history_schema_version;
    uint64_t m_file_ident;
    bool m_upgraded = false;
    std::unique_ptr<BinaryColumn> m_arr;
};
}

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

#endif /* TEST_TEST_TABLE_HELPER_HPP_ */
