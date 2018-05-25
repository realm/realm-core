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

#include <stdexcept>
#include <utility>
#include <iomanip>

#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/db.hpp>
#include <realm/replication.hpp>
#include <realm/util/logger.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_string.hpp>
#include <realm/array_binary.hpp>
#include <realm/array_timestamp.hpp>

using namespace realm;
using namespace realm::util;


namespace {

class InputStreamImpl : public _impl::NoCopyInputStream {
public:
    InputStreamImpl(const char* data, size_t size) noexcept
        : m_begin(data)
        , m_end(data + size)
    {
    }

    ~InputStreamImpl() noexcept
    {
    }

    bool next_block(const char*& begin, const char*& end) override
    {
        if (m_begin != 0) {
            begin = m_begin;
            end = m_end;
            m_begin = nullptr;
            return (end > begin);
        }
        return false;
    }
    const char* m_begin;
    const char* const m_end;
};

} // anonymous namespace

std::string TrivialReplication::get_database_path()
{
    return m_database_file;
}

void TrivialReplication::initialize(DB&)
{
    // Nothing needs to be done here
}

void TrivialReplication::do_initiate_transact(Group& group, version_type, bool history_updated)
{
    m_group = &group;
    char* data = m_stream.get_data();
    size_t size = m_stream.get_size();
    set_buffer(data, data + size);
    m_history_updated = history_updated;
}

Replication::version_type TrivialReplication::do_prepare_commit(version_type orig_version)
{
    char* data = m_stream.get_data();
    size_t size = write_position() - data;
    version_type new_version = prepare_changeset(data, size, orig_version); // Throws
    return new_version;
}

void TrivialReplication::do_finalize_commit() noexcept
{
    finalize_changeset();
}

void TrivialReplication::do_abort_transact() noexcept
{
}

void TrivialReplication::do_interrupt() noexcept
{
}

void TrivialReplication::do_clear_interrupt() noexcept
{
}

