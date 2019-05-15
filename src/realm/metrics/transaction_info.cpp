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

#include <realm/metrics/transaction_info.hpp>

#if REALM_METRICS

using namespace realm;
using namespace metrics;

TransactionInfo::TransactionInfo(TransactionInfo::TransactionType type)
    : m_realm_disk_size(0)
    , m_realm_free_space(0)
    , m_total_objects(0)
    , m_type(type)
    , m_num_versions(0)
    , m_num_decrypted_pages(0)
{
    if (m_type == write_transaction) {
        m_fsync_time = std::make_shared<MetricTimerResult>();
        m_write_time = std::make_shared<MetricTimerResult>();
    }
}

TransactionInfo::~TransactionInfo() noexcept
{
}

TransactionInfo::TransactionType TransactionInfo::get_transaction_type() const
{
    return m_type;
}

double TransactionInfo::get_transaction_time() const
{
    return m_transaction_time.get_elapsed_seconds();
}

double TransactionInfo::get_fsync_time() const
{
    if (m_fsync_time) {
        return m_fsync_time->get_elapsed_seconds();
    }
    return 0;
}

double TransactionInfo::get_write_time() const
{
    if (m_write_time) {
        return m_write_time->get_elapsed_seconds();
    }
    return 0;
}

size_t TransactionInfo::get_disk_size() const
{
    return m_realm_disk_size;
}

size_t TransactionInfo::get_free_space() const
{
    return m_realm_free_space;
}

size_t TransactionInfo::get_total_objects() const
{
    return m_total_objects;
}

size_t TransactionInfo::get_num_available_versions() const
{
    return m_num_versions;
}

size_t TransactionInfo::get_num_decrypted_pages() const
{
    return m_num_decrypted_pages;
}

void TransactionInfo::update_stats(size_t disk_size, size_t free_space, size_t total_objects,
                                   size_t available_versions, size_t num_decrypted_pages)
{
    m_realm_disk_size = disk_size;
    m_realm_free_space = free_space;
    m_total_objects = total_objects;
    m_num_versions = available_versions;
    m_num_decrypted_pages = num_decrypted_pages;
}

void TransactionInfo::finish_timer()
{
    m_transaction_time.report_seconds(m_transact_timer.get_elapsed_time());
}

#endif // REALM_METRICS
