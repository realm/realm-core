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

#include <realm/group.hpp>
#include <realm/metrics/metrics.hpp>

#if REALM_METRICS

using namespace realm;
using namespace realm::metrics;

Metrics::Metrics(size_t max_history_size)
    : m_max_num_queries(max_history_size)
    , m_max_num_transactions(max_history_size)
{
    m_query_info = std::make_unique<QueryInfoList>(max_history_size);
    m_transaction_info = std::make_unique<TransactionInfoList>(max_history_size);
}

Metrics::~Metrics() noexcept
{
}

size_t Metrics::num_query_metrics() const
{
    return m_query_info ? m_query_info->size() : 0;
}

size_t Metrics::num_transaction_metrics() const
{
    return m_transaction_info ? m_transaction_info->size() : 0;
}

void Metrics::add_query(QueryInfo info)
{
    REALM_ASSERT_DEBUG(m_query_info);
    m_query_info->insert(info);
}

void Metrics::add_transaction(TransactionInfo info)
{
    REALM_ASSERT_DEBUG(m_transaction_info);
    m_transaction_info->insert(info);
}

void Metrics::start_read_transaction()
{
    REALM_ASSERT_DEBUG(!m_pending_read);
    m_pending_read = std::make_unique<TransactionInfo>(TransactionInfo::read_transaction);
}

void Metrics::start_write_transaction()
{
    REALM_ASSERT_DEBUG(!m_pending_write);
    m_pending_write = std::make_unique<TransactionInfo>(TransactionInfo::write_transaction);
}

void Metrics::end_read_transaction(size_t total_size, size_t free_space, size_t num_objects, size_t num_versions,
                                   size_t num_decrypted_pages)
{
    REALM_ASSERT_DEBUG(m_transaction_info);
    if (m_pending_read) {
        m_pending_read->update_stats(total_size, free_space, num_objects, num_versions, num_decrypted_pages);
        m_pending_read->finish_timer();
        add_transaction(*m_pending_read);
        m_pending_read.reset(nullptr);
    }
}

void Metrics::end_write_transaction(size_t total_size, size_t free_space, size_t num_objects, size_t num_versions,
                                    size_t num_decrypted_pages)
{
    REALM_ASSERT_DEBUG(m_transaction_info);
    if (m_pending_write) {
        m_pending_write->update_stats(total_size, free_space, num_objects, num_versions, num_decrypted_pages);
        m_pending_write->finish_timer();
        add_transaction(*m_pending_write);
        m_pending_write.reset(nullptr);
    }
}

std::unique_ptr<MetricTimer> Metrics::report_fsync_time(const Group& g)
{
    std::shared_ptr<Metrics> instance = g.get_metrics();
    if (instance) {
        REALM_ASSERT_DEBUG(instance->m_transaction_info);
        if (instance->m_pending_write) {
            return std::make_unique<MetricTimer>(instance->m_pending_write->m_fsync_time);
        }

    }
    return nullptr;
}

std::unique_ptr<MetricTimer> Metrics::report_write_time(const Group& g)
{
    std::shared_ptr<Metrics> instance = g.get_metrics();
    if (instance) {
        REALM_ASSERT_DEBUG(instance->m_transaction_info);
        if (instance->m_pending_write) {
            return std::make_unique<MetricTimer>(instance->m_pending_write->m_write_time);
        }

    }
    return nullptr;
}


std::unique_ptr<Metrics::QueryInfoList> Metrics::take_queries()
{

    std::unique_ptr<QueryInfoList> values = std::make_unique<QueryInfoList>(m_max_num_queries);
    values.swap(m_query_info);
    return values;
}

std::unique_ptr<Metrics::TransactionInfoList> Metrics::take_transactions()
{
    std::unique_ptr<TransactionInfoList> values = std::make_unique<TransactionInfoList>(m_max_num_transactions);
    values.swap(m_transaction_info);
    return values;
}



#endif // REALM_METRICS
