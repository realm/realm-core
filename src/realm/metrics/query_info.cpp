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

#include <realm/metrics/query_info.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/query.hpp>

#if REALM_METRICS

using namespace realm;
using namespace realm::metrics;

QueryInfo::QueryInfo(const Query* query)
{
    REALM_ASSERT(query);

    const Group* group = query->m_table->get_parent_group();
    REALM_ASSERT(group);

    StringData table_name = group->get_table_name(query->m_table->get_index_in_group());

    m_type = table_name;
}

QueryInfo::~QueryInfo() noexcept
{
}

std::unique_ptr<MetricTimer> QueryInfo::track(const Query* query)
{
    REALM_ASSERT(query);
    const Group* group = query->m_table->get_parent_group();
    REALM_ASSERT(group);

    std::shared_ptr<Metrics> metrics = group->get_metrics();
    if (!metrics)
        return nullptr;

    QueryInfo info(query);
    info.m_query_time = std::make_shared<MetricTimerResult>();
    metrics->add_query(info);

    return std::make_unique<MetricTimer>(info.m_query_time);
}


#endif // REALM_METRICS
