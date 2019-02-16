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
#include <realm/query_engine.hpp>

#if REALM_METRICS

using namespace realm;
using namespace realm::metrics;

QueryInfo::QueryInfo(const Query* query, QueryType type)
    : m_type(type)
{
    REALM_ASSERT(query);

    const Group* group = query->m_table->get_parent_group();
    REALM_ASSERT(group);

    m_description = query->get_description();
    m_table_name = query->m_table->get_name();
}

QueryInfo::~QueryInfo() noexcept
{
}

std::string QueryInfo::get_description() const
{
    return m_description;
}

std::string QueryInfo::get_table_name() const
{
    return m_table_name;
}

QueryInfo::QueryType QueryInfo::get_type() const
{
    return m_type;
}

double QueryInfo::get_query_time() const
{
    if (m_query_time) {
        return m_query_time->get_elapsed_seconds();
    }
    return 0;
}

std::unique_ptr<MetricTimer> QueryInfo::track(const Query* query, QueryType type)
{
    REALM_ASSERT_DEBUG(query);

    if (!bool(query->m_table) || !query->m_table->is_attached()) {
        return nullptr;
    }

    const Group* group = query->m_table->get_parent_group();

    // If the table is not attached to a group we cannot track it's metrics.
    if (!group)
        return nullptr;

    std::shared_ptr<Metrics> metrics = group->get_metrics();
    if (!metrics)
        return nullptr;

    QueryInfo info(query, type);
    info.m_query_time = std::make_shared<MetricTimerResult>();
    metrics->add_query(info);

    return std::make_unique<MetricTimer>(info.m_query_time);
}

QueryInfo::QueryType QueryInfo::type_from_action(Action action)
{
    switch (action)
    {
        case act_ReturnFirst:
            return type_Find;
        case act_Sum:
            return type_Sum;
        case act_Max:
            return type_Maximum;
        case act_Min:
            return type_Minimum;
        case act_Average:
            return type_Average;
        case act_Count:
            return type_Count;
        case act_FindAll:
            return type_FindAll;
        case act_CallIdx:
        case act_CallbackIdx:
        case act_CallbackVal:
        case act_CallbackNone:
        case act_CallbackBoth:
            return type_Invalid;
    };
    REALM_UNREACHABLE();
}

#endif // REALM_METRICS
