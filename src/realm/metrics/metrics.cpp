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

#include <realm/metrics/metrics.hpp>

#if REALM_METRICS

using namespace realm;
using namespace realm::metrics;

Metrics::~Metrics() noexcept
{
}

size_t Metrics::num_query_metrics() const
{
    return bool(m_query_info) ? m_query_info->size() : 0;
}

size_t Metrics::num_transaction_metrics() const
{
    return bool(m_transaction_info) ? m_transaction_info->size() : 0;
}


#endif // REALM_METRICS
