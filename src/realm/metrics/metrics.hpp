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

#ifndef REALM_METRICS_HPP
#define REALM_METRICS_HPP

#include <vector>

#include <realm/metrics/query_info.hpp>
#include <realm/metrics/transaction_info.hpp>
#include <realm/util/features.h>

namespace realm {
namespace metrics {

#if REALM_METRICS

class Metrics {
public:
    ~Metrics() noexcept;

private:
    std::vector<QueryInfo> m_query_info;
    std::vector<TransactionInfo> m_transaction_info;
};


#else

struct Metrics
{
};

#endif // REALM_METRICS

} // namespace metrics
} // namespace realm



#endif // REALM_METRICS_HPP
