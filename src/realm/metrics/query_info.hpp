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

#ifndef REALM_QUERY_INFO_HPP
#define REALM_QUERY_INFO_HPP

#include <string>

#include <realm/util/features.h>

#if REALM_METRICS

namespace realm {

class Query; // forward declare in namespace realm

namespace metrics {

enum QueryType {
    query_find,
    query_find_all,
    query_count,
    query_sum,
    query_average,
    query_maximum
};

class QueryInfo {
public:
    QueryInfo(const Query* query);
    ~QueryInfo() noexcept;

private:
    std::string m_type;
    double m_start_time;
    double m_end_time;
};

} // namespace metrics
} // namespace realm

#endif // REALM_METRICS

#endif // REALM_QUERY_INFO_HPP
