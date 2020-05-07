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

#ifndef REALM_TEST_CLIENT_STATISTICS_HPP
#define REALM_TEST_CLIENT_STATISTICS_HPP

#include <cstddef>

namespace realm {
namespace test_client {

/// Specified sample must be sorted ascendingly. Also, the iterator type must be
/// of the 'random access' type. This function returns zero if the sample is
/// empty.
template <class I>
double fractile(I begin, I end, double fraction) noexcept
{
    std::size_t n = end - begin;
    if (n == 0)
        return 0;
    double point = fraction * n;
    double point_1 = point + 0.5;
    std::size_t i_1 = std::size_t(point_1);
    if (i_1 == 0)
        return double(begin[i_1]);
    std::size_t i_0 = i_1 - 1;
    if (i_0 >= n - 1)
        return double(begin[i_0]);
    double point_0 = point_1 - 1;
    double weight_0 = double(i_1 - point_0);
    double weight_1 = double(point_1 - i_1);
    return double(weight_0 * begin[i_0] + weight_1 * begin[i_1]);
}

} // namespace test_client
} // namespace realm

#endif // REALM_TEST_CLIENT_STATISTICS_HPP
