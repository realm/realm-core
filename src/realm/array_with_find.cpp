/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

#include <realm/array_with_find.hpp>

namespace realm {

void ArrayWithFind::find_all(IntegerColumn* result, int64_t value, size_t col_offset, size_t begin, size_t end) const
{
    REALM_ASSERT_3(begin, <=, m_array.size());
    REALM_ASSERT(end == npos || (begin <= end && end <= m_array.size()));

    if (end == npos)
        end = m_array.m_size;

    QueryStateFindAll state(*result);
    REALM_TEMPEX2(find_optimized, Equal, m_array.m_width, (value, begin, end, col_offset, &state, nullptr));

    return;
}


bool ArrayWithFind::find(int cond, int64_t value, size_t start, size_t end, size_t baseindex,
                         QueryStateBase* state) const
{
    if (cond == cond_Equal) {
        return find<Equal>(value, start, end, baseindex, state, nullptr);
    }
    if (cond == cond_NotEqual) {
        return find<NotEqual>(value, start, end, baseindex, state, nullptr);
    }
    if (cond == cond_Greater) {
        return find<Greater>(value, start, end, baseindex, state, nullptr);
    }
    if (cond == cond_Less) {
        return find<Less>(value, start, end, baseindex, state, nullptr);
    }
    if (cond == cond_None) {
        return find<None>(value, start, end, baseindex, state, nullptr);
    }
    else if (cond == cond_LeftNotNull) {
        return find<NotNull>(value, start, end, baseindex, state, nullptr);
    }
    REALM_ASSERT_DEBUG(false);
    return false;
}

size_t ArrayWithFind::first_set_bit(uint32_t v) const
{
    // (v & -v) is UB when v is INT_MIN
    if (int32_t(v) == std::numeric_limits<int32_t>::min())
        return 31;
    static const int MultiplyDeBruijnBitPosition[32] = {0,  1,  28, 2,  29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4,  8,
                                                        31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6,  11, 5,  10, 9};
    return MultiplyDeBruijnBitPosition[(uint32_t((v & -int32_t(v)) * 0x077CB531U)) >> 27];
}

size_t ArrayWithFind::first_set_bit64(int64_t v) const
{
    unsigned int v0 = unsigned(v);
    if (v0 != 0)
        return first_set_bit(v0);
    unsigned int v1 = unsigned(uint64_t(v) >> 32);
    return first_set_bit(v1) + 32;
}


} // namespace realm
