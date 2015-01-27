#include "tightdb/array_integer.hpp"

#include <vector>

using namespace tightdb;


std::vector<int64_t> ArrayInteger::ToVector() const
{
    std::vector<int64_t> v;
    const size_t count = size();
    for (size_t t = 0; t < count; ++t)
        v.push_back(get_data(t));
    return v;
}

void ArrayIntNull::ensure_non_null(int64_t value)
{
    if (m_width == 64) {
        if (value == m_null) {
            // Find a new value for m_null, and if it's usable, replace existing
            // nulls with the new magic value.
            while (true) {
                // FIXME: This isn't exploit-proof (except on OpenBSD), because rand() is only pseudorandom,
                // and btw also isn't thread-safe. Solution is to provide a better random function.
                int64_t candidate = static_cast<int64_t>(rand()) * rand() * rand();
                if (can_use_as_null(candidate)) {
                    replace_nulls_with(candidate);
                    break;
                }
            }
        }
    }
    else {
        if (value >= m_ubound) {
            m_null = m_ubound;

            size_t new_width = bit_width(value + 1); // +1 because we need room for ubound too

            if (new_width == 64) {
                // Width will be upgraded to 64, so we can just choose a new NULL just outside
                // the bounds.
                replace_nulls_with(m_ubound + 1); // Expands array
            }
            else {
                TIGHTDB_ASSERT(new_width <= 32);
                int64_t new_null = (1UL << (new_width - 1)) - 1; // == m_ubound after upgrade
                replace_nulls_with(new_null); // Expands array
            }
        }
    }
}
