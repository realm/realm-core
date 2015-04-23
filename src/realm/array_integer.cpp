#include "realm/array_integer.hpp"
#include "realm/column.hpp"

#include <vector>

using namespace realm;

// Find max and min value, but break search if difference exceeds 'maxdiff' (in which case *min and *max is set to 0)
// Useful for counting-sort functions
template <size_t w>
bool ArrayInteger::minmax(size_t from, size_t to, uint64_t maxdiff, int64_t *min, int64_t *max) const
{
    int64_t min2;
    int64_t max2;
    size_t t;

    max2 = Array::get<w>(from);
    min2 = max2;

    for (t = from + 1; t < to; t++) {
        int64_t v = Array::get<w>(t);
        // Utilizes that range test is only needed if max2 or min2 were changed
        if (v < min2) {
            min2 = v;
            if (uint64_t(max2 - min2) > maxdiff)
                break;
        }
        else if (v > max2) {
            max2 = v;
            if (uint64_t(max2 - min2) > maxdiff)
                break;
        }
    }

    if (t < to) {
        *max = 0;
        *min = 0;
        return false;
    }
    else {
        *max = max2;
        *min = min2;
        return true;
    }
}


std::vector<int64_t> ArrayInteger::ToVector() const
{
    std::vector<int64_t> v;
    const size_t count = size();
    for (size_t t = 0; t < count; ++t)
        v.push_back(Array::get(t));
    return v;
}

MemRef ArrayIntNull::create_array(Type type, bool context_flag, std::size_t size, int_fast64_t value, Allocator& alloc)
{
    MemRef r = Array::create(type, context_flag, wtype_Bits, size + 1, value, alloc);
    ArrayIntNull arr(alloc);
    arr.init_from_mem(r);
    if (arr.m_width == 64) {
        int_fast64_t null_value = value ^ 1; // Just anything different from value.
        arr.Array::set(0, null_value);
    }
    else {
        arr.Array::set(0, arr.m_ubound);
    }
    return r;
}

namespace {
    int64_t next_null_candidate(int64_t previous_candidate) {
        uint64_t x = static_cast<uint64_t>(previous_candidate);
        // Increment by a prime number. This guarantees that we will
        // eventually hit every possible integer in the 2^64 range.
        x += 0xfffffffbULL;
        return static_cast<int64_t>(x);
    }
}

int_fast64_t ArrayIntNull::choose_random_null(int64_t incoming)
{
    // We just need any number -- it could have been `rand()`, but
    // random numbers are hard, and we don't want to risk locking mutices
    // or saving state. The top of the stack should be "random enough".
    int64_t candidate = reinterpret_cast<int64_t>(&candidate);

    while (true) {
        candidate = next_null_candidate(candidate);
        if (candidate == incoming) {
            continue;
        }
        if (can_use_as_null(candidate)) {
            return candidate;
        }
    }
}

bool ArrayIntNull::can_use_as_null(int64_t candidate)
{
    return find_first(candidate) == npos;
}

void ArrayIntNull::replace_nulls_with(int64_t new_null)
{
    // FIXME: Optimize!
    int64_t old_null = Array::get(0);
    Array::set(0, new_null);
    for (size_t i = 0; i < size(); ++i) {
        if (Array::get(i+1) == old_null) {
            Array::set(i+1, new_null);
        }
    }
}


void ArrayIntNull::ensure_not_null(int64_t value)
{
    if (m_width == 64) {
        if (value == null_value()) {
            int_fast64_t new_null = choose_random_null(value);
            replace_nulls_with(new_null);
        }
    }
    else {
        if (value >= m_ubound) {
            size_t new_width = bit_width(value);
            int64_t new_upper_bound = Array::ubound_for_width(new_width);

            // We're using upper bound as magic NULL value, so we have to check
            // explicitly that the incoming value doesn't happen to be the new
            // NULL value. If it is, we upgrade one step further.
            if (new_width < 64 && value == new_upper_bound) {
                new_width = (new_width == 0 ? 1 : new_width * 2);
                new_upper_bound = Array::ubound_for_width(new_width);
            }

            int64_t new_null;
            if (new_width == 64) {
                // Width will be upgraded to 64, so we need to pick a random NULL.
                new_null = choose_random_null(value);
            }
            else {
                new_null = new_upper_bound;
            }

            replace_nulls_with(new_null); // Expands array
        }
    }
}

void ArrayIntNull::find_all(Column* result, int64_t value, std::size_t col_offset, std::size_t begin, std::size_t end) const
{
    ++begin;
    if (end != npos) {
        ++end;
    }
    Array::find_all(result, value, col_offset, begin, end);
    result->adjust(-1);
}
