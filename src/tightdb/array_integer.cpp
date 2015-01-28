#include "tightdb/array_integer.hpp"

#include <vector>

using namespace tightdb;

// Sort array.
void ArrayInteger::sort()
{
    TIGHTDB_TEMPEX(sort, m_width, ());
}

// Sort array
template <size_t w>
void ArrayInteger::sort()
{
    if (m_size < 2)
        return;

    size_t lo = 0;
    size_t hi = m_size - 1;
    std::vector<size_t> count;
    int64_t min;
    int64_t max;
    bool b = false;

    // in avg case QuickSort is O(n*log(n)) and CountSort O(n + range), and memory usage is sizeof(size_t)*range for CountSort.
    // Se we chose range < m_size as treshold for deciding which to use
    if (m_width <= 8) {
        max = m_ubound;
        min = m_lbound;
        b = true;
    }
    else {
        // If range isn't suited for CountSort, it's *probably* discovered very early, within first few values,
        // in most practical cases, and won't add much wasted work. Max wasted work is O(n) which isn't much
        // compared to QuickSort.
        b = minmax<w>(lo, hi + 1, m_size, &min, &max);
    }

    if (b) {
        for (int64_t t = 0; t < max - min + 1; t++)
            count.push_back(0);

        // Count occurences of each value
        for (size_t t = lo; t <= hi; t++) {
            size_t i = to_size_t(get_data<w>(t) - min); // FIXME: The value of (get<w>(t) - min) cannot necessarily be stored in size_t.
            count[i]++;
        }

        // Overwrite original array with sorted values
        size_t dst = 0;
        for (int64_t i = 0; i < max - min + 1; i++) {
            size_t c = count[unsigned(i)];
            for (size_t j = 0; j < c; j++) {
                set_data<w>(dst, i + min);
                dst++;
            }
        }
    }
    else {
        QuickSort(lo, hi);
    }

    return;
}

// Find max and min value, but break search if difference exceeds 'maxdiff' (in which case *min and *max is set to 0)
// Useful for counting-sort functions
template <size_t w>
bool ArrayInteger::minmax(size_t from, size_t to, uint64_t maxdiff, int64_t *min, int64_t *max) const
{
    int64_t min2;
    int64_t max2;
    size_t t;

    max2 = get_data<w>(from);
    min2 = max2;

    for (t = from + 1; t < to; t++) {
        int64_t v = get_data<w>(t);
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
        v.push_back(get_data(t));
    return v;
}
