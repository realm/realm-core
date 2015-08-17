#include <algorithm>

#include "verified_integer.hpp"

using namespace realm;
using namespace realm::test_util;


void VerifiedInteger::verify_neighbours(size_t index)
{
    if (v.size() > index)
        REALM_ASSERT(v[index] == u.get(index));

    if (index > 0)
        REALM_ASSERT(v[index - 1] == u.get(index - 1));

    if (v.size() > index + 1)
        REALM_ASSERT(v[index + 1] == u.get(index + 1));
}

void VerifiedInteger::add(int64_t value)
{
    v.push_back(value);
    u.add(value);
    REALM_ASSERT(v.size() == u.size());
    verify_neighbours(v.size());
    REALM_ASSERT(occasional_verify());
}

void VerifiedInteger::insert(size_t index, int64_t value)
{
    v.insert(v.begin() + index, value);
    u.insert(index, value);
    REALM_ASSERT(v.size() == u.size());
    verify_neighbours(index);
    REALM_ASSERT(occasional_verify());
}

int64_t VerifiedInteger::get(size_t index)
{
    REALM_ASSERT(v[index] == u.get(index));
    return v[index];
}

int64_t VerifiedInteger::sum(size_t start, size_t end)
{
    int64_t sum = 0;

    if (start == end)
        return 0;

    if (end == size_t(-1))
        end = v.size();

    for (size_t t = start; t < end; ++t)
        sum += v[t];

    REALM_ASSERT(sum == u.sum(start, end));
    return sum;
}

int64_t VerifiedInteger::maximum(size_t start, size_t end)
{
    if (end == size_t(-1))
        end = v.size();

    if (end == start)
        return 0;

    int64_t max = v[start];

    for (size_t t = start + 1; t < end; ++t)
        if (v[t] > max)
            max = v[t];

    REALM_ASSERT(max == u.maximum(start, end));
    return max;
}

int64_t VerifiedInteger::minimum(size_t start, size_t end)
{
    if (end == size_t(-1))
        end = v.size();

    if (end == start)
        return 0;

    int64_t min = v[start];

    for (size_t t = start + 1; t < end; ++t)
        if (v[t] < min)
            min = v[t];

    REALM_ASSERT(min == u.minimum(start, end));
    return min;
}

void VerifiedInteger::set(size_t index, int64_t value)
{
    v[index] = value;
    u.set(index, value);
    verify_neighbours(index);
    REALM_ASSERT(occasional_verify());
}

void VerifiedInteger::erase(size_t index)
{
    v.erase(v.begin() + index);
    u.erase(index);
    REALM_ASSERT(v.size() == u.size());
    verify_neighbours(index);
    REALM_ASSERT(occasional_verify());
}

void VerifiedInteger::clear()
{
    v.clear();
    u.clear();
    REALM_ASSERT(v.size() == u.size());
    REALM_ASSERT(occasional_verify());
}

size_t VerifiedInteger::find_first(int64_t value)
{
    std::vector<int64_t>::iterator it = std::find(v.begin(), v.end(), value);
    size_t index = std::distance(v.begin(), it);
    size_t index2 = u.find_first(value);
    REALM_ASSERT(index == index2 || (it == v.end() && index2 == size_t(-1)));
    static_cast<void>(index2);
    return index;
}

size_t VerifiedInteger::size()
{
    REALM_ASSERT(v.size() == u.size());
    return v.size();
}

// todo/fixme, end ignored
void VerifiedInteger::find_all(IntegerColumn &c, int64_t value, size_t start, size_t end)
{
    std::vector<int64_t>::iterator ita = v.begin() + start;
    std::vector<int64_t>::iterator itb = end == size_t(-1) ? v.end() : v.begin() + (end == size_t(-1) ? v.size() : end);;
    std::vector<size_t> result;
    while (ita != itb) {
        ita = std::find(ita, itb, value);
        size_t index = std::distance(v.begin(), ita);
        if (index < v.size()) {
            result.push_back(index);
            ita++;
        }
    }

    c.clear();

    u.find_all(c, value);
    if (c.size() != result.size())
        REALM_ASSERT(false);
    for (size_t t = 0; t < result.size(); ++t) {
        if (result[t] != size_t(c.get(t)))
            REALM_ASSERT(false);
    }

    return;
}

bool VerifiedInteger::verify()
{
    REALM_ASSERT(u.size() == v.size());
    if (u.size() != v.size())
        return false;

    for (size_t t = 0; t < v.size(); ++t) {
        REALM_ASSERT(v[t] == u.get(t));
        if (v[t] != u.get(t))
            return false;
    }
    return true;
}

// makes it run amortized the same time complexity as original, even though the row count grows
bool VerifiedInteger::occasional_verify()
{
    if (m_random.draw_int_max(v.size() / 10) == 0)
        return verify();
    return true;
}

VerifiedInteger::~VerifiedInteger()
{
    u.destroy();
}
