#ifndef VER_INT_H
#define VER_INT_H

#include <vector>
#include <string>
#include <algorithm>
#ifdef _MSC_VER
    #include "win32\stdint.h"
#endif
#include <stdio.h>
#include "verified_integer.hpp"

using namespace std;
using namespace tightdb;

void VerifiedInteger::VerifyNeighbours(size_t ndx)
{
    if(v.size() > ndx)
        assert(v[ndx] == u.Get(ndx));

    if(ndx > 0)
        assert(v[ndx - 1] == u.Get(ndx - 1));

    if(v.size() > ndx + 1)
        assert(v[ndx + 1] == u.Get(ndx + 1));
}

void VerifiedInteger::add(int64_t value)
{
    v.push_back(value);
    u.add(value);
    assert(v.size() == u.Size());
    VerifyNeighbours(v.size());
    assert(ConditionalVerify());
}

void VerifiedInteger::Insert(size_t ndx, int64_t value)
{
    v.insert(v.begin() + ndx, value);
    u.Insert(ndx, value);
    assert(v.size() == u.Size());
    VerifyNeighbours(ndx);
    assert(ConditionalVerify());
}

int64_t VerifiedInteger::Get(size_t ndx)
{
    assert(v[ndx] == u.Get(ndx));
    return v[ndx];
}

int64_t VerifiedInteger::Sum(size_t start, size_t end)
{
    int64_t sum = 0;

    if(start == end)
        return 0;

    if(end == size_t(-1))
        end = v.size();

    for(size_t t = start; t < end; ++t)
        sum += v[t];

    assert(sum == u.sum(start, end));
    return sum;
}

int64_t VerifiedInteger::maximum(size_t start, size_t end)
{
    if(end == size_t(-1))
        end = v.size();

    if(end == start)
        return 0;

    int64_t max = v[start];

    for(size_t t = start + 1; t < end; ++t)
        if(v[t] > max)
            max = v[t];

    assert(max == u.maximum(start, end));
    return max;
}

int64_t VerifiedInteger::minimum(size_t start, size_t end)
{
    if(end == size_t(-1))
        end = v.size();

    if(end == start)
        return 0;

    int64_t min = v[start];

    for(size_t t = start + 1; t < end; ++t)
        if(v[t] < min)
            min = v[t];

    assert(min == u.minimum(start, end));
    return min;
}

void VerifiedInteger::Set(size_t ndx, int64_t value)
{
    v[ndx] = value;
    u.Set(ndx, value);
    VerifyNeighbours(ndx);
    assert(ConditionalVerify());
}

void VerifiedInteger::Delete(size_t ndx)
{
    v.erase(v.begin() + ndx);
    u.Delete(ndx);
    assert(v.size() == u.Size());
    VerifyNeighbours(ndx);
    assert(ConditionalVerify());
}

void VerifiedInteger::Clear()
{
    v.clear();
    u.Clear();
    assert(v.size() == u.Size());
    assert(ConditionalVerify());
}

size_t VerifiedInteger::find_first(int64_t value)
{
    std::vector<int64_t>::iterator it = std::find(v.begin(), v.end(), value);
    size_t ndx = std::distance(v.begin(), it);
    size_t index2 = u.find_first(value);
    assert(ndx == index2 || (it == v.end() && index2 == size_t(-1)));
    (void)index2;
    return ndx;
}

size_t VerifiedInteger::Size(void)
{
    assert(v.size() == u.Size());
    return v.size();
}

// todo/fixme, end ignored
void VerifiedInteger::find_all(Array &c, int64_t value, size_t start, size_t end)
{
    std::vector<int64_t>::iterator ita = v.begin() + start;
    std::vector<int64_t>::iterator itb = end == size_t(-1) ? v.end() : v.begin() + (end == size_t(-1) ? v.size() : end);;
    std::vector<size_t> result;
    while(ita != itb) {
        ita = std::find(ita, itb, value);
        size_t ndx = std::distance(v.begin(), ita);
        if(ndx < v.size()) {
            result.push_back(ndx);
            ita++;
        }
    }

    c.Clear();

    u.find_all(c, value);
    if (c.Size() != result.size())
        assert(false);
    for(size_t t = 0; t < result.size(); ++t) {
        if (result[t] != (size_t)c.Get(t))
            assert(false);
    }

    return;
}

bool VerifiedInteger::Verify(void)
{
    assert(u.Size() == v.size());
    if (u.Size() != v.size())
        return false;

    for(size_t t = 0; t < v.size(); ++t) {
        assert(v[t] == u.Get(t));
        if (v[t] != u.Get(t))
            return false;
    }
    return true;
}

// makes it run amortized the same time complexity as original, even though the row count grows
bool VerifiedInteger::ConditionalVerify(void)
{
    if(((uint64_t)rand() * (uint64_t)rand())  % (v.size() / 10 + 1) == 0) {
        return Verify();
    }
    else {
        return true;
    }
}

void VerifiedInteger::Destroy(void)
{
    u.Destroy();
}

#endif
