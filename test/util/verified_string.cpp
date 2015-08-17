#include <algorithm>

#include "verified_string.hpp"

using namespace realm;
using namespace realm::test_util;


VerifiedString::VerifiedString():
    u(Allocator::get_default(), StringColumn::create(Allocator::get_default()))
{
}


VerifiedString::~VerifiedString()
{
    u.destroy();
}

void VerifiedString::verify_neighbours(size_t index)
{
    if (v.size() > index)
        REALM_ASSERT(v[index] == u.get(index));

    if (index > 0)
        REALM_ASSERT(v[index - 1] == u.get(index - 1));

    if (v.size() > index + 1)
        REALM_ASSERT(v[index + 1] == u.get(index + 1));
}

void VerifiedString::add(StringData value)
{
    v.push_back(value);
    u.add(value);
    REALM_ASSERT(v.size() == u.size());
    verify_neighbours(v.size());
    REALM_ASSERT(conditional_verify());
}


void VerifiedString::insert(size_t index, StringData value)
{
    v.insert(v.begin() + index, value);
    u.insert(index, value);
    REALM_ASSERT(v.size() == u.size());
    verify_neighbours(index);
    REALM_ASSERT(conditional_verify());
}


StringData VerifiedString::get(size_t index)
{
    REALM_ASSERT(v[index] == u.get(index));
    return v[index];
}

void VerifiedString::set(size_t index, StringData value)
{
    v[index] = value;
    u.set(index, value);
    verify_neighbours(index);
    REALM_ASSERT(conditional_verify());
}

void VerifiedString::erase(size_t index)
{
    v.erase(v.begin() + index);
    u.erase(index);
    REALM_ASSERT(v.size() == u.size());
    verify_neighbours(index);
    REALM_ASSERT(conditional_verify());
}

void VerifiedString::clear()
{
    v.clear();
    u.clear();
    REALM_ASSERT(v.size() == u.size());
    REALM_ASSERT(conditional_verify());
}

size_t VerifiedString::find_first(StringData value)
{
    std::vector<std::string>::iterator it = std::find(v.begin(), v.end(), value);
    size_t index = std::distance(v.begin(), it);
    size_t index2 = u.find_first(value);
    static_cast<void>(index2);
    REALM_ASSERT(index == index2 || (it == v.end() && index2 == size_t(-1)));
    return index;
}

size_t VerifiedString::size()
{
    REALM_ASSERT(v.size() == u.size());
    return v.size();
}

// todo/fixme, end ignored
void VerifiedString::find_all(IntegerColumn& c, StringData value, size_t start, size_t end)
{
    std::vector<std::string>::iterator ita = v.begin() + start;
    std::vector<std::string>::iterator itb = v.begin() + (end == size_t(-1) ? v.size() : end);
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
    size_t cs = c.size();
    if (cs != result.size())
        REALM_ASSERT(false);
    for (size_t t = 0; t < result.size(); ++t) {
        if (result[t] != size_t(c.get(t)))
            REALM_ASSERT(false);
    }

    return;
}

bool VerifiedString::verify()
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
bool VerifiedString::conditional_verify()
{
    if ((uint64_t(rand()) * uint64_t(rand()))  % (v.size() / 10 + 1) == 0) {
        return verify();
    }
    else {
        return true;
    }
}
