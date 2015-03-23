#include <algorithm>

#include <tightdb/util/assert.hpp>

#include "wildcard.hpp"

using namespace std;


namespace tightdb {
namespace test_util {


wildcard_pattern::wildcard_pattern(const string& text):
    m_text(text)
{
    size_t pos = m_text.find('*');
    if (pos == string::npos) {
        m_cards.push_back(card(0, m_text.size()));
        return;
    }
    m_cards.push_back(card(0, pos));
    ++pos;
    for (;;) {
        size_t pos_2 = m_text.find('*', pos);
        if (pos_2 == string::npos)
            break;
        if (pos_2 != pos)
            m_cards.push_back(card(pos, pos_2));
        pos = pos_2 + 1;
    }
    m_cards.push_back(card(pos, m_text.size()));
}

bool wildcard_pattern::match(const char* begin, const char* end) const REALM_NOEXCEPT
{
    const char* begin_2 = begin;
    const char* end_2   = end;

    size_t num_cards = m_cards.size();
    REALM_ASSERT(num_cards >= 1);

    typedef string::const_iterator str_iter;

    // Check anchored prefix card
    {
        const card& card = m_cards.front();
        if (size_t(end_2 - begin_2) < card.m_size)
            return false;
        str_iter card_begin = m_text.begin() + card.m_offset;
        if (!equal(begin_2, begin_2 + card.m_size, card_begin))
            return false;
        begin_2 += card.m_size;
    }

    if (num_cards == 1)
        return begin_2 == end_2;

    // Check anchored suffix card
    {
        const card& card = m_cards.back();
        if (size_t(end_2 - begin_2) < card.m_size)
            return false;
        str_iter card_begin = m_text.begin() + card.m_offset;
        if (!equal(end_2 - card.m_size, end_2, card_begin))
            return false;
        end_2 -= card.m_size;
    }

    // Check unanchored infix cards
    for (size_t i = 1; i != num_cards-1; ++i) {
        const card& card = m_cards[i];
        str_iter card_begin = m_text.begin() + card.m_offset;
        str_iter card_end   = card_begin + card.m_size;
        begin_2 = search(begin_2, end_2, card_begin, card_end);
        if (begin_2 == end_2)
            return false;
        begin_2 += card.m_size;
    }

    return true;
}


} // namespace test_util
} // namespace tightdb
