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

#include <algorithm>

#include <realm/util/assert.hpp>

#include "wildcard.hpp"


namespace realm {
namespace test_util {


wildcard_pattern::wildcard_pattern(const std::string& text)
    : m_text(text)
{
    size_t pos = m_text.find('*');
    if (pos == std::string::npos) {
        m_cards.push_back(card(0, m_text.size()));
        return;
    }
    m_cards.push_back(card(0, pos));
    ++pos;
    for (;;) {
        size_t pos_2 = m_text.find('*', pos);
        if (pos_2 == std::string::npos)
            break;
        if (pos_2 != pos)
            m_cards.push_back(card(pos, pos_2));
        pos = pos_2 + 1;
    }
    m_cards.push_back(card(pos, m_text.size()));
}

bool wildcard_pattern::match(const char* begin, const char* end) const noexcept
{
    const char* begin_2 = begin;
    const char* end_2 = end;

    size_t num_cards = m_cards.size();
    REALM_ASSERT(num_cards >= 1);

    typedef std::string::const_iterator str_iter;

    // Check anchored prefix card
    {
        const card& front_card = m_cards.front();
        if (size_t(end_2 - begin_2) < front_card.m_size)
            return false;
        str_iter card_begin = m_text.begin() + front_card.m_offset;
        if (!equal(begin_2, begin_2 + front_card.m_size, card_begin))
            return false;
        begin_2 += front_card.m_size;
    }

    if (num_cards == 1)
        return begin_2 == end_2;

    // Check anchored suffix card
    {
        const card& back_card = m_cards.back();
        if (size_t(end_2 - begin_2) < back_card.m_size)
            return false;
        str_iter card_begin = m_text.begin() + back_card.m_offset;
        if (!equal(end_2 - back_card.m_size, end_2, card_begin))
            return false;
        end_2 -= back_card.m_size;
    }

    // Check unanchored infix cards
    for (size_t i = 1; i != num_cards - 1; ++i) {
        const card& card_i = m_cards[i];
        str_iter card_begin = m_text.begin() + card_i.m_offset;
        str_iter card_end = card_begin + card_i.m_size;
        begin_2 = search(begin_2, end_2, card_begin, card_end);
        if (begin_2 == end_2)
            return false;
        begin_2 += card_i.m_size;
    }

    return true;
}


} // namespace test_util
} // namespace realm
