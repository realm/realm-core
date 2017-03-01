/*************************************************************************
 *
 * Copyright 2017 Realm Inc.
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

#include "string_data.hpp"

using namespace realm;

bool StringData::matchlike(const StringData& text, const StringData& pattern) noexcept
{
    std::vector<size_t> textpos;
    std::vector<size_t> patternpos;
    size_t p1 = 0; // position in text (haystack)
    size_t p2 = 0; // position in pattern (needle)

    while (true) {
        if (p1 == text.size()) {
            if (p2 == pattern.size())
                return true;
            if (p2 == pattern.size() - 1 && pattern[p2] == '*')
                return true;
            goto no_match;
        }
        if (p2 == pattern.size())
            goto no_match;

        if (pattern[p2] == '*') {
            textpos.push_back(p1);
            patternpos.push_back(++p2);
            continue;
        }
        if (pattern[p2] == '?') {
            // utf-8 encoded characters may take up multiple bytes
            if ((text[p1] & 0x80) == 0) {
                ++p1;
                ++p2;
                continue;
            }
            else {
                size_t p = 1;
                while (p1 + p != text.size() && (text[p1 + p] & 0xc0) == 0x80)
                    ++p;
                p1 += p;
                ++p2;
                continue;
            }
        }

        if (pattern[p2] == text[p1]) {
            ++p1;
            ++p2;
            continue;
        }

    no_match:
        if (textpos.empty())
            return false;
        else {
            if (p1 == text.size()) {
                textpos.pop_back();
                patternpos.pop_back();

                if (textpos.empty())
                    return false;

                p1 = textpos.back();
            }
            else {
                p1 = textpos.back();
                textpos.back() = ++p1;
            }
            p2 = patternpos.back();
        }
    }
}
