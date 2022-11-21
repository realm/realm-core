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

#include <realm/tokenizer.hpp>

namespace realm {

Tokenizer::~Tokenizer() {}

Tokenizer& Tokenizer::reset(const std::string_view text)
{
    m_text = text;
    m_start_pos = m_text.data();
    m_cur_pos = m_start_pos;
    m_end_pos = m_cur_pos + m_text.size();

    return *this;
}

std::set<std::string> Tokenizer::get_all_tokens()
{
    std::set<std::string> tokens;
    while (next()) {
        tokens.emplace(get_token());
    }
    return tokens;
}

class DefaultTokenizer : public Tokenizer {
public:
    bool next() override;
};

// Mapping of Latin-1 characters into the corresponding lowercase character with diacritics removed
static const uint8_t utf8_map[64] = {
    0x61, 0x61, 0x61, 0x61, 0x61, 0xe5, 0xe6, 0x63, 0x65, 0x65, 0x65, 0x65, 0x69, 0x69, 0x69, 0x69,
    0xf0, 0x6e, 0x6f, 0x6f, 0x6f, 0x6f, 0x6f, 0x0,  0xf8, 0x75, 0x75, 0x75, 0x75, 0x79, 0xfe, 0xdf,
    0x61, 0x61, 0x61, 0x61, 0x61, 0xe5, 0xe6, 0x63, 0x65, 0x65, 0x65, 0x65, 0x69, 0x69, 0x69, 0x69,
    0xf0, 0x6e, 0x6f, 0x6f, 0x6f, 0x6f, 0x6f, 0x0,  0xf8, 0x75, 0x75, 0x75, 0x75, 0x79, 0xfe, 0xff,
};

bool DefaultTokenizer::next()
{
    char* bufp = m_buffer;
    char* end_buffer = m_buffer + s_buffer_size;
    enum { searching, building, finished } state = searching;

    using traits = std::char_traits<char>;
    while (m_cur_pos < m_end_pos && state != finished) {
        signed char c = static_cast<signed char>(*m_cur_pos); // char may not be signed by default
        bool is_alnum = false;
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')) {
            // c is a lowercase ASCII character. Can be added directly.
            is_alnum = true;
            if (bufp < end_buffer)
                *bufp++ = c;
        }
        else if ((c >= 'A' && c <= 'Z')) {
            // c is an uppercase ASCII character. Can be added after adding 0x20.
            is_alnum = true;
            if (bufp < end_buffer)
                *bufp++ = c + 0x20;
        }
        else if (traits::to_int_type(c) > 0x7f) {
            auto i = traits::to_int_type(c);
            if ((i & 0xE0) == 0xc0) {
                // 2 byte utf-8
                m_cur_pos++;
                // Construct unicode value
                auto u = ((i << 6) + (traits::to_int_type(*m_cur_pos) & 0x3F)) & 0x7FF;
                if ((u >= 0xC0) && (u < 0xff)) {
                    // u is a letter from Latin-1 Supplement block - map to output
                    is_alnum = true;
                    if (auto o = utf8_map[u & 0x3f]) {
                        if (o < 0x80) {
                            // ASCII
                            if (bufp < end_buffer)
                                *bufp++ = char(o);
                        }
                        else {
                            if (bufp < end_buffer - 1) {
                                *bufp++ = static_cast<char>((o >> 6) | 0xC0);
                                *bufp++ = static_cast<char>((o & 0x3f) | 0x80);
                            }
                        }
                    }
                }
            }
            else if ((i & 0xF0) == 0xE0) {
                // 3 byte utf-8
                m_cur_pos += 2;
            }
            else if ((i & 0xF8) == 0xF0) {
                // 4 byte utf-8
                m_cur_pos += 3;
            }
        }

        if (is_alnum) {
            state = building;
        }
        else {
            if (state == building) {
                state = finished;
            }
        }
        ++m_cur_pos;
    }
    m_size = bufp - m_buffer;
    return state != searching;
}

std::unique_ptr<Tokenizer> Tokenizer::get_instance()
{
    return std::make_unique<DefaultTokenizer>();
}

} // namespace realm
