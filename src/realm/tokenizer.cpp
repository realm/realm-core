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

void Tokenizer::reset(const std::string_view text)
{
    m_text = text;
    m_start_pos = m_text.data();
    m_cur_pos = m_start_pos;
    m_end_pos = m_cur_pos + m_text.size();
}

std::set<std::string> Tokenizer::get_all_tokens()
{
    std::set<std::string> tokens;
    while (next()) {
        tokens.emplace(get_token());
    }
    return tokens;
}

TokenInfo Tokenizer::get_token_info()
{
    TokenInfo token_info;
    while (next()) {
        auto it = token_info.find(std::string(get_token()));
        if (it == token_info.end()) {
            token_info.emplace(std::string(get_token()), TokenPositions{get_position()});
        }
        else {
            it->second.emplace_back(get_position());
        }
    }
    return token_info;
}

class DefaultTokenizer : public Tokenizer {
public:
    bool next() override;
};

static const uint8_t utf8_map[32] = {0x61, 0x61, 0x61, 0x61, 0x61, 0xe5, 0xe6, 0x63, 0x65, 0x65, 0x65,
                                     0x65, 0x69, 0x69, 0x69, 0x69, 0xf0, 0x6e, 0x6f, 0x6f, 0x6f, 0x6f,
                                     0x6f, 0x0,  0xf8, 0x75, 0x75, 0x75, 0x75, 0x79, 0xfe, 0x0};

bool DefaultTokenizer::next()
{
    char* bufp = m_buffer;
    char* end_buffer = &m_buffer[sizeof(m_buffer)];
    bool first_alnum_found = false;
    const char* token_end;

    typedef std::char_traits<char> traits;
    while (m_cur_pos < m_end_pos) {
        token_end = m_cur_pos;
        auto c = *m_cur_pos;
        bool alnum = false;
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')) {
            // c is a lowercase ASCII character. Can be added directly.
            alnum = true;
            if (bufp < end_buffer)
                *bufp++ = c;
        }
        else if ((c >= 'A' && c <= 'Z')) {
            // c is an uppercase ASCII character. Can be added after adding 0x20.
            alnum = true;
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
                if ((u >= 0xC0) && (u < 0x100)) {
                    // u is a letter from Latin-1 Supplement block - map to output
                    alnum = true;
                    if (auto o = utf8_map[u & 0x1f]) {
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
        if (alnum && !first_alnum_found) {
            m_start = m_cur_pos - m_start_pos;
            first_alnum_found = true;
        }
        m_cur_pos++;
        // If !alnum and we have found something, we can return
        // character is interpreted as a separator and we can store the
        // word collected so far - if anything collected
        if (!alnum && first_alnum_found) {
            break;
        }
    }
    if (first_alnum_found) {
        m_end = token_end - m_start_pos;
        m_size = bufp - m_buffer;
    }

    return first_alnum_found;
}

std::unique_ptr<Tokenizer> Tokenizer::get_instance()
{
    return std::make_unique<DefaultTokenizer>();
}

} // namespace realm

#ifdef TOKENIZER_UNITTEST

#include <cassert>
#include <iostream>

int main()
{
    auto tok = realm::Tokenizer::get_instance();
    tok->reset("to be or not to be");
    auto tokens = tok->get_all_tokens();
    assert(tokens.size() == 4);
    tok->reset("To be or not to be");
    auto info = tok->get_token_info();
    assert(info.size() == 4);
    assert(info["to"].size() == 2);
    assert(info["to"][0].first == 0);
    assert(info["to"][0].second == 2);
    assert(info["to"][1].first == 13);
    assert(info["to"][1].second == 15);
    tok->reset("Jeg gik mig over sø og land");
    info = tok->get_token_info();
    assert(info.size() == 7);
    assert(info["sø"][0].first == 17);
    assert(info["sø"][0].second == 20);
}
#endif
