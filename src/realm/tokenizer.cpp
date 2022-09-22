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
#include <realm/unicode.hpp>

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

/*
std::set<std::string> tokenize(const StringData text)
{
    std::set<std::string> words;

    const char* str = text.data();
    const size_t len = text.size();
    size_t start = 0;

    for (size_t i = 0; i < len; ++i) {
        signed char c = static_cast<signed char>(str[i]); // char may not be signed by default

        // Words are alnum + unicode chars above 128 (special unicode whitespace
        // chars are not used as word separators)
        bool is_alnum = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                        c < 0; // sign bit is set = unicode above 128

        if (is_alnum)
            continue;

        if (i > start) {
            StringData w(str + start, i - start);

            // All words are converted to lowercase (not very unicode aware)
            // This is also where we could do stemming.
            if (auto opt = case_map(w, false)) {
                words.emplace(*opt);
            }
        }
        start = i + 1;
    }
    if (start < len) {
        StringData w(str + start, len - start);
        if (auto opt = case_map(w, false)) {
            words.emplace(*opt);
        }
    }

    return words;
}
*/

bool DefaultTokenizer::next()
{
    const char* str = nullptr;
    while (m_cur_pos < m_end_pos) {
        signed char c = static_cast<signed char>(*m_cur_pos); // char may not be signed by default

        // Words are alnum + unicode chars above 128 (special unicode whitespace
        // chars are not used as word separators)
        bool is_alnum = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                        c < 0; // sign bit is set = unicode above 128

        if (is_alnum) {
            if (!str)
                str = m_cur_pos;
        }
        else {
            if (str) {
                break;
            }
        }
        ++m_cur_pos;
    }
    if (str) {
        StringData w(str, m_cur_pos - str);
        m_buffer = case_map(w, false);
        return bool(m_buffer);
    }
    return false;
}

std::unique_ptr<Tokenizer> Tokenizer::get_instance()
{
    return std::make_unique<DefaultTokenizer>();
}

} // namespace realm
