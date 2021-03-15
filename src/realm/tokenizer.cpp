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
#include <string>
#include <atomic>

namespace realm {

const char* stop_words[] = {
    "ad",    "af",    "alle",   "alt",   "anden", "at",    "blev",  "blive", "bliver", "da",   "de",    "dem",
    "den",   "denne", "der",    "deres", "det",   "dette", "dig",   "din",   "disse",  "dog",  "du",    "efter",
    "eller", "en",    "end",    "er",    "et",    "for",   "fra",   "ham",   "han",    "hans", "har",   "havde",
    "have",  "hende", "hendes", "her",   "hos",   "hun",   "hvad",  "hvis",  "hvor",   "i",    "ikke",  "ind",
    "jeg",   "jer",   "jo",     "kunne", "man",   "mange", "med",   "meget", "men",    "mig",  "min",   "mine",
    "mit",   "mod",   "ned",    "noget", "nogle", "nu",    "når",   "og",    "også",   "om",   "op",    "os",
    "over",  "på",    "selv",   "sig",   "sin",   "sine",  "sit",   "skal",  "skulle", "som",  "sådan", "thi",
    "til",   "ud",    "under",  "var",   "vi",    "vil",   "ville", "vor",   "være",   "været"};

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
        auto token = get_token();
        if (!is_stop_word(token))
            tokens.emplace(get_token());
    }
    return tokens;
}

TokenInfoMap Tokenizer::get_token_info()
{
    TokenInfoMap info;
    size_t num_tokens = 0;
    while (next()) {
        auto it = info.find(std::string(get_token()));
        if (it == info.end()) {
            info.emplace(std::string(get_token()), TokenInfo(num_tokens, get_range()));
        }
        else {
            TokenInfo& i = it->second;
            i.positions.emplace_back(num_tokens);
            i.ranges.emplace_back(get_range());
            i.weight *= 2;
            i.frequency += (1 / i.weight);
        }
        num_tokens++;
    }
    for (auto& it : info) {
        TokenInfo& i = it.second;
        double coeff = (0.5 * i.positions.size() / num_tokens) + 0.5;
        i.weight = i.frequency * coeff;
    }
    return info;
}

namespace {

template <class T>
double get_sub_proximity(unsigned pos, T begin, T end)
{
    std::vector<unsigned>& positions = **begin;
    auto next = begin + 1;
    double proximity = 0; // The closer, the higher proximity
    double sub_proximity = 0;
    for (auto i : positions) {
        if (i > pos) {
            if (next == end) {
                // End of line. this must be the best score we can get
                return 1. / (i - pos);
            }
            auto p = get_sub_proximity(i, next, end);
            if (p > sub_proximity) {
                sub_proximity = p;
                proximity = 1. / (i - pos);
            }
        }
    }
    return proximity + sub_proximity;
}

template <class T>
double get_position_proximity(T begin, T end)
{
    double proximity = 0; // The closer, the higher proximity
    std::vector<unsigned>& positions = **begin;
    auto next = begin + 1;
    // Iterate over the individual positions to see which of them get the best score;
    for (auto i : positions) {
        auto p = get_sub_proximity(i, next, end);
        if (p > proximity) {
            proximity = p;
        }
    }
    return proximity;
}

} // namespace

double Tokenizer::get_rank(std::string_view tokens, std::string_view text)
{
    reset(text);
    auto info = get_token_info();

    std::vector<std::vector<unsigned>*> positions;
    double weight = 0;
    reset(tokens);
    while (next()) {
        std::string tok(get_token());
        auto& i = info[tok];
        positions.push_back(&i.positions);
        weight += i.weight;
    }

    // Proximity only relevant if more than one token
    if (positions.size() > 1) {
        weight += get_position_proximity(positions.begin(), positions.end());
    }

    return weight;
}

TokenRangesMap Tokenizer::get_ranges(std::string_view tokens, std::string_view text)
{
    TokenRangesMap ret;
    reset(text);
    auto info = get_token_info();

    reset(tokens);
    while (next()) {
        std::string tok(get_token());
        auto& i = info[tok];
        ret.emplace(tok, i.ranges);
    }

    return ret;
}

class DefaultTokenizer : public Tokenizer {
public:
    DefaultTokenizer()
    {
        if (!s_stop_words_has_been_built.test_and_set()) {
            for (auto s : stop_words) {
                s_stop_words.insert(s);
            }
        }
    }
    bool next() override;
    bool is_stop_word(std::string_view token)
    {
        return s_stop_words.find(token) != s_stop_words.end();
    }

private:
    static std::set<std::string_view> s_stop_words;
    static std::atomic_flag s_stop_words_has_been_built;
};

std::atomic_flag DefaultTokenizer::s_stop_words_has_been_built = ATOMIC_FLAG_INIT;
std::set<std::string_view> DefaultTokenizer::s_stop_words;

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
                else if (u == 0xad) {
                    // Soft hyphen - just ignore
                    alnum = true;
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

// compile: g++ -DTOKENIZER_UNITTEST=1 -I.. --std=c++17 -g -o test_tokenizer tokenizer.cpp

#include <cassert>
#include <iostream>
#include <fstream>
#include <chrono>

using namespace std::chrono;

char buffer[256 * 256];

int main(int argc, const char* argv[])
{
    auto tok = realm::Tokenizer::get_instance();
    tok->reset("to be or not to be");
    auto tokens = tok->get_all_tokens();
    assert(tokens.size() == 4);
    tok->reset("To be or not to be");
    realm::TokenInfoMap info = tok->get_token_info();
    assert(info.size() == 4);
    realm::TokenInfo& i(info["to"]);
    assert(i.positions.size() == 2);
    assert(i.positions[0] == 0);
    assert(i.positions[1] == 4);
    assert(i.ranges.size() == 2);
    assert(i.ranges[0].first == 0);
    assert(i.ranges[0].second == 2);
    assert(i.ranges[1].first == 13);
    assert(i.ranges[1].second == 15);
    tok->reset("Jeg gik mig over sø og land");
    info = tok->get_token_info();
    assert(info.size() == 7);
    realm::TokenInfo& j(info["sø"]);
    assert(j.ranges[0].first == 17);
    assert(j.ranges[0].second == 20);
    tok->get_rank("gik mig over", "Du gik dig over, jeg gik mig over sø og land");
    if (argc > 1) {
        std::ifstream istr(argv[1]);
        istr.read(buffer, sizeof(buffer));
        std::string_view text(buffer, istr.gcount());

        tok->reset(text);
        auto t1 = steady_clock::now();
        auto tokens = tok->get_all_tokens();
        auto t2 = steady_clock::now();
        tok->reset(text);
        auto info = tok->get_token_info();
        auto t3 = steady_clock::now();
        std::cout << "tokenize: " << duration_cast<microseconds>(t2 - t1).count() << " us" << std::endl;
        std::cout << "info: " << duration_cast<microseconds>(t3 - t2).count() << " us" << std::endl;
    }
}
#endif
