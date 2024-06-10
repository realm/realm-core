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
#include <realm/exceptions.hpp>

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
std::pair<std::set<std::string>, std::set<std::string>> Tokenizer::get_search_tokens()
{
    std::vector<std::string_view> incl;
    std::vector<std::string_view> excl;

    const char* begin = nullptr;
    const char* end = nullptr;
    auto add_token = [&] {
        if (begin) {
            if (*begin == '-') {
                begin++;
                excl.emplace_back(begin, end - begin);
            }
            else {
                incl.emplace_back(begin, end - begin);
            }
            begin = nullptr;
        }
    };
    for (; m_cur_pos != m_end_pos; m_cur_pos++) {
        if (isspace(static_cast<unsigned char>(*m_cur_pos))) {
            add_token();
        }
        else {
            if (begin) {
                end++;
            }
            else {
                begin = m_cur_pos;
                end = m_cur_pos + 1;
            }
        }
    }
    add_token();

    std::set<std::string> includes;
    std::set<std::string> excludes;

    for (auto& tok : incl) {
        reset(tok);
        next();
        if (tok.back() == '*') {
            std::string str(get_token());
            str += '*';
            includes.insert(str);
        }
        else {
            includes.emplace(get_token());
        }
        if (next()) {
            throw InvalidArgument("Non alphanumeric characters not allowed inside search word");
        }
    }
    for (auto& tok : excl) {
        reset(tok);
        next();
        std::string t(get_token());
        if (includes.count(t)) {
            throw InvalidArgument("You can't include and exclude the same token");
        }
        excludes.emplace(t);
        if (next()) {
            throw InvalidArgument("Non alphanumeric characters not allowed inside search word");
        }
    }

    return {includes, excludes};
}

TokenInfoMap Tokenizer::get_token_info()
{
    TokenInfoMap info;
    unsigned num_tokens = 0;
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
            if (state == searching) {
                m_start = unsigned(m_cur_pos - m_start_pos);
            }
            state = building;
        }
        else {
            if (state == building) {
                m_end = unsigned(m_cur_pos - m_start_pos);
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

#ifdef TOKENIZER_UNITTEST

// compile: g++ -DTOKENIZER_UNITTEST=1 -I.. --std=c++17 -g -o test_tokenizer tokenizer.cpp
// call like this:
// ./tokenizer_test ~/some/path/to/file -d
//  echo 'fasdfsdfsd fas df' | ./tokenizer_test -d

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>

static std::ostream& operator<<(std::ostream& out, const realm::TokenInfo& info)
{
    out << "\n\t\tweight: " << info.weight << "\n\t\tfrequency: " << info.frequency << "\n\t\tpositions: [";
    for (auto p : info.positions)
        out << p << ", ";
    out << "]\n\t\tranges: [";
    for (auto&& [s, e] : info.ranges)
        out << "(" << s << ", " << e << "), ";
    out << "]";
    return out;
}

static std::ostream& operator<<(std::ostream& out, const realm::TokenInfoMap& infoMap)
{
    out << "TokenInfoMap(size: " << infoMap.size();
    for (auto&& [token, info] : infoMap)
        out << "\n\t" << token << " (" << info << ")";
    out << ")";
    return out;
}

using namespace std::chrono;

char buffer[256 * 256];

int main(int argc, const char* argv[])
{
    std::string_view arg1(argc > 1 ? argv[1] : "");
    std::string_view arg2(argc > 2 ? argv[2] : "");
    bool dump = arg2 == "-d" || arg1 == "-d";

    std::string_view text;
    if (!arg1.empty() && arg1 != "-d") {
        std::cout << "Reading from file [" << argv[1] << "]..." << std::endl;
        std::ifstream istr(argv[1]);
        istr.read(buffer, sizeof(buffer));
        text = std::string_view(buffer, istr.gcount());
    }
    else {
        std::cout << "Reading from stdin..." << std::endl;
        std::cin.read(buffer, sizeof(buffer));
        text = std::string_view(buffer, std::cin.gcount());
    }

    auto tok = realm::Tokenizer::get_instance();
    tok->reset(text);
    auto t1 = steady_clock::now();
    auto tokens = tok->get_all_tokens();
    auto t2 = steady_clock::now();
    tok->reset(text);
    auto info = tok->get_token_info();
    auto t3 = steady_clock::now();
    std::cout << "tokenize: " << duration_cast<microseconds>(t2 - t1).count() << " us" << std::endl;
    std::cout << "info: " << duration_cast<microseconds>(t3 - t2).count() << " us" << std::endl;

    if (dump)
        std::cout << info << std::endl;

    return 0;
}
#endif
