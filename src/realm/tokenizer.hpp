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

#ifndef REALM_TOKENIZER_HPP
#define REALM_TOKENIZER_HPP

#include <string_view>
#include <vector>
#include <map>
#include <set>
#include <memory>

namespace realm {

using TokenRange = std::pair<unsigned, unsigned>;
using TokenRanges = std::vector<TokenRange>;
using TokenPositions = std::vector<unsigned>;

struct TokenInfo {
    TokenInfo() = default;
    explicit TokenInfo(unsigned position, TokenRange pos)
    {
        positions.emplace_back(position);
        ranges.emplace_back(pos);
    }
    double weight = 1.;
    double frequency = 1.;
    TokenPositions positions;
    TokenRanges ranges;
};

using TokenInfoMap = std::map<std::string, TokenInfo>;
using TokenRangesMap = std::map<std::string, TokenRanges>;

class Tokenizer {
public:
    virtual ~Tokenizer();

    virtual void reset(std::string_view text);
    virtual bool next() = 0;
    virtual bool is_stop_word(std::string_view token) = 0;

    std::string_view get_token()
    {
        return {m_buffer, m_size};
    }

    TokenRange get_range()
    {
        return {m_start, m_end};
    }

    std::set<std::string> get_all_tokens();
    TokenInfoMap get_token_info();

    double get_rank(std::string_view tokens, std::string_view text);
    TokenRangesMap get_ranges(std::string_view tokens, std::string_view text);

    static std::unique_ptr<Tokenizer> get_instance();

protected:
    std::string_view m_text;
    char m_buffer[32];
    size_t m_size = 0;
    const char* m_start_pos = nullptr;
    const char* m_cur_pos = nullptr;
    const char* m_end_pos = nullptr;
    unsigned m_start = 0;
    unsigned m_end = 0;
};

} // namespace realm

#endif /* REALM_TOKENIZER_HPP */
