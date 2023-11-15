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

#include <string>
#include <string_view>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <optional>

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

class Tokenizer {
public:
    virtual ~Tokenizer();

    virtual Tokenizer& reset(std::string_view text);
    virtual bool next() = 0;

    std::string_view get_token()
    {
        return {m_buffer, m_size};
    }
    std::set<std::string> get_all_tokens();
    std::pair<std::set<std::string>, std::set<std::string>> get_search_tokens();
    TokenInfoMap get_token_info();

    static std::unique_ptr<Tokenizer> get_instance();

protected:
    std::string_view m_text;
    const char* m_start_pos = nullptr;
    const char* m_cur_pos = nullptr;
    const char* m_end_pos = nullptr;
    size_t m_size = 0;
    unsigned m_start = 0;
    unsigned m_end = 0;

    // Words longer than 64 chars will be truncated
    static constexpr int s_buffer_size = 64;
    char m_buffer[s_buffer_size];

    TokenRange get_range()
    {
        return {m_start, m_end};
    }
};

} // namespace realm

#endif /* REALM_TOKENIZER_HPP */
