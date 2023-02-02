/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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
#ifndef FUZZ_LOGGER_HPP
#define FUZZ_LOGGER_HPP
#include <fstream>
#include <iostream>

class FuzzLog {
public:
    FuzzLog() = default;

    template <typename T>
    FuzzLog& operator<<(const T& v)
    {
        if (m_active) {
            m_out << v;
            m_out.flush();
        }
        return *this;
    }

    void enable_logging(const std::string& path)
    {
        m_out.open(path, std::ios::out);
        m_active = true;
    }

private:
    std::fstream m_out;
    bool m_active{false};
};

#endif