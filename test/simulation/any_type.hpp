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

#ifndef REALM_ANY_TYPE_HPP
#define REALM_ANY_TYPE_HPP

#include <string>
#include <vector>

#include <realm/binary_data.hpp>
#include <realm/data_type.hpp>
#include <realm/string_data.hpp>
#include <realm/timestamp.hpp>

#include "stable_key.hpp"

namespace realm {
namespace simulation {

struct StableLink {
    StableKey table;
    StableKey column;
    StableKey row;
};

class AnyType {
public:
    AnyType();
    ~AnyType() noexcept;
    AnyType(bool) noexcept;
    AnyType(int64_t) noexcept;
    AnyType(float) noexcept;
    AnyType(double) noexcept;
    AnyType(StringData) noexcept;
    AnyType(BinaryData) noexcept;
    AnyType(Timestamp) noexcept;
    AnyType(StableLink) noexcept;
    DataType get_type() const noexcept
    {
        return m_type;
    }

    int64_t get_int() const noexcept;
    bool get_bool() const noexcept;
    float get_float() const noexcept;
    double get_double() const noexcept;
    StringData get_string() const noexcept;
    BinaryData get_binary() const noexcept;
    Timestamp get_timestamp() const noexcept;
    StableLink get_link() const noexcept;

    void set_int(int64_t) noexcept;
    void set_bool(bool) noexcept;
    void set_float(float) noexcept;
    void set_double(double) noexcept;
    void set_string(StringData) noexcept;
    void set_binary(BinaryData) noexcept;
    void set_binary(const char* data, size_t size) noexcept;
    void set_timestamp(Timestamp) noexcept;
    void set_link(StableLink) noexcept;

private:
    DataType m_type;
    union {
        int64_t m_int;
        bool m_bool;
        float m_float;
        double m_double;
        Timestamp m_timestamp;
    };
    StableLink m_link;
    std::string m_data;
    std::vector<StableKey> m_linklist;
};

} // namespace simulation
} // namespace realm

#endif // REALM_ANY_TYPE_HPP
