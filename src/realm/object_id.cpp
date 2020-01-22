/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#include <realm/object_id.hpp>
#include <realm/util/assert.hpp>
#include <chrono>
#include <atomic>
#include <random>

using namespace std::chrono;

static std::mt19937 generator(std::mt19937::result_type(system_clock::now().time_since_epoch().count()));

static std::atomic<std::mt19937::result_type> seq(generator());
static const char hex_digits[] = "0123456789abcdef";

namespace realm {

ObjectId::ObjectId()
{
    memset(m_bytes, 0, sizeof(m_bytes));
}

ObjectId::ObjectId(const null&) noexcept
{
    memset(m_bytes, 0, sizeof(m_bytes));
}

ObjectId::ObjectId(const char* init)
{
    char buf[3];
    REALM_ASSERT(strlen(init) == 24);

    buf[2] = '\0';

    size_t j = 0;
    for (size_t i = 0; i < sizeof(m_bytes); i++) {
        buf[0] = init[j++];
        buf[1] = init[j++];
        m_bytes[i] = char(strtol(buf, nullptr, 16));
    }
}

ObjectId::ObjectId(Timestamp d, int machine_id, int process_id)
{
    auto sec = uint32_t(d.get_seconds());
    // Store in big endian so that memcmp can be used for comparison
    m_bytes[0] = sec >> 24;
    m_bytes[1] = (sec >> 16) & 0xff;
    m_bytes[2] = (sec >> 8) & 0xff;
    m_bytes[3] = sec & 0xff;

    memcpy(m_bytes + 4, &machine_id, 3);
    memcpy(m_bytes + 7, &process_id, 2);

    auto r = seq++;
    // Also store random number as big endian. This ensures that objects created
    // later within the same second will also be sorted correctly
    m_bytes[9] = (r >> 16) & 0xff;
    m_bytes[10] = (r >> 8) & 0xff;
    m_bytes[11] = r & 0xff;
}

Timestamp ObjectId::get_timestamp() const
{
    // Convert back to little endian
    uint32_t sec = (uint32_t(m_bytes[0]) << 24) + (uint32_t(m_bytes[1]) << 16) + (uint32_t(m_bytes[2]) << 8) +
                   uint32_t(m_bytes[3]);

    return Timestamp(sec, 0);
}

std::string ObjectId::to_string() const
{
    std::string ret;
    for (size_t i = 0; i < sizeof(m_bytes); i++) {
        ret += hex_digits[m_bytes[i] >> 4];
        ret += hex_digits[m_bytes[i] & 0xf];
    }
    return ret;
}


} // namespace realm
