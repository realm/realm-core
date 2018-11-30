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

#ifndef REALM_UTIL_INTERNAL_LOGGER_HPP
#define REALM_UTIL_INTERNAL_LOGGER_HPP

#include <cstring>
#include <iostream>
#include <thread>

namespace realm {
namespace util {


struct LogEntry {
    size_t event_nr;
    std::thread::id thread_id;
    const char* op;
    static size_t next_event;
    virtual std::ostream& print(std::ostream& os) = 0;
};

struct LogRef : public LogEntry {
    size_t ref;
    static constexpr int end = 16;
    static LogRef buffer[end];
    static int next;
    std::ostream& print(std::ostream& os) override;
};

struct LogSlab : public LogEntry {
    size_t request;
    size_t ref;
    static constexpr int end = 64;
    static LogSlab buffer[end];
    static int next;
    std::ostream& print(std::ostream& os) override;
};

struct LogFileAlloc : public LogEntry {
    size_t request;
    size_t ref;
    static constexpr int end = 64;
    static LogFileAlloc buffer[end];
    static int next;
    std::ostream& print(std::ostream& os) override;
};

struct LogFileOpen : public LogEntry {
    char name[32]; // suffix of name
    static constexpr int end = 4;
    static LogFileOpen buffer[];
    static int next;
    void set_name(const char* nm);
    std::ostream& print(std::ostream& os) override;
};

std::ostream& operator<<(std::ostream& os, LogEntry& e);

template <typename Type, typename Func> void log_internal(const char* op_name, Func func) {
    Type& e = Type::buffer[Type::next++];
    memset(&e, 0, sizeof(e));
    if (Type::next == Type::end)
        Type::next = 0;
    e.event_nr = LogEntry::next_event++;
    e.op = op_name;
    e.thread_id = std::this_thread::get_id();
    func(e);
}

void dump_internal_logs(std::ostream&);
} // namespace util
} // namespace realm

#endif
