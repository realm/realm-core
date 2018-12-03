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
#include <vector>
#include <thread>

namespace realm {
namespace util {


class LogEntry {
public:
    size_t event_nr = 0;
    bool partial = true;
    std::thread::id thread_id;
    const char* op = "empty";
    static size_t next_event;
    virtual std::ostream& print(std::ostream& os) = 0;
    virtual ~LogEntry() { }
protected:
    LogEntry() {}
};

class LogRef : public LogEntry { // currently used for write transaction start/end
public:
    size_t ref;
    static constexpr int end = 32;
    static std::vector<LogRef> buffer;
    static int next;
    std::ostream& print(std::ostream& os) override;
    virtual ~LogRef() {}
    LogRef() {}
};

class LogSlabOp : public LogEntry {
public:
    size_t request;
    size_t ref;
    static constexpr int end = 64;
    static std::vector<LogSlabOp> buffer;
    static int next;
    std::ostream& print(std::ostream& os) override;
    virtual ~LogSlabOp() { }
    LogSlabOp() {}
};

class LogFileStorageOp : public LogEntry {
public:
    size_t request;
    size_t ref;
    static constexpr int end = 64;
    static std::vector<LogFileStorageOp> buffer;
    static int next;
    std::ostream& print(std::ostream& os) override;
    virtual ~LogFileStorageOp() { }
    LogFileStorageOp() {}
};

struct LogFileOp : public LogEntry {
    static constexpr unsigned int suffix_size = 64;
    char name[suffix_size]; // suffix of name
    static constexpr int end = 16;
    static std::vector<LogFileOp> buffer;
    static int next;
    void set_name(const std::string& nm);
    std::ostream& print(std::ostream& os) override;
    virtual ~LogFileOp() { }
    LogFileOp() {}
};

std::ostream& operator<<(std::ostream& os, LogEntry& e);

template <typename Type, typename Func> void log_internal(const char* op_name, Func func) {
    Type& e = *(Type::buffer.begin() + Type::next++);
    if (Type::next == Type::end)
        Type::next = 0;
    e.partial = true;
    e.event_nr = LogEntry::next_event++;
    e.op = op_name;
    e.thread_id = std::this_thread::get_id();
    func(e);
    e.partial = false;
}

void dump_internal_logs(std::ostream&);
} // namespace util
} // namespace realm

#endif
