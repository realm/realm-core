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

#include "internal_logger.hpp"
#include <algorithm>

namespace realm {
namespace util {

size_t LogEntry::next_event = 1;

std::vector<LogRef> LogRef::buffer(LogRef::end);
std::vector<LogSlab> LogSlab::buffer(LogSlab::end);
std::vector<LogFileAlloc> LogFileAlloc::buffer(LogFileAlloc::end);
std::vector<LogFileOpen> LogFileOpen::buffer(LogFileOpen::end);

int LogSlab::next = 0;
int LogRef::next = 0;
int LogFileAlloc::next = 0;
int LogFileOpen::next = 0;

void LogFileOpen::set_name(const char* fname)
{
    int len = strlen(fname);
    if (len > 31)
        strncpy(name, fname + (len - 31), 31);
    else
        strncpy(name, fname, 31);
}

std::vector<LogEntry*> entries(LogSlab::end + LogRef::end + LogFileAlloc::end + LogFileOpen::end);

void dump_internal_logs(std::ostream& os)
{
    int nr = 0;
    for (auto& e : LogFileOpen::buffer)
        if (e.event_nr) entries[nr++] = &e;
    for (auto& e : LogSlab::buffer)
        if (e.event_nr) entries[nr++] = &e;
    for (auto& e : LogRef::buffer)
        if (e.event_nr) entries[nr++] = &e;
    for (auto& e : LogFileAlloc::buffer)
        if (e.event_nr) entries[nr++] = &e;
    std::sort(entries.begin(), entries.begin() + nr, [](auto& a, auto&b) { return a->event_nr < b->event_nr; });
    os << std::endl << "Internal logs:" << std::endl;
    size_t prev_event_nr = 0;
    for (int i = 0; i < nr; ++i) {
        if (prev_event_nr + 1 < entries[i]->event_nr) os << "    ..." << std::endl;
        LogEntry& e = *(entries[i]);
        os << e;
        prev_event_nr = entries[i]->event_nr;
    }
}

std::ostream& operator<<(std::ostream& os, LogEntry& e)
{
    if (e.event_nr) {
    	os << "    ";
    	if (e.partial) os << "<incomplete:> ";
        os << e.thread_id << " " << e.event_nr << ": " << e.op << "(";
        e.print(os) << ")" << std::endl;
    }
    return os;
}

std::ostream& LogFileOpen::print(std::ostream& os) {
    return os << name;
}

std::ostream& LogRef::print(std::ostream& os) {
    return os << ref;
}

std::ostream& LogFileAlloc::print(std::ostream& os) {
    return os << ref << ", " << request;
}

std::ostream& LogSlab::print(std::ostream& os) {
    return os << ref << ", " << request;
}

}
}

