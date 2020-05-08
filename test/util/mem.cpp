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

#include <stdexcept>

#include <realm/util/features.h>
#include <realm/util/backtrace.hpp>

#if defined _WIN32
#include <windows.h>
#include <psapi.h>
#elif REALM_PLATFORM_APPLE
#include <mach/mach.h>
#elif defined REALM_HAVE_LIBPROCPS
// Requires libprocps (formerly known as libproc)
#include <proc/readproc.h>
#endif

#include "mem.hpp"

namespace realm {
namespace test_util {


size_t get_mem_usage()
{
#if defined _WIN32

    PROCESS_MEMORY_COUNTERS_EX pmc;
    GetProcessMemoryInfo(GetCurrentProcess(), reinterpret_cast<PPROCESS_MEMORY_COUNTERS>(&pmc), sizeof(pmc));
    return pmc.PrivateUsage;

#elif REALM_PLATFORM_APPLE

    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
    if (KERN_SUCCESS !=
        task_info(mach_task_self(), TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&t_info), &t_info_count))
        return -1;
    // resident size is in t_info.resident_size;
    // virtual size is in t_info.virtual_size;
    // FIXME: Virtual size does not seem to contain a usfull metric as
    // expected. It is way too large. If resident size, as expected,
    // includes swapped out memory, it is not the metric we need
    // either, yet we will yse the resident size for now.
    return t_info.resident_size;

#elif defined REALM_HAVE_LIBPROCPS

    struct proc_t usage;
    look_up_our_self(&usage);
    // The header file says 'vsize' is in number of pages, yet it
    // definitely appears to be in bytes.
    return usage.vsize;

#else

    throw util::runtime_error("Not supported");

#endif
}

} // namespace test_util
} // namespace realm
