#if defined _WIN32
#  include <windows.h>
#  include <psapi.h>
#elif defined __APPLE__
#  include <mach/mach.h>
#elif defined TIGHTDB_HAVE_LIBPROCPS
// Requires libprocps (formerly known as libproc)
#  include <proc/readproc.h>
#endif

#include "mem.hpp"

using namespace std;


#ifdef _WIN32

namespace {

int compare(const void* a, const void* b)
{
    if (*PDWORD(a) == *PDWORD(b))
        return 0;
    return *PDWORD(a) > *PDWORD(b) ? 1 : -1;
}

// Calculate Private Working Set
// Source: http://www.codeproject.com/KB/cpp/XPWSPrivate.aspx
DWORD calculate_ws_private(DWORD process_id)
{
    DWORD dWorkingSetPages[1024 * 128]; // hold the working set information get from QueryWorkingSet()
    DWORD dPageSize = 0x1000;

    DWORD dSharedPages = 0;
    DWORD dPrivatePages = 0;
    DWORD dPageTablePages = 0;

    HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, process_id);

    if (hProcess) throw RuntimeError("Failed");

    __try {
        if (!QueryWorkingSet(hProcess, dWorkingSetPages, sizeof (dWorkingSetPages)))
            __leave;

        DWORD dPages = dWorkingSetPages[0];

        qsort(&dWorkingSetPages[1], dPages, sizeof (DWORD), compare);

        for (DWORD i = 1; i <= dPages; ++i) {
            DWORD dCurrentPageStatus = 0;
            DWORD dCurrentPageAddress;
            DWORD dNextPageAddress;
            DWORD dNextPageFlags;
            DWORD dPageAddress = dWorkingSetPages[i] & 0xFFFFF000;
            DWORD dPageFlags = dWorkingSetPages[i] & 0x00000FFF;

            while (i <= dPages) { // iterate all pages
                dCurrentPageStatus++;

                if (i == dPages) break; //if last page

                dCurrentPageAddress = dWorkingSetPages[i] & 0xFFFFF000;
                dNextPageAddress = dWorkingSetPages[i+1] & 0xFFFFF000;
                dNextPageFlags = dWorkingSetPages[i+1] & 0x00000FFF;

                //decide whether iterate further or exit
                //(this is non-contiguous page or have different flags)
                if ((dNextPageAddress == (dCurrentPageAddress + dPageSize)) && (dNextPageFlags == dPageFlags)) {
                    i++;
                }
                else break;
            }

            if ((dPageAddress < 0xC0000000) || (dPageAddress > 0xE0000000)) {
                if ( dPageFlags & 0x100 ) // this is shared one
                    dSharedPages += dCurrentPageStatus;

                else // private one
                    dPrivatePages += dCurrentPageStatus;
            }
            else {
                dPageTablePages += dCurrentPageStatus; //page table region
            }
        }

        DWORD dTotal = dPages * 4;
        DWORD dShared = dSharedPages * 4;
        DWORD WSPrivate = dTotal - dShared;

        return WSPrivate;
    }
    __finally {
        CloseHandle(hProcess);
    }
    throw RuntimeError("Failed");
}

} // anonymous namespace

#endif // _WIN32


namespace tightdb {
namespace test_util {


size_t get_mem_usage()
{
#if defined _WIN32

    // FIXME: Does this return virtual size or resident set size? What
    // we need is the virtual size, i.e., we want to include that
    // which is temporarily swapped out.
    return calculate_ws_private(GetCurrentProcessId());

#elif defined __APPLE__

    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
    if (KERN_SUCCESS != task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count)) return -1;
    // resident size is in t_info.resident_size;
    // virtual size is in t_info.virtual_size;
    // FIXME: Virtual size does not seem to contain a usfull metric as
    // expected. It is way too large. If resident size, as expected,
    // includes swapped out memory, it is not the metric we need
    // either, yet we will yse the resident size for now.
    return t_info.resident_size;

#elif defined TIGHTDB_HAVE_LIBPROCPS

    struct proc_t usage;
    look_up_our_self(&usage);
    // The header file says 'vsize' is in number of pages, yet it
    // definitely appears to be in bytes.
    return usage.vsize;

#else

    throw RuntimeError("Not supported");

#endif
}

} // namespace test_util
} // namespace tightdb
