/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#if !REALM_PLATFORM_WINDOWS

#include "file_mapper.hpp"

#include <cerrno>
#include <sys/mman.h>

#include <realm/util/errno.hpp>

#ifdef REALM_ENABLE_ENCRYPTION

#include "encrypted_file_mapping.hpp"

#include <memory>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <atomic>

#include <realm/util/errno.hpp>
#include <realm/util/encryption_not_supported_exception.hpp>
#include <realm/util/shared_ptr.hpp>
#include <realm/util/terminate.hpp>
#include <realm/util/thread.hpp>
#include <string.h> // for memset

#if REALM_PLATFORM_APPLE
#  include <mach/mach.h>
#  include <mach/exc.h>
#endif

#if REALM_PLATFORM_ANDROID
#  include <linux/unistd.h>
#  include <sys/syscall.h>
#endif

using namespace realm;
using namespace realm::util;

namespace {
bool handle_access(void *addr);

#if REALM_PLATFORM_APPLE

#if REALM_ARCHITECTURE_AMD64 || REALM_ARCHITECTURE_ARM64
typedef int64_t NativeCodeType;
#  define REALM_EXCEPTION_BEHAVIOR MACH_EXCEPTION_CODES|EXCEPTION_STATE_IDENTITY
#else
typedef int32_t NativeCodeType;
#  define REALM_EXCEPTION_BEHAVIOR EXCEPTION_STATE_IDENTITY
#endif

// These structures and the message IDs mostly defined by the .def files included
// with the mach SDK, but parts of it are missing from the iOS SDK and on OS X
// you can only see either the 32-bit or 64-bit versions at a time, but we need
// both to be able to forward unhandled messages from our 64-bit handler to a
// 32-bit handler

#ifdef  __MigPackStructs
#  pragma pack(4)
#endif

template<typename CodeType>
struct ExceptionInfo {
    NDR_record_t NDR;
    exception_type_t exception;
    mach_msg_type_number_t codeCnt;
    CodeType code[2];
};

struct ExceptionSourceThread {
    mach_msg_body_t body;
    mach_msg_port_descriptor_t thread;
    mach_msg_port_descriptor_t task;
};

struct ExceptionState {
    int flavor;
    mach_msg_type_number_t old_stateCnt;
    natural_t old_state[224];
};

template<typename CodeType>
struct RaiseRequest {
    mach_msg_header_t head;
    ExceptionSourceThread thread;
    ExceptionInfo<CodeType> exception;

    typedef int has_thread;
};

template<typename CodeType>
struct RaiseStateRequest {
    mach_msg_header_t head;
    ExceptionInfo<CodeType> exception;
    ExceptionState state;

    typedef int has_state;
};

template<typename CodeType>
struct RaiseStateIdentityRequest {
    mach_msg_header_t head;
    ExceptionSourceThread thread;
    ExceptionInfo<CodeType> exception;
    ExceptionState state;

    typedef int has_thread;
    typedef int has_state;
};

#ifdef  __MigPackStructs
#  pragma pack()
#endif

enum MachExceptionMessageID {
    msg_Request = 2401,
    msg_RequestState = 2402,
    msg_RequestStateIdentity = 2403
};

// Our exception port and the one we replaced which we will forward anything we
// don't handle to
mach_port_t exception_port = MACH_PORT_NULL;
mach_port_t old_port = MACH_PORT_NULL;
exception_behavior_t old_behavior;
thread_state_flavor_t old_flavor;

void check_error(kern_return_t kr)
{
    if (kr != KERN_SUCCESS)
        REALM_TERMINATE(mach_error_string(kr));
}

void send_mach_msg(mach_msg_header_t *msg)
{
    kern_return_t kr = mach_msg(msg, MACH_SEND_MSG, msg->msgh_size,
                                0, MACH_PORT_NULL, // no reply needed
                                MACH_MSG_TIMEOUT_NONE, MACH_PORT_NULL);
    check_error(kr);
}

// Construct and send a reply to the given message
template<typename CodeType>
void send_reply(const RaiseStateIdentityRequest<CodeType>& request, kern_return_t ret_code)
{
    __Reply__exception_raise_state_identity_t reply;
    bzero(&reply, sizeof reply);

    mach_msg_size_t state_size = request.state.old_stateCnt * sizeof request.state.old_state[0];
    REALM_ASSERT_3(sizeof(reply.new_state), >=, state_size);

    reply.Head.msgh_bits = MACH_MSGH_BITS(MACH_MSGH_BITS_REMOTE(request.head.msgh_bits), 0);
    reply.Head.msgh_remote_port = request.head.msgh_remote_port;
    reply.Head.msgh_id = request.head.msgh_id + 100; // msgid of replies is request+100
    reply.NDR = request.exception.NDR;
    reply.RetCode = ret_code;
    reply.flavor = request.state.flavor;
    reply.new_stateCnt = request.state.old_stateCnt;
    memcpy(reply.new_state, request.state.old_state, state_size);

    // subtract the unused portion of the state from the message size
    reply.Head.msgh_size = sizeof reply - sizeof reply.new_state + state_size;

    send_mach_msg(&reply.Head);
}

template<typename CodeType>
void copy_state(const RaiseRequest<CodeType>&, const RaiseStateIdentityRequest<NativeCodeType>&)
{
    // RaiseRequest does not have state
}

template<template<typename> class ForwardType, typename ForwardCodeType>
void copy_state(ForwardType<ForwardCodeType>& forward,
                const RaiseStateIdentityRequest<NativeCodeType>& request,
                typename ForwardType<ForwardCodeType>::has_state = 0)
{
    mach_msg_size_t state_size = request.state.old_stateCnt * sizeof request.state.old_state[0];
    REALM_ASSERT_3(sizeof(forward.state.old_state), >=, state_size);

    forward.state.flavor = old_flavor;
    if (old_flavor == request.state.flavor) {
        forward.state.old_stateCnt = request.state.old_stateCnt;
        memcpy(forward.state.old_state, request.state.old_state, state_size);
    }
    else {
        kern_return_t kr = thread_get_state(request.thread.thread.name,
                                            old_flavor,
                                            forward.state.old_state,
                                            &forward.state.old_stateCnt);
        check_error(kr);
    }

    forward.head.msgh_size -= sizeof request.state.old_state - state_size;
}

template<typename CodeType>
void copy_thread(const RaiseStateRequest<CodeType>&, const RaiseStateIdentityRequest<NativeCodeType>&)
{
    // RaiseStateRequest does not have a thread
}

template<template<typename> class ForwardType, typename ForwardCodeType>
void copy_thread(ForwardType<ForwardCodeType>& forward,
                const RaiseStateIdentityRequest<NativeCodeType>& request,
                typename ForwardType<ForwardCodeType>::has_thread = 0)
{
    forward.thread.body = request.thread.body;
    forward.thread.thread = request.thread.thread;
    forward.thread.task = request.thread.task;
}

template<template<typename> class ForwardType, typename ForwardCodeType>
void convert_and_forward_message(const RaiseStateIdentityRequest<NativeCodeType>& request, MachExceptionMessageID msg)
{
    ForwardType<ForwardCodeType> forward;
    forward.head = request.head;
    forward.head.msgh_id = msg;
    forward.head.msgh_size = sizeof forward;
    forward.head.msgh_local_port = old_port;
    forward.exception.NDR = request.exception.NDR;
    forward.exception.exception = request.exception.exception;
    forward.exception.codeCnt = request.exception.codeCnt;
    forward.exception.code[0] = static_cast<ForwardCodeType>(request.exception.code[0]);
    forward.exception.code[1] = static_cast<ForwardCodeType>(request.exception.code[1]);
    copy_thread(forward, request);
    copy_state(forward, request);

    // The 64-bit IDs are offset 4 from the enum values (which are the 32-bit IDs)
    if (sizeof forward.exception.code == 8)
        forward.head.msgh_id += 4;

    mach_msg_return_t mr = mach_msg(&forward.head,
                                    MACH_SEND_MSG,
                                    forward.head.msgh_size,
                                    0,
                                    MACH_PORT_NULL,
                                    MACH_MSG_TIMEOUT_NONE,
                                    MACH_PORT_NULL);
    if (mr != MACH_MSG_SUCCESS) {
        // Failed to message the old port, so just fall back to behaving as if
        // there was no old port
        send_reply(request, KERN_FAILURE);
        return;
    }
}

void handle_exception()
{
    // Wait for a message
    RaiseStateIdentityRequest<NativeCodeType> request;
    bzero(&request, sizeof request);
    request.head.msgh_local_port = exception_port;
    request.head.msgh_size = sizeof request;
    mach_msg_return_t mr = mach_msg(&request.head,
                                    MACH_RCV_MSG,
                                    0, request.head.msgh_size,
                                    exception_port,
                                    MACH_MSG_TIMEOUT_NONE,
                                    MACH_PORT_NULL);
    check_error(mr);

    if (request.exception.code[0] == KERN_PROTECTION_FAILURE) {
        if (handle_access(reinterpret_cast<void*>(request.exception.code[1]))) {
            // Tell the thread to retry the instruction that faulted and continue running
            send_reply(request, KERN_SUCCESS);
            return;
        }
    }

    // We couldn't handle this error, so forward it on to the handler we replaced

    if (old_port == MACH_PORT_NULL) {
        // There is none, so just fail to handle the message
        send_reply(request, KERN_FAILURE);
        return;
    }

    // The old handler may have asked for messages in a different format from
    // what we're using, so create a new message in that format and send it
    switch (old_behavior) {
        case EXCEPTION_DEFAULT:
            convert_and_forward_message<RaiseRequest, int32_t>(request, msg_Request);
            return;
        case exception_behavior_t(EXCEPTION_DEFAULT | MACH_EXCEPTION_CODES):
            convert_and_forward_message<RaiseRequest, int64_t>(request, msg_Request);
            return;
        case EXCEPTION_STATE:
            convert_and_forward_message<RaiseStateRequest, int32_t>(request, msg_RequestState);
            return;
        case exception_behavior_t(EXCEPTION_STATE | MACH_EXCEPTION_CODES):
            convert_and_forward_message<RaiseStateRequest, int64_t>(request, msg_RequestState);
            return;
        case EXCEPTION_STATE_IDENTITY:
            convert_and_forward_message<RaiseStateRequest, int32_t>(request, msg_RequestStateIdentity);
            return;
        case exception_behavior_t(EXCEPTION_STATE_IDENTITY | MACH_EXCEPTION_CODES):
            convert_and_forward_message<RaiseStateRequest, int64_t>(request, msg_RequestStateIdentity);
            return;
        default:
            REALM_TERMINATE("Unsupported exception behavior");
    }
}

void exception_handler_loop()
{
    while (true)
        handle_exception();
}

void install_handler()
{
    static bool has_installed_handler = false;
    if (has_installed_handler)
        return;
    has_installed_handler = true;

    // Create a port and ask to be able to read from it
    kern_return_t kr;
    kr = mach_port_allocate(mach_task_self(),
                            MACH_PORT_RIGHT_RECEIVE,
                            &exception_port);
    check_error(kr);

    kr = mach_port_insert_right(mach_task_self(),
                                exception_port, exception_port,
                                MACH_MSG_TYPE_MAKE_SEND);
    check_error(kr);

    // Atomically set our port as the handler for EXC_BAD_ACCESS and read the
    // old port so we can forward unhanlded errors to it
    mach_msg_type_number_t old_count;
    exception_mask_t old_mask;
    kr = task_swap_exception_ports(mach_task_self(),
                                   EXC_MASK_BAD_ACCESS,
                                   exception_port,
                                   REALM_EXCEPTION_BEHAVIOR,
                                   MACHINE_THREAD_STATE,
                                   &old_mask,
                                   &old_count,
                                   &old_port,
                                   &old_behavior,
                                   &old_flavor);
    check_error(kr);
    REALM_ASSERT_3(old_mask, ==, EXC_MASK_BAD_ACCESS);
    REALM_ASSERT_3(old_count, ==, 1);

    new Thread(exception_handler_loop);
}

#else // REALM_PLATFORM_APPLE

#if REALM_PLATFORM_ANDROID && REALM_ARCHITECTURE_ARM64
// bionic's sigaction() is broken on arm64, so use the syscall directly
int sigaction_wrapper(int signal, const struct sigaction* new_action, struct sigaction* old_action) {
    __kernel_sigaction kernel_new_action;
    kernel_new_action.sa_flags = new_action->sa_flags;
    kernel_new_action.sa_handler = new_action->sa_handler;
    kernel_new_action.sa_mask = new_action->sa_mask;

    __kernel_sigaction kernel_old_action;
    int result = syscall(__NR_rt_sigaction, signal, &kernel_new_action,
                         &kernel_old_action, sizeof(sigset_t));
    old_action->sa_flags = kernel_old_action.sa_flags;
    old_action->sa_handler = kernel_old_action.sa_handler;
    old_action->sa_mask = kernel_old_action.sa_mask;

    return result;
}
#else
#  define sigaction_wrapper sigaction
#endif

// The signal handlers which our handlers replaced, if any, for forwarding
// signals for segfaults outside of our encrypted pages
struct sigaction old_segv;
struct sigaction old_bus;

void* expected_si_addr;

enum {
    signal_test_state_Untested,
    signal_test_state_Works,
    signal_test_state_Broken
};

volatile sig_atomic_t signal_test_state = signal_test_state_Untested;

void signal_handler(int code, siginfo_t* info, void* ctx)
{
    if (signal_test_state == signal_test_state_Untested) {
        signal_test_state = info->si_addr == expected_si_addr ? signal_test_state_Works : signal_test_state_Broken;
        mprotect(expected_si_addr, page_size(), PROT_READ | PROT_WRITE);
        return;
    }

    if (handle_access(info->si_addr))
        return;

    // forward unhandled signals
    if (code == SIGSEGV) {
        if (old_segv.sa_sigaction)
            old_segv.sa_sigaction(code, info, ctx);
        else if (old_segv.sa_handler)
            old_segv.sa_handler(code);
        else
            REALM_TERMINATE("Segmentation fault");
    }
    else if (code == SIGBUS) {
        if (old_bus.sa_sigaction)
            old_bus.sa_sigaction(code, info, ctx);
        else if (old_bus.sa_handler)
            old_bus.sa_handler(code);
        else
            REALM_TERMINATE("Segmentation fault");
    }
    else
        REALM_TERMINATE("Segmentation fault");
}

void install_handler()
{
    static bool has_installed_handler = false;
    // Test failed before, just throw the exception
    if (signal_test_state == signal_test_state_Broken)
        throw EncryptionNotSupportedOnThisDevice();

    if (has_installed_handler)
        return;

    has_installed_handler = true;

    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = signal_handler;
    action.sa_flags = SA_SIGINFO;

    if (sigaction_wrapper(SIGSEGV, &action, &old_segv) != 0)
        REALM_TERMINATE("sigaction SEGV failed");
    if (sigaction_wrapper(SIGBUS, &action, &old_bus) != 0)
        REALM_TERMINATE("sigaction SIGBUS failed");

    // Test if the SIGSEGV handler is actually sent the address, as on some
    // devices it's always 0
    size_t size = page_size();

    // Allocate an unreadable/unwritable block of address space
    expected_si_addr = ::mmap(0, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (expected_si_addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
    }

    // Should produce a SIGSEGV with si_addr = expected_si_addr
    mprotect(expected_si_addr, size, PROT_NONE);
    *static_cast<char *>(expected_si_addr) = 0;

    ::munmap(expected_si_addr, size);
    if (signal_test_state != signal_test_state_Works)
        throw EncryptionNotSupportedOnThisDevice();
}

#endif // REALM_PLATFORM_APPLE

class SpinLockGuard {
public:
    SpinLockGuard(std::atomic<bool>& lock) : m_lock(lock)
    {
        while (m_lock.exchange(true, std::memory_order_acquire)) ;
    }

    ~SpinLockGuard()
    {
        m_lock.store(false, std::memory_order_release);
    }

private:
    std::atomic<bool>& m_lock;
};

// A list of all of the active encrypted mappings for a single file
struct mappings_for_file {
    dev_t device;
    ino_t inode;
    SharedPtr<SharedFileInfo> info;
};

// Group the information we need to map a SIGSEGV address to an
// EncryptedFileMapping for the sake of cache-friendliness with 3+ active
// mappings (and no worse with only two
struct mapping_and_addr {
    SharedPtr<EncryptedFileMapping> mapping;
    void* addr;
    size_t size;
};

std::atomic<bool> mapping_lock;
std::vector<mapping_and_addr> mappings_by_addr;
std::vector<mappings_for_file> mappings_by_file;

// If there's any active mappings when the program exits, deliberately leak them
// to avoid flushing things that were in the middle of being modified on a different thrad
struct AtExit {
    ~AtExit()
    {
        if (!mappings_by_addr.empty())
            (new std::vector<mapping_and_addr>)->swap(mappings_by_addr);
        if (!mappings_by_file.empty())
            (new std::vector<mappings_for_file>)->swap(mappings_by_file);
    }
} at_exit;

bool handle_access(void *addr)
{
    SpinLockGuard lock(mapping_lock);
    for (size_t i = 0; i < mappings_by_addr.size(); ++i) {
        mapping_and_addr& m = mappings_by_addr[i];
        if (m.addr > addr || static_cast<char*>(m.addr) + m.size <= addr)
            continue;

        m.mapping->handle_access(addr);
        return true;
    }
    return false;
}

mapping_and_addr* find_mapping_for_addr(void* addr, size_t size)
{
    for (size_t i = 0; i < mappings_by_addr.size(); ++i) {
        mapping_and_addr& m = mappings_by_addr[i];
        if (m.addr == addr && m.size == size)
            return &m;
    }

    return 0;
}

void add_mapping(void* addr, size_t size, int fd, size_t file_offset,
                 File::AccessMode access, const char* encryption_key)
{
    struct stat st;
    if (fstat(fd, &st)) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("fstat() failed: ", err));
    }

    if (st.st_size > 0 && static_cast<size_t>(st.st_size) < page_size())
        throw DecryptionFailed();

    SpinLockGuard lock(mapping_lock);
    install_handler();

    std::vector<mappings_for_file>::iterator it;
    for (it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
        if (it->inode == st.st_ino && it->device == st.st_dev)
            break;
    }

    // Get the potential memory allocation out of the way so that mappings_by_addr.push_back can't throw
    mappings_by_addr.reserve(mappings_by_addr.size() + 1);

    if (it == mappings_by_file.end()) {
        mappings_by_file.reserve(mappings_by_file.size() + 1);

        fd = dup(fd);
        if (fd == -1) {
            int err = errno; // Eliminate any risk of clobbering
            throw std::runtime_error(get_errno_msg("dup() failed: ", err));
        }

        mappings_for_file f;
        f.device = st.st_dev;
        f.inode = st.st_ino;
        try {
            f.info = new SharedFileInfo(reinterpret_cast<const uint8_t*>(encryption_key), fd);
        }
        catch (...) {
            ::close(fd);
            throw;
        }

        mappings_by_file.push_back(f); // can't throw due to reserve() above
        it = mappings_by_file.end() - 1;
    }

    try {
        mapping_and_addr m;
        m.addr = addr;
        m.size = size;
        m.mapping = new EncryptedFileMapping(*it->info, file_offset, addr, size, access);
        mappings_by_addr.push_back(m); // can't throw due to reserve() above
    }
    catch (...) {
        if (it->info->mappings.empty()) {
            ::close(it->info->fd);
            mappings_by_file.erase(it);
        }
        throw;
    }
}

void remove_mapping(void* addr, size_t size)
{
    size = round_up_to_page_size(size);
    SpinLockGuard lock(mapping_lock);
    mapping_and_addr* m = find_mapping_for_addr(addr, size);
    if (!m)
        return;

    mappings_by_addr.erase(mappings_by_addr.begin() + (m - &mappings_by_addr[0]));
    for (std::vector<mappings_for_file>::iterator it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
        if (it->info->mappings.empty()) {
            if (::close(it->info->fd) != 0) {
                int err = errno; // Eliminate any risk of clobbering
                if (err == EBADF || err == EIO) // todo, how do we handle EINTR?
                    throw std::runtime_error(get_errno_msg("close() failed: ", err));
            }
            mappings_by_file.erase(it);
            break;
        }
    }
}

void* mmap_anon(size_t size)
{
    void* addr = ::mmap(0, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
    }
    return addr;
}

} // anonymous namespace
#endif

namespace realm {
namespace util {

#ifdef REALM_ENABLE_ENCRYPTION
size_t round_up_to_page_size(size_t size) noexcept
{
    return (size + page_size() - 1) & ~(page_size() - 1);
}
#endif

void* mmap(int fd, size_t size, File::AccessMode access, std::size_t offset, const char* encryption_key)
{
#ifdef REALM_ENABLE_ENCRYPTION
    if (encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        add_mapping(addr, size, fd, offset, access, encryption_key);
        return addr;
    }
    else
#else
    REALM_ASSERT(!encryption_key);
    static_cast<void>(encryption_key);
#endif
    {
        int prot = PROT_READ;
        switch (access) {
            case File::access_ReadWrite:
                prot |= PROT_WRITE;
                break;
            case File::access_ReadOnly:
                break;
        }

        void* addr = ::mmap(0, size, prot, MAP_SHARED, fd, offset);
        if (addr != MAP_FAILED)
            return addr;
    }

    int err = errno; // Eliminate any risk of clobbering
    throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
}

void munmap(void* addr, size_t size) noexcept
{
#ifdef REALM_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif
    if(::munmap(addr, size) != 0) {
        int err = errno;
        throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
    }
}

void* mremap(int fd, size_t file_offset, void* old_addr, size_t old_size, 
             File::AccessMode a, size_t new_size)
{
#ifdef REALM_ENABLE_ENCRYPTION
    {
        SpinLockGuard lock(mapping_lock);
        size_t rounded_old_size = round_up_to_page_size(old_size);
        if (mapping_and_addr* m = find_mapping_for_addr(old_addr, rounded_old_size)) {
            size_t rounded_new_size = round_up_to_page_size(new_size);
            if (rounded_old_size == rounded_new_size)
                return old_addr;

            void* new_addr = mmap_anon(rounded_new_size);
            m->mapping->set(new_addr, rounded_new_size, file_offset);
            int i = ::munmap(old_addr, rounded_old_size);
            m->addr = new_addr;
            m->size = rounded_new_size;
            if (i != 0) {
                int err = errno;
                throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
            }
            return new_addr;
        }
    }
#endif

#ifdef _GNU_SOURCE
    {
        void* new_addr = ::mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
        if (new_addr != MAP_FAILED)
            return new_addr;
        int err = errno; // Eliminate any risk of clobbering
        if (err != ENOTSUP)
            throw std::runtime_error(get_errno_msg("mremap(): failed: ", err));
    }
    // Fall back to no-mremap case if it's not supported
#endif

    void* new_addr = mmap(fd, new_size, a, file_offset, nullptr);
    if (::munmap(old_addr, old_size) != 0) {
        int err = errno;
        throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
    }
    return new_addr;
}

void msync(void* addr, size_t size)
{
#ifdef REALM_ENABLE_ENCRYPTION
    { // first check the encrypted mappings
        SpinLockGuard lock(mapping_lock);
        if (mapping_and_addr* m = find_mapping_for_addr(addr, round_up_to_page_size(size))) {
            m->mapping->flush();
            m->mapping->sync();
            return;
        }
    }
#endif

    // not an encrypted mapping

    // FIXME: on iOS/OSX fsync may not be enough to ensure crash safety.
    // Consider adding fcntl(F_FULLFSYNC). This most likely also applies to msync.
    //
    // See description of fsync on iOS here:
    // https://developer.apple.com/library/ios/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html
    //
    // See also
    // https://developer.apple.com/library/ios/documentation/Cocoa/Conceptual/CoreData/Articles/cdPersistentStores.html
    // for a discussion of this related to core data.
    if (::msync(addr, size, MS_SYNC) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("msync() failed: ", err));
    }
}

}
}

#endif // !REALM_PLATFORM_WINDOWS
