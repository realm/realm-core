/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_FILE_IPM_HPP
#define TIGHTDB_FILE_IPM_HPP

#include <cstddef>
#include <stdint.h>
#include <stdexcept>
#include <string>
#include <streambuf>

#include <tightdb/util/features.h>
#include <tightdb/util/assert.hpp>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/file.hpp>
#include <tightdb/util/thread.hpp>

namespace tightdb {


class IPMFile
{

public:
    // Intro

    // An IPMFile can be in one of the following states:
    //
    // - nonexisting: the file does not exist.
    //
    // - stale: the file exists, but no one is accessing it.
    //
    //          Note: from the applications point of view, there is no operational
    //          difference between stale and nonexisting. The difference is only visible
    //          to an observer searching for the file outside the IPMFile abstraction.
    //
    // - shared: the file exists, has been initialized and mapped
    //           and multiple threads have completed 'open' (but not 'close')
    //           and may access the file.
    //
    // - exclusive: the file exists, has been initialized and mapped
    //              and a single thread has access to the file. Further
    //              more, no other thread can complete the 'open' call
    //              and transition the file to either shared or exclusive state,
    //              until the thread with exclusive access explicitly chooses to
    //              share the file or crashes.
    //
    // - indeterminate: the file exists, but it cannot be determined that
    //                  no one is accessing it. Such files are NEVER automatically
    //                  deleted or reinitialized. Instead the situation is reported
    //                  by throwing an exception.

    // The tasks of the IPMFile abstraction is as follows:
    // 1) To minimize the occurence of indeterminate IPMFiles, to properly
    //    recognize them and to signal using exceptions.
    // 2) To properly recognize stale IPMFiles and re-initialize them.
    // 3) To automatically remove IPMFiles if no one accesses them. This is a "best effort";
    //    there is no guarantee of always removing unreferenced files.
    // 4) To ensure proper (re)initialization and assignment of exclusive ownership.
    // 5) To ensure atomic transitions between the first four states.
    //    (Hence, you cannot WAIT for exclusive access, as that would lead to deadlocks)

    // Limitations:
    // The implementation of IPMFile may rely on file locks and requires that IPMFiles
    // are accessed EXCLUSIVELY through the IPMFile abstraction. Accessing the underlying
    // files using primitives not offered through the IPMFile abstraction causes undefined
    // behavior.

    // Helper declarations:
    class PresumablyStaleFile : public std::runtime_error {
    public:
        PresumablyStaleFile(const std::string& s) : std::runtime_error(s) {};
    };

    struct IPMFileSharedInfo {
        // IPMFile specific fields:
        util::Atomic<uint32_t> m_transition_count;
        util::Atomic<uint32_t> m_exit_count;
    };

    template<typename T>
    struct IPMFileWrapper {
        IPMFileSharedInfo info;
        T user_data;
    };

    // Operations:

    // Associate an IPMFile object with a file name.
    IPMFile(std::string file_path);
    IPMFile();
    void associate(std::string file_path); // throws if already open

    // Open the associated file, map it into memory and superimpose a structure
    // of type T on top of it. NOTE: the IPMFile abstraction may add additional state
    // to the file, so you cannot assume that the returned pointer points to the start
    // of the file.
    //
    // Atomically:
    // - If the file is indeterminate, throws.
    // - If the file is nonexisting or stale, it is initialized to zeroes
    //   and transitions to exclusive state.
    // - If the file is already in the exclusive state, open will wait for it
    //   to enter one of the other states, then retry. Providing a non-zero
    //   timeout will limit the wait. If the timeout expires without beeing able
    //   to enter either the shared or the exclusive state, open will throw.
    // - If the file is already in the shared state, open will return.
    // - Further: If the file is already in the exclusive state and is closed
    //   by a process with exclusive access, without first beeing shared,
    //   exactly one of any waiting calls to open will complete with exclusive
    //   access.

    template<class T> 
    T* open(bool& is_exclusive, int msec_timeout = 0);
    // untyped:
    void* open(bool& is_exclusive, size_t size, int msec_timeout = 0);

    // Get the file. Useful for manipulating additional mappings of the file.
    // Remember: map it to Map<IPMFileWrapper<T>>, and access pointer->user_data
    // subfields.
    // FIXME: find more elegant api for this form of mapping, so that IPMFileWrapper 
    // isn't exposed here.
    util::File& get_file();

    // release exclusive access if you have it, ignored otherwise. Transitioning
    // to shared state is atomic, i.e. no other process can gain exclusive access
    // during the transition.
    void share();

    // unmap memory and close the file. May also initiate cleanup, removal of the
    // file if no one else uses it, etc.
    // If close is called by a thread having the file in exclusive state, the file
    // transitions to either 'stale' og 'nonexistant' atomically.
    // the atomicity ensures that any threads waiting in 'open' will see an initialized
    // file and that exactly one of those waiting threads will get exclusive access
    // to the file.
    void close();
    ~IPMFile();

    // Try to obtain exclusive access. Returns true if the caller is guaranteed to
    // be the only accessor of the file. If succesfull, other threads trying to open
    // the file will wait for this thread to share() the file again (or die).
    // Transitioning to exclusive state is atomic, i.e. no other process can
    // gain exclusive access before this process gets it. Note, that there is no
    // option to WAIT for exclusive access. In case of contention, NO thread gets
    // exclusive access, all requests just returns false.
    //
    // If caller set promise_to_exit, he is obliged to not do any further calls
    // to change state, except calling close. In return, the IPMFile guarantees
    // that in case of a) contention and b) no one not contending, exactly one 
    // of those contenders promising to exit will in fact get exclusivity.
    //
    // If promise_to_exit is set, and try_get_exclusive_access fails, the caller
    // has also lost the right to access memory mapped by the file. To detect
    // violation of this restriction, the primary mapping is removed and the
    // file is closed.
    // FIXME: do the same to secondary mapping(s)
    bool try_get_exclusive_access(bool promise_to_exit = false);

    // inquire exclusivity - do not attempt to get it if we don't have it.
    bool has_exclusive_access();

    // true if the file has been removed since it was opened. On some systems (UNIX),
    // it is possible to remove a file even though it is open. This call also returns
    // true if the file has been replaced by another file with the same name.
    bool is_removed();
    // NOTE: On systems where an open file can be deleted (UNIX derivatives), doing so
    // whithout going through the IPMFile interface can violate basic assumptions in
    // the implementation. The IPMFile implementation may assume that files are only
    // removed under its own control. Hence, if is_removed() returns true, it should
    // most likely be treated as a potentially serious malfunction.
    //
    // FIXME: Not sure if this method is needed .. or dangerous

private:
    struct IPMFileImplementation;
    IPMFileImplementation* m_impl;

};


template<class T> 
T* IPMFile::open(bool& is_exclusive, int msec_timeout)
{
    return reinterpret_cast<T*>( open(is_exclusive, sizeof(T), msec_timeout) );
}



}

#endif
