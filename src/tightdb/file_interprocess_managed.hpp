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

#include <tightdb/config.h>
#include <tightdb/assert.hpp>
#include <tightdb/unique_ptr.hpp>
#include <tightdb/safe_int_ops.hpp>

namespace tightdb {


template<class T> class IPMFile
{

public:
    // Intro

    // An IPMFile can be in one of the following states:
    //
    // - nonexisting: the file does not exist.
    //
    // - stale: the file exists, but noone is accessing it.
    //
    // - shared: the file exists, has been initialized and mapped
    //           and multiple threads have access to the file.
    //
    // - exclusive: the file exists, has been initialized and mapped
    //              and a single thread has access to the file. Further
    //              more, no other thread can complete the 'open' call
    //              and transition the file to either shared or exclusive state,
    //              until the thread with exclusive access explicitly chooses to
    //              share the file or crashes.
    //
    // - indeterminate: the file exists, but it cannot be determined that
    //                  noone is accessing it. Such files are NEVER automatically
    //                  deleted or reinitialized. Instead the situation is reported
    //                  by throwing an exception.

    // The tasks of the IPMFile abstraction is as follows:
    // 1) To minimize the occurence of indeterminate IPMFiles, to properly
    //    recognize them and to signal using exceptions.
    // 2) To properly recognize stale IPMFiles and re-initialize them.
    // 3) To automatically remove IPMFiles if noone accesses them. This is a "best effort";
    //    there is no guarantee of always removing unreferenced files.
    // 4) To ensure proper (re)initialization and assignment of exclusive ownership.
    // 5) To ensure atomic transitions between the first four states.
    //    (Hence, you cannot WAIT for exclusive access, as that would lead to deadlocks)

    // Limitations:
    // The implementation of IPMFile may rely on file locks and requires that IPMFiles
    // are accessed EXCLUSIVELY through the IPMFile abstraction. Accessing the underlying
    // files using primitives not offered through the IPMFile abstraction causes undefined
    // behavior.

    // Operations:

    // Open file with a given name, map it into memory and superimpose a structure
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
    T* open(std::string file_path, bool& is_exclusive, int msec_timeout = 0);

    // Reopen a file which has been opened earlier, then closed again. Similar to
    // open, just reusing the parameters provided in an earlier call to open.
    T* reopen(int msec_timeout = 0);

    // obtain an additional mapping of the file at a new address. Such mappings must
    // be explicitly removed by the user by calling remove_map. It is not possible
    // to remove or remap the initial mapping (returned by open), except through call
    // to close, but additional mappings can be added and removed.
    T* add_map();
    void remove_map(T*);

    // release exclusive access if you have it, ignored otherwise. Transitioning
    // to shared state is atomic, i.e. no other process can gain exclusive access
    // during the transition.
    void share();

    // unmap memory and close the file. May also initiate cleanup, removal of the
    // file if no one else uses it, etc.
    void close();
    ~IPMFile();

    // Try to obtain exclusive access. Returns true if the caller is guaranteed to
    // be the only accessor of the file. If succesfull, other threads trying to open
    // the file will wait for this thread to share() the file again (or die).
    // Transitioning to exclusive state is atomic, i.e. no other process can
    // gain exclusive access before this process gets it. Note, that there is no
    // option to WAIT for exclusive access. In case of contention, NO thread gets
    // exclusive access, all requests just returns false.
    bool got_exclusive_access();

    // invalidate the file. This unmaps memory and closes the file, and also ensures
    // that
    // - is_valid() will return false for any thread already having opened the file.
    // - open() will re-initialize the file before using it again, or create and work on a
    //   different file with the same name - whichever matches the underlying system best.
    void invalidate();
    bool is_valid();

    // true if the file has been removed since it was opened. On some systems (UNIX),
    // it is possible to remove a file even though it is open. This call also returns
    // true if the file has been replaced by another file with the same name.
    bool is_removed();
}

}
