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
    struct IPMBlock {
        // internal fields goes here
        T users_shared_state;
    };

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
    //              and transition the file to shared state, until the thread
    //              with exclusive access explicitly chooses to share the file
    //              or crashes.
    //
    // - indeterminate: the file exists, but it cannot be determined that
    //                  noone is accessing it. Such files are NEVER automatically
    //                  deleted or reinitialized.

    // The tasks of the IPMFile abstraction is as follows:
    // 1) To minimize the occurence of indeterminate IPMFiles, to properly
    //    recognize them and to signal using exceptions.
    // 2) To properly recognize stale IPMFiles and re-initialize them.
    // 3) To automatically remove IPMFiles if noone accesses them.
    // 4) To ensure proper (re)initialization and assignment of exclusive ownership.
    // 5) To ensure atomic transitions between the first four states.
    //    (Hence, you cannot WAIT for exclusive access, as that would lead to deadlocks)

    // Opens file with a given name and maps a structure of type T into memory.
    // Atomically:
    // - If the file is indeterminate, throws.
    // - If the file is nonexisting or stale, it is initialized to zeroes
    //   and transitions to exclusive state.
    // - If the file is already in the exclusive state, open will wait for it
    //   to enter one of the other states, then retry.
    // - If the file is already in the shared state, open will return.
    T* open(std::string file_path, bool& is_exclusive);

    // release exclusive access if you have it, ignored otherwise. Transitioning
    // to shared state is atomic, i.e. no other process can gain exclusive access
    // during the transition.
    void share();

    // unmap memory and close the file.
    void close();

    // Try to obtain exclusive access. Returns true if the caller is guaranteed to
    // be the only accessor of the file. If succesfull, other threads trying to open
    // the file will wait for this thread to share() the file again (or die).
    bool got_exclusive_access();

}

}
