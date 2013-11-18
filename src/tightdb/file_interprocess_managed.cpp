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

#include "file_interprocess_managed.hpp"
#include "thread.hpp"
#include "utilities.hpp"
#include "file.hpp"
#include <limits>

namespace tightdb {

struct IPMFile::IPMFileImplementation {
    std::string m_path;
    bool m_is_open;
    File m_file;
    File::Map<IPMFileSharedInfo> m_file_map;
    IPMFileSharedInfo* m_info;
    void init(std::string);
};

void IPMFile::IPMFileImplementation::init(std::string path)
{
    m_path = path;
    m_is_open = false;
}


IPMFile::IPMFile(std::string file_path)
{
    m_impl = new IPMFileImplementation;
    m_impl->init(file_path);
}


IPMFile::IPMFile()
{
    m_impl = new IPMFileImplementation;
    m_impl->init("");
}


void IPMFile::associate(std::string file_path)
{
    if (m_impl->m_is_open)
        throw std::runtime_error("New association cannot be established while file is open");

    m_impl->m_path = file_path;
}


void* IPMFile::open(bool& is_exclusive, size_t size, int msec_timeout)
{
    if (m_impl->m_path == "")
        throw std::runtime_error("Must associate with filename before opening");
    if (m_impl->m_is_open)
        throw std::runtime_error("Cannot open already opened file");
    if (msec_timeout == 0) msec_timeout = std::numeric_limits<int>::max();

    while (msec_timeout >= 0) {

        size_t file_size = 0;
        bool need_init;
        bool got_exclusive;
        bool got_shared;
        m_impl->m_file.open(m_impl->m_path, need_init);

        // Try to get a lock - preferably an exclusive lock,
        // but if not be contend with a shared lock for now. It is important to try
        // (and potentially retry) in order to ensure that ONE of the clients
        // gets exclusive access if possible.
        got_exclusive = m_impl->m_file.try_lock_exclusive();
        if (got_exclusive) {
            got_shared = false;
        }
        else {
            got_shared = m_impl->m_file.try_lock_shared();
        }
        if (!got_exclusive && !got_shared) {

            // someone else has exclusive access, backout and retry
            m_impl->m_file.close();
            msec_timeout--;
            micro_sleep(1000);
            continue;
        }

        // If the file is smaller than a size limit we trust the exclusive lock and initialize
        // the file. The file is only smaller than the size limit before initialization.
        // If the file is too small and we didn't get the exclusive lock, we back out and retry.
        if (int_cast_with_overflow_detect(m_impl->m_file.get_size(), file_size)) {

            m_impl->m_file.unlock();
            m_impl->m_file.close();
            // don't remove it, require operator intervention:
            throw std::runtime_error("Lock file too large");
        }
        if (file_size < size+sizeof(IPMFileSharedInfo)) {

            if (got_exclusive) {

                // initialize the file. zero transition count, zero activity count:
                file_size = sizeof (IPMFileSharedInfo) + size;
                char* empty_buf = new char[file_size];
                std::fill(empty_buf, empty_buf+file_size, 0);
                m_impl->m_file.write(empty_buf, file_size);
                delete[] empty_buf;

            }
            if (got_shared) {

                // we need exclusive access to init the file, so back out and retry:
                m_impl->m_file.close();
                msec_timeout--;
                micro_sleep(1000);
                continue;
            }
        }

        // The file has proper size and we can map it!
        File::CloseGuard fcg(m_impl->m_file);
        m_impl->m_file_map.map(m_impl->m_file, File::access_ReadWrite, 
                               sizeof (IPMFileSharedInfo) + size, File::map_NoSync);
        File::UnmapGuard fug_1(m_impl->m_file_map);

        // for files of proper size, having an exclusive lock does not alone guarantee exclusivity.
        // - other threads may have held the exclusive lock and are now asking to enter shared state,
        //   we just beat them (race) and got the exclusive lock
        // - other threads may be trying to get the exclusive lock to detect if they are alone,
        //   and to do that, they have to release their shared lock. But we beat them (race) and
        //   got the exclusive lock.
        // - other threads may have released their locks and are exiting (possibly with one of them
        //   trying to delete the file in a moment)
        // All the above scenarios are detected by inspecting a transition count. The transition
        // count is incremented whenever a thread intiates a state transition and decremented when
        // the transition is complete. A non-zero transition count indicates that the exclusive lock
        // should not be trusted.
        //
        // A program crashing mid-transition will leave the lock file permanently damaged, in the
        // sense that no one can obtain exclusive access until the lock file is removed. The state
        // transitions are implemented such that they minimize the frequency.
        //
        // As long as programs are mid-transition, they must increment an activity counter within
        // a very short time frame. When a file with non-zero transition count is encountered, the
        // activity counter can be sampled and if it doesn't change within a short time frame, the
        // file may be considered to be in indeterminate state, and an exception is thrown to
        // indicate that operator intervention is required.

        // TODO: Look for transition changes
    };
    throw PresumablyStaleFile(m_impl->m_path);
}


void* IPMFile::add_map(size_t size)
{
    return NULL;
}


void IPMFile::remove_map(void*)
{
}


void IPMFile::share()
{
}


void IPMFile::close()
{
}


IPMFile::~IPMFile()
{
    if (m_impl->m_is_open) close();
}


bool IPMFile::try_get_exclusive_access()
{
    // FIXME
    return false;
}


bool IPMFile::is_removed()
{
    // FIXME
    return false;
}

} // namespace tightdb
