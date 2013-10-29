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
#include "file.hpp"
#include <limits>

namespace tightdb {

// The contents of the shared file is ONLY EVER changed by threads holding a lock
// (either shared or exclusive). The file may be removed/deleted without holding a
// lock, but ONLY if it is in the Stale state.

struct IPMFileSharedInfo {
    // IPMFile specific fields:
    enum State { Unitialized = 0, Ready, Stale };
    Atomic<State> m_state;
    Atomic<uint32_t> m_transition_count;
};

struct IPMFile::IPMFileImplementation {
    std::string m_path;
    bool m_is_open;
    File m_file;
    File::Map<IMPFileSharedInfo> m_file_map;
    IPMFileMap* m_info;
    void init(std::string);
};

void IPMFile::IPMFileImplementation::init(std::string path)
{
    m_path = path;
    m_is_open = false;
    m_map = NULL;
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

    bool timeleft = true;
    while (msec_timeout >= 0) {

        size_t file_size = 0;
        bool need_init;
        bool got_exclusive;
        bool got_shared;
        m_impl->m_file.open(m_impl->m_path, need_init);

        // Try to get a lock - preferably an exclusive lock,
        // but if not be contend with a shared lock. It is important to try
        // (and potentially retry) in order to ensure that ONE of the clients
        // gets exclusive access if possible.
        got_exclusive = m_impl->m_file.try_lock_exclusive();
        if (got_exclusive)
            got_shared = false;
        else
            got_shared = m_impl->m_file.try_lock_shared();
        if (!got_exclusive && !got_shared) {

            // someone else has exclusive access, backout and retry
            m_impl->m_file.close();
            msec_timeout--;
            micro_sleep(1000);
            continue;

        }

        // Regardless of lock, if the file is marked as stale, try to remove it (non-faulting way)
        // and start all over again. Unfortunately, to get access to the State field, we need to
        // map it, and to map it we must be sure that the file is large enough, hence first some
        // checks for file size
        if (int_cast_with_overflow_detect(m_impl->m_file.get_size(), file_size)) {

            m_impl->m_file.unlock();
            m_impl->m_file.close();
            // File::try_remove(m_impl->m_path); <-- don't remove it, require operator intervention
            throw std::runtime_error("Lock file too large");
        }
        if (file_size < size+sizeof(IPMFileMap)) {

            m_impl->m_file.unlock();
            m_impl->m_file.close();
            File::try_remove(m_impl->m_path);
        }
        // now we can safely map the file and then check if it is marked Stale.
        // (Finding a file marked stale is very unlikely. We mark files Stale just prior to closing
        // and deleting them, so the only way to get a stay file is if a thread crashes after deciding
        // to remove the file, but before actually removing it.



        // Coming here, we have a lock AND we have an initialized file


        // coming here we have a lock, either exclusive or shared.
        // we now do a set of refined checks
        // 1- if the file is too small to be mapped and we have exclusive access, initialise it
        //    (otherwise backout and retry)
        // 2- if the file is ok in size, but marked as stale, delete it and retry
        // 3- if we have exclusive access, the file isn't stale and the transition count is non zero,
        //    backout and retry. We assume that some other process is transitioning the state of the file.

        // File Initialization step
        if (need_init) {

            // complete zero-initialization of entire mapped area
            file_size = sizeof (IPMFileMap) + size;
            char* empty_buf = new char[file_size];
            std::fill(empty_buf, empty_buf+file_size, 0);
            m_impl->m_file.write(empty_buf, file_size);
            delete[] empty_buf;

        } 
        else {

            // wait for file to be large enough to be mapped
            File::CloseGuard fcg(m_impl->m_file);
            if (int_cast_with_overflow_detect(m_impl->m_file.get_size(), file_size)) {

                throw std::runtime_error("Lock file too large");
            }
            while (msec_timeout >= 0 && file_size < size+sizeof(IPMFileMap)) {

                msec_timeout--;
                micro_sleep(1000);
                if (int_cast_with_overflow_detect(m_impl->m_file.get_size(), file_size)) {
                    throw std::runtime_error("Lock file too large");
                }
            }
            if (msec_timeout <= 0) {

                throw PresumablyStaleFile(m_impl->m_path);
            }
            fcg.release();
        }

        File::CloseGuard fcg(m_impl->m_file);
        // File is now known to be at least large enough to be mapped

        // File mapping step
        m_impl->m_file_map.map(m_impl->m_file, File::access_ReadWrite, 
                               sizeof (IPMFileSharedInfo), File::map_NoSync);
        File::UnmapGuard fug_1(m_impl->m_file_map);

        // Transition to fully initialized


    } while (timeleft);

    return NULL;
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
