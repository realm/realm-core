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

#include <iostream>
using namespace std;

namespace tightdb {

struct IPMFile::IPMFileImplementation {
    std::string m_path;
    bool m_is_open;
    bool m_is_exclusive;
    File m_file;
    File::Map<IPMFileWrapper<uint64_t> > m_file_map;
    void init(std::string);
};

void IPMFile::IPMFileImplementation::init(std::string path)
{
    m_path = path;
    m_is_open = false;
    m_is_exclusive = false;
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

        cerr << "open: attempt to open file " << msec_timeout << endl;
        size_t file_size = 0;
        bool need_init;
        bool got_exclusive;
        bool got_shared;
        m_impl->m_file.open(m_impl->m_path, need_init);

        // Try to get a lock - preferably an exclusive lock,
        // but if not be contend with a shared lock for now. It is important to try
        // (and potentially retry) in order to ensure that ONE of the clients
        // gets exclusive access if possible. It is also important to back out
        // and reopen the file in case of contention, because contention may be
        // an indication that someone is in the process of removing the file, and
        // if so we really, really need to get to the new one (or create it) instead
        // of reusing a file which has been deleted.
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
                m_impl->m_file.unlock();
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
        File::UnmapGuard fug(m_impl->m_file_map);

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
        // transitions are implemented such that they should minimize the frequency.

        IPMFileWrapper<uint64_t>* wrapped_data = m_impl->m_file_map.get_addr();
        IPMFileSharedInfo* info = & wrapped_data->info;
        if (info->m_transition_count.load_acquire() || info->m_exit_count.load_acquire()) {
            // conflict or stale file...
            m_impl->m_file.unlock();
            micro_sleep(1000);
            msec_timeout--;
            continue;
        }

        // lock is now considered valid
        is_exclusive = m_impl->m_is_exclusive = got_exclusive;
        fug.release();
        fcg.release();
        if (is_exclusive)
            cerr << "open: exclusive" << endl;
        else
            cerr << "open: shared" << endl;
        m_impl->m_is_open = true;
        return static_cast<void*>( & wrapped_data->user_data );
    };
    cerr << "open: timeout, throwing PresumablyStaleFile" << endl;
    throw PresumablyStaleFile(m_impl->m_path);
}


void IPMFile::share()
{
    if (m_impl->m_is_exclusive) {
        cerr << "exclusive -> shared (begin)" << endl;
        // transition to shared state:
        IPMFileWrapper<uint64_t>* wrapped_data = m_impl->m_file_map.get_addr();
        IPMFileSharedInfo* info = & wrapped_data->info;
        info->m_transition_count.inc();
        m_impl->m_file.unlock();
        m_impl->m_file.lock_shared();
        info->m_transition_count.dec();
        m_impl->m_is_exclusive = false;
        cerr << "exclusive -> shared (done)" << endl;
    }
}


void IPMFile::close()
{
    if (m_impl->m_is_open) {
        cerr << "closing..." << endl;

        bool is_exclusive = try_get_exclusive_access(true);
        if (is_exclusive) {

            // we now poison the file by incrementing the transition count. This
            // prevents anybody from reusing the file. It is a necessary precaution
            // as we are going to delete the file in a moment. If someone else started
            // reusing the file after we unlock it but before it is deleted, the world
            // might see a general increase in unhappiness. We don't want that, do we?
            IPMFileWrapper<uint64_t>* wrapped_data = m_impl->m_file_map.get_addr();
            IPMFileSharedInfo* info = & wrapped_data->info;
            info->m_transition_count.inc();
            m_impl->m_file_map.unmap();
            m_impl->m_file.unlock();
            m_impl->m_file.close();
            m_impl->m_is_open = false;
            cerr << "removing..." << endl;
            File::try_remove(m_impl->m_path);
        }
        // If is_exclusive is false, try_get_exclusive_access has already closed the file.
    }
}

File& IPMFile::get_file()
{
    return m_impl->m_file;
}

IPMFile::~IPMFile()
{
    if (m_impl->m_is_open) close();
}

bool IPMFile::has_exclusive_access()
{
    return m_impl->m_is_exclusive;
}

bool IPMFile::try_get_exclusive_access(bool promise_to_exit)
{
    if (m_impl->m_is_exclusive)
        return true;

    cerr << "shared -> exclusive (begin)" << (promise_to_exit ? "  <exiting>" : "") << endl;
    // try to transition to exclusive state:
    IPMFileWrapper<uint64_t>* wrapped_data = m_impl->m_file_map.get_addr();
    IPMFileSharedInfo* info = & wrapped_data->info;

    if (promise_to_exit) {

        // look for conflicts early, before trying to grab the lock
        if (info->m_transition_count.load_relaxed()) {
            cerr << "shared -> exclusive (early-out contention)" << endl;
            return false;
        }

        // go for it:
        info->m_exit_count.inc();
        m_impl->m_file.unlock();
        if (m_impl->m_file.try_lock_exclusive()) {
            
            // got the lock, but look out for contenders.
            // ignore contention for exit.
            if (info->m_transition_count.load_relaxed() == 0) {

                // no conflicts:
                cerr << "shared -> exclusive (succes)" << endl;
                m_impl->m_is_exclusive = true;
                // poison the file by incrementing the transition count.
                // This makes sure that no one get exclusive access to the file
                // if the caller fails before removing the file (through close)
                // we get a stale file.
                info->m_transition_count.inc();
                return true;
            }
        }
        // could not get the lock, or there was contention. As we have promised
        // to exit, we loose all locks and close the file, but we must revert exit count first.
        cerr << "shared -> exclusive (contention) -> implicitly closed" << endl;
        info->m_exit_count.dec();
        m_impl->m_file_map.unmap();
        m_impl->m_file.close();
        m_impl->m_is_open = false;
        return false;
        
    }
    else /* not promise_to_exit */ {

        if (info->m_transition_count.load_acquire() || info->m_exit_count.load_acquire()) {
            // conflict, even before lifting the shared lock, so just give up
            cerr << "shared -> exclusive (early-out contention)" << endl;
            return false;
        }

        // indicate to other lock contenders, that we are here even though we 
        // will now operate without locks for a brief moment. 
        info->m_transition_count.inc();

        // go for it:
        m_impl->m_file.unlock();
        if (m_impl->m_file.try_lock_exclusive()) {

            // we got the exclusive lock! look for conflicts:
            if (info->m_transition_count.load_relaxed() + info->m_exit_count.load_relaxed() == 1) {

                // no conflicts
                info->m_transition_count.dec();
                m_impl->m_is_exclusive = true;
                cerr << "shared -> exclusive (succes)" << endl;
                return true;
            }
        }
        // coming here, either because we couldn't get the exclusive lock, or because
        // a conflict was detected. Re-acquire shared lock and indicate that we did
        // not get the exclusive access.
        m_impl->m_file.lock_shared();
        info->m_transition_count.dec();
        cerr << "shared -> exclusive (contention)" << endl;
        return false;
    }
}


bool IPMFile::is_removed()
{
    // TODO:
    // to implement this, we need to rely on inode info, timestampts and other stuff...
    return m_impl->m_file.is_removed();
}

} // namespace tightdb
