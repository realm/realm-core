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

namespace tightdb {

struct IPMFileImplementation {
    std::string m_path;
    bool m_is_open;
    File m_file;
};

struct IPMFileMap {
    // IPMFile specific fields:
    Enum State { Uninit, Initialized, Stale };
    Atomic<State> m_state;
    Atomic<uint32> m_transition_count;
    // Client space:
    char m_client[0];
};

IPMFile::IPMFile(std::string file_path) : IPMFile()
{
    m_impl->m_path = file_path;
}


IPMFile::IPMFile()
{
    m_impl = new IPMFileImplementation;
    m_impl->m_path = "";
    m_is_open = false;
}


void IPMFile::associate(std::string file_path)
{
    if (m_impl->m_is_open)
        throw runtime_error("New association cannot be established while file is open");

    m_impl->m_path = file_path;
}


void* IPMFile::open(bool& is_exclusive, size_t size, int msec_timeout = 0)
{
    if (m_impl->is_open)
        throw runtime_error("Cannot open already opened file");
}


void* IPMFile::add_map(size_t size)
{
    return NULL
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
