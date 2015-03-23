/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
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

#include <tightdb/util/platform_specific_condvar.hpp>





using namespace std;
using namespace realm;
using namespace realm::util;

string PlatformSpecificCondVar::internal_naming_prefix = "/RealmsBigFriendlySemaphore";

void PlatformSpecificCondVar::set_resource_naming_prefix(std::string prefix)
{
    internal_naming_prefix = prefix + "RLM";
}

PlatformSpecificCondVar::PlatformSpecificCondVar()
{
    m_shared_part = 0;
    m_sem = 0;
}






void PlatformSpecificCondVar::close() REALM_NOEXCEPT
{
    if (m_sem) { // true if emulating a process shared condvar
        sem_close(m_sem);
        m_sem = 0;
        return; // we don't need to clean up the SharedPart
    }
    // we don't do anything to the shared part, other CondVars may shared it
    m_shared_part = 0;
}


PlatformSpecificCondVar::~PlatformSpecificCondVar() REALM_NOEXCEPT
{
    close();
}



void PlatformSpecificCondVar::set_shared_part(SharedPart& shared_part, std::string path, std::size_t offset_of_condvar)
{
    REALM_ASSERT(m_shared_part == 0);
    close();
    m_shared_part = &shared_part;
    static_cast<void>(path);
    static_cast<void>(offset_of_condvar);
#ifdef REALM_CONDVAR_EMULATION
    m_sem = get_semaphore(path,offset_of_condvar);
#endif
}

sem_t* PlatformSpecificCondVar::get_semaphore(std::string path, std::size_t offset)
{
    uint64_t magic = 0;
    for (unsigned int i=0; i<path.length(); ++i)
        magic ^= (i+1) * 0x794e80091e8f2bc7ULL * (unsigned int)(path[i]);
    std::string name = internal_naming_prefix;
    magic *= (offset+1);
    name += 'A'+(magic % 23);
    magic /= 23;
    name += 'A'+(magic % 23);
    magic /= 23;
    name += 'A'+(magic % 23);
    magic /= 23;
    REALM_ASSERT(m_shared_part);
    if (m_sem == 0) {
        m_sem = sem_open(name.c_str(), O_CREAT, S_IRWXG | S_IRWXU, 0);
        // FIXME: error checking
    }
    return m_sem;
}


void PlatformSpecificCondVar::init_shared_part(SharedPart& shared_part) {
#ifdef REALM_CONDVAR_EMULATION
    shared_part.waiters = 0;
    shared_part.signal_counter = 0;
#else
    new ((void*) &shared_part) CondVar(CondVar::process_shared_tag());
#endif // REALM_CONDVAR_EMULATION
}


