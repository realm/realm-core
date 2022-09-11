////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "test_file.hpp"

#include "test_utils.hpp"
#include "../util/crypt_key.hpp"
#include <realm/object-store/impl/realm_coordinator.hpp>

#include <realm/db.hpp>
#include <realm/disable_sync_to_disk.hpp>
#include <realm/history.hpp>
#include <realm/string_data.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/file.hpp>

#include <cstdlib>
#include <iostream>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>

inline static int mkstemp(char* _template)
{
    return _open(_mktemp(_template), _O_CREAT | _O_TEMPORARY, _S_IREAD | _S_IWRITE);
}
#else
#include <unistd.h>
#endif

#if REALM_HAVE_CLANG_FEATURE(thread_sanitizer)
#include <condition_variable>
#include <functional>
#include <thread>
#include <map>
#endif

using namespace realm;

TestFile::TestFile()
{
    disable_sync_to_disk();
    m_temp_dir = util::make_temp_dir();
    path = (fs::path(m_temp_dir) / "realm.XXXXXX").string();
    if (const char* crypt_key = test_util::crypt_key()) {
        encryption_key = std::vector<char>(crypt_key, crypt_key + 64);
    }
    int fd = mkstemp(path.data());
    if (fd < 0) {
        int err = errno;
        throw std::system_error(err, std::system_category());
    }
#ifdef _WIN32
    _close(fd);
    _unlink(path.c_str());
#else // POSIX
    close(fd);
    unlink(path.c_str());
#endif

    schema_version = 0;
}

TestFile::~TestFile()
{
    if (!m_persist) {
        try {
            util::File::try_remove(path);
            util::try_remove_dir_recursive(m_temp_dir);
        }
        catch (...) {
            // clean up is best effort, ignored.
        }
    }
}

DBOptions TestFile::options() const
{
    DBOptions options;
    options.durability = in_memory ? DBOptions::Durability::MemOnly : DBOptions::Durability::Full;
    return options;
}

InMemoryTestFile::InMemoryTestFile()
{
    in_memory = true;
    encryption_key = std::vector<char>();
}

#if REALM_HAVE_CLANG_FEATURE(thread_sanitizer)
// MARK: - TsanNotifyWorker
// A helper which synchronously runs on_change() on a fixed background thread
// so that ThreadSanitizer can potentially detect issues
// This deliberately uses an unsafe spinlock for synchronization to ensure that
// the code being tested has to supply all required safety
static class TsanNotifyWorker {
public:
    TsanNotifyWorker()
    {
        m_thread = std::thread([&] {
            work();
        });
    }

    void work()
    {
        while (true) {
            auto value = m_signal.load(std::memory_order_relaxed);
            if (value == 0 || value == 1)
                continue;
            if (value == 2)
                return;

            if (value & 1) {
                // Synchronize on the first handover of a given coordinator.
                value &= ~1;
                m_signal.load();
            }

            auto c = reinterpret_cast<_impl::RealmCoordinator*>(value);
            c->on_change();
            m_signal.store(1, std::memory_order_relaxed);
        }
    }

    ~TsanNotifyWorker()
    {
        m_signal = 2;
        m_thread.join();
    }

    void on_change(const std::shared_ptr<_impl::RealmCoordinator>& c)
    {
        auto& it = m_published_coordinators[c.get()];
        if (it.lock()) {
            m_signal.store(reinterpret_cast<uintptr_t>(c.get()), std::memory_order_relaxed);
        }
        else {
            // Synchronize on the first handover of a given coordinator.
            it = c;
            m_signal = reinterpret_cast<uintptr_t>(c.get()) | 1;
        }

        while (m_signal.load(std::memory_order_relaxed) != 1)
            ;
    }

private:
    std::atomic<uintptr_t> m_signal{0};
    std::thread m_thread;
    std::map<_impl::RealmCoordinator*, std::weak_ptr<_impl::RealmCoordinator>> m_published_coordinators;
} s_worker;

void on_change_but_no_notify(Realm& realm)
{
    s_worker.on_change(_impl::RealmCoordinator::get_existing_coordinator(realm.config().path));
}

void advance_and_notify(Realm& realm)
{
    on_change_but_no_notify(realm);
    realm.notify();
}

#else // REALM_HAVE_CLANG_FEATURE(thread_sanitizer)

void on_change_but_no_notify(Realm& realm)
{
    _impl::RealmCoordinator::get_coordinator(realm.config().path)->on_change();
}

void advance_and_notify(Realm& realm)
{
    on_change_but_no_notify(realm);
    realm.notify();
}
#endif
