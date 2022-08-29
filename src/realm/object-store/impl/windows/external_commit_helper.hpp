////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include <realm/db.hpp>

#include <future>
#include <windows.h>

namespace realm {
struct RealmConfig;
namespace _impl {
class RealmCoordinator;

namespace win32 {
template <class T, void (*Initializer)(T&)>
class SharedMemory {
public:
    SharedMemory(std::string name)
    {
        // assume another process have already initialzied the shared memory
        bool shouldInit = false;

        std::wstring wname(name.begin(), name.end());
        LPCWSTR lpName = wname.c_str();

        m_mapped_file = OpenFileMappingW(FILE_MAP_ALL_ACCESS, FALSE, lpName);
        auto error = GetLastError();

        if (m_mapped_file == NULL) {
            m_mapped_file = CreateFileMappingW(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE, 0, sizeof(T), lpName);
            error = GetLastError();

            // initialize since this is the first process creating the shared memory
            shouldInit = true;
        }

        if (m_mapped_file == NULL) {
            throw std::system_error(error, std::system_category());
        }

        LPVOID view = MapViewOfFile(m_mapped_file, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(T));
        error = GetLastError();
        if (view == NULL) {
            throw std::system_error(error, std::system_category());
        }
        m_memory = reinterpret_cast<T*>(view);

        if (shouldInit) {
            try {
                Initializer(get());
            }
            catch (...) {
                UnmapViewOfFile(m_memory);
                throw;
            }
        }
    }

    T* operator->() const noexcept
    {
        return m_memory;
    }

    T& get() const noexcept
    {
        return *m_memory;
    }

    ~SharedMemory()
    {
        if (m_memory) {
            UnmapViewOfFile(m_memory);
            m_memory = nullptr;
        }

        if (m_mapped_file) {
            CloseHandle(m_mapped_file);
            m_mapped_file = nullptr;
        }
    }

private:
    T* m_memory = nullptr;
    HANDLE m_mapped_file = nullptr;
};
} // namespace win32

class ExternalCommitHelper {
public:
    ExternalCommitHelper(RealmCoordinator& parent, const RealmConfig&);
    ~ExternalCommitHelper();

    void notify_others();

private:
    void listen();

    RealmCoordinator& m_parent;

    // The listener thread
    std::thread m_thread;

    struct SharedPart {
        util::InterprocessCondVar::SharedPart cv;
        int64_t num_signals;

        static void init(SharedPart& sp)
        {
            util::InterprocessCondVar::init_shared_part(sp.cv);
            sp.num_signals = 0;
        }
    };

    win32::SharedMemory<SharedPart, SharedPart::init> m_shared_part;

    util::InterprocessCondVar m_commit_available;
    util::InterprocessMutex m_mutex;
    bool m_keep_listening = true;
    int64_t m_last_count;
};

} // namespace _impl
} // namespace realm
