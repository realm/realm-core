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
namespace _impl {
class RealmCoordinator;

namespace win32 {
// #if REALM_WINDOWS
#define OpenFileMappingInternal(dwDesiredAccess, bInheritHandle, lpName)\
        OpenFileMappingW(dwDesiredAccess, bInheritHandle, lpName);

#define CreateFileMappingInternal(hFile, lpFileMappingAttributes, flProtect, dwMaximumSizeHigh, dwMaximumSizeLow, lpName)\
        CreateFileMappingW(hFile, lpFileMappingAttributes, flProtect, dwMaximumSizeHigh, dwMaximumSizeLow, lpName);

#define MapViewOfFileInternal(hFileMappingObject, dwDesiredAccess, dwFileOffsetHigh, dwFileOffsetLow, dwNumberOfBytesToMap)\
        MapViewOfFile(hFileMappingObject, dwDesiredAccess, dwFileOffsetHigh, dwFileOffsetLow, dwNumberOfBytesToMap);

// // #elif REALM_UWP
// // #define OpenFileMappingInternal(dwDesiredAccess, bInheritHandle, lpName)\
// //         OpenFileMappingFromApp(dwDesiredAccess, bInheritHandle, lpName);

// // #define CreateFileMappingInternal(hFile, lpFileMappingAttributes, flProtect, dwMaximumSizeHigh, dwMaximumSizeLow, lpName)\
// //         CreateFileMappingFromApp(hFile, lpFileMappingAttributes, flProtect, dwMaximumSizeHigh, dwMaximumSizeLow, lpName);

// // #define MapViewOfFileInternal(hFileMappingObject, dwDesiredAccess, dwFileOffsetHigh, dwFileOffsetLow, dwNumberOfBytesToMap)\
// //         MapViewOfFileFromApp(hFileMappingObject, dwDesiredAccess, dwFileOffsetHigh, dwFileOffsetLow, dwNumberOfBytesToMap);

// #elif
// #error Unknown win32 platform
// #endif

template <class T, void (*Initializer)(T&)>
class SharedMemory {
public:
    SharedMemory(LPCWSTR name) {
        //assume another process have already initialzied the shared memory
        bool shouldInit = false;

        HANDLE mapping = OpenFileMappingInternal(FILE_MAP_ALL_ACCESS, FALSE, name);
        auto error = GetLastError();

        if (mapping == NULL) {
            mapping = CreateFileMappingInternal(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE, 0, sizeof(T), name);
            error = GetLastError();

            //init since this is the first process creating the shared memory
            shouldInit = true;
        }

        if (mapping == NULL) {
            throw std::system_error(error, std::system_category());
        }

        LPVOID view = MapViewOfFileInternal(mapping, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(T));
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

    T& get() const noexcept { return *m_memory; }

    ~SharedMemory() {
        if (m_memory) {
            UnmapViewOfFile(m_memory);
            m_memory = nullptr;
        }
    }

private:
    T* m_memory = nullptr;
};
}

class ExternalCommitHelper {
public:
    ExternalCommitHelper(RealmCoordinator& parent);
    ~ExternalCommitHelper();

    void notify_others();

private:
    void listen();

    RealmCoordinator& m_parent;

    // The listener thread
    std::future<void> m_thread;

    win32::SharedMemory<util::InterprocessCondVar::SharedPart, util::InterprocessCondVar::init_shared_part> m_condvar_shared;

    util::InterprocessCondVar m_commit_available;
    util::InterprocessMutex m_mutex;
    bool m_keep_listening = true;
};

} // namespace _impl
} // namespace realm
