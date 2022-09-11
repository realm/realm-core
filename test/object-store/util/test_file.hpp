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

#ifndef REALM_TEST_UTIL_TEST_FILE_HPP
#define REALM_TEST_UTIL_TEST_FILE_HPP

#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/util/tagged_bool.hpp>

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>

#include <thread>

namespace realm {
class Schema;
enum class SyncSessionStopPolicy;
struct DBOptions;
struct SyncConfig;
} // namespace realm

class JoiningThread {
public:
    template <typename... Args>
    JoiningThread(Args&&... args)
        : m_thread(std::forward<Args>(args)...)
    {
    }
    ~JoiningThread()
    {
        if (m_thread.joinable())
            m_thread.join();
    }
    void join()
    {
        m_thread.join();
    }

private:
    std::thread m_thread;
};


struct TestFile : realm::Realm::Config {
    TestFile();
    ~TestFile();

    // The file should outlive the object, ie. should not be deleted in destructor
    void persist()
    {
        m_persist = true;
    }

    realm::DBOptions options() const;

private:
    bool m_persist = false;
    std::string m_temp_dir;
};

struct InMemoryTestFile : TestFile {
    InMemoryTestFile();
};

void advance_and_notify(realm::Realm& realm);
void on_change_but_no_notify(realm::Realm& realm);

#endif
