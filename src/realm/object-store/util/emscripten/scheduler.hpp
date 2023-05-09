////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#include <realm/object-store/util/scheduler.hpp>
#include <emscripten/eventloop.h>

#if defined(__EMSCRIPTEN_PTHREADS__) || defined(__EMSCRIPTEN_WASM_WORKERS__)
#error "This scheduler implementation is not compatible with multi-threaded WebAssembly."
#endif

namespace realm::util {
class EmscriptenScheduler : public realm::util::Scheduler {
public:
    EmscriptenScheduler() = default;

    ~EmscriptenScheduler()
    {
        if (m_timeout) {
            emscripten_clear_timeout(*m_timeout);
        }
    }

    bool is_on_thread() const noexcept override
    {
        return true;
    }
    bool is_same_as(const Scheduler* other) const noexcept override
    {
        return dynamic_cast<const EmscriptenScheduler*>(other) != nullptr;
    }
    bool can_invoke() const noexcept override
    {
        return true;
    }

    void invoke(UniqueFunction<void()>&& fn) override
    {
        m_queue.push(std::move(fn));
        if (!m_timeout) {
            m_timeout = emscripten_set_timeout(timeout_callback, 0, this);
        }
    }

private:
    InvocationQueue m_queue;
    std::optional<int> m_timeout;

    static void timeout_callback(void* user_data)
    {
        auto scheduler = reinterpret_cast<EmscriptenScheduler*>(user_data);
        scheduler->m_timeout.reset();
        scheduler->m_queue.invoke_all();
    }
};
} // namespace realm::util
